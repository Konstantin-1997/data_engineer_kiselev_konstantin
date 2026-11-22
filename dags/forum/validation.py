"""Проверки качества данных на pandera.

Три точки контроля:
  logs_schema / users_schema — сгенерированные данные до загрузки (validate_generated_data);
  stg_schema                 — логи периода перед расчётом метрик (validate_data);
  mart_schema                — готовые метрики: значения в допустимых диапазонах (transform_aggregates).
Все вызовы — lazy=True: собираются ВСЕ нарушения сразу, а не первое.
"""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa

from forum import config as C

ACTIONS = list(C.ACTIONS)


# ---------- последовательность событий ----------

def _login_time(df: pd.DataFrame) -> pd.Series:
    """Для каждой строки — время успешного логина её сессии (NaT, если сессии нет или логина в ней нет)."""
    logins = df[(df["action_type"] == "login") & (df["status"] == "success") & df["session_id"].notna()]
    first = logins.groupby("session_id")["event_time"].min()
    return df["session_id"].map(first)


def no_logout_without_login(df: pd.DataFrame) -> pd.Series:
    """Логаут только в сессии, где раньше был логин."""
    is_logout = df["action_type"] == "logout"
    login_at = _login_time(df)
    return ~is_logout | (login_at.notna() & (df["event_time"] >= login_at))


def no_topic_without_auth(df: pd.DataFrame) -> pd.Series:
    """Успешно создать тему может только залогиненный: есть user_id и сессия, начавшаяся раньше."""
    ok_create = (df["action_type"] == "create_topic") & (df["status"] == "success")
    login_at = _login_time(df)
    return ~ok_create | (df["user_id"].notna() & login_at.notna() & (df["event_time"] >= login_at))


def session_events_after_login(df: pd.DataFrame) -> pd.Series:
    """Любое действие внутри сессии — после её логина."""
    login_at = _login_time(df)
    return df["session_id"].isna() | (login_at.notna() & (df["event_time"] >= login_at))


def error_has_code(df: pd.DataFrame) -> pd.Series:
    return (df["status"] == "error") == df["error_code"].notna()


def one_login_per_session(df: pd.DataFrame) -> bool:
    logins = df[df["action_type"] == "login"]
    return bool((logins.groupby("session_id").size() <= 1).all())


# ---------- требования ТЗ к сгенерированным данным ----------

def _per_day(df: pd.DataFrame) -> pd.DataFrame:
    days = df["event_time"].dt.tz_convert("UTC").dt.date
    return df.assign(day=days)


def each_action_min_per_day(df: pd.DataFrame) -> bool:
    """Минимум MIN_PER_DAY действий каждого типа в каждый день (успешных)."""
    d = _per_day(df[df["status"] == "success"])
    counts = d.groupby(["day", "action_type"]).size().unstack(fill_value=0).reindex(columns=ACTIONS, fill_value=0)
    return bool((counts >= C.MIN_PER_DAY).all().all())


def topic_auth_errors_per_day(df: pd.DataFrame) -> bool:
    """Минимум MIN_TOPIC_AUTH_ERRORS_PER_DAY ошибок «создание темы без логина» в каждый день."""
    d = _per_day(df)
    err = d[(d["action_type"] == "create_topic") & (d["error_code"] == "AUTH_REQUIRED") & d["user_id"].isna()]
    per_day = err.groupby("day").size().reindex(sorted(d["day"].unique()), fill_value=0)
    return bool((per_day >= C.MIN_TOPIC_AUTH_ERRORS_PER_DAY).all())


def anon_messages_balanced(df: pd.DataFrame) -> bool:
    """Сообщения: доля анонимов за месяц в ANON_MESSAGES_SHARE (примерно поровну)."""
    msgs = df[(df["action_type"] == "write_message") & (df["status"] == "success")]
    share = msgs["user_id"].isna().mean()
    lo, hi = C.ANON_MESSAGES_SHARE
    return bool(lo <= share <= hi)


def one_calendar_month(df: pd.DataFrame) -> bool:
    """Данные покрывают ровно один календарный месяц: все дни месяца есть, лишних нет."""
    days = pd.Series(_per_day(df)["day"].unique())
    first = days.min()
    month_days = pd.date_range(first.replace(day=1), periods=pd.Timestamp(first).days_in_month, freq="D").date
    return bool(set(days) == set(month_days))


# ---------- схемы ----------

_log_columns = {
    "user_id": pa.Column("Int64", nullable=True),
    "session_id": pa.Column(str, nullable=True),
    "action_type": pa.Column(str, pa.Check.isin(ACTIONS), nullable=False),
    "object_type": pa.Column(str, pa.Check.isin(["topic", "message"]), nullable=True),
    "object_id": pa.Column("Int64", nullable=True),
    "status": pa.Column(str, pa.Check.isin(["success", "error"]), nullable=False),
    "error_code": pa.Column(str, nullable=True),
    "event_time": pa.Column(pd.DatetimeTZDtype(tz="UTC"), nullable=False),
}
_sequence_checks = [
    pa.Check(no_logout_without_login, error="logout без предшествующего login в той же сессии"),
    pa.Check(no_topic_without_auth, error="успешное создание темы без авторизации"),
    pa.Check(session_events_after_login, error="действие в сессии раньше её логина"),
    pa.Check(error_has_code, error="status=error без error_code или наоборот"),
    pa.Check(one_login_per_session, error="больше одного логина в сессии"),
]

logs_schema = pa.DataFrameSchema(
    {**_log_columns, "metadata": pa.Column(object, nullable=False)},
    checks=_sequence_checks + [
        pa.Check(each_action_min_per_day, error=f"меньше {C.MIN_PER_DAY} действий какого-то типа в какой-то день"),
        pa.Check(topic_auth_errors_per_day, error=f"меньше {C.MIN_TOPIC_AUTH_ERRORS_PER_DAY} ошибок AUTH_REQUIRED у create_topic в какой-то день"),
        pa.Check(anon_messages_balanced, error="доля анонимных сообщений вне диапазона «примерно поровну»"),
        pa.Check(one_calendar_month, error="данные не покрывают ровно один календарный месяц"),
    ],
    strict=True,
    coerce=True,
)

users_schema = pa.DataFrameSchema(
    {
        "id": pa.Column("int64", unique=True, nullable=False),
        "username": pa.Column(str, unique=True, nullable=False),
        "email": pa.Column(str, pa.Check.str_matches(r"^[^@\s]+@[^@\s]+$"), unique=True, nullable=False),
        "registered_at": pa.Column(pd.DatetimeTZDtype(tz="UTC"), nullable=False),
    },
    strict=True,
    coerce=True,
)

# stg: последовательность проверяется по всему окну, включая день-запас до периода
stg_schema = pa.DataFrameSchema(
    {**_log_columns, "id": pa.Column("int64", unique=True, nullable=False), "event_date": pa.Column(object, nullable=False)},
    checks=_sequence_checks,
    strict=True,
    coerce=True,
)

mart_schema = pa.DataFrameSchema(
    {
        "day": pa.Column(object, nullable=False, unique=True),
        "new_accounts": pa.Column("int64", pa.Check.ge(0)),
        "anon_messages_pct": pa.Column(float, pa.Check.in_range(0, 100), nullable=True),
        "total_messages": pa.Column("int64", pa.Check.ge(0)),
        "topics_growth_pct": pa.Column(float, pa.Check.ge(-100), nullable=True),
        "avg_session_duration_sec": pa.Column(float, pa.Check.ge(0), nullable=True),
        "d1_retention_pct": pa.Column(float, pa.Check.in_range(0, 100), nullable=True),
    },
    strict=True,
    coerce=True,
)
