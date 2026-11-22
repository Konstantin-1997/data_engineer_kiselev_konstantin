"""Тесты генератора и проверок качества. Запуск: docker compose exec airflow-scheduler pytest -q /opt/airflow/tests"""
import sys

import pandas as pd
import pandera.pandas as pa
import pytest

sys.path.insert(0, "/opt/airflow/dags")

from forum import config as C  # noqa: E402
from forum.generator import generate_logs, generate_users, month_bounds  # noqa: E402
from forum.validation import logs_schema, users_schema  # noqa: E402

MONTH, SEED = "2026-10", 7


@pytest.fixture(scope="module")
def data():
    users = generate_users(MONTH, SEED)
    logs = pd.DataFrame(generate_logs(users, MONTH, SEED)).astype({"user_id": "Int64", "object_id": "Int64"})
    return pd.DataFrame(users), logs


def test_month_bounds():
    assert month_bounds("2026-12") == (pd.Timestamp("2026-12-01").date(), pd.Timestamp("2027-01-01").date())


def test_valid_data_passes(data):
    users, logs = data
    users_schema.validate(users, lazy=True)
    logs_schema.validate(logs, lazy=True)


def test_deterministic():
    a = generate_logs(generate_users(MONTH, SEED), MONTH, SEED)
    b = generate_logs(generate_users(MONTH, SEED), MONTH, SEED)
    assert a == b


def test_thresholds_per_day(data):
    _, logs = data
    days = logs["event_time"].dt.date
    ok = logs[logs.status == "success"]
    counts = ok.groupby([days[ok.index], "action_type"]).size().unstack(fill_value=0)
    assert counts.shape[0] == 31 and (counts >= C.MIN_PER_DAY).all().all()
    errors = logs[(logs.action_type == "create_topic") & (logs.error_code == "AUTH_REQUIRED")].groupby(days).size()
    assert len(errors) == 31 and (errors >= C.MIN_TOPIC_AUTH_ERRORS_PER_DAY).all()


def _fails(df: pd.DataFrame, text: str):
    with pytest.raises(pa.errors.SchemaErrors) as e:
        logs_schema.validate(df, lazy=True)
    found = e.value.failure_cases.astype(str).to_string()      # таблица нарушений pandera: колонка, проверка, значение
    assert text in found, found[:2000]


def test_catches_logout_without_login(data):
    _, logs = data
    bad = logs.drop(index=logs[logs.action_type == "login"].index[:1])
    _fails(bad, "logout без предшествующего login")


def test_catches_topic_without_auth(data):
    _, logs = data
    bad = logs.copy()
    i = bad[(bad.action_type == "create_topic") & (bad.status == "error")].index[0]
    bad.loc[i, ["status", "error_code"]] = ["success", None]
    _fails(bad, "успешное создание темы без авторизации")


def test_catches_unexpected_null(data):
    _, logs = data
    bad = logs.copy()
    bad.loc[bad.index[0], "action_type"] = None
    _fails(bad, "action_type")


def test_catches_too_few_actions(data):
    _, logs = data
    first_day = logs["event_time"].dt.date.min()
    deletes = logs[(logs.action_type == "delete_topic") & (logs["event_time"].dt.date == first_day)].index
    _fails(logs.drop(index=deletes), "меньше 5 действий")
