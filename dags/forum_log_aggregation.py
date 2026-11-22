"""DAG forum_log_aggregation: из логов в БД — дневные метрики форума в CSV.

    check_data_availability → extract_raw_logs → validate_data → transform_aggregates → export_to_csv → alert_on_anomaly

Расписание: ежедневно, обрабатывает предыдущий день — это data interval запуска [вчера 00:00, сегодня 00:00) UTC.
Вручную: Trigger DAG w/ config, параметры start_date и end_date ('YYYY-MM-DD', оба включительно) — любой период.
Повторный запуск за тот же период перезаписывает stg/int/mart за период и CSV-файл (идемпотентно).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.sensors.base import PokeReturnValue

from forum import config as C

log = logging.getLogger(__name__)


@dag(
    dag_id="forum_log_aggregation",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # плановый запуск за день без данных ждёт в сенсоре до таймаута; ручной запуск за другой период не должен стоять
    # за ним в очереди. Параллельные запуски безопасны: каждый заменяет только свой период в транзакциях,
    # а всё считается заново из raw — даже пересекающиеся периоды дают одинаковый результат.
    max_active_runs=3,
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="Начало периода (включительно). Пусто — день запуска по расписанию."),
        "end_date": Param(None, type=["null", "string"], format="date", description="Конец периода (включительно)."),
    },
    tags=["forum", "aggregation"],
    doc_md=__doc__,
)
def forum_log_aggregation():

    @task
    def resolve_period(params=None, data_interval_start=None, data_interval_end=None) -> dict:
        """Период — полуинтервал [start, end): из Params, иначе из data interval (вчерашний день по расписанию)."""
        if params.get("start_date") or params.get("end_date"):
            if not (params.get("start_date") and params.get("end_date")):
                raise AirflowFailException("нужны оба параметра: start_date и end_date")
            start = date.fromisoformat(params["start_date"])
            end = date.fromisoformat(params["end_date"]) + timedelta(days=1)
        else:
            start, end = data_interval_start.date(), data_interval_end.date()
        if start >= end:
            raise AirflowFailException(f"пустой период: {start} … {end}")
        log.info("период обработки: [%s, %s)", start, end)
        return {"start": start.isoformat(), "end": end.isoformat()}

    @task.sensor(poke_interval=C.SENSOR_POKE_SECONDS, timeout=C.SENSOR_TIMEOUT_SECONDS, mode="reschedule")
    def check_data_availability(period: dict) -> PokeReturnValue:
        """Есть ли логи за КАЖДЫЙ день периода. Нет — ждём (reschedule не занимает слот), по таймауту — падаем."""
        from forum.db import hook
        missing = hook().get_first(
            """SELECT count(*) FROM generate_series(%(start)s::date, %(end)s::date - 1, interval '1 day') d
                WHERE NOT EXISTS (SELECT 1 FROM raw.forum_logs
                                   WHERE event_time >= d::timestamptz AND event_time < (d + interval '1 day')::timestamptz)""",
            parameters=period)[0]
        log.info("дней без данных в периоде: %s", missing)
        return PokeReturnValue(is_done=missing == 0)

    @task
    def extract_raw_logs(period: dict) -> dict:
        from forum.db import hook, run_sql_file
        run_sql_file("stg_extract.sql", date.fromisoformat(period["start"]), date.fromisoformat(period["end"]))
        rows, sessions = hook().get_first(
            """SELECT (SELECT count(*) FROM stg.forum_logs_daily WHERE event_date >= %(start)s::date - 1 AND event_date < %(end)s::date),
                      (SELECT count(*) FROM stg.sessions WHERE logout_date >= %(start)s::date - 1 AND logout_date < %(end)s::date)""",
            parameters=period)
        log.info("stg: логов %s, сессий %s (с днём-запасом)", rows, sessions)
        return {**period, "stg_rows": rows, "stg_sessions": sessions}

    @task
    def validate_data(ctx: dict) -> dict:
        """pandera по логам окна: нет неожиданных NULL, допустимые значения, последовательность событий;
        плюс SQL-проверка сессий: длительность неотрицательна."""
        from forum.db import hook
        from forum.validation import stg_schema
        df = hook().get_pandas_df(
            """SELECT id, user_id, session_id::text AS session_id, action_type, object_type, object_id, status, error_code,
                      event_time, event_date
                 FROM stg.forum_logs_daily WHERE event_date >= %(start)s::date - 1 AND event_date < %(end)s::date""",
            parameters=ctx)
        df["user_id"] = df["user_id"].astype("Int64")
        df["object_id"] = df["object_id"].astype("Int64")
        stg_schema.validate(df, lazy=True)
        bad_sessions = hook().get_first("SELECT count(*) FROM stg.sessions WHERE logout_at < login_at OR duration_sec < 0")[0]
        if bad_sessions:
            raise AirflowFailException(f"сессий с выходом раньше входа: {bad_sessions}")
        log.info("stg прошёл проверки: %s строк", len(df))
        return ctx

    @task
    def transform_aggregates(ctx: dict) -> dict:
        from forum.db import hook, run_sql_file
        from forum.validation import mart_schema
        start, end = date.fromisoformat(ctx["start"]), date.fromisoformat(ctx["end"])
        run_sql_file("int_metrics.sql", start, end)
        run_sql_file("mart_daily_stats.sql", start, end)
        mart = hook().get_pandas_df(
            "SELECT day, new_accounts, anon_messages_pct, total_messages, topics_growth_pct, avg_session_duration_sec, "
            "d1_retention_pct FROM mart.forum_daily_stats WHERE day >= %(start)s AND day < %(end)s ORDER BY day",
            parameters=ctx)
        mart_schema.validate(mart, lazy=True)          # метрики в допустимых диапазонах
        if len(mart) != (end - start).days:
            raise AirflowFailException(f"в витрине {len(mart)} дней, ожидалось {(end - start).days}")
        log.info("витрина: %s дней", len(mart))
        return ctx

    @task
    def export_to_csv(ctx: dict) -> dict:
        """COPY из витрины прямо в файл. Имя файла — из периода: повторный запуск перезаписывает тот же файл."""
        from forum.db import hook
        last = (date.fromisoformat(ctx["end"]) - timedelta(days=1)).isoformat()
        path = Path(C.OUTPUT_DIR) / f"forum_daily_stats_{ctx['start']}_{last}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        sql = ("COPY (SELECT day, new_accounts, anon_messages_pct, total_messages, topics_growth_pct, "
               "avg_session_duration_sec, d1_retention_pct FROM mart.forum_daily_stats "
               f"WHERE day >= '{ctx['start']}' AND day < '{ctx['end']}' ORDER BY day) TO STDOUT WITH (FORMAT csv, HEADER)")
        hook().copy_expert(sql, str(path))
        log.info("CSV: %s", path)
        return {**ctx, "csv": str(path)}

    @task
    def alert_on_anomaly(ctx: dict) -> list:
        """Метрика вне нормы (config.ANOMALY_RULES) → предупреждение в лог. Аномалия в данных — не поломка
        пайплайна, поэтому задача не падает."""
        from forum.db import hook
        cols = list(C.ANOMALY_RULES)
        rows = hook().get_records(
            f"SELECT day, {', '.join(cols)} FROM mart.forum_daily_stats WHERE day >= %(start)s AND day < %(end)s ORDER BY day",
            parameters=ctx)
        found = []
        for row in rows:
            day, values = row[0], dict(zip(cols, row[1:]))
            for metric, (lo, hi) in C.ANOMALY_RULES.items():
                v = values[metric]
                if v is None:
                    continue
                if (lo is not None and v < lo) or (hi is not None and v > hi):
                    found.append(f"{day} {metric}={v} (норма {lo} … {hi})")
        for f in found:
            log.warning("АНОМАЛИЯ: %s", f)
        log.info("аномалий: %s", len(found))
        return found

    period = resolve_period()
    ctx = extract_raw_logs(period)
    check_data_availability(period) >> ctx
    alert_on_anomaly(export_to_csv(transform_aggregates(validate_data(ctx))))


forum_log_aggregation()
