"""Работа с базой форума: загрузка месяца (партиция + идемпотентная замена), запуск SQL-файлов трансформаций."""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from forum import config as C
from forum.generator import month_bounds

log = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"


def hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=C.CONN_ID)


def partition_name(month: str) -> str:
    return "forum_logs_" + month.replace("-", "_")


def load_month(users: pd.DataFrame, logs: pd.DataFrame, month: str) -> dict:
    """Одна транзакция: партиция месяца → пользователи (upsert) → удалить логи месяца → COPY логов.
    Повторный запуск за тот же месяц даёт тот же результат, без дублей."""
    start, end = month_bounds(month)
    part = partition_name(month)
    conn = hook().get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            # строки этого месяца в default-партиции не дадут создать партицию — переносить их не задача загрузки
            cur.execute("SELECT count(*) FROM raw.forum_logs_default WHERE event_time >= %s AND event_time < %s", (start, end))
            if cur.fetchone()[0]:
                raise RuntimeError(f"в raw.forum_logs_default есть строки за {month}: разберитесь до загрузки")
            cur.execute(f"CREATE TABLE IF NOT EXISTS raw.{part} PARTITION OF raw.forum_logs "
                        "FOR VALUES FROM (%s) TO (%s)", (f"{start} 00:00:00+00", f"{end} 00:00:00+00"))
            cur.executemany(
                """INSERT INTO raw.users (id, username, email, registered_at) VALUES (%s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, email = EXCLUDED.email,
                                                  registered_at = EXCLUDED.registered_at""",
                list(users[["id", "username", "email", "registered_at"]].itertuples(index=False, name=None)))
            cur.execute("DELETE FROM raw.forum_logs WHERE event_time >= %s AND event_time < %s", (start, end))
            deleted = cur.rowcount
            buf = io.StringIO()
            w = csv.writer(buf)
            for r in logs.itertuples(index=False):
                w.writerow([
                    "" if pd.isna(r.user_id) else int(r.user_id), r.session_id or "", r.action_type, r.object_type or "",
                    "" if pd.isna(r.object_id) else int(r.object_id), r.status, r.error_code or "",
                    r.event_time.isoformat(), json.dumps(r.metadata, ensure_ascii=False),
                ])
            buf.seek(0)
            cur.copy_expert(
                "COPY raw.forum_logs (user_id, session_id, action_type, object_type, object_id, status, error_code, "
                "event_time, metadata) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
            cur.execute(f"SELECT count(*) FROM raw.{part}")
            loaded = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    hook().run("ANALYZE raw.forum_logs; ANALYZE raw.users", autocommit=True)
    result = {"partition": f"raw.{part}", "users": len(users), "logs_loaded": loaded, "logs_replaced": deleted}
    log.info("загружено: %s", result)
    return result


def run_sql_file(name: str, start: date, end: date) -> None:
    """Выполнить файл из sql/ одной транзакцией. В файле — параметры %(start)s и %(end)s: полуинтервал дат [start, end)."""
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    hook().run(sql, parameters={"start": start, "end": end}, autocommit=False)
    log.info("выполнено %s за [%s, %s)", name, start, end)
