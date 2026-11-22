"""DAG forum_data_generation: генерирует пользователей и логи форума за один календарный месяц и грузит в raw.

    generate_users → generate_logs → validate_generated_data → load_to_postgres

Запуск вручную (schedule=None) с параметрами:
    month — 'YYYY-MM'; пусто → прошлый полный месяц относительно даты запуска
    seed  — зерно рандомайзера: одинаковые month и seed → одинаковые данные
Между задачами через XCom идут только пути к parquet-файлам, сами данные — в файлах ./data/<run_id>/.
Повторный запуск за тот же месяц заменяет его данные (идемпотентно).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from forum import config as C

log = logging.getLogger(__name__)


@dag(
    dag_id="forum_data_generation",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "month": Param("", type="string", pattern=r"^(\d{4}-(0[1-9]|1[0-2]))?$",
                       description="Месяц 'YYYY-MM'. Пусто — прошлый полный месяц."),
        "seed": Param(42, type="integer", description="Зерно рандомайзера"),
    },
    tags=["forum", "generation"],
    doc_md=__doc__,
)
def forum_data_generation():

    @task
    def generate_users(params=None, run_id=None, logical_date=None) -> dict:
        from forum.generator import generate_users as gen
        month = params["month"] or logical_date.start_of("month").subtract(months=1).format("YYYY-MM")
        out = Path(C.DATA_DIR) / run_id.replace(":", "_").replace("+", "_")
        out.mkdir(parents=True, exist_ok=True)
        users = pd.DataFrame(gen(month, params["seed"]))
        path = out / "users.parquet"
        users.to_parquet(path, index=False)
        log.info("месяц %s: пользователей %s (до месяца %s, новых %s)", month, len(users), C.BASE_USERS, len(users) - C.BASE_USERS)
        return {"month": month, "dir": str(out), "users": str(path)}

    @task
    def generate_logs(ctx: dict, params=None) -> dict:
        from forum.generator import generate_logs as gen
        users = pd.read_parquet(ctx["users"]).to_dict("records")
        logs = pd.DataFrame(gen(users, ctx["month"], params["seed"]))
        logs["metadata"] = logs["metadata"].map(lambda m: m or {})
        path = Path(ctx["dir"]) / "logs.parquet"
        logs.astype({"user_id": "Int64", "object_id": "Int64"}).to_parquet(path, index=False)
        counts = logs.groupby("action_type").size().to_dict()
        log.info("событий: %s, по типам: %s", len(logs), counts)
        return {**ctx, "logs": str(path)}

    @task
    def validate_generated_data(ctx: dict) -> dict:
        from forum.validation import logs_schema, users_schema
        users = pd.read_parquet(ctx["users"])
        logs = pd.read_parquet(ctx["logs"])
        users_schema.validate(users, lazy=True)
        logs_schema.validate(logs, lazy=True)          # нарушение → SchemaErrors, задача падает, загрузки не будет
        anon = logs[(logs.action_type == "write_message") & (logs.status == "success")]["user_id"].isna().mean()
        log.info("проверки пройдены: %s событий, анонимных сообщений %.1f%%", len(logs), 100 * anon)
        return ctx

    @task
    def load_to_postgres(ctx: dict) -> dict:
        from forum.db import load_month
        users = pd.read_parquet(ctx["users"])
        logs = pd.read_parquet(ctx["logs"])
        return load_month(users, logs, ctx["month"])

    load_to_postgres(validate_generated_data(generate_logs(generate_users())))


forum_data_generation()
