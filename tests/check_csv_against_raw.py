"""Независимая сверка CSV с сырыми логами: те же шесть метрик, посчитанные в pandas из raw.forum_logs.
Запуск: docker compose exec airflow-scheduler python /opt/airflow/tests/check_csv_against_raw.py <файл.csv>"""
import sys
from datetime import timedelta

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

csv_path = sys.argv[1]
got = pd.read_csv(csv_path, parse_dates=["day"])
start, end = got["day"].min().date(), got["day"].max().date() + timedelta(days=1)
logs = PostgresHook("forum_db").get_pandas_df(
    "SELECT user_id, session_id::text AS session_id, action_type, status, event_time FROM raw.forum_logs "
    "WHERE event_time >= %(s)s AND event_time < %(e)s", parameters={"s": start - timedelta(days=2), "e": end})
logs["day"] = logs["event_time"].dt.tz_convert("UTC").dt.date
ok = logs[logs["status"] == "success"]
days = pd.Index([start - timedelta(days=1) + timedelta(days=i) for i in range((end - start).days + 1)])


def per_day(mask):
    return ok[mask].groupby("day").size().reindex(days, fill_value=0)


msgs = per_day(ok.action_type == "write_message")
anon = per_day((ok.action_type == "write_message") & ok.user_id.isna())
topics = per_day(ok.action_type == "create_topic")
exp = pd.DataFrame(index=days)
exp["new_accounts"] = per_day(ok.action_type == "register")
exp["anon_messages_pct"] = (100 * anon / msgs.where(msgs > 0)).round(2)
exp["total_messages"] = msgs
exp["topics_growth_pct"] = (100 * (topics - topics.shift()) / topics.shift().where(topics.shift() > 0)).round(2)
ses = ok[ok.session_id.notna()]
s = pd.DataFrame({"login": ses[ses.action_type == "login"].groupby("session_id")["event_time"].min(),
                  "logout": ses[ses.action_type == "logout"].groupby("session_id")["event_time"].max()}).dropna()
s["day"] = s["logout"].dt.date
exp["avg_session_duration_sec"] = (s["logout"] - s["login"]).dt.total_seconds().groupby(s["day"]).mean().round(2)
reg = ok[ok.action_type == "register"][["user_id", "day"]]
active = ok.dropna(subset=["user_id"])[["user_id", "day"]].drop_duplicates().assign(back=1)
reg = reg.assign(day=reg["day"] + timedelta(days=1)).merge(active, on=["user_id", "day"], how="left")
r = reg.groupby("day").agg(n=("user_id", "size"), back=("back", "sum"))
exp["d1_retention_pct"] = (100 * r["back"] / r["n"]).round(2)
exp = exp.loc[[d for d in days if start <= d < end]].reset_index(names="day")

got["day"] = got["day"].dt.date
bad = 0
for col in exp.columns[1:]:
    a, b = got[col].astype(float).to_numpy(), exp[col].astype(float).to_numpy()
    diff = ~(np.isclose(a, b, atol=0.011) | (np.isnan(a) & np.isnan(b)))
    print(f"{col:26} {'OK' if not diff.any() else f'РАСХОЖДЕНИЙ {diff.sum()}'}")
    bad += diff.sum()
print("CSV совпадает с независимым расчётом из raw" if not bad else "ЕСТЬ РАСХОЖДЕНИЯ")
sys.exit(1 if bad else 0)
