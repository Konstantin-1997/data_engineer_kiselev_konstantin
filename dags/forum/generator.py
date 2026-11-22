"""Генерация пользователей и логов форума за один календарный месяц.

Главная идея: события строятся СЕССИЯМИ (login → действия → logout) и обрабатываются в порядке времени,
поэтому данные корректны по построению: логаута без логина и темы без авторизации не бывает, удаляют только свою
уже созданную тему, смотрят только существующую. Проверки качества (validation.py) это подтверждают, а не чинят.

Детерминированность: одинаковые месяц и seed → одинаковые данные (свой random.Random, глобальный random не трогаем).
"""
from __future__ import annotations

import random
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone

from forum import config as C

UTC = timezone.utc


def month_bounds(month: str) -> tuple[date, date]:
    """'2026-10' → (2026-10-01, 2026-11-01): полуинтервал [start, end)."""
    y, m = (int(x) for x in month.split("-"))
    start = date(y, m, 1)
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return start, end


def _uid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generate_users(month: str, seed: int) -> list[dict]:
    """Пользователи: BASE_USERS «старых» (зарегистрированы до месяца) + новые на каждый день месяца.
    Число регистраций в день — рандомайзер с нижним порогом MIN_PER_DAY."""
    start, end = month_bounds(month)
    rng = random.Random(f"users|{month}|{seed}")
    month_start = datetime.combine(start, time(), UTC)
    users = []
    for i in range(1, C.BASE_USERS + 1):
        users.append(dict(id=i, registered_at=month_start - timedelta(days=rng.randint(30, 400), seconds=rng.randint(0, 86399))))
    offset = (start.year * 100 + start.month) * 10_000          # id новых пользователей не пересекаются между месяцами
    k, d = 0, start
    while d < end:
        for _ in range(rng.randint(C.MIN_PER_DAY, C.MAX_REGISTRATIONS_PER_DAY)):
            k += 1
            reg = datetime.combine(d, time(), UTC) + timedelta(seconds=rng.randint(6 * 3600, 20 * 3600))
            users.append(dict(id=offset + k, registered_at=reg))
        d += timedelta(days=1)
    for u in users:
        u["username"] = f"user{u['id']}"
        u["email"] = f"user{u['id']}@example.com"
    return users


def generate_logs(users: list[dict], month: str, seed: int) -> list[dict]:
    start, end = month_bounds(month)
    rng = random.Random(f"logs|{month}|{seed}")
    month_start = datetime.combine(start, time(), UTC)
    events: list[dict] = []
    id_base = (start.year * 100 + start.month) * 1_000_000
    seq = {"topic": id_base, "message": id_base}

    def next_id(kind: str) -> int:
        seq[kind] += 1
        return seq[kind]

    def add(t, action, user=None, session=None, otype=None, oid=None, status="success", err=None, meta=None):
        events.append(dict(user_id=user, session_id=session, action_type=action, object_type=otype, object_id=oid,
                           status=status, error_code=err, event_time=t, metadata=meta or {}))

    # темы, созданные до месяца: их можно смотреть и удалять с первого дня (в логах месяца их создания нет)
    old_users = [u for u in users if u["registered_at"] < month_start]
    alive: dict[int, tuple[int, datetime]] = {}                 # topic_id → (автор, когда создана)
    for _ in range(C.PRE_EXISTING_TOPICS):
        alive[id_base - rng.randint(1, 999_999)] = (rng.choice(old_users)["id"], month_start - timedelta(days=1))

    new_by_day = defaultdict(list)
    for u in users:
        if u["registered_at"] >= month_start:
            new_by_day[u["registered_at"].date()].append(u)
    known = list(old_users)                                     # могут войти: зарегистрированы раньше
    yesterday_new: list[dict] = []

    d = start
    while d < end:
        day0 = datetime.combine(d, time(), UTC)

        def at(lo_h: float, hi_h: float) -> datetime:
            return day0 + timedelta(seconds=rng.randint(int(lo_h * 3600), int(hi_h * 3600) - 1))

        target = {a: rng.randint(C.MIN_PER_DAY, C.MAX_PER_DAY) for a in C.ACTIONS}
        new_today = new_by_day.get(d, [])

        # 1. новички: первый заход анонимом, через несколько минут — регистрация
        for u in new_today:
            visitor = _uid(rng)
            add(u["registered_at"] - timedelta(seconds=rng.randint(60, 1800)), "first_visit", meta={"visitor_id": visitor})
            add(u["registered_at"], "register", user=u["id"], meta={"visitor_id": visitor})
        for _ in range(max(0, target["first_visit"] - len(new_today)) + rng.randint(0, 5)):
            add(at(0, 24), "first_visit", meta={"visitor_id": _uid(rng)})          # зашли и ушли, не регистрируясь

        # 2. сессии: сначала часть вчерашних новичков (retention D1), потом остальные
        n_sessions = max(target["login"], target["logout"]) + rng.randint(0, 4)
        returners = rng.sample(yesterday_new, k=int(len(yesterday_new) * C.D1_RETURN_SHARE))
        session_users = returners + [rng.choice(known + new_today) for _ in range(max(0, n_sessions - len(returners)))]
        sessions = []
        for i, u in enumerate(session_users):
            if u["registered_at"] >= day0:                      # новичок: входит после регистрации
                login = u["registered_at"] + timedelta(seconds=rng.randint(60, 3600))
            else:
                login = at(0, 21.9)
            end_at = login + timedelta(seconds=rng.randint(5 * 60, 120 * 60))
            has_logout = i < target["logout"] or rng.random() < C.LOGOUT_SHARE
            sessions.append(dict(user=u["id"], sid=_uid(rng), login=login, end=end_at, logout=has_logout))

        # 3. действия внутри сессий: сколько каждого — рандомайзер, в какую сессию и когда — случайно
        logged_views = rng.randint(1, target["view_topic"])
        plan = []                                               # (время, действие, сессия)
        for action, count in (("create_topic", target["create_topic"]), ("delete_topic", target["delete_topic"]),
                              ("write_message", target["write_message"]), ("view_topic", logged_views)):
            for _ in range(count):
                s = rng.choice(sessions)
                span = (s["end"] - s["login"]).total_seconds()
                plan.append((s["login"] + timedelta(seconds=rng.uniform(1, span - 1)), action, s))
        # анонимы: столько же сообщений примерно, сколько у залогиненных (±20%), просмотры, попытки создать тему
        anon = [(at(0, 24), "write_message") for _ in range(max(C.MIN_PER_DAY, round(target["write_message"] * rng.uniform(0.8, 1.2))))]
        anon += [(at(0, 24), "view_topic") for _ in range(target["view_topic"] - logged_views + rng.randint(0, 3))]
        anon += [(at(0, 24), "create_topic") for _ in range(rng.randint(C.MIN_TOPIC_AUTH_ERRORS_PER_DAY, C.MAX_TOPIC_AUTH_ERRORS_PER_DAY))]

        for s in sessions:
            add(s["login"], "login", user=s["user"], session=s["sid"])
            if s["logout"]:
                add(s["end"], "logout", user=s["user"], session=s["sid"])

        # обработка строго по времени: темы появляются и исчезают в правильном порядке
        timeline = sorted([(t, a, s) for t, a, s in plan] + [(t, a, None) for t, a in anon], key=lambda x: x[0])
        for t, action, s in timeline:
            user, sid = (s["user"], s["sid"]) if s else (None, None)
            if action == "create_topic" and s:
                tid = next_id("topic")
                alive[tid] = (user, t)
                add(t, action, user, sid, "topic", tid, status="success", err=None)
            elif action == "create_topic":                      # аноним: сервер отвечает ошибкой авторизации
                add(t, action, None, None, "topic", None, status="error", err="AUTH_REQUIRED", meta={"visitor_id": _uid(rng)})
            elif action == "delete_topic":
                own = [tid for tid, (a, ct) in alive.items() if a == user and ct < t]
                if not own:                                     # своей темы ещё нет — создаёт (внутри той же сессии) и удаляет
                    tid = next_id("topic")
                    add(s["login"] + (t - s["login"]) / 2, "create_topic", user, sid, "topic", tid)
                    own = [tid]
                tid = rng.choice(own)
                alive.pop(tid, None)
                add(t, action, user, sid, "topic", tid)
            elif action == "view_topic":
                existing = [tid for tid, (_, ct) in alive.items() if ct < t]
                add(t, action, user, sid, "topic", rng.choice(existing), meta=None if s else {"visitor_id": _uid(rng)})
            elif action == "write_message":
                add(t, action, user, sid, "message", next_id("message"), meta=None if s else {"visitor_id": _uid(rng)})

        known.extend(new_today)
        yesterday_new = new_today
        d += timedelta(days=1)

    events.sort(key=lambda e: e["event_time"])
    return events
