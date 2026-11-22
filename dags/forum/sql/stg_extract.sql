-- extract_raw_logs: логи периода из raw в stg. Окно — [start − 1 день, end): день до периода нужен для
-- topics_growth_pct (рост к вчера) и D1 retention (вчерашние регистрации). Фильтр по event_time —
-- планировщик читает только нужные месячные партиции (partition pruning).
-- Идемпотентно: окно сначала очищается, затем заполняется заново — в одной транзакции.
SET TIME ZONE 'UTC';

DELETE FROM stg.forum_logs_daily
 WHERE event_date >= %(start)s::date - 1 AND event_date < %(end)s::date;

INSERT INTO stg.forum_logs_daily (id, user_id, session_id, action_type, object_type, object_id, status, error_code,
                                  event_time, event_date)
SELECT id, user_id, session_id, action_type, object_type, object_id, status, error_code,
       event_time, (event_time AT TIME ZONE 'UTC')::date
  FROM raw.forum_logs
 WHERE event_time >= (%(start)s::date - 1)::timestamptz
   AND event_time <  %(end)s::date::timestamptz;

-- сессии: login и logout склеены по session_id; сессия относится к дню выхода.
-- Логин ищем ещё на день раньше окна — сессия может начаться до полуночи.
DELETE FROM stg.sessions
 WHERE logout_date >= %(start)s::date - 1 AND logout_date < %(end)s::date;

INSERT INTO stg.sessions (session_id, user_id, login_at, logout_at, duration_sec, logout_date)
SELECT session_id, user_id, login_at, logout_at,
       round(extract(epoch FROM logout_at - login_at)::numeric, 2),
       (logout_at AT TIME ZONE 'UTC')::date
  FROM (SELECT session_id,
               min(user_id) AS user_id,
               min(event_time) FILTER (WHERE action_type = 'login'  AND status = 'success') AS login_at,
               max(event_time) FILTER (WHERE action_type = 'logout' AND status = 'success') AS logout_at
          FROM raw.forum_logs
         WHERE session_id IS NOT NULL
           AND event_time >= (%(start)s::date - 2)::timestamptz
           AND event_time <  %(end)s::date::timestamptz
         GROUP BY session_id) s
 WHERE login_at IS NOT NULL AND logout_at IS NOT NULL AND logout_at >= login_at
   AND (logout_at AT TIME ZONE 'UTC')::date >= %(start)s::date - 1
   AND (logout_at AT TIME ZONE 'UTC')::date <  %(end)s::date;
