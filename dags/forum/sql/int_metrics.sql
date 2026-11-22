-- transform_aggregates, шаг 1: дневные счётчики и когорты удержания. Без оконных функций — только count/sum.
-- Окно дней — [start − 1, end): вчерашний день нужен витрине для роста тем и D1 retention.
SET TIME ZONE 'UTC';

DELETE FROM int.day_metrics WHERE day >= %(start)s::date - 1 AND day < %(end)s::date;

INSERT INTO int.day_metrics (day, new_accounts, total_messages, anon_messages, total_topics, closed_sessions, sum_session_sec)
SELECT d.day,
       coalesce(l.new_accounts, 0), coalesce(l.total_messages, 0), coalesce(l.anon_messages, 0), coalesce(l.total_topics, 0),
       coalesce(s.closed_sessions, 0), coalesce(s.sum_session_sec, 0)
  FROM generate_series(%(start)s::date - 1, %(end)s::date - 1, interval '1 day') AS d(day)
  LEFT JOIN (
        SELECT event_date AS day,
               count(*) FILTER (WHERE action_type = 'register'      AND status = 'success')                     AS new_accounts,
               count(*) FILTER (WHERE action_type = 'write_message' AND status = 'success')                     AS total_messages,
               count(*) FILTER (WHERE action_type = 'write_message' AND status = 'success' AND user_id IS NULL) AS anon_messages,
               count(*) FILTER (WHERE action_type = 'create_topic'  AND status = 'success')                     AS total_topics
          FROM stg.forum_logs_daily
         GROUP BY event_date) l ON l.day = d.day
  LEFT JOIN (
        SELECT logout_date AS day, count(*) AS closed_sessions, sum(duration_sec) AS sum_session_sec
          FROM stg.sessions
         GROUP BY logout_date) s ON s.day = d.day;

-- когорта дня D = зарегистрировавшиеся в D; вернулся — есть любое его событие в D + 1.
-- Для витрины за [start, end) нужны когорты [start − 1, end − 1).
DELETE FROM int.retention_cohorts WHERE reg_day >= %(start)s::date - 1 AND reg_day < %(end)s::date - 1;

INSERT INTO int.retention_cohorts (reg_day, user_id, returned_d1)
SELECT r.event_date, r.user_id,
       EXISTS (SELECT 1 FROM raw.forum_logs x                       -- индекс (user_id, event_time), одна партиция
                WHERE x.user_id = r.user_id
                  AND x.event_time >= (r.event_date + 1)::timestamptz
                  AND x.event_time <  (r.event_date + 2)::timestamptz)
  FROM stg.forum_logs_daily r
 WHERE r.action_type = 'register' AND r.status = 'success' AND r.user_id IS NOT NULL
   AND r.event_date >= %(start)s::date - 1 AND r.event_date < %(end)s::date - 1
ON CONFLICT (reg_day, user_id) DO UPDATE SET returned_d1 = EXCLUDED.returned_d1;
