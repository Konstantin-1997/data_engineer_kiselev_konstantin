-- transform_aggregates, шаг 2: витрина за [start, end). Оконная функция LAG — только здесь.
-- Деление на ноль → NULL: «посчитать нельзя» и «ноль процентов» — разные вещи.
SET TIME ZONE 'UTC';

DELETE FROM mart.forum_daily_stats WHERE day >= %(start)s::date AND day < %(end)s::date;

INSERT INTO mart.forum_daily_stats (day, new_accounts, anon_messages_pct, total_messages, topics_growth_pct,
                                    avg_session_duration_sec, d1_retention_pct)
WITH w AS (
    SELECT m.*,
           LAG(day)          OVER (ORDER BY day) AS prev_day,
           LAG(total_topics) OVER (ORDER BY day) AS prev_topics
      FROM int.day_metrics m
     WHERE day >= %(start)s::date - 1 AND day < %(end)s::date
), cohorts AS (
    SELECT reg_day + 1 AS day, count(*) AS cohort, count(*) FILTER (WHERE returned_d1) AS returned
      FROM int.retention_cohorts
     WHERE reg_day >= %(start)s::date - 1 AND reg_day < %(end)s::date - 1
     GROUP BY reg_day
)
SELECT w.day,
       w.new_accounts,
       round(100.0 * w.anon_messages / nullif(w.total_messages, 0), 2),
       w.total_messages,
       CASE WHEN w.prev_day = w.day - 1                                  -- LAG только по соседнему дню
            THEN round(100.0 * (w.total_topics - w.prev_topics) / nullif(w.prev_topics, 0), 2) END,
       round(w.sum_session_sec / nullif(w.closed_sessions, 0), 2),
       round(100.0 * c.returned / nullif(c.cohort, 0), 2)
  FROM w
  LEFT JOIN cohorts c ON c.day = w.day
 WHERE w.day >= %(start)s::date;
