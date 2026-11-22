# data_engineer_kiselev_konstantin

Тестовое задание Data Engineer: логи форума в PostgreSQL, генерация и агрегация данных в Apache Airflow 2.x,
проверки качества на pandera.

## Запуск

```bash
docker-compose up
```

Поднимаются PostgreSQL 17 и Airflow 2.11.2 (webserver, scheduler; одноразовый `airflow-init` мигрирует метабазу
и создаёт пользователя). Первый запуск собирает образ Airflow с pandera — несколько минут.

| Что | Где |
|---|---|
| Airflow UI | http://localhost:8080, логин `admin`, пароль `admin` |
| PostgreSQL | `localhost:5433`, база `forum`, пользователь и пароль `forum` |
| Итоговые CSV | `./output/forum_daily_stats_<начало>_<конец>.csv` |

На Linux, если Airflow не может писать в `./data` и `./output`: `echo "AIRFLOW_UID=$(id -u)" > .env` и снова `docker-compose up`.

### Шаги после запуска

1. **Сгенерировать данные.** В UI: DAG `forum_data_generation` → ▶ *Trigger DAG w/ config* →
   `{"month": "2026-10", "seed": 42}` (пустой `month` — прошлый полный месяц). DAG стоит на паузе: включается
   тумблером, ручной запуск выполнится после включения.
2. **Посчитать метрики за период.** DAG `forum_log_aggregation` → включить → *Trigger DAG w/ config* →
   `{"start_date": "2026-10-01", "end_date": "2026-10-31"}` (оба конца включительно).
3. **Результат** — `./output/forum_daily_stats_2026-10-01_2026-10-31.csv`, 31 строка.

То же из консоли:

```bash
docker-compose exec airflow-scheduler bash
airflow dags unpause forum_data_generation
airflow dags trigger forum_data_generation --conf '{"month": "2026-10", "seed": 42}'
airflow dags unpause forum_log_aggregation
airflow dags trigger forum_log_aggregation --conf '{"start_date": "2026-10-01", "end_date": "2026-10-31"}'
```

Когда `forum_log_aggregation` включён, по расписанию (`@daily`) он обрабатывает **вчерашний** день — это data
interval запуска. Если за вчера данных нет (сгенерирован другой месяц), сенсор ждёт до 30 минут и задача падает
по таймауту — это правильное поведение: метрики не считаются из пустоты. Ручной запуск за другой период при этом
не ждёт в очереди (`max_active_runs=3`).

### Тесты и проверки

```bash
docker-compose exec airflow-scheduler python -m pytest -q /opt/airflow/tests
docker-compose exec airflow-scheduler python /opt/airflow/tests/check_csv_against_raw.py \
    /opt/airflow/output/forum_daily_stats_2026-10-01_2026-10-31.csv
```

- `tests/test_generator.py` — генератор выполняет требования ТЗ, детерминирован, а проверки pandera ловят
  нарушения: логаут без логина, тема без авторизации, неожиданный NULL, нехватку действий в дне.
- `tests/check_csv_against_raw.py` — независимая сверка: те же шесть метрик в pandas прямо из `raw.forum_logs`
  против CSV.

## Структура

```
docker-compose.yml            PostgreSQL + Airflow одной командой
Dockerfile, requirements.txt  образ Airflow 2.11.2 + pandera
sql/init/
  00_airflow_db.sql           отдельная база под метаданные Airflow
  01_schema.sql               схемы raw / stg / int / mart, партиционирование, индексы, ограничения
dags/
  forum_data_generation.py    DAG генерации (только порядок задач)
  forum_log_aggregation.py    DAG агрегации (только порядок задач)
  forum/
    config.py                 пороги, правила аномалий, пути — всё настраиваемое в одном месте
    generator.py              генерация пользователей и логов сессиями
    validation.py             схемы pandera: сгенерированные данные, stg, витрина
    db.py                     загрузка месяца (партиция + COPY), запуск SQL-файлов
    sql/                      трансформации: stg_extract, int_metrics, mart_daily_stats
tests/                        тесты генератора и сверка CSV с raw
data/   (не в git)            промежуточные parquet генератора
output/ (не в git)            итоговые CSV
```

## Схема БД

Четыре схемы — четыре слоя. Каждая таблица пишется одной задачей и читается только следующими слоями.

```
raw.users ◄── user_id ── raw.forum_logs (партиции по месяцам)
                               │ extract_raw_logs: период + 1 день назад
                               ▼
                         stg.forum_logs_daily ── GROUP BY session_id ──► stg.sessions
                               │ transform_aggregates                         │
                               ▼                                              ▼
                         int.day_metrics ◄───────────── закрытые сессии по дню выхода
                         int.retention_cohorts (регистрация D, вернулся ли в D + 1)
                               │ LAG по дням, проценты
                               ▼
                         mart.forum_daily_stats ── COPY ──► CSV
```

### raw.forum_logs — лог действий (требование 1.3)

| Колонка | Смысл |
|---|---|
| `id` | identity; первичный ключ — `(id, event_time)`: уникальный ключ партиционированной таблицы обязан содержать ключ партиции |
| `user_id` | кто совершил действие, FK → `raw.users`; `NULL` — аноним |
| `session_id` | сессия login … logout (у анонимов нет) |
| `action_type` | `first_visit`, `register`, `login`, `logout`, `create_topic`, `view_topic`, `delete_topic`, `write_message` |
| `object_type`, `object_id` | объект действия: тема или сообщение и его id |
| `status`, `error_code` | ответ сервера: `success` / `error` + код (`AUTH_REQUIRED`) |
| `event_time` | дата и время действия, UTC — ключ партиционирования |
| `metadata` | jsonb: `visitor_id` анонима и т. п. |

Правила ТЗ закреплены прямо в базе ограничениями `CHECK`: успешное создание темы без `user_id` вставить нельзя
(1.1), у ошибки всегда есть код, сессия бывает только у залогиненного. Сообщение пишет кто угодно (1.2). Попытка
анонима создать тему логируется как `status = 'error'`, `error_code = 'AUTH_REQUIRED'`.

## Почему партиционирование RANGE по `event_time`, по месяцу (требование 1.4)

**Стратегия:** `PARTITION BY RANGE (event_time)`, одна партиция на календарный месяц (`raw.forum_logs_YYYY_MM`,
UTC) + `raw.forum_logs_default` как страховка. Партицию месяца создаёт `load_to_postgres` перед загрузкой.

**Почему по времени:**
- логи только дописываются и растут во времени; почти все запросы — за период (день метрик, месяц загрузки);
- **partition pruning**: запрос за день читает одну партицию — проверено `EXPLAIN`:

  ```
  Aggregate
    ->  Index Only Scan using forum_logs_2026_10_action_type_event_time_idx on forum_logs_2026_10
          Index Cond: ((action_type = 'write_message') AND (event_time >= '2026-10-15') AND (event_time < '2026-10-16'))
  ```
- **retention**: старые логи удаляются `DROP`/`DETACH PARTITION` — мгновенно и без раздувания таблицы, вместо
  тяжёлого `DELETE` миллионов строк; старые партиции можно вынести на дешёвый диск;
- `VACUUM` / `ANALYZE` и индексы работают на уровне небольших партиций.

**Почему по месяцу, а не по дню:** при объёмах форума дневные партиции дали бы сотни маленьких таблиц и лишнюю
работу планировщику. Перейти на дневные стоит при десятках миллионов событий в сутки.

**Альтернативы, и почему нет:**
- `HASH (user_id)` — нет отсечения по времени, старые данные одной командой не удалить;
- `LIST (action_type)` — неравномерные партиции, а метрики читают все типы сразу и фильтруют по дате.

`raw.forum_logs_default` в норме пустая: строки месяца в ней не дали бы создать партицию этого месяца,
поэтому загрузка это проверяет до создания партиции.

## Почему такие индексы (требование 1.5)

Индексы созданы на родительской таблице — PostgreSQL создаёт их в каждой партиции, включая будущие.
Каждый индекс существует ради конкретного запроса: лишний индекс только замедляет вставку в таблицу логов.

| Индекс | Под какой запрос |
|---|---|
| `(user_id, event_time)` | история пользователя; D1 retention — `EXISTS` события пользователя в следующем дне |
| `(action_type, event_time)` | дневные агрегаты: сообщения, темы, регистрации за день (см. `EXPLAIN` выше — Index Only Scan) |
| `(session_id, event_time) WHERE session_id IS NOT NULL` | склейка login и logout в сессию; частичный: у анонимов сессий нет |
| `(object_type, object_id) WHERE object_id IS NOT NULL` | всё о конкретной теме или сообщении (просмотры, удаление) |
| `(event_time) WHERE status = 'error'` | ошибки за период (создание темы без логина, алерты); частичный: ошибок мало |

В `stg` и `int` таблицы маленькие (период), там первичные ключи и индекс `(event_date, action_type)` под группировку.
На больших объёмах для `event_time` стоит рассмотреть BRIN: он весит килобайты вместо мегабайт, потому что логи
физически лежат в порядке времени.

## DAG `forum_data_generation`

`generate_users → generate_logs → validate_generated_data → load_to_postgres`

- **generate_users** — 150 пользователей, зарегистрированных до месяца, плюс новые каждый день (рандомайзер,
  не меньше 5 в день). id новых не пересекаются между месяцами.
- **generate_logs** — события строятся **сессиями** (login → действия → logout) и обрабатываются строго по
  времени, поэтому данные корректны по построению: удаляют только свою созданную тему, смотрят существующую.
  Количество каждого действия в день — рандомайзер от 5 до 15. Анонимы: первые заходы, просмотры, сообщения
  (примерно столько же, сколько у залогиненных) и 2–4 попытки создать тему в день → ошибка `AUTH_REQUIRED`.
  Половина вчерашних новичков заходит сегодня — иначе D1 retention был бы нулём.
- **validate_generated_data** — pandera: типы и отсутствие неожиданных NULL; последовательность (нет логаута без
  логина, нет создания темы без авторизации, действия в сессии — после логина); пороги ТЗ (≥ 5 действий каждого
  типа в день, ≥ 2 ошибки создания темы в день, 40–60 % анонимных сообщений, ровно один месяц).
  Нарушение → задача падает, загрузки нет.
- **load_to_postgres** — одной транзакцией: партиция месяца → пользователи (upsert) → удалить логи месяца →
  `COPY` логов. Повторный запуск за тот же месяц заменяет его — без дублей.

Одинаковые `month` и `seed` дают одинаковые данные. Через XCom передаются только пути к parquet-файлам.

## DAG `forum_log_aggregation`

`resolve_period → check_data_availability → extract_raw_logs → validate_data → transform_aggregates → export_to_csv → alert_on_anomaly`

- **Период** — полуинтервал `[start, end)`: из Params (`start_date`, `end_date` включительно) или из data interval
  планового запуска (вчерашний день). `datetime.now()` нигде не используется — перезапуск за 15.10 считает ровно 15.10.
- **check_data_availability** — сенсор (`mode="reschedule"`, не занимает слот): данные есть за каждый день периода.
- **extract_raw_logs** — SQL внутри базы: логи окна `[start − 1 день, end)` в `stg.forum_logs_daily`, сессии в
  `stg.sessions`. День-запас нужен для роста тем к вчера и D1 retention.
- **validate_data** — pandera по `stg`: NULL, допустимые значения, последовательность событий; сессии с
  неотрицательной длительностью.
- **transform_aggregates** — `int.day_metrics`, `int.retention_cohorts`, `mart.forum_daily_stats`; результат
  проверяется pandera — метрики в допустимых диапазонах (проценты 0–100, рост ≥ −100, счётчики ≥ 0).
- **export_to_csv** — `COPY (SELECT … FROM mart…) TO STDOUT` прямо в файл; имя из периода — перезапуск
  перезаписывает тот же файл.
- **alert_on_anomaly** — метрики вне норм из `config.ANOMALY_RULES` (в т. ч. `anon_messages_pct > 80`) →
  `WARNING` в лог. Аномалия в данных — не поломка пайплайна, задача не падает.

Каждая задача идемпотентна: заменяет свой период (`DELETE` + `INSERT` в одной транзакции).

### Метрики

| Поле | Определение |
|---|---|
| `day` | календарный день, UTC |
| `new_accounts` | успешные `register` за день |
| `anon_messages_pct` | `write_message` без `user_id` / все успешные `write_message` × 100; нет сообщений → пусто |
| `total_messages` | успешные `write_message` |
| `topics_growth_pct` | (создано тем сегодня − вчера) / вчера × 100; «количество тем» = созданные за день; вчера 0 → пусто |
| `avg_session_duration_sec` | среднее (logout − login) по сессиям, закрытым в этот день; сессия относится к дню выхода |
| `d1_retention_pct` | из зарегистрировавшихся вчера — доля тех, у кого сегодня есть хотя бы одно событие; метрика стоит в строке дня возврата, иначе её нельзя посчитать при ежедневном запуске за вчера |

Пустое значение в CSV — «посчитать нельзя», а не ноль: «рост 0 %» и «роста не с чем сравнить» — разные утверждения.

## Допущения

- Время везде UTC: `timestamptz`, границы партиций и дней.
- «Минимум 2 ошибки создания темы» трактуется строже — **в каждый день**.
- «Количество тем» в росте — созданные за день (поток), а не живущие на форуме (запас).
- Сессия без logout («закрыл вкладку») в длительность не входит; сессия через полночь — в день выхода.
- Генерация — один месяц за запуск (`schedule=None`); месяцы можно догенерировать отдельными запусками.

## Что сделал бы в продакшене иначе

- Партиции на месяцы вперёд — отдельной задачей или `pg_partman`; retention через `DETACH PARTITION`.
- Загрузка вместо одного `COPY` на месяц — инкрементально, по мере поступления событий, с водяным знаком.
- CSV — в объектное хранилище (S3), алерты — в мессенджер или почту, а не только в лог.
- Отдельный пользователь БД для Airflow с правами только на свои схемы; секреты — не в compose-файле.
