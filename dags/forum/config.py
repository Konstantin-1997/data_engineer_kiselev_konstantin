"""Константы пайплайна: всё, что может захотеться поменять, — здесь, а не в коде задач."""
from __future__ import annotations

CONN_ID = "forum_db"                      # Airflow Connection (задаётся переменной AIRFLOW_CONN_FORUM_DB в docker-compose)
DATA_DIR = "/opt/airflow/data"            # промежуточные файлы генератора (parquet)
OUTPUT_DIR = "/opt/airflow/output"        # итоговые CSV

ACTIONS = ("first_visit", "register", "login", "logout", "create_topic", "view_topic", "delete_topic", "write_message")

# ---- генерация ----
MIN_PER_DAY = 5                           # ТЗ: минимум 5 действий каждого типа в день
MAX_PER_DAY = 15                          # верхняя граница рандомайзера
MAX_REGISTRATIONS_PER_DAY = 10
MIN_TOPIC_AUTH_ERRORS_PER_DAY = 2         # ТЗ: минимум 2 ошибки создания темы без логина (берём «в день» — строже)
MAX_TOPIC_AUTH_ERRORS_PER_DAY = 4
BASE_USERS = 150                          # пользователи, зарегистрированные до генерируемого месяца
PRE_EXISTING_TOPICS = 30                  # темы, созданные до месяца: их можно смотреть и удалять с первого дня
D1_RETURN_SHARE = 0.5                     # доля вчерашних новичков, которые заходят сегодня
LOGOUT_SHARE = 0.95                       # доля сессий, закрытых выходом (остальные — «закрыл вкладку»)
ANON_MESSAGES_SHARE = (0.40, 0.60)        # ТЗ: примерно поровну анонимов и залогиненных среди сообщений

# ---- агрегация ----
SENSOR_POKE_SECONDS = 60
SENSOR_TIMEOUT_SECONDS = 30 * 60

# ---- аномалии: (нижняя граница, верхняя граница); None — без границы. Нарушение = предупреждение в логе
ANOMALY_RULES = {
    "anon_messages_pct": (None, 80.0),    # ТЗ: больше 80% сообщений от анонимов
    "total_messages": (1, None),          # день без сообщений
    "new_accounts": (1, None),            # день без регистраций
    "avg_session_duration_sec": (30.0, 4 * 3600.0),
    "topics_growth_pct": (-90.0, 500.0),  # резкий обвал или взлёт создания тем
    "d1_retention_pct": (5.0, None),      # почти никто не вернулся на следующий день
}
