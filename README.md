# Финальный проект. Анализ транзакционной активности

Разработка системы получения и анализа информации о транзакционной активности пользователей финтех-стартапа.

## Применяемые технологии

| Технология | Назначение |
|------------|------------|
| PostgreSQL | Источник данных |
| Apache Airflow | Оркестратор ETL-процессов |
| Vertica | Аналитическое хранилище (DWH) |
| Metabase | BI-инструмент для визуализации |

## План реализации

1. Скачивание и запуск Docker-образа с инфраструктурой проекта
2. Подключение к источнику (PostgreSQL)
3. Анализ данных источника
4. Подключение к Vertica
5. Создание схемы `*__STAGING` и таблиц для сырых данных (`transactions`, `currencies`)
6. Создание схемы `*__DWH` и таблицы витрины (`global_metrics`)
7. DAG загрузки данных из PostgreSQL в `*__STAGING`
8. DAG обработки и загрузки данных из `*__STAGING` в `*__DWH`
9. Подключение Metabase к Vertica и создание дашборда

## Структура проекта

```
de-project-final/
└── src/
    ├── dags/                  # DAG-файлы Airflow
    │   ├── dag_pg_to_vertica.py
    │   └── dag_stg_to_mart.py
    ├── py/                    # Python-модули
    │   ├── stg_loader.py      # Загрузка из источника в STG
    │   ├── mart_loader.py     # Загрузка из STG в витрину
    │   ├── SqlHelper.py       # Вспомогательный класс для SQL и логов
    │   └── config.py          # Настройка путей
    ├── sql/                   # SQL-скрипты
    │   ├── ddl/               # DDL таблиц
    │   │   ├── stg/           # Таблицы STG-слоя
    │   │   └── cdm/           # Таблицы слоя витрин
    │   ├── load_from_src/     # Загрузка из источника в STG
    │   └── load_to_mart/      # Загрузка из STG в витрину
    └── img/                   # Скриншоты дашборда
```

## 0. Запуск Docker-образа

Контейнер запущен из образа `cr.yandex/crp1r8pht0n0gl25aug1/de-final-prj:latest`.

Доступные сервисы:

| Сервис | URL |
|--------|-----|
| Airflow | http://localhost:8280/airflow/ |
| Metabase | http://localhost:8998/ |

> PostgreSQL в контейнере присутствует, но для выбранного сценария не используется.

## 1. Подключение к источнику (PostgreSQL)

Подключение выполнено через DBeaver.

**Параметры подключения:**
- База данных: `db1`
- Хост: `rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net`
- Порт: `6432`

## 2. Анализ данных источника

### 2.1. Таблица `transactions`

Содержит информацию о движении денежных средств между клиентами в разных валютах.

**Структура:**

| Поле | Тип | Описание |
|------|-----|----------|
| `operation_id` | varchar(60) | ID транзакции |
| `account_number_from` | int | Счет отправителя |
| `account_number_to` | int | Счет получателя |
| `currency_code` | int | Трехзначный код валюты |
| `country` | varchar(30) | Страна-источник |
| `status` | varchar(30) | Статус транзакции |
| `transaction_type` | varchar(30) | Тип транзакции |
| `amount` | int | Сумма транзакции |
| `transaction_dt` | timestamp | Дата и время транзакции |

**Статусы:** `queued`, `in_progress`, `blocked`, `done`, `chargeback`

**Типы транзакций:** `authorisation`, `sbp_incoming`, `sbp_outgoing`, `transfer_incoming`, `transfer_outgoing`, `c2b_partner_incoming`, `c2b_partner_outgoing`

### 2.2. Таблица `currencies`

Справочник курсов валют.

**Структура:**

| Поле | Тип | Описание |
|------|-----|----------|
| `date_update` | timestamp | Дата обновления курса |
| `currency_code` | int | Код валюты |
| `currency_code_with` | int | Код парной валюты |
| `currency_with_div` | numeric(5,3) | Курс: единиц `currency_code_with` за 1 единицу `currency_code` |

## 3. Подключение к Vertica

Подключение выполнено через DBeaver с использованием учетных данных из спринта 6.

## 4. Создание STG-слоя

Схема: `VT260224AD30FB__STAGING`

### 4.1. Таблица `transactions`

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.transactions 
(
    operation_id       VARCHAR(60),
    account_number_from INT,
    account_number_to  INT,
    currency_code      INT,
    country            VARCHAR(30),
    status             VARCHAR(30),
    transaction_type   VARCHAR(30),
    amount             INT,
    transaction_dt     TIMESTAMP
)
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES;
```

- Сортировка по `transaction_dt` ускоряет фильтрацию по дате
- Сегментация по `HASH(operation_id, transaction_dt)` обеспечивает равномерное распределение по нодам

### 4.2. Таблица `currencies`

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.currencies
(
    date_update        TIMESTAMP,
    currency_code      INT,
    currency_code_with INT,
    currency_with_div  NUMERIC(5,3)
)
ORDER BY date_update
SEGMENTED BY HASH(currency_code, date_update) ALL NODES;
```

### 4.3. Таблица логов `load_log`

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.load_log
(
    schema_name   VARCHAR(100),
    table_name    VARCHAR(100),
    load_end      TIMESTAMP,
    rows_loaded   INTEGER,
    status        VARCHAR(20),
    error_message VARCHAR(1000)
)
ORDER BY load_end
SEGMENTED BY HASH(table_name, load_end) ALL NODES;
```

## 5. Создание витрины `global_metrics`

Схема: `VT260224AD30FB__DWH`

**Поля витрины:**

| Поле | Тип | Описание |
|------|-----|----------|
| `date_update` | TIMESTAMP NOT NULL | Дата расчета |
| `currency_from` | INT NOT NULL | Код валюты транзакции |
| `amount_total` | INT NOT NULL | Общая сумма транзакций в валюте 420 |
| `cnt_transactions` | INT NOT NULL | Количество транзакций |
| `avg_transactions_per_account` | NUMERIC(14,2) NOT NULL | Средний объем транзакций с аккаунта |
| `cnt_accounts_make_transactions` | INT NOT NULL | Количество уникальных аккаунтов |

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.global_metrics
(
    date_update                    TIMESTAMP NOT NULL,
    currency_from                  INT NOT NULL,
    amount_total                   INT NOT NULL,
    cnt_transactions               INT NOT NULL,
    avg_transactions_per_account   NUMERIC(14,2) NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL,
    CONSTRAINT pk_global_metrics PRIMARY KEY (date_update, currency_from)
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
```

> **Примечание:** `PRIMARY KEY` в Vertica носит информационный характер (не проверяет уникальность). Добавлен для документирования логической уникальности пары `(date_update, currency_from)`.

**Обоснование типов данных:**
- `amount_total` / `cnt_transactions` / `cnt_accounts_make_transactions` — `INT`, целые неотрицательные числа
- `avg_transactions_per_account` — `NUMERIC(14,2)`, максимальное значение ~99.99 млрд, точность 2 знака
- `date_update` — `TIMESTAMP`, данные источника без часового пояса

## 6. DAG загрузки данных: PostgreSQL → STG

**DAG:** `pg_to_vertica`
**Расписание:** ежедневно в 03:00

### 6.1. Подключения и переменные Airflow

| Тип | Имя | Назначение |
|-----|-----|------------|
| Connection | `SRC_conn` | Подключение к PostgreSQL |
| Connection | `DWH_conn` | Подключение к Vertica |
| Variable | `src_schema` | Схема источника |
| Variable | `stg_schema` | Схема STG-слоя |

### 6.2. Модуль `stg_loader.py` (класс `Loader`)

**Методы:**

| Метод | Назначение |
|-------|------------|
| `__init__` | Инициализация PostgresHook, VerticaHook, схем |
| `_load_sql` | Чтение SQL-скриптов из файлов |
| `_log_writer` | Запись логов в `load_log` |
| `get_transactions_data` | Загрузка `transactions` из PostgreSQL → DataFrame |
| `get_currencies_data` | Загрузка `currencies` из PostgreSQL → DataFrame |
| `load_data` | Загрузка DataFrame → Vertica через CSV/COPY |

**Процесс загрузки:** Источник → Pandas DataFrame → CSV в памяти → `COPY` в Vertica

### 6.3. Ключевые решения

1. **Инкрементальная загрузка** — интервал `[low_threshold, high_threshold]`, где:
   - `low_threshold` = последняя дата в STG + 1 день
   - `high_threshold` = контекстная дата Airflow
2. **Параметризованные SQL-запросы** — защита от SQL-инъекций
3. **Единый метод `load_data`** для обеих таблиц
4. **Загрузка через `COPY` из CSV в памяти** — минимизация ROS-контейнеров в Vertica
5. **Логирование** — запись в `load_log` при успехе и ошибке

### 6.4. Структура DAG

2 таски, последовательно:
1. `load_transactions`
2. `load_currencies`

## 7. DAG обработки данных: STG → DWH

**DAG:** `stg_to_mart`
**Расписание:** ежедневно в 04:00

### 7.1. Модуль `mart_loader.py` (класс `MartLoader`)

**Метод `load_mart_global_metrics`:**

1. Определяется `low_threshold` (последняя дата в витрине + 1 день)
2. Определяется `high_threshold` (вчерашняя дата)
3. Выполняется параметризованный SQL с `MERGE` за весь интервал **одним запросом** (без цикла по дням)

### 7.2. SQL-скрипт `load_mart_global_metrics.sql`

**Логика запроса:**

1. Из `transactions` отбираются данные:
   - `account_number_from >= 0` — исключение тестовых аккаунтов
   - `status = 'done'` — только успешные транзакции
   - `transaction_dt` в интервале `(low_threshold, high_threshold]`
2. Агрегация по `(date_update, currency_from)`:
   - `SUM(amount)` — общая сумма
   - `COUNT(*)` — количество транзакций
   - `COUNT(DISTINCT account_number_from)` — уникальные аккаунты
   - Средний объем = сумма / кол-во уникальных аккаунтов
3. `LEFT JOIN` с `currencies` по `(currency_code, date_update)`, где `currency_code_with = 420`
4. Конвертация сумм: для валюты 420 без изменений, для остальных — умножение на курс
5. `MERGE` — защита от дублирования при повторных запусках

### 7.3. Модуль `SqlHelper.py`

| Метод | Назначение |
|-------|------------|
| `load_sql` | Чтение SQL-скриптов из директории `sql/` |
| `write_stg_log` | Запись логов STG-загрузок |
| `write_dwh_log` | Запись логов DWH-загрузок с интервалами `[low_threshold, high_threshold]` |

### 7.4. Таблица логов DWH

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.load_log
(
    schema_name    VARCHAR(100),
    table_name     VARCHAR(100),
    low_threshold  TIMESTAMP,
    high_threshold TIMESTAMP,
    load_end       TIMESTAMP,
    status         VARCHAR(20),
    error_message  VARCHAR(1000)
)
ORDER BY load_end
SEGMENTED BY HASH(table_name, load_end) ALL NODES;
```

### 7.5. Ключевые решения

1. **Загрузка одним SQL-запросом** без цикла по дням — одно подключение к БД
2. **`MERGE` вместо `INSERT`** — защита от дублей
3. **Конвертация в валюту 420** — единая валюта для аналитики
4. **Параметризация** — защита от SQL-инъекций
5. **Согласованность расписаний** — DWH-загрузка (04:00) запускается до STG-загрузки (05:00)

## 8. Дашборд в Metabase

Подключение к Vertica, схема `VT260224AD30FB__DWH`, таблица `global_metrics`.

### Визуализации

| № | Метрика | Тип графика | Настройки |
|---|--------|-------------|-----------|
| 1 | Сумма переводов по дням | Bar chart | X: `date_update`, Y: `SUM(amount_total)`, Group: `currency_from` |
| 2 | Средний объем на пользователя | Line chart | X: `date_update`, Y: `avg_transactions_per_account`, Group: `currency_from` |
| 3 | Уникальные пользователи | Bar chart | X: `date_update`, Y: `cnt_accounts_make_transactions`, Group: `currency_from` |
| 4 | Общий оборот (единая валюта) | Line chart | X: `date_update`, Y: `SUM(amount_total)`, без группировки |

### Фильтры дашборда

| Фильтр | Тип | Применение |
|--------|-----|------------|
| Дата | Date Range | Ко всем графикам |
| Валюта (`currency_from`) | Category | К графикам 1-3 |

> Фильтр по валюте не применяется к графику общего оборота, так как он показывает совокупную сумму по всем валютам.
