from __future__ import annotations

from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "cbr_dwh_to_gp_fct"


SQL_DIM_DATE = """
SET search_path TO cbr_dwh, public;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     integer      NOT NULL,        
    full_date    date         NOT NULL,
    year         integer      NOT NULL,
    quarter      integer      NOT NULL,
    month        integer      NOT NULL,
    month_name   varchar(20),
    day          integer      NOT NULL,
    day_of_week  integer      NOT NULL,         
    day_name     varchar(20),
    is_weekend   boolean      NOT NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
)
DISTRIBUTED REPLICATED;

INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, is_weekend
)
SELECT DISTINCT
    EXTRACT(YEAR FROM f.rate_date)::int * 10000
      + EXTRACT(MONTH FROM f.rate_date)::int * 100
      + EXTRACT(DAY FROM f.rate_date)::int        AS date_key,
    f.rate_date                                   AS full_date,
    EXTRACT(YEAR FROM f.rate_date)::int          AS year,
    EXTRACT(QUARTER FROM f.rate_date)::int       AS quarter,
    EXTRACT(MONTH FROM f.rate_date)::int         AS month,
    TO_CHAR(f.rate_date, 'TMMonth')              AS month_name,
    EXTRACT(DAY FROM f.rate_date)::int           AS day,
    EXTRACT(ISODOW FROM f.rate_date)::int        AS day_of_week,
    TO_CHAR(f.rate_date, 'TMDay')                AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM f.rate_date) IN (6,7) THEN true ELSE false END AS is_weekend
FROM cbr_dwh.cbr_daily_rates_fct f
LEFT JOIN dim_date d
  ON d.full_date = f.rate_date
WHERE d.full_date IS NULL;
"""


SQL_DIM_CURRENCY = """
SET search_path TO cbr_dwh, public;

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_key  bigserial,
    char_code     varchar(10)  NOT NULL,
    num_code      varchar(10),
    nominal       integer,
    name          varchar(255) NOT NULL,
    is_active     boolean      NOT NULL DEFAULT true,
    valid_from    date,
    valid_to      date,
    CONSTRAINT dim_currency_pkey PRIMARY KEY (currency_key),
    CONSTRAINT dim_currency_uk UNIQUE (char_code)
)
DISTRIBUTED REPLICATED;

-- Новые валюты
INSERT INTO dim_currency (
    char_code, num_code, nominal, name, is_active, valid_from
)
SELECT DISTINCT
    f.char_code,
    f.num_code,
    f.nominal,
    f.name,
    true,
    MIN(f.rate_date) OVER (PARTITION BY f.char_code) AS valid_from
FROM cbr_dwh.cbr_daily_rates_fct f
LEFT JOIN dim_currency dc
  ON dc.char_code = f.char_code
WHERE dc.char_code IS NULL;

-- Обновление атрибутов (SCD Type 1)
UPDATE dim_currency dc
SET
    num_code = src.num_code,
    nominal = src.nominal,
    name     = src.name
FROM (
    SELECT DISTINCT char_code, num_code, nominal, name
    FROM cbr_dwh.cbr_daily_rates_fct
) src
WHERE dc.char_code = src.char_code;
"""


SQL_FACT_RATES = """
SET search_path TO cbr_dwh, public;

CREATE TABLE IF NOT EXISTS fact_cbr_daily_rates (
    date_key      integer       NOT NULL,
    currency_key  bigint        NOT NULL,
    rate_value    numeric(18,4) NOT NULL,
    load_dttm     timestamptz   NOT NULL,
    CONSTRAINT fact_cbr_daily_rates_pk PRIMARY KEY (date_key, currency_key)
)
DISTRIBUTED BY (date_key, currency_key);

TRUNCATE TABLE fact_cbr_daily_rates;

INSERT INTO fact_cbr_daily_rates (
    date_key,
    currency_key,
    rate_value,
    load_dttm
)
SELECT
    EXTRACT(YEAR FROM f.rate_date)::int * 10000
      + EXTRACT(MONTH FROM f.rate_date)::int * 100
      + EXTRACT(DAY FROM f.rate_date)::int        AS date_key,
    dc.currency_key,
    f.value       AS rate_value,
    f.load_dttm
FROM cbr_dwh.cbr_daily_rates_fct f
JOIN dim_currency dc
  ON dc.char_code = f.char_code;
"""


with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        start_date=datetime(2025, 11, 20),
        schedule_interval="@daily",
        catchup=True,
        max_active_runs=1,
        tags=["cbr", "dwh", "greenplum", "kimball"],
) as dag:

    @task(task_id="load_stage_to_gp")
    def load_stage_to_gp():
        dwh = PostgresHook(postgres_conn_id="dwh_postgres")
        src_records = dwh.get_records(
            """
            SELECT
                rate_date,
                char_code,
                num_code,
                nominal,
                name,
                value,
                load_dttm
            FROM cbr_dwh.cbr_daily_rates_stage
            """
        )

        gp = PostgresHook(postgres_conn_id="greenplum_gpdb")

        gp.run("TRUNCATE TABLE cbr_dwh.cbr_daily_rates_fct;")

        if not src_records:
            return

        epoch = date(1970, 1, 1)
        prepared_rows = []

        for row in src_records:
            (
                rate_date_raw,
                char_code,
                num_code,
                nominal,
                name,
                value,
                load_dttm_raw,
            ) = row

            if isinstance(rate_date_raw, (datetime, date)):
                rate_date_val = (
                    rate_date_raw.date()
                    if isinstance(rate_date_raw, datetime)
                    else rate_date_raw
                )
            else:
                rate_date_val = epoch + timedelta(days=int(rate_date_raw))

            if isinstance(load_dttm_raw, str):
                load_dttm_val = datetime.fromisoformat(
                    load_dttm_raw.replace("Z", "+00:00")
                )
            else:
                load_dttm_val = load_dttm_raw

            nominal_val = int(nominal) if nominal is not None else None
            value_val = float(value) if value is not None else None

            prepared_rows.append(
                (
                    rate_date_val,
                    char_code,
                    num_code,
                    nominal_val,
                    name,
                    value_val,
                    load_dttm_val,
                )
            )

        gp.insert_rows(
            table="cbr_dwh.cbr_daily_rates_fct",
            rows=prepared_rows,
            target_fields=[
                "rate_date",
                "char_code",
                "num_code",
                "nominal",
                "name",
                "value",
                "load_dttm",
            ],
            commit_every=1000,
        )

    dim_date_task = PostgresOperator(
        task_id="build_dim_date",
        postgres_conn_id="greenplum_gpdb",
        sql=SQL_DIM_DATE,
    )

    dim_currency_task = PostgresOperator(
        task_id="build_dim_currency",
        postgres_conn_id="greenplum_gpdb",
        sql=SQL_DIM_CURRENCY,
    )

    fact_rates_task = PostgresOperator(
        task_id="build_fact_cbr_daily_rates",
        postgres_conn_id="greenplum_gpdb",
        sql=SQL_FACT_RATES,
    )

    load_stage_to_gp() >> [dim_date_task, dim_currency_task] >> fact_rates_task
