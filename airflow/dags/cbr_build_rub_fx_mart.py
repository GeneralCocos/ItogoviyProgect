from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "cbr_build_rub_fx_mart"

SQL_BUILD_RUB_FX_MART = """
SET search_path TO cbr_dwh, public;

-- 1. Создаём витрину, если её ещё нет
CREATE TABLE IF NOT EXISTS mart_cbr_rub_fx (
    date_key         integer       NOT NULL,
    full_date        date          NOT NULL,
    char_code        varchar(10)   NOT NULL,          -- код валюты (USD, EUR, ...)
    currency_name    varchar(255)  NOT NULL,          -- имя валюты
    nominal          integer       NOT NULL,          -- номинал (как в ЦБ)
    rate_value       numeric(18,4) NOT NULL,          -- курс (рублей за nominal единиц валюты)
    rate_prev        numeric(18,4),                   -- курс вчера
    rate_change_abs  numeric(18,4),                   -- Δ в абсолюте
    rate_change_pct  numeric(18,6),                   -- Δ в процентах
    load_dttm        timestamptz   NOT NULL,
    CONSTRAINT mart_cbr_rub_fx_pk PRIMARY KEY (date_key, char_code)
)
DISTRIBUTED BY (date_key, char_code);

-- 2. Полностью пересчитаем витрину
TRUNCATE TABLE mart_cbr_rub_fx;

INSERT INTO mart_cbr_rub_fx (
    date_key,
    full_date,
    char_code,
    currency_name,
    nominal,
    rate_value,
    rate_prev,
    rate_change_abs,
    rate_change_pct,
    load_dttm
)
WITH fact AS (
    SELECT
        f.date_key,
        d.full_date,
        dc.char_code,
        dc.name        AS currency_name,
        dc.nominal,
        f.rate_value,
        f.load_dttm,
        LAG(f.rate_value) OVER (
            PARTITION BY f.currency_key
            ORDER BY d.full_date
        ) AS rate_prev
    FROM fact_cbr_daily_rates f
    JOIN dim_date d
      ON d.date_key = f.date_key
    JOIN dim_currency dc
      ON dc.currency_key = f.currency_key
)
SELECT
    date_key,
    full_date,
    char_code,
    currency_name,
    nominal,
    rate_value,
    rate_prev,
    CASE
        WHEN rate_prev IS NULL THEN NULL
        ELSE rate_value - rate_prev
    END AS rate_change_abs,
    CASE
        WHEN rate_prev IS NULL OR rate_prev = 0 THEN NULL
        ELSE (rate_value - rate_prev) / rate_prev
    END AS rate_change_pct,
    load_dttm
FROM fact;
"""


with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        start_date=datetime(2025, 11, 20),
        schedule_interval="@daily",
        catchup=True,
        max_active_runs=1,
        tags=["cbr", "greenplum", "mart", "kimball"],
) as dag:

    build_rub_fx_mart = PostgresOperator(
        task_id="build_rub_fx_mart",
        postgres_conn_id="greenplum_gpdb",  # коннект к Greenplum
        sql=SQL_BUILD_RUB_FX_MART,
    )
