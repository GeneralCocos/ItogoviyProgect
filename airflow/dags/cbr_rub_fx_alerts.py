from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


DAG_ID = "cbr_rub_fx_alerts"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


SQL_INIT_ALERT_TABLE = """
CREATE SCHEMA IF NOT EXISTS cbr_alerts;

CREATE TABLE IF NOT EXISTS cbr_alerts.fx_rate_alerts (
    alert_id          bigserial PRIMARY KEY,
    alert_dttm        timestamptz NOT NULL DEFAULT now(), 
    full_date         date        NOT NULL,           
    char_code         varchar(10) NOT NULL,         
    rate_value        numeric(18,4) NOT NULL,            
    rate_prev         numeric(18,4),                     
    rate_change_abs   numeric(18,4),                     
    rate_change_pct   numeric(18,4),                    
    rule_name         text        NOT NULL,              
    rule_details      text,                              
    source_system     text        NOT NULL DEFAULT 'greenplum',
    load_dttm         timestamptz NOT NULL DEFAULT now()
)
DISTRIBUTED REPLICATED;
"""


with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        start_date=datetime(2025, 11, 20),
        schedule_interval="0 7 * * *",
        catchup=False,
        max_active_runs=1,
        tags=["cbr", "alerts", "greenplum"],
) as dag:

    init_alert_table = PostgresOperator(
        task_id="init_fx_alerts_table",
        postgres_conn_id="greenplum_gpdb",
        sql=SQL_INIT_ALERT_TABLE,
    )

    @task(task_id="generate_fx_alerts")
    def generate_fx_alerts():
        gp = PostgresHook(postgres_conn_id="greenplum_gpdb")

        sql_last_day = """
            SELECT max(full_date)::date
            FROM cbr_dwh.mart_cbr_rub_fx
        """
        last_day_rows = gp.get_records(sql_last_day)
        if not last_day_rows or last_day_rows[0][0] is None:
            return

        last_day = last_day_rows[0][0]

        sql_alerts = """
            SELECT
                full_date,
                char_code,
                rate_value,
                rate_prev,
                rate_change_abs,
                rate_change_pct
            FROM cbr_dwh.mart_cbr_rub_fx
            WHERE full_date = %s
              AND char_code IN ('USD', 'EUR', 'CNY')
              AND ABS(rate_change_pct) >= 2.0
        """

        rows = gp.get_records(sql_alerts, parameters=(last_day,))
        if not rows:
            return

        insert_rows = []
        for full_date, char_code, rate_value, rate_prev, ch_abs, ch_pct in rows:
            rule_name = "DAILY_FX_MOVE_2PCT"
            rule_details = (
                f"Курс {char_code} изменился на {ch_pct}% "
                f"({ch_abs} руб.) за день {full_date}"
            )
            insert_rows.append(
                (
                    full_date,
                    char_code,
                    rate_value,
                    rate_prev,
                    ch_abs,
                    ch_pct,
                    rule_name,
                    rule_details,
                )
            )

        gp.insert_rows(
            table="cbr_alerts.fx_rate_alerts",
            rows=insert_rows,
            target_fields=[
                "full_date",
                "char_code",
                "rate_value",
                "rate_prev",
                "rate_change_abs",
                "rate_change_pct",
                "rule_name",
                "rule_details",
            ],
            commit_every=100,
        )

    init_alert_table >> generate_fx_alerts()
