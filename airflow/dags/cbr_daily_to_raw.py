from __future__ import annotations

from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


RAW_CONN_ID = "raw_postgres"
CBR_BASE_URL = "https://www.cbr.ru"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _fetch_cbr_daily_xml(date_obj: datetime) -> bytes:
    date_str = date_obj.strftime("%d/%m/%Y")
    url = f"{CBR_BASE_URL}/scripts/XML_daily.asp"
    params = {"date_req": date_str}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.content


def _parse_xml(xml_bytes: bytes, rate_date: datetime):
    root = ET.fromstring(xml_bytes)
    rows = []

    for valute in root.findall("Valute"):
        num_code_el = valute.find("NumCode")
        char_code_el = valute.find("CharCode")
        nominal_el = valute.find("Nominal")
        name_el = valute.find("Name")
        value_el = valute.find("Value")

        num_code = num_code_el.text if num_code_el is not None else None
        char_code = char_code_el.text if char_code_el is not None else None
        nominal_raw = nominal_el.text if nominal_el is not None else "1"
        name = name_el.text if name_el is not None else ""
        value_raw = value_el.text if value_el is not None else "0"

        nominal = int(nominal_raw.replace(",", "."))
        value = float(value_raw.replace(",", "."))

        rows.append(
            (
                rate_date.date(),
                num_code,
                char_code,
                nominal,
                name,
                value,
            )
        )

    return rows


with DAG(
        dag_id="cbr_daily_to_raw",
        description="Загрузка ежедневных курсов ЦБ РФ из XML в RAW Postgres",
        start_date=datetime(2025, 7, 1),
        schedule_interval="@daily",
        catchup=True,
        default_args=default_args,
        tags=["cbr", "raw", "xml"],
) as dag:

    @task
    def load_daily_rates():
        context = get_current_context()
        ds = context["ds"]
        date_obj = datetime.strptime(ds, "%Y-%m-%d")

        xml_bytes = _fetch_cbr_daily_xml(date_obj)

        rows = _parse_xml(xml_bytes, date_obj)

        if not rows:
            return

        hook = PostgresHook(postgres_conn_id=RAW_CONN_ID)
        conn = hook.get_conn()
        insert_sql = """
            INSERT INTO cbr.cbr_daily_rates_raw (
                rate_date,
                num_code,
                char_code,
                nominal,
                name,
                value
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (rate_date, char_code) DO UPDATE
            SET num_code  = EXCLUDED.num_code,
                nominal   = EXCLUDED.nominal,
                name      = EXCLUDED.name,
                value     = EXCLUDED.value,
                load_dttm = now();
        """

        with conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)

    load_daily_rates()
