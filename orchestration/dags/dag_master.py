"""
DAG Master: dieu phoi end-to-end Ingestion -> Transform -> Validate (Trino).
Nguoi phu trach: Thanh
"""
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/alerts")
from notify import notify_failure  # noqa: E402

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    "owner": "thanh",
    "retries": 0,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="dag_master",
    start_date=datetime(2026, 7, 13),
    schedule="0 2 * * *",          # 2h sang hang ngay
    catchup=False,
    default_args=default_args,
    tags=["master"],
) as dag:

    run_ingestion = TriggerDagRunOperator(
        task_id="run_ingestion",
        trigger_dag_id="dag_ingestion",
        wait_for_completion=True,
    )

    run_transform = TriggerDagRunOperator(
        task_id="run_transform",
        trigger_dag_id="dag_transform",
        wait_for_completion=True,
    )

    validate_gold = TrinoOperator(
        task_id="validate_gold_not_empty",
        trino_conn_id="trino_default",
        sql="SELECT count(*) FROM iceberg.gold.fact_van_hanh_co_quan",
    )

    run_ingestion >> run_transform >> validate_gold
