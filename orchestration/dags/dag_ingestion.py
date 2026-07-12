"""
DAG Ingestion: kich hoat NiFi flow (HttpOperator) + Spark parse XML.
Co retry va buoc clean-up du lieu rac neu chay lai (theo bang action items).
Nguoi phu trach: Thanh
"""
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/alerts")
from notify import notify_failure  # noqa: E402

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "thanh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="dag_ingestion",
    start_date=datetime(2026, 7, 13),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "kien"],
) as dag:

    def cleanup_partial_data(**_):
        """Don du lieu rac neu lan chay truoc fail giua chung (clean-up)."""
        # TODO: xoa partition/snapshot dang do trong Bronze neu can
        pass

    cleanup = PythonOperator(task_id="cleanup_partial", python_callable=cleanup_partial_data)

    trigger_nifi = HttpOperator(
        task_id="trigger_nifi_api_flow",
        http_conn_id="nifi_api",          # tao Connection trong Airflow UI
        endpoint="TODO/process-groups/.../run-status",
        method="PUT",
    )

    parse_xml = SparkSubmitOperator(
        task_id="spark_parse_xml_to_bronze",
        conn_id="spark_default",           # spark://spark-master:7077
        application="/opt/jobs/ingestion/parse_xml_to_bronze.py",
    )

    cleanup >> [trigger_nifi, parse_xml]
