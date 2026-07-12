"""
DAG Transformation: dieu phoi chuoi Spark ETL Bronze -> Silver -> Gold.
Thiet lap trigger_rule va data dependency chinh xac (theo bang action items).
Nguoi phu trach: Thanh
"""
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/alerts")
from notify import notify_failure  # noqa: E402

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "thanh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="dag_transform",
    start_date=datetime(2026, 7, 13),
    schedule=None,                 # duoc trigger boi dag_master
    catchup=False,
    default_args=default_args,
    tags=["transform", "quan"],
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        conn_id="spark_default",
        application="/opt/jobs/transform/spark-etl/bronze_to_silver.py",
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        conn_id="spark_default",
        application="/opt/jobs/transform/spark-agg/silver_to_gold.py",
        trigger_rule="all_success",   # chi chay khi Silver OK
    )

    bronze_to_silver >> silver_to_gold
