"""
DAG Transformation: Dieu phoi chuoi Spark ETL Bronze -> Silver -> Gold.
Tich hop CustomSparkSubmitOperator (lay code/config tu pipeline-api qua Keycloak authentication).

Luong thuc thi:
  spark_bronze_to_silver (bronze_to_silver.py)
           |
  spark_build_dim_tables (build_dim_tables.py)
           |
  spark_silver_to_gold   (silver_to_gold.py)
           |
  verify_and_finish_transform (notify success)

Nguoi phu trach: Thanh + Quan
"""
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/alerts")
from notify import notify_failure, notify_success  # noqa: E402

from airflow import DAG
from airflow.operators.python import PythonOperator
from operators.custom_spark_submit_operator import CustomSparkSubmitOperator

default_args = {
    "owner": "thanh",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}


def _finish_transform(**context):
    print(f"[transform] Hoan thanh luong Transform (Bronze -> Silver -> Gold) cho ds = {context['ds']}")


with DAG(
    dag_id="dag_transform",
    description="Bronze -> Silver -> Gold Dimensions & Facts (Spark + Iceberg + StarRocks)",
    start_date=datetime(2026, 7, 13),
    schedule=None,                 # duoc trigger boi dag_master
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transform", "thanh", "quan", "custom"],
) as dag:

    # Task 1: Bronze -> Silver (dedup, chuan hoa)
    spark_bronze_to_silver = CustomSparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        pipeline_name="silver",
        name="transform-silver-{{ ds }}",
    )

    # Task 2: Build Gold Dimensions (Iceberg Gold + StarRocks gold_realtime)
    spark_build_dims = CustomSparkSubmitOperator(
        task_id="spark_build_dim_tables",
        pipeline_name="gold_dims",
        name="transform-dims-{{ ds }}",
    )

    # Task 3: Silver -> Gold Facts (Periodic snapshot)
    spark_silver_to_gold = CustomSparkSubmitOperator(
        task_id="spark_silver_to_gold",
        pipeline_name="gold",
        name="transform-gold-{{ ds }}",
    )

    verify_transform = PythonOperator(
        task_id="verify_and_finish_transform",
        python_callable=_finish_transform,
        on_success_callback=notify_success,
        trigger_rule="all_success",
    )

    spark_bronze_to_silver >> spark_build_dims >> spark_silver_to_gold >> verify_transform
