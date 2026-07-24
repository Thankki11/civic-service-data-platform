"""
DAG INGESTION — dieu phoi luong nap du lieu THAT cua Kien qua kien truc custom.

Cau truc (3 nhanh batch song song):
  cleanup_partial_data
     |- seed_and_sync_xml ------> spark_job2_xml   (pipeline bronze_xml)
     |- nifi_fetch_api ---------> spark_job4_api   (pipeline bronze_api)
     |- spark_job1_master        (pipeline bronze_master, doc JDBC truc tiep)
                                       |
                               verify_and_finish

Operator custom (orchestration/plugins):
  - CustomSparkSubmitOperator: lay ma PySpark tu pipeline-api (Keycloak ROPC),
    tu sinh ENV catalog tu spark_conf, roi spark-submit.
  - NifiOperator: trigger + poll process group NiFi (nhanh API/JSON).

Nhanh CDC (job3 streaming) KHONG nam trong DAG batch nay — chay nhu service
always-on rieng (xem docker-compose service `spark-cdc-bronze`).

Variables:
  ingestion_force_fail = "true"  -> ep verify FAIL de test alert (Test 2.3)
Connections (khai bao qua env trong docker-compose):
  spark_default, keycloak_default, nifi_default
Nguoi phu trach: Thanh (Layer 6)
"""
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/alerts")
from notify import notify_failure, notify_success  # noqa: E402

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from operators.custom_spark_submit_operator import CustomSparkSubmitOperator
from operators.nifi_operator import NifiOperator

default_args = {
    "owner": "thanh",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_failure,
}

FORCE_FAIL = Variable.get("ingestion_force_fail", default_var="false").lower() == "true"

REPO = "/opt/airflow/repo"


def _cleanup(**context):
    """Don du lieu do dang neu lan chay truoc fail giua chung.

    Khi Bronze that san sang: goi expire_snapshots / xoa partition tam cua
    logical_date nay (phoi hop voi Kien). Hien ghi log ro rang de xac nhan.
    """
    print(f"[cleanup] logical_date = {context['ds']} — chua co gi de don")


def _maybe_fail():
    if FORCE_FAIL:
        raise RuntimeError("ingestion_force_fail=true — ep fail de test alert.")
    print("[verify] force_fail dang tat, luong ket thuc binh thuong.")


with DAG(
    dag_id="dag_ingestion",
    description="Cleanup -> (XML | API/NiFi | Master) -> Bronze Iceberg",
    start_date=datetime(2026, 7, 13),
    schedule=None,              # chạy tay -> tránh backlog auto-run nghẽn max_active_runs
    catchup=False,
    max_active_runs=1,          # tranh 2 run chong nhau ghi de Bronze
    default_args=default_args,
    tags=["ingestion", "thanh", "custom"],
) as dag:

    cleanup = PythonOperator(
        task_id="cleanup_partial_data",
        python_callable=_cleanup,
    )

    # --- Nhanh XML: sinh XML + day len landing (cung 1 container/volume) ---
    seed_and_sync_xml = BashOperator(
        task_id="seed_and_sync_xml",
        # Sinh XML vào /tmp (repo mount tu Windows khong cho airflow-user ghi),
        # roi sync len landing bang boto3.
        bash_command=(
            "rm -rf /tmp/xmlgen && mkdir -p /tmp/xmlgen && cd /tmp/xmlgen && "
            f"MINIO_ENDPOINT=http://minio:9000 "
            f"python {REPO}/data-generator/data_Transactional.py && "
            f"SOURCE_DIR=raw/xml MINIO_ENDPOINT=http://minio:9000 "
            f"python {REPO}/ingestion/sync_xml_to_landing.py"
        ),
    )
    spark_job2_xml = CustomSparkSubmitOperator(
        task_id="spark_job2_xml_to_bronze",
        pipeline_name="bronze_xml",
        name="ingestion-xml-{{ ds }}",
    )

    # --- Nhanh API/JSON: NiFi keo tu mock-api -> landing, roi Spark parse ---
    nifi_fetch_api = NifiOperator(
        task_id="nifi_fetch_api",
        process_group_name="api_ingestion",
        wait_for_completion=True,
    )
    spark_job4_api = CustomSparkSubmitOperator(
        task_id="spark_job4_api_to_bronze",
        pipeline_name="bronze_api",
        name="ingestion-api-{{ ds }}",
    )

    # --- Nhanh Master: doc JDBC source_db truc tiep ---
    spark_job1_master = CustomSparkSubmitOperator(
        task_id="spark_job1_master_to_bronze",
        pipeline_name="bronze_master",
        name="ingestion-master-{{ ds }}",
    )

    verify = PythonOperator(
        task_id="verify_and_finish",
        python_callable=_maybe_fail,
        on_success_callback=notify_success,   # bao ✅ khi ca luong xong
        trigger_rule="all_success",
    )

    cleanup >> [seed_and_sync_xml, nifi_fetch_api, spark_job1_master]
    seed_and_sync_xml >> spark_job2_xml
    nifi_fetch_api >> spark_job4_api
    [spark_job2_xml, spark_job4_api, spark_job1_master] >> verify
