"""
DAG TEST NGAY 1 — nghiem thu he thong alert Telegram/Slack.
Nguoi phu trach: Thanh

Muc dich:
  task_ok        -> phai XANH, va (neu bat success alert) nhan tin ✅
  task_fail      -> co tinh FAIL sau 1 lan retry -> phai nhan tin ❌
                    co dung dag_id/task_id/link log

Day la DAG dung 1 lan de test, KHONG schedule tu dong.
"""
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/alerts")
from notify import notify_failure, notify_success  # noqa: E402

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "thanh",
    "retries": 1,                          # fail se retry 1 lan roi moi bao
    "retry_delay": timedelta(seconds=20),
    "on_failure_callback": notify_failure,
}


def _ok():
    print("Task nay luon thanh cong — dung de doi chieu voi task fail.")


def _boom():
    raise RuntimeError("LOI CO TINH de test alert — thay tin nhan nay trong Telegram/Slack la DUNG.")


with DAG(
    dag_id="dag_00_test_alert",
    description="Test alert Telegram/Slack — trigger tay, khong schedule",
    start_date=datetime(2026, 7, 13),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["test", "thanh", "day-1"],
) as dag:

    task_ok = PythonOperator(
        task_id="task_ok",
        python_callable=_ok,
        on_success_callback=notify_success,   # nhan ca tin ✅ de biet 2 chieu deu chay
    )

    task_fail = PythonOperator(
        task_id="task_fail",
        python_callable=_boom,
    )

    task_ok >> task_fail
