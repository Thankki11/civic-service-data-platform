"""
Alert module — gui thong bao Telegram/Slack tu Airflow callback.
Nguoi phu trach: Thanh (Layer 6)

Cach dung trong DAG:
    default_args = {"on_failure_callback": notify_failure}
    # tuy chon: on_success_callback=notify_success cho task quan trong

Bien moi truong can co (khai bao trong docker-compose / .env):
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID   (it nhat 1 kenh)
    SLACK_WEBHOOK_URL                       (tuy chon)
"""
import logging
import os

import requests

log = logging.getLogger(__name__)
TIMEOUT = 10


def _send_telegram(msg: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not (token and chat_id):
        log.warning("Telegram chua cau hinh (thieu TELEGRAM_BOT_TOKEN/CHAT_ID)")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        return True
    except Exception as e:  # khong de alert lam fail them task
        log.error("Gui Telegram loi: %s", e)
        return False


def _send_slack(msg: str) -> bool:
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return False
    try:
        r = requests.post(url, json={"text": msg}, timeout=TIMEOUT)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error("Gui Slack loi: %s", e)
        return False


def _broadcast(msg: str):
    ok_tg = _send_telegram(msg)
    ok_sl = _send_slack(msg)
    if not (ok_tg or ok_sl):
        log.error("Khong gui duoc alert qua kenh nao! Kiem tra .env")


def notify_failure(context):
    """Callback khi task FAIL — gan vao on_failure_callback."""
    ti = context["task_instance"]
    msg = (
        "❌ AIRFLOW TASK FAILED\n"
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"Lan thu: {ti.try_number - 1}/{ti.max_tries + 1}\n"
        f"Thoi diem: {context.get('ts')}\n"
        f"Log: {ti.log_url}"
    )
    _broadcast(msg)


def notify_success(context):
    """Callback khi task SUCCESS — chi gan cho task cuoi/quan trong."""
    ti = context["task_instance"]
    msg = (
        "✅ AIRFLOW TASK SUCCESS\n"
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"Thoi diem: {context.get('ts')}"
    )
    _broadcast(msg)
