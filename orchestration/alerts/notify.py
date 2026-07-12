"""
Callback gui alert qua Telegram/Slack khi task Airflow that bai.
Nguoi phu trach: Thanh
Gan vao DAG: default_args = {"on_failure_callback": notify_failure}
"""
import os
import requests


def notify_failure(context):
    ti = context["task_instance"]
    msg = (
        f"❌ Airflow task FAILED\n"
        f"DAG: {ti.dag_id}\nTask: {ti.task_id}\n"
        f"Execution: {context['ts']}\nLog: {ti.log_url}"
    )

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        requests.post(
            f"https://api.telegram.org/bot{tg_token}/sendMessage",
            json={"chat_id": tg_chat, "text": msg},
            timeout=10,
        )

    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        requests.post(slack_url, json={"text": msg}, timeout=10)
