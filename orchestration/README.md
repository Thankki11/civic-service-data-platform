# Orchestration — Thành (Layer 6 & 7)

Output cuối tuần: **kiểm soát được thời gian / task / fail của toàn luồng** qua Airflow, alert Telegram/Slack, CI/CD Jenkins deploy DAG.

| Việc | File |
|------|------|
| DAG Ingestion (NiFi + parse XML, retry, clean-up) | `dags/dag_ingestion.py` |
| DAG Transformation (chuỗi Spark ETL, trigger_rule) | `dags/dag_transform.py` |
| DAG Master điều phối end-to-end | `dags/dag_master.py` |
| Alert Telegram/Slack khi task fail | `alerts/notify.py` |
| Jenkins auto-deploy DAG từ Git | `jenkins/Jenkinsfile` |

Operator dùng theo bảng action items: SparkSubmitOperator, HttpOperator, TrinoOperator.
