# Kiến trúc chi tiết

(Điền sơ đồ chi tiết + quyết định thiết kế tại đây. Sơ đồ tổng quan xem README gốc.)

## Mapping Layer theo bảng action items
- Layer 2 — Ingestion/Buffer: Debezium, Kafka, NiFi (Kiên)
- Layer 3 — Storage: MinIO, Iceberg, Spark ETL, Hive Metastore (Kiên/Quân)
- Layer 4 — Compute: StarRocks, Trino (Quân/Trung)
- Layer 5 — Presentation: Superset, Data Dictionary (Trung)
- Layer 6 — Orchestration: Airflow DAG, alert Telegram/Slack (Thành)
- Layer 7 — CI/CD: Git + Jenkins (Thành)
