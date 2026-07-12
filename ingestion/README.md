# Ingestion — Kiên (Layer 2 & 3)

Output cuối tuần: **dữ liệu Bronze nằm trong MinIO/Iceberg** từ 3 nhánh nguồn.

| Nhánh | Công nghệ | File |
|-------|-----------|------|
| CDC từ OLTP DB | Debezium → Kafka topic `db_cdc_events` | `debezium/`, `kafka/` |
| External API (JSON) | NiFi (retry, backpressure, heap tuning) | `nifi/` |
| File XML từ Landing Zone | Spark Batch parse → Iceberg/Parquet | `spark-batch/` |

Lưu ý từ bảng action items:
- Tài khoản DB cho Debezium phải có quyền đọc WAL/binlog
- Kafka topic: cân nhắc số partition / replication factor / retention để tối ưu throughput
- NiFi: cài cảnh báo khi hàng đợi bị nghẽn
- Schema file XML có thể thay đổi tùy bản dump — parse phải chịu được schema drift
