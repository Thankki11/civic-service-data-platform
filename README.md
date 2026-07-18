# Data Lakehouse Project

Hệ thống Data Lakehouse end-to-end: **Ingestion → Bronze → Silver → Gold → Dashboard**

## Kiến trúc tổng quan

```
┌─────────────── NGUỒN DỮ LIỆU ───────────────┐
│  OLTP DB ──► Debezium (CDC) ──► Kafka       │
│  External API ──► NiFi (JSON)               │
│  File XML ──► Landing Zone (MinIO)          │
└──────────────────┬──────────────────────────┘
                   ▼
        ┌── BRONZE (MinIO + Iceberg) ──┐   ◄── Kiên (DE)
        │   Spark Batch parse XML      │
        │   CDC Sync Connector         │
        └──────────┬───────────────────┘
                   ▼
        ┌── SILVER (dedup, chuẩn hóa) ─┐   ◄── Quân (DE)
        │   Spark ETL                  │
        └──────────┬───────────────────┘
                   ▼
        ┌── GOLD (Dim/Fact, Mart) ─────┐   ◄── Quân + Trung
        │   Spark Aggregation          │
        │   Hive Metastore (catalog)   │
        │   StarRocks (real-time)      │
        └──────────┬───────────────────┘
                   ▼
        ┌── PRESENTATION ──────────────┐   ◄── Trung (DA)
        │   Trino Query Engine         │
        │   Apache Superset Dashboard  │
        └──────────────────────────────┘

  Toàn bộ luồng điều phối bởi Airflow DAG      ◄── Thành (DE)
  Alert Telegram/Slack khi task fail
  CI/CD: Git + Jenkins ──► deploy DAG
```

## Cấu trúc thư mục

| Thư mục          | Người phụ trách | Nội dung                                             |
|------------------|-----------------|------------------------------------------------------|
| `ingestion/`     | Kiên            | Debezium, Kafka topic, NiFi flow, Spark parse XML    |
| `transform/`     | Quân            | Spark ETL/Agg, StarRocks, Hive Metastore             |
| `orchestration/` | Thành           | Airflow DAGs, alert, Jenkins CI/CD                   |
| `warehouse/`     | Trung           | DDL Dim/Fact, SQL Trino, Superset, data validation   |
| `data-generator/`| Kiên            | Script sinh file XML mẫu vào Landing Zone, sinh data API, data vào DB OLTP |
| `docs/`          | Chung           | Kiến trúc + Data Dictionary                          |

## Khởi động môi trường

```bash
cp .env.example .env        # sửa credential nếu cần
docker compose up -d
```

## Query Gold bằng Trino

Trino được khai báo sẵn trong Compose và dùng catalog `iceberg` để đọc metadata
từ Hive Metastore, file Iceberg từ MinIO. Có thể khởi động riêng và kiểm tra:

```bash
docker compose up -d trino
docker compose exec trino trino --execute "SHOW TABLES FROM iceberg.gold"
```

Superset (khi được dựng) chỉ cần kết nối đến `trino:8080`; nó không kết nối
trực tiếp đến MinIO.

Các UI sau khi chạy:

| Service        | URL                    | Ghi chú                    |
|----------------|------------------------|----------------------------|
| MinIO Console  | http://localhost:9001  | Landing Zone + Lakehouse   |
| NiFi           | https://localhost:8443 | Flow ingestion API         |
| Airflow        | http://localhost:8080  | DAG điều phối              |
| Spark Master   | http://localhost:8081  | Theo dõi job Spark         |
| StarRocks FE   | http://localhost:8030  | Real-time OLAP             |
| Trino          | http://localhost:8085  | Query engine cho Gold      |
| Superset       | http://localhost:8088  | Dashboard                  |

## Quy ước Git

- `main`: ổn định, chỉ merge từ `dev` khi đã test end-to-end
- `dev`: nhánh tích hợp chung
- `feature/<tên>-<việc>`: ví dụ `feature/kien-nifi-api`, `feature/trung-ddl-gold`

Quy trình: tạo branch từ `dev` → commit → mở Pull Request vào `dev` → ít nhất 1 người review → merge. Jenkins theo dõi nhánh `dev` để tự deploy DAG lên Airflow Staging.

**Không commit file `.env`, credential, hay dữ liệu thật.**
