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

---

# Hướng dẫn chi tiết luồng Ingestion (từ nhánh Ingestion)

Hệ thống Data Platform được thiết kế để thu thập dữ liệu (Ingestion) từ nhiều nguồn khác nhau (RDBMS CDC, XML, REST API) vào trung tâm dữ liệu **Iceberg Lakehouse** dựa trên kiến trúc Bronze-Silver-Gold. Mọi thành phần từ Storage (MinIO), Compute (Spark), Metadata (Hive Metastore) đến Messaging (Kafka, Debezium) đều được đóng gói bằng Docker.

Dưới đây là hướng dẫn chi tiết các bước để khởi chạy và thử nghiệm toàn bộ luồng dữ liệu của dự án.

## 1. Yêu cầu hệ thống (Prerequisites)
- Đã cài đặt **Docker** và **Docker Compose**.

## 2. Khởi động hạ tầng (Infrastructure)

Chạy lệnh sau để khởi động toàn bộ cụm hạ tầng bao gồm: PostgreSQL, MinIO, Kafka, Zookeeper, Debezium, Spark Master/Worker, Hive Metastore và Mock API:

```bash
docker-compose up -d
```

## 3. Khởi tạo Cơ sở dữ liệu và Sinh Dữ liệu (Mock Data)

Hệ thống cung cấp các công cụ sinh dữ liệu giả lập (mock data) nằm trong thư mục `data-generator/`.

### 3.1. Khởi tạo Master Data (PostgreSQL)
Chạy script để sinh ra file SQL chứa dữ liệu tĩnh (Danh mục):
```bash
python data-generator/data_master.py
```
*(Lệnh này sẽ tạo ra file `mock_database_csv/master_data_init.sql`)*

Sau đó, nạp file SQL này vào container PostgreSQL (`source_db`) để tạo bảng và import dữ liệu:
- **Trên Windows (CMD/PowerShell):**
  ```cmd
  cmd /c "docker exec -i source_db psql -U source_db -d source_db < mock_database_csv/master_data_init.sql"
  ```
- **Trên Linux/Mac:**
  ```bash
  docker exec -i source_db psql -U source_db -d source_db < mock_database_csv/master_data_init.sql
  ```

### 3.2. Sinh dữ liệu giao dịch XML (Transactional Data)
Chạy script sau để tạo ra các gói tin XML giả lập (Hồ sơ dịch vụ công):
```bash
python data-generator/data_Transactional.py
```
Các file XML sẽ được sinh ra và lưu tại thư mục `raw/xml/`.

---

## 4. Khởi chạy luồng dữ liệu Batch (Batch Ingestion)

Các Job của Spark được thiết kế để nạp dữ liệu vào lớp **Bronze** của Iceberg Lakehouse.

### 4.1. Nạp Master Data (Job 1)
Luồng này sử dụng JDBC để đọc dữ liệu danh mục từ PostgreSQL và ghi thẳng vào bảng Iceberg:
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-data/ingestion/jdbc-db/spark-batch/job1_master_data.py
```

### 4.2. Nạp dữ liệu XML (Job 2)
Đầu tiên, đồng bộ các file XML vừa sinh lên Storage MinIO (Landing Zone):
```bash
# Nếu chạy từ máy ngoài:
python sync_xml_to_landing.py

# Hoặc dùng Docker (nếu máy ngoài không có thư viện boto3):
docker exec -it mock-api bash -c "pip install boto3 && MINIO_ENDPOINT=http://minio:9000 /python sync_xml_to_landing.py"
```

Sau khi đồng bộ xong, gọi Spark chạy luồng xử lý XML (Job 2):
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-data/ingestion/spark-batch/job2_transactional_xml.py
```

### 4.3. Nạp dữ liệu qua API (Job 4)
Mock API Server (cổng 5000) đã tự động chạy qua file `docker-compose.yml`. Dùng Spark để gọi API lấy dữ liệu thanh toán và ghi vào Iceberg:
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-data/ingestion/nifi/job4_api_ingestion.py
```

---

## 5. Khởi chạy luồng Dữ liệu Streaming CDC (Real-time Ingestion)

### 5.1. Đăng ký Debezium Connector
Gắn connector để Debezium bắt đầu bắt các sự kiện thay đổi (Insert/Update/Delete) từ PostgreSQL đẩy vào Kafka:
```bash
curl.exe -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8084/connectors/ -d "@debezium_config.json"
```

### 5.2. Chạy Spark Structured Streaming (Job 3)
Chạy Job bắt sự kiện từ Kafka và ghi liên tục vào Iceberg Lakehouse:
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-data/ingestion/streaming-kafka/job3_streaming_cdc.py
```
*(Job này sẽ chạy liên tục để lắng nghe Kafka).*

