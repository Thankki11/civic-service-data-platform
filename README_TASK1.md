# Data Platform - Enterprise Lakehouse Ingestion

Hệ thống Data Platform được thiết kế để thu thập dữ liệu (Ingestion) từ nhiều nguồn khác nhau (RDBMS CDC, XML, REST API) vào trung tâm dữ liệu **Iceberg Lakehouse** dựa trên kiến trúc Bronze-Silver-Gold. Mọi thành phần từ Storage (MinIO), Compute (Spark), Metadata (Hive Metastore) đến Messaging (Kafka, Debezium) đều được đóng gói bằng Docker.

Dưới đây là hướng dẫn chi tiết các bước để khởi chạy và thử nghiệm toàn bộ luồng dữ liệu của dự án.

---

##  1. Yêu cầu hệ thống (Prerequisites)
- Đã cài đặt **Docker** và **Docker Compose**.
---

##  2. Khởi động hạ tầng (Infrastructure)

Chạy lệnh sau để khởi động toàn bộ cụm hạ tầng bao gồm: PostgreSQL, MinIO, Kafka, Zookeeper, Debezium, Spark Master/Worker, Hive Metastore, NiFi và Mock API:

```bash
docker-compose up -d
```

**Lưu ý quan trọng đối với NiFi:**
Sau khi các container khởi động, bạn phải chạy script sau (mở Terminal mới) để tự động cài đặt thư viện AWS S3 vào bên trong NiFi, giúp NiFi có thể ghi dữ liệu xuống Iceberg. Chờ khoảng 1-2 phút cho NiFi khởi động lại:
```bash
python setup_nifi_s3.py
```


##  3. Khởi tạo Cơ sở dữ liệu và Sinh Dữ liệu (Mock Data)

Hệ thống cung cấp các công cụ sinh dữ liệu giả lập (mock data) nằm trong thư mục `data-generator/`.

### 3.1. Khởi tạo Master Data (PostgreSQL)
chạy container : docker compose run --rm data-master-gen

### 3.2. Sinh dữ liệu giao dịch XML (Transactional Data)
chạy container : docker compose run --rm data-transaction-gen

##  4. Khởi chạy luồng dữ liệu Batch (Batch Ingestion)

Các Job của Spark được thiết kế để nạp dữ liệu vào lớp **Bronze** của Iceberg Lakehouse.

### 4.1. Nạp Master Data (Job 1)
Luồng này sử dụng JDBC để đọc dữ liệu danh mục từ PostgreSQL và ghi thẳng vào bảng Iceberg:
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-data/ingestion/jdbc-db/job1_master_data.py
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

### 4.3. Nạp dữ liệu qua API bằng Apache NiFi (Job 4)
Mock API Server (cổng 5000) tự động sinh ra các giao dịch thanh toán định dạng JSON. Chúng ta sử dụng **Apache NiFi** thay vì Spark để hút dữ liệu này tự động vào Iceberg.

**Các bước chạy trên giao diện NiFi:**
1. Truy cập NiFi tại `https://localhost:8443/nifi` (Tài khoản/Mật khẩu mặc định trong docker-compose: `admin` / `adminadminadmin`).
2. Bật (Enable) các dịch vụ nền tảng: `JsonTreeReader` và `HiveCatalogService`.
3. Nhấp chuột phải vào khối **`InvokeHTTP`** (kết nối với `http://mock-api:5000/api/payments/recent`) và khối **`PutIceberg`**, chọn **Start**.
4. NiFi sẽ định kỳ hút dữ liệu (ví dụ: mỗi 10 phút/lần) từ API và tự động ghi nối tiếp (append) vào bảng `bronze_api.payment_transactions` trên Iceberg mà không cần code Spark.

##  5. Khởi chạy luồng Dữ liệu Streaming CDC (Real-time Ingestion)

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