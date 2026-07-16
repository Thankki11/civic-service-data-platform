# NiFi flow — nhánh API/JSON

Flow **api_ingestion**: `InvokeHTTP` gọi `http://mock-api:5000/api/payments/recent`
→ `PutS3Object` ghi mảng JSON vào `s3a://landing-zone/api_payments_<ts>.json` trên
MinIO. Sau đó Spark `job4_api_ingestion.py` đọc các file này nạp vào Bronze.

`NifiOperator` trong `dag_ingestion` chỉ **trigger + poll** process group này —
không đụng vào cấu hình flow.

## Dựng flow (một lần)

NiFi không tự import flow lúc khởi động (khác Keycloak). Dùng script REST:

```bash
# chạy từ trong network docker (vd exec vào 1 container có python + requests),
# hoặc trên host với NIFI_BASE_URL=https://localhost:8443
python platform/nifi/bootstrap_flow.py
# -> in ra <PROCESS_GROUP_ID>
```

Script tạo process group `api_ingestion` + 2 processor + connection, tự
auto-terminate các relationship thừa. Biến môi trường: `NIFI_BASE_URL`,
`NIFI_USER`, `NIFI_PASSWORD`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`,
`MINIO_SECRET_KEY`, `MOCK_API_URL` (xem docstring).

## Nối vào Airflow

Lấy PG id ở trên gán vào Variable để `NifiOperator` dùng:

```bash
docker compose exec airflow airflow variables set nifi_api_pg_id <PROCESS_GROUP_ID>
```

## Ghi chú

- PutS3Object dùng cho MinIO: bắt buộc `Endpoint Override URL` + credential khớp
  `.env` (`minio_access_key`/`minio_secret_key`).
- Tên property processor có thể lệch nhẹ giữa các bản NiFi — nếu bootstrap báo lỗi
  property, mở NiFi UI (https://localhost:8443) kiểm tra tên hiển thị và chỉnh
  trong `bootstrap_flow.py`.
- Muốn build tay trên UI cũng được: tạo PG `api_ingestion`, thêm InvokeHTTP (GET,
  Remote URL = mock-api) nối relationship `Response` sang PutS3Object (bucket
  `landing-zone`, Object Key `api_payments_${now():format('yyyyMMddHHmmssSSS')}.json`).
