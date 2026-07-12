# Superset Data Connections — Trung

Theo bảng action items: tách biệt dashboard **Real-time (StarRocks)** và **Batch (Trino)**.

## 1. Trino (Batch — Gold Mart)
- SQLAlchemy URI: `trino://trino@trino:8080/iceberg`
- Kiểm tra quyền truy cập của service account kết nối (yêu cầu trong bảng)

## 2. StarRocks (Real-time)
- StarRocks nói MySQL protocol → URI: `starrocks://root:@starrocks:9030/`
  (hoặc `mysql://root:@starrocks:9030/` nếu chưa cài driver starrocks)

## Export dashboard
Sau khi dựng xong: Settings → Export dashboard → commit file .zip/.yaml vào thư mục này
để mọi người import lại được.
