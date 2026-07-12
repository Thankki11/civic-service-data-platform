# Đăng ký metadata vào Hive Metastore — Quân

Iceberg catalog `type=hive` tự đăng ký bảng khi Spark ghi (`writeTo(...)`).
Việc cần kiểm tra:

1. Sau khi chạy 3 job Spark, xác nhận namespace `bronze`, `silver`, `gold` có trong Metastore.
2. Trino đọc được cùng catalog: cấu hình `trino-catalog/iceberg.properties`:

```
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
```

3. Chạy `SHOW TABLES FROM iceberg.gold;` trên Trino — phải thấy đúng bảng Spark vừa tạo
   → đảm bảo Schema Catalog nhất quán giữa Trino/Spark (yêu cầu trong bảng action items).
