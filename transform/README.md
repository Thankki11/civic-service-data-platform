# Transform — Quân (Layer 3 & 4)

Output cuối tuần: **tầng Gold warehouse (Dim/Fact) sẵn sàng cho DA**, metadata đăng ký trong Hive Metastore, StarRocks consume được stream Kafka.

| Việc | File |
|------|------|
| Bronze → Silver (dedup, chuẩn hóa) | `spark-etl/bronze_to_silver.py` |
| Silver → Gold (Sum, Count... theo Dim/Fact) | `spark-agg/silver_to_gold.py` |
| StarRocks table model + Routine Load | `starrocks/` |
| Đăng ký metadata Bronze/Silver/Gold | `metastore/` |

Lưu ý từ bảng action items:
- Chọn table model StarRocks phù hợp: Duplicate / Aggregate / Unique / Primary Key
- Routine Load: kiểm soát offset để không mất dữ liệu khi có sự cố mạng
- Spark job: thiết kế tính toán phân tán tránh OOM (repartition, tránh collect)
- Schema catalog phải nhất quán giữa Trino và Spark (cùng Hive Metastore)
