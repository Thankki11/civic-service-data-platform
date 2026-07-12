# Warehouse & Dashboard — Trung (DA, Layer 3/4/5)

Output cuối tuần: **dashboard Superset trực quan hóa dữ liệu từ Gold**, tách riêng Real-time (StarRocks) và Batch (Trino).

| Việc | File |
|------|------|
| DDL bảng Dim/Fact ở Gold (kiểu dữ liệu, ràng buộc, partition) | `ddl/` |
| SQL tối ưu trên Trino (hạn chế join nặng, tận dụng partition) | `trino-queries/` |
| Data connection + export dashboard Superset | `superset/` |
| Data Validation: đối soát BI vs dữ liệu nguồn | `validation/` |

Data Dictionary (định nghĩa KPI thống nhất với nghiệp vụ) đặt tại `docs/data-dictionary.md`.
