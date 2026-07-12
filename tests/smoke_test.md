# Smoke test end-to-end (chạy chung ngày T6)

1. `python data-generator/generate_xml.py` → file XML có trong `landing-zone`
2. Trigger `dag_master` trên Airflow UI
3. Kiểm tra:
   - [ ] Bronze có dữ liệu: `SELECT count(*) FROM iceberg.bronze.raw_xml` (Trino)
   - [ ] Silver dedup đúng, Gold có fact
   - [ ] StarRocks Routine Load state = RUNNING, offset tăng
   - [ ] Dashboard Superset hiển thị số liệu
   - [ ] Cố tình làm fail 1 task → nhận alert Telegram/Slack
   - [ ] Validation SQL (warehouse/validation) khớp số trên dashboard
