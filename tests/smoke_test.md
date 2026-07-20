# Smoke test end-to-end (chạy chung ngày T6)

1. `docker compose run --rm data-transactional-gen`, đồng bộ XML bằng
   `ingestion/sync_xml_to_landing.py` → file XML có trong `landing-zone`
2. Trigger `dag_master` trên Airflow UI
3. Kiểm tra:
   - [ ] Bronze XML có dữ liệu: `SELECT count(*) FROM iceberg.bronze_dvc_xml.application_xml` (Trino)
   - [ ] Silver có dữ liệu: `SELECT count(*) FROM iceberg.silver.application_events` (Trino)
   - [ ] Gold có dữ liệu: `SELECT count(*) FROM iceberg.gold.fact_van_hanh_co_quan` (Trino)
   - [ ] Silver dedup đúng, Gold có fact
   - [ ] StarRocks Routine Load state = RUNNING, offset tăng
   - [ ] Dashboard Superset hiển thị số liệu
   - [ ] Cố tình làm fail 1 task → nhận alert Telegram/Slack
   - [ ] Validation SQL (warehouse/validation) khớp số trên dashboard
