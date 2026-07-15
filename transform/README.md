# Transform: Bronze → Silver → Gold and real-time serving

This directory consumes the Bronze tables produced by the existing ingestion
jobs. It does **not** change XML parsing, JDBC master-data ingestion, API
ingestion, or Debezium configuration.

## Batch contract

1. `spark-etl/bronze_to_silver.py` reads
   `lakehouse.bronze_dvc_xml.application_xml`. It parses the JSON payload of
   each XML packet, deduplicates packet/history/payment keys, and rebuilds:

   - `lakehouse.silver.application_events`
   - `lakehouse.silver.application_current`
   - `lakehouse.silver.application_history`
   - `lakehouse.silver.payment`

2. Before Gold runs, load the small reference dimensions manually according to
   [`../warehouse/ddl/gold_dim_fact.sql`](../warehouse/ddl/gold_dim_fact.sql).
   In particular, `dim_thoi_gian` must contain every source date and
   `stt_ngay_lam_viec` must be cumulative over non-holiday days. Add the
   `can_bo_id = -1` unknown member to `dim_can_bo`.

   Master-data mapping is direct: `Status → dim_trang_thai`, `Service →
   dim_dich_vu_cong`, `Agency + Province + Ward → dim_co_quan`, and `Officer +
   Role → dim_can_bo`.

3. `spark-agg/silver_to_gold.py --as-of-date YYYY-MM-DD` snapshots only that
   date. It deletes and rewrites that Gold partition, which makes retries and
   historical backfills idempotent. It produces:

   - `fact_ton_dong_ho_so`: one open application at end-of-day;
   - `fact_van_hanh_co_quan`: one agency's daily KPI aggregates.

For the supplied archive, use a date covered by `dim_thoi_gian`, for example:

```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark-data/transform/spark-etl/bronze_to_silver.py

/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark-data/transform/spark-agg/silver_to_gold.py \
  --as-of-date 2026-07-08
```

`REJECTED` is the rework signal because the supplied master data has no separate
`PENDING`/`YEU_CAU_BO_SUNG` status. If a real source adds that state, replace
the one constant in `silver_to_gold.py` with the approved rework-code set.

## Real-time contract

Start the StarRocks service, then execute
`starrocks/routine_load.sql` through the MySQL port (`localhost:9030`). The
script creates Primary Key ODS tables, bitmap indexes for frequent filters, two
Routine Load jobs, and the 10-second asynchronous materialized view
`civic_rt.fact_xu_ly_ho_so`.

The Routine Load mappings preserve both Debezium `before` and `after`: `after`
upserts the new version, while `before.id` provides the key to delete an OLTP
row. Do not flatten Debezium at the connector if delete correctness matters.

```bash
docker compose up -d starrocks kafka debezium-connect source_db
Get-Content transform\starrocks\routine_load.sql |
  docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -u root
```

For production, use separate FE/BE nodes, a pinned StarRocks version, SSD
persistent indexes, multiple Kafka partitions, and `replication_num >= 3`.
