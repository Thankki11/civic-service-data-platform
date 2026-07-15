-- Run in Trino after a batch. These are reconciliation checks, not dashboard SQL.

-- Bronze -> Silver counts have expected differences because one XML packet can
-- emit a history record, no history record, and/or a payment record.
SELECT 'bronze_xml_packets' AS dataset, count(*) AS row_count
FROM iceberg.bronze_dvc_xml.application_xml
UNION ALL
SELECT 'silver_application_events', count(*)
FROM iceberg.silver.application_events
UNION ALL
SELECT 'silver_application_history', count(*)
FROM iceberg.silver.application_history
UNION ALL
SELECT 'silver_payment', count(*)
FROM iceberg.silver.payment
UNION ALL
SELECT 'gold_backlog_rows', count(*)
FROM iceberg.gold.fact_ton_dong_ho_so
UNION ALL
SELECT 'gold_agency_day_rows', count(*)
FROM iceberg.gold.fact_van_hanh_co_quan;

-- A Gold agency/day total must reconcile to its atomic backlog snapshot.
SELECT
    f.thoi_gian_id,
    f.co_quan_id,
    f.so_luong_ton_dong AS gold_ton_dong,
    count(b.ho_so_id) AS snapshot_ton_dong
FROM iceberg.gold.fact_van_hanh_co_quan f
LEFT JOIN iceberg.gold.fact_ton_dong_ho_so b
    ON b.thoi_gian_id = f.thoi_gian_id
   AND b.co_quan_id = f.co_quan_id
GROUP BY 1, 2, 3
HAVING f.so_luong_ton_dong <> count(b.ho_so_id);
