-- Run in Trino after a batch. These are reconciliation checks, not dashboard SQL.

-- Bronze -> Silver counts have expected differences because one XML packet can
-- emit a history record, no history record, and/or a payment record.
SELECT 'bronze_xml_packets' AS dataset, count(*) AS row_count
FROM iceberg.bronze_dvc_xml.application_xml
UNION ALL
SELECT 'silver_application', count(*)
FROM iceberg.silver.application
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

-- SCD2: moi natural key chi duoc co mot version current, va khoang hieu luc
-- khong duoc dao nguoc. Chay tung query cho 4 dim SCD2 khi build dim xong.
SELECT co_quan_id, count(*) AS current_versions
FROM iceberg.gold.dim_co_quan
WHERE is_current
GROUP BY co_quan_id
HAVING count(*) <> 1;

SELECT dv_cong_id, effective_from_ts, effective_to_ts
FROM iceberg.gold.dim_dich_vu_cong
WHERE effective_to_ts IS NOT NULL
  AND effective_to_ts <= effective_from_ts;

-- Fact batch phai tham chieu dung version SLA da luu, khong join natural key
-- de tranh nhan dong khi Service co nhieu version.
SELECT f.ho_so_id, f.thoi_gian_id
FROM iceberg.gold.fact_ton_dong_ho_so f
LEFT JOIN iceberg.gold.dim_dich_vu_cong d
  ON f.dim_dich_vu_cong_sk = d.dim_dich_vu_cong_sk
WHERE f.dim_dich_vu_cong_sk IS NULL OR d.dim_dich_vu_cong_sk IS NULL;
