-- Data Validation — Trung
-- Doi soat so lieu tren BI voi du lieu nguon goc (theo bang action items)
-- Chay tren Trino, so ket qua voi con so tren dashboard.

-- 1. Tong ban ghi qua cac tang phai giai thich duoc chenh lech (dedup o Silver)
SELECT 'bronze' AS layer, count(*) AS cnt FROM iceberg.bronze.raw_xml
UNION ALL
SELECT 'silver', count(*) FROM iceberg.silver.cleaned
UNION ALL
SELECT 'gold_fact', sum(total_records) FROM iceberg.gold.fact_main;

-- 2. TODO: doi soat tung KPI theo Data Dictionary
