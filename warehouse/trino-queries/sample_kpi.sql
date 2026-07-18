-- Leadership KPI by month. The thoi_gian_id predicate enables Iceberg
-- partition pruning before joining the small calendar dimension.
SELECT
    d.nam,
    d.thang,
    SUM(f.so_luong_tiep_nhan) AS tong_tiep_nhan,
    SUM(f.tong_chi_phi) AS tong_doanh_thu_vnd,
    SUM(f.so_luong_ton_dong) AS tong_ton_dong,
    100.0 * SUM(f.so_luong_tre_han)
        / NULLIF(SUM(f.so_luong_tiep_nhan) + SUM(f.so_luong_ton_dong), 0) AS ty_le_tre_han_pct,
    100.0 * SUM(f.so_luong_rework)
        / NULLIF(SUM(f.so_luong_tiep_nhan), 0) AS ty_le_rework_pct
FROM iceberg.gold.fact_van_hanh_co_quan f
JOIN iceberg.gold.dim_thoi_gian d
    ON d.thoi_gian_id = f.thoi_gian_id
WHERE f.thoi_gian_id BETWEEN 20260701 AND 20260731
GROUP BY 1, 2
ORDER BY 1, 2;
