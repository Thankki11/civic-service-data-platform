-- SQL toi uu tren Trino Query Engine — Trung
-- Nguyen tac (theo bang action items):
--   * Han che join qua nang
--   * Tan dung partition (loc theo date_key truoc)
--   * Chi select cot can thiet

SELECT
    d.year,
    d.month,
    SUM(f.total_amount) AS revenue
FROM iceberg.gold.fact_main f
JOIN iceberg.gold.dim_date d ON f.date_key = d.date_key
WHERE f.date_key >= 20260701          -- partition pruning
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
