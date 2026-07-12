-- DDL tang Gold: Dim/Fact — Trung (DA)
-- Dinh nghia ro kieu du lieu, rang buoc va phan vung (partition)
-- theo bang action items. Gui som cho Quan de job Aggregation ghi dung schema.
-- Chay bang Spark SQL (Iceberg): spark-sql --conf ... -f gold_dim_fact.sql

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
    date_key      INT COMMENT 'yyyymmdd',
    full_date     DATE,
    year          INT,
    quarter       INT,
    month         INT,
    day_of_week   INT
) USING iceberg;

-- TODO: cac dim khac theo Data Model (dim_customer, dim_product...)

CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_main (
    date_key       INT,
    dim_key_1      BIGINT,
    dim_key_2      BIGINT,
    total_records  BIGINT,
    total_amount   DECIMAL(18, 2)
) USING iceberg
PARTITIONED BY (date_key);       -- partition de Trino query nhanh
