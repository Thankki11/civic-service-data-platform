-- DDL tang Gold: Dim/Fact — Trung (DA)
-- Dinh nghia ro kieu du lieu, rang buoc va phan vung (partition)
-- theo bang action items. Gui som cho Quan de job Aggregation ghi dung schema.
-- Chay bang Spark SQL (Iceberg): spark-sql --conf ... -f gold_dim_fact.sql

-- 1. TABLES: DIMENSION (Khong phan vung)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_thoi_gian (
    thoi_gian_id  INT COMMENT 'yyyymmdd',
    ngay          INT,
    thang         INT,
    quy           INT,
    nam           INT,
    is_weekend    BOOLEAN,
    is_holiday    BOOLEAN
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_co_quan (
    id            INT,
    ma_co_quan    STRING,
    ten_co_quan   STRING,
    cap_co_quan   STRING,
    ma_phuong     STRING,
    ten_phuong    STRING,
    ma_quan       STRING,
    ten_quan      STRING,
    ma_tinh       STRING,
    ten_tinh      STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_trang_thai (
    id            INT,
    ma_trang_thai STRING,
    ten_trang_thai STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_can_bo (
    id            INT,
    name          STRING,
    role          STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_dich_vu_cong (
    id              INT,
    name            STRING,
    thoi_han_tra_kq INT
) USING iceberg;

-- ---------------------------------------------------------------------------
-- 2. TABLES: FACT (Phan vung theo thoi_gian_id)
-- ---------------------------------------------------------------------------

-- Fact 1: Ghi nhan thoi gian xu ly cua tung hanh dong (Transactional)
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_xu_ly_ho_so (
    id              BIGINT,
    ho_so_id        INT,
    trang_thai_id   INT,
    can_bo_id       INT,
    dv_cong_id      INT,
    thoi_gian_xu_ly INT COMMENT 'Thoi gian thuc te cua buoc nay (gio)',
    co_bi_qua_han   INT COMMENT '1: Tre han, 0: Dung han',
    thoi_gian_id    INT COMMENT 'Partition key (yyyymmdd)'
) USING iceberg
PARTITIONED BY (thoi_gian_id);

-- Fact 2: Chup nhanh tinh trang ho so ton dong cuoi ngay (Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_ton_dong_ho_so (
    id                        BIGINT,
    ho_so_id                  INT,
    trang_thai_id             INT,
    can_bo_id                 INT,
    dv_cong_id                INT,
    so_ngay_ton_dong_hien_tai INT COMMENT 'So ngay ngam tai trang thai hien tai',
    tong_thoi_gian_da_xu_ly   INT COMMENT 'Tuoi ho so tinh tu luc tiep nhan',
    so_luong                  INT COMMENT 'Luon la 1 de toi uu SUM()',
    thoi_gian_id              INT COMMENT 'Partition key (yyyymmdd)'
) USING iceberg
PARTITIONED BY (thoi_gian_id);

-- Fact 3: Tong hop hieu suat cua cac co quan (Aggregated)
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_van_hanh_co_quan (
    id                  BIGINT,
    co_quan_id          INT,
    so_luong_tiep_nhan  INT COMMENT 'Tong tiep nhan trong ngay',
    so_luong_dung_han   INT COMMENT 'Tong xu ly dung han trong ngay',
    so_luong_tre_han    INT COMMENT 'Tong xu ly tre han trong ngay',
    so_luong_rework     INT COMMENT 'Tong so ho so bi tra lai/yeu cau bo sung',
    so_luong_ton_dong   INT COMMENT 'Tong ton dong cuoi ngay',
    tong_chi_phi        DECIMAL(18, 2) COMMENT 'Tong doanh thu phi/le phi',
    thoi_gian_id        INT COMMENT 'Partition key (yyyymmdd)'
) USING iceberg
PARTITIONED BY (thoi_gian_id);