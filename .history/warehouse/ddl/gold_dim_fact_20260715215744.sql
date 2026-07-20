-- Gold Iceberg schema for the public-service application warehouse.
--
-- Dimensions are small, slowly-changing reference data. They are deliberately
-- loaded by the team as controlled files (rather than being rebuilt in each
-- batch run). The two batch jobs only read these dimensions.
--
-- Important: application IDs in the supplied XML/CDC data are values such as
-- HS_04248, not BIGINTs. Keep all business identifiers that originate from the
-- OLTP system as STRING.

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

-- A calendar row must exist for every date that can occur in the source data.
-- stt_ngay_lam_viec is cumulative and makes elapsed-business-day calculations
-- O(1): seq(end_date) - seq(start_date). It remains unchanged on holidays.
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_thoi_gian (
    thoi_gian_id           INT,
    ngay                   DATE,
    ngay_trong_thang       INT,
    thang                  INT,
    quy                    INT,
    nam                    INT,
    thu_trong_tuan         INT,
    co_phai_la_ngay_nghi   BOOLEAN,
    stt_ngay_lam_viec      INT
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_co_quan (
    co_quan_id             INT,
    ten                    STRING,
    tinh                   STRING,
    phuong                 STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_trang_thai (
    trang_thai_id          INT,
    ma_trang_thai          STRING,
    ten_trang_thai         STRING
) USING iceberg;

-- Seed and retain a row can_bo_id = -1 (Unknown / Chua phan cong). It prevents
-- a missing dimension key for applications that have not reached ASSIGNED.
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_can_bo (
    can_bo_id              INT,
    ten                    STRING,
    vi_tri                 STRING,
    co_quan_id             INT
) USING iceberg;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_dich_vu_cong (
    dv_cong_id             INT,
    ten                    STRING,
    thoi_han_tra_kq_ngay   INT
) USING iceberg;

-- Grain: one status-transition event (one Application_History record).
-- This table is populated by the real-time StarRocks path; its Iceberg schema
-- is retained so the lakehouse and BI semantic model have a consistent contract.
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_xu_ly_ho_so (
    id                     STRING,
    ho_so_id               STRING,
    co_quan_id             INT,
    trang_thai_id          INT,
    can_bo_id              INT,
    dv_cong_id             INT,
    thoi_diem_bat_dau      TIMESTAMP,
    thoi_diem_ket_thuc     TIMESTAMP,
    thoi_gian_xu_ly        INT COMMENT 'Thoi gian xu ly cua buoc, tinh theo gio lam viec',
    co_bi_qua_han          INT COMMENT '1 neu vuot SLA dich vu, 0 neu dung han',
    thoi_gian_id           INT
) USING iceberg
PARTITIONED BY (thoi_gian_id);

-- Grain: one application that is still open at the end of one reporting day.
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_ton_dong_ho_so (
    id                         STRING,
    ho_so_id                   STRING,
    trang_thai_id              INT,
    co_quan_id                 INT,
    can_bo_id                  INT,
    dv_cong_id                 INT,
    so_ngay_ton_dong_hien_tai  INT COMMENT 'Ngay lam viec tai trang thai hien tai',
    tong_thoi_gian_da_xu_ly    INT COMMENT 'Tuoi ho so theo ngay lam viec',
    so_luong                   INT COMMENT 'Hang so 1 de toi uu SUM',
    thoi_gian_id               INT
) USING iceberg
PARTITIONED BY (thoi_gian_id);

-- Grain: one agency in one reporting day. This is the leadership KPI mart.
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_van_hanh_co_quan (
    id                     STRING,
    co_quan_id             INT,
    so_luong_tiep_nhan     INT,
    so_luong_dung_han      INT,
    so_luong_tre_han       INT,
    so_luong_rework        INT,
    so_luong_ton_dong      INT,
    tong_chi_phi           BIGINT COMMENT 'VND, chi giao dich SUCCESS',
    thoi_gian_id           INT
) USING iceberg
PARTITIONED BY (thoi_gian_id);
