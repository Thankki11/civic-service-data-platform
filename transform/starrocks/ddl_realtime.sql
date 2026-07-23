-- Small Type-1 dimension mirrors used alongside the physical streaming fact.
-- They are loaded by transform/spark-agg/build_dim_tables.py.

CREATE DATABASE IF NOT EXISTS gold_realtime;
USE gold_realtime;

CREATE TABLE IF NOT EXISTS dim_thoi_gian (
    thoi_gian_id         INT NOT NULL,
    ngay_date            DATE,
    ngay                 INT,
    thang                INT,
    quy                  INT,
    nam                  INT,
    co_phai_la_ngay_nghi BOOLEAN,
    stt_ngay_lam_viec    INT
) PRIMARY KEY (thoi_gian_id)
DISTRIBUTED BY HASH(thoi_gian_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_co_quan (
    co_quan_id INT NOT NULL,
    ten        VARCHAR(255),
    tinh       VARCHAR(255),
    phuong     VARCHAR(255)
) PRIMARY KEY (co_quan_id)
DISTRIBUTED BY HASH(co_quan_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_trang_thai (
    trang_thai_id  INT NOT NULL,
    ma_trang_thai  VARCHAR(50),
    ten_trang_thai VARCHAR(255)
) PRIMARY KEY (trang_thai_id)
DISTRIBUTED BY HASH(trang_thai_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_can_bo (
    can_bo_id INT NOT NULL,
    ten       VARCHAR(255),
    vi_tri    VARCHAR(255)
) PRIMARY KEY (can_bo_id)
DISTRIBUTED BY HASH(can_bo_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_dich_vu_cong (
    dv_cong_id      INT NOT NULL,
    ten             VARCHAR(255),
    thoi_han_tra_kq INT
) PRIMARY KEY (dv_cong_id)
DISTRIBUTED BY HASH(dv_cong_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
