-- Real-time CDC path: PostgreSQL -> Debezium -> Kafka -> StarRocks.
--
-- Prerequisite: Debezium must use the default envelope with payload.before,
-- payload.after, payload.op and payload.ts_ms.  The existing connector writes
-- the two topics consumed below:
--   postgres_server.public.Application
--   postgres_server.public.Application_History
--
-- This script is intentionally independent of the XML batch path.  Routine
-- Load keeps offsets and gives exactly-once loading semantics; Primary Key
-- tables make CDC updates/deletes idempotent at the serving layer.

CREATE DATABASE IF NOT EXISTS civic_rt;
USE civic_rt;

-- Small dimensions are loaded manually by the team. They are replicated with
-- the same numeric keys as Gold/Iceberg, so dashboards can use one semantic
-- model across batch and real-time facts.
CREATE TABLE IF NOT EXISTS dim_thoi_gian (
    thoi_gian_id INT NOT NULL,
    ngay DATE,
    co_phai_la_ngay_nghi BOOLEAN,
    stt_ngay_lam_viec INT
)
PRIMARY KEY (thoi_gian_id)
DISTRIBUTED BY HASH(thoi_gian_id) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_persistent_index" = "true");

CREATE TABLE IF NOT EXISTS dim_trang_thai (
    trang_thai_id INT NOT NULL,
    ma_trang_thai VARCHAR(64),
    ten_trang_thai VARCHAR(255)
)
PRIMARY KEY (trang_thai_id)
DISTRIBUTED BY HASH(trang_thai_id) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_persistent_index" = "true");

CREATE TABLE IF NOT EXISTS dim_dich_vu_cong (
    dv_cong_id INT NOT NULL,
    ten VARCHAR(255),
    thoi_han_tra_kq_ngay INT
)
PRIMARY KEY (dv_cong_id)
DISTRIBUTED BY HASH(dv_cong_id) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_persistent_index" = "true");

CREATE TABLE IF NOT EXISTS dim_co_quan (
    co_quan_id INT NOT NULL,
    ten VARCHAR(255),
    tinh VARCHAR(255),
    phuong VARCHAR(255)
)
PRIMARY KEY (co_quan_id)
DISTRIBUTED BY HASH(co_quan_id) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_persistent_index" = "true");

CREATE TABLE IF NOT EXISTS dim_can_bo (
    can_bo_id INT NOT NULL,
    ten VARCHAR(255),
    vi_tri VARCHAR(255),
    co_quan_id INT
)
PRIMARY KEY (can_bo_id)
DISTRIBUTED BY HASH(can_bo_id) BUCKETS 1
PROPERTIES ("replication_num" = "1", "enable_persistent_index" = "true");

-- CDC serving state.  Primary Key is the correct model here: each business key
-- has one latest version and Debezium can emit c/u/d.  Persistent PK indices
-- reduce memory pressure while maintaining efficient point upserts/lookups.
CREATE TABLE IF NOT EXISTS ods_application_cdc (
    id VARCHAR(64) NOT NULL,
    name VARCHAR(1024),
    created_at DATETIME,
    Applicantid VARCHAR(64),
    Statusid INT,
    Serviceid INT,
    Agencyid INT,
    updated_at DATETIME,
    source_ts_ms BIGINT,
    INDEX idx_application_status (Statusid) USING BITMAP,
    INDEX idx_application_agency (Agencyid) USING BITMAP
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true",
    "persistent_index_type" = "LOCAL",
    "compression" = "LZ4"
);

CREATE TABLE IF NOT EXISTS ods_application_history_cdc (
    id VARCHAR(64) NOT NULL,
    Applicationid VARCHAR(64),
    Statusid INT,
    Statusid2 INT,
    Officerid INT,
    action_time DATETIME,
    note VARCHAR(2048),
    source_ts_ms BIGINT,
    INDEX idx_history_application (Applicationid) USING BITMAP,
    INDEX idx_history_status (Statusid2) USING BITMAP
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true",
    "persistent_index_type" = "LOCAL",
    "compression" = "LZ4"
);

-- Map before AND after explicitly.  after is used for c/u; before supplies the
-- key for a delete, so a Debezium DELETE actually removes the Primary Key row.
-- PostgreSQL timestamp values in the current connector are microseconds.
CREATE ROUTINE LOAD civic_rt.rl_application_cdc ON ods_application_cdc
COLUMNS (
    after_id, before_id, after_name, after_created_at_us, after_applicant_id,
    after_status_id, after_service_id, after_agency_id, after_updated_at_us,
    kafka_op, kafka_ts_ms,
    id = ifnull(after_id, before_id),
    name = after_name,
    created_at = from_unixtime(CAST(after_created_at_us / 1000000 AS BIGINT)),
    Applicantid = after_applicant_id,
    Statusid = CAST(after_status_id AS INT),
    Serviceid = CAST(after_service_id AS INT),
    Agencyid = CAST(after_agency_id AS INT),
    updated_at = from_unixtime(CAST(after_updated_at_us / 1000000 AS BIGINT)),
    source_ts_ms = CAST(kafka_ts_ms AS BIGINT),
    __op = IF(kafka_op = 'd', 'delete', 'upsert')
)
WHERE kafka_op IN ('c', 'u', 'd', 'r')
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\",\"$.payload.before.id\",\"$.payload.after.name\",\"$.payload.after.created_at\",\"$.payload.after.Applicantid\",\"$.payload.after.Statusid\",\"$.payload.after.Serviceid\",\"$.payload.after.Agencyid\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]",
    "strict_mode" = "false",
    "max_error_number" = "1000",
    "max_batch_interval" = "5",
    "max_batch_rows" = "200000",
    "desired_concurrent_number" = "1",
    "merge_condition" = "source_ts_ms"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "postgres_server.public.Application",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

CREATE ROUTINE LOAD civic_rt.rl_application_history_cdc ON ods_application_history_cdc
COLUMNS (
    after_id, before_id, after_application_id, after_from_status_id,
    after_to_status_id, after_officer_id, after_action_time_us, after_note,
    kafka_op, kafka_ts_ms,
    id = ifnull(after_id, before_id),
    Applicationid = after_application_id,
    Statusid = CAST(after_from_status_id AS INT),
    Statusid2 = CAST(after_to_status_id AS INT),
    Officerid = CAST(after_officer_id AS INT),
    action_time = from_unixtime(CAST(after_action_time_us / 1000000 AS BIGINT)),
    note = after_note,
    source_ts_ms = CAST(kafka_ts_ms AS BIGINT),
    __op = IF(kafka_op = 'd', 'delete', 'upsert')
)
WHERE kafka_op IN ('c', 'u', 'd', 'r')
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\",\"$.payload.before.id\",\"$.payload.after.Applicationid\",\"$.payload.after.Statusid\",\"$.payload.after.Statusid2\",\"$.payload.after.Officerid\",\"$.payload.after.action_time\",\"$.payload.after.note\",\"$.payload.op\",\"$.payload.ts_ms\"]",
    "strict_mode" = "false",
    "max_error_number" = "1000",
    "max_batch_interval" = "5",
    "max_batch_rows" = "200000",
    "desired_concurrent_number" = "1",
    "merge_condition" = "source_ts_ms"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "postgres_server.public.Application_History",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- Grain: one Application_History event.  An async MV refreshes from the two
-- PK ODS tables every 10 seconds. It uses LAG() to derive the preceding status
-- time, then Dim_Thoi_Gian to exclude holidays from the elapsed duration.
--
-- This requires StarRocks >= 3.1 because the MV definition uses window
-- functions. Query the MV itself for real-time BI; do not query the ODS tables.
CREATE MATERIALIZED VIEW IF NOT EXISTS fact_xu_ly_ho_so
REFRESH ASYNC EVERY (INTERVAL 10 SECOND)
DISTRIBUTED BY HASH(id) BUCKETS 8
ORDER BY (thoi_gian_id, co_quan_id, trang_thai_id)
AS
WITH ordered_history AS (
    SELECT
        h.id AS history_id,
        h.Applicationid AS ho_so_id,
        a.Agencyid AS co_quan_id,
        h.Statusid2 AS trang_thai_id,
        ifnull(h.Officerid, -1) AS can_bo_id,
        a.Serviceid AS dv_cong_id,
        LAG(h.action_time) OVER (
            PARTITION BY h.Applicationid ORDER BY h.action_time, h.id
        ) AS thoi_diem_bat_dau,
        h.action_time AS thoi_diem_ket_thuc,
        s.thoi_han_tra_kq_ngay
    FROM ods_application_history_cdc h
    INNER JOIN ods_application_cdc a ON h.Applicationid = a.id
    LEFT JOIN dim_dich_vu_cong s ON a.Serviceid = s.dv_cong_id
    WHERE h.action_time IS NOT NULL
), business_duration AS (
    SELECT
        o.history_id,
        o.ho_so_id,
        o.co_quan_id,
        o.trang_thai_id,
        o.can_bo_id,
        o.dv_cong_id,
        o.thoi_diem_bat_dau,
        o.thoi_diem_ket_thuc,
        o.thoi_han_tra_kq_ngay,
        SUM(
            CASE
                WHEN o.thoi_diem_bat_dau IS NULL THEN 0
                WHEN d.co_phai_la_ngay_nghi THEN 0
                WHEN DATE(o.thoi_diem_bat_dau) = DATE(o.thoi_diem_ket_thuc)
                    THEN TIMESTAMPDIFF(MINUTE, o.thoi_diem_bat_dau, o.thoi_diem_ket_thuc)
                WHEN d.ngay = DATE(o.thoi_diem_bat_dau)
                    THEN TIMESTAMPDIFF(MINUTE, o.thoi_diem_bat_dau,
                                       DATE_ADD(DATE(o.thoi_diem_bat_dau), INTERVAL 1 DAY))
                WHEN d.ngay = DATE(o.thoi_diem_ket_thuc)
                    THEN TIMESTAMPDIFF(MINUTE, DATE(o.thoi_diem_ket_thuc), o.thoi_diem_ket_thuc)
                ELSE 1440
            END
        ) AS business_minutes
    FROM ordered_history o
    LEFT JOIN dim_thoi_gian d
        ON d.ngay BETWEEN DATE(o.thoi_diem_bat_dau) AND DATE(o.thoi_diem_ket_thuc)
    GROUP BY
        o.history_id, o.ho_so_id, o.co_quan_id, o.trang_thai_id, o.can_bo_id,
        o.dv_cong_id, o.thoi_diem_bat_dau, o.thoi_diem_ket_thuc,
        o.thoi_han_tra_kq_ngay
)
SELECT
    history_id AS id,
    ho_so_id,
    co_quan_id,
    trang_thai_id,
    can_bo_id,
    dv_cong_id,
    thoi_diem_bat_dau,
    thoi_diem_ket_thuc,
    CAST(CEIL(ifnull(business_minutes, 0) / 60) AS INT) AS thoi_gian_xu_ly,
    CASE
        WHEN ifnull(business_minutes, 0) > ifnull(thoi_han_tra_kq_ngay, 0) * 1440 THEN 1
        ELSE 0
    END AS co_bi_qua_han,
    CAST(DATE_FORMAT(thoi_diem_ket_thuc, '%Y%m%d') AS INT) AS thoi_gian_id
FROM business_duration;

-- Operational checks after creating the two Routine Load jobs.
SHOW ROUTINE LOAD FROM civic_rt;
SHOW MATERIALIZED VIEWS FROM civic_rt;
