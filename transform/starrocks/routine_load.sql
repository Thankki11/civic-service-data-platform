-- ============================================================================
-- File: transform/starrocks/routine_load.sql
-- Phu trach: Quan (DE)
-- Muc dich: 2 Routine Load doc TRUC TIEP tu Kafka (khong qua Flink) de nap
--           du lieu CDC realtime vao 2 bang ODS dinh nghia trong
--           ddl_realtime.sql. Bang fact_xu_ly_ho_so KHONG duoc Routine Load
--           nao nap truc tiep - no la Materialized View tu dong REFRESH tu
--           2 bang ODS nay (xem ddl_realtime.sql).
--
-- Cach chay (1 lan, thu cong, SAU KHI da chay ddl_realtime.sql):
--   mysql -h 127.0.0.1 -P 9030 -u root < routine_load.sql
--
-- LUU Y VE TEN TOPIC KAFKA
--   So do kien truc tong quan (Layer 2) ve 1 hop "Kafka Topic [db_cdc_events]"
--   mang tinh KHAI NIEM (1 luong CDC chung). Tren thuc te, Debezium
--   (ingestion/debezium_config.json, topic.prefix=postgres_server) sinh ra
--   MOI BANG 1 topic RIENG dang <prefix>.<schema>.<table>. O day dung DUNG
--   ten topic thuc te de Routine Load chay duoc:
--     - postgres_server.public.Application
--     - postgres_server.public.Application_History
-- ============================================================================

USE gold_realtime;

-- ----------------------------------------------------------------------------
-- 1. Routine Load: Application -> ods_application_rt (Primary Key, upsert)
--    Ho tro INSERT/UPDATE/snapshot (op=c/u/r -> upsert) va DELETE (op=d).
--    Debezium DELETE dat payload.after = null, nen khoa phai lay COALESCE tu
--    after.id/before.id. Connector da tat tombstone de Routine Load khong gap
--    ban tin value = null sau DELETE.
-- ----------------------------------------------------------------------------
CREATE ROUTINE LOAD gold_realtime.rl_application ON ods_application_rt
COLUMNS (
    after_ho_so_id, before_ho_so_id,
    ten_ho_so, applicant_id, dv_cong_id, co_quan_id, trang_thai_id,
    tmp_created_at, tmp_updated_at, tmp_op,
    ho_so_id = COALESCE(after_ho_so_id, before_ho_so_id),
    created_at = from_unixtime(tmp_created_at DIV 1000000),
    updated_at = from_unixtime(tmp_updated_at DIV 1000000),
    __op = CASE WHEN tmp_op = 'd' THEN 1 ELSE 0 END
)
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\",\"$.payload.before.id\",\"$.payload.after.name\",\"$.payload.after.Applicantid\",\"$.payload.after.Serviceid\",\"$.payload.after.Agencyid\",\"$.payload.after.Statusid\",\"$.payload.after.created_at\",\"$.payload.after.updated_at\",\"$.payload.op\"]",

    -- === KIEM SOAT OFFSET / TRANH MAT DU LIEU KHI SU CO MANG ===
    -- OFFSET_BEGINNING chi ap dung LAN DAU TAO JOB. Sau do StarRocks TU LUU offset da COMMIT theo
    -- tung Kafka partition trong metadata noi bo: neu job bi PAUSE do mang
    -- chap chon roi RESUME lai, no tiep tuc DUNG vi tri da commit - khong
    -- doc lai (trung lap), khong bo sot (mat du lieu).
    "max_batch_interval" = "10",      -- commit moi 10s -> neu su co, luong
                                       -- phai nap lai khi resume la toi thieu
    "max_batch_rows" = "200000", -- nguong toi thieu cua StarRocks 4.1
    "max_error_number" = "0",
    "strict_mode" = "true",
    "desired_concurrent_number" = "1"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "postgres_server.public.Application",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    "property.group.id" = "starrocks_rt_application"
);


-- ----------------------------------------------------------------------------
-- 2. Routine Load: Application_History -> ods_application_history_rt
--    (Duplicate Key, append-only)
--    Day la NGUON DUY NHAT de tinh fact_xu_ly_ho_so - xem giai thich chi
--    tiet trong phan tra loi cau hoi "doc CDC before/after Application hay
--    Application_History" gui kem trong tin nhan.
--    Bang nay la INSERT-ONLY trong nghiep vu: chi chap nhan INSERT CDC (c)
--    va snapshot ban dau (r). StarRocks 4.1 chi cho WHERE tham chieu cot dich,
--    nen cdc_op la cot ky thuat trong ODS de loc c/r truoc khi ghi fact.
-- ----------------------------------------------------------------------------
CREATE ROUTINE LOAD gold_realtime.rl_application_history ON ods_application_history_rt
COLUMNS (
    id, ho_so_id, trang_thai_truoc_id, trang_thai_id, can_bo_id,
    tmp_action_time, note, tmp_op,
    cdc_op = tmp_op,
    action_time = from_unixtime(tmp_action_time DIV 1000000)
),
WHERE cdc_op IN ('c', 'r')
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.payload.after.id\",\"$.payload.after.Applicationid\",\"$.payload.after.Statusid\",\"$.payload.after.Statusid2\",\"$.payload.after.Officerid\",\"$.payload.after.action_time\",\"$.payload.after.note\",\"$.payload.op\"]",
    "max_batch_interval" = "10",
    "max_batch_rows" = "200000", -- nguong toi thieu cua StarRocks 4.1
    "max_error_number" = "0",
    "strict_mode" = "true",
    "desired_concurrent_number" = "1"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "postgres_server.public.Application_History",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    "property.group.id" = "starrocks_rt_application_history"
);


-- ----------------------------------------------------------------------------
-- 3. Cau lenh giam sat (chay dinh ky thu cong, hoac tich hop vao alert cua
--    Thanh trong orchestration/alerts/notify.py)
-- ----------------------------------------------------------------------------
-- SHOW ROUTINE LOAD FOR gold_realtime.rl_application \G
-- SHOW ROUTINE LOAD FOR gold_realtime.rl_application_history \G
-- -- Chu y cot "State": neu = PAUSED/CANCELLED nghia la job da NGUNG NHAN
-- -- DU LIEU (rui ro mat du lieu khi het retention Kafka) -> can xu ly ngay:
-- RESUME ROUTINE LOAD FOR gold_realtime.rl_application;
-- RESUME ROUTINE LOAD FOR gold_realtime.rl_application_history;
