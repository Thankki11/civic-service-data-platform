-- ============================================================================
-- File: transform/starrocks/ddl_realtime.sql
-- Phu trach: Quan (DE)
-- Muc dich: Tao database + toan bo bang StarRocks phuc vu LUONG REAL-TIME,
--           chay SONG SONG voi luong batch Bronze->Silver->Gold (Spark/Iceberg).
-- Cach chay (1 lan, thu cong, TRUOC khi chay routine_load.sql):
--   mysql -h 127.0.0.1 -P 9030 -u root < ddl_realtime.sql
--
-- LUU Y NANG CAP: CREATE TABLE IF NOT EXISTS khong the thay doi key model
-- hoac kieu cot cua bang da ton tai. Neu da khoi tao phien ban cu (history.id
-- la BIGINT, key bat dau bang id), hay reset RIENG database gold_realtime va
-- tao lai 2 Routine Load trong moi truong demo truoc khi chay file nay.
--
-- TAI SAO 2 BANG ODS + 1 MATERIALIZED VIEW MA KHONG PHAI 1 ROUTINE LOAD DUY
-- NHAT DO THANG VAO 1 BANG FACT_XU_LY_HO_SO?
--   Nguon du lieu cho fact nay la bang Application_History - nhung bang do
--   KHONG co san co_quan_id/dv_cong_id (2 khoa nay chi ton tai o bang
--   Application). Routine Load chi doc dc TU 1 topic Kafka va lam bien doi
--   THEO TUNG DONG (khong JOIN duoc voi topic/bang khac). Vi vay can:
--     (1) 1 Routine Load rieng dua Application_History -> ods_application_history_rt
--     (2) 1 Routine Load rieng dua Application         -> ods_application_rt
--     (3) 1 Materialized View JOIN ca 2 + tinh cot dan xuat (thoi_gian_xu_ly,
--         co_bi_qua_han) = fact_xu_ly_ho_so
--   Van la "Routine Load doc Kafka truc tiep, khong can Flink" dung nhu yeu
--   cau ban dau, chi la duoc chia thanh 2 buoc nho de dam bao fact du du
--   lieu de BI drill-down theo co quan/dich vu cong.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS gold_realtime;
USE gold_realtime;

-- ----------------------------------------------------------------------------
-- 1. ODS_APPLICATION_RT - "guong" (mirror) realtime cua bang Application
--    Model: PRIMARY KEY - Application la thuc the BIEN DOI (moi hanh dong xu
--    ly deu UPDATE lai Statusid/updated_at tren CUNG 1 dong) => can upsert
--    dung ngu nghia CDC (insert/update/delete) theo ho_so_id.
--    CHI dung de LAM GIAU (enrich) MV ben duoi - KHONG dung de "doi chieu
--    before/after" lay lich su trang thai, vi Primary Key table CHI giu
--    ban ghi MOI NHAT (cac phien ban cu bi xoa khoi storage khi co upsert
--    moi - xem StarRocks docs ve co che Delete+Insert cua Primary Key model).
--    Muon co lich su xu ly PHAI dung Application_History (bang so 2).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_application_rt (
    ho_so_id      VARCHAR(20)  NOT NULL COMMENT 'Application.id, vd HS_00001',
    ten_ho_so     VARCHAR(500)          COMMENT 'Application.name',
    applicant_id  VARCHAR(20)           COMMENT 'Application.Applicantid',
    dv_cong_id    INT                   COMMENT 'Application.Serviceid',
    co_quan_id    INT                   COMMENT 'Application.Agencyid',
    trang_thai_id INT                   COMMENT 'Application.Statusid (trang thai hien tai)',
    created_at    DATETIME              COMMENT 'Thoi diem nop chinh thuc',
    updated_at    DATETIME              COMMENT 'Lan cap nhat gan nhat'
) PRIMARY KEY (ho_so_id)
DISTRIBUTED BY HASH(ho_so_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"   -- index nam tren disk, tranh ton RAM khi du lieu lon
);


-- ----------------------------------------------------------------------------
-- 2. ODS_APPLICATION_HISTORY_RT - "guong" realtime cua Application_History
--    Model: DUPLICATE KEY - day la NHAT KY (log) append-only, moi ban ghi la
--    1 HANH DONG XU LY BAT BIEN (khong bao gio update/delete trong nghiep vu
--    thuc te). Day CHINH LA NGUON DUY NHAT de tinh fact_xu_ly_ho_so vi moi
--    dong da tu mang theo ca trang thai truoc (Statusid) va sau (Statusid2)
--    - KHONG can tu dien giai (diff) before/after cua CDC o cap Application.
--    Sap xep (sort key) theo (ho_so_id, action_time, id) de cac hanh dong cua
--    CUNG 1 ho so nam gan nhau tren dia + cung 1 bucket (DISTRIBUTED BY
--    HASH(ho_so_id)) -> window function LAG trong MV ben duoi chay nhanh,
--    khong bi shuffle giua cac BE.
--    trang_thai_truoc_id/ trang_thai_id luu day du chuyen trang thai de BI
--    co the drill-down audit; thoi_gian_xu_ly van tinh bang LAG(action_time),
--    khong suy dien lai tu Statusid.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_application_history_rt (
    ho_so_id            VARCHAR(20)   NOT NULL COMMENT 'Application_History.Applicationid',
    action_time         DATETIME      NOT NULL COMMENT 'Thoi diem thuc hien hanh dong',
    id                  VARCHAR(20)   NOT NULL COMMENT 'Application_History.id, vd H_a1b2c3d4',
    trang_thai_truoc_id INT                    COMMENT 'Statusid - xem luu y ve chat luong du lieu o tren',
    trang_thai_id       INT                    COMMENT 'Statusid2 - trang thai KET QUA cua hanh dong nay',
    can_bo_id           INT                    COMMENT 'Officerid',
    note                VARCHAR(1000),
    cdc_op              VARCHAR(1)             COMMENT 'Ky thuat: chi cho phep CDC c/r'
) DUPLICATE KEY (ho_so_id, action_time, id)
DISTRIBUTED BY HASH(ho_so_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");


-- ----------------------------------------------------------------------------
-- 3. 5 BANG DIM (mirror ben StarRocks, nap boi build_dim_tables.py)
--    PRIMARY KEY de moi lan chay lai script nap dim (INSERT qua JDBC) se tu
--    dong UPSERT theo khoa chinh, khong tao du lieu trung.
-- ----------------------------------------------------------------------------
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


-- ----------------------------------------------------------------------------
-- 4. FACT_XU_LY_HO_SO - Async Materialized View (REALTIME, KHONG phai bang
--    vat ly duoc Routine Load nap truc tiep)
--
--    thoi_gian_xu_ly = chenh lech (GIO) so voi action_time cua HANH DONG
--    TRUOC DO tren CUNG ho_so_id, lay bang window function LAG - day la chi
--    so "thoi gian cua 1 BUOC xu ly", KHAC voi fact_ton_dong_ho_so (batch,
--    Spark) do la "tuoi cua CA ho so tinh tu 23:59:59".
--
--    thoi_gian_xu_ly = thoi gian cua 1 BUOC (tu action truoc den action hien
--    tai), dung de phan tich bottleneck theo trang thai/can bo.
--
--    co_bi_qua_han = KPI TREN CA HO SO, chi danh gia tai su kien COMPLETED
--    (ma_trang_thai = COMPLETED trong master data): so ngay lam viec tu created_at den action
--    COMPLETED lon hon SLA cua dich vu. Cong thuc dung stt_ngay_lam_viec cua
--    dim_thoi_gian, vi vay da loai tru T7/CN/ngay le da khai bao. REJECTED
--    khong duoc tinh la ho so hoan thanh dung/tre han.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS fact_xu_ly_ho_so
DISTRIBUTED BY HASH(ho_so_id) BUCKETS 1
-- StarRocks 4.1+ áp đặt materialized_view_min_refresh_interval = 60 giây.
REFRESH ASYNC EVERY (INTERVAL 60 SECOND)
PROPERTIES ("replication_num" = "1")
AS
SELECT
    e.id,
    e.ho_so_id,
    a.co_quan_id,
    e.trang_thai_id,
    IFNULL(e.can_bo_id, -1)                                                     AS can_bo_id,
    a.dv_cong_id,
    e.thoi_gian_xu_ly,
    CASE
        WHEN tt.ma_trang_thai = 'COMPLETED'
         AND s.thoi_han_tra_kq IS NOT NULL
         AND cal_created.stt_ngay_lam_viec IS NOT NULL
         AND cal_action.stt_ngay_lam_viec IS NOT NULL
        THEN GREATEST(
            cal_action.stt_ngay_lam_viec - cal_created.stt_ngay_lam_viec,
            0
        )
        ELSE NULL
    END                                                                           AS tong_ngay_lam_viec_xu_ly,
    CASE
        WHEN tt.ma_trang_thai = 'COMPLETED'
         AND s.thoi_han_tra_kq IS NOT NULL
         AND cal_created.stt_ngay_lam_viec IS NOT NULL
         AND cal_action.stt_ngay_lam_viec IS NOT NULL
         AND GREATEST(
            cal_action.stt_ngay_lam_viec - cal_created.stt_ngay_lam_viec,
            0
         ) > s.thoi_han_tra_kq
        THEN 1 ELSE 0
    END                                                                           AS co_bi_qua_han,
    e.thoi_gian_id
FROM (
    SELECT
        id, ho_so_id, trang_thai_id, can_bo_id, action_time,
        TIMESTAMPDIFF(SECOND,
            LAG(action_time) OVER (PARTITION BY ho_so_id ORDER BY action_time),
            action_time) / 3600.0                                                AS thoi_gian_xu_ly,
        CAST(date_format(action_time, '%Y%m%d') AS INT)                          AS thoi_gian_id
    FROM ods_application_history_rt
) e
JOIN ods_application_rt a   ON e.ho_so_id = a.ho_so_id
LEFT JOIN [BROADCAST] dim_dich_vu_cong s
    ON a.dv_cong_id = s.dv_cong_id
LEFT JOIN [BROADCAST] dim_trang_thai tt
    ON e.trang_thai_id = tt.trang_thai_id
LEFT JOIN [BROADCAST] dim_thoi_gian cal_created
    ON CAST(a.created_at AS DATE) = cal_created.ngay_date
LEFT JOIN [BROADCAST] dim_thoi_gian cal_action
    ON CAST(e.action_time AS DATE) = cal_action.ngay_date;


-- ----------------------------------------------------------------------------
-- 5. BI SEMANTIC VIEW - View nay giu fact_xu_ly_ho_so o grain 1 hanh dong va
--    chi them mo ta tu 5 dim. Tat ca dim deu nho, nen dat ben PHAI cua
--    BROADCAST JOIN; fact lon khong bi shuffle. Superset truy van view nay
--    hoac truy van fact + cac join tuong duong.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_fact_xu_ly_ho_so_bi AS
SELECT
    f.*,
    cq.ten              AS ten_co_quan,
    tt.ma_trang_thai,
    tt.ten_trang_thai,
    cb.ten              AS ten_can_bo,
    cb.vi_tri           AS vi_tri_can_bo,
    dv.ten              AS ten_dich_vu_cong,
    dv.thoi_han_tra_kq,
    tg.ngay_date,
    tg.ngay,
    tg.thang,
    tg.quy,
    tg.nam
FROM fact_xu_ly_ho_so f
LEFT JOIN [BROADCAST] dim_co_quan cq
    ON f.co_quan_id = cq.co_quan_id
LEFT JOIN [BROADCAST] dim_trang_thai tt
    ON f.trang_thai_id = tt.trang_thai_id
LEFT JOIN [BROADCAST] dim_can_bo cb
    ON f.can_bo_id = cb.can_bo_id
LEFT JOIN [BROADCAST] dim_dich_vu_cong dv
    ON f.dv_cong_id = dv.dv_cong_id
LEFT JOIN [BROADCAST] dim_thoi_gian tg
    ON f.thoi_gian_id = tg.thoi_gian_id;
