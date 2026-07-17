from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0. NGAY CHOT SO
# ---------------------------------------------------------------------------
snapshot_date = (datetime.now() - timedelta(days=1)).date()

cutoff_ts = f"{snapshot_date} 23:59:59"          # moc thoi gian chot so trong ngay
thoi_gian_id = int(snapshot_date.strftime("%Y%m%d"))
print(f"[*] Chay Gold aggregation cho snapshot_date = {snapshot_date} (thoi_gian_id={thoi_gian_id})")

STATUS_COMPLETED = 7
STATUS_REJECTED = 8

# ---------------------------------------------------------------------------
# 1. SPARK SESSION
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"
CATALOG = "lakehouse"

spark = (
    SparkSession.builder
    .appName(f"Transform_SilverToGold_{snapshot_date}")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "thrift://hive-metastore:9083")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_WAREHOUSE)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # Quan trong: chi ghi de dung partition (ngay) dang xu ly, giu nguyen lich su
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")


def save_gold_partitioned(df, table_name):
    full_name = f"{CATALOG}.gold.{table_name}"
    schema_sql = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        PARTITIONED BY (thoi_gian_id)
        LOCATION 's3a://lakehouse/warehouse/gold/{table_name}'
    """)
    df.write.format("iceberg").mode("overwrite").save(full_name)
    print(f"[+] gold.{table_name} <- da ghi partition thoi_gian_id={thoi_gian_id}")


silver_application = spark.table(f"{CATALOG}.silver.application")
silver_history = spark.table(f"{CATALOG}.silver.application_history")
silver_service = spark.table(f"{CATALOG}.silver.service").select(
    F.col("id").alias("dv_cong_id"), F.col("processing_time")
)
silver_agency = spark.table(f"{CATALOG}.silver.agency").select(F.col("id").alias("co_quan_id"))
silver_payment = spark.table(f"{CATALOG}.silver.payment")

# Calendar is a small controlled dimension.  Do not calculate SLA with
# timestamp / 86400: that counts Saturday, Sunday and configured holidays.
calendar = spark.table(f"{CATALOG}.gold.dim_thoi_gian").select(
    F.col("ngay_date").cast("date").alias("ngay_date"),
    F.col("stt_ngay_lam_viec").cast("int").alias("stt_ngay_lam_viec"),
)
if calendar.filter(F.col("ngay_date") == F.lit(str(snapshot_date))).limit(1).count() == 0:
    raise RuntimeError(
        f"dim_thoi_gian chua co ngay {snapshot_date}; hay chay build_dim_tables.py truoc Gold job."
    )

calendar_created = F.broadcast(calendar.select(
    F.col("ngay_date").alias("created_date"),
    F.col("stt_ngay_lam_viec").alias("created_workday_seq"),
))
calendar_action = F.broadcast(calendar.select(
    F.col("ngay_date").alias("action_date"),
    F.col("stt_ngay_lam_viec").alias("action_workday_seq"),
))
calendar_cutoff = F.broadcast(
    calendar.filter(F.col("ngay_date") == F.lit(str(snapshot_date))).select(
        F.col("stt_ngay_lam_viec").alias("cutoff_workday_seq")
    )
)

# ===========================================================================
# 2. FACT_TON_DONG_HO_SO  (Periodic Snapshot Fact - 1 dong/1 ngay/1 ho so mo)
#    Mapping logic theo dung bao cao thuc tap (muc "Mapping Logic - Fact
#    Xu Ly & Fact Ton Dong"):
#      so_ngay_ton_dong_hien_tai = Moc chot (23:59:59) - action_time cua
#                                  buoc trang thai hien tai (lan cap nhat
#                                  gan nhat tinh den cutoff)
#      tong_thoi_gian_da_xu_ly   = Moc chot (23:59:59) - Application.created_at
# ===========================================================================
# Trang thai/can bo "as-of" phai lay tu history truoc moc chot. Khong duoc
# dung trang_thai hien tai cua silver.application: job Gold chay luc 02:00
# co the da nhin thay thay doi cua ngay moi va lam sai snapshot hom qua.
w_latest_before_cutoff = Window.partitionBy("ho_so_id").orderBy(F.col("action_time").desc())
latest_state_as_of_cutoff = (
    silver_history
    .filter(F.col("action_time") <= F.lit(cutoff_ts))
    .withColumn("rn", F.row_number().over(w_latest_before_cutoff))
    .filter("rn = 1")
    .select(
        "ho_so_id",
        F.col("trang_thai_id").alias("trang_thai_id_as_of"),
        F.col("can_bo_id"),
        F.col("action_time").alias("latest_action_time"),
    )
)

# Application giu cac thuoc tinh on dinh (co quan, dich vu, ngay nop). Trang
# thai de quyet dinh ton dong phai la trang thai cua history tai cutoff.
# Neu event DELETE xay ra SAU cutoff, ho so van phai xuat hien trong snapshot
# cua ngay truoc; neu DELETE xay ra truoc cutoff thi loai ra.
open_apps = (
    silver_application
    .filter(
        (F.col("created_at") <= F.lit(cutoff_ts))
        & ((F.col("da_bi_xoa") == False) | (F.col("event_time") > F.lit(cutoff_ts)))  # noqa: E712
    )
    .join(latest_state_as_of_cutoff, on="ho_so_id", how="left")
    .withColumn(
        "trang_thai_id",
        F.coalesce(F.col("trang_thai_id_as_of"), F.col("trang_thai_id")),
    )
    .filter(~F.col("trang_thai_id").isin(STATUS_COMPLETED, STATUS_REJECTED))
)

fact_ton_dong_raw = (
    open_apps.repartition(16, "ho_so_id")
    .join(calendar_created, F.to_date("created_at") == F.col("created_date"), "left")
    .join(calendar_action, F.to_date("latest_action_time") == F.col("action_date"), "left")
    .crossJoin(calendar_cutoff)
    .withColumn("can_bo_id", F.coalesce(F.col("can_bo_id"), F.lit(-1)))
    .withColumn(
        "so_ngay_ton_dong_hien_tai",
        F.greatest(
            F.col("cutoff_workday_seq")
            - F.coalesce(F.col("action_workday_seq"), F.col("created_workday_seq")),
            F.lit(0),
        ).cast("int"),
    )
    .withColumn(
        "tong_thoi_gian_da_xu_ly",
        F.greatest(F.col("cutoff_workday_seq") - F.col("created_workday_seq"), F.lit(0)).cast("int"),
    )
)

fact_ton_dong_ho_so = fact_ton_dong_raw.select(
    F.xxhash64(F.lit(thoi_gian_id), F.col("ho_so_id")).alias("id"),   # surrogate key BIGINT
    F.col("ho_so_id"),                                                 # STRING - xem ghi chu dau file
    F.col("trang_thai_id"),
    F.col("co_quan_id"),
    F.col("can_bo_id"),
    F.col("dv_cong_id"),
    F.col("so_ngay_ton_dong_hien_tai"),
    F.col("tong_thoi_gian_da_xu_ly"),
    F.lit(1).alias("so_luong"),
    F.lit(thoi_gian_id).alias("thoi_gian_id"),
)

save_gold_partitioned(fact_ton_dong_ho_so, "fact_ton_dong_ho_so")

# cache() vi bang nay nho (so ho so dang mo), duoc TAI SU DUNG ngay ben duoi
# cho fact_van_hanh_co_quan.so_luong_ton_dong -> DAM BAO 2 fact luon khop so
# (khong tinh backlog 2 lan bang 2 cach khac nhau -> tranh lech du lieu khi
# len dashboard)
fact_ton_dong_ho_so.cache()

# ===========================================================================
# 3. FACT_VAN_HANH_CO_QUAN (Aggregated Fact - 1 dong/1 ngay/1 co quan)
# ===========================================================================

# 3.1 So luong tiep nhan trong ngay
received_today = (
    silver_application
    .filter(F.to_date("created_at") == F.lit(str(snapshot_date)))
    .groupBy("co_quan_id")
    .agg(F.count("*").alias("so_luong_tiep_nhan"))
)

# 3.2 So luong ton dong trong ngay = tai su dung fact_ton_dong_ho_so vua tinh
backlog_today = (
    fact_ton_dong_ho_so
    .groupBy("co_quan_id")
    .agg(F.sum("so_luong").alias("so_luong_ton_dong"))
)

# 3.3 Ho so COMPLETED trong ngay -> tach dung han / tre han
#     (Tong thoi gian xu ly thuc te = COMPLETED.action_time - Application.
#     created_at, so sanh voi Service.processing_time. Luong hien tai
#     REJECTED la trang thai KET THUC - khong quay lai xu ly tiep - nen
#     khong co buoc tru thoi gian REJECTED nhu mo ta cho truong hop tong
#     quat hon trong bao cao.)
completed_today = (
    silver_history
    .filter((F.col("trang_thai_id") == STATUS_COMPLETED) & (F.to_date("action_time") == F.lit(str(snapshot_date))))
    .join(
        silver_application.select("ho_so_id", "co_quan_id", "created_at", "dv_cong_id"),
        on="ho_so_id", how="inner",
    )
    .join(F.broadcast(silver_service), on="dv_cong_id", how="left")
    .join(calendar_created, F.to_date("created_at") == F.col("created_date"), "left")
    .join(calendar_action, F.to_date("action_time") == F.col("action_date"), "left")
    .withColumn(
        "tong_ngay_lam_viec_xu_ly",
        F.greatest(F.col("action_workday_seq") - F.col("created_workday_seq"), F.lit(0)),
    )
    .withColumn(
        "tre_han",
        F.when(F.col("tong_ngay_lam_viec_xu_ly") > F.col("processing_time"), 1).otherwise(0),
    )
)

on_off_time_today = (
    completed_today
    .groupBy("co_quan_id")
    .agg(
        F.sum(F.when(F.col("tre_han") == 1, 1).otherwise(0)).alias("so_luong_tre_han_da_dong"),
        F.sum(F.when(F.col("tre_han") == 0, 1).otherwise(0)).alias("so_luong_dung_han"),
    )
)

# Theo bao cao nghiep vu, KPI tre han gom ca 2 nhom: da COMPLETED tre va
# dang ton nhung tuoi ho so da vuot SLA. Nhom thu hai lay truc tiep tu fact
# snapshot vua tao de cung mot dinh nghia ton dong tren moi dashboard.
overdue_open_today = (
    fact_ton_dong_ho_so
    .join(F.broadcast(silver_service), on="dv_cong_id", how="left")
    .filter(F.col("tong_thoi_gian_da_xu_ly") > F.col("processing_time"))
    .groupBy("co_quan_id")
    .agg(F.countDistinct("ho_so_id").alias("so_luong_tre_han_dang_mo"))
)

# 3.4 Rework: KHONG CO trang thai rieng "yeu cau bo sung" trong 8 status hien
#     co (chi co PENDING_APPROVAL = cho ky duyet, KHAC nghia voi "cho bo
#     sung"). TAM DUNG REJECTED lam proxy gan nhat cho "ho so bi tra lai" -
#     CAN DOI NGHIEP VU XAC NHAN LAI, hoac bo sung 1 status/flag rieng
#     "YEU_CAU_BO_SUNG" trong tuong lai de do chinh xac hon.
rework_today = (
    silver_history
    .filter((F.col("trang_thai_id") == STATUS_REJECTED) & (F.to_date("action_time") == F.lit(str(snapshot_date))))
    .join(silver_application.select("ho_so_id", "co_quan_id"), on="ho_so_id", how="inner")
    .groupBy("co_quan_id")
    .agg(F.count("*").alias("so_luong_rework"))
)

# 3.5 Tong chi phi thu trong ngay
fee_today = (
    silver_payment
    .filter(
        (F.to_date("thoi_gian_tt") == F.lit(str(snapshot_date)))
        & (F.upper(F.col("trang_thai_tt")) == F.lit("SUCCESS"))
    )
    .join(silver_application.select("ho_so_id", "co_quan_id"), on="ho_so_id", how="inner")
    .groupBy("co_quan_id")
    .agg(F.sum("so_tien").alias("tong_chi_phi"))
)

# 3.6 Hop nhat: xuat phat tu TOAN BO danh sach co quan (silver.agency) de moi
#     co quan deu co 1 dong/ngay du hom do khong phat sinh gi (tranh dashboard
#     "thieu cot" gay hieu nham la khong co du lieu)
fact_van_hanh_co_quan = (
    silver_agency
    .join(received_today, "co_quan_id", "left")
    .join(backlog_today, "co_quan_id", "left")
    .join(on_off_time_today, "co_quan_id", "left")
    .join(overdue_open_today, "co_quan_id", "left")
    .join(rework_today, "co_quan_id", "left")
    .join(fee_today, "co_quan_id", "left")
    .fillna(0, subset=[
        "so_luong_tiep_nhan", "so_luong_ton_dong", "so_luong_tre_han_da_dong",
        "so_luong_tre_han_dang_mo", "so_luong_dung_han", "so_luong_rework", "tong_chi_phi",
    ])
    .select(
        F.xxhash64(F.lit(thoi_gian_id), F.col("co_quan_id")).alias("id"),  # surrogate key BIGINT
        F.col("co_quan_id"),
        F.col("so_luong_tiep_nhan").cast("int"),
        F.col("so_luong_dung_han").cast("int"),
        (F.col("so_luong_tre_han_da_dong") + F.col("so_luong_tre_han_dang_mo")).cast("int").alias("so_luong_tre_han"),
        F.col("so_luong_rework").cast("int"),
        F.col("so_luong_ton_dong").cast("int"),
        F.col("tong_chi_phi").cast("long"),
        F.lit(thoi_gian_id).alias("thoi_gian_id"),
    )
)

save_gold_partitioned(fact_van_hanh_co_quan, "fact_van_hanh_co_quan")

fact_ton_dong_ho_so.unpersist()
print("[+] Hoan tat Silver -> Gold.")
spark.stop()
