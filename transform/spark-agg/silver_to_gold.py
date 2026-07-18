import argparse
import os
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0. NGAY CHOT SO
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Build one daily Gold snapshot; a rerun replaces only that date partition."
)
parser.add_argument(
    "--snapshot-date",
    default=os.getenv("SNAPSHOT_DATE"),
    help="Ngay can chot so, dinh dang YYYY-MM-DD. Mac dinh: hom qua.",
)
args = parser.parse_args()

if args.snapshot_date:
    try:
        snapshot_date = date.fromisoformat(args.snapshot_date)
    except ValueError as exc:
        raise ValueError("--snapshot-date phai co dinh dang YYYY-MM-DD") from exc
else:
    snapshot_date = (datetime.now() - timedelta(days=1)).date()

if snapshot_date > date.today():
    raise ValueError("Khong the chot Gold cho ngay trong tuong lai.")

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
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")


def replace_gold_snapshot_partition(df, table_name):
    """Thay dung partition ngay chot so cua periodic snapshot fact.

    Khong APPEND (rerun se trung grain) va khong MERGE theo dong (co the con
    sot mot ho so da dong sau khi tinh lai). overwrite theo filter la mot
    Iceberg atomic commit, chi dong vao partition thoi_gian_id dang chay.
    """
    full_name = f"{CATALOG}.gold.{table_name}"
    schema_sql = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        PARTITIONED BY (thoi_gian_id)
        LOCATION 's3a://lakehouse/warehouse/gold/{table_name}'
    """)
    existing_columns = {field.name.lower() for field in spark.table(full_name).schema.fields}
    for field in df.schema.fields:
        if field.name.lower() not in existing_columns:
            spark.sql(
                f"ALTER TABLE {full_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}"
            )
    df.writeTo(full_name).overwrite(F.col("thoi_gian_id") == F.lit(thoi_gian_id))
    print(f"[+] gold.{table_name} <- REPLACE partition thoi_gian_id={thoi_gian_id}")


silver_application = spark.table(f"{CATALOG}.silver.application")
silver_history = spark.table(f"{CATALOG}.silver.application_history")
silver_agency = spark.table(f"{CATALOG}.silver.agency").select(F.col("id").alias("co_quan_id"))
silver_payment = spark.table(f"{CATALOG}.silver.payment")


def join_scd2_as_of(df, table_name, dimension_key, fact_key, as_of_ts, sk_column, attributes=()):
    """Gan version dimension co hieu luc tai thoi diem cua fact.

    `attributes` la tuple (ten cot dim, alias trong dataframe ket qua), vi du
    ("thoi_han_tra_kq", "sla_tai_thoi_diem_tiep_nhan").
    """
    lookup_key = f"_{table_name}_{dimension_key}"
    select_columns = [
        F.col(dimension_key).alias(lookup_key),
        F.col(sk_column),
        F.col("effective_from_ts"),
        F.col("effective_to_ts"),
    ]
    select_columns.extend(F.col(column).alias(alias) for column, alias in attributes)
    dimension = spark.table(f"{CATALOG}.gold.{table_name}").select(*select_columns)
    condition = (
        (F.col(fact_key) == F.col(lookup_key))
        & (as_of_ts >= F.col("effective_from_ts"))
        & ((F.col("effective_to_ts").isNull()) | (as_of_ts < F.col("effective_to_ts")))
    )
    return df.join(F.broadcast(dimension), condition, "left").drop(
        lookup_key, "effective_from_ts", "effective_to_ts"
    )

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
# ===========================================================================
w_latest_before_cutoff = Window.partitionBy("ho_so_id").orderBy(
    F.col("action_time").desc(), F.col("history_id").desc()
)

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

# Application chi giu cac thuoc tinh bat bien cua ho so (co quan, dich vu,
# nguoi nop, ngay tiep nhan). Theo nghiep vu, trang thai KHONG lay tu
# Application.Statusid: no luon la trang thai hien tai va se lam sai backfill.
# Trang thai tai cutoff chi lay tu Application_History. Neu mot ho so chua co
# history (loi/tre ingest), quy tac fallback la RECEIVED (1), khong phai
# Statusid hien tai cua Application.
#
# Neu event DELETE xay ra SAU cutoff, ho so van phai xuat hien trong snapshot
# ngay cu; neu DELETE xay ra truoc cutoff thi loai ra. Cac thuoc tinh con lai
# cua Application duoc xem la bat bien, nen current-state Silver van dung duoc
# cho mot ngay qua khu.
application_attributes = silver_application.filter(F.col("created_at") <= F.lit(cutoff_ts))
open_apps = (
    application_attributes
    .filter(
        (F.col("created_at") <= F.lit(cutoff_ts))
        & ((F.col("da_bi_xoa") == False) | (F.col("event_time") > F.lit(cutoff_ts)))  # noqa: E712
    )
    .join(latest_state_as_of_cutoff, on="ho_so_id", how="left")
    .withColumn(
        "trang_thai_id",
        F.coalesce(F.col("trang_thai_id_as_of"), F.lit(1)),
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

# Fact snapshot luu surrogate key SCD2. Co quan/trang thai/can bo duoc gan
# theo cutoff cua snapshot; SLA dich vu duoc gan tai luc tiep nhan ho so.
fact_ton_dong_raw = fact_ton_dong_raw.withColumn("_cutoff_ts", F.lit(cutoff_ts).cast("timestamp"))
fact_ton_dong_raw = join_scd2_as_of(
    fact_ton_dong_raw, "dim_co_quan", "co_quan_id", "co_quan_id", F.col("_cutoff_ts"), "dim_co_quan_sk"
)
fact_ton_dong_raw = join_scd2_as_of(
    fact_ton_dong_raw, "dim_trang_thai", "trang_thai_id", "trang_thai_id", F.col("_cutoff_ts"), "dim_trang_thai_sk"
)
fact_ton_dong_raw = join_scd2_as_of(
    fact_ton_dong_raw, "dim_can_bo", "can_bo_id", "can_bo_id", F.col("_cutoff_ts"), "dim_can_bo_sk"
)
fact_ton_dong_raw = join_scd2_as_of(
    fact_ton_dong_raw,
    "dim_dich_vu_cong",
    "dv_cong_id",
    "dv_cong_id",
    F.col("created_at"),
    "dim_dich_vu_cong_sk",
    (("thoi_han_tra_kq", "sla_tai_thoi_diem_tiep_nhan"),),
)

fact_ton_dong_ho_so = fact_ton_dong_raw.select(
    F.xxhash64(F.lit(thoi_gian_id), F.col("ho_so_id")).alias("id"),   # surrogate key BIGINT
    F.col("ho_so_id"), 
    F.col("trang_thai_id"),
    F.col("dim_trang_thai_sk"),
    F.col("co_quan_id"),
    F.col("dim_co_quan_sk"),
    F.col("can_bo_id"),
    F.col("dim_can_bo_sk"),
    F.col("dv_cong_id"),
    F.col("dim_dich_vu_cong_sk"),
    F.col("so_ngay_ton_dong_hien_tai"),
    F.col("tong_thoi_gian_da_xu_ly"),
    F.lit(1).alias("so_luong"),
    F.lit(thoi_gian_id).alias("thoi_gian_id"),
)

replace_gold_snapshot_partition(fact_ton_dong_ho_so, "fact_ton_dong_ho_so")

# cache() vi bang nay nho (so ho so dang mo), duoc TAI SU DUNG ngay ben duoi cho fact_van_hanh_co_quan.so_luong_ton_dong -> DAM BAO 2 fact luon khop so
fact_ton_dong_ho_so.cache()

# ===========================================================================
# 3. FACT_VAN_HANH_CO_QUAN (Aggregated Fact - 1 dong/1 ngay/1 co quan)
# ===========================================================================

# 3.1 So luong tiep nhan trong ngay
received_today = (
    application_attributes
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
#     created_at, so sanh voi Service.processing_time.
completed_today = (
    silver_history
    .filter((F.col("trang_thai_id") == STATUS_COMPLETED) & (F.to_date("action_time") == F.lit(str(snapshot_date))))
    .join(
        application_attributes.select("ho_so_id", "co_quan_id", "created_at", "dv_cong_id"),
        on="ho_so_id", how="inner",
    )
    .join(calendar_created, F.to_date("created_at") == F.col("created_date"), "left")
    .join(calendar_action, F.to_date("action_time") == F.col("action_date"), "left")
)
completed_today = join_scd2_as_of(
    completed_today,
    "dim_dich_vu_cong",
    "dv_cong_id",
    "dv_cong_id",
    F.col("created_at"),
    "dim_dich_vu_cong_sk",
    (("thoi_han_tra_kq", "sla_tai_thoi_diem_tiep_nhan"),),
)
completed_today = (
    completed_today
    .withColumn(
        "tong_ngay_lam_viec_xu_ly",
        F.greatest(F.col("action_workday_seq") - F.col("created_workday_seq"), F.lit(0)),
    )
    .withColumn(
        "tre_han",
        F.when(F.col("tong_ngay_lam_viec_xu_ly") > F.col("sla_tai_thoi_diem_tiep_nhan"), 1).otherwise(0),
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
    .join(
        F.broadcast(
            spark.table(f"{CATALOG}.gold.dim_dich_vu_cong").select(
                "dim_dich_vu_cong_sk", F.col("thoi_han_tra_kq").alias("sla_tai_thoi_diem_tiep_nhan")
            )
        ),
        on="dim_dich_vu_cong_sk",
        how="left",
    )
    .filter(F.col("tong_thoi_gian_da_xu_ly") > F.col("sla_tai_thoi_diem_tiep_nhan"))
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
    .join(application_attributes.select("ho_so_id", "co_quan_id"), on="ho_so_id", how="inner")
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
    .join(application_attributes.select("ho_so_id", "co_quan_id"), on="ho_so_id", how="inner")
    .groupBy("co_quan_id")
    .agg(F.sum("so_tien").alias("tong_chi_phi"))
)

# 3.6 Hop nhat: xuat phat tu TOAN BO danh sach co quan (silver.agency) de moi co quan deu co 1 dong/ngay du hom do khong phat sinh gi (tranh dashboard "thieu cot" gay hieu nham la khong co du lieu)

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

fact_van_hanh_co_quan = join_scd2_as_of(
    fact_van_hanh_co_quan.withColumn("_cutoff_ts", F.lit(cutoff_ts).cast("timestamp")),
    "dim_co_quan",
    "co_quan_id",
    "co_quan_id",
    F.col("_cutoff_ts"),
    "dim_co_quan_sk",
).drop("_cutoff_ts")

replace_gold_snapshot_partition(fact_van_hanh_co_quan, "fact_van_hanh_co_quan")

fact_ton_dong_ho_so.unpersist()
print("[+] Hoan tat Silver -> Gold.")
spark.stop()
