# ============================================================================
# JOB: Bronze -> Silver
# File : transform/spark-etl/bronze_to_silver.py
# Phu trach: Quan (DE)  |  Lich chay: dag_transform (Airflow), sau ingestion
#
# MUC DICH
#   Ho so cong duoc nap vao Bronze qua 2 kenh SONG SONG khong dong bo voi nhau:
#     (1) Kenh XML (Landing Zone -> job2) : bronze_dvc_xml.application_xml
#     (2) Kenh CDC (Debezium/Kafka -> job3): bronze_oltp_core.*_cdc
#   Job nay HOP NHAT ca 2 kenh thanh 1 ban ghi "single source of truth" duy
#   nhat cho tung thuc the (ho so, lich su xu ly, tai lieu, thanh toan...),
#   dong thoi lam sach danh muc (master data).
#
# CAC BANG SILVER TAO RA (namespace lakehouse.silver)
#   - application            : trang thai HIEN TAI cua tung ho so (1 dong/ho_so_id)
#   - application_history    : nhat ky xu ly, append-only (1 dong/hanh dong)
#   - document               : tai lieu dinh kem HIEN TAI cua ho so
#   - payment                : giao dich thanh toan (hop nhat kenh XML + API)
#   - applicant               : cong dan nop ho so (tu CDC)
#   - province/ward/status/service/agency/role/permission/document_type/
#     officer/officer_role    : ban sao lam sach cua 10 bang danh muc Bronze
#
# LUU Y QUAN TRONG VE DEDUP (day la diem de sai nhat neu lam vay)
#   Kenh XML co the gui UPDATE dang "PARTIAL_STATUS" - CHI mang theo 1 cot
#   Statusid, cac cot con lai (name, Applicantid...) la NULL. Neu dedup kieu
#   dropDuplicates(["ho_so_id"]) hoac lay row_number don gian theo thoi gian,
#   ta se VO TINH lay phai 1 dong PARTIAL va lam mat du lieu cac cot khac.
#   => Giai phap: "forward-fill" tung cot (last non-null value) theo thu tu
#   thoi gian TRUOC, sau do moi chon dong moi nhat. Da kiem chung bang du
#   lieu XML mau that (xem phan mo ta ket qua kem theo).
#
# THIET KE TRANH OOM
#   - Khong dung .collect()/.toPandas() tren du lieu lon
#   - repartition() theo khoa nghiep vu (ho_so_id) TRUOC khi lam Window de
#     moi executor chi xu ly du lieu cua 1 nhom ho_so_id, tranh 1 partition
#     phai gom qua nhieu du lieu (data skew) gay OOM
#   - AQE (Adaptive Query Execution) bat san: Spark tu dong gop/tach lai
#     partition va tu xu ly skew join khi runtime phat hien lech du lieu
#   - Chi select() dung cot can thiet ngay tu buoc doc Bronze (column pruning)
#
# Y NGHIA "OVERWRITE" O DAY
#   Voi quy mo du lieu hien tai (vai chuc - vai tram nghin dong), job nay
#   TINH LAI TOAN BO Silver tu Bronze moi lan chay (mode=overwrite) de dam
#   bao idempotent (chay lai bao nhieu lan cung ra 1 ket qua). Khi du lieu
#   len den muc hang trieu/ty dong, nen chuyen sang MERGE INTO ... WHEN
#   MATCHED/NOT MATCHED chi voi phan Bronze moi nap theo watermark
#   (ingested_at > lan chay truoc) de tranh quet lai toan bo lich su.
# ============================================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType, TimestampType

# ---------------------------------------------------------------------------
# 1. SPARK SESSION - dong bo config voi ingestion/job1..job4 (Iceberg + MinIO)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"
CATALOG = "lakehouse"

spark = (
    SparkSession.builder
    .appName("Transform_BronzeToSilver")
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
    # --- Chong OOM: AQE tu dong gop/tach partition + xu ly skew join ---
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")

N_PART = 32  # so partition muc tieu khi repartition theo khoa nghiep vu (ho_so_id)


def save_silver(df, table_name, partition_cols=None):
    """Ham dung chung: tao bang Iceberg neu chua co (theo dung mau CREATE TABLE
    IF NOT EXISTS ma job1/job2/job4 dang dung de dong bo phong cach code trong
    repo), roi ghi de (overwrite) toan bo du lieu.
    """
    full_name = f"{CATALOG}.silver.{table_name}"
    schema_sql = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    part_clause = f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        {part_clause}
        LOCATION 's3a://lakehouse/warehouse/silver/{table_name}'
    """)
    df.write.format("iceberg").mode("overwrite").save(full_name)
    print(f"[+] silver.{table_name} <- da ghi xong")


def read_optional_bronze(table_name, schema):
    """Doc nguon Bronze neu da duoc ingest, nguoc lai tra DataFrame rong.

    XML va API la hai kenh batch co the chua co file trong mot lan demo. Silver
    van phai chay duoc voi CDC, thay vi fail ngay khi bang Bronze chua duoc tao.
    Schema rong giu unionByName va schema Gold on dinh.
    """
    if spark.catalog.tableExists(table_name):
        return spark.table(table_name)
    print(f"[!] Khong tim thay {table_name}; su dung DataFrame rong cho lan chay nay.")
    return spark.createDataFrame([], schema)


BRONZE_XML_TABLE = f"{CATALOG}.bronze_dvc_xml.application_xml"
BRONZE_API_TABLE = f"{CATALOG}.bronze_api.payment_transactions"

bronze_xml = read_optional_bronze(
    BRONZE_XML_TABLE,
    StructType([
        StructField("ma_goi_tin", StringType()),
        StructField("ma_du_lieu", StringType()),
        StructField("loai_du_lieu", StringType()),
        StructField("ngay_cap_nhat", StringType()),
        StructField("su_kien", StringType()),
        StructField("id_ban_ghi", StringType()),
        StructField("data_payload", StringType()),
        StructField("file_name", StringType()),
        StructField("ingested_at", TimestampType()),
    ]),
)
bronze_api_payments = read_optional_bronze(
    BRONZE_API_TABLE,
    StructType([
        StructField("id_ban_ghi", StringType()),
        StructField("payment_status", StringType()),
        StructField("tax_code", StringType()),
        StructField("amount", DoubleType()),
        StructField("method", StringType()),
        StructField("timestamp", StringType()),
        StructField("file_name", StringType()),
        StructField("ingested_at", TimestampType()),
    ]),
)


# ---------------------------------------------------------------------------
# 2. DANH MUC (MASTER DATA) - 10 bang, chi lam sach/chuan hoa
#    (Bronze da duoc job1 OVERWRITE toan bo moi lan chay nen khong co van
#    de "ban ghi cu/moi" can dedup theo thoi gian o day)
# ---------------------------------------------------------------------------
MASTER_TABLES = [
    "province", "ward", "status", "service", "agency",
    "role", "permission", "document_type", "officer", "officer_role",
]

for t in MASTER_TABLES:
    df = spark.table(f"{CATALOG}.bronze_master_data.{t}")
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    drop_cols = [c for c in ("ingested_at", "file_name") if c in df.columns]
    if drop_cols:
        df = df.drop(*drop_cols)
    df = df.dropDuplicates()
    save_silver(df, t)


# ---------------------------------------------------------------------------
# 3. SILVER.APPLICATION - hop nhat kenh XML (packet) + kenh CDC (Debezium)
#    Xem ghi chu forward-fill o dau file.
# ---------------------------------------------------------------------------
app_payload_schema = StructType([
    StructField("name", StringType()),
    StructField("Applicantid", StringType()),
    StructField("Serviceid", StringType()),
    StructField("Agencyid", StringType()),
    StructField("created_at", StringType()),
    StructField("Statusid", StringType()),
])

# 3.1 Kenh XML: moi packet la 1 "quan sat" tai 1 thoi diem (co the la FULL
#     hoac chi PARTIAL_STATUS), ngay_cap_nhat la moc thoi gian cua quan sat do
xml_app_events = (
    bronze_xml
    .withColumn("p", F.from_json("data_payload", app_payload_schema))
    .select(
        F.col("id_ban_ghi").alias("ho_so_id"),
        F.col("p.name").alias("ten_ho_so"),
        F.col("p.Applicantid").alias("applicant_id"),
        F.col("p.Serviceid").cast("int").alias("dv_cong_id"),
        F.col("p.Agencyid").cast("int").alias("co_quan_id"),
        F.to_timestamp("p.created_at").alias("created_at"),
        F.col("p.Statusid").cast("int").alias("trang_thai_id"),
        # ngay_cap_nhat la event time nghiep vu. Fallback ingested_at giu cho
        # window co thu tu xac dinh neu packet XML bi thieu/loi timestamp.
        F.coalesce(F.to_timestamp("ngay_cap_nhat"), F.col("ingested_at")).alias("event_time"),
        (F.col("su_kien") == F.lit("DELETE")).alias("is_deleted_event"),
        F.lit(1).alias("source_priority"),  # CDC thang XML neu trung event_time
    )
)

# 3.2 Kenh CDC: da co cau truc san, moi dong la 1 quan sat tai thoi diem updated_at
cdc_app_events = (
    spark.table(f"{CATALOG}.bronze_oltp_core.application_cdc")
    .select(
        F.col("id").alias("ho_so_id"),
        F.col("name").alias("ten_ho_so"),
        F.col("Applicantid").alias("applicant_id"),
        F.col("Serviceid").alias("dv_cong_id"),
        F.col("Agencyid").alias("co_quan_id"),
        F.col("created_at"),
        F.col("Statusid").alias("trang_thai_id"),
        F.coalesce(F.col("updated_at"), F.col("ingested_at")).alias("event_time"),
        F.lit(False).alias("is_deleted_event"),
        F.lit(2).alias("source_priority"),
    )
)

application_events = (
    xml_app_events.unionByName(cdc_app_events)
    .repartition(N_PART, "ho_so_id")   # chong data skew truoc khi Window
)

w_forward = (
    Window.partitionBy("ho_so_id").orderBy("event_time", "source_priority")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
for c in ["ten_ho_so", "applicant_id", "dv_cong_id", "co_quan_id", "created_at", "trang_thai_id"]:
    application_events = application_events.withColumn(c, F.last(c, ignorenulls=True).over(w_forward))

w_latest_app = Window.partitionBy("ho_so_id").orderBy(
    F.col("event_time").desc(), F.col("source_priority").desc()
)
silver_application = (
    application_events
    .withColumn("rn", F.row_number().over(w_latest_app))
    .filter("rn = 1")
    .drop("rn")
    .drop("source_priority")
    .withColumnRenamed("is_deleted_event", "da_bi_xoa")
    .withColumn("cap_nhat_luc", F.current_timestamp())
)

save_silver(silver_application, "application")


# ---------------------------------------------------------------------------
# 4. SILVER.APPLICATION_HISTORY - nhat ky xu ly, append-only, bat bien
#    Grain = 1 dong = 1 hanh dong xu ly = 1 ban ghi Application_History goc.
#    Day la nguon truc tiep cho fact_xu_ly_ho_so (streaming, StarRocks) va
#    cho can_bo_id "dang giu" ho so trong fact_ton_dong_ho_so (ben duoi).
# ---------------------------------------------------------------------------
history_array_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("Statusid", StringType()),
    StructField("Statusid2", StringType()),
    StructField("Officerid", StringType()),
    StructField("action_time", StringType()),
]))

xml_history = (
    bronze_xml
    .withColumn(
        "p",
        F.from_json("data_payload", StructType([
            StructField("application_history_array", history_array_schema)
        ])),
    )
    .filter(F.col("p.application_history_array").isNotNull())
    .select(F.col("id_ban_ghi").alias("ho_so_id"), F.explode("p.application_history_array").alias("h"))
    .select(
        F.col("h.id").alias("history_id"),
        F.col("ho_so_id"),
        F.col("h.Statusid").cast("int").alias("trang_thai_truoc_id"),
        F.col("h.Statusid2").cast("int").alias("trang_thai_id"),
        F.col("h.Officerid").cast("int").alias("can_bo_id"),
        F.to_timestamp("h.action_time").alias("action_time"),
        F.lit(None).cast("string").alias("note"),
    )
)

cdc_history = (
    spark.table(f"{CATALOG}.bronze_oltp_core.application_history_cdc")
    .select(
        F.col("id").alias("history_id"),
        F.col("Applicationid").alias("ho_so_id"),
        F.col("Statusid").alias("trang_thai_truoc_id"),
        F.col("Statusid2").alias("trang_thai_id"),
        F.col("Officerid").alias("can_bo_id"),
        F.col("action_time"),
        F.col("note"),
    )
)

# Khu trung theo history_id: cung 1 su kien co the "nhin thay" qua ca 2 kenh
# trong moi truong demo nay (XML resend + CDC). Ly thuyet 2 ban phai giong
# nhau, chi giu 1 ban de tranh nhan doi khi tinh trung binh/dem so luong.
w_hist = Window.partitionBy("history_id").orderBy(F.col("action_time").desc())
silver_application_history = (
    xml_history.unionByName(cdc_history)
    .repartition(N_PART, "ho_so_id")
    .withColumn("rn", F.row_number().over(w_hist))
    .filter("rn = 1")
    .drop("rn")
    # -1 = Unknown, dung theo dung luu y trong data-dictionary.md de tranh
    # JOIN rong khi ho so moi RECEIVED chua duoc phan cong can bo
    .withColumn("can_bo_id", F.coalesce(F.col("can_bo_id"), F.lit(-1)))
)

save_silver(silver_application_history, "application_history")


# ---------------------------------------------------------------------------
# 5. SILVER.DOCUMENT - tai lieu dinh kem HIEN TAI (last write wins theo id)
# ---------------------------------------------------------------------------
doc_item_schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("file_url", StringType()),
    StructField("Document_Typeid", StringType()),
])

xml_docs = (
    bronze_xml
    .withColumn(
        "p",
        F.from_json(
            "data_payload",
            StructType([StructField("document_array", ArrayType(doc_item_schema))]),
        ),
    )
    .filter(F.col("p.document_array").isNotNull())
    .select(F.col("id_ban_ghi").alias("ho_so_id"), F.to_timestamp("ngay_cap_nhat").alias("event_time"),
            F.explode("p.document_array").alias("d"))
    .select(
        F.col("d.id").alias("document_id"),
        F.col("ho_so_id"),
        F.col("d.name").alias("ten_tai_lieu"),
        F.col("d.file_url"),
        F.col("d.Document_Typeid").cast("int").alias("loai_tai_lieu_id"),
        F.col("event_time"),
    )
)

cdc_docs = (
    spark.table(f"{CATALOG}.bronze_oltp_core.document_cdc")
    .select(
        F.col("id").alias("document_id"),
        F.col("Applicationid").alias("ho_so_id"),
        F.col("name").alias("ten_tai_lieu"),
        F.col("file_url"),
        F.col("Document_Typeid").alias("loai_tai_lieu_id"),
        F.col("ingested_at").alias("event_time"),
    )
)

w_doc = Window.partitionBy("document_id").orderBy(F.col("event_time").desc())
silver_document = (
    xml_docs.unionByName(cdc_docs)
    .repartition(N_PART, "ho_so_id")
    .withColumn("rn", F.row_number().over(w_doc))
    .filter("rn = 1")
    .drop("rn")
)

save_silver(silver_document, "document")


# ---------------------------------------------------------------------------
# 6. SILVER.PAYMENT - hop nhat kenh XML (payment_array) + kenh API thanh toan
#    GIA DINH CAN LUU Y: day la 2 KENH GHI NHAN THANH TOAN KHAC NHAU
#    (XML export tu he thong mot cua, va API tu cong thanh toan/thue). Vi
#    khong co khoa chung tin cay giua 2 nguon (XML co transaction_code, API
#    co tax_code, khong giao nhau), o day tam UNION ca 2 va gan nhan nguon
#    du lieu de doi soat. Neu ve sau xac dinh duoc day la 1 nguon trung nhau
#    thi can bo sung logic dedup rieng - can doi chieu voi doi nghiep vu.
# ---------------------------------------------------------------------------
payment_item_schema = StructType([
    StructField("id", StringType()),
    StructField("amount", StringType()),
    StructField("method", StringType()),
    StructField("status", StringType()),
    StructField("transaction_code", StringType()),
    StructField("paid_at", StringType()),
])

xml_payments = (
    bronze_xml
    .withColumn(
        "p",
        F.from_json(
            "data_payload",
            StructType([StructField("payment_array", ArrayType(payment_item_schema))]),
        ),
    )
    .filter(F.col("p.payment_array").isNotNull())
    .select(F.col("id_ban_ghi").alias("ho_so_id"), F.explode("p.payment_array").alias("pm"))
    .select(
        F.col("pm.transaction_code").alias("ma_giao_dich"),
        F.col("ho_so_id"),
        F.col("pm.amount").cast("double").alias("so_tien"),
        F.col("pm.method").alias("phuong_thuc"),
        F.col("pm.status").alias("trang_thai_tt"),
        F.to_timestamp("pm.paid_at").alias("thoi_gian_tt"),
        F.lit("XML_MOT_CUA").alias("nguon_du_lieu"),
    )
)

api_payments = (
    bronze_api_payments
    .select(
        F.concat(F.lit("API_"), F.col("id_ban_ghi"), F.lit("_"), F.col("timestamp")).alias("ma_giao_dich"),
        F.col("id_ban_ghi").alias("ho_so_id"),
        F.col("amount").alias("so_tien"),
        F.col("method").alias("phuong_thuc"),
        F.col("payment_status").alias("trang_thai_tt"),
        F.to_timestamp("timestamp").alias("thoi_gian_tt"),  # STRING trong Bronze -> can to_timestamp
        F.lit("API_THANH_TOAN").alias("nguon_du_lieu"),
    )
)

silver_payment = xml_payments.unionByName(api_payments).dropDuplicates(["ma_giao_dich"])
save_silver(silver_payment, "payment")


# ---------------------------------------------------------------------------
# 7. SILVER.APPLICANT - cong dan nop ho so (chi co kenh CDC)
# ---------------------------------------------------------------------------
applicant_events = spark.table(f"{CATALOG}.bronze_oltp_core.applicant_cdc")
w_applicant = Window.partitionBy("id").orderBy(F.col("updated_at").desc())
silver_applicant = (
    applicant_events
    .withColumn("rn", F.row_number().over(w_applicant))
    .filter("rn = 1")
    .drop("rn", "ingested_at")
)
save_silver(silver_applicant, "applicant")

print("[+] Hoan tat Bronze -> Silver.")
spark.stop()
