# ============================================================================
# JOB: Build 5 bang DIM (dim_thoi_gian, dim_co_quan, dim_trang_thai,
#      dim_can_bo, dim_dich_vu_cong)
# File : transform/spark-agg/build_dim_tables.py
# Phu trach: Quan (DE)
#
# KHI NAO CHAY JOB NAY
#   KHONG nam trong lich Airflow hang ngay. Day la du lieu danh muc RAT IT
#   thay doi (danh sach co quan, trang thai, dich vu cong, can bo...), nen
#   nhom chu dong chay THU CONG job nay 1 lan luc khoi tao he thong, va chay
#   lai (idempotent - chay bao nhieu lan cung ra ket qua dung) MOI KHI danh
#   muc co thay doi (them co quan moi, doi SLA dich vu...).
#
# GHI RA 2 NOI trong CUNG 1 lan chay
#   (1) lakehouse.gold.dim_* (Iceberg, qua Hive Metastore) - de Trino truy van.
#   (2) StarRocks gold_realtime.dim_* (qua JDBC/MySQL protocol) - de bang
#       Materialized View fact_xu_ly_ho_so (real-time) JOIN vao lay
#       thoi_han_tra_kq, ten co quan... Cac bang dim ben StarRocks dung
#       PRIMARY KEY model nen INSERT lai (mode=append qua JDBC) se TU DONG
#       UPSERT (REPLACE) theo khoa chinh, khong tao du lieu trung khi chay
#       lai job nhieu lan.
#
# YEU CAU TRUOC KHI CHAY LAN DAU
#   Da chay transform/starrocks/ddl_realtime.sql (tao database gold_realtime
#   va cac bang dim/ods/MV ben StarRocks) va da co bang lakehouse.gold.dim_*
#   (transform/warehouse/ddl/gold_dim_fact.sql, da co san boi Trung).
# ============================================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"
CATALOG = "lakehouse"

# MySQL Connector/J mac dinh gui tung gia tri cua batch thanh INSERT rieng le.
# rewriteBatchedStatements gop thanh multi-value INSERT, tranh tao hang nghin
# version tren Primary Key tablet cua StarRocks khi nap dim_thoi_gian.
STARROCKS_JDBC_URL = (
    "jdbc:mysql://starrocks:9030/gold_realtime"
    "?rewriteBatchedStatements=true&useServerPrepStmts=false"
)
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""

spark = (
    SparkSession.builder
    .appName("Build_Dim_Tables")
    # Driver JDBC da duoc DockerFile cai san. Tranh phu thuoc Maven runtime
    # khi cluster khong co Internet; StarRocks dung MySQL wire protocol.
    .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar")
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
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")


def save_dim(df, table_name):
    """Ghi 1 dim vao ca Iceberg gold (Trino) va StarRocks gold_realtime (MV realtime)."""
    full_name = f"{CATALOG}.gold.{table_name}"
    schema_sql = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        LOCATION 's3a://lakehouse/warehouse/gold/{table_name}'
    """)
    # Iceberg does not infer newly added dimension fields from CREATE TABLE IF
    # NOT EXISTS. Evolve the existing local schema before the overwrite so an
    # environment created with the old calendar DDL can receive ngay_date and
    # stt_ngay_lam_viec without dropping its table.
    existing_columns = {field.name.lower() for field in spark.table(full_name).schema.fields}
    for field in df.schema.fields:
        if field.name.lower() not in existing_columns:
            spark.sql(
                f"ALTER TABLE {full_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}"
            )
    df.write.format("iceberg").mode("overwrite").save(full_name)

    # Dimension chi co it dong. Gom mot partition va ghi theo batch de tranh
    # hang tram JDBC connection/commit nho vao StarRocks (dac biet dim_time).
    # Primary Key model van upsert idempotent khi job duoc chay lai.
    (
        df.coalesce(1).write.format("jdbc")
        .option("url", STARROCKS_JDBC_URL)
        .option("dbtable", table_name)
        .option("user", STARROCKS_USER)
        .option("password", STARROCKS_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", "1000")
        .mode("append")   # bang dim ben StarRocks la PRIMARY KEY -> INSERT trung khoa se tu UPSERT
        .save()
    )
    print(f"[+] dim.{table_name} <- da ghi vao Iceberg (gold.{table_name}) va StarRocks (gold_realtime.{table_name})")


# ---------------------------------------------------------------------------
# 1. DIM_THOI_GIAN - date spine sinh bang Spark (khong can bang nguon)
#    co_phai_la_ngay_nghi = Thu 7/CN HOAC nam trong danh sach ngay le co dinh
#    (GIA DINH DON GIAN HOA: chi liet ke cac ngay le duong lich co dinh; CHUA
#    tinh ngay le AM LICH (Tet Nguyen Dan...) vi ngay am lich doi moi nam,
#    can bang tra cuu rieng - de nghi bo sung sau boi nguoi phu trach nghiep
#    vu neu can chinh xac tuyet doi cho KPI "thoi_gian_xu_ly" loai tru ngay nghi.
# ---------------------------------------------------------------------------
FIXED_HOLIDAYS_MMDD = {"01-01", "04-30", "05-01", "09-02"}  # Tet Duong, 30/4, 1/5, Quoc Khanh

date_spine = (
    # Pham vi demo: du de bao phu du lieu mau va dashboard gan hien tai,
    # nhung khong tao dimension xa hon nhu mot he thong production.
    spark.sql("SELECT explode(sequence(to_date('2023-01-01'), to_date('2028-12-31'), interval 1 day)) AS ngay_dt")
    .withColumn("thoi_gian_id", F.date_format("ngay_dt", "yyyyMMdd").cast("int"))
    .withColumn("ngay", F.dayofmonth("ngay_dt"))
    .withColumn("thang", F.month("ngay_dt"))
    .withColumn("quy", F.quarter("ngay_dt"))
    .withColumn("nam", F.year("ngay_dt"))
    .withColumn("thu_trong_tuan", F.dayofweek("ngay_dt"))  # 1=CN, 7=Thu7 (Spark convention)
    .withColumn("mmdd", F.date_format("ngay_dt", "MM-dd"))
    .withColumn(
        "co_phai_la_ngay_nghi",
        (F.col("thu_trong_tuan").isin(1, 7)) | (F.col("mmdd").isin(*FIXED_HOLIDAYS_MMDD)),
    )
    # Cumulative workday sequence lets Gold calculate elapsed business days
    # with two small dimension joins instead of an expensive date-range join.
    .withColumn(
        "stt_ngay_lam_viec",
        F.sum(F.when(~F.col("co_phai_la_ngay_nghi"), F.lit(1)).otherwise(F.lit(0))).over(
            Window.orderBy("ngay_dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
    .select(
        "thoi_gian_id",
        F.col("ngay_dt").alias("ngay_date"),
        "ngay",
        "thang",
        "quy",
        "nam",
        "co_phai_la_ngay_nghi",
        "stt_ngay_lam_viec",
    )
)
save_dim(date_spine, "dim_thoi_gian")


# ---------------------------------------------------------------------------
# 2. DIM_CO_QUAN - Agency + resolve ten Tinh/Phuong
# ---------------------------------------------------------------------------
silver_agency = spark.table(f"{CATALOG}.silver.agency")
silver_province = spark.table(f"{CATALOG}.silver.province").select(
    F.col("id").alias("Provinceid"), F.col("name").alias("tinh")
)
silver_ward = spark.table(f"{CATALOG}.silver.ward").select(
    F.col("id").alias("Wardid"), F.col("name").alias("phuong")
)

dim_co_quan = (
    silver_agency
    .join(F.broadcast(silver_province), "Provinceid", "left")
    .join(F.broadcast(silver_ward), "Wardid", "left")
    .select(
        F.col("id").alias("co_quan_id"),
        F.col("name").alias("ten"),
        F.col("tinh"),
        F.col("phuong"),
    )
)
save_dim(dim_co_quan, "dim_co_quan")


# ---------------------------------------------------------------------------
# 3. DIM_TRANG_THAI - Status
# ---------------------------------------------------------------------------
dim_trang_thai = (
    spark.table(f"{CATALOG}.silver.status")
    .select(
        F.col("id").alias("trang_thai_id"),
        F.col("code").alias("ma_trang_thai"),
        F.col("name").alias("ten_trang_thai"),
    )
)
save_dim(dim_trang_thai, "dim_trang_thai")


# ---------------------------------------------------------------------------
# 4. DIM_CAN_BO - Officer + Role (qua Officer_Role), + 1 dong "Unknown" (-1)
#    De tranh JOIN rong khi ho so moi RECEIVED chua duoc phan cong can bo
#    (theo dung luu y trong data-dictionary.md)
# ---------------------------------------------------------------------------
silver_officer = spark.table(f"{CATALOG}.silver.officer")
silver_officer_role = spark.table(f"{CATALOG}.silver.officer_role")
silver_role = spark.table(f"{CATALOG}.silver.role").select(F.col("id").alias("Roleid"), F.col("name").alias("vi_tri"))

# 1 can bo co the co nhieu vai tro trong Officer_Role (n-n) -> lay 1 vai tro
# dai dien (vai tro dau tien theo Roleid) de dim_can_bo giu dung grain
# "1 dong/1 can bo" nhu DDL da dinh nghia.
from pyspark.sql import Window  # noqa: E402 - import cuc bo cho ro muc dich dung

w_role = Window.partitionBy("Officerid").orderBy("Roleid")
officer_role_primary = (
    silver_officer_role
    .withColumn("rn", F.row_number().over(w_role))
    .filter("rn = 1")
    .select("Officerid", "Roleid")
)

dim_can_bo_real = (
    silver_officer
    .join(officer_role_primary, silver_officer.id == officer_role_primary.Officerid, "left")
    .join(F.broadcast(silver_role), "Roleid", "left")
    .select(
        F.col("id").alias("can_bo_id"),
        F.col("name").alias("ten"),
        F.coalesce(F.col("vi_tri"), F.lit("Chua xac dinh vai tro")).alias("vi_tri"),
    )
)
dim_can_bo_unknown = spark.createDataFrame(
    [(-1, "Khong xac dinh", "N/A")], schema="can_bo_id int, ten string, vi_tri string"
)
dim_can_bo = dim_can_bo_real.unionByName(dim_can_bo_unknown)
save_dim(dim_can_bo, "dim_can_bo")


# ---------------------------------------------------------------------------
# 5. DIM_DICH_VU_CONG - Service (processing_time -> thoi_han_tra_kq)
# ---------------------------------------------------------------------------
dim_dich_vu_cong = (
    spark.table(f"{CATALOG}.silver.service")
    .select(
        F.col("id").alias("dv_cong_id"),
        F.col("name").alias("ten"),
        F.col("processing_time").alias("thoi_han_tra_kq"),
    )
)
save_dim(dim_dich_vu_cong, "dim_dich_vu_cong")

print("[+] Hoan tat build 5 bang dim (Iceberg + StarRocks).")
spark.stop()
