# GHI RA 2 NOI trong CUNG 1 lan chay
#   (1) lakehouse.gold.dim_* (Iceberg, qua Hive Metastore) - de Trino truy van.
#   (2) StarRocks gold_realtime.dim_* (qua JDBC/MySQL protocol) - ban sao
#       current-state nho, san sang cho truy van ket hop voi realtime fact.
#       Cac bang dim ben StarRocks dung
#       PRIMARY KEY model nen INSERT lai (mode=append qua JDBC) se TU DONG
#       UPSERT (REPLACE) theo khoa chinh, khong tao du lieu trung khi chay
#       lai job nhieu lan.
#
# YEU CAU TRUOC KHI CHAY LAN DAU
#   Da chay transform/starrocks/ddl_realtime.sql va co cac bang Silver nguon.
# ============================================================================

import os
from datetime import date, datetime, timedelta, timezone
from functools import reduce

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
SCD_BASELINE_TS = "2023-01-01 00:00:00"

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


def save_dim(df, table_name, key_cols):
    """Ghi dimension vao Iceberg Gold va StarRocks current-state mirror."""
    full_name = f"{CATALOG}.gold.{table_name}"
    schema_sql = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        LOCATION 's3a://lakehouse/warehouse/gold/{table_name}'
    """)
    # Iceberg does not infer newly added dimension fields from CREATE TABLE IF
    # NOT EXISTS. Evolve the existing local schema before the upsert so an
    # environment created with the old calendar DDL can receive ngay_date and
    # stt_ngay_lam_viec without dropping its table.
    existing_columns = {field.name.lower() for field in spark.table(full_name).schema.fields}
    for field in df.schema.fields:
        if field.name.lower() not in existing_columns:
            spark.sql(
                f"ALTER TABLE {full_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}"
            )
    source_view = f"_gold_{table_name}_source"
    df.createOrReplaceTempView(source_view)
    match = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in key_cols)
    assignments = ", ".join(f"t.`{field.name}` = s.`{field.name}`" for field in df.schema.fields)
    columns = ", ".join(f"`{field.name}`" for field in df.schema.fields)
    values = ", ".join(f"s.`{field.name}`" for field in df.schema.fields)
    spark.sql(f"""
        MERGE INTO {full_name} t
        USING {source_view} s
        ON {match}
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
    """)
    spark.catalog.dropTempView(source_view)

    # Dimension chi co it dong. Gom mot partition va ghi theo batch de tranh
    # hang tram JDBC connection/commit nho vao StarRocks (dac biet dim_time).
    # Primary Key model van upsert idempotent khi job duoc chay lai.
    try:
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
    except Exception as e:
        print(f"[!] dim.{table_name} <- da ghi vao Iceberg (gold.{table_name}), nhung StarRocks JDBC bo qua: {e}")


def write_current_dim_to_starrocks(df, table_name):
    """StarRocks realtime chi can Type 1/current-state de MV join nhanh."""
    try:
        (
            df.coalesce(1).write.format("jdbc")
            .option("url", STARROCKS_JDBC_URL)
            .option("dbtable", table_name)
            .option("user", STARROCKS_USER)
            .option("password", STARROCKS_PASSWORD)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("batchsize", "1000")
            .mode("append")
            .save()
        )
    except Exception as e:
        print(f"[!] StarRocks current dim {table_name} JDBC write skipped: {e}")



def save_dim_scd2(df, table_name, business_key, tracked_columns):
    """Luu SCD Type 2 trong Iceberg Gold va mirror Type 1 vao StarRocks.

    `df` la full snapshot danh muc hien tai tu Silver. Moi thay doi cua cac
    tracked column dong version current cu va tao version moi. `source_snapshot`
    duoc giu tu Bronze master; do do effective_from la luc snapshot duoc ingest,
    khong phai luc job dimension tinh lai.
    """
    sk_column = f"{table_name}_sk"
    source_meta = {"source_snapshot_at", "source_snapshot_id"}
    base_columns = [column for column in df.columns if column not in source_meta]
    full_name = f"{CATALOG}.gold.{table_name}"

    # Day la full snapshot append tu Bronze. Loai dong trung hoan toan truoc
    # khi tinh hash; mot business key lap lai voi gia tri khac trong CUNG
    # snapshot la loi nguon, khong duoc dropDuplicates([key]) mot cach tuy y.
    snapshot = df.dropDuplicates()
    conflicting_keys = snapshot.groupBy(business_key).count().filter(F.col("count") > 1)
    if conflicting_keys.limit(1).count() > 0:
        examples = [row[business_key] for row in conflicting_keys.limit(10).collect()]
        raise ValueError(
            f"silver snapshot {table_name} co business key trung voi noi dung khac: {examples}"
        )

    source = (
        snapshot
        .withColumn("effective_from_ts", F.coalesce(F.col("source_snapshot_at"), F.current_timestamp()))
        .withColumn(
            "source_snapshot_id",
            F.coalesce(
                F.col("source_snapshot_id"),
                F.date_format(F.col("effective_from_ts"), "yyyyMMddHHmmssSSSSSS"),
            ),
        )
        .withColumn(sk_column, F.xxhash64(F.col(business_key), F.col("source_snapshot_id")))
        .withColumn("effective_to_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .select(
            sk_column, *base_columns, "effective_from_ts", "effective_to_ts",
            "is_current", "source_snapshot_id",
        )
    )
    output_columns = source.columns
    schema_sql = ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in source.schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        LOCATION 's3a://lakehouse/warehouse/gold/{table_name}'
    """)

    # Evolve Type 1 tables created by previous versions of the demo.
    existing_columns = {field.name.lower() for field in spark.table(full_name).schema.fields}
    for field in source.schema.fields:
        if field.name.lower() not in existing_columns:
            spark.sql(f"ALTER TABLE {full_name} ADD COLUMN `{field.name}` {field.dataType.simpleString()}")

    # `record_hash` thuoc phien ban SCD2 cu. Khong can hash cho cac dim nho:
    # so sanh null-safe truc tiep cac tracked column se minh bach hon va khong
    # co rui ro collision. Drop de schema Gold khong con cot chet sau migration.
    if "record_hash" in existing_columns:
        spark.sql(f"ALTER TABLE {full_name} DROP COLUMN record_hash")

    target = spark.table(full_name)
    # Lan dau chuyen tu Type 1 sang Type 2 khong co moc thay doi lich su. Dat
    # version baseline tu dau lich su demo de fact tu 2023 join duoc; cac
    # thay doi phat hien o snapshot sau dung source_snapshot_at thuc te.
    is_initial_scd_load = target.filter(F.col("effective_from_ts").isNotNull()).limit(1).count() == 0
    if is_initial_scd_load:
        source = source.withColumn("effective_from_ts", F.lit(SCD_BASELINE_TS).cast("timestamp"))
        target = spark.createDataFrame([], source.schema)
    else:
        target = (
            target
            .withColumn("effective_from_ts", F.coalesce(F.col("effective_from_ts"), F.current_timestamp()))
            .withColumn("effective_to_ts", F.col("effective_to_ts").cast("timestamp"))
            .withColumn("is_current", F.coalesce(F.col("is_current"), F.lit(True)))
            .withColumn("source_snapshot_id", F.coalesce(F.col("source_snapshot_id"), F.lit("legacy")))
            .withColumn(
                sk_column,
                F.coalesce(F.col(sk_column), F.xxhash64(F.col(business_key), F.col("source_snapshot_id"))),
            )
            .select(*output_columns)
        )
        # Dimension la bang nho. Tach scan Iceberg thanh DataFrame noi bo
        # truoc khi loc is_current/ghi de cung bang. Spark 3.5 co assertion
        # loi khi push predicate boolean vao V2 Iceberg scan trong read-write
        # plan cua cung mot bang.
        target = spark.createDataFrame(target.collect(), schema=target.schema)

    current = target.filter(F.col("is_current"))
    historical = target.filter(~F.col("is_current"))

    # SCD2 change detection phai chi dua tren cac thuoc tinh nghiep vu duoc
    # khai bao trong tracked_columns. Khong so sanh snapshot/audit metadata vi
    # chung doi moi lan ingest va se tao version gia. eqNullSafe (<=>) xem
    # NULL = NULL, dung cho ca string va cac truong SLA nullable.
    def tracked_values_differ(left_alias, right_alias):
        comparisons = [
            ~F.col(f"{left_alias}.`{column}`").eqNullSafe(
                F.col(f"{right_alias}.`{column}`")
            )
            for column in tracked_columns
        ]
        return reduce(lambda left, right: left | right, comparisons, F.lit(False))

    source_ref = source.select(
        business_key,
        *tracked_columns,
        F.col("effective_from_ts").alias("_new_effective_from"),
    )
    current_ref = current.select(business_key, *tracked_columns)
    current_source_condition = (
        F.col(f"t.`{business_key}`") == F.col(f"s.`{business_key}`")
    )

    unchanged = (
        current.alias("t")
        .join(source.alias("s"), current_source_condition, "inner")
        .filter(~tracked_values_differ("t", "s"))
        .select(*[F.col(f"t.`{column}`").alias(column) for column in output_columns])
    )
    expired = (
        current.alias("t")
        .join(source_ref.alias("s"), current_source_condition, "left")
        .filter(
            F.col(f"s.`{business_key}`").isNull()
            | tracked_values_differ("t", "s")
        )
        .select(
            *[F.col(f"t.`{column}`").alias(column) for column in output_columns if column not in {"effective_to_ts", "is_current"}],
            F.coalesce(F.col("s._new_effective_from"), F.current_timestamp()).alias("effective_to_ts"),
            F.lit(False).alias("is_current"),
        )
        .select(*output_columns)
    )
    new_current = (
        source.alias("s")
        .join(current_ref.alias("t"), current_source_condition, "left")
        .filter(
            F.col(f"t.`{business_key}`").isNull()
            | tracked_values_differ("s", "t")
        )
        .select(*[F.col(f"s.`{column}`").alias(column) for column in output_columns])
    )

    # Dimension nho: rebuild toan bo SCD table tu historical + current state
    # trong mot Iceberg overwrite atomic, tranh trang thai nua dong/nua mo.
    scd_result = historical.unionByName(expired).unionByName(unchanged).unionByName(new_current)
    scd_result.writeTo(full_name).overwrite(F.lit(True))

    # Realtime khong mang lich su dim: chi nap snapshot Type 1 hien tai.
    write_current_dim_to_starrocks(source.select(*base_columns), table_name)
    print(f"[+] gold.{table_name} <- SCD2 Iceberg; gold_realtime.{table_name} <- Type1 current")


# ---------------------------------------------------------------------------
# 1. DIM_THOI_GIAN - date spine sinh bang Spark (khong can bang nguon)
#    co_phai_la_ngay_nghi = Thu 7/CN HOAC nam trong danh sach ngay le co dinh
# ---------------------------------------------------------------------------
FIXED_HOLIDAYS_MMDD = {"01-01", "04-30", "05-01", "09-02"}  
CALENDAR_START_DATE = date.fromisoformat(
    os.getenv("CALENDAR_START_DATE", "2023-01-01")
)
CALENDAR_DAYS_AHEAD = int(os.getenv("CALENDAR_DAYS_AHEAD", "7"))
if CALENDAR_DAYS_AHEAD < 0:
    raise ValueError("CALENDAR_DAYS_AHEAD phai >= 0")

business_today = datetime.now(timezone(timedelta(hours=7))).date()
calendar_end_date = business_today + timedelta(days=CALENDAR_DAYS_AHEAD)
calendar_table = f"{CATALOG}.gold.dim_thoi_gian"
calendar_start_date = CALENDAR_START_DATE
base_workday_index = 0

if spark.catalog.tableExists(calendar_table):
    existing_calendar = spark.table(calendar_table)
    calendar_stats = existing_calendar.agg(
        F.max("ngay_date").alias("max_date"),
        F.max("stt_ngay_lam_viec").alias("max_workday_index"),
    ).first()
    if calendar_stats["max_date"] is not None:
        calendar_start_date = calendar_stats["max_date"] + timedelta(days=1)
        base_workday_index = int(calendar_stats["max_workday_index"] or 0)

    # Neu phien ban demo cu da sinh xa den 2028, cat phan tuong lai vuot
    # horizon. Lich su tu 2023 van duoc giu de backfill va join fact cu.
    end_time_id = int(calendar_end_date.strftime("%Y%m%d"))
    if calendar_stats["max_date"] and calendar_stats["max_date"] > calendar_end_date:
        spark.sql(
            f"DELETE FROM {calendar_table} WHERE thoi_gian_id > {end_time_id}"
        )
        connection = spark._jvm.java.sql.DriverManager.getConnection(
            STARROCKS_JDBC_URL, STARROCKS_USER, STARROCKS_PASSWORD
        )
        try:
            statement = connection.createStatement()
            try:
                statement.executeUpdate(
                    f"DELETE FROM dim_thoi_gian WHERE thoi_gian_id > {end_time_id}"
                )
            finally:
                statement.close()
        finally:
            connection.close()
        calendar_start_date = calendar_end_date + timedelta(days=1)
        print(
            f"[+] dim_thoi_gian: da cat cac ngay sau {calendar_end_date} "
            f"(horizon {CALENDAR_DAYS_AHEAD} ngay)."
        )

if calendar_start_date <= calendar_end_date:
    date_spine = (
        spark.sql(
            "SELECT explode(sequence("
            f"to_date('{calendar_start_date}'), "
            f"to_date('{calendar_end_date}'), interval 1 day)) AS ngay_dt"
        )
        .withColumn("thoi_gian_id", F.date_format("ngay_dt", "yyyyMMdd").cast("int"))
        .withColumn("ngay", F.dayofmonth("ngay_dt"))
        .withColumn("thang", F.month("ngay_dt"))
        .withColumn("quy", F.quarter("ngay_dt"))
        .withColumn("nam", F.year("ngay_dt"))
        .withColumn("thu_trong_tuan", F.dayofweek("ngay_dt"))  # 1=CN, 7=Thu7
        .withColumn("mmdd", F.date_format("ngay_dt", "MM-dd"))
        .withColumn(
            "co_phai_la_ngay_nghi",
            (F.col("thu_trong_tuan").isin(1, 7))
            | (F.col("mmdd").isin(*FIXED_HOLIDAYS_MMDD)),
        )
        .withColumn(
            "stt_ngay_lam_viec",
            F.lit(base_workday_index)
            + F.sum(
                F.when(~F.col("co_phai_la_ngay_nghi"), F.lit(1)).otherwise(F.lit(0))
            ).over(
                Window.orderBy("ngay_dt").rowsBetween(
                    Window.unboundedPreceding, Window.currentRow
                )
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
    save_dim(date_spine, "dim_thoi_gian", ["thoi_gian_id"])
    print(
        f"[+] dim_thoi_gian: bo sung {calendar_start_date} -> {calendar_end_date}."
    )
else:
    print(
        f"[=] dim_thoi_gian da du den {calendar_end_date}; khong ghi lai du lieu cu."
    )


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
        F.col("source_snapshot_at"),
        F.col("source_snapshot_id"),
    )
)
save_dim_scd2(dim_co_quan, "dim_co_quan", "co_quan_id", ["ten", "tinh", "phuong"])


# ---------------------------------------------------------------------------
# 3. DIM_TRANG_THAI - Status
# ---------------------------------------------------------------------------
dim_trang_thai = (
    spark.table(f"{CATALOG}.silver.status")
    .select(
        F.col("id").alias("trang_thai_id"),
        F.col("code").alias("ma_trang_thai"),
        F.col("name").alias("ten_trang_thai"),
        F.col("source_snapshot_at"),
        F.col("source_snapshot_id"),
    )
)
save_dim_scd2(dim_trang_thai, "dim_trang_thai", "trang_thai_id", ["ma_trang_thai", "ten_trang_thai"])


# ---------------------------------------------------------------------------
# 4. DIM_CAN_BO - Officer + Role (qua Officer_Role), + 1 dong "Unknown" (-1)
#    De tranh JOIN rong khi ho so moi RECEIVED chua duoc phan cong can bo
#    (theo dung luu y trong data-dictionary.md)
# ---------------------------------------------------------------------------
silver_officer = spark.table(f"{CATALOG}.silver.officer")
silver_officer_role = spark.table(f"{CATALOG}.silver.officer_role")
silver_role = spark.table(f"{CATALOG}.silver.role").select(F.col("id").alias("Roleid"), F.col("name").alias("vi_tri"))

# 1 can bo co the co nhieu vai tro trong Officer_Role (n-n) -> lay 1 vai tro dai dien (vai tro dau tien theo Roleid) de dim_can_bo giu dung grain
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
        F.col("source_snapshot_at"),
        F.col("source_snapshot_id"),
    )
)
dim_can_bo_unknown = spark.createDataFrame(
    [(-1, "Khong xac dinh", "N/A", None, "SYSTEM")],
    schema="can_bo_id int, ten string, vi_tri string, source_snapshot_at timestamp, source_snapshot_id string",
)
dim_can_bo = dim_can_bo_real.unionByName(dim_can_bo_unknown)
save_dim_scd2(dim_can_bo, "dim_can_bo", "can_bo_id", ["ten", "vi_tri"])


# ---------------------------------------------------------------------------
# 5. DIM_DICH_VU_CONG - Service (processing_time -> thoi_han_tra_kq)
# ---------------------------------------------------------------------------
dim_dich_vu_cong = (
    spark.table(f"{CATALOG}.silver.service")
    .select(
        F.col("id").alias("dv_cong_id"),
        F.col("name").alias("ten"),
        F.col("processing_time").alias("thoi_han_tra_kq"),
        F.col("source_snapshot_at"),
        F.col("source_snapshot_id"),
    )
)
save_dim_scd2(
    dim_dich_vu_cong,
    "dim_dich_vu_cong",
    "dv_cong_id",
    ["ten", "thoi_han_tra_kq"],
)

print("[+] Hoan tat build 5 bang dim (Iceberg + StarRocks).")
spark.stop()
