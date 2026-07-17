# ============================================================================
# JOB: Bronze -> Silver
#
# Quy tac nguon da doi chieu voi generator mau:
#   - XML va CDC sinh hai tap ho so bo sung nhau (HS_00001... va HS_100000...).
#     Vi vay union theo tung thuc the, khong bo qua mot nguon chi vi nguon kia
#     da co bang Bronze.
#   - API payment la mot kenh bo sung cho payment trong XML.
#   - tableExists chi dung de bo qua input chua ingest, khong chon nguon.
#   - Cach ghi phu thuoc grain: current-state MERGE, event/transaction APPEND
#     idempotent, danh muc full-snapshot thi replace toan bo snapshot.
# ============================================================================

from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"
CATALOG = "lakehouse"
N_PART = 32

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
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")


def bronze_exists(table_name):
    return spark.catalog.tableExists(table_name)


def create_silver_table(df, table_name):
    """Tao Iceberg table neu chua ton tai va tra ve ten day du."""
    full_name = f"{CATALOG}.silver.{table_name}"
    schema_sql = ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in df.schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} ({schema_sql})
        USING iceberg
        LOCATION 's3a://lakehouse/warehouse/silver/{table_name}'
    """)
    return full_name


def merge_current_state(df, table_name, key_cols):
    """MERGE cho bang chi giu trang thai hien tai cua moi business key."""
    full_name = create_silver_table(df, table_name)

    source_view = f"_silver_{table_name}_source"
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
    print(f"[+] silver.{table_name} <- MERGE current-state theo {', '.join(key_cols)}")


def append_new_events(df, table_name, key_cols):
    """APPEND event bat bien, chi chen key chua co de job chay lai khong trung."""
    full_name = create_silver_table(df, table_name)
    source_view = f"_silver_{table_name}_source"
    df.createOrReplaceTempView(source_view)
    match = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in key_cols)
    columns = ", ".join(f"`{field.name}`" for field in df.schema.fields)
    select_columns = ", ".join(f"s.`{field.name}`" for field in df.schema.fields)
    spark.sql(f"""
        INSERT INTO {full_name} ({columns})
        SELECT {select_columns}
        FROM {source_view} s
        LEFT ANTI JOIN {full_name} t
          ON {match}
    """)
    spark.catalog.dropTempView(source_view)
    print(f"[+] silver.{table_name} <- APPEND event moi theo {', '.join(key_cols)}")


def replace_master_snapshot(df, table_name):
    """Danh muc Bronze la full snapshot, nen Silver phai phan anh dung snapshot do."""
    full_name = create_silver_table(df, table_name)
    df.writeTo(full_name).overwrite(F.lit(True))
    print(f"[+] silver.{table_name} <- REPLACE full master snapshot")


def latest(df, key_col, order_col, tie_breaker=None):
    order_by = [F.col(order_col).desc_nulls_last()]
    if tie_breaker is not None:
        order_by.append(F.col(tie_breaker).desc_nulls_last())
    window = Window.partitionBy(key_col).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


BRONZE_XML_TABLE = f"{CATALOG}.bronze_dvc_xml.application_xml"
BRONZE_API_TABLE = f"{CATALOG}.bronze_api.payment_transactions"
CDC_APPLICATION = f"{CATALOG}.bronze_oltp_core.application_cdc"
CDC_HISTORY = f"{CATALOG}.bronze_oltp_core.application_history_cdc"
CDC_DOCUMENT = f"{CATALOG}.bronze_oltp_core.document_cdc"
CDC_APPLICANT = f"{CATALOG}.bronze_oltp_core.applicant_cdc"


def read_optional_bronze(table_name):
    """Bo qua rieng input chua ingest; khong dung de loai bo input khac."""
    if bronze_exists(table_name):
        return spark.table(table_name)
    print(f"[!] Chua co {table_name}; bo qua rieng input nay.")
    return None


def union_available(*dataframes):
    """Union tat ca input dang co, khong co quy tac fallback theo ton tai bang."""
    available = [df for df in dataframes if df is not None]
    if not available:
        return None
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), available)


def parse_xml_collection(xml_df, json_path, item_schema):
    """Parse wrapper.item cua XML; item co the la mot object hoac mot array."""
    items_json = F.get_json_object(F.col("data_payload"), json_path)
    normalized_json = F.when(
        F.trim(items_json).startswith("["), F.trim(items_json)
    ).otherwise(F.concat(F.lit("["), F.trim(items_json), F.lit("]")))
    return (
        xml_df
        .withColumn("_items_json", items_json)
        .filter(F.col("_items_json").isNotNull())
        .withColumn("_items", F.from_json(normalized_json, ArrayType(item_schema)))
        .filter(F.size("_items") > 0)
    )


bronze_xml = read_optional_bronze(BRONZE_XML_TABLE)
bronze_api_payment = read_optional_bronze(BRONZE_API_TABLE)
bronze_cdc_application = read_optional_bronze(CDC_APPLICATION)
bronze_cdc_history = read_optional_bronze(CDC_HISTORY)
bronze_cdc_document = read_optional_bronze(CDC_DOCUMENT)
bronze_cdc_applicant = read_optional_bronze(CDC_APPLICANT)


# Danh muc chi co nguon Bronze master, do do khong lien quan den XML fallback.
MASTER_TABLES = [
    "province", "ward", "status", "service", "agency", "role", "permission",
    "document_type", "officer", "officer_role",
]
for table in MASTER_TABLES:
    source = f"{CATALOG}.bronze_master_data.{table}"
    if not bronze_exists(source):
        print(f"[!] Khong tim thay {source}; giu nguyen silver.{table}.")
        continue
    master = spark.table(source)
    for column in [field.name for field in master.schema.fields if field.dataType.simpleString() == "string"]:
        master = master.withColumn(column, F.trim(F.col(column)))
    master = master.drop(*[column for column in ("ingested_at", "file_name") if column in master.columns])
    replace_master_snapshot(master.dropDuplicates(["id"]), table)


# APPLICATION: union XML + CDC. Cac ID mau khong giao nhau; source_priority
# chi la tie-break deterministic neu mot ngay co cung ID va event_time.
xml_application = None
if bronze_xml is not None:
    xml_application = bronze_xml.select(
        F.col("id_ban_ghi").alias("ho_so_id"),
        F.get_json_object("data_payload", "$.name").alias("ten_ho_so"),
        F.get_json_object("data_payload", "$.Applicantid").alias("applicant_id"),
        F.get_json_object("data_payload", "$.Serviceid").cast("int").alias("dv_cong_id"),
        F.get_json_object("data_payload", "$.Agencyid").cast("int").alias("co_quan_id"),
        F.to_timestamp(F.get_json_object("data_payload", "$.created_at")).alias("created_at"),
        F.get_json_object("data_payload", "$.Statusid").cast("int").alias("trang_thai_id"),
        F.coalesce(F.to_timestamp("ngay_cap_nhat"), F.col("ingested_at")).alias("event_time"),
        (F.col("su_kien") == F.lit("DELETE")).alias("da_bi_xoa"),
        F.lit(1).alias("source_priority"),
    )

cdc_application = None
if bronze_cdc_application is not None:
    cdc_application = bronze_cdc_application.select(
        F.col("id").alias("ho_so_id"), F.col("name").alias("ten_ho_so"),
        F.col("Applicantid").alias("applicant_id"), F.col("Serviceid").alias("dv_cong_id"),
        F.col("Agencyid").alias("co_quan_id"), F.col("created_at"),
        F.col("Statusid").alias("trang_thai_id"),
        F.coalesce(F.col("updated_at"), F.col("ingested_at")).alias("event_time"),
        F.lit(False).alias("da_bi_xoa"), F.lit(2).alias("source_priority"),
    )

application_events = union_available(xml_application, cdc_application)
if application_events is not None:
    application_events = application_events.repartition(N_PART, "ho_so_id")
    forward = Window.partitionBy("ho_so_id").orderBy("event_time", "source_priority").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    for column in ["ten_ho_so", "applicant_id", "dv_cong_id", "co_quan_id", "created_at", "trang_thai_id"]:
        application_events = application_events.withColumn(column, F.last(column, ignorenulls=True).over(forward))
    application = latest(application_events, "ho_so_id", "event_time", "source_priority").drop("source_priority")
    merge_current_state(application, "application", ["ho_so_id"])


history_item_schema = StructType([
    StructField("id", StringType()), StructField("Statusid", StringType()),
    StructField("Statusid2", StringType()), StructField("Officerid", StringType()),
    StructField("action_time", StringType()), StructField("note", StringType()),
])
xml_history = None
if bronze_xml is not None:
    xml_history = parse_xml_collection(
        bronze_xml, "$.application_history_array.application_history", history_item_schema
    ).select(F.col("id_ban_ghi").alias("ho_so_id"), F.explode("_items").alias("item")).select(
        F.col("item.id").alias("history_id"), "ho_so_id",
        F.col("item.Statusid").cast("int").alias("trang_thai_truoc_id"),
        F.col("item.Statusid2").cast("int").alias("trang_thai_id"),
        F.coalesce(F.col("item.Officerid").cast("int"), F.lit(-1)).alias("can_bo_id"),
        F.to_timestamp("item.action_time").alias("action_time"), F.col("item.note").alias("note"),
        F.lit(1).alias("source_priority"),
    )

cdc_history = None
if bronze_cdc_history is not None:
    cdc_history = bronze_cdc_history.select(
        F.col("id").alias("history_id"), F.col("Applicationid").alias("ho_so_id"),
        F.col("Statusid").alias("trang_thai_truoc_id"), F.col("Statusid2").alias("trang_thai_id"),
        F.coalesce(F.col("Officerid"), F.lit(-1)).alias("can_bo_id"), F.col("action_time"), F.col("note"),
        F.lit(2).alias("source_priority"),
    )

history = union_available(xml_history, cdc_history)
if history is not None:
    history = latest(history, "history_id", "action_time", "source_priority").drop("source_priority")
    append_new_events(history, "application_history", ["history_id"])


document_item_schema = StructType([
    StructField("id", StringType()), StructField("name", StringType()), StructField("file_url", StringType()),
    StructField("Document_Typeid", StringType()),
])
xml_document = None
if bronze_xml is not None:
    xml_document = parse_xml_collection(
        bronze_xml, "$.document_array.document", document_item_schema
    ).select(
        F.col("id_ban_ghi").alias("ho_so_id"),
        F.coalesce(F.to_timestamp("ngay_cap_nhat"), F.col("ingested_at")).alias("event_time"),
        F.explode("_items").alias("item"),
    ).select(
        F.col("item.id").alias("document_id"), "ho_so_id", F.col("item.name").alias("ten_tai_lieu"), F.col("item.file_url"),
        F.col("item.Document_Typeid").cast("int").alias("loai_tai_lieu_id"), "event_time", F.lit(1).alias("source_priority"),
    )

cdc_document = None
if bronze_cdc_document is not None:
    cdc_document = bronze_cdc_document.select(
        F.col("id").alias("document_id"), F.col("Applicationid").alias("ho_so_id"), F.col("name").alias("ten_tai_lieu"),
        F.col("file_url"), F.col("Document_Typeid").alias("loai_tai_lieu_id"), F.col("ingested_at").alias("event_time"),
        F.lit(2).alias("source_priority"),
    )

document = union_available(xml_document, cdc_document)
if document is not None:
    document = latest(document, "document_id", "event_time", "source_priority").drop("source_priority")
    merge_current_state(document, "document", ["document_id"])


payment_item_schema = StructType([
    StructField("amount", StringType()), StructField("method", StringType()), StructField("status", StringType()),
    StructField("transaction_code", StringType()), StructField("paid_at", StringType()),
])
xml_payment = None
if bronze_xml is not None:
    xml_payment = parse_xml_collection(
        bronze_xml, "$.payment_array.payment", payment_item_schema
    ).select(F.col("id_ban_ghi").alias("ho_so_id"), F.explode("_items").alias("item")).select(
        F.col("item.transaction_code").alias("ma_giao_dich"), "ho_so_id", F.col("item.amount").cast("double").alias("so_tien"),
        F.col("item.method").alias("phuong_thuc"), F.col("item.status").alias("trang_thai_tt"),
        F.to_timestamp("item.paid_at").alias("thoi_gian_tt"), F.lit("XML_MOT_CUA").alias("nguon_du_lieu"),
        F.to_timestamp("item.paid_at").alias("event_time"), F.lit(1).alias("source_priority"),
    )

api_payment = None
if bronze_api_payment is not None:
    api_payment = bronze_api_payment.select(
        # Mock API khong co transaction id; tax_code la reference duy nhat cua
        # giao dich. Khong dung ho_so_id + timestamp vi hai payment co the cung giay.
        F.concat(F.lit("API_"), F.col("tax_code")).alias("ma_giao_dich"),
        F.col("id_ban_ghi").alias("ho_so_id"), F.col("amount").alias("so_tien"), F.col("method").alias("phuong_thuc"),
        F.col("payment_status").alias("trang_thai_tt"), F.to_timestamp("timestamp").alias("thoi_gian_tt"),
        F.lit("API_THANH_TOAN").alias("nguon_du_lieu"), F.col("ingested_at").alias("event_time"), F.lit(2).alias("source_priority"),
    )

payment = union_available(xml_payment, api_payment)
if payment is not None:
    payment = latest(payment, "ma_giao_dich", "event_time", "source_priority").drop("source_priority")
    append_new_events(payment.drop("event_time"), "payment", ["ma_giao_dich"])


if bronze_cdc_applicant is not None:
    applicant = latest(bronze_cdc_applicant, "id", "updated_at").drop("ingested_at")
    merge_current_state(applicant, "applicant", ["id"])

print("[+] Hoan tat Bronze -> Silver: union cac input nghiep vu da ingest.")
spark.stop()
