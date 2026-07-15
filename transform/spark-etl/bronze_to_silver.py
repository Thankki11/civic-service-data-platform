"""Build typed, deduplicated Silver entities from the immutable XML Bronze log.

The XML ingestion job intentionally stores ``du_lieu`` as JSON text in
``lakehouse.bronze_dvc_xml.application_xml``.  This job does not change that
ingestion contract.  It only parses the JSON, applies the event ordering, and
creates four reusable Silver tables:

* application_events   -- one typed XML packet per ma_goi_tin;
* application_current  -- latest non-deleted application state;
* application_history  -- one status-transition event per history ID;
* payment               -- one latest version per payment ID.

The supplied archive mixes full and partial XML packets.  ``last(...,
ignorenulls=True)`` is therefore essential: a partial status update must not
erase the service, agency, or submitted timestamp received in the INSERT.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType, TimestampType


BRONZE_APPLICATION_XML = "lakehouse.bronze_dvc_xml.application_xml"
BRONZE_APPLICATION_CDC = "lakehouse.bronze_oltp_core.application_cdc"
BRONZE_HISTORY_CDC = "lakehouse.bronze_oltp_core.application_history_cdc"
BRONZE_API_PAYMENT = "lakehouse.bronze_api.payment_transactions"
SILVER_EVENTS = "lakehouse.silver.application_events"
SILVER_CURRENT = "lakehouse.silver.application_current"
SILVER_HISTORY = "lakehouse.silver.application_history"
SILVER_PAYMENT = "lakehouse.silver.payment"


def build_spark() -> SparkSession:
    """Return the same Iceberg/Hive/MinIO catalog used by the ingestion jobs."""
    return (
        SparkSession.builder.appName("bronze-to-silver-dvc")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def ensure_silver_tables(spark: SparkSession) -> None:
    """Create stable schemas before writing, so a malformed source cannot drift them."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_EVENTS} (
            ma_goi_tin STRING, ho_so_id STRING, su_kien STRING, event_ts TIMESTAMP,
            ten_ho_so STRING, nguoi_nop_id STRING, dv_cong_id INT, co_quan_id INT,
            created_at TIMESTAMP, trang_thai_id INT, file_name STRING, ingested_at TIMESTAMP,
            processed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(event_ts))
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_CURRENT} (
            ho_so_id STRING, ten_ho_so STRING, nguoi_nop_id STRING, dv_cong_id INT,
            co_quan_id INT, created_at TIMESTAMP, trang_thai_id INT, event_ts TIMESTAMP,
            is_deleted BOOLEAN, processed_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_HISTORY} (
            history_id STRING, ho_so_id STRING, trang_thai_truoc_id INT,
            trang_thai_id INT, can_bo_id INT, action_time TIMESTAMP, ghi_chu STRING,
            event_ts TIMESTAMP, ma_goi_tin STRING, processed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(action_time))
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_PAYMENT} (
            payment_id STRING, ho_so_id STRING, so_tien BIGINT, phuong_thuc STRING,
            payment_status STRING, ma_giao_dich STRING, paid_at TIMESTAMP, event_ts TIMESTAMP,
            ma_goi_tin STRING, processed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(paid_at))
        """
    )


def overwrite_table(df, table_name: str) -> None:
    """Rebuild a derived Silver table deterministically from the append-only Bronze log."""
    df.writeTo(table_name).overwrite(F.lit(True))


def as_json_array(json_column):
    """Normalise an object-or-array XML representation to a JSON array string."""
    trimmed = F.trim(json_column)
    return F.when(
        json_column.isNull() | (trimmed == "") | (trimmed == "null"),
        F.lit(None).cast("string"),
    ).when(
        trimmed.startswith("["), json_column
    ).otherwise(F.concat(F.lit("["), json_column, F.lit("]")))


def build_application_events(bronze):
    """Type the top-level application attributes and remove duplicated packets."""
    events = (
        bronze.select(
            "ma_goi_tin",
            F.col("id_ban_ghi").cast("string").alias("ho_so_id"),
            F.upper(F.trim("su_kien")).alias("su_kien"),
            F.coalesce(
                F.to_timestamp("ngay_cap_nhat", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.get_json_object("data_payload", "$.created_at"), "yyyy-MM-dd HH:mm:ss"),
            ).alias("event_ts"),
            F.get_json_object("data_payload", "$.name").alias("ten_ho_so"),
            F.get_json_object("data_payload", "$.Applicantid").alias("nguoi_nop_id"),
            F.get_json_object("data_payload", "$.Serviceid").cast(IntegerType()).alias("dv_cong_id"),
            F.get_json_object("data_payload", "$.Agencyid").cast(IntegerType()).alias("co_quan_id"),
            F.to_timestamp(F.get_json_object("data_payload", "$.created_at"), "yyyy-MM-dd HH:mm:ss").alias("created_at"),
            F.get_json_object("data_payload", "$.Statusid").cast(IntegerType()).alias("trang_thai_id"),
            "data_payload",
            "file_name",
            "ingested_at",
        )
        .filter(F.col("ma_goi_tin").isNotNull() & F.col("ho_so_id").isNotNull())
    )
    duplicate_window = Window.partitionBy("ma_goi_tin").orderBy(F.col("ingested_at").desc_nulls_last())
    return (
        events.withColumn("_packet_rank", F.row_number().over(duplicate_window))
        .filter(F.col("_packet_rank") == 1)
        .drop("_packet_rank")
        .withColumn("processed_at", F.current_timestamp())
    )


def build_cdc_application_events(spark: SparkSession):
    """Adapt the existing Spark-CDC Bronze table to the canonical event schema.

    The current ingestion job only writes c/u records to Iceberg. Deletes are
    handled correctly by the dedicated StarRocks CDC path, while this adapter
    lets the batch mart also include the CDC application stream when it exists.
    """
    if not spark.catalog.tableExists(BRONZE_APPLICATION_CDC):
        return None
    cdc = spark.table(BRONZE_APPLICATION_CDC)
    event_ts = F.coalesce(F.col("updated_at").cast("timestamp"), F.col("created_at").cast("timestamp"), F.col("ingested_at"))
    return cdc.select(
        F.concat_ws("|", F.lit("cdc-app"), F.col("id"), event_ts.cast("string"), F.col("op")).alias("ma_goi_tin"),
        F.col("id").cast("string").alias("ho_so_id"),
        F.when(F.col("op") == F.lit("d"), F.lit("DELETE"))
        .when(F.col("op").isin("c", "r"), F.lit("INSERT"))
        .otherwise(F.lit("UPDATE"))
        .alias("su_kien"),
        event_ts.alias("event_ts"),
        F.col("name").alias("ten_ho_so"),
        F.col("Applicantid").cast("string").alias("nguoi_nop_id"),
        F.col("Serviceid").cast(IntegerType()).alias("dv_cong_id"),
        F.col("Agencyid").cast(IntegerType()).alias("co_quan_id"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("Statusid").cast(IntegerType()).alias("trang_thai_id"),
        F.lit(None).cast("string").alias("data_payload"),
        F.lit("kafka-cdc").alias("file_name"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.current_timestamp().alias("processed_at"),
    ).filter(F.col("ho_so_id").isNotNull())


def build_cdc_history(spark: SparkSession):
    """Adapt Debezium history Bronze rows when the batch CDC job has run."""
    if not spark.catalog.tableExists(BRONZE_HISTORY_CDC):
        return None
    cdc = spark.table(BRONZE_HISTORY_CDC)
    return (
        cdc.select(
            F.col("id").cast("string").alias("history_id"),
            F.col("Applicationid").cast("string").alias("ho_so_id"),
            F.col("Statusid").cast(IntegerType()).alias("trang_thai_truoc_id"),
            F.col("Statusid2").cast(IntegerType()).alias("trang_thai_id"),
            F.col("Officerid").cast(IntegerType()).alias("can_bo_id"),
            F.col("action_time").cast("timestamp").alias("action_time"),
            F.col("note").alias("ghi_chu"),
            F.coalesce(F.col("action_time").cast("timestamp"), F.col("ingested_at")).alias("event_ts"),
            F.concat_ws("|", F.lit("cdc-history"), F.col("id")).alias("ma_goi_tin"),
            F.current_timestamp().alias("processed_at"),
        )
        .filter(F.col("history_id").isNotNull() & F.col("ho_so_id").isNotNull())
    )


def build_history(events):
    """Explode the status-history payload, handling both one-object and array packets."""
    history_schema = ArrayType(
        StructType(
            [
                StructField("id", StringType(), True),
                StructField("Statusid", StringType(), True),
                StructField("Statusid2", StringType(), True),
                StructField("Officerid", StringType(), True),
                StructField("action_time", StringType(), True),
                StructField("note", StringType(), True),
            ]
        )
    )
    history_json = F.get_json_object("data_payload", "$.application_history_array.application_history")
    exploded = (
        events.withColumn("_history", F.from_json(as_json_array(history_json), history_schema))
        .select("ho_so_id", "event_ts", "ma_goi_tin", F.explode_outer("_history").alias("history"))
        .select(
            F.col("history.id").cast("string").alias("history_id"),
            "ho_so_id",
            F.col("history.Statusid").cast(IntegerType()).alias("trang_thai_truoc_id"),
            F.col("history.Statusid2").cast(IntegerType()).alias("trang_thai_id"),
            F.col("history.Officerid").cast(IntegerType()).alias("can_bo_id"),
            F.to_timestamp("history.action_time", "yyyy-MM-dd HH:mm:ss").alias("action_time"),
            F.col("history.note").alias("ghi_chu"),
            "event_ts",
            "ma_goi_tin",
        )
        .filter(F.col("history_id").isNotNull())
    )
    dedup_window = Window.partitionBy("history_id").orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("ma_goi_tin").desc()
    )
    return (
        exploded.withColumn("_history_rank", F.row_number().over(dedup_window))
        .filter(F.col("_history_rank") == 1)
        .drop("_history_rank")
        .withColumn("action_time", F.coalesce("action_time", "event_ts"))
        .withColumn("processed_at", F.current_timestamp())
    )


def build_payments(events):
    """Explode payment additions/full snapshots and keep the latest version of a payment."""
    payment_schema = ArrayType(
        StructType(
            [
                StructField("id", StringType(), True),
                StructField("amount", StringType(), True),
                StructField("method", StringType(), True),
                StructField("status", StringType(), True),
                StructField("transaction_code", StringType(), True),
                StructField("paid_at", StringType(), True),
            ]
        )
    )
    payment_json = F.get_json_object("data_payload", "$.payment_array.payment")
    exploded = (
        events.withColumn("_payment", F.from_json(as_json_array(payment_json), payment_schema))
        .select("ho_so_id", "event_ts", "ma_goi_tin", F.explode_outer("_payment").alias("payment"))
        .select(
            F.col("payment.id").cast("string").alias("payment_id"),
            "ho_so_id",
            F.col("payment.amount").cast(LongType()).alias("so_tien"),
            F.col("payment.method").alias("phuong_thuc"),
            F.upper(F.col("payment.status")).alias("payment_status"),
            F.col("payment.transaction_code").alias("ma_giao_dich"),
            F.to_timestamp("payment.paid_at", "yyyy-MM-dd HH:mm:ss").alias("paid_at"),
            "event_ts",
            "ma_goi_tin",
        )
        .filter(F.col("payment_id").isNotNull())
    )
    dedup_window = Window.partitionBy("payment_id").orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("ma_goi_tin").desc()
    )
    return (
        exploded.withColumn("_payment_rank", F.row_number().over(dedup_window))
        .filter(F.col("_payment_rank") == 1)
        .drop("_payment_rank")
        .withColumn("paid_at", F.coalesce("paid_at", "event_ts"))
        .withColumn("processed_at", F.current_timestamp())
    )


def build_api_payments(spark: SparkSession):
    """Bring the existing NiFi/API Bronze feed into the canonical payment entity."""
    if not spark.catalog.tableExists(BRONZE_API_PAYMENT):
        return None
    api = spark.table(BRONZE_API_PAYMENT)
    paid_at = F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
    # The mock API has no transaction ID. This deterministic natural key is the
    # safest available deduplication key without changing ingestion.
    payment_id = F.sha2(
        F.concat_ws(
            "|",
            F.lit("api"),
            F.col("id_ban_ghi"),
            F.col("timestamp"),
            F.col("amount").cast("string"),
            F.col("method"),
            F.col("tax_code"),
        ),
        256,
    )
    return api.select(
        payment_id.alias("payment_id"),
        F.col("id_ban_ghi").cast("string").alias("ho_so_id"),
        F.col("amount").cast(LongType()).alias("so_tien"),
        F.col("method").alias("phuong_thuc"),
        F.upper(F.col("payment_status")).alias("payment_status"),
        F.col("tax_code").alias("ma_giao_dich"),
        paid_at.alias("paid_at"),
        F.coalesce(paid_at, F.col("ingested_at")).alias("event_ts"),
        F.lit("api-payment").alias("ma_goi_tin"),
        F.current_timestamp().alias("processed_at"),
    ).filter(F.col("ho_so_id").isNotNull())


def deduplicate_history(history):
    window = Window.partitionBy("history_id").orderBy(F.col("event_ts").desc_nulls_last(), F.col("ma_goi_tin").desc())
    return (
        history.withColumn("_history_rank", F.row_number().over(window))
        .filter(F.col("_history_rank") == 1)
        .drop("_history_rank")
    )


def deduplicate_payments(payments):
    window = Window.partitionBy("payment_id").orderBy(F.col("event_ts").desc_nulls_last(), F.col("ma_goi_tin").desc())
    return (
        payments.withColumn("_payment_rank", F.row_number().over(window))
        .filter(F.col("_payment_rank") == 1)
        .drop("_payment_rank")
    )


def build_current_application(events):
    """Merge full and partial packets in their business-event order."""
    state_window = (
        Window.partitionBy("ho_so_id")
        .orderBy(F.col("event_ts").asc_nulls_last(), F.col("ma_goi_tin").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    latest_window = Window.partitionBy("ho_so_id").orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("ma_goi_tin").desc()
    )
    merged = (
        events.withColumn("ten_ho_so", F.last("ten_ho_so", ignorenulls=True).over(state_window))
        .withColumn("nguoi_nop_id", F.last("nguoi_nop_id", ignorenulls=True).over(state_window))
        .withColumn("dv_cong_id", F.last("dv_cong_id", ignorenulls=True).over(state_window))
        .withColumn("co_quan_id", F.last("co_quan_id", ignorenulls=True).over(state_window))
        .withColumn("created_at", F.last("created_at", ignorenulls=True).over(state_window))
        .withColumn("trang_thai_id", F.last("trang_thai_id", ignorenulls=True).over(state_window))
        .withColumn("is_deleted", (F.col("su_kien") == F.lit("DELETE")))
        .withColumn("_current_rank", F.row_number().over(latest_window))
        .filter(F.col("_current_rank") == 1)
        .drop("_current_rank", "data_payload", "file_name", "ingested_at", "su_kien", "ma_goi_tin")
        .withColumn("processed_at", F.current_timestamp())
    )
    return merged


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_silver_tables(spark)
        if not spark.catalog.tableExists(BRONZE_APPLICATION_XML):
            raise RuntimeError(
                f"Khong tim thay {BRONZE_APPLICATION_XML}. Hay chay job2_transactional_xml.py truoc."
            )

        xml_events = build_application_events(spark.table(BRONZE_APPLICATION_XML))
        xml_history = build_history(xml_events)
        xml_payments = build_payments(xml_events)

        cdc_events = build_cdc_application_events(spark)
        cdc_history = build_cdc_history(spark)
        api_payments = build_api_payments(spark)

        application_events = xml_events if cdc_events is None else xml_events.unionByName(cdc_events)
        application_history = xml_history if cdc_history is None else deduplicate_history(xml_history.unionByName(cdc_history))
        payments = xml_payments if api_payments is None else deduplicate_payments(xml_payments.unionByName(api_payments))

        application_events = application_events.repartition("ho_so_id")
        application_history = application_history.repartition("ho_so_id")
        payments = payments.repartition("ho_so_id")
        application_current = build_current_application(application_events).repartition("ho_so_id")

        # Drop payload only after history/payment have parsed it.
        overwrite_table(
            application_events.drop("data_payload").select(
                "ma_goi_tin", "ho_so_id", "su_kien", "event_ts", "ten_ho_so", "nguoi_nop_id",
                "dv_cong_id", "co_quan_id", "created_at", "trang_thai_id", "file_name",
                "ingested_at", "processed_at",
            ),
            SILVER_EVENTS,
        )
        overwrite_table(application_current, SILVER_CURRENT)
        overwrite_table(application_history, SILVER_HISTORY)
        overwrite_table(payments, SILVER_PAYMENT)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
