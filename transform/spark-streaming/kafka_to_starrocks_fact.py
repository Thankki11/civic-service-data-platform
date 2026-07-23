"""Join Application/Application_History CDC and load a physical realtime fact.

This is the primary realtime serving job. It is independent from
ingestion/streaming-kafka/job3_streaming_cdc.py, which persists the same CDC
topics to Bronze Iceberg for the batch path.

Join contract:
* every Application status change has one matching Application_History insert;
* Application.id equals Application_History.Applicationid;
* the before/after status pair identifies the transition of that application;
* unmatched/late records are discarded after the one-minute watermark.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
APPLICATION_TOPIC = os.getenv(
    "APPLICATION_TOPIC", "postgres_server.public.Application"
)
HISTORY_TOPIC = os.getenv(
    "HISTORY_TOPIC", "postgres_server.public.Application_History"
)
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "s3a://lakehouse/checkpoints/fact_xu_ly_ho_so_stream",
)

# The all-in-one image advertises its colocated BE as 127.0.0.1. Port 8080 is
# its bundled FE proxy, which follows the Stream Load redirect inside the
# StarRocks container so remote Spark executors never connect to that loopback.
STARROCKS_FE_HTTP_URL = os.getenv("STARROCKS_FE_HTTP_URL", "starrocks:8080")
STARROCKS_FE_JDBC_URL = os.getenv(
    "STARROCKS_FE_JDBC_URL", "jdbc:mysql://starrocks:9030"
)
STARROCKS_TABLE = os.getenv(
    "STARROCKS_TABLE", "gold_realtime.fact_xu_ly_ho_so_stream"
)
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "")

WATERMARK = "1 minute"
TRIGGER_INTERVAL = "5 seconds"


APPLICATION_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("created_at", LongType()),
        StructField("Applicantid", StringType()),
        StructField("Statusid", IntegerType()),
        StructField("Serviceid", IntegerType()),
        StructField("Agencyid", IntegerType()),
        StructField("updated_at", LongType()),
    ]
)

HISTORY_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("Applicationid", StringType()),
        StructField("Statusid", IntegerType()),
        StructField("Statusid2", IntegerType()),
        StructField("Officerid", IntegerType()),
        StructField("action_time", LongType()),
        StructField("note", StringType()),
    ]
)

SOURCE_SCHEMA = StructType(
    [
        StructField("ts_ms", LongType()),
        StructField("txId", LongType()),
    ]
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("total_order", LongType()),
        StructField("data_collection_order", LongType()),
    ]
)


def cdc_envelope_schema(row_schema: StructType) -> StructType:
    payload_schema = StructType(
        [
            StructField("before", row_schema),
            StructField("after", row_schema),
            StructField("source", SOURCE_SCHEMA),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField("transaction", TRANSACTION_SCHEMA),
        ]
    )
    return StructType([StructField("payload", payload_schema)])


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Kafka_To_StarRocks_Realtime_Fact")
        .config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "3"))
        .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "1"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"))
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("value").cast("string").alias("json_value"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
    )


def parse_application(raw: DataFrame) -> DataFrame:
    payload = F.from_json(
        F.col("json_value"), cdc_envelope_schema(APPLICATION_SCHEMA)
    ).getField("payload")
    parsed = raw.withColumn("payload", payload)
    return (
        parsed.filter(F.col("payload.op").isin("c", "u"))
        .select(
            F.col("payload.after.id").alias("ho_so_id"),
            F.col("payload.after.name").alias("ten_ho_so"),
            F.col("payload.after.Applicantid").alias("applicant_id"),
            F.col("payload.after.Agencyid").alias("co_quan_id"),
            F.col("payload.after.Serviceid").alias("dv_cong_id"),
            F.col("payload.before.Statusid").alias("app_before_status_id"),
            F.col("payload.after.Statusid").alias("app_after_status_id"),
            F.expr("timestamp_micros(payload.after.created_at)").alias("created_at"),
            F.expr("timestamp_micros(payload.before.updated_at)").alias(
                "app_before_updated_at"
            ),
            F.expr("timestamp_micros(payload.after.updated_at)").alias(
                "app_after_updated_at"
            ),
            # Kept only as audit metadata. Business matching does not depend
            # on PostgreSQL's internal transaction identifier.
            F.col("payload.source.txId").cast("string").alias("transaction_id"),
            F.coalesce(
                F.expr("timestamp_millis(payload.source.ts_ms)"),
                F.col("kafka_timestamp"),
            ).alias("app_event_time"),
            F.col("kafka_timestamp").alias("app_kafka_timestamp"),
            F.col("kafka_partition").alias("application_kafka_partition"),
            F.col("kafka_offset").alias("application_kafka_offset"),
        )
        .filter(F.col("ho_so_id").isNotNull())
    )


def parse_history(raw: DataFrame) -> DataFrame:
    payload = F.from_json(
        F.col("json_value"), cdc_envelope_schema(HISTORY_SCHEMA)
    ).getField("payload")
    parsed = raw.withColumn("payload", payload)
    return (
        parsed.filter(F.col("payload.op") == "c")
        .select(
            F.col("payload.after.id").alias("id"),
            F.col("payload.after.Applicationid").alias("ho_so_id"),
            F.col("payload.after.Statusid").alias("trang_thai_truoc_id"),
            F.col("payload.after.Statusid2").alias("trang_thai_id"),
            F.col("payload.after.Officerid").alias("can_bo_id"),
            F.expr("timestamp_micros(payload.after.action_time)").alias(
                "action_time"
            ),
            F.col("payload.source.txId").cast("string").alias("transaction_id"),
            F.coalesce(
                F.expr("timestamp_millis(payload.source.ts_ms)"),
                F.col("kafka_timestamp"),
            ).alias("history_event_time"),
            F.col("kafka_timestamp").alias("history_kafka_timestamp"),
            F.col("kafka_partition").alias("history_kafka_partition"),
            F.col("kafka_offset").alias("history_kafka_offset"),
        )
        .filter(F.col("id").isNotNull() & F.col("ho_so_id").isNotNull())
    )


def build_fact(application: DataFrame, history: DataFrame) -> DataFrame:
    app = application.withWatermark("app_event_time", WATERMARK).alias("a")
    hist = history.withWatermark("history_event_time", WATERMARK).alias("h")

    # Business match: Application.id = Application_History.Applicationid plus
    # the status transition. History.id is not a join key; it uniquely
    # identifies the output fact row for replay-safe upsert in StarRocks.
    # The time range is only the state-eviction bound.
    join_condition = F.expr(
        """
        a.ho_so_id = h.ho_so_id
        AND a.app_after_status_id = h.trang_thai_id
        AND (a.app_before_status_id IS NULL
             OR a.app_before_status_id = h.trang_thai_truoc_id)
        AND h.history_event_time >= a.app_event_time - INTERVAL 1 MINUTE
        AND h.history_event_time <= a.app_event_time + INTERVAL 1 MINUTE
        """
    )

    joined = app.join(hist, join_condition, "inner")

    # Duration belongs to one Application UPDATE event, so it is calculated
    # entirely from that event's before/after images. It does not depend on the
    # Application_History insert sharing the same PostgreSQL transaction.
    # History.action_time remains the authoritative timestamp of the fact event.
    duration_seconds = F.when(
        F.col("a.app_before_updated_at").isNotNull()
        & F.col("a.app_after_updated_at").isNotNull()
        & (F.col("a.app_after_updated_at") >= F.col("a.app_before_updated_at")),
        (
            F.col("a.app_after_updated_at").cast("double")
            - F.col("a.app_before_updated_at").cast("double")
        ).cast("long"),
    )

    fact = joined.select(
        F.col("h.ho_so_id").alias("ho_so_id"),
        F.col("h.id").alias("id"),
        F.col("a.ten_ho_so").alias("ten_ho_so"),
        F.col("a.applicant_id").alias("applicant_id"),
        F.col("a.co_quan_id").alias("co_quan_id"),
        F.col("a.dv_cong_id").alias("dv_cong_id"),
        F.col("h.trang_thai_truoc_id").alias("trang_thai_truoc_id"),
        F.col("h.trang_thai_id").alias("trang_thai_id"),
        F.col("h.can_bo_id").alias("can_bo_id"),
        F.col("a.created_at").alias("created_at"),
        F.col("a.app_before_updated_at").alias("previous_action_time"),
        F.col("h.action_time").alias("action_time"),
        duration_seconds.alias("thoi_gian_xu_ly_seconds"),
        (duration_seconds / F.lit(3600.0)).alias("thoi_gian_xu_ly"),
        F.date_format(F.col("h.action_time"), "yyyyMMdd")
        .cast("int")
        .alias("thoi_gian_id"),
        F.col("h.transaction_id").alias("transaction_id"),
        F.col("a.app_event_time").alias("application_event_time"),
        F.col("h.history_event_time").alias("history_event_time"),
        F.greatest(
            F.col("a.app_kafka_timestamp"), F.col("h.history_kafka_timestamp")
        ).alias("kafka_event_time"),
        F.greatest(
            F.col("a.app_event_time"), F.col("h.history_event_time")
        ).alias("source_event_time"),
        F.col("a.application_kafka_partition").alias(
            "application_kafka_partition"
        ),
        F.col("a.application_kafka_offset").alias("application_kafka_offset"),
        F.col("h.history_kafka_partition").alias("history_kafka_partition"),
        F.col("h.history_kafka_offset").alias("history_kafka_offset"),
    ).withColumn("fact_loaded_at", F.current_timestamp())

    return (
        fact.withColumn(
            "join_skew_ms",
            F.abs(
                F.expr(
                    "unix_millis(application_event_time) - "
                    "unix_millis(history_event_time)"
                )
            ),
        )
        .withColumn(
            "kafka_to_fact_latency_ms",
            F.expr("unix_millis(fact_loaded_at) - unix_millis(kafka_event_time)"),
        )
        .withColumn(
            "source_to_fact_latency_ms",
            F.expr("unix_millis(fact_loaded_at) - unix_millis(source_event_time)"),
        )
        .drop("source_event_time")
        .select(
            "ho_so_id",
            "id",
            "ten_ho_so",
            "applicant_id",
            "co_quan_id",
            "dv_cong_id",
            "trang_thai_truoc_id",
            "trang_thai_id",
            "can_bo_id",
            "created_at",
            "previous_action_time",
            "action_time",
            "thoi_gian_xu_ly_seconds",
            "thoi_gian_xu_ly",
            "thoi_gian_id",
            "transaction_id",
            "application_event_time",
            "history_event_time",
            "kafka_event_time",
            "fact_loaded_at",
            "join_skew_ms",
            "kafka_to_fact_latency_ms",
            "source_to_fact_latency_ms",
            "application_kafka_partition",
            "application_kafka_offset",
            "history_kafka_partition",
            "history_kafka_offset",
        )
    )


def start_query(fact: DataFrame):
    columns = ",".join(fact.columns)
    return (
        fact.writeStream.format("starrocks")
        .option("starrocks.fe.http.url", STARROCKS_FE_HTTP_URL)
        .option("starrocks.fe.jdbc.url", STARROCKS_FE_JDBC_URL)
        .option("starrocks.table.identifier", STARROCKS_TABLE)
        .option("starrocks.user", STARROCKS_USER)
        .option("starrocks.password", STARROCKS_PASSWORD)
        .option("starrocks.columns", columns)
        .option("starrocks.timezone", "Asia/Ho_Chi_Minh")
        .option("starrocks.write.label.prefix", "spark-fact-xu-ly-")
        # Connector default la 300000 ms; phai ha xuong 5s de khong pha vo
        # muc tieu realtime cua Spark trigger.
        .option("starrocks.write.flush.interval.ms", "5000")
        .option("starrocks.write.buffer.rows", "50000")
        .option("starrocks.write.max.retries", "3")
        .option("starrocks.write.retry.interval.ms", "1000")
        .option("starrocks.write.num.partitions", "1")
        .option("starrocks.write.properties.strict_mode", "true")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("fact_xu_ly_ho_so_stream")
        .start()
    )


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    application = parse_application(read_topic(spark, APPLICATION_TOPIC))
    history = parse_history(read_topic(spark, HISTORY_TOPIC))
    query = start_query(build_fact(application, history))
    query.awaitTermination()


if __name__ == "__main__":
    main()
