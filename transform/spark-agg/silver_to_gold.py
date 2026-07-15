"""Create the two daily batch Gold facts from Silver application entities.

The job is intentionally parameterised by an *as-of date*.  Re-running a day
first deletes only that Iceberg partition and then writes it again, so it is
safe for Airflow retries and for backfilling the supplied June/July XML archive.

Dimension tables are not rebuilt here.  They are small controlled reference
datasets and must be loaded into ``lakehouse.gold`` before this job runs.
"""

import argparse
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


SILVER_EVENTS = "lakehouse.silver.application_events"
SILVER_HISTORY = "lakehouse.silver.application_history"
SILVER_PAYMENT = "lakehouse.silver.payment"

DIM_TIME = "lakehouse.gold.dim_thoi_gian"
DIM_STATUS = "lakehouse.gold.dim_trang_thai"
DIM_SERVICE = "lakehouse.gold.dim_dich_vu_cong"
FACT_BACKLOG = "lakehouse.gold.fact_ton_dong_ho_so"
FACT_AGENCY = "lakehouse.gold.fact_van_hanh_co_quan"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily Gold public-service facts")
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Snapshot date in YYYY-MM-DD; default is the Spark job's current date.",
    )
    args = parser.parse_args()
    try:
        args.as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    except ValueError as exc:
        parser.error("--as-of-date phai co dang YYYY-MM-DD")
        raise exc  # Kept for static analysers; argparse.error exits.
    return args


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver-to-gold-dvc")
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


def require_tables(spark: SparkSession, table_names: list[str]) -> None:
    missing = [name for name in table_names if not spark.catalog.tableExists(name)]
    if missing:
        raise RuntimeError(
            "Thieu bang bat buoc: "
            + ", ".join(missing)
            + ". Hay chay DDL Gold va nap dimension thu cong truoc khi aggregate."
        )


def ensure_fact_tables(spark: SparkSession) -> None:
    """Keep the job deployable even when only the DDL has not been run yet."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FACT_BACKLOG} (
            id STRING, ho_so_id STRING, trang_thai_id INT, co_quan_id INT,
            can_bo_id INT, dv_cong_id INT, so_ngay_ton_dong_hien_tai INT,
            tong_thoi_gian_da_xu_ly INT, so_luong INT, thoi_gian_id INT
        ) USING iceberg PARTITIONED BY (thoi_gian_id)
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FACT_AGENCY} (
            id STRING, co_quan_id INT, so_luong_tiep_nhan INT,
            so_luong_dung_han INT, so_luong_tre_han INT, so_luong_rework INT,
            so_luong_ton_dong INT, tong_chi_phi BIGINT, thoi_gian_id INT
        ) USING iceberg PARTITIONED BY (thoi_gian_id)
        """
    )


def latest_application_as_of(events: DataFrame, cutoff_ts: str) -> DataFrame:
    """Reconstruct the application state valid at cutoff from full + partial events."""
    state_window = (
        Window.partitionBy("ho_so_id")
        .orderBy(F.col("event_ts").asc_nulls_last(), F.col("ma_goi_tin").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    latest_window = Window.partitionBy("ho_so_id").orderBy(
        F.col("event_ts").desc_nulls_last(), F.col("ma_goi_tin").desc()
    )
    return (
        events.filter(F.col("event_ts") <= F.lit(cutoff_ts).cast("timestamp"))
        .withColumn("ten_ho_so", F.last("ten_ho_so", ignorenulls=True).over(state_window))
        .withColumn("dv_cong_id", F.last("dv_cong_id", ignorenulls=True).over(state_window))
        .withColumn("co_quan_id", F.last("co_quan_id", ignorenulls=True).over(state_window))
        .withColumn("created_at", F.last("created_at", ignorenulls=True).over(state_window))
        .withColumn("trang_thai_id", F.last("trang_thai_id", ignorenulls=True).over(state_window))
        .withColumn("is_deleted", F.col("su_kien") == F.lit("DELETE"))
        .withColumn("_latest_rank", F.row_number().over(latest_window))
        .filter((F.col("_latest_rank") == 1) & ~F.col("is_deleted"))
        .drop("_latest_rank")
    )


def build_calendar(spark: SparkSession, as_of_date: date) -> tuple[DataFrame, DataFrame]:
    """Expose working-day sequence values for O(1) ageing/SLA calculations."""
    calendar = spark.table(DIM_TIME).select(
        "thoi_gian_id",
        F.col("ngay").cast("date").alias("ngay"),
        F.coalesce(F.col("co_phai_la_ngay_nghi"), F.lit(False)).alias("co_phai_la_ngay_nghi"),
        F.col("stt_ngay_lam_viec").cast("int").alias("stt_ngay_lam_viec"),
    )
    as_of_calendar = calendar.filter(F.col("ngay") == F.lit(as_of_date.isoformat()).cast("date"))
    if as_of_calendar.limit(1).count() == 0:
        raise RuntimeError(f"dim_thoi_gian chua co ngay chot {as_of_date.isoformat()}")
    return calendar, as_of_calendar.select(
        F.col("thoi_gian_id").alias("as_of_thoi_gian_id"),
        F.col("stt_ngay_lam_viec").alias("as_of_workday_seq"),
    )


def add_business_age(applications: DataFrame, calendar: DataFrame, as_of_calendar: DataFrame) -> DataFrame:
    """Add application age and current-status age using manual calendar reference data."""
    start_calendar = calendar.select(
        F.col("ngay").alias("created_date"), F.col("stt_ngay_lam_viec").alias("created_workday_seq")
    )
    status_calendar = calendar.select(
        F.col("ngay").alias("status_date"), F.col("stt_ngay_lam_viec").alias("status_workday_seq")
    )
    aged = (
        applications.join(start_calendar, F.to_date("created_at") == F.col("created_date"), "left")
        .join(status_calendar, F.to_date("last_action_time") == F.col("status_date"), "left")
        .crossJoin(as_of_calendar)
        .withColumn(
            "tong_thoi_gian_da_xu_ly",
            F.greatest(F.coalesce(F.col("as_of_workday_seq") - F.col("created_workday_seq"), F.lit(0)), F.lit(0)),
        )
        .withColumn(
            "so_ngay_ton_dong_hien_tai",
            F.greatest(
                F.coalesce(F.col("as_of_workday_seq") - F.col("status_workday_seq"), F.lit(0)), F.lit(0)
            ),
        )
    )
    return aged


def completed_metrics(
    history: DataFrame,
    applications: DataFrame,
    calendar: DataFrame,
    as_of_date: date,
) -> DataFrame:
    """Count same-day COMPLETED transitions, split by business-day SLA."""
    status = spark_table_with_alias(history.sparkSession, DIM_STATUS, "status").select(
        F.col("trang_thai_id").alias("completed_status_id"), F.col("ma_trang_thai").alias("completed_status_code")
    )
    start_calendar = calendar.select(
        F.col("ngay").alias("created_date"), F.col("stt_ngay_lam_viec").alias("created_workday_seq")
    )
    end_calendar = calendar.select(
        F.col("ngay").alias("completed_date"), F.col("stt_ngay_lam_viec").alias("completed_workday_seq")
    )
    completed = (
        history.filter(F.to_date("action_time") == F.lit(as_of_date.isoformat()).cast("date"))
        .join(status, F.col("trang_thai_id") == F.col("completed_status_id"), "inner")
        .filter(F.col("completed_status_code") == F.lit("COMPLETED"))
        .join(
            applications.select("ho_so_id", "co_quan_id", "dv_cong_id", "created_at"),
            "ho_so_id",
            "inner",
        )
        .join(start_calendar, F.to_date("created_at") == F.col("created_date"), "left")
        .join(end_calendar, F.to_date("action_time") == F.col("completed_date"), "left")
        .withColumn(
            "business_days_to_complete",
            F.greatest(
                F.coalesce(F.col("completed_workday_seq") - F.col("created_workday_seq"), F.lit(0)), F.lit(0)
            ),
        )
        .dropDuplicates(["ho_so_id"])
    )
    service = spark_table_with_alias(history.sparkSession, DIM_SERVICE, "service").select(
        F.col("dv_cong_id").alias("service_id"), F.col("thoi_han_tra_kq_ngay").alias("sla_ngay")
    )
    return (
        completed.join(service, F.col("dv_cong_id") == F.col("service_id"), "left")
        .groupBy("co_quan_id")
        .agg(
            F.sum(F.when(F.col("business_days_to_complete") > F.col("sla_ngay"), 1).otherwise(0)).cast("int").alias(
                "so_luong_tre_han"
            ),
            F.sum(F.when(F.col("business_days_to_complete") <= F.col("sla_ngay"), 1).otherwise(0)).cast("int").alias(
                "so_luong_dung_han"
            ),
        )
    )


def spark_table_with_alias(spark: SparkSession, table: str, alias: str) -> DataFrame:
    """Small helper keeping joins readable without relying on SQL strings."""
    return spark.table(table).alias(alias)


def overwrite_partition(spark: SparkSession, table: str, date_key: int, dataframe: DataFrame) -> None:
    """Idempotently replace exactly one reporting-date partition."""
    spark.sql(f"DELETE FROM {table} WHERE thoi_gian_id = {date_key}")
    dataframe.writeTo(table).append()


def main() -> None:
    args = parse_args()
    as_of_date: date = args.as_of_date
    cutoff_ts = f"{as_of_date.isoformat()} 23:59:59"
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_fact_tables(spark)
        require_tables(spark, [SILVER_EVENTS, SILVER_HISTORY, SILVER_PAYMENT, DIM_TIME, DIM_STATUS, DIM_SERVICE])

        calendar, as_of_calendar = build_calendar(spark, as_of_date)
        events = spark.table(SILVER_EVENTS)
        history = spark.table(SILVER_HISTORY).filter(F.col("action_time") <= F.lit(cutoff_ts).cast("timestamp"))
        payments = spark.table(SILVER_PAYMENT).filter(F.col("paid_at") <= F.lit(cutoff_ts).cast("timestamp"))
        applications = latest_application_as_of(events, cutoff_ts)

        latest_history_window = Window.partitionBy("ho_so_id").orderBy(
            F.col("action_time").desc_nulls_last(), F.col("history_id").desc()
        )
        last_history = (
            history.withColumn("_last_history_rank", F.row_number().over(latest_history_window))
            .filter(F.col("_last_history_rank") == 1)
            .select(
                "ho_so_id",
                F.col("can_bo_id").alias("last_can_bo_id"),
                F.col("action_time").alias("last_action_time"),
            )
        )
        status = spark.table(DIM_STATUS).select("trang_thai_id", "ma_trang_thai")
        service_sla = spark.table(DIM_SERVICE).select(
            F.col("dv_cong_id").alias("sla_dv_cong_id"),
            F.col("thoi_han_tra_kq_ngay").cast("int").alias("sla_ngay"),
        )
        application_state = (
            applications.join(last_history, "ho_so_id", "left")
            .join(status, "trang_thai_id", "left")
            .join(service_sla, F.col("dv_cong_id") == F.col("sla_dv_cong_id"), "left")
            .withColumn("can_bo_id", F.coalesce("last_can_bo_id", F.lit(-1)).cast("int"))
            .withColumn("last_action_time", F.coalesce("last_action_time", "created_at"))
        )
        aged_state = add_business_age(application_state, calendar, as_of_calendar)
        date_key = int(as_of_date.strftime("%Y%m%d"))

        # Fact 1: exact day-end inventory, excluding both terminal outcomes.
        backlog = (
            aged_state.filter(~F.col("ma_trang_thai").isin("COMPLETED", "REJECTED"))
            .select(
                F.sha2(F.concat_ws("|", F.lit(str(date_key)), "ho_so_id"), 256).alias("id"),
                "ho_so_id",
                "trang_thai_id",
                "co_quan_id",
                "can_bo_id",
                "dv_cong_id",
                F.col("so_ngay_ton_dong_hien_tai").cast("int"),
                F.col("tong_thoi_gian_da_xu_ly").cast("int"),
                F.lit(1).cast("int").alias("so_luong"),
                F.lit(date_key).cast("int").alias("thoi_gian_id"),
            )
            .repartition("co_quan_id")
        )
        overwrite_partition(spark, FACT_BACKLOG, date_key, backlog)

        # Daily flow measures. REJECTED is used as the actual supplied-data
        # rework signal: the source has no separate PENDING/BOSUNG status.
        received = (
            applications.filter(F.to_date("created_at") == F.lit(as_of_date.isoformat()).cast("date"))
            .groupBy("co_quan_id")
            .agg(F.countDistinct("ho_so_id").cast("int").alias("so_luong_tiep_nhan"))
        )
        rework = (
            history.filter(F.to_date("action_time") == F.lit(as_of_date.isoformat()).cast("date"))
            .join(status, "trang_thai_id", "left")
            .filter(F.col("ma_trang_thai") == F.lit("REJECTED"))
            .join(applications.select("ho_so_id", "co_quan_id"), "ho_so_id", "inner")
            .groupBy("co_quan_id")
            .agg(F.countDistinct("ho_so_id").cast("int").alias("so_luong_rework"))
        )
        revenue = (
            payments.filter(
                (F.to_date("paid_at") == F.lit(as_of_date.isoformat()).cast("date"))
                & (F.col("payment_status") == F.lit("SUCCESS"))
            )
            .join(applications.select("ho_so_id", "co_quan_id"), "ho_so_id", "inner")
            .groupBy("co_quan_id")
            .agg(F.coalesce(F.sum("so_tien"), F.lit(0)).cast("bigint").alias("tong_chi_phi"))
        )
        backlog_metrics = backlog.groupBy("co_quan_id").agg(F.sum("so_luong").cast("int").alias("so_luong_ton_dong"))
        # The leadership overdue KPI includes both completed-late applications
        # and applications that are still open but have already exceeded SLA.
        overdue_open = (
            aged_state.filter(
                ~F.col("ma_trang_thai").isin("COMPLETED", "REJECTED")
                & (F.col("tong_thoi_gian_da_xu_ly") > F.coalesce(F.col("sla_ngay"), F.lit(0)))
            )
            .groupBy("co_quan_id")
            .agg(F.countDistinct("ho_so_id").cast("int").alias("so_luong_tre_han_dang_mo"))
        )
        completed = completed_metrics(history, applications, calendar, as_of_date)

        agency_keys = (
            received.select("co_quan_id")
            .unionByName(rework.select("co_quan_id"))
            .unionByName(revenue.select("co_quan_id"))
            .unionByName(backlog_metrics.select("co_quan_id"))
            .unionByName(overdue_open.select("co_quan_id"))
            .unionByName(completed.select("co_quan_id"))
            .filter(F.col("co_quan_id").isNotNull())
            .dropDuplicates()
        )
        agency_fact = (
            agency_keys.join(received, "co_quan_id", "left")
            .join(completed, "co_quan_id", "left")
            .join(rework, "co_quan_id", "left")
            .join(backlog_metrics, "co_quan_id", "left")
            .join(overdue_open, "co_quan_id", "left")
            .join(revenue, "co_quan_id", "left")
            .fillna(
                0,
                [
                    "so_luong_tiep_nhan",
                    "so_luong_dung_han",
                    "so_luong_tre_han",
                    "so_luong_tre_han_dang_mo",
                    "so_luong_rework",
                    "so_luong_ton_dong",
                    "tong_chi_phi",
                ],
            )
            .select(
                F.concat_ws("|", F.lit(str(date_key)), F.col("co_quan_id").cast("string")).alias("id"),
                F.col("co_quan_id").cast("int"),
                F.col("so_luong_tiep_nhan").cast("int"),
                F.col("so_luong_dung_han").cast("int"),
                (F.col("so_luong_tre_han") + F.col("so_luong_tre_han_dang_mo")).cast("int").alias("so_luong_tre_han"),
                F.col("so_luong_rework").cast("int"),
                F.col("so_luong_ton_dong").cast("int"),
                F.col("tong_chi_phi").cast("bigint"),
                F.lit(date_key).cast("int").alias("thoi_gian_id"),
            )
            .repartition("co_quan_id")
        )
        overwrite_partition(spark, FACT_AGENCY, date_key, agency_fact)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
