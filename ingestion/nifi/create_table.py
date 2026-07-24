"""Pipeline bronze_api version 1.0.0: chi khoi tao bang Iceberg."""
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("CreateIcebergTable")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.lakehouse.type", "hive")
    .config(
        "spark.sql.catalog.lakehouse.uri",
        "thrift://hive-metastore:9083",
    )
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://lakehouse/warehouse/",
    )
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_api")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS lakehouse.bronze_api.payment_transactions (
        id_ban_ghi STRING,
        payment_status STRING,
        tax_code STRING,
        amount DOUBLE,
        method STRING,
        timestamp STRING,
        file_name STRING,
        ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(ingested_at))
    LOCATION 's3a://lakehouse/warehouse/bronze/api/payment_transaction'
    """
)
print("TABLE CREATED SUCCESSFULLY")
spark.stop()
