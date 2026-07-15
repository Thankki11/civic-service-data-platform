from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"
BRONZE_TABLE = "lakehouse.bronze_api.payment_transactions"

spark = SparkSession.builder \
    .appName("CreateIcebergTable") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hive") \
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.lakehouse.warehouse", ICEBERG_WAREHOUSE) \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_api")
spark.sql("""
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
""")
print("TABLE CREATED SUCCESSFULLY!")
