"""
Spark ETL: Bronze -> Silver. Lam sach, chuan hoa, loai trung lap (dedup).
Nguoi phu trach: Quan
Luu y: tranh OOM — dung dropDuplicates tren cot khoa, repartition hop ly,
khong collect() ve driver.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("bronze-to-silver")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hive")
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.adaptive.enabled", "true")   # AQE xu ly skew
    .getOrCreate()
)

bronze = spark.table("lakehouse.bronze.raw_xml")

silver = (
    bronze
    .dropDuplicates(["id"])                     # TODO: sua cot khoa that
    .withColumn("processed_at", F.current_timestamp())
    # TODO: chuan hoa kieu du lieu, trim, cast theo Data Dictionary cua Trung
)

silver.writeTo("lakehouse.silver.cleaned").createOrReplace()
spark.stop()
