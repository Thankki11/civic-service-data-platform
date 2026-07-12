"""
Spark Aggregation: Silver -> Gold. Tinh chi so tong hop (Sum, Count...)
theo mo hinh Dim/Fact. DDL bang Gold do Trung dinh nghia o warehouse/ddl/.
Nguoi phu trach: Quan
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("silver-to-gold")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hive")
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

silver = spark.table("lakehouse.silver.cleaned")

# TODO: build dim tables theo DDL cua Trung (warehouse/ddl/)
fact = (
    silver.groupBy("dim_key_1", "dim_key_2")     # TODO
    .agg(
        F.count("*").alias("total_records"),
        F.sum("amount").alias("total_amount"),   # TODO
    )
)

fact.writeTo("lakehouse.gold.fact_main").createOrReplace()
spark.stop()
