"""
Spark Batch: parse file XML tu Landing Zone (MinIO) -> bang Bronze (Iceberg).
Nguoi phu trach: Kien
Chay: spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,com.databricks:spark-xml_2.12:0.18.0 ...
Luu y: schema XML co the thay doi theo ban dump -> nen doc schema linh hoat.
"""
from pyspark.sql import SparkSession

LANDING = "s3a://landing-zone/xml/"
BRONZE_TABLE = "lakehouse.bronze.raw_xml"   # catalog.iceberg

spark = (
    SparkSession.builder
    .appName("parse-xml-to-bronze")
    # Iceberg catalog tro toi Hive Metastore + MinIO
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hive")
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

df = (
    spark.read.format("xml")
    .option("rowTag", "record")        # TODO: sua theo cau truc XML that
    .load(LANDING)
)

# TODO: them cot audit (ingest_time, source_file) truoc khi ghi
df.writeTo(BRONZE_TABLE).createOrReplace()

spark.stop()
