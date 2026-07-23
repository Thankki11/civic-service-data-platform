"""
Job Rewrite Data Files (Compaction) cho cac bang Bronze Iceberg.
---------------------------------------------------------------------
Muc dich:
- Chay THUONG XUYEN (vi du: 1 tieng / 3 tieng 1 lan hoac daily).
- Gom cac file nho (small files) tu micro-batch streaming thanh file Parquet chuan (~128MB).
- KHONG XOA SNAPSHOT CU: Giup duy tri kha nang Time Travel, Backup va Rollback du lieu khi can.
"""

import os
import sys
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio_secret_key")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse/")
HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

BRONZE_TABLES = [
    "lakehouse.bronze_oltp_core.application_cdc",
    "lakehouse.bronze_oltp_core.application_history_cdc",
    "lakehouse.bronze_oltp_core.document_cdc",
    "lakehouse.bronze_oltp_core.applicant_cdc",
]

def get_spark_session():
    return SparkSession.builder \
        .appName("Bronze_Iceberg_Rewrite_Data_Files") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", HIVE_METASTORE_URI) \
        .config("spark.sql.catalog.lakehouse.warehouse", ICEBERG_WAREHOUSE) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def rewrite_table_data(spark, table_name):
    print(f"\n=======================================================")
    print(f"[INFO] REWRITE DATA FILES (COMPACTION): {table_name}")
    print(f"=======================================================")
    
    try:
        spark.read.table(table_name).limit(1).collect()
    except Exception as e:
        print(f"[WARN] Bang {table_name} chua ton tai hoac khong the doc. Bo qua. Details: {e}")
        return

    try:
        compaction_df = spark.sql(f"""
            CALL lakehouse.system.rewrite_data_files(
                table => '{table_name}',
                options => map(
                    'target-file-size-bytes', '134217728', -- Gom thanh file ~128MB
                    'min-input-files', '5'                 -- Gom neu co tu 5 file nho tro len
                )
            )
        """)
        res = compaction_df.collect()
        for row in res:
            print(f"   -> Ket qua Rewrite Data Files: {row}")
        
        # Toi uu them file manifest
        spark.sql(f"CALL lakehouse.system.rewrite_manifests(table => '{table_name}')")
        print(f"[SUCCESS] Hoan tat Compaction cho {table_name}")
    except Exception as e:
        print(f"[ERROR] Loi khi Rewrite Data Files tren {table_name}: {e}")

def run():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("[START] BAT DAU JOB REWRITE DATA FILES (COMPACTION) CHO BRONZE ICEBERG")
    for table in BRONZE_TABLES:
        rewrite_table_data(spark, table)
        
    print("\n[DONE] HOAN THANH JOB REWRITE DATA FILES.")
    spark.stop()

if __name__ == "__main__":
    run()
