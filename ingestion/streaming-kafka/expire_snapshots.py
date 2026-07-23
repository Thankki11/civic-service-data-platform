"""
Job Expire Snapshots & Clean Storage cho cac bang Bronze Iceberg.
---------------------------------------------------------------------
Muc dich:
- Chay THUA HON (vi du: Hang tuan hoac Hang thang).
- Xoa cac Snapshot qua han (vi du: giu lai 7 - 14 ngay cho muc dich backup / audit / rollback).
- Don dep vat ly cac file rac (orphan files) thuc su khong con dung den khoi MinIO (S3).
"""

import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio_secret_key")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse/")
HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

# So ngay giu snapshot cho muc dich backup / rollback (mac dinh 7 ngay)
RETAIN_DAYS = int(os.environ.get("RETAIN_DAYS", "7"))
RETAIN_LAST_SNAPSHOTS = int(os.environ.get("RETAIN_LAST_SNAPSHOTS", "10"))

BRONZE_TABLES = [
    "lakehouse.bronze_oltp_core.application_cdc",
    "lakehouse.bronze_oltp_core.application_history_cdc",
    "lakehouse.bronze_oltp_core.document_cdc",
    "lakehouse.bronze_oltp_core.applicant_cdc",
]

def get_spark_session():
    return SparkSession.builder \
        .appName("Bronze_Iceberg_Expire_Snapshots") \
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

def expire_table_snapshots(spark, table_name):
    print(f"\n=======================================================")
    print(f"[INFO] EXPIRE SNAPSHOTS & CLEAN STORAGE: {table_name}")
    print(f"=======================================================")
    
    try:
        spark.read.table(table_name).limit(1).collect()
    except Exception as e:
        print(f"[WARN] Bang {table_name} chua ton tai hoac khong the doc. Bo qua. Details: {e}")
        return

    older_than_ts = (datetime.now() - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"-> Giu snapshot trong {RETAIN_DAYS} ngay gan nhat (Xoa snapshot truoc thoi diem: {older_than_ts})")

    try:
        expire_df = spark.sql(f"""
            CALL lakehouse.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{older_than_ts}',
                retain_last => {RETAIN_LAST_SNAPSHOTS}
            )
        """)
        res = expire_df.collect()
        for row in res:
            print(f"   -> Ket qua Expire Snapshots: {row}")
            
        # Don dep file mo coi (Orphan files)
        print(f"   -> Dang don dep orphan files cho {table_name}...")
        spark.sql(f"CALL lakehouse.system.remove_orphan_files(table => '{table_name}')")
        print(f"[SUCCESS] Hoan tat Expire Snapshots cho {table_name}")
    except Exception as e:
        print(f"[ERROR] Loi khi Expire Snapshots tren {table_name}: {e}")

def run():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("[START] BAT DAU JOB EXPIRE SNAPSHOTS (RETENTION & CLEANUP) CHO BRONZE ICEBERG")
    for table in BRONZE_TABLES:
        expire_table_snapshots(spark, table)
        
    print("\n[DONE] HOAN THANH JOB EXPIRE SNAPSHOTS.")
    spark.stop()

if __name__ == "__main__":
    run()
