from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

MINIO_ENDPOINT = "http://minio:9000"  
MINIO_ACCESS_KEY = "minio_access_key" 
MINIO_SECRET_KEY = "minio_secret_key" 
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"   


spark = SparkSession.builder \
    .appName("Bronze_Ingestion_Job1_MasterData") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hive") \
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.lakehouse.warehouse", ICEBERG_WAREHOUSE) \
    .getOrCreate()

# ==========================================
# DANH SÁCH BẢNG MASTER DATA
# ==========================================
master_tables = [
    "Province", "Ward", "Status", "Service", "Agency", 
    "Role", "Permission", "Document_Type", 
    "Officer", "Officer_Role"
]

def process_master_data():
    for table_name in master_tables:
        print(f"[*] Đang xử lý bảng Master Data: {table_name}...")
        
        
        try:
            # 1. Đọc dữ liệu từ PostgreSQL qua JDBC
            df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://source_db:5432/source_db") \
                .option("dbtable", f'"{table_name}"') \
                .option("user", "source_db") \
                .option("password", "source_db") \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            # 2. Thêm cột Audit
            df_enriched = df \
                .withColumn("ingested_at", current_timestamp()) \
                .withColumn("file_name", lit(f"jdbc:postgresql://source_db/{table_name}"))
            
            # Tên bảng đích trên Iceberg
            iceberg_table_name = f"lakehouse.bronze_master_data.{table_name.lower()}"
            
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_master_data")
            
            # Tạo schema string
            schema_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df_enriched.schema.fields])
            
            # 3. Ghi đè (Overwrite) toàn bộ dữ liệu vào bảng Iceberg trên MinIO
            print(f"[*] Đang ghi đè (Overwrite) vào Iceberg: {iceberg_table_name}...")
            
            # Tạo bảng nếu chưa có với Partitioning và Location tĩnh
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {iceberg_table_name} ({schema_sql})
                USING iceberg
                PARTITIONED BY (days(ingested_at))
                LOCATION 's3a://lakehouse/warehouse/bronze/master_data/{table_name.lower()}'
            """)
            
            df_enriched.write \
                .format("iceberg") \
                .mode("overwrite") \
                .save(iceberg_table_name)
                
            print(f"[+] Đã ghi thành công bảng: {iceberg_table_name}")
            
        except Exception as e:
            print(f"[-] Lỗi khi xử lý bảng {table_name}: {str(e)}")

if __name__ == "__main__":
    process_master_data()
    spark.stop()
