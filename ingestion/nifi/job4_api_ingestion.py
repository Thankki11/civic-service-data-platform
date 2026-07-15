from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"

# Đọc file JSON từ NiFi bắn vào Landing Zone
LANDING_ZONE_PATH = "s3a://landing-zone/api_payments_*.json"
BRONZE_TABLE = "lakehouse.bronze_api.payment_transactions"


spark = SparkSession.builder \
    .appName("Bronze_Ingestion_Job4_API_JSON") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.1,org.apache.hadoop:hadoop-aws:3.3.4") \
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
    .config("spark.cores.max", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

# ==========================================
# SCHEMA CỦA JSON API
# ==========================================
json_schema = StructType([
    StructField("id_ban_ghi", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("tax_code", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("method", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def process_api_json():
    logging.info(f"[*] Bắt đầu quyét thư mục {LANDING_ZONE_PATH}...")
    
    # Sử dụng try-except vì nếu không có file nào khớp pattern, Spark có thể ném lỗi AnalysisException
    try:
        # Pyspark đọc JSON, nhưng JSON của mock-api là 1 Array các Object: [{}, {}]
        # Pyspark có thể tự động parse array JSON nếu dùng spark.read.json với multiLine=True?
        # Mặc định spark.read.json xử lý JSON lines, nhưng nếu dữ liệu là Array JSON thì phải multiLine=True
        df_raw = spark.read \
            .schema(json_schema) \
            .option("multiLine", True) \
            .json(LANDING_ZONE_PATH)
    except Exception as e:
        logging.info("Không có file JSON mới nào hoặc lỗi phân tích: " + str(e))
        return

    # Lấy danh sách các file đang được đọc để xóa sau khi load xong
    files_to_delete = df_raw.inputFiles()
    
    if not files_to_delete:
        logging.info("Không có file JSON mới nào trong Landing Zone Kết thúc Job.")
        return
        
    logging.info(f"[*] Đã tìm thấy {len(files_to_delete)} file JSON mới.")

    # Thêm Metadata Audit
    df_bronze = df_raw \
        .withColumn("file_name", input_file_name()) \
        .withColumn("ingested_at", current_timestamp())

    # Ghi vào Iceberg
    logging.info(f"[*] Đang ghi dữ liệu vào bảng Iceberg: {BRONZE_TABLE}...")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_api")
    
    # Tạo schema string
    schema_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df_bronze.schema.fields])
    
    # Tạo bảng với partition
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} ({schema_sql})
        USING iceberg
        PARTITIONED BY (days(ingested_at))
        LOCATION 's3a://lakehouse/warehouse/bronze/api/payment_transaction'
    """)

    
    df_bronze.write \
        .format("iceberg") \
        .mode("append") \
        .save(BRONZE_TABLE)
        
    logging.info("[+] Đã nạp thành công vào Bronze!")

    # ==========================================
    # TRANSIENT CLEANUP: Xóa file khỏi Landing Zone
    # ==========================================
    logging.info("[*] Đang thực hiện dọn dẹp (xóa) các file JSON đã xử lý...")
    Path = spark._jvm.org.apache.hadoop.fs.Path
    hadoop_conf = spark._jsc.hadoopConfiguration()
    
    deleted_count = 0
    for file_uri in files_to_delete:
        try:
            path_obj = Path(file_uri)
            fs = path_obj.getFileSystem(hadoop_conf)
            if fs.delete(path_obj, False):
                deleted_count += 1
        except Exception as e:
            logging.error(f"Lỗi khi xóa file {file_uri}: {e}")
            
    logging.info(f"Đã dọn dẹp sạch sẽ {deleted_count} file JSON khỏi Landing Zone.")

if __name__ == "__main__":
    process_api_json()
    spark.stop()
