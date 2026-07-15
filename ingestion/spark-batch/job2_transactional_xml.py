from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col, explode, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
ICEBERG_WAREHOUSE = "s3a://lakehouse/warehouse/"
# Pattern đệ quy: đọc tất cả các file .xml trong các thư mục con theo cấu trúc ngày tháng
LANDING_ZONE_PATH = "s3a://landing-zone/raw/xml/*/*/*/*/*/*.xml"


spark = SparkSession.builder \
    .appName("Bronze_Ingestion_Job2_TransactionalXML") \
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
    .getOrCreate()


# SCHEMA & UDF ĐỂ PARSE VÀ VALIDATE XML

# Khai báo Schema cho hàm UDF trả về gồm 3 phần: Trạng thái hợp lệ, Lỗi (nếu có), và Mảng dữ liệu Packet
udf_schema = StructType([
    StructField("is_valid", BooleanType(), True),
    StructField("error_msg", StringType(), True),
    StructField("packets", ArrayType(StructType([
        StructField("ma_goi_tin", StringType(), True),
        StructField("ma_du_lieu", StringType(), True),
        StructField("loai_du_lieu", StringType(), True),
        StructField("ngay_cap_nhat", StringType(), True),
        StructField("su_kien", StringType(), True),
        StructField("id_ban_ghi", StringType(), True),
        StructField("data_payload", StringType(), True)
    ])), True)
])

@udf(udf_schema)
def validate_and_extract_udf(xml_string):
    """
    Python UDF: Vừa bóc tách dữ liệu, vừa kiểm tra Schema (Validate).
    Nếu lỗi cấu trúc hoặc parse hỏng, sẽ ném cờ is_valid = False kèm theo lý do để đẩy vào Quarantine.
    """
    import xml.etree.ElementTree as ET
    import json
    
    def xml_to_dict(elem):
        d = {}
        if elem.attrib:
            d.update({('@' + k): v for k, v in elem.attrib.items()})
        if len(elem) > 0:
            for child in elem:
                val = xml_to_dict(child)
                if child.tag in d:
                    if type(d[child.tag]) is list:
                        d[child.tag].append(val)
                    else:
                        d[child.tag] = [d[child.tag], val]
                else:
                    d[child.tag] = val
        else:
            if not d:
                return elem.text
            else:
                d['#text'] = elem.text
        return d
        
    if not xml_string or not xml_string.strip():
        return (False, "File rỗng hoặc không có nội dung", [])
        
    try:
        # Pyspark đọc wholetext trả về chuỗi Unicode (str).
        # Nếu file có dòng <?xml version='1.0' encoding='UTF-8'?>, ElementTree trên Python 3 
        # sẽ báo lỗi ValueError: Unicode strings with encoding declaration are not supported.
        # Khắc phục: Chuyển string thành dạng bytes bằng cách encode('utf-8') trước khi parse.
        root = ET.fromstring(xml_string.encode('utf-8'))
        packets = [root] if root.tag == 'packet' else root.findall('.//packet')
        
        if not packets:
            return (False, "Không tìm thấy thẻ <packet> nào trong file", [])
            
        results = []
        for packet in packets:
            metadata = packet.find("metadata")
            if metadata is None:
                return (False, "Thiếu thẻ <metadata> quan trọng", [])
                
            # Dùng strip() để loại bỏ các dấu space/newline thừa nếu có
            ma_goi_tin = (metadata.findtext("ma_goi_tin") or "").strip()
            ma_du_lieu = (metadata.findtext("ma_du_lieu") or "").strip()
            loai_du_lieu = (metadata.findtext("loai_du_lieu") or "").strip()
            ngay_cap_nhat = (metadata.findtext("ngay_cap_nhat") or "").strip()
            su_kien = (metadata.findtext("su_kien") or "").strip()
            id_ban_ghi = (metadata.findtext("id_ban_ghi") or "").strip()
            
            # Sàng lọc (Validate chuẩn thực chiến): Kiểm tra toàn diện
            if not ma_goi_tin:
                return (False, "Thiếu mã gói tin (ma_goi_tin)", [])
            if not su_kien or su_kien not in ['INSERT', 'UPDATE', 'DELETE']:
                return (False, f"Sự kiện không hợp lệ hoặc bị thiếu (su_kien: {su_kien})", [])
            if not id_ban_ghi:
                return (False, "Thiếu ID bản ghi hồ sơ (id_ban_ghi) - Không thể merge ở Silver", [])
            
            du_lieu_node = packet.find("du_lieu")
            if du_lieu_node is None:
                return (False, "Thiếu thẻ <du_lieu> bao bọc payload", [])
                
            # Chuyển đổi thẻ <du_lieu> từ XML sang chuỗi JSON để lưu vào Bronze
            data_payload = json.dumps(xml_to_dict(du_lieu_node), ensure_ascii=False)
            
            results.append((ma_goi_tin, ma_du_lieu, loai_du_lieu, ngay_cap_nhat, su_kien, id_ban_ghi, data_payload))
            
        return (True, None, results) # PASS: Hợp lệ
        
    except ET.ParseError as e:
        return (False, f"Lỗi cú pháp XML (Parse Error): {str(e)}", []) # FAIL: Rác/Hỏng cấu trúc
    except Exception as e:
        return (False, f"Lỗi không xác định: {str(e)}", []) # FAIL: Lỗi khác

# ==========================================
# HÀM XỬ LÝ CHÍNH THEO LUỒNG QUARANTINE
# ==========================================
def process_transactional_data():
    print("[*] Đang đọc Transactional XML từ Landing Zone...")
    
    # 1. Đọc nội dung thô (Raw) từ TẤT CẢ thư mục con (INSERT, UPDATE_...)
    # Bắt buộc phải dùng tham số wholetext=True trong hàm text() để Spark đọc toàn bộ file thành 1 chuỗi (1 dòng)
    df_raw = spark.read \
        .text(LANDING_ZONE_PATH, wholetext=True) \
        .withColumn("file_name", input_file_name()) \
        .withColumn("ingested_at", current_timestamp())
    
    # 2. Chạy bước Validate (Sàng lọc bằng Spark)
    df_processed = df_raw.withColumn("validation_result", validate_and_extract_udf(col("value")))
    
    # 3. Phân luồng: PASS và FAIL (Quarantine)
    df_pass = df_processed.filter(col("validation_result.is_valid") == True)
    df_quarantine = df_processed.filter(col("validation_result.is_valid") == False)
    
    # =========================================================
    # LUỒNG 1: PASS -> LƯU VÀO BRONZE (DỮ LIỆU CHUẨN)
    # =========================================================
    df_pass_final = df_pass.select(
        "file_name",
        "ingested_at",
        explode(col("validation_result.packets")).alias("packet")
    ).select(
        col("packet.ma_goi_tin"),
        col("packet.ma_du_lieu"),
        col("packet.loai_du_lieu"),
        col("packet.ngay_cap_nhat"),
        col("packet.su_kien"),
        col("packet.id_ban_ghi"),
        col("packet.data_payload"),
        col("file_name"),
        col("ingested_at")
    )
    
    print(f"[*] Đang ghi dữ liệu chuẩn (PASS) vào bảng: lakehouse.bronze_dvc_xml.application_xml...")
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_dvc_xml")
    
    # Tạo bảng nếu chưa tồn tại với cấu hình Partitioning theo ngày nạp (days(ingested_at))
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.bronze_dvc_xml.application_xml (
            ma_goi_tin STRING,
            ma_du_lieu STRING,
            loai_du_lieu STRING,
            ngay_cap_nhat STRING,
            su_kien STRING,
            id_ban_ghi STRING,
            data_payload STRING,
            file_name STRING,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
        LOCATION 's3a://lakehouse/warehouse/bronze/dvc_xml/application_xml'
    """)
    
    df_pass_final.write.format("iceberg").mode("append").saveAsTable("lakehouse.bronze_dvc_xml.application_xml")
    
    # =========================================================
    # LUỒNG 2: FAIL -> QUARANTINE ZONE (LỖI XML, RÁC)
    # =========================================================
    # Lưu trữ bằng chứng đối soát, bao gồm toàn bộ nội dung file gốc và nguyên nhân lỗi
    df_quarantine_final = df_quarantine.select(
        col("file_name"),
        col("value").alias("raw_content"),
        col("validation_result.error_msg").alias("error_reason"),
        col("ingested_at")
    )
    
    quarantine_path = "s3a://quarantine-zone/xml_errors/"
    print(f"[*] Đang lưu các file rác/lỗi (FAIL) ra ngoài (Quarantine Bucket): {quarantine_path}...")
    
    # Lưu bằng chứng đối soát dưới dạng JSON file thô thay vì nạp vào kho Iceberg Bronze
    df_quarantine_final.write \
        .format("json") \
        .mode("append") \
        .save(quarantine_path)
    
    print("[+] Đã hoàn thành quá trình Ingestion và Sàng lọc!")

if __name__ == "__main__":
    process_transactional_data()
    spark.stop()
