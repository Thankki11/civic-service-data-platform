from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

def process_stream():
    # Khởi tạo Spark Session với các config cho Kafka, Iceberg
    spark = SparkSession.builder \
        .appName("Realtime CDC Streaming with Iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_access_key") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.cores.max", "6") \
        .config("spark.executor.cores", "3") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Hàm tạo luồng Streaming chung cho các bảng phẳng
    def create_cdc_stream(table_name, after_schema, iceberg_table):
        kafka_topic = f"postgres_server.public.{table_name}"
        print(f"[*] Bắt đầu đọc luồng từ Kafka topic: {kafka_topic}...")
        
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        root_schema = StructType([
            StructField("payload", StructType([
                StructField("after", after_schema, True),
                StructField("op", StringType(), True)
            ]), True)
        ])

        # Debezium updated_at có thể ở dạng microseconds, chia cho 1000000 để ra giây
        df_final = df_stream.selectExpr("CAST(value AS STRING) as json_payload") \
            .withColumn("root", from_json(col("json_payload"), root_schema)) \
            .select("root.payload.after.*", "root.payload.op") \
            .filter("op IN ('c', 'u')") \
            .withColumn("ingested_at", current_timestamp())

        # Nếu có cột updated_at hoặc action_time dạng Long từ Debezium (thường là microsecond)
        if "updated_at" in df_final.columns:
            df_final = df_final.withColumn("updated_at", (col("updated_at") / 1000000).cast(TimestampType()))
        if "created_at" in df_final.columns:
            df_final = df_final.withColumn("created_at", (col("created_at") / 1000000).cast(TimestampType()))
        if "action_time" in df_final.columns:
            df_final = df_final.withColumn("action_time", (col("action_time") / 1000000).cast(TimestampType()))
        
        df_final = df_final \
            .withColumn("ingested_at", current_timestamp())

        # Schema của Iceberg table dựa trên schema của df_final
        schema_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in df_final.schema.fields])
        
        table_path = f"lakehouse.bronze_oltp_core.{iceberg_table}"
        
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_oltp_core")
        
        # Tạo bảng nếu chưa có với Partitioning
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_path} ({schema_sql})
            USING iceberg
            PARTITIONED BY (days(ingested_at))
            LOCATION 's3a://lakehouse/warehouse/bronze/oltp_core/{iceberg_table}'
        """)

        def write_to_iceberg(batch_df, batch_id):
            batch_df.write.format("iceberg").mode("append").save(table_path)

        query = df_final.writeStream \
            .foreachBatch(write_to_iceberg) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", f"s3a://lakehouse/checkpoints/{iceberg_table}") \
            .start()
            
        return query

    # =================================================================================
    # LUỒNG 1: APPLICATION
    # =================================================================================
    app_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("created_at", LongType(), True),
        StructField("Applicantid", StringType(), True),
        StructField("Statusid", IntegerType(), True),
        StructField("Serviceid", IntegerType(), True),
        StructField("Agencyid", IntegerType(), True),
        StructField("updated_at", LongType(), True)
    ])
    q_app = create_cdc_stream("Application", app_schema, "application_cdc")

    # =================================================================================
    # LUỒNG 2: APPLICATION HISTORY
    # =================================================================================
    hist_schema = StructType([
        StructField("id", StringType(), True),
        StructField("Applicationid", StringType(), True),
        StructField("Statusid", IntegerType(), True),
        StructField("Statusid2", IntegerType(), True),
        StructField("Officerid", IntegerType(), True),
        StructField("action_time", LongType(), True),
        StructField("note", StringType(), True)
    ])
    q_hist = create_cdc_stream("Application_History", hist_schema, "application_history_cdc")

    # =================================================================================
    # LUỒNG 3: DOCUMENT
    # =================================================================================
    doc_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("Applicationid", StringType(), True),
        StructField("file_url", StringType(), True),
        StructField("Document_Typeid", IntegerType(), True)
    ])
    q_doc = create_cdc_stream("Document", doc_schema, "document_cdc")

    # =================================================================================
    # LUỒNG 4: APPLICANT
    # =================================================================================
    applicant_schema = StructType([
        StructField("id", StringType(), True),
        StructField("identity_num", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("password", StringType(), True),
        StructField("Provinceid", IntegerType(), True),
        StructField("Wardid", IntegerType(), True),
        StructField("updated_at", LongType(), True)
    ])
    q_applicant = create_cdc_stream("Applicant", applicant_schema, "applicant_cdc")

    # Chờ tất cả các luồng
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
