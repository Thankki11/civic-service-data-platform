import os
import glob
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cấu hình MinIO
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9002') # 9002 on host, 9000 inside docker network
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minio_access_key')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minio_secret_key')
BUCKET_NAME = 'landing-zone'

# Cấu hình thư mục nguồn (local)
SOURCE_DIR = os.environ.get('SOURCE_DIR', 'raw/xml')

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1' # Default for MinIO
    )

def sync_xml_to_landing():
    """Đồng bộ toàn bộ file XML từ thư mục nguồn lên Landing Zone (MinIO)"""
    if not os.path.exists(SOURCE_DIR):
        logging.error(f"Thư mục nguồn '{SOURCE_DIR}' không tồn tại. Vui lòng chạy data_Transactional.py trước.")
        return

    s3_client = get_s3_client()
    
    # Kiểm tra bucket tồn tại chưa
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logging.info(f"Bucket {BUCKET_NAME} không tồn tại. Đang tạo...")
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            logging.error(f"Lỗi truy cập bucket: {e}")
            return

    # Tìm tất cả file .xml đệ quy trong SOURCE_DIR
    xml_files = glob.glob(os.path.join(SOURCE_DIR, '**/*.xml'), recursive=True)
    
    if not xml_files:
        logging.warning(f"Không tìm thấy file .xml nào trong '{SOURCE_DIR}'")
        return

    logging.info(f"Tìm thấy {len(xml_files)} file XML. Bắt đầu đồng bộ...")

    uploaded_count = 0
    for local_file in xml_files:
        # Giữ nguyên cấu trúc thư mục con (VD: dvc/2026/07/13/...)
        rel_path = os.path.relpath(local_file, SOURCE_DIR).replace(os.sep, '/')
        # MinIO S3 object key sẽ là: raw/xml/dvc/2026/07/13/...
        s3_key = f"raw/xml/{rel_path}"
        
        try:
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            logging.info(f"Đã upload: {local_file} -> s3://{BUCKET_NAME}/{s3_key}")
            
            # Xóa file local sau khi upload thành công để dọn dẹp
            os.remove(local_file)
            uploaded_count += 1
        except Exception as e:
            logging.error(f"Lỗi khi upload {local_file}: {e}")
            
    logging.info(f"✅ Hoàn tất đồng bộ {uploaded_count}/{len(xml_files)} file lên Landing Zone.")
    
    # Tùy chọn: Xóa các thư mục rỗng
    for root, dirs, files in os.walk(SOURCE_DIR, topdown=False):
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):
                try:
                    os.rmdir(dir_path)
                except OSError:
                    pass

if __name__ == "__main__":
    logging.info("🚀 Bắt đầu tiến trình đồng bộ XML lên Landing Zone (dành cho Airflow)...")
    sync_xml_to_landing()
