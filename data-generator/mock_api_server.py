import time
import random
import logging
from flask import Flask, jsonify
from faker import Faker

# Tắt log mặc định của Flask để giao diện Terminal sạch sẽ
import logging as flask_logging
flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
fake = Faker('vi_VN')

@app.route('/api/payments/recent', methods=['GET'])
def get_recent_payments():
    """
    Giả lập một Endpoint của Ngân hàng/Cổng thanh toán.
    Mỗi lần gọi sẽ trả về 500 giao dịch thanh toán thành công ngẫu nhiên (Big Data).
    """
    start_time = time.time()
    
    # Số lượng giao dịch sinh ra trong 1 lần gọi (Để test Big Data)
    BATCH_SIZE = random.randint(300, 700)
    
    payments = []
    for _ in range(BATCH_SIZE):
        # Giả lập mã hồ sơ trùng khớp với các hệ thống khác
        app_id = f"HS_{random.randint(1, 20000):05d}" 
        
        payment = {
            "id_ban_ghi": app_id,
            "payment_status": "SUCCESS",
            "tax_code": fake.bothify(text='TAX-########'),
            "amount": random.choice([50000, 100000, 200000]),
            "method": random.choice(["Chuyen_khoan", "Vi_dien_tu", "Tien_mat_tai_quay"]),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "ingested_at": int(time.time() * 1000)
        }
        payments.append(payment)
    
    elapsed = time.time() - start_time
    logging.info(f"Đã sinh và trả về {BATCH_SIZE} giao dịch thanh toán (Mất {elapsed:.3f}s)")
    
    # Trả về Mảng JSON (Array) để NiFi húp trọn và dùng SplitJson chặt ra
    return jsonify(payments)

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "UP", "message": "Mock API Server is running!"})

if __name__ == "__main__":

    print("MASSIVE PAYMENT API SIMULATOR (NIFI INGESTION)")
    print("API Endpoint đã sẵn sàng tại:")
    print(" GET http://localhost:5000/api/payments/recent")
 
    print("Chờ Apache NiFi gọi tới")
    
    # Chạy Server
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
