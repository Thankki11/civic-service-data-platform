import os
import random
import time
from faker import Faker
import psycopg2

fake = Faker('vi_VN')
random.seed(7)

print("BẮT ĐẦU GENERATE")

# Kết nối CSDL
conn = None
cursor = None
try:
    print("\n[*] Đang kết nối tới PostgreSQL để nạp dữ liệu...")
    time.sleep(2)
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", "source_db"),
        database=os.environ.get("DB_NAME", "source_db"),
        user=os.environ.get("DB_USER", "source_db"),
        password=os.environ.get("DB_PASS", "source_db"),
        port=os.environ.get("DB_PORT", "5432")
    )
    cursor = conn.cursor()
    print("Kết nối thành công")
except Exception as e:
    print(f" Không thể kết nối CSDL : {e}")

def save_to_db(table_name, data):
    if not data or not cursor: return
    
    # 1. Tạo bảng
    cols = []
    for k, v in data[0].items():
        if isinstance(v, int):
            cols.append(f'"{k}" INT')
        else:
            cols.append(f'"{k}" VARCHAR(255)')
    create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' + ',\n  '.join(cols) + '\n);'
    cursor.execute(create_stmt)
    
    # 2. Xoá dữ liệu cũ
    cursor.execute(f'TRUNCATE TABLE "{table_name}" CASCADE;')
    
    # 3. Nạp dữ liệu mới
    for row in data:
        vals = []
        for v in row.values():
            if isinstance(v, int):
                vals.append(str(v))
            elif v is None:
                vals.append('NULL')
            else:
                s = str(v).replace("'", "''")
                vals.append(f"'{s}'")
        cols_str = ', '.join([f'"{k}"' for k in row.keys()])
        vals_str = ', '.join(vals)
        insert_stmt = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({vals_str});'
        cursor.execute(insert_stmt)
        
    conn.commit()
    print(f"Đã nạp trực tiếp vào DB bảng: {table_name} ({len(data)} dòng)")

# ==========================================
# 1. CÁC BẢNG DANH MỤC CƠ BẢN
# ==========================================
status_data = [
    {'id': 1, 'code': 'RECEIVED', 'name': 'Mới tiếp nhận', 'description': 'Hồ sơ đã nộp thành công'},
    {'id': 2, 'code': 'ASSIGNED', 'name': 'Đã phân công', 'description': 'Lãnh đạo đã giao hồ sơ'},
    {'id': 3, 'code': 'PROCESSING', 'name': 'Đang thẩm định', 'description': 'Chuyên viên đang kiểm tra'},
    {'id': 4, 'code': 'PENDING_APPROVAL', 'name': 'Chấp nhận hồ sơ', 'description': 'Chờ Lãnh đạo ký duyệt'},
    {'id': 5, 'code': 'APPROVED', 'name': 'Đã được ký', 'description': 'Lãnh đạo đã ký duyệt'},
    {'id': 6, 'code': 'READY', 'name': 'Chờ trả kết quả', 'description': 'Chờ người dân đến lấy'},
    {'id': 7, 'code': 'COMPLETED', 'name': 'Đã hoàn thành', 'description': 'Kết thúc quy trình'},
    {'id': 8, 'code': 'REJECTED', 'name': 'Từ chối', 'description': 'Hồ sơ bị từ chối'}
]
save_to_db('Status', status_data)

service_data = [
    {'id': 1, 'name': 'Đăng ký Thành lập Doanh nghiệp', 'processing_time': 3, 'description': 'Quy định 3 ngày làm việc'},
    {'id': 2, 'name': 'Cấp đổi Giấy phép lái xe', 'processing_time': 5, 'description': 'Quy định 5 ngày làm việc'}
]
save_to_db('Service', service_data)

doc_type_data = [
    {'id': 1, 'code': 'Don_de_nghi_dang_ky_DN', 'name': 'Đơn đề nghị đăng ký doanh nghiệp', 'description': ''},
    {'id': 2, 'code': 'CCCD_nguoi_dai_dien', 'name': 'CCCD/CMND của người đại diện', 'description': ''},
    {'id': 3, 'code': 'Dieu_le_cong_ty', 'name': 'Điều lệ công ty', 'description': ''},
    {'id': 4, 'code': 'Giay_uy_quyen', 'name': 'Giấy ủy quyền', 'description': ''},
    {'id': 5, 'code': 'Giay_chung_nhan_dang_ky_DN', 'name': 'Giấy chứng nhận đăng ký doanh nghiệp (Kết quả)', 'description': 'Tài liệu output'}
]
save_to_db('Document_Type', doc_type_data)

province_data = [{'id': 1, 'name': 'Thành phố Hà Nội', 'postal_code': '100000'}, {'id': 2, 'name': 'Thành phố Hồ Chí Minh', 'postal_code': '700000'}]
save_to_db('Province', province_data)

ward_data = [{'id': i, 'name': f"{random.choice(['Phường', 'Xã'])} {fake.street_name()}", 'Provinceid': random.choice([1, 2])} for i in range(1, 21)]
save_to_db('Ward', ward_data)

agency_data = [
    {'id': 1, 'name': 'Phòng Đăng ký kinh doanh - Sở KHĐT Hà Nội', 'Provinceid': 1, 'Wardid': 1},
    {'id': 2, 'name': 'Phòng Đăng ký kinh doanh - Sở KHĐT TP.HCM', 'Provinceid': 2, 'Wardid': 15},
    {'id': 3, 'name': 'Phòng Kinh tế - UBND Quận Hoàn Kiếm', 'Provinceid': 1, 'Wardid': 3}
]
save_to_db('Agency', agency_data)

role_data = [
    {'id': 1, 'code': 'ONE_STOP', 'name': 'Cán bộ Một cửa'},
    {'id': 2, 'code': 'EXPERT', 'name': 'Chuyên viên thụ lý'},
    {'id': 3, 'code': 'LEADER', 'name': 'Lãnh đạo phòng'}
]
save_to_db('Role', role_data)

permission_data = [
    {'id': 1, 'name': 'Tiếp nhận hồ sơ', 'description': 'Quyền nhận và trả kết quả', 'Roleid': 1},
    {'id': 2, 'name': 'Thẩm định hồ sơ', 'description': 'Quyền xem xét tính hợp lệ', 'Roleid': 2},
    {'id': 3, 'name': 'Ký duyệt cấp phép', 'description': 'Quyền phê duyệt cuối cùng', 'Roleid': 3}
]
save_to_db('Permission', permission_data)

officer_configs = [
    (101, 'Nguyễn Văn Cán Bộ 1', 1), (102, 'Trần Thị Cán Bộ 2', 1), (103, 'Lê Văn Cán Bộ 3', 1),
    (201, 'Phạm Lãnh Đạo 1', 3), (202, 'Vũ Lãnh Đạo 2', 3), 
    (301, 'Đinh Chuyên Viên 1', 2), (302, 'Hoàng Chuyên Viên 2', 2), (303, 'Ngô Chuyên Viên 3', 2), (304, 'Bùi Chuyên Viên 4', 2) 
]

officer_data = []
officer_role_data = []
for idx, (off_id, name, role_id) in enumerate(officer_configs):
    officer_data.append({
        'id': off_id,
        'identity_num': fake.numerify(text='0010########'),
        'name': name,
        'email': f"canbo{off_id}@dvc.gov.vn",
        'phone': fake.phone_number(),
        'password': 'hashed_pw',
        'Agencyid': random.choice([1, 2, 3])
    })
    officer_role_data.append({
        'id': idx + 1,
        'Officerid': off_id,
        'Roleid': role_id
    })

save_to_db('Officer', officer_data)
save_to_db('Officer_Role', officer_role_data)

if conn:
    conn.close()

print("\nToàn bộ 10 bảng đã được đẩy trực tiếp lên DB ")