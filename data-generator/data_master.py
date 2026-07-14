import os
import random
from faker import Faker

fake = Faker('vi_VN')
random.seed(7)

OUTPUT_DIR = 'mock_database_csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)

SQL_FILE = os.path.join(OUTPUT_DIR, 'master_data_init.sql')
with open(SQL_FILE, 'w', encoding='utf-8') as f:
    f.write("-- TẠO BẢNG VÀ NẠP DỮ LIỆU MASTER DATA\n\n")

def save_to_sql(table_name, data):
    if not data: return
    with open(SQL_FILE, 'a', encoding='utf-8') as f:
        cols = []
        for k, v in data[0].items():
            if isinstance(v, int):
                cols.append(f'"{k}" INT')
            else:
                cols.append(f'"{k}" VARCHAR(255)')
        f.write(f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' + ',\n  '.join(cols) + '\n);\n')
        f.write(f'TRUNCATE TABLE "{table_name}" CASCADE;\n')
        
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
            f.write(f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({vals_str});\n')
        f.write("\n")
    print(f"Đã xuất SQL cho bảng: {table_name} ({len(data)} dòng)")

print("BẮT ĐẦU GENERATE 11 BẢNG TĨNH (MASTER DATA)...")

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
save_to_sql('Status', status_data)

service_data = [
    {'id': 1, 'name': 'Đăng ký Thành lập Doanh nghiệp', 'processing_time': 3, 'description': 'Quy định 3 ngày làm việc'},
    {'id': 2, 'name': 'Cấp đổi Giấy phép lái xe', 'processing_time': 5, 'description': 'Quy định 5 ngày làm việc'}
]
save_to_sql('Service', service_data)

doc_type_data = [
    {'id': 1, 'code': 'Don_de_nghi_dang_ky_DN', 'name': 'Đơn đề nghị đăng ký doanh nghiệp', 'description': ''},
    {'id': 2, 'code': 'CCCD_nguoi_dai_dien', 'name': 'CCCD/CMND của người đại diện', 'description': ''},
    {'id': 3, 'code': 'Dieu_le_cong_ty', 'name': 'Điều lệ công ty', 'description': ''},
    {'id': 4, 'code': 'Giay_uy_quyen', 'name': 'Giấy ủy quyền', 'description': ''},
    {'id': 5, 'code': 'Giay_chung_nhan_dang_ky_DN', 'name': 'Giấy chứng nhận đăng ký doanh nghiệp (Kết quả)', 'description': 'Tài liệu output'}
]
save_to_sql('Document_Type', doc_type_data)

province_data = [{'id': 1, 'name': 'Thành phố Hà Nội', 'postal_code': '100000'}, {'id': 2, 'name': 'Thành phố Hồ Chí Minh', 'postal_code': '700000'}]
save_to_sql('Province', province_data)

ward_data = [{'id': i, 'name': f"{random.choice(['Phường', 'Xã'])} {fake.street_name()}", 'Provinceid': random.choice([1, 2])} for i in range(1, 21)]
save_to_sql('Ward', ward_data)

agency_data = [
    {'id': 1, 'name': 'Phòng Đăng ký kinh doanh - Sở KHĐT Hà Nội', 'Provinceid': 1, 'Wardid': 1},
    {'id': 2, 'name': 'Phòng Đăng ký kinh doanh - Sở KHĐT TP.HCM', 'Provinceid': 2, 'Wardid': 15},
    {'id': 3, 'name': 'Phòng Kinh tế - UBND Quận Hoàn Kiếm', 'Provinceid': 1, 'Wardid': 3}
]
save_to_sql('Agency', agency_data)

role_data = [
    {'id': 1, 'code': 'ONE_STOP', 'name': 'Cán bộ Một cửa'},
    {'id': 2, 'code': 'EXPERT', 'name': 'Chuyên viên thụ lý'},
    {'id': 3, 'code': 'LEADER', 'name': 'Lãnh đạo phòng'}
]
save_to_sql('Role', role_data)

permission_data = [
    {'id': 1, 'name': 'Tiếp nhận hồ sơ', 'description': 'Quyền nhận và trả kết quả', 'Roleid': 1},
    {'id': 2, 'name': 'Thẩm định hồ sơ', 'description': 'Quyền xem xét tính hợp lệ', 'Roleid': 2},
    {'id': 3, 'name': 'Ký duyệt cấp phép', 'description': 'Quyền phê duyệt cuối cùng', 'Roleid': 3}
]
save_to_sql('Permission', permission_data)


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

save_to_sql('Officer', officer_data)
save_to_sql('Officer_Role', officer_role_data)

print(f"\nHOÀN TẤT! Dữ liệu 11 bảng tĩnh đã được lưu vào file SQL: {SQL_FILE}")