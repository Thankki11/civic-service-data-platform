"""
Generate mock XML packet cho ĐÚNG schema thật trong báo cáo thực tập (dựa theo ERD)
"""

import os
import csv
import random
import shutil
from datetime import datetime, timedelta
from lxml import etree
from faker import Faker

fake = Faker('vi_VN')
random.seed(7)

now = datetime.now()
OUTPUT_DIR = f'raw/xml/dvc/{now.strftime("%Y/%m/%d")}'

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

manifest_rows = []
_counter = {}

def next_id(prefix, width=6):
    _counter[prefix] = _counter.get(prefix, 0) + 1
    # Dùng số nguyên cho ID các bảng, trừ metadata
    if prefix in ['HS', 'DOC', 'PAY', 'HIST']:
        return _counter[prefix]
    return f"{prefix}_{_counter[prefix]:0{width}d}"

def sub_text(parent, tag, value, **attrs):
    el = etree.SubElement(parent, tag, **attrs)
    el.text = '' if value is None else str(value)
    return el

def build_metadata(root, ma_goi_tin, su_kien, id_ho_so, ngay_cap_nhat):
    md = etree.SubElement(root, 'metadata')
    sub_text(md, 'ma_goi_tin', ma_goi_tin)
    sub_text(md, 'ma_du_lieu', 'DL_HO_SO')
    sub_text(md, 'loai_du_lieu', 'XML')
    sub_text(md, 'ngay_cap_nhat', ngay_cap_nhat.strftime('%Y-%m-%d %H:%M:%S'))
    sub_text(md, 'su_kien', su_kien)
    sub_text(md, 'id_ban_ghi', f"HS_{id_ho_so:05d}")
    return md

def write_packet(scenario, ma_goi_tin, root_el, su_kien, id_ho_so, t, note=''):
    folder = os.path.join(OUTPUT_DIR, scenario)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{ma_goi_tin}.xml")
    etree.ElementTree(root_el).write(path, encoding='utf-8', xml_declaration=True, pretty_print=True)
    manifest_rows.append({
        'file_path': os.path.relpath(path, OUTPUT_DIR).replace(os.sep, '/'),
        'ma_goi_tin': ma_goi_tin, 'kich_ban': scenario, 'su_kien': su_kien,
        'id_ho_so': id_ho_so, 'ngay_cap_nhat': t.strftime('%Y-%m-%d %H:%M:%S'), 'ghi_chu': note,
    })

SERVICE_IDS = [1, 2]
AGENCY_IDS = [1, 2, 3]
OFFICER_IDS_1CUA = [101, 102, 103]
OFFICER_IDS_LEAD = [201, 202]
OFFICER_IDS_CHUYEN_VIEN = [301, 302, 303, 304]

STATUS_CODE_TO_ID = {
    'RECEIVED': 1, 'ASSIGNED': 2, 'PROCESSING': 3, 'PENDING_APPROVAL': 4,
    'APPROVED': 5, 'READY': 6, 'COMPLETED': 7, 'REJECTED': 8
}

DOCUMENT_TYPES_INPUT = [1, 2, 3, 4]
DOCUMENT_TYPE_RESULT = 5

DOC_TYPE_NAMES = {
    1: 'Don_de_nghi_dang_ky_DN',
    2: 'CCCD_nguoi_dai_dien',
    3: 'Dieu_le_cong_ty',
    4: 'Giay_uy_quyen',
    5: 'Giay_chung_nhan_dang_ky_DN'
}

STATUS_FLOW = ['ASSIGNED', 'PROCESSING', 'PENDING_APPROVAL', 'APPROVED', 'READY', 'COMPLETED']
STATUS_OFFICER_MAP = {
    'RECEIVED': OFFICER_IDS_1CUA,
    'ASSIGNED': OFFICER_IDS_LEAD, 'PROCESSING': OFFICER_IDS_CHUYEN_VIEN,
    'PENDING_APPROVAL': OFFICER_IDS_CHUYEN_VIEN, 'APPROVED': OFFICER_IDS_LEAD,
    'READY': OFFICER_IDS_1CUA, 'COMPLETED': OFFICER_IDS_1CUA, 'REJECTED': OFFICER_IDS_LEAD,
}
LY_DO_TU_CHOI = ['Hồ sơ thiếu chữ ký người đại diện', 'Ngành nghề đăng ký thuộc danh mục cấm',
                 'Thông tin vốn điều lệ không hợp lệ', 'Trùng tên doanh nghiệp đã đăng ký']

def gen_document(la_ket_qua=False):
    doc_type = DOCUMENT_TYPE_RESULT if la_ket_qua else random.choice(DOCUMENT_TYPES_INPUT)
    return {
        'id': next_id('DOC'),
        'name': f"{DOC_TYPE_NAMES[doc_type]}_{fake.uuid4()[:8]}.pdf",
        'file_url': f"https://storage.dvc.gov.vn/docs/{fake.uuid4()}.pdf",
        'Document_Typeid': doc_type
    }

def gen_payment(t):
    return {
        'id': next_id('PAY'),
        'amount': random.choice([50000, 100000, 200000]),
        'method': random.choice(['Chuyen_khoan', 'Vi_dien_tu', 'Tien_mat_tai_quay']),
        'status': 'SUCCESS',
        'transaction_code': fake.bothify(text='TX-????-####'),
        'paid_at': t.strftime('%Y-%m-%d %H:%M:%S')
    }

def write_du_lieu(parent, ho_so, mode, doc_items=None, pay_items=None, hist_items=None):
    du_lieu = etree.SubElement(parent, 'du_lieu')
    if mode == 'FULL':
        sub_text(du_lieu, 'name', ho_so['name'])
        sub_text(du_lieu, 'Applicantid', ho_so['Applicantid'])
        sub_text(du_lieu, 'Serviceid', ho_so['Serviceid'])
        sub_text(du_lieu, 'Agencyid', ho_so['Agencyid'])
        sub_text(du_lieu, 'created_at', ho_so['created_at'])
        sub_text(du_lieu, 'Statusid', ho_so['Statusid'])
    elif mode == 'PARTIAL_STATUS':
        sub_text(du_lieu, 'Statusid', ho_so['Statusid'])

    if doc_items:
        arr = etree.SubElement(du_lieu, 'document_array')
        for d, action in doc_items:
            el = etree.SubElement(arr, 'document', **({'action': action} if action else {}))
            sub_text(el, 'id', d['id'])
            if action != 'DELETE':
                sub_text(el, 'name', d['name'])
                sub_text(el, 'file_url', d['file_url'])
                sub_text(el, 'Document_Typeid', d['Document_Typeid'])

    if pay_items:
        arr = etree.SubElement(du_lieu, 'payment_array')
        for p, action in pay_items:
            el = etree.SubElement(arr, 'payment', **({'action': action} if action else {}))
            sub_text(el, 'id', p['id'])
            if action != 'DELETE':
                sub_text(el, 'amount', p['amount'])
                sub_text(el, 'method', p['method'])
                sub_text(el, 'status', p['status'])
                sub_text(el, 'transaction_code', p['transaction_code'])
                sub_text(el, 'paid_at', p['paid_at'])

    if hist_items:
        arr = etree.SubElement(du_lieu, 'application_history_array')
        for h, action in hist_items:
            el = etree.SubElement(arr, 'application_history', **({'action': action} if action else {}))
            sub_text(el, 'id', h['id'])
            if h.get('Statusid') is not None:
                sub_text(el, 'Statusid', h['Statusid'])
            if h.get('Statusid2') is not None:
                sub_text(el, 'Statusid2', h['Statusid2'])
            sub_text(el, 'Officerid', h['Officerid'])
            sub_text(el, 'action_time', h['action_time'])
            if h.get('note'):
                sub_text(el, 'note', h['note'])

    return du_lieu

def gen_history_entry(status_code_truoc, status_code_sau, officer_id, t, note=None):
    return {
        'id': next_id('HIST'),
        'Statusid': STATUS_CODE_TO_ID.get(status_code_truoc) if status_code_truoc else '',
        'Statusid2': STATUS_CODE_TO_ID.get(status_code_sau),
        'Officerid': officer_id,
        'action_time': t.strftime('%Y-%m-%d %H:%M:%S'),
        'note': note,
    }

NUM_HO_SO = 200
now = datetime.now()

for h in range(1, NUM_HO_SO + 1):
    id_ho_so = next_id('HS')
    t = now - timedelta(days=random.randint(3, 45))

    docs = {}
    for _ in range(random.randint(2, 4)):
        d = gen_document()
        docs[d['id']] = d

    service_id = random.choice(SERVICE_IDS)
    if service_id == 1:
        app_name = f"Hồ sơ đăng ký doanh nghiệp - {fake.company()}"
    else:
        app_name = f"Hồ sơ cấp đổi GPLX - {fake.name()}"

    ho_so = {
        'name': app_name,
        'Applicantid': random.randint(1, 200),
        'Serviceid': service_id,
        'Agencyid': random.choice(AGENCY_IDS),
        'created_at': t.strftime('%Y-%m-%d %H:%M:%S'),
        'current_status_code': 'RECEIVED',
        'Statusid': STATUS_CODE_TO_ID['RECEIVED']
    }

    first_hist = gen_history_entry(None, 'RECEIVED', random.choice(STATUS_OFFICER_MAP['RECEIVED']), t)

    payments = {}
    pay_items_insert = None
    da_dong_le_phi = False
    if random.random() < 0.4:
        p = gen_payment(t)
        payments[p['id']] = p
        pay_items_insert = [(p, None)]
        da_dong_le_phi = True

    root = etree.Element('packet')
    ma_goi_tin = next_id('PKG')
    build_metadata(root, ma_goi_tin, 'INSERT', id_ho_so, t)
    write_du_lieu(root, ho_so, mode='FULL',
                  doc_items=[(d, None) for d in docs.values()],
                  pay_items=pay_items_insert,
                  hist_items=[(first_hist, None)])
    write_packet('INSERT', ma_goi_tin, root, 'INSERT', id_ho_so, t)

    rut_ho_so = False
    for step in STATUS_FLOW:
        t = t + timedelta(hours=random.randint(2, 36))

        if random.random() < 0.30:
            d = random.choice(list(docs.values()))
            d['file_url'] = f"https://storage.dvc.gov.vn/docs/{fake.uuid4()}.pdf"
            root = etree.Element('packet')
            ma_goi_tin = next_id('PKG')
            build_metadata(root, ma_goi_tin, 'UPDATE', id_ho_so, t)
            write_du_lieu(root, ho_so, mode='PARTIAL', doc_items=[(d, 'UPDATE')])
            write_packet('UPDATE_DOCUMENT_PARTIAL', ma_goi_tin, root, 'UPDATE', id_ho_so, t)
            t = t + timedelta(hours=random.randint(1, 6))

        if random.random() < 0.15:
            d = gen_document()
            docs[d['id']] = d
            root = etree.Element('packet')
            ma_goi_tin = next_id('PKG')
            build_metadata(root, ma_goi_tin, 'UPDATE', id_ho_so, t)
            write_du_lieu(root, ho_so, mode='PARTIAL', doc_items=[(d, 'ADD')])
            write_packet('UPDATE_DOCUMENT_ADD', ma_goi_tin, root, 'UPDATE', id_ho_so, t)
            t = t + timedelta(hours=random.randint(1, 6))

        if not da_dong_le_phi and random.random() < 0.35:
            p = gen_payment(t)
            payments[p['id']] = p
            da_dong_le_phi = True
            root = etree.Element('packet')
            ma_goi_tin = next_id('PKG')
            build_metadata(root, ma_goi_tin, 'UPDATE', id_ho_so, t)
            write_du_lieu(root, ho_so, mode='PARTIAL', pay_items=[(p, 'ADD')])
            write_packet('UPDATE_PAYMENT_ADD', ma_goi_tin, root, 'UPDATE', id_ho_so, t)
            t = t + timedelta(hours=random.randint(1, 6))

        if random.random() < 0.02:
            root = etree.Element('packet')
            ma_goi_tin = next_id('PKG')
            build_metadata(root, ma_goi_tin, 'DELETE', id_ho_so, t)
            etree.SubElement(root, 'du_lieu')
            write_packet('DELETE', ma_goi_tin, root, 'DELETE', id_ho_so, t)
            rut_ho_so = True
            break

        if step != 'ASSIGNED' and random.random() < 0.07:
            ly_do = random.choice(LY_DO_TU_CHOI)
            hist = gen_history_entry(ho_so['current_status_code'], 'REJECTED',
                                      random.choice(STATUS_OFFICER_MAP['REJECTED']), t, note=ly_do)
            ho_so['current_status_code'] = 'REJECTED'
            ho_so['Statusid'] = STATUS_CODE_TO_ID['REJECTED']
            root = etree.Element('packet')
            ma_goi_tin = next_id('PKG')
            build_metadata(root, ma_goi_tin, 'UPDATE', id_ho_so, t)
            write_du_lieu(root, ho_so, mode='PARTIAL_STATUS', hist_items=[(hist, 'ADD')])
            write_packet('UPDATE_STATUS_CHANGE', ma_goi_tin, root, 'UPDATE', id_ho_so, t)
            break

        hist = gen_history_entry(ho_so['current_status_code'], step,
                                  random.choice(STATUS_OFFICER_MAP[step]), t)
        ho_so['current_status_code'] = step
        ho_so['Statusid'] = STATUS_CODE_TO_ID[step]
        root = etree.Element('packet')
        ma_goi_tin = next_id('PKG')
        build_metadata(root, ma_goi_tin, 'UPDATE', id_ho_so, t)

        if step == 'APPROVED':
            ket_qua_doc = gen_document(la_ket_qua=True)
            docs[ket_qua_doc['id']] = ket_qua_doc
            write_du_lieu(root, ho_so, mode='PARTIAL_STATUS',
                          doc_items=[(ket_qua_doc, 'ADD')], hist_items=[(hist, 'ADD')])
        else:
            write_du_lieu(root, ho_so, mode='PARTIAL_STATUS', hist_items=[(hist, 'ADD')])

        write_packet('UPDATE_STATUS_CHANGE', ma_goi_tin, root, 'UPDATE', id_ho_so, t)

        if step in ('COMPLETED',):
            break

    if not rut_ho_so and random.random() < 0.05:
        t = t + timedelta(hours=random.randint(1, 12))
        root = etree.Element('packet')
        ma_goi_tin = next_id('PKG')
        build_metadata(root, ma_goi_tin, 'UPDATE', id_ho_so, t)
        write_du_lieu(root, ho_so, mode='FULL',
                      doc_items=[(d, None) for d in docs.values()],
                      pay_items=[(p, None) for p in payments.values()] or None,
                      hist_items=None)
        write_packet('UPDATE_FULL', ma_goi_tin, root, 'UPDATE', id_ho_so, t)

print(f"- Tổng số hồ sơ: {NUM_HO_SO}")
manifest_path = os.path.join(OUTPUT_DIR, 'manifest.csv')
with open(manifest_path, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=['file_path', 'ma_goi_tin', 'kich_ban', 'su_kien',
                                            'id_ho_so', 'ngay_cap_nhat', 'ghi_chu'])
    writer.writeheader()
    writer.writerows(manifest_rows)

print(f"\nTỔNG SỐ PACKET: {len(manifest_rows)}")