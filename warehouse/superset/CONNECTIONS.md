# Superset local demo

Ba dashboard do nhóm BI bàn giao được export từ Superset nhưng đang tham chiếu
đến một MySQL cũ (`civic_batch` và `civic_realtime`). Không import trực tiếp ba
file gốc nếu muốn đọc dữ liệu của pipeline này.

## Luồng dữ liệu dùng cho BI

- Batch: Superset -> Trino -> các bảng fact/dim trong `iceberg.gold`.
- Realtime: Superset -> các bảng fact/dim trong `gold_realtime` của StarRocks.
- Kết nối batch:
  `trino://superset@trino:8080/iceberg`.
- Kết nối realtime:
  `mysql+pymysql://root@starrocks:9030/gold_realtime`.

Các câu join fact/dim được lưu ngay trong virtual dataset của gói dashboard,
không tạo thêm view vật lý hay file DDL. Realtime đi thẳng vào StarRocks để
tránh thêm một query hop qua Trino. Trino chỉ phục vụ batch Iceberg.

## Gói import

Ba file đã được chuyển đúng connection nằm trong `warehouse/superset/imports`:

- `dashboard-cap-1-bi.zip`
- `dashboard-cap-2-bi.zip`
- `dashboard-cap-3-bi.zip`

## Chạy và import bằng giao diện web

```powershell
docker compose up -d --build
```

Mở `http://localhost:8088`, đăng nhập bằng giá trị
`SUPERSET_ADMIN_USERNAME`/`SUPERSET_ADMIN_PASSWORD`. Nếu chưa khai báo hai biến
này thì tài khoản demo mặc định là `admin`/`admin`.

Trong Superset, vào **Settings -> Import Dashboards**, bật **Overwrite** nếu đã
import trước đó, rồi lần lượt tải lên ba file `*-bi.zip`.

Không cần mount thư mục `Send Anywhere...` vào container khi import bằng web:
trình duyệt tự tải ZIP lên Superset. Mount chỉ cần thiết nếu muốn tự động hóa
import bằng CLI.

Superset dùng SQLite trong named volume `superset-home` để giảm RAM cho bản demo
local. Volume này lưu user, connection, dataset, chart và dashboard khi container
được recreate. Khi triển khai nhiều người dùng, hãy đổi metadata database sang
PostgreSQL.
