# Transform layer: Bronze → Silver → Gold và Real-time serving

Tài liệu này mô tả phần **transform** của dự án dịch vụ hồ sơ công. Phạm vi là Spark/Iceberg và StarRocks; chi tiết cơ chế ingest XML, API, CDC vào Bronze không thuộc tài liệu này.

## 1. Mục tiêu và hai đường dữ liệu

```text
Batch
Bronze (master + XML + CDC + API)
    -> bronze_to_silver.py
Silver (dữ liệu đã chuẩn hóa)
    -> build_dim_tables.py (khi danh mục đổi)
Gold dimensions
    -> silver_to_gold.py (mỗi ngày)
Gold fact_ton_dong_ho_so + fact_van_hanh_co_quan

Real-time
Debezium/Kafka Application -----------------> Routine Load -> ods_application_rt (PK/upsert)
Debezium/Kafka Application_History ---------> Routine Load -> ods_application_history_rt (Duplicate/append)
                                                               -> fact_xu_ly_ho_so (Async MV)
                                                               -> vw_fact_xu_ly_ho_so_bi (dim broadcast join)
```

Hai đường cùng dùng các khóa nghiệp vụ `ho_so_id`, `co_quan_id`, `dv_cong_id`, `trang_thai_id`, `can_bo_id` và `thoi_gian_id`. Batch tối ưu cho snapshot/aggregate theo ngày; StarRocks tối ưu drill-down sự kiện gần thời gian thực.

## 2. Grain và nghiệp vụ cần nhớ

| Đối tượng | Grain | Nguồn chính | Ý nghĩa |
|---|---:|---|---|
| `silver.application` | 1 dòng / hồ sơ | XML + CDC | Bản trạng thái hiện tại, đã forward-fill dữ liệu XML partial. |
| `silver.application_history` | 1 dòng / hành động | XML + CDC | Nhật ký chuyển trạng thái bất biến. |
| `gold.fact_ton_dong_ho_so` | 1 dòng / hồ sơ mở / ngày | Silver application + history | Snapshot backlog ở 23:59:59. |
| `gold.fact_van_hanh_co_quan` | 1 dòng / cơ quan / ngày | Các fact/nguồn Silver | KPI lãnh đạo. |
| `gold_realtime.fact_xu_ly_ho_so` | 1 dòng / hành động | StarRocks ODS history + application | Duration từng bước xử lý realtime. |

Trạng thái: `RECEIVED(1) -> ASSIGNED(2) -> PROCESSING(3) -> PENDING_APPROVAL(4) -> APPROVED(5) -> READY(6) -> COMPLETED(7)`; `REJECTED(8)` là trạng thái kết thúc.

`Application_History` là nguồn duy nhất cho lịch sử. Không suy luận lịch sử từ `before/after` của bảng `Application`, vì bảng này chỉ giữ trạng thái cuối cùng.

## 3. Điều kiện trước khi chạy transform

1. Docker Desktop/Engine đang chạy.
2. Bronze master data, CDC tables đã tồn tại. XML và API là nguồn optional: Silver vẫn chạy khi hai table Bronze này chưa tồn tại, nhưng không có payment/XML contribution trong lần chạy đó.
3. Lần đầu chạy StarRocks cần tạo schema trước khi `build_dim_tables.py` ghi JDBC.
4. `data-master-gen` cần chạy ít nhất một lần để tạo master data và schema OLTP mà simulator CDC dùng.

Các bucket `landing-zone`, `lakehouse`, `quarantine-zone` được service `minio-init` tạo tự động khi compose khởi động.

## 4. Khởi động môi trường demo

Từ PowerShell, tại root project:

```powershell
docker compose up -d --build
docker compose ps
docker compose --profile manual run --rm data-master-gen
```

Đăng ký Debezium sau khi Postgres đã có bảng giao dịch:

```powershell
Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8084/connectors' `
  -ContentType 'application/json' `
  -InFile '.\ingestion\debezium_config.json'

Invoke-RestMethod -Uri 'http://localhost:8084/connectors/postgres-connector/status'
```

Khi cần sinh CDC realtime:

```powershell
docker compose --profile streaming up -d stream-simulator
```

`stream-simulator` được đặt trong profile riêng để không ghi lỗi vào Postgres trước khi schema OLTP và Debezium sẵn sàng.

## 5. Thứ tự chạy transform lần đầu

### 5.1 Tạo StarRocks serving schema

```powershell
Get-Content '.\transform\starrocks\ddl_realtime.sql' -Raw |
  docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -uroot
```

Nếu database `gold_realtime` được tạo bằng DDL cũ, phải reset riêng database đó và hai Routine Load trước. `CREATE TABLE IF NOT EXISTS` không thể đổi kiểu `history.id` hoặc key model của bảng có sẵn.

### 5.2 Đưa dữ liệu vào Bronze bằng các ingest job có sẵn

Phần này là precondition của transform. Với CDC Bronze, Spark streaming job phải đang chạy để có `application_cdc`, `application_history_cdc`, `document_cdc`, `applicant_cdc`.

### 5.3 Bronze → Silver

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-data/transform/spark-etl/bronze_to_silver.py
```

### 5.4 Build dimensions

Chạy lần đầu và chạy lại khi Agency/Status/Service/Officer/Role hoặc lịch nghỉ thay đổi.

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-data/transform/spark-agg/build_dim_tables.py
```

### 5.5 Silver → Gold batch

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-data/transform/spark-agg/silver_to_gold.py
```

Job chốt số cho **hôm qua**. Nó ghi đè động đúng partition `thoi_gian_id` của ngày đó, không làm mất các ngày Gold trước.

### 5.6 Tạo Routine Load realtime

```powershell
Get-Content '.\transform\starrocks\routine_load.sql' -Raw |
  docker exec -i starrocks mysql -h 127.0.0.1 -P 9030 -uroot

docker exec -it starrocks mysql -h 127.0.0.1 -P 9030 -uroot `
  -e 'SHOW ROUTINE LOAD FROM gold_realtime\G'
```

Ở các ngày tiếp theo, thứ tự batch chỉ là Bronze sẵn sàng → `bronze_to_silver.py` → `silver_to_gold.py`. Dimensions không cần build hằng ngày nếu danh mục không đổi.

## 6. Mô tả từng file transform

### `spark-etl/bronze_to_silver.py`

**Spark session và `save_silver()`**

- Kết nối Hive Metastore/Iceberg qua MinIO.
- Bật AQE và skew join để hạn chế OOM.
- `save_silver()` tạo Iceberg table nếu chưa có và overwrite toàn bộ Silver. Đây là lựa chọn idempotent cho demo; Bronze vẫn là lịch sử gốc.
- `read_optional_bronze()` trả DataFrame rỗng đúng schema nếu XML/API chưa được ingest. Nhờ đó demo CDC-only không fail toàn bộ job.

**Master data**

- Đọc 10 bảng `bronze_master_data.*`.
- Trim chuỗi, bỏ metadata ingest, `dropDuplicates()`.
- Ghi thành `silver.province`, `ward`, `status`, `service`, `agency`, `role`, `permission`, `document_type`, `officer`, `officer_role`.

**Application: điểm quan trọng nhất của Silver**

- XML có thể là `PARTIAL_STATUS`, chỉ có `Statusid`; CDC thường có full row.
- Hai nguồn được union và sắp theo `event_time`, thêm `source_priority` để CDC thắng khi đồng thời điểm.
- `last(..., ignorenulls=True)` trên từng cột thực hiện forward-fill: status packet partial không làm mất tên hồ sơ, dịch vụ, cơ quan, người nộp hoặc ngày tạo.
- Sau forward-fill, `row_number` chọn event mới nhất cho mỗi `ho_so_id`.
- `da_bi_xoa` là dấu xóa từ XML, còn `event_time` giữ lại để Gold biết delete xảy ra trước hay sau cutoff.

**Application history và document**

- ID là business key dạng chuỗi (`H_xxx`, `DOC_xxx`), tuyệt đối không cast sang số.
- Union XML/CDC và dedup theo `history_id` hoặc `document_id` bằng event mới nhất.
- `can_bo_id = -1` là Unknown member, giúp fact join được dim ngay cả khi hồ sơ mới nhận chưa phân công.

**Payment và applicant**

- Payment XML/API được union; hiện không có khóa đối soát tin cậy xuyên nguồn nên giữ `nguon_du_lieu` để tránh dedup sai.
- Applicant là SCD current-state đơn giản: giữ version có `updated_at` mới nhất.

### `spark-agg/build_dim_tables.py`

File này xây cả Iceberg Gold và StarRocks dimensions trong cùng một lần chạy.

- `dim_thoi_gian`: date spine demo 2023–2028, thứ/ngày/tháng/quý/năm, cờ nghỉ và `stt_ngay_lam_viec` tích lũy.
- `stt_ngay_lam_viec` là kỹ thuật quan trọng: thay vì join mọi ngày trong một khoảng thời gian, số ngày làm việc = `seq_ngày_kết_thúc - seq_ngày_bắt_đầu`.
- `dim_co_quan`: Agency join Province/Ward bằng Spark broadcast.
- `dim_trang_thai`: map id/code/name.
- `dim_can_bo`: Officer join Officer_Role/Role, chọn một role đại diện và thêm member `-1`.
- `dim_dich_vu_cong`: Service, trong đó `processing_time` trở thành SLA `thoi_han_tra_kq`.
- Ghi StarRocks bằng JDBC append vào Primary Key table, nên cùng key sẽ upsert. Vì dimension nhỏ, dữ liệu được `coalesce(1)`, ghi theo batch 1.000 dòng và bật `rewriteBatchedStatements`; nếu không, MySQL JDBC có thể biến một batch thành hàng nghìn `INSERT` đơn lẻ, làm StarRocks chạm giới hạn phiên bản tablet.

### `spark-agg/silver_to_gold.py`

**Quy ước thời gian**

- `snapshot_date = hôm qua`; `cutoff_ts = 23:59:59` của ngày đó.
- Fact Gold partition theo `thoi_gian_id = yyyymmdd`.
- `partitionOverwriteMode=dynamic` chỉ thay đúng partition đang xử lý.

**`fact_ton_dong_ho_so`**

- Lấy action history mới nhất **không vượt cutoff**, không dùng trạng thái Silver hiện tại.
- Điều này tránh lỗi khi job 02:00 đã nhìn thấy cập nhật của ngày mới: trạng thái COMPLETED lúc 01:00 hôm nay không được làm mất backlog của hôm qua.
- Hồ sơ mở là trạng thái khác COMPLETED/REJECTED tại cutoff.
- `so_ngay_ton_dong_hien_tai`: ngày làm việc từ action gần nhất đến cutoff.
- `tong_thoi_gian_da_xu_ly`: ngày làm việc từ lúc tiếp nhận đến cutoff.
- `can_bo_id`: officer ở action gần nhất; nếu chưa có là `-1`.

**`fact_van_hanh_co_quan`**

- Base từ toàn bộ Agency để mỗi cơ quan luôn có một dòng, kể cả ngày không phát sinh.
- `so_luong_tiep_nhan`: Application có `created_at` trong ngày.
- Đúng/trễ hạn: chỉ event COMPLETED trong ngày; SLA = ngày làm việc từ created đến completed so với `Service.processing_time`.
- Trễ hạn cuối cùng = completed trễ + hồ sơ còn mở đã quá SLA tại cutoff.
- Backlog lấy trực tiếp từ `fact_ton_dong_ho_so`, tránh hai công thức backlog khác nhau.
- Payment SUCCESS đóng góp `tong_chi_phi`.
- Rework hiện dùng REJECTED làm proxy vì nguồn chưa có trạng thái/sự kiện “yêu cầu bổ sung”.

### `starrocks/ddl_realtime.sql`

- `ods_application_rt`: Primary Key `(ho_so_id)`, mirror current state để upsert CDC nhanh.
- `ods_application_history_rt`: Duplicate Key append-only, sort `(ho_so_id, action_time, id)` và hash cùng `ho_so_id`; phù hợp window `LAG` theo hồ sơ.
- 5 dimensions mirror có Primary Key, nạp bởi `build_dim_tables.py`.
- `fact_xu_ly_ho_so`: Async MV refresh 60 giây (mức tối thiểu của StarRocks 4.1). `LAG(action_time)` tính duration một bước; KPI quá hạn chỉ đánh giá ở event `COMPLETED` bằng ngày làm việc toàn hồ sơ, không so duration một bước với SLA toàn dịch vụ.
- `vw_fact_xu_ly_ho_so_bi`: semantic view cho BI, broadcast join tất cả dim nhỏ ở phía phải.

### `starrocks/routine_load.sql`

- `rl_application`: Kafka topic `postgres_server.public.Application` vào PK ODS. `c/u/r` là upsert; `d` lấy key bằng `COALESCE(payload.after.id, payload.before.id)` rồi gán `__op=1` để delete.
- Debezium đã tắt tombstone để Routine Load không nhận record `null` sau delete.
- `rl_application_history`: chỉ nhận `c/r`; `cdc_op` là cột kỹ thuật trong ODS để StarRocks 4.1 lọc hợp lệ. Update/delete history là vi phạm quy ước append-only và không được làm phát sinh event fact.
- Routine Load exactly-once theo offset/transaction; `desired_concurrent_number=1` đúng với demo một Kafka partition/một BE.

### `warehouse/ddl/gold_dim_fact.sql` và `warehouse/validation/*`

- DDL là contract Iceberg Gold. `ho_so_id` và history id là STRING, không ép các mã `HS_xxx`, `H_xxx` về BIGINT.
- `validate_bi_vs_source.sql` kiểm tra số dòng, và đối soát backlog aggregate với snapshot atomic.

## 7. Kiểm tra sau khi chạy

```sql
-- StarRocks: Routine Load phải RUNNING, Progress tăng.
SHOW ROUTINE LOAD FROM gold_realtime;

-- History giữ đúng ID chuỗi và không bị null do cast sai.
SELECT id, ho_so_id, action_time
FROM gold_realtime.ods_application_history_rt
WHERE id IS NULL OR id = '';

-- KPI quá hạn chỉ có ý nghĩa tại COMPLETED.
SELECT f.*
FROM gold_realtime.fact_xu_ly_ho_so f
JOIN gold_realtime.dim_trang_thai s ON f.trang_thai_id = s.trang_thai_id
WHERE s.ma_trang_thai <> 'COMPLETED'
  AND (f.tong_ngay_lam_viec_xu_ly IS NOT NULL OR f.co_bi_qua_han <> 0);

-- BI view: EXPLAIN phải thấy BROADCAST cho các dimension nhỏ.
EXPLAIN SELECT *
FROM gold_realtime.vw_fact_xu_ly_ho_so_bi
WHERE thoi_gian_id >= 20260701;
```

## 8. Giới hạn đang được ghi nhận

1. XML payload hiện có cấu trúc lồng `application_history_array.application_history`, `document_array.document`, `payment_array.payment`; parser Silver đang chờ mảng trực tiếp. XML contribution cần sửa riêng trước khi bật cho dữ liệu thật. CDC-only vẫn chạy được.
2. `dim_thoi_gian` mới có cuối tuần và ngày lễ dương cố định. KPI SLA chỉ hoàn toàn chính xác khi bổ sung lịch nghỉ Tết/hoán đổi ngày làm việc chính thức.
3. Rework chưa có event nghiệp vụ riêng; REJECTED chỉ là proxy demo.
4. Agency và Service được xem là bất biến sau khi hồ sơ được tạo. Nếu nghiệp vụ cho phép đổi hai thuộc tính này, cần snapshot chúng vào history hoặc xây SCD để không gán lại lịch sử theo giá trị hiện tại.
5. Compose hiện phục vụ Spark, MinIO, Kafka, Debezium, Hive Metastore và StarRocks. Airflow/Trino/Superset chưa được khai báo service trong `docker-compose.yml`; các DAG/SQL kết nối tương ứng chỉ chạy được khi các service đó được triển khai thêm.
