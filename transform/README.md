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
| `silver.application` | 1 dòng / hồ sơ | XML + CDC | Hợp nhất hai tập hồ sơ bổ sung trong data mẫu; chọn version mới nhất theo `event_time`. |
| `silver.application_history` | 1 dòng / hành động | XML + CDC | Nhật ký bất biến, union hai input rồi append chống trùng theo `history_id`; partition Iceberg theo `days(action_time)`. |
| `gold.fact_ton_dong_ho_so` | 1 dòng / hồ sơ mở / ngày | Silver application + history + dim SCD2 | Snapshot backlog ở 23:59:59, mang surrogate key version của dim. |
| `gold.fact_van_hanh_co_quan` | 1 dòng / cơ quan / ngày | Các fact/nguồn Silver + dim SCD2 | KPI lãnh đạo và version cơ quan tại ngày chốt. |
| `gold_realtime.fact_xu_ly_ho_so` | 1 dòng / hành động | StarRocks ODS history + application | Duration từng bước xử lý realtime. |

Trạng thái: `RECEIVED(1) -> ASSIGNED(2) -> PROCESSING(3) -> PENDING_APPROVAL(4) -> APPROVED(5) -> READY(6) -> COMPLETED(7)`; `REJECTED(8)` là trạng thái kết thúc.

`Application_History` là nguồn duy nhất cho lịch sử. Không suy luận lịch sử từ `before/after` của bảng `Application`, vì bảng này chỉ giữ trạng thái cuối cùng.

## 3. Điều kiện trước khi chạy transform

1. Docker Desktop/Engine đang chạy.
2. Bronze master data cần sẵn sàng. XML, CDC và API được đọc độc lập và union khi cùng tạo một thực thể Silver; bảng input chưa được ingest chỉ bị bỏ qua riêng input đó.
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

Job chốt số cho **hôm qua**. Hai Gold fact là periodic snapshot: job thay thế nguyên partition `thoi_gian_id` của ngày đang chốt bằng atomic Iceberg overwrite. Cách này idempotent khi chạy lại và cũng xóa dòng backlog cũ nếu hồ sơ đã đóng sau khi tính lại; các partition ngày khác không bị ảnh hưởng.

Backfill chạy từng ngày để mỗi lần ghi là một Iceberg commit atomic và dễ retry:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-data/transform/spark-agg/silver_to_gold.py `
  --snapshot-date 2026-07-15
```

Khi backfill, `Application` chỉ dùng cho các thuộc tính bất biến (`created_at`, `co_quan_id`, `dv_cong_id`...). Trạng thái mở/đóng của hồ sơ luôn là event mới nhất trong `Application_History` không vượt quá `23:59:59` của ngày backfill; vì vậy `Statusid` hiện tại trong `Application` không làm sai snapshot quá khứ. Backfill dùng để tạo/điều chỉnh ảnh chụp Gold cho ngày quá khứ khi Bronze/Silver đã nhận dữ liệu muộn, không phải để sinh thêm dữ liệu hằng ngày.

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

**Spark session và các hàm ghi Silver**

- Kết nối Hive Metastore/Iceberg qua MinIO.
- Bật AQE và skew join để hạn chế OOM.
- Chiến lược ghi Silver theo nghĩa nghiệp vụ: master data là full snapshot nên replace cả bảng; `application`, `document`, `applicant` là current-state nên `MERGE`; `application_history` và `payment` là event/giao dịch bất biến nên `APPEND` chống trùng theo business key.
- Theo generator mẫu, XML (`HS_00001...`) và CDC (`HS_100000...`) là hai tập hồ sơ bổ sung. Job union các input này; `tableExists` chỉ dùng để không đọc một input chưa ingest, không còn là quy tắc chọn nguồn.

**Master data**

- Đọc 10 bảng `bronze_master_data.*`.
- Trim chuỗi, bỏ metadata ingest, `dropDuplicates()`.
- Ghi thành `silver.province`, `ward`, `status`, `service`, `agency`, `role`, `permission`, `document_type`, `officer`, `officer_role`.

**Application: điểm quan trọng nhất của Silver**

- XML `PARTIAL_STATUS` được forward-fill bằng `last(..., ignorenulls=True)` sau khi union với CDC, rồi chọn event mới nhất cho mỗi `ho_so_id`.
- `da_bi_xoa` được lấy từ event DELETE của XML; `event_time` được giữ để Gold xét cutoff.

**Application history và document**

- ID là business key dạng chuỗi (`H_xxx`, `DOC_xxx`), tuyệt đối không cast sang số.
- Union XML và CDC; trong tập hợp chung dedup theo `history_id` hoặc `document_id` bằng event mới nhất.
- `can_bo_id = -1` là Unknown member, giúp fact join được dim ngay cả khi hồ sơ mới nhận chưa phân công.

**Payment và applicant**

- Payment union XML và API. XML dùng `transaction_code`; mock API không có transaction id nên dùng `tax_code` làm payment reference, tránh gộp hai giao dịch cùng hồ sơ/cùng giây.
- Applicant là SCD current-state đơn giản: giữ version có `updated_at` mới nhất.

### `spark-agg/build_dim_tables.py`

File này xây cả Iceberg Gold và StarRocks dimensions trong cùng một lần chạy.

- `dim_thoi_gian`: date spine demo 2023–2028, thứ/ngày/tháng/quý/năm, cờ nghỉ và `stt_ngay_lam_viec` tích lũy.
- `stt_ngay_lam_viec` là kỹ thuật quan trọng: thay vì join mọi ngày trong một khoảng thời gian, số ngày làm việc = `seq_ngày_kết_thúc - seq_ngày_bắt_đầu`.
- `dim_co_quan`: Agency join Province/Ward bằng Spark broadcast; Iceberg Gold lưu SCD2 theo thay đổi tên/địa bàn.
- `dim_trang_thai`: map id/code/name, có version SCD2 để không đổi nhãn lịch sử nếu danh mục bị sửa.
- `dim_can_bo`: Officer join Officer_Role/Role, chọn một role đại diện, thêm member `-1`, và lưu SCD2 khi tên/vị trí đổi.
- `dim_dich_vu_cong`: Service, trong đó `processing_time` trở thành SLA `thoi_han_tra_kq`; đây là SCD2 quan trọng nhất.
- Iceberg batch dùng SCD2 (`*_sk`, `effective_from_ts`, `effective_to_ts`, `is_current`). Thay đổi được phát hiện bằng so sánh null-safe trực tiếp các tracked column nghiệp vụ; StarRocks giữ Type 1 current-state trong Primary Key table để MV realtime broadcast join nhanh.
- Vì dimension nhỏ, JDBC ghi theo `coalesce(1)`, batch 1.000 dòng và `rewriteBatchedStatements`.

### `spark-agg/silver_to_gold.py`

**Quy ước thời gian**

- `snapshot_date = hôm qua`; `cutoff_ts = 23:59:59` của ngày đó.
- Fact Gold partition theo `thoi_gian_id = yyyymmdd`.
- Hai fact Gold là periodic snapshot, do đó replace duy nhất partition `thoi_gian_id` của ngày chốt thay vì `APPEND` hoặc `MERGE` theo dòng.

**`fact_ton_dong_ho_so`**

- Lấy action history mới nhất **không vượt cutoff**, không dùng trạng thái Silver hiện tại.
- Điều này tránh lỗi khi job 02:00 đã nhìn thấy cập nhật của ngày mới: trạng thái COMPLETED lúc 01:00 hôm nay không được làm mất backlog của hôm qua.
- Hồ sơ mở là trạng thái khác COMPLETED/REJECTED tại cutoff.
- `so_ngay_ton_dong_hien_tai`: ngày làm việc từ action gần nhất đến cutoff.
- `tong_thoi_gian_da_xu_ly`: ngày làm việc từ lúc tiếp nhận đến cutoff.
- `can_bo_id`: officer ở action gần nhất; nếu chưa có là `-1`.
- Fact lưu thêm `dim_trang_thai_sk`, `dim_co_quan_sk`, `dim_can_bo_sk`, `dim_dich_vu_cong_sk` để BI join đúng version dimension.

**`fact_van_hanh_co_quan`**

- Base từ toàn bộ Agency để mỗi cơ quan luôn có một dòng, kể cả ngày không phát sinh.
- `so_luong_tiep_nhan`: Application có `created_at` trong ngày.
- Đúng/trễ hạn: chỉ event COMPLETED trong ngày; SLA = ngày làm việc từ created đến completed so với version `dim_dich_vu_cong` có hiệu lực khi tiếp nhận.
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

1. Bronze master phải chạy snapshot ít nhất hai lần để SCD2 có thể phát hiện thay đổi. Lần migrate đầu tiên bootstrap version hiện có từ `2023-01-01`; không thể khôi phục business-effective date trước khi pipeline bắt đầu lưu snapshot.
2. `dim_thoi_gian` mới có cuối tuần và ngày lễ dương cố định. KPI SLA chỉ hoàn toàn chính xác khi bổ sung lịch nghỉ Tết/hoán đổi ngày làm việc chính thức.
3. Rework chưa có event nghiệp vụ riêng; REJECTED chỉ là proxy demo.
4. Agency và Service được xem là bất biến sau khi hồ sơ được tạo. Nếu nghiệp vụ cho phép đổi hai thuộc tính này, cần snapshot chúng vào history hoặc xây SCD để không gán lại lịch sử theo giá trị hiện tại.
5. Compose hiện phục vụ Spark, MinIO, Kafka, Debezium, Hive Metastore và StarRocks. Airflow/Trino/Superset chưa được khai báo service trong `docker-compose.yml`; các DAG/SQL kết nối tương ứng chỉ chạy được khi các service đó được triển khai thêm.
