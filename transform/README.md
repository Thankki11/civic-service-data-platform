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
Debezium/Kafka Application -----------------+
                                             +-> Spark stream-stream join (5s)
Debezium/Kafka Application_History ---------+       -> fact_xu_ly_ho_so_stream (physical PK)

Debezium/Kafka Application/History/Document/Applicant
                                             -> ingest team -> Bronze Iceberg
```

Hai đường cùng dùng các khóa nghiệp vụ `ho_so_id`, `co_quan_id`, `dv_cong_id`, `trang_thai_id`, `can_bo_id` và `thoi_gian_id`. Batch tối ưu cho snapshot/aggregate theo ngày; StarRocks tối ưu drill-down sự kiện gần thời gian thực.

## 2. Grain và nghiệp vụ cần nhớ

| Đối tượng | Grain | Nguồn chính | Ý nghĩa |
|---|---:|---|---|
| `silver.application` | 1 dòng / hồ sơ | XML + CDC | Hợp nhất hai tập hồ sơ bổ sung trong data mẫu; chọn version mới nhất theo `event_time`. |
| `silver.application_history` | 1 dòng / hành động | XML + CDC | Nhật ký bất biến, union hai input rồi append chống trùng theo `history_id`; partition Iceberg theo `days(action_time)`. |
| `gold.fact_ton_dong_ho_so` | 1 dòng / hồ sơ mở / ngày | Silver application + history + dim SCD2 | Snapshot backlog ở 23:59:59, mang surrogate key version của dim. |
| `gold.fact_van_hanh_co_quan` | 1 dòng / cơ quan / ngày | Các fact/nguồn Silver + dim SCD2 | KPI lãnh đạo và version cơ quan tại ngày chốt. |
| `gold_realtime.fact_xu_ly_ho_so_stream` | 1 dòng / hành động | Spark join Application + Application_History CDC | Physical realtime fact, duration từng bước xử lý. |

Trạng thái: `RECEIVED(1) -> ASSIGNED(2) -> PROCESSING(3) -> PENDING_APPROVAL(4) -> APPROVED(5) -> READY(6) -> COMPLETED(7)`; `REJECTED(8)` là trạng thái kết thúc.

`Application_History` là nguồn duy nhất cho lịch sử. Không suy luận lịch sử từ `before/after` của bảng `Application`, vì bảng này chỉ giữ trạng thái cuối cùng.

## 3. Điều kiện trước khi chạy transform

1. Docker Desktop/Engine đang chạy.
2. Bronze master data cần sẵn sàng. XML, CDC và API được đọc độc lập và union khi cùng tạo một thực thể Silver; bảng input chưa được ingest chỉ bị bỏ qua riêng input đó.
3. Compose tự tạo Kafka topic, đăng ký Debezium connector và tạo bảng StarRocks bằng các init service idempotent. Cấu hình PostgreSQL CDC và job Kafka → Bronze thuộc phạm vi ingest.
   Riêng `public."Application"` phải có `REPLICA IDENTITY FULL` để Debezium gửi
   `before.Statusid` và `before.updated_at` cho fact realtime; đây là migration
   một lần ở source, không cần một PostgreSQL init container chạy thường trực.
4. PostgreSQL `source_db` chứa hai database tách biệt: `source_db` cho OLTP/CDC và
   `metastore` cho Hive metadata. Init script tạo `metastore` khi volume PostgreSQL
   được khởi tạo lần đầu, nên không cần container PostgreSQL thứ hai.
5. Chỉ khi dùng simulator demo, `data-master-gen` cần chạy trước để tạo schema và
   master data mà simulator tham chiếu.

Các bucket `landing-zone`, `lakehouse`, `quarantine-zone` được service `minio-init` tạo tự động khi compose khởi động.

## 4. Khởi động môi trường demo

Từ PowerShell, tại root project:

```powershell
docker compose up -d --build
docker compose ps
```

`data-master-gen` chỉ chạy thủ công khi cần bootstrap dữ liệu demo; nó không
được tự khởi động cùng hạ tầng và không phải dependency của transform.

`docker compose up` tự khởi động `spark-fact-stream`. Container này chỉ giữ
Spark driver; executor chạy trên một worker local được giới hạn 8 cores/6 GB.
Kiểm tra connector và realtime stream:

```powershell
Invoke-RestMethod -Uri 'http://localhost:8084/connectors/postgres-connector/status'
docker compose ps spark-fact-stream
docker compose logs --tail=100 spark-fact-stream
```

Khi cần sinh CDC realtime:

```powershell
docker compose --profile streaming up -d stream-simulator
```

`stream-simulator` vẫn ở profile riêng vì chỉ là nguồn sinh sự kiện demo, không
phải thành phần production của pipeline.

## 5. Thứ tự chạy transform lần đầu

### 5.1 Tạo StarRocks serving schema

`starrocks-init` chạy `ddl_realtime.sql` và `ddl_streaming_fact.sql` để tạo năm
dimension mirror và physical fact. Nó không tạo semantic/BI view.
Spark ghi qua FE proxy tích hợp của image all-in-one tại `starrocks:8080`; proxy
theo Stream Load redirect tới BE loopback bên trong đúng container StarRocks.

### 5.2 Đưa dữ liệu vào Bronze bằng các ingest job có sẵn

Các job ingest của nhóm phụ trách nguồn tạo `application_cdc`,
`application_history_cdc`, `document_cdc`, `applicant_cdc`. Transform chỉ coi
các bảng Bronze này là input contract và không điều phối ingest.

### 5.3 Bronze → Silver

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --conf spark.cores.max=4 `
  --executor-cores 4 --executor-memory 3g `
  /opt/spark-data/transform/spark-etl/bronze_to_silver.py
```

Lệnh `docker compose exec` chỉ là cách chạy tay khi Airflow chưa được cài. Khi có
Airflow, `SparkSubmitOperator` trên Airflow worker thay thế lệnh này và giữ vai
trò driver client-mode; Compose không tạo batch driver container riêng.

### 5.4 Build dimensions

Chạy lần đầu và chạy lại khi Agency/Status/Service/Officer/Role hoặc lịch nghỉ thay đổi.

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --conf spark.cores.max=4 `
  --executor-cores 4 --executor-memory 3g `
  /opt/spark-data/transform/spark-agg/build_dim_tables.py
```

`dim_thoi_gian` giữ lịch sử từ `CALENDAR_START_DATE=2023-01-01`, nhưng tương lai
chỉ đến ngày hiện tại cộng `CALENDAR_DAYS_AHEAD=7`. Mỗi ngày job chỉ thêm ngày
còn thiếu; nếu bản demo cũ đã sinh xa hơn horizon thì phần tương lai dư được
xóa khỏi cả Iceberg và StarRocks.

### 5.5 Silver → Gold batch

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 --conf spark.cores.max=4 `
  --executor-cores 4 --executor-memory 3g `
  /opt/spark-data/transform/spark-agg/silver_to_gold.py
```

Job chốt số cho **hôm qua**. Hai Gold fact là periodic snapshot: job thay thế nguyên partition `thoi_gian_id` của ngày đang chốt bằng atomic Iceberg overwrite. Cách này idempotent khi chạy lại và cũng xóa dòng backlog cũ nếu hồ sơ đã đóng sau khi tính lại; các partition ngày khác không bị ảnh hưởng.

Backfill chạy từng ngày để mỗi lần ghi là một Iceberg commit atomic và dễ retry:

```powershell
docker compose exec -T spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.cores.max=4 --executor-cores 4 --executor-memory 3g `
  /opt/spark-data/transform/spark-agg/silver_to_gold.py --snapshot-date 2026-07-15
```

Khi backfill, `Application` chỉ dùng cho các thuộc tính bất biến (`created_at`, `co_quan_id`, `dv_cong_id`...). Trạng thái mở/đóng của hồ sơ luôn là event mới nhất trong `Application_History` không vượt quá `23:59:59` của ngày backfill; vì vậy `Statusid` hiện tại trong `Application` không làm sai snapshot quá khứ. Backfill dùng để tạo/điều chỉnh ảnh chụp Gold cho ngày quá khứ khi Bronze/Silver đã nhận dữ liệu muộn, không phải để sinh thêm dữ liệu hằng ngày.

### 5.6 Spark Structured Streaming StarRocks

Compose không còn batch driver service. Airflow sau này submit từng application
theo thứ tự; process task trên Airflow worker chạy driver client-mode, còn
scheduler/webserver Airflow không kiêm driver. Mỗi application vẫn là một task
riêng để retry và quan sát độc lập.

`docker compose up -d` tự chạy `spark-fact-stream`: join
`Application.id = Application_History.Applicationid` và khớp cặp trạng thái,
rồi ghi physical Primary Key
fact trong StarRocks. `debezium-init` tự POST/PUT connector; `starrocks-init` tự
tạo bảng trước khi driver được phép start. Service có `restart: unless-stopped`.

Job dùng trigger 5 giây, watermark 1 phút và checkpoint riêng trên MinIO.
Compose đặt `STARTING_OFFSETS=earliest` để lần chạy đầu không bỏ sót event đã có
trong Kafka; các lần restart sau Spark tiếp tục từ offsets trong checkpoint.
Muốn replay phải dùng một checkpoint path mới, không tái sử dụng checkpoint cũ.

Kiểm tra đường realtime chính:

```sql
SELECT COUNT(*), MAX(fact_loaded_at)
FROM gold_realtime.fact_xu_ly_ho_so_stream;
```

`kafka_to_fact_latency_ms` lấy Kafka record timestamp tới thời điểm Spark bắt
đầu gửi dòng fact qua Stream Load. Nó phản ánh Kafka wait + trigger + join và
thiếu vài mili giây commit/visibility của StarRocks. Nếu cần đo end-to-end tuyệt
đối, cần thêm probe bên ngoài ghi nhận lúc một `history_id` truy vấn thấy được.

Checkpoint trên MinIO có thêm network I/O ở cuối micro-batch, nhưng với state
join chỉ giữ một phút và trigger 5 giây thì chi phí thường nhỏ hơn đáng kể so với
chu kỳ trigger. Đây không phải một database mới: Spark dùng state store mặc định
và MinIO chỉ giữ log/checkpoint bền vững để phục hồi. Không đặt checkpoint trên
filesystem của container vì mất worker hoặc recreate container sẽ mất khả năng
resume an toàn.

Ở các ngày tiếp theo, Airflow chạy: Bronze sẵn sàng → `bronze_to_silver.py` →
`build_dim_tables.py` → `silver_to_gold.py`. Dimension job chạy lại hằng ngày
vẫn idempotent và đồng thời bổ sung ngày còn thiếu cho `dim_thoi_gian`.

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

- `dim_thoi_gian`: giữ lịch sử từ 2023, chỉ sinh tăng dần đến `today + 7 ngày`, gồm thứ/ngày/tháng/quý/năm, cờ nghỉ và `stt_ngay_lam_viec` tích lũy.
- `stt_ngay_lam_viec` là kỹ thuật quan trọng: thay vì join mọi ngày trong một khoảng thời gian, số ngày làm việc = `seq_ngày_kết_thúc - seq_ngày_bắt_đầu`.
- `dim_co_quan`: Agency join Province/Ward bằng Spark broadcast; Iceberg Gold lưu SCD2 theo thay đổi tên/địa bàn.
- `dim_trang_thai`: map id/code/name, có version SCD2 để không đổi nhãn lịch sử nếu danh mục bị sửa.
- `dim_can_bo`: Officer join Officer_Role/Role, chọn một role đại diện, thêm member `-1`, và lưu SCD2 khi tên/vị trí đổi.
- `dim_dich_vu_cong`: Service, trong đó `processing_time` trở thành SLA `thoi_han_tra_kq`; đây là SCD2 quan trọng nhất.
- Iceberg batch dùng SCD2 (`*_sk`, `effective_from_ts`, `effective_to_ts`, `is_current`). Thay đổi được phát hiện bằng so sánh null-safe trực tiếp các tracked column nghiệp vụ; StarRocks giữ Type 1 current-state trong Primary Key table để truy vấn realtime có thể broadcast join khi xây lớp BI sau này.
- Vì dimension nhỏ, JDBC ghi theo `coalesce(1)`, batch 1.000 dòng và `rewriteBatchedStatements`.

### `spark-agg/silver_to_gold.py`

**Quy ước thời gian**

- `snapshot_date = hôm qua` theo UTC+07:00; `cutoff_ts = 23:59:59` của ngày đó.
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

- Tạo 5 Primary Key dimension mirror, được nạp bởi `build_dim_tables.py`.

### `starrocks/ddl_streaming_fact.sql`

- `fact_xu_ly_ho_so_stream`: physical Primary Key `(ho_so_id, id)` do Spark ghi.
- Không tạo view BI; phần trình bày sẽ tự join fact với dimension khi được triển khai.

### `spark-streaming/kafka_to_starrocks_fact.py`

- Đọc hai topic bằng hai Kafka streaming source độc lập.
- Join `Application.id = Application_History.Applicationid` cùng cặp trạng thái trước/sau. `source.txId` chỉ được giữ làm metadata đối soát, không còn là điều kiện join. Watermark và time range một phút chỉ giới hạn streaming state.
- `thoi_gian_xu_ly` lấy `Application.after.updated_at - Application.before.updated_at`; `Application_History.action_time` vẫn là thời điểm nghiệp vụ của fact. Event tạo hồ sơ đầu tiên có duration `NULL`.
- Ghi `fact_xu_ly_ho_so_stream` bằng StarRocks Spark Connector, trigger và connector flush cùng 5 giây. Primary Key `(ho_so_id, id)` làm replay/retry idempotent.
- Lưu Kafka partition/offset, source time và các latency millisecond để đối soát và giám sát.

### `warehouse/ddl/gold_dim_fact.sql` và `warehouse/validation/*`

- DDL là contract Iceberg Gold. `ho_so_id` và history id là STRING, không ép các mã `HS_xxx`, `H_xxx` về BIGINT.
- `validate_bi_vs_source.sql` kiểm tra số dòng, và đối soát backlog aggregate với snapshot atomic.

## 7. Kiểm tra sau khi chạy

```sql
-- Physical fact tăng và timestamp load gần thời gian hiện tại.
SELECT COUNT(*), MAX(fact_loaded_at)
FROM gold_realtime.fact_xu_ly_ho_so_stream;

-- Primary Key bảo đảm một history event chỉ có một fact.
SELECT COUNT(*) AS rows, COUNT(DISTINCT id) AS history_ids
FROM gold_realtime.fact_xu_ly_ho_so_stream;
```

## 8. Giới hạn đang được ghi nhận

1. Bronze master phải chạy snapshot ít nhất hai lần để SCD2 có thể phát hiện thay đổi. Lần migrate đầu tiên bootstrap version hiện có từ `2023-01-01`; không thể khôi phục business-effective date trước khi pipeline bắt đầu lưu snapshot.
2. `dim_thoi_gian` mới có cuối tuần và ngày lễ dương cố định. KPI SLA chỉ hoàn toàn chính xác khi bổ sung lịch nghỉ Tết/hoán đổi ngày làm việc chính thức.
3. Rework chưa có event nghiệp vụ riêng; REJECTED chỉ là proxy demo.
4. Agency và Service được xem là bất biến sau khi hồ sơ được tạo. Nếu nghiệp vụ cho phép đổi hai thuộc tính này, cần snapshot chúng vào history hoặc xây SCD để không gán lại lịch sử theo giá trị hiện tại.
5. Compose hiện phục vụ Spark, MinIO, Kafka, Debezium, Hive Metastore,
   StarRocks và Trino. Airflow/Superset chưa được khai báo service; DAG/dashboard
   chỉ chạy được khi triển khai thêm hai service đó.
