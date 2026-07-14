# Data Dictionary — Thống nhất Business Logic & KPI

Tài liệu hóa định nghĩa chỉ số để tránh hiểu lầm giữa đội ngũ Kỹ thuật dữ liệu (Data Engineer), Phân tích dữ liệu (Data Analyst) và các bên Nghiệp vụ (Business). Các công thức này sẽ được áp dụng tại tầng Presentation (Trino / Apache Superset) để trực quan hóa lên Dashboard Lãnh đạo.

## 1. Nhóm Chỉ số Hiệu suất Cơ quan (Aggregated KPIs)

Nhóm chỉ số này phục vụ Lãnh đạo cấp cao nhìn nhận bức tranh tổng thể, truy vấn trực tiếp từ bảng tổng hợp `lakehouse.gold.fact_van_hanh_co_quan`.

| Chỉ số / KPI | Định nghĩa nghiệp vụ | Công thức | Bảng nguồn | Ghi chú |
| --- | --- | --- | --- | --- |
| **Tổng hồ sơ tiếp nhận** | Số lượng hồ sơ mới được nộp và bắt đầu đưa vào quy trình xử lý trong kỳ báo cáo (ngày/tháng/năm). | `SUM(so_luong_tiep_nhan)` | `gold.fact_van_hanh_co_quan` | Dùng để đo lường đầu vào. |
| **Tổng doanh thu phí/lệ phí** | Tổng số tiền đã thu từ các hồ sơ giao dịch thành công. | `SUM(tong_chi_phi)` | `gold.fact_van_hanh_co_quan` | Kiểu dữ liệu `BIGINT` (VNĐ). |
| **Tổng tồn đọng cuối ngày** | Số lượng hồ sơ chưa hoàn thành, đang nằm chờ xử lý tính đến cuối kỳ báo cáo. | `SUM(so_luong_ton_dong)` | `gold.fact_van_hanh_co_quan` |  Phản ánh "hàng tồn kho" thực tế. |
| **Tỷ lệ trễ hạn (%)** | Tỷ trọng hồ sơ xử lý quá hạn quy định trên tổng khối lượng công việc đang gánh vác (bao gồm hồ sơ đã đóng và hồ sơ đang bị ngâm). | `(SUM(so_luong_tre_han) / (SUM(so_luong_tiep_nhan) + SUM(so_luong_ton_dong))) * 100` | `gold.fact_van_hanh_co_quan` | KPI quan trọng nhất để đánh giá chất lượng dịch vụ hành chính công. |
| **Tỷ lệ Rework (%)** | Tỷ lệ hồ sơ bị trả lại hoặc yêu cầu công dân bổ sung giấy tờ so với lượng nộp vào. | `(SUM(so_luong_rework) / SUM(so_luong_tiep_nhan)) * 100` | `gold.fact_van_hanh_co_quan` |  Đo lường tính phức tạp hoặc bất cập của UX/UI đầu vào.|
| **Tải công việc (Workload)** | Khối lượng hồ sơ mà một cơ quan phải xử lý trong kỳ (Mới nhận + Đang tồn). | `SUM(so_luong_tiep_nhan) + SUM(so_luong_ton_dong)` | `gold.fact_van_hanh_co_quan` | Hỗ trợ phân bổ, điều động nhân sự. |

## 2. Nhóm Chỉ số Chi tiết (Transactional & Snapshot KPIs)

Nhóm chỉ số này dùng để phân tích chuyên sâu (Drill-down) tìm nguyên nhân gốc rễ (Bottleneck) khi phát hiện vấn đề ở cấp cơ quan. Dữ liệu lấy từ `gold.fact_xu_ly_ho_so` và `gold.fact_ton_dong_ho_so`.

| Chỉ số / KPI | Định nghĩa nghiệp vụ | Công thức | Bảng nguồn | Ghi chú |
| --- | --- | --- | --- | --- |
| **Số lượng tồn đọng theo Trạng thái** | Lượng hồ sơ đang kẹt lại tại một khâu cụ thể (VD: Chờ phân công, Đang thẩm định). | `SUM(so_luong)` kết hợp `GROUP BY trang_thai_id` | `gold.fact_ton_dong_ho_so` | Có thể lọc thêm `WHERE co_quan_id = X` để xem nút thắt của riêng 1 cơ quan. |
| **Thời gian xử lý trung bình** | Thời gian trung bình để hoàn thành một bước xử lý hồ sơ thực tế. | `AVG(thoi_gian_xu_ly)` | `gold.fact_xu_ly_ho_so` |  Đã loại trừ thời gian của cột `co_phai_la_ngay_nghi` từ bảng `dim_thoi_gian`.|
| **Tuổi tồn đọng trung bình** | Thời gian trung bình (tính từ lúc tiếp nhận) của các hồ sơ đang bị kẹt lại chưa hoàn thành. | `AVG(tong_thoi_gian_da_xu_ly)` | `gold.fact_ton_dong_ho_so` |  Dùng để vẽ biểu đồ Báo cáo tuổi nợ (Aging Report). |
| **Số ngày ngâm hồ sơ tại khâu** | Khoảng thời gian hồ sơ bị đình trệ trên bàn làm việc của 1 cán bộ cụ thể hoặc 1 trạng thái cụ thể. | `AVG(so_ngay_ton_dong_hien_tai)` | `gold.fact_ton_dong_ho_so` |  Dùng để xác định nút thắt cổ chai trong quy trình.|

## 3. Các chiều Phân tích (Dimensions)

Để cắt lớp (Slicing/Dicing) các chỉ số trên, Superset sẽ kết hợp (JOIN) các bảng Fact với các bảng Dimension theo khóa chính (`INT`):

*   **Theo Cơ quan:** Drill-down hiệu suất từ toàn hệ thống xuống từng đơn vị thông qua `gold.dim_co_quan USING (co_quan_id)`.
*   **Theo Thời gian:** Xem xét xu hướng lịch sử qua `gold.dim_thoi_gian USING (thoi_gian_id)`.
*   **Theo Trạng thái:** Phân tích điểm nghẽn quy trình qua `gold.dim_trang_thai USING (trang_thai_id)`.
*   **Theo Nhân sự:** Đánh giá năng lực giải quyết của cá nhân qua `gold.dim_can_bo USING (can_bo_id)`. *(Lưu ý: Job ETL cần set mặc định `can_bo_id = -1` (Unknown) đối với các hồ sơ mới nộp vào luồng chờ phân công để tránh lỗi JOIN rỗng).*
*   **Theo Thủ tục:** Đo lường mức độ phức tạp của từng loại dịch vụ qua `gold.dim_dich_vu_cong USING (dv_cong_id)`.