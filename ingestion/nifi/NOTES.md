# NiFi flow — gọi External API (JSON)

Export template flow (.json/.xml) vào thư mục này để mọi người import.

Checklist tuning (theo bảng action items):
- [ ] Backpressure threshold trên các connection
- [ ] Heap size (chỉnh trong docker-compose: NIFI_JVM_HEAP_MAX)
- [ ] Concurrent tasks trên processor gọi API
- [ ] Cơ chế retry khi API lỗi (RetryFlowFile / penalty)
- [ ] Dùng processor chuẩn (InvokeHTTP, EvaluateJsonPath...) để tối ưu bộ nhớ
- [ ] Cảnh báo khi queue nghẽn (Reporting Task / monitor bulletin)
