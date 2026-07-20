# Hướng Dẫn Cấu Hình Jenkins CI/CD (Data Lakehouse Platform)

## 1. Truy Cập Jenkins

Sau khi `docker compose up -d`, truy cập Jenkins tại:

```
http://localhost:8090
```

Lấy mật khẩu admin lần đầu:

```powershell
docker exec data-lakehouse-skeleton-jenkins-1 cat /var/jenkins_home/secrets/initialAdminPassword
```

---

## 2. Cài Đặt Plugins Cần Thiết

Tại **Manage Jenkins → Plugins → Available plugins**, tìm và cài:

| Plugin | Mục đích |
|---|---|
| **Git** | Kết nối GitHub repository |
| **Pipeline** | Declarative Pipeline syntax |
| **Docker Pipeline** | Chạy `docker build` trong pipeline |
| **Workspace Cleanup** | `cleanWs()` trong post block |
| **Timestamper** | Hiển thị timestamp trong log |
| **AnsiColor** | Màu sắc trong console log |

> Chọn **Install suggested plugins** ở bước đầu tiên là đủ cho hầu hết các plugin trên.

---

## 3. Khai Báo Credentials

Vào **Manage Jenkins → Credentials → (global) → Add Credentials**:

### Credential 1: Telegram Bot Token
- **Kind**: Secret text
- **ID**: `telegram-bot-token`
- **Secret**: `<token lấy từ .env: TELEGRAM_BOT_TOKEN>`

### Credential 2: Telegram Chat ID
- **Kind**: Secret text
- **ID**: `telegram-chat-id`
- **Secret**: `<chat ID lấy từ .env: TELEGRAM_CHAT_ID>`

---

## 4. Tạo Multibranch Pipeline Job

1. Vào **New Item** → Đặt tên `data-lakehouse-cicd` → Chọn **Multibranch Pipeline** → OK.
2. Trong **Branch Sources** → Thêm **Git**:
   - **Project Repository**: `https://github.com/Thankki11/civic-service-data-platform.git`
3. Trong **Build Configuration**:
   - **Mode**: by Jenkinsfile
   - **Script Path**: `orchestration/jenkins/Jenkinsfile`
4. Trong **Scan Multibranch Pipeline Triggers**:
   - Tích **Periodically if not otherwise run**: `1 minute`
5. Nhấn **Save** → Jenkins sẽ tự scan và tạo job cho nhánh `dev`.

---

## 5. Cấp Quyền Docker Cho Jenkins Container

Jenkins cần quyền gọi Docker socket của host. Sau khi compose up:

```powershell
# Kiểm tra GID của docker group trên host (Windows/WSL2):
# Trong WSL terminal:
getent group docker

# Hoặc chạy lệnh này để Jenkins có thể dùng docker:
docker exec -u root data-lakehouse-skeleton-jenkins-1 apt-get install -y docker.io
```

> Lưu ý: `docker-compose.yml` đã mount `/var/run/docker.sock` vào Jenkins container.

---

## 6. Kiểm Tra Pipeline Hoạt Động

Sau khi setup xong:

1. Push bất kỳ thay đổi lên nhánh `dev`.
2. Jenkins sẽ phát hiện trong vòng 1 phút và tự động chạy pipeline.
3. Quan sát tại **http://localhost:8090/job/data-lakehouse-cicd/job/dev/**
4. Nhận thông báo kết quả qua Telegram.

---

## 7. Luồng CI/CD Tổng Quan

```
push lên dev
    │
    ▼ (Jenkins poll mỗi 1 phút)
Stage 1: Lint & Static Analysis
    ├── py_compile tất cả DAG files
    ├── ruff style check
    └── yaml.safe_load kiểm tra docker-compose & registry.yaml
    │
Stage 2: Unit Tests
    └── pytest tests/ (test_dag_parse.py + test_registry_yaml.py)
    │
Stage 3: Build Docker Images
    ├── docker build apache_spark:3.5.1
    ├── docker build pipeline-api:latest
    └── docker build airflow-custom:latest
    │
Stage 4: Deploy to Staging
    ├── docker cp DAGs → airflow container
    ├── docker cp plugins → airflow container
    ├── docker cp alerts → airflow container
    └── docker restart pipeline-api (nếu registry.yaml thay đổi)
    │
Post: Telegram notify (✅ SUCCESS / ❌ FAILURE)
```
