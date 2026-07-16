# _hivejars — vá hive-metastore

`apache/hive:4.0.0` không kèm driver JDBC Postgres + hadoop-aws → không tự tạo
được schema và thư mục bảng trên S3/MinIO. `docker-compose.override.yml` mount
các jar ở đây vào `/opt/hive/lib` + `core-site.xml` vào Hadoop conf.

Chỉ `core-site.xml` được commit. Các file `.jar` bị `.gitignore` chặn (nặng,
GitHub giới hạn 100MB). Lấy lại jar bằng cách trích từ image Spark đã build:

```bash
docker compose build spark-master
cid=$(docker create apache_spark:3.5.1)
for j in postgresql-42.6.0.jar hadoop-aws-3.3.4.jar aws-java-sdk-bundle-1.12.262.jar; do
  docker cp "$cid:/opt/spark/jars/$j" "_hivejars/$j"
done
docker rm "$cid"
```
