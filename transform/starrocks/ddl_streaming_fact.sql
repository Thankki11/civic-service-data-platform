-- Bang fact vat ly DUY NHAT cho realtime, duoc Spark Structured Streaming
-- ghi truc tiep qua StarRocks Spark Connector. Primary Key giup replay/retry
-- theo checkpoint khong nhan doi cung mot history event.

CREATE DATABASE IF NOT EXISTS gold_realtime;
USE gold_realtime;

CREATE TABLE IF NOT EXISTS fact_xu_ly_ho_so_stream (
    ho_so_id                      VARCHAR(20)  NOT NULL,
    id                            VARCHAR(20)  NOT NULL COMMENT 'Application_History.id',
    ten_ho_so                     VARCHAR(500),
    applicant_id                  VARCHAR(20),
    co_quan_id                    INT,
    dv_cong_id                    INT,
    trang_thai_truoc_id           INT,
    trang_thai_id                 INT,
    can_bo_id                     INT,
    created_at                    DATETIME,
    previous_action_time          DATETIME COMMENT 'Application.before.updated_at',
    action_time                   DATETIME COMMENT 'Application_History.action_time',
    thoi_gian_xu_ly_seconds       BIGINT COMMENT 'Application.after.updated_at - before.updated_at',
    thoi_gian_xu_ly               DOUBLE COMMENT 'So gio xu ly cua buoc trang thai',
    thoi_gian_id                  INT,
    transaction_id               VARCHAR(100),
    application_event_time       DATETIME,
    history_event_time           DATETIME,
    kafka_event_time             DATETIME,
    fact_loaded_at               DATETIME,
    join_skew_ms                  BIGINT COMMENT 'Do lech source time giua hai CDC event',
    kafka_to_fact_latency_ms      BIGINT COMMENT 'Kafka record timestamp den luc Spark gui fact',
    source_to_fact_latency_ms     BIGINT COMMENT 'Postgres/Debezium source time den luc Spark gui fact',
    application_kafka_partition  INT,
    application_kafka_offset     BIGINT,
    history_kafka_partition      INT,
    history_kafka_offset         BIGINT
) PRIMARY KEY (ho_so_id, id)
DISTRIBUTED BY HASH(ho_so_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"
);
