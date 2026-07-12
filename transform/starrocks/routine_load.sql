-- StarRocks: table model + Routine Load consume Kafka real-time
-- Nguoi phu trach: Quan
-- Chon model theo bang action items: Duplicate/Aggregate/Unique/Primary Key

-- Vi du: Primary Key model cho du lieu CDC (co update/delete)
CREATE TABLE IF NOT EXISTS rt_events (
    id BIGINT,
    event_time DATETIME,
    payload JSON
    -- TODO: sua cot theo schema topic db_cdc_events
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);

-- Routine Load: kiem soat offset tranh mat du lieu khi su co mang
CREATE ROUTINE LOAD rt_events_load ON rt_events
PROPERTIES (
    "format" = "json",
    "max_error_number" = "1000"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "db_cdc_events",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
