#!/usr/bin/env bash
# Tao topic db_cdc_events — Kien chinh partition/replication/retention theo throughput
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic db_cdc_events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000   # 7 ngay
