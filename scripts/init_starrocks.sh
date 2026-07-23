#!/usr/bin/env sh
set -eu

MYSQL="mysql -h starrocks -P 9030 -u root"

until mysqladmin ping -h starrocks -P 9030 -u root --silent; do
  sleep 2
done

$MYSQL < /opt/project/transform/starrocks/ddl_realtime.sql
$MYSQL < /opt/project/transform/starrocks/ddl_streaming_fact.sql

echo "[+] StarRocks dimensions and physical streaming fact are ready."
