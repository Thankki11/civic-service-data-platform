#!/usr/bin/env sh
set -eu

MYSQL="mysql --connect-timeout=5 -h starrocks -P 9030 -u root"

until mysqladmin --connect-timeout=5 ping -h starrocks -P 9030 -u root --silent; do
  sleep 2
done

# FE co the nhan ket noi truoc khi BE dang ky dung luong. DDL tao PRIMARY KEY
# table trong khoang nay se loi "Cluster has no available capacity".
until $MYSQL -N -B -e "SHOW BACKENDS" \
  | awk -F '\t' '$9 == "true" { found=1 } END { exit !found }'; do
  sleep 2
done

$MYSQL < /opt/project/transform/starrocks/ddl_realtime.sql
$MYSQL < /opt/project/transform/starrocks/ddl_streaming_fact.sql

echo "[+] StarRocks dimensions and physical streaming fact are ready."
