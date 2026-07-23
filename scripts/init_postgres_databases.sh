#!/usr/bin/env sh
set -eu

# This script is executed by the official PostgreSQL image only when the
# source_db volume is initialized for the first time. Hive metadata lives in a
# separate database while sharing the same local PostgreSQL server.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
SELECT 'CREATE DATABASE metastore'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'metastore')\gexec
SQL
