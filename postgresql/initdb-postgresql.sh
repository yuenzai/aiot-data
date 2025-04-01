#!/bin/bash

create_schema() {
local username=$1
local password=$2
cat <<-EOF
\echo create schame: ${username}
CREATE USER ${username} WITH PASSWORD '${password}';
CREATE SCHEMA ${username} AUTHORIZATION ${username};
ALTER USER ${username} SET search_path TO ${username};
\echo schema created: ${username}
EOF
}

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE DATABASE aiot_data;
\c aiot_data
$(create_schema iceberg CJVixCszaS+7raa/5326YJDq3xrSBXHg)
$(create_schema prefect CJVixCszaS+7raa/5326YJDq3xrSBXHg)
SET search_path TO prefect;
CREATE EXTENSION pg_trgm;
EOSQL
