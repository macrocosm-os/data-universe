#!/bin/bash

# Thông tin kết nối database
DB_NAME="subnet13"
DB_USER="sn13"
DB_PASSWORD="ahateamtop1sn13"
DB_HOST="localhost"
DB_PORT="5432"

# Gọi hàm PostgreSQL
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT create_data_entity_partitions();"

# Ghi log
echo "$(date): Executed partition maintenance" >> /tmp/partition_maintenance.log