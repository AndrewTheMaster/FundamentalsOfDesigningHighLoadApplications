#!/bin/bash
set -euo pipefail

# PostgreSQL benchmark script for lab3 comparison
# Tests same datasets as LSM benchmark

CONTAINER_NAME="lsmdb-pg-bench"
DB_NAME="benchmark"
DB_USER="postgres"
DB_PASS="postgres"

cleanup() {
    echo "[pg-bench] Останавливаем PostgreSQL контейнер..."
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
}

trap cleanup EXIT

echo "[pg-bench] Запускаем PostgreSQL в Docker..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_PASSWORD="$DB_PASS" \
    -e POSTGRES_DB="$DB_NAME" \
    -p 5432:5432 \
    postgres:15-alpine >/dev/null

echo "[pg-bench] Ждём готовности PostgreSQL..."
for i in {1..30}; do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" >/dev/null 2>&1; then
    echo "[pg-bench] ERROR: PostgreSQL не запустился"
    exit 1
fi

benchmark_dataset() {
    local dataset_name=$1
    local csv_path=$2
    local delimiter=${3:-","}
    
    echo ""
    echo "[pg-bench] ========================================"
    echo "[pg-bench] Dataset: $dataset_name"
    echo "[pg-bench] CSV: $csv_path"
    echo "[pg-bench] ========================================"
    
    # Create table
    local table_name="bench_${dataset_name}"
    docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" <<EOF
DROP TABLE IF EXISTS ${table_name} CASCADE;
CREATE TABLE ${table_name} (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_${table_name}_key ON ${table_name}(key);
EOF
    
    # Import data with same transformation as LSM bench
    echo "[pg-bench] Импортируем данные..."
    local start_write=$(date +%s.%N)
    
    # Use Python script to transform and import
    docker exec -i "$CONTAINER_NAME" python3 <<PYTHON_SCRIPT
import csv
import json
import sys
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='${DB_NAME}',
    user='${DB_USER}',
    password='${DB_PASS}'
)
cur = conn.cursor()

delimiter = '${delimiter}'
csv_path = '/tmp/import.csv'

# Read CSV and insert
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=delimiter)
    row_index = 0
    batch = []
    
    for record in reader:
        sum_len = sum(len(field) for field in record)
        payload = json.dumps({
            'row_index': row_index,
            'fields': record,
            'sum_len': sum_len
        })
        key = f'${dataset_name}:{row_index:09d}'
        
        batch.append((key, payload))
        row_index += 1
        
        if len(batch) >= 10000:
            cur.executemany(
                "INSERT INTO ${table_name} (key, value) VALUES (%s, %s)",
                batch
            )
            conn.commit()
            batch = []
            if row_index % 250000 == 0:
                print(f"Processed {row_index} rows", file=sys.stderr)
    
    if batch:
        cur.executemany(
            "INSERT INTO ${table_name} (key, value) VALUES (%s, %s)",
            batch
        )
        conn.commit()

cur.close()
conn.close()
PYTHON_SCRIPT
    
    local end_write=$(date +%s.%N)
    local write_time=$(echo "$end_write - $start_write" | bc)
    
    # Get database size
    local db_size_mb=$(docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT pg_database_size('${DB_NAME}') / 1024.0 / 1024.0;
    " | tr -d ' ')
    
    # Full table scan (read benchmark)
    echo "[pg-bench] Выполняем полное чтение..."
    local start_read=$(date +%s.%N)
    docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT COUNT(*) FROM ${table_name};
        SELECT * FROM ${table_name} LIMIT 1;
    " >/dev/null
    local end_read=$(date +%s.%N)
    local read_time=$(echo "$end_read - $start_read" | bc)
    
    # Output results
    echo ""
    echo "[pg-bench] Результаты для $dataset_name:"
    echo "  Размер БД: ${db_size_mb} MB"
    echo "  Время записи: ${write_time} s"
    echo "  Время чтения: ${read_time} s"
    echo ""
    
    # Save to JSON
    cat > "bench-results/pg_${dataset_name}.json" <<EOF
{
  "dataset": "${dataset_name}",
  "db_size_mb": ${db_size_mb},
  "write_time_s": ${write_time},
  "read_time_s": ${read_time}
}
EOF
}

# Install Python and psycopg2 in container
echo "[pg-bench] Устанавливаем зависимости..."
docker exec "$CONTAINER_NAME" sh -c "
    apk add --no-cache python3 py3-pip >/dev/null 2>&1
    pip3 install psycopg2-binary >/dev/null 2>&1
"

# Copy CSV files to container
echo "[pg-bench] Копируем CSV файлы..."
docker cp "archive (1)/USDJPY 1M_CANDLESTICK DATA 2015-2025/USD_JPY_2015_07_2025_ASK.csv" \
    "${CONTAINER_NAME}:/tmp/trade_data.csv"
docker cp "archive (2)/tweets.csv" \
    "${CONTAINER_NAME}:/tmp/tweets.csv"

# Run benchmarks
benchmark_dataset "trade_data" "/tmp/trade_data.csv" ","
benchmark_dataset "tweets" "/tmp/tweets.csv" ";"

echo ""
echo "[pg-bench] Все тесты завершены!"
echo "[pg-bench] Результаты сохранены в bench-results/pg_*.json"

