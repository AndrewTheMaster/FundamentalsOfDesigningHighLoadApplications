#!/bin/bash
set -euo pipefail

CONTAINER_NAME="lsmdb-pg-bench"
DB_NAME="benchmark"
DB_USER="postgres"
DB_PASS="postgres"

cleanup() {
    echo "[pg-bench] Останавливаем PostgreSQL..."
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

echo "[pg-bench] PostgreSQL готов"
echo ""

# Run benchmarks
echo "[pg-bench] Запускаем бенчмарк trade_data..."
go run ./cmd/pgbench \
    -dataset "archive (1)/USDJPY 1M_CANDLESTICK DATA 2015-2025/USD_JPY_2015_07_2025_ASK.csv" \
    -name trade_data \
    -delim ","

echo ""
echo "[pg-bench] Запускаем бенчмарк tweets..."
go run ./cmd/pgbench \
    -dataset "archive (2)/tweets.csv" \
    -name tweets \
    -delim ";"

echo ""
echo "[pg-bench] Все тесты завершены!"
echo "[pg-bench] Результаты в bench-results/pg_*.json"

