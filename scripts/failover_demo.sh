#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export LSMDB_REPLICATION_FACTOR="${LSMDB_REPLICATION_FACTOR:-2}"

log() {
  printf '\n[failover-demo] %s\n' "$*"
}

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" == "1" ]]; then
    log "KEEP_CLUSTER=1 → оставляем контейнеры запущенными"
    return
  fi
  log "Останавливаем docker compose"
  docker compose down >/dev/null
}
trap cleanup EXIT

wait_health() {
  local addr=$1
  for attempt in {1..30}; do
    if curl -fs --max-time 2 "http://$addr/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

put_key() {
  local key=$1 value=$2
  log "PUT ${key}=${value}"
  if ! curl -sS --max-time 5 -X POST -d "key=${key}&value=${value}" "http://localhost:8081/api/put"; then
    log "PUT ${key} завершился ошибкой"
    exit 1
  fi
  printf '\n'
}

get_key() {
  local key=$1
  log "GET ${key}"
  if ! curl -sS --max-time 5 -i "http://localhost:8081/api/get?key=${key}"; then
    log "GET ${key} завершился ошибкой (см. вывод выше)"
    exit 1
  fi
  printf '\n'
}

log "Запускаем 3-нодовый кластер (репликация=${LSMDB_REPLICATION_FACTOR})"
docker compose up -d --build >/dev/null

for port in 8081 8082 8083; do
  log "Ждём health от localhost:${port}"
  if ! wait_health "localhost:${port}"; then
    log "Нода на порту ${port} не прошла health-check вовремя"
    exit 1
  fi
done

put_key "user:1" "Alice"
put_key "config:timeout" "30s"
get_key "user:1"

log "Свежие логи router (node1)"
docker compose logs node1 --tail=20

log "Гасим node3 для имитации аварии"
docker compose stop node3 >/dev/null
sleep 2

get_key "user:1"

log "Логи node1 после падения node3"
docker compose logs node1 --tail=60

log "При необходимости подними node3 обратно: docker compose start node3"
log "Скрипт завершён"

