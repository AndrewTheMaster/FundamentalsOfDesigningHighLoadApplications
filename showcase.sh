#!/usr/bin/env bash
set -euo pipefail

# ============================================
# showcase.sh — демонстрационный сценарий Raft-кластера
# ============================================

MODE="${1:-docker}"  # "docker" or "host"

if [[ "$MODE" == "docker" ]]; then
  # Внутренние адреса контейнеров (работает только из docker-сети)
  NODES=("http://node1:8080" "http://node2:8080" "http://node3:8080")
  BASE="${BASE:-http://node1:8080}"
else
  # Адреса с хоста через проброшенные порты
  NODES=("http://localhost:8081" "http://localhost:8082" "http://localhost:8083")
  BASE="${BASE:-http://localhost:8081}"
fi

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERR ]${NC} $*"; }

require() { command -v "$1" >/dev/null 2>&1 || { err "Missing dependency: $1"; exit 1; }; }
require docker
require curl
require python3

# -------------------------
# Helpers
# -------------------------

# Ждем, что /health начнет отвечать
wait_health() {
  local url="$1"
  for _ in {1..60}; do
    if curl -fsS --connect-timeout 1 --max-time 2 "$url/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# Лидер определяется так:
#  - лидер на PUT отвечает 200
#  - фолловер отвечает 307 (redirect to leader)
detect_leader() {
  local key="__leader_probe__" value="probe"
  for u in "${NODES[@]}"; do
    local code
    code="$(curl -s -o /dev/null -w "%{http_code}" \
      --connect-timeout 2 --max-time 4 \
      -X PUT "$u/api/string" -d "key=$key&value=$value" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$u"; return 0
    fi
  done
  echo ""; return 1
}

# PUT с редиректом
put_key() {
  local base="$1" key="$2" value="$3"
  curl -fsS -L --connect-timeout 2 --max-time 8 \
    -X PUT "$base/api/string" -d "key=$key&value=$value" >/dev/null
}

# PUT без редиректа
put_key_no_redirect() {
  local base="$1" key="$2" value="$3"
  curl -fsS --connect-timeout 2 --max-time 8 \
    -X PUT "$base/api/string" -d "key=$key&value=$value" >/dev/null
}

# Показываем, куда редиректит фолловер
probe_redirect_location() {
  local base="$1" key="$2" value="$3"
  curl -s -i --connect-timeout 2 --max-time 5 \
    -X PUT "$base/api/string" -d "key=$key&value=$value" \
    | tr -d '\r' | grep -i '^location:' || true
}

# GET key
get_key() {
  local base="$1" key="$2"
  local enc
  enc="$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$key'''))")"
  curl -fsS --connect-timeout 2 --max-time 6 "$base/api/string?key=$enc"
}

node_name_from_url() {
  local u="$1"
  if [[ "$u" == *"node1"* || "$u" == *"8081"* ]]; then echo "node1"; return; fi
  if [[ "$u" == *"node2"* || "$u" == *"8082"* ]]; then echo "node2"; return; fi
  if [[ "$u" == *"node3"* || "$u" == *"8083"* ]]; then echo "node3"; return; fi
  echo "node?"
}

# DELETE key
del_key() {
  local base="$1" key="$2"
  local enc
  enc="$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$key'''))")"

  local ok=""

  # helper: run and capture http_code
  _try() {
    local code="$1"; shift
    local method="$1"; shift
    local url="$1"; shift
    local data="${1:-}"
    local http
    if [[ -n "$data" ]]; then
      http="$(curl -s -o /dev/null -w "%{http_code}" \
        --connect-timeout 2 --max-time 6 \
        -X "$method" "$url" -d "$data" || true)"
    else
      http="$(curl -s -o /dev/null -w "%{http_code}" \
        --connect-timeout 2 --max-time 6 \
        -X "$method" "$url" || true)"
    fi
    if [[ "$http" == "200" || "$http" == "204" ]]; then
      ok="$code"
      return 0
    fi
    return 1
  }

  _try "DELETE /api/string" "DELETE" "$base/api/string?key=$enc" && true
  if [[ -z "$ok" ]]; then _try "DELETE /api" "DELETE" "$base/api?key=$enc" && true; fi
  if [[ -z "$ok" ]]; then _try "POST /api/delete" "POST" "$base/api/delete" "key=$key" && true; fi
  if [[ -z "$ok" ]]; then _try "POST /api/string/delete" "POST" "$base/api/string/delete" "key=$key" && true; fi

  if [[ -n "$ok" ]]; then
    info "   DELETE succeeded via: $ok ✅"
    return 0
  fi

  err "DELETE failed: no supported delete endpoint found (got 405/404/etc)."
  err "Tried: DELETE /api/string, DELETE /api, POST /api/delete, POST /api/string/delete"
  return 1
}

# =========================
# Scenario
# =========================

info "MODE=$MODE"
info "Nodes: ${NODES[*]}"
info "Base (initial): $BASE"
echo

info "1) Up cluster (docker compose up -d --build)"
docker compose up -d --build

info "2) Wait health on all nodes"
for u in "${NODES[@]}"; do
  info "   waiting: $u/health"
  if ! wait_health "$u"; then
    err "Health timeout for $u"
    docker compose ps
    docker compose logs --tail 200
    exit 1
  fi
done
info "All nodes healthy ✅"
echo

# Определяем лидера
info "3) Show leader (by PUT probe: leader=200, follower=307)"
leader="$(detect_leader || true)"
if [[ -z "$leader" ]]; then
  warn "Could not detect leader via PUT (unexpected)."
else
  info "Leader detected: $leader ($(node_name_from_url "$leader"))"
fi
echo

# Для стабильности считаем BASE=лидер
if [[ -n "$leader" ]]; then
  BASE="$leader"
  info "BASE updated to leader: $BASE"
  echo
fi

# CRUD
info "4) PUT/GET/DELETE basic flow"

k="demo-key"
v="hello-raft"

info "   PUT $k=$v via BASE=$BASE (no redirect, stable)"
put_key_no_redirect "$BASE" "$k" "$v"

info "   Redirect demo: PUT via a follower (should return 307 + Location)"
follower=""
for u in "${NODES[@]}"; do
  if [[ "$u" != "$BASE" ]]; then follower="$u"; break; fi
done
if [[ -n "$follower" ]]; then
  loc="$(probe_redirect_location "$follower" "__redirect_demo__" "1")"
  info "   follower=$follower  ${loc:-"(no Location captured)"}"
fi

info "   GET from every node (reads should work cluster-wide)"
for u in "${NODES[@]}"; do
  body="$(get_key "$u" "$k" || true)"
  info "   GET $u -> $body"
done

info "   DELETE via BASE (auto-detect endpoint)"
del_key "$BASE" "$k"

info "   GET after delete (expected: 404-like codes)"
for u in "${NODES[@]}"; do
  code="$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout 2 --max-time 4 \
    "$u/api/string?key=$k" || true)"
  info "   GET $u -> HTTP $code"
done
echo

# Failover
info "5) Failover: kill LEADER, show new leader, verify data still available"

k2="survive-key"
v2="i-should-survive"
put_key_no_redirect "$BASE" "$k2" "$v2"
info "   Put survival key: $k2=$v2"

leader_now="$(detect_leader || true)"
if [[ -z "$leader_now" ]]; then
  warn "Leader not detected right now; fallback stop node2."
  leader_to_stop="node2"
else
  leader_to_stop="$(node_name_from_url "$leader_now")"
fi

info "   Stopping leader container: $leader_to_stop"
docker compose stop "$leader_to_stop"

sleep 3

info "   Detect new leader among remaining nodes"
new_leader="$(detect_leader || true)"
if [[ -n "$new_leader" ]]; then
  info "New leader: $new_leader ($(node_name_from_url "$new_leader")) ✅"
else
  warn "New leader not detected (check logs)."
fi

info "   Read survival key from remaining nodes (should succeed if RF>=2)"
for u in "${NODES[@]}"; do
  if [[ "$u" == *"$leader_to_stop"* ]]; then continue; fi
  code="$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout 2 --max-time 4 \
    "$u/api/string?key=$k2" || true)"
  body="$(curl -s --connect-timeout 2 --max-time 4 \
    "$u/api/string?key=$k2" || true)"
  info "   GET $u -> HTTP $code  body: $body"
done
echo

# Recovery
info "6) Recovery: start stopped node, check it becomes healthy and can read data"
docker compose start "$leader_to_stop"

recover_url=""
if [[ "$MODE" == "docker" ]]; then
  recover_url="http://${leader_to_stop}:8080"
else
  if [[ "$leader_to_stop" == "node1" ]]; then recover_url="http://localhost:8081"; fi
  if [[ "$leader_to_stop" == "node2" ]]; then recover_url="http://localhost:8082"; fi
  if [[ "$leader_to_stop" == "node3" ]]; then recover_url="http://localhost:8083"; fi
fi

info "   waiting: $recover_url/health"
if ! wait_health "$recover_url"; then
  err "Recovered node did not become healthy"
  docker compose logs "$leader_to_stop" --tail 200
  exit 1
fi

info "   Read survival key from recovered node (should catch up)"
code="$(curl -s -o /dev/null -w "%{http_code}" \
  --connect-timeout 2 --max-time 4 \
  "$recover_url/api/string?key=$k2" || true)"
body="$(curl -s --connect-timeout 2 --max-time 4 \
  "$recover_url/api/string?key=$k2" || true)"
info "   GET recovered -> HTTP $code  body: $body"
echo

info "DONE ✅ Cluster showcase finished."
info "For logs use 'docker compose logs -f node1 node2 node3' in another terminal"
