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
  local tries="${1:-30}"   # 30 * 0.5s = 15s
  local i u code loc leader_url
  for ((i=0; i<tries; i++)); do
    for u in "${NODES[@]}"; do
      # Сначала проверяем доступность узла (health check)
      health_code="$(curl -s -o /dev/null -w "%{http_code}" \
        --connect-timeout 1 --max-time 2 \
        "$u/health" 2>/dev/null || echo "000")"
      
      # Пропускаем недоступные узлы
      if [[ "$health_code" != "200" ]]; then
        continue
      fi
      
      loc="$(curl -s -o /dev/null -D - \
        --connect-timeout 1 --max-time 2 \
        -X PUT "$u/api/string" -d "key=__leader_probe__&value=1" \
        2>/dev/null \
        | awk -F': ' 'tolower($1)=="location"{print $2}' | tr -d '\r')"
      code="$(curl -s -o /dev/null -w "%{http_code}" \
        --connect-timeout 1 --max-time 2 \
        -X PUT "$u/api/string" -d "key=__leader_probe__&value=1" 2>/dev/null || echo "000")"

      if [[ "$code" == "200" ]]; then
        # Проверяем, что найденный лидер действительно доступен
        leader_url="$u"
        if curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 --max-time 2 "$leader_url/health" 2>/dev/null | grep -q "200"; then
          echo "$leader_url"
        return 0
        fi
      fi
      if [[ "$code" == "307" && -n "$loc" ]]; then
        if [[ "$loc" == http* ]]; then
          leader_url="${loc%/api/string}"
          # Проверяем, что лидер из Location действительно доступен
          if curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 --max-time 2 "$leader_url/health" 2>/dev/null | grep -q "200"; then
            echo "$leader_url"
        return 0
          fi
        fi
      fi
    done
    sleep 0.5
  done
  return 1
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
  code="$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout 2 --max-time 8 \
    -X PUT "$base/api/string" -d "key=$key&value=$value" || true)"
  if [[ "$code" != "200" ]]; then
    err "PUT expected 200 from leader, got HTTP $code at $base (leader may have changed)"
    return 1
  fi
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

  # follow redirect to leader if base is follower
  code="$(curl -s -o /dev/null -w "%{http_code}" \
    -L --connect-timeout 2 --max-time 8 \
    -X DELETE "$base/api?key=$enc" || true)"

  if [[ "$code" == "200" || "$code" == "204" ]]; then
    info "   DELETE succeeded via: DELETE /api ✅"
    return 0
  fi

  err "DELETE failed: expected 200/204, got HTTP $code"
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

# re-check leader right before CRUD (leader may change during startup)
leader="$(detect_leader || true)"
if [[ -n "$leader" ]]; then BASE="$leader"; fi
info "BASE re-checked before CRUD: $BASE"


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

info "   GET after delete (expected 404)"
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

info "   Detect new leader among remaining nodes"
new_leader="$(detect_leader 40 || true)"  # до 20s
if [[ -n "$new_leader" ]]; then
  info "New leader: $new_leader ($(node_name_from_url "$new_leader")) ✅"
else
  warn "New leader not detected (check logs)."
fi

info "   Read survival key from remaining nodes (should succeed if RF>=2)"
ok=0
for u in "${NODES[@]}"; do
  # skip stopped node in host mode correctly:
  if [[ "$leader_to_stop" == "node1" && "$u" == "http://localhost:8081" ]]; then continue; fi
  if [[ "$leader_to_stop" == "node2" && "$u" == "http://localhost:8082" ]]; then continue; fi
  if [[ "$leader_to_stop" == "node3" && "$u" == "http://localhost:8083" ]]; then continue; fi

  code="$(curl -s -o /dev/null -w "%{http_code}" \
    --connect-timeout 2 --max-time 4 \
    "$u/api/string?key=$k2" || true)"
  body="$(curl -s --connect-timeout 2 --max-time 4 \
    "$u/api/string?key=$k2" || true)"
  info "   GET $u -> HTTP $code  body: $body"
  if [[ "$code" == "200" ]]; then ok=1; fi
done

if [[ "$ok" != "1" ]]; then
  err "Survival key not readable from any remaining node (expected at least one 200)"
  exit 1
fi

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

# Quorum Loss Test
info "7) Quorum Loss Test: stop 2 of 3 nodes (no quorum, cluster should stop accepting writes)"

# Put a key before quorum loss
k3="before-quorum-loss"
v3="test-value"
put_key_no_redirect "$BASE" "$k3" "$v3"
info "   Put key before quorum loss: $k3=$v3"

# Determine which nodes to stop (stop 2 nodes, leave 1)
leader_before_quorum="$(detect_leader || true)"
if [[ -z "$leader_before_quorum" ]]; then
  warn "Could not detect leader before quorum test"
  leader_before_quorum="$BASE"
fi

# Stop 2 nodes (all except one)
nodes_to_stop=()
remaining_node=""
for u in "${NODES[@]}"; do
  node_name="$(node_name_from_url "$u")"
  if [[ "$node_name" != "$(node_name_from_url "$leader_before_quorum")" ]]; then
    nodes_to_stop+=("$node_name")
    if [[ ${#nodes_to_stop[@]} -eq 2 ]]; then
      break
    fi
  fi
done

# If we don't have 2 nodes to stop, stop any 2
if [[ ${#nodes_to_stop[@]} -lt 2 ]]; then
  nodes_to_stop=("node2" "node3")
fi

remaining_node=""
for u in "${NODES[@]}"; do
  node_name="$(node_name_from_url "$u")"
  skip=false
  for stop_node in "${nodes_to_stop[@]}"; do
    if [[ "$node_name" == "$stop_node" ]]; then
      skip=true
      break
    fi
  done
  if [[ "$skip" == false ]]; then
    remaining_node="$u"
    break
  fi
done

info "   Stopping 2 nodes (quorum loss): ${nodes_to_stop[*]}"
info "   Remaining node: $remaining_node (should not be able to accept writes)"

for stop_node in "${nodes_to_stop[@]}"; do
  docker compose stop "$stop_node"
done

# Wait a bit for cluster to detect quorum loss
sleep 3

# Try to write to remaining node - should fail or hang
info "   Attempting PUT to remaining node (should fail without quorum)"
quorum_test_key="quorum-test-key"
quorum_test_value="should-fail"
quorum_code=""
quorum_code="$(curl -s -o /dev/null -w "%{http_code}" \
  --connect-timeout 2 --max-time 5 \
  -X PUT "$remaining_node/api/string" -d "key=$quorum_test_key&value=$quorum_test_value" || echo "000")"

if [[ "$quorum_code" == "200" ]]; then
  warn "   PUT succeeded without quorum (unexpected - may indicate timing issue)"
elif [[ "$quorum_code" == "307" ]]; then
  warn "   Got redirect without quorum (leader may still be trying to replicate)"
elif [[ "$quorum_code" == "000" || "$quorum_code" == "500" || "$quorum_code" == "503" ]]; then
  info "   PUT failed/hung without quorum ✅ (expected behavior)"
else
  # Normalize code (remove leading zeros if any)
  normalized_code=$(echo "$quorum_code" | sed 's/^0*//')
  if [[ -z "$normalized_code" || "$normalized_code" == "000" ]]; then
    info "   PUT failed/hung without quorum ✅ (expected behavior - connection timeout/refused)"
  else
    info "   PUT returned HTTP $normalized_code (cluster may be in degraded state)"
  fi
fi

# Try to read - should still work if data was replicated
info "   Attempting GET from remaining node (reads may still work)"
read_code="$(curl -s -o /dev/null -w "%{http_code}" \
  --connect-timeout 2 --max-time 4 \
  "$remaining_node/api/string?key=$k3" || echo "000")"
if [[ "$read_code" == "200" ]]; then
  info "   GET succeeded (read-only access may work without quorum)"
else
  info "   GET returned HTTP $read_code"
fi

# Restore quorum
info "   Restoring quorum: starting stopped nodes"
for stop_node in "${nodes_to_stop[@]}"; do
  docker compose start "$stop_node"
done

# Wait for nodes to recover
info "   Waiting for nodes to become healthy"
for stop_node in "${nodes_to_stop[@]}"; do
  recover_url=""
  if [[ "$MODE" == "docker" ]]; then
    recover_url="http://${stop_node}:8080"
  else
    if [[ "$stop_node" == "node1" ]]; then recover_url="http://localhost:8081"; fi
    if [[ "$stop_node" == "node2" ]]; then recover_url="http://localhost:8082"; fi
    if [[ "$stop_node" == "node3" ]]; then recover_url="http://localhost:8083"; fi
  fi
  if [[ -n "$recover_url" ]]; then
    wait_health "$recover_url" || warn "   Node $stop_node health check timeout"
  fi
done

# Wait for leader election
sleep 2
final_leader="$(detect_leader 20 || true)"
if [[ -n "$final_leader" ]]; then
  info "   Leader re-elected after quorum restored: $final_leader ✅"
else
  warn "   Leader not detected after quorum restore"
fi

# Verify cluster is operational again
info "   Verifying cluster is operational after quorum restore"
test_key="after-quorum-restore"
test_value="cluster-working"
if put_key_no_redirect "$final_leader" "$test_key" "$test_value" 2>/dev/null; then
  info "   Cluster operational after quorum restore ✅"
else
  warn "   Cluster may still be recovering"
fi
echo

info "DONE ✅ Cluster showcase finished."
info "For logs use 'docker compose logs -f node1 node2 node3' in another terminal"
