#!/usr/bin/env bash
# Acceptance for multiraft multi-process gRPC demo (design §5.2).
#
# Starts 3 OS processes via run_demo_cluster.sh, kills a real leader PID,
# checks failover + durability, restarts the killed node, then runs unit tests.
#
# Compatible with macOS Bash 3.2 (no mapfile / namerefs).
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/cluster_lib.sh
. "$ROOT/scripts/cluster_lib.sh"
BASE_PORT="${BASE_PORT:-21000}"
GROUPS="${GROUPS:-10}"
NODES="${NODES:-3}"
DATA="${ACCEPTANCE_DATA:-$ROOT/.acceptance-data}"
WORKDIR=""

log() { printf '[acceptance] %s\n' "$*"; }
fail() { printf '[acceptance] FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
  stop_all_nodes
  if [[ -n "${WORKDIR}" && -d "${WORKDIR}" ]]; then
    rm -rf "${WORKDIR}"
  fi
}
trap cleanup EXIT

admin_url() {
  DATA_DIR="$DATA" cluster_admin_url "$1"
}

http_get() {
  curl -fsS --max-time 3 "$1"
}

stop_all_nodes() {
  local id pid
  if [[ ! -d "$DATA" ]]; then
    return 0
  fi
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    if [[ -f "$DATA/node-$id.pid" ]]; then
      pid="$(cat "$DATA/node-$id.pid" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
      fi
    fi
    id=$((id + 1))
  done
}

start_cluster() {
  local wipe="${1:-wipe}"
  if [[ "$wipe" == "wipe" ]]; then
    stop_all_nodes
    rm -rf "$DATA"
  fi
  mkdir -p "$DATA"
  DATA_DIR="$DATA" BASE_PORT="$BASE_PORT" GROUPS="$GROUPS" NODES="$NODES" \
    "$ROOT/scripts/run_demo_cluster.sh" >/dev/null
  log "cluster started data_dir=${DATA}"
}

start_one_node() {
  local id="$1"
  DATA_DIR="$DATA" BASE_PORT="$BASE_PORT" GROUPS="$GROUPS" NODES="$NODES" \
    MULTIRAFT_ROOT="$ROOT" \
    "$ROOT/scripts/start_one_node.sh" "$id"
  log "restarted node ${id} pid=$(cat "$DATA/node-$id.pid")"
}

# Echo first live admin base URL, or empty.
find_live_admin() {
  local id url
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    url="$(admin_url "$id")"
    if http_get "${url}/groups/0/value" >/dev/null 2>&1; then
      echo "$url"
      return 0
    fi
    id=$((id + 1))
  done
  return 1
}

wait_any_admin() {
  local deadline=$((SECONDS + 60))
  local url
  while (( SECONDS < deadline )); do
    if url="$(find_live_admin)"; then
      log "admin ready: ${url}"
      return 0
    fi
    sleep 0.3
  done
  fail "no admin HTTP ready within 60s; see ${DATA}/node-*.log"
}

# Probe all live admins for group g; print "group value leader" using max value
# and first non-empty leader seen.
fetch_group_agg() {
  local g="$1"
  local id url json best_value best_leader v leader
  best_value=""
  best_leader=""
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    url="$(admin_url "$id")"
    if json="$(http_get "${url}/groups/${g}/value" 2>/dev/null)"; then
      v="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["value"])' "$json")"
      leader="$(python3 -c '
import json,sys
d=json.loads(sys.argv[1])
print("" if d.get("leader") is None else str(d["leader"]))
' "$json")"
      if [[ -z "$best_value" ]] || [[ "$v" -gt "$best_value" ]]; then
        best_value="$v"
      fi
      if [[ -z "$best_leader" && -n "$leader" ]]; then
        best_leader="$leader"
      fi
    fi
    id=$((id + 1))
  done
  [[ -n "$best_value" ]] || return 1
  echo "${g} ${best_value} ${best_leader}"
}

# Write lines "group value leader" to $1 (one per group), aggregating live admins.
fetch_all_groups() {
  local out="$1"
  local g line
  : >"$out"
  g=0
  while [[ "$g" -lt "$GROUPS" ]]; do
    line="$(fetch_group_agg "$g")" || return 1
    echo "$line" >>"$out"
    g=$((g + 1))
  done
}

assert_groups_healthy() {
  local label="$1"
  local file="$2"
  local line g value leader
  local count=0
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    g=$(echo "$line" | awk '{print $1}')
    value=$(echo "$line" | awk '{print $2}')
    leader=$(echo "$line" | awk '{print $3}')
    [[ -n "$leader" ]] || fail "${label}: group ${g} has no leader"
    [[ "$value" =~ ^-?[0-9]+$ ]] || fail "${label}: group ${g} bad value '${value}'"
    count=$((count + 1))
  done <"$file"
  [[ "$count" -eq "$GROUPS" ]] || fail "${label}: expected ${GROUPS} groups, got ${count}"
  log "${label}: ${GROUPS} groups have leaders + values"
}

values_file() {
  awk '{print $2}'
}

# Pick a leader node id from any live admin's view of group 0.
pick_leader_node() {
  local id url json leader
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    url="$(admin_url "$id")"
    if json="$(http_get "${url}/groups/0/value" 2>/dev/null)"; then
      leader="$(python3 -c '
import json,sys
d=json.loads(sys.argv[1])
print("" if d.get("leader") is None else str(d["leader"]))
' "$json")"
      if [[ -n "$leader" ]]; then
        echo "$leader"
        return 0
      fi
    fi
    id=$((id + 1))
  done
  return 1
}

# --- prep ---
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/multiraft-acceptance.XXXXXX")"
F1="${WORKDIR}/g1.txt"
F2="${WORKDIR}/g2.txt"
F3="${WORKDIR}/g3.txt"
F4="${WORKDIR}/g4.txt"
F5="${WORKDIR}/g5.txt"

# --- build + start ---
log "building + starting multi-process demo"
export DATA_DIR="$DATA"
export BASE_PORT GROUPS NODES
start_cluster wipe
wait_any_admin

# --- 1: multi-group writes ---
log "check 1: multi-group writes (leaders + increasing values)"
fetch_all_groups "$F1"
assert_groups_healthy "check1-initial" "$F1"

sleep 2
fetch_all_groups "$F2"
assert_groups_healthy "check1-later" "$F2"

values_file <"$F1" >"${WORKDIR}/v_before.txt"
values_file <"$F2" >"${WORKDIR}/v_after.txt"
python3 -c '
import sys
b = [int(x) for x in open(sys.argv[1]).read().split()]
a = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(a) == len(b) and len(a) > 0
for i, (x, y) in enumerate(zip(b, a)):
    if y <= x:
        raise SystemExit("group %d did not increase (%s -> %s)" % (i, x, y))
' "${WORKDIR}/v_before.txt" "${WORKDIR}/v_after.txt" \
  || fail "check1: not all group values increased"
log "check 1 OK: all ${GROUPS} group values increased"

# --- 2: shared connections on a live node ---
log "check 2: unique_peer_links O(nodes)"
LIVE_ADMIN="$(find_live_admin)" || fail "check2: no live admin"
LINKS_JSON="$(http_get "${LIVE_ADMIN}/metrics/links")"
LINKS="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["unique_peer_links"])' "$LINKS_JSON")"
[[ "$LINKS" -lt 10 ]] || fail "check2: unique_peer_links=${LINKS} not < 10"
[[ "$LINKS" -le 6 ]] || fail "check2: unique_peer_links=${LINKS} not <= 6"
log "check 2 OK: unique_peer_links=${LINKS} on ${LIVE_ADMIN}"

# --- 3: kill real leader OS process ---
log "check 3: kill real leader process; wait for failover"
fetch_all_groups "$F1"
values_file <"$F1" >"${WORKDIR}/pre_failover_values.txt"
KILL_NODE="$(pick_leader_node)" || fail "check3: could not determine a leader node id"
KILL_PID="$(cat "$DATA/node-${KILL_NODE}.pid")"
[[ -n "$KILL_PID" ]] || fail "check3: missing pid file for node ${KILL_NODE}"
kill -0 "$KILL_PID" 2>/dev/null || fail "check3: pid ${KILL_PID} for node ${KILL_NODE} not running"
log "killing leader node ${KILL_NODE} pid=${KILL_PID}"
kill "$KILL_PID"
wait "$KILL_PID" 2>/dev/null || true

FAILOVER_DEADLINE=$((SECONDS + 60))
while true; do
  if fetch_all_groups "$F2" 2>/dev/null; then
    if python3 -c '
import sys
kill = sys.argv[1]
ok = True
for line in open(sys.argv[2]):
    parts = line.split()
    if len(parts) < 3 or not parts[2] or parts[2] == kill:
        ok = False
        break
sys.exit(0 if ok else 1)
' "$KILL_NODE" "$F2"; then
      break
    fi
  fi
  if (( SECONDS >= FAILOVER_DEADLINE )); then
    fail "check3: groups did not failover away from node ${KILL_NODE} within 60s"
  fi
  sleep 0.5
done
assert_groups_healthy "check3-after-failover" "$F2"
log "check 3 OK: leaders are not node ${KILL_NODE}"

# --- 4: committed durability ---
log "check 4: values >= pre-kill"
values_file <"$F2" >"${WORKDIR}/post_failover_values.txt"
python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit("group %d lost committed value (%d -> %d)" % (i, a, b))
' "${WORKDIR}/pre_failover_values.txt" "${WORKDIR}/post_failover_values.txt" \
  || fail "check4: committed values lost after failover"

sleep 2
fetch_all_groups "$F3"
values_file <"$F3" >"${WORKDIR}/post_grow_values.txt"
python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
mid = [int(x) for x in open(sys.argv[2]).read().split()]
late = [int(x) for x in open(sys.argv[3]).read().split()]
assert len(pre) == len(mid) == len(late)
grew = 0
for i, (a, b, c) in enumerate(zip(pre, mid, late)):
    if c < a:
        raise SystemExit("group %d value regressed (%d -> %d)" % (i, a, c))
    if c > b:
        grew += 1
if grew < 1:
    raise SystemExit("no group advanced after failover")
print(grew)
' "${WORKDIR}/pre_failover_values.txt" \
  "${WORKDIR}/post_failover_values.txt" \
  "${WORKDIR}/post_grow_values.txt" >"${WORKDIR}/grew.txt" \
  || fail "check4: values not retained / not advancing after failover"
GREW="$(cat "${WORKDIR}/grew.txt")"
log "check 4 OK: committed values retained; ${GREW}/${GROUPS} groups still advancing"

cp "${WORKDIR}/post_grow_values.txt" "${WORKDIR}/pre_restart_values.txt"

# --- 5: restart killed node with same data_dir ---
log "check 5: restart killed node ${KILL_NODE} with same data_dir"
start_one_node "$KILL_NODE"

CATCHUP_DEADLINE=$((SECONDS + 60))
while true; do
  if http_get "$(admin_url "$KILL_NODE")/groups/0/value" >/dev/null 2>&1 \
    && fetch_all_groups "$F4" 2>/dev/null; then
    values_file <"$F4" >"${WORKDIR}/restart_values.txt"
    if python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit(1)
sys.exit(0)
' "${WORKDIR}/pre_restart_values.txt" "${WORKDIR}/restart_values.txt"; then
      break
    fi
  fi
  if (( SECONDS >= CATCHUP_DEADLINE )); then
    fail "check5: restarted node did not catch up within 60s"
  fi
  sleep 0.5
done
assert_groups_healthy "check5-after-restart" "$F4"
python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit("group %d lost value across restart (%d -> %d)" % (i, a, b))
' "${WORKDIR}/pre_restart_values.txt" "${WORKDIR}/restart_values.txt" \
  || fail "check5: values not restored after restart"
log "check 5 OK: FSM values restored/caught-up after node restart"

# --- 6: unit tests ---
log "check 6: not_leader + grpc_cluster tests"
cargo test -p multiraft-net --test not_leader --manifest-path "$ROOT/Cargo.toml"
cargo test -p multiraft-net --test grpc_cluster --manifest-path "$ROOT/Cargo.toml"
log "check 6 OK"

log "ACCEPTANCE OK"
printf 'ACCEPTANCE OK\n'
