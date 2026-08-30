#!/usr/bin/env bash
# Multi-process gRPC chaos: kill nodes under several scenarios, wait for leaders,
# ensure values never go backwards, restart killed nodes, wait for catch-up.
#
# Compatible with macOS Bash 3.2 (no mapfile / namerefs / assoc arrays).
#
# Env:
#   SCENARIO     random|kill_leader|kill_follower|rolling|double_kill|standby|all
#                (default: random)
#   ROUNDS       (default 5) — per-scenario rounds; rolling = full node pass per round
#   BASE_PORT    (default 22000 — avoid clashing with acceptance)
#   GROUPS       (default 5)
#   NODES        (default 3)
#   STANDBY      0|1 — start Learner Standby (node NODES+1); forced on for SCENARIO=standby
#   CHAOS_DATA   (default $ROOT/.chaos-data)
set -euo pipefail

export PATH="${HOME}/.cargo/bin:${PATH}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/cluster_lib.sh
. "$ROOT/scripts/cluster_lib.sh"
BASE_PORT="${BASE_PORT:-22000}"
GROUPS="${GROUPS:-5}"
NODES="${NODES:-3}"
ROUNDS="${ROUNDS:-5}"
SCENARIO="${SCENARIO:-random}"
STANDBY="${STANDBY:-0}"
if [[ "$SCENARIO" == "standby" ]]; then
  STANDBY=1
fi
DATA="${CHAOS_DATA:-$ROOT/.chaos-data}"
WORKDIR=""
# Voter count for majority; Standby is NODES+1 when STANDBY=1.
MAX_NODE_ID="$NODES"
PEER_NODES="$NODES"
if [[ "$STANDBY" == "1" ]]; then
  MAX_NODE_ID=$((NODES + 1))
  PEER_NODES=$((NODES + 1))
fi

log() { printf '[chaos] %s\n' "$*"; }
fail() { printf '[chaos] FAIL: %s\n' "$*" >&2; exit 1; }

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
  local id pid max
  if [[ ! -d "$DATA" ]]; then
    return 0
  fi
  max=$((NODES + 2))
  id=1
  while [[ "$id" -le "$max" ]]; do
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
    STANDBY="$STANDBY" \
    "$ROOT/scripts/run_demo_cluster.sh" >/dev/null
  log "cluster started data_dir=${DATA} STANDBY=${STANDBY}"
}

start_one_node() {
  local id="$1"
  DATA_DIR="$DATA" BASE_PORT="$BASE_PORT" GROUPS="$GROUPS" NODES="$NODES" \
    PEER_NODES="${PEER_NODES:-$NODES}" STANDBY="$STANDBY" \
    JEPSEN="${JEPSEN:-0}" NO_AUTO_PROPOSE="${NO_AUTO_PROPOSE:-0}" \
    MULTIRAFT_ROOT="$ROOT" \
    "$ROOT/scripts/start_one_node.sh" "$id"
  log "restarted node ${id} pid=$(cat "$DATA/node-$id.pid")"
}

readd_standby() {
  local standby_id="$1"
  local id admin_port attempt=0
  while [[ "$attempt" -lt 30 ]]; do
    id=1
    while [[ "$id" -le "$NODES" ]]; do
      admin_port=$((BASE_PORT + 100 + id - 1))
      if curl -sf -X POST \
        "http://127.0.0.1:${admin_port}/admin/add_standby/0/${standby_id}" \
        >/dev/null 2>&1; then
        log "add_standby ${standby_id} ok via node ${id}"
        return 0
      fi
      id=$((id + 1))
    done
    attempt=$((attempt + 1))
    sleep 0.3
  done
  log "warning: add_standby ${standby_id} did not succeed"
}

# POST path on any live admin (voters + optional Standby) until one succeeds.
post_voter_admin() {
  local path="$1"
  local body="${2:-}"
  local id admin_port attempt=0 max_id
  max_id="$NODES"
  if [[ "$STANDBY" == "1" ]]; then
    max_id=$((NODES + 1))
  fi
  while [[ "$attempt" -lt 40 ]]; do
    id=1
    while [[ "$id" -le "$max_id" ]]; do
      admin_port=$((BASE_PORT + 100 + id - 1))
      if [[ -n "$body" ]]; then
        if curl -sf -X POST "http://127.0.0.1:${admin_port}${path}" \
          -H 'content-type: application/json' -d "$body" >/dev/null 2>&1; then
          echo "$id"
          return 0
        fi
      else
        if curl -sf -X POST "http://127.0.0.1:${admin_port}${path}" \
          >/dev/null 2>&1; then
          echo "$id"
          return 0
        fi
      fi
      id=$((id + 1))
    done
    attempt=$((attempt + 1))
    sleep 0.25
  done
  return 1
}

# Newest ad fetch_url from any live voter (empty if none).
best_ad_fetch_url() {
  local id admin_port json url
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    admin_port=$((BASE_PORT + 100 + id - 1))
    if json="$(curl -fsS --max-time 2 "http://127.0.0.1:${admin_port}/admin/best_snapshot_ad/0" 2>/dev/null)"; then
      url="$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(d.get("ad",{}).get("fetch_url",""))' "$json")"
      if [[ -n "$url" ]]; then
        echo "$url"
        return 0
      fi
    fi
    id=$((id + 1))
  done
  return 1
}

# Wait until a node's admin HTTP is accepting requests.
wait_admin_ready() {
  local id="$1"
  local label="${2:-admin-${id}}"
  local admin_port attempt=0
  admin_port=$((BASE_PORT + 100 + id - 1))
  while [[ "$attempt" -lt 60 ]]; do
    if curl -sf --max-time 1 "http://127.0.0.1:${admin_port}/admin/groups/0/status" \
      >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 0.25
  done
  fail "${label}: admin not ready on :${admin_port}"
}

# Pull Standby snapshot into a wiped voter; retries until install or hard fail.
replicate_from_standby() {
  local victim="$1"
  local label="${2:-replicate}"
  local admin_port fetch_url attempt=0 body resp
  admin_port=$((BASE_PORT + 100 + victim - 1))
  wait_admin_ready "$victim" "${label}-admin"
  fetch_url="$(best_ad_fetch_url)" || {
    log "warning: ${label}: no best_snapshot_ad; relying on Raft log catch-up"
    return 1
  }
  log "replicate wiped node ${victim} from ${fetch_url}"
  body="$(python3 -c 'import json,sys; print(json.dumps({"fetch_url":sys.argv[1]}))' "$fetch_url")"
  while [[ "$attempt" -lt 20 ]]; do
    resp="$(curl -sS -w '\n%{http_code}' -X POST \
      "http://127.0.0.1:${admin_port}/admin/replicate_standby_snapshot/0" \
      -H 'content-type: application/json' \
      -d "$body" 2>/dev/null || true)"
    if echo "$resp" | tail -n1 | grep -q '^200$'; then
      log "${label}: Installed via fetch_url"
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 0.4
  done
  log "warning: ${label}: replicate with fetch_url failed after retries (log catch-up may still work)"
  return 1
}

wait_standby_catalog() {
  local standby_id="$1"
  local label="$2"
  local admin_port deadline json
  admin_port=$((BASE_PORT + 100 + standby_id - 1))
  deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if json="$(curl -fsS --max-time 2 "http://127.0.0.1:${admin_port}/admin/catalog/0" 2>/dev/null)"; then
      if python3 -c 'import json,sys; d=json.loads(sys.argv[1]); assert d.get("ok") and d.get("last_index",0)>0' "$json"; then
        log "${label}: standby catalog ready"
        return 0
      fi
    fi
    sleep 0.4
  done
  fail "${label}: standby ${standby_id} catalog not ready"
}

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
  local deadline=$((SECONDS + 90))
  local url
  while (( SECONDS < deadline )); do
    if url="$(find_live_admin)"; then
      log "admin ready: ${url}"
      return 0
    fi
    sleep 0.3
  done
  fail "no admin HTTP ready within 90s; see ${DATA}/node-*.log"
}

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
}

values_file() {
  awk '{print $2}'
}

# List node ids whose pid file points at a live process (space-separated).
live_node_ids() {
  local id pid out=""
  id=1
  while [[ "$id" -le "$NODES" ]]; do
    if [[ -f "$DATA/node-$id.pid" ]]; then
      pid="$(cat "$DATA/node-$id.pid" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        if [[ -z "$out" ]]; then
          out="$id"
        else
          out="$out $id"
        fi
      fi
    fi
    id=$((id + 1))
  done
  echo "$out"
}

count_words() {
  # shellcheck disable=SC2206
  local arr=($1)
  echo "${#arr[@]}"
}

pick_random_live_node() {
  local live ids count idx
  live="$(live_node_ids)"
  [[ -n "$live" ]] || return 1
  # shellcheck disable=SC2206
  ids=($live)
  count=${#ids[@]}
  [[ "$count" -gt 0 ]] || return 1
  idx=$((RANDOM % count))
  echo "${ids[$idx]}"
}

# Leader of group 0 from any live admin JSON (empty if unknown).
group0_leader() {
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

# Pick a live node that is not the reported group-0 leader.
pick_live_follower() {
  local leader live id
  leader="$(group0_leader)" || return 1
  live="$(live_node_ids)"
  for id in $live; do
    if [[ "$id" != "$leader" ]]; then
      echo "$id"
      return 0
    fi
  done
  return 1
}

assert_values_not_backwards() {
  local pre="$1"
  local post="$2"
  local label="$3"
  python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post), "length mismatch"
for i, (a, b) in enumerate(zip(pre, post)):
    if b < a:
        raise SystemExit("group %d went backwards (%d -> %d)" % (i, a, b))
' "$pre" "$post" || fail "${label}: values went backwards"
}

wait_all_groups_healthy() {
  local label="$1"
  local out="$2"
  local deadline=$((SECONDS + 90))
  while true; do
    if fetch_all_groups "$out" 2>/dev/null; then
      if python3 -c '
import sys
ok = True
for line in open(sys.argv[1]):
    parts = line.split()
    if len(parts) < 3 or not parts[2]:
        ok = False
        break
sys.exit(0 if ok else 1)
' "$out"; then
        assert_groups_healthy "$label" "$out"
        return 0
      fi
    fi
    if (( SECONDS >= deadline )); then
      fail "${label}: leaders not restored within 90s"
    fi
    sleep 0.5
  done
}

wait_node_catchup() {
  local node_id="$1"
  local values_pre="$2"
  local after_file="$3"
  local values_post="$4"
  local label="$5"
  local deadline=$((SECONDS + 90))
  while true; do
    if http_get "$(admin_url "$node_id")/groups/0/value" >/dev/null 2>&1 \
      && fetch_all_groups "$after_file" 2>/dev/null; then
      values_file <"$after_file" >"$values_post"
      if python3 -c '
import sys
pre = [int(x) for x in open(sys.argv[1]).read().split()]
post = [int(x) for x in open(sys.argv[2]).read().split()]
assert len(pre) == len(post)
for a, b in zip(pre, post):
    if b < a:
        raise SystemExit(1)
sys.exit(0)
' "$values_pre" "$values_post"; then
        assert_groups_healthy "${label}-restart" "$after_file"
        assert_values_not_backwards "$values_pre" "$values_post" "${label}-restart"
        return 0
      fi
    fi
    if (( SECONDS >= deadline )); then
      fail "${label}: restarted node ${node_id} did not catch up within 90s"
    fi
    sleep 0.5
  done
}

# Kill one live node, wait healthy + non-decreasing, restart, catch-up.
kill_restart_one() {
  local kill_node="$1"
  local label="$2"
  local kill_pid

  fetch_all_groups "$SNAPSHOT" || fail "${label}: fetch before kill"
  assert_groups_healthy "${label}-pre" "$SNAPSHOT"
  values_file <"$SNAPSHOT" >"$VALUES_PRE"

  [[ -n "$kill_node" ]] || fail "${label}: empty kill target"
  kill_pid="$(cat "$DATA/node-${kill_node}.pid" 2>/dev/null || true)"
  [[ -n "$kill_pid" ]] || fail "${label}: missing pid for node ${kill_node}"
  kill -0 "$kill_pid" 2>/dev/null || fail "${label}: pid ${kill_pid} not running"

  # Never drop below majority (ceil(NODES/2)+1 live after kill).
  local live_before live_count majority
  live_before="$(live_node_ids)"
  live_count="$(count_words "$live_before")"
  majority=$(( NODES / 2 + 1 ))
  if [[ "$((live_count - 1))" -lt "$majority" ]]; then
    fail "${label}: refusing kill of ${kill_node}: would leave $((live_count - 1)) < majority ${majority}"
  fi

  log "kill -9 node ${kill_node} pid=${kill_pid}"
  kill -9 "$kill_pid"
  wait "$kill_pid" 2>/dev/null || true

  wait_all_groups_healthy "${label}-postkill" "$AFTER"
  values_file <"$AFTER" >"$VALUES_POST"
  assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "${label}-postkill"
  log "${label}: leaders restored; values non-decreasing"

  cp "$VALUES_POST" "$VALUES_PRE"
  start_one_node "$kill_node"
  wait_node_catchup "$kill_node" "$VALUES_PRE" "$AFTER" "$VALUES_POST" "$label"
  log "${label}: node ${kill_node} catch-up OK"
}

run_random_rounds() {
  local round=1 kill_node
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== random round ${round}/${ROUNDS} ==="
    kill_node="$(pick_random_live_node)" || fail "random round ${round}: no live nodes"
    kill_restart_one "$kill_node" "random-r${round}"
    round=$((round + 1))
  done
}

run_kill_leader_rounds() {
  local round=1 kill_node
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== kill_leader round ${round}/${ROUNDS} ==="
    kill_node="$(group0_leader)" || fail "kill_leader round ${round}: no group-0 leader"
    # Ensure target is live.
    if ! kill -0 "$(cat "$DATA/node-${kill_node}.pid" 2>/dev/null || echo)" 2>/dev/null; then
      fail "kill_leader round ${round}: leader ${kill_node} not live"
    fi
    kill_restart_one "$kill_node" "kill_leader-r${round}"
    round=$((round + 1))
  done
}

run_kill_follower_rounds() {
  local round=1 kill_node
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== kill_follower round ${round}/${ROUNDS} ==="
    kill_node="$(pick_live_follower)" || fail "kill_follower round ${round}: no live follower"
    kill_restart_one "$kill_node" "kill_follower-r${round}"
    round=$((round + 1))
  done
}

run_rolling_rounds() {
  local round=1 id
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== rolling round ${round}/${ROUNDS} (nodes 1..${NODES}) ==="
    id=1
    while [[ "$id" -le "$NODES" ]]; do
      kill_restart_one "$id" "rolling-r${round}-n${id}"
      id=$((id + 1))
    done
    round=$((round + 1))
  done
}

run_double_kill_rounds() {
  local round=1 first second
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== double_kill round ${round}/${ROUNDS} ==="
    first="$(pick_random_live_node)" || fail "double_kill round ${round}: no live nodes"
    kill_restart_one "$first" "double_kill-r${round}-a"
    # After restart, pick another (prefer different) live node.
    second="$(pick_random_live_node)" || fail "double_kill round ${round}: no live after restart"
    if [[ "$second" == "$first" ]]; then
      second="$(pick_live_follower 2>/dev/null || true)"
      if [[ -z "${second:-}" ]]; then
        second="$(pick_random_live_node)" || fail "double_kill round ${round}: no second target"
      fi
    fi
    kill_restart_one "$second" "double_kill-r${round}-b"
    round=$((round + 1))
  done
}

# Kill/restart Standby (does not affect voter majority). Then kill leader once.
run_standby_kill_restart_rounds() {
  local round=1 standby_id kill_pid
  standby_id=$((NODES + 1))
  while [[ "$round" -le "$ROUNDS" ]]; do
    log "=== standby kill/restart round ${round}/${ROUNDS} (node ${standby_id}) ==="
    fetch_all_groups "$SNAPSHOT" || fail "standby-r${round}: fetch before kill"
    assert_groups_healthy "standby-r${round}-pre" "$SNAPSHOT"
    values_file <"$SNAPSHOT" >"$VALUES_PRE"

    kill_pid="$(cat "$DATA/node-${standby_id}.pid" 2>/dev/null || true)"
    [[ -n "$kill_pid" ]] || fail "standby-r${round}: missing pid for standby ${standby_id}"
    log "kill -9 standby ${standby_id} pid=${kill_pid}"
    kill -9 "$kill_pid"
    wait "$kill_pid" 2>/dev/null || true

    wait_all_groups_healthy "standby-r${round}-postkill" "$AFTER"
    values_file <"$AFTER" >"$VALUES_POST"
    assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "standby-r${round}-postkill"

    cp "$VALUES_POST" "$VALUES_PRE"
    start_one_node "$standby_id"
    readd_standby "$standby_id"
    # Voters must remain healthy; standby catch-up is best-effort (admin may be stale).
    wait_all_groups_healthy "standby-r${round}-restart" "$AFTER"
    values_file <"$AFTER" >"$VALUES_POST"
    assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "standby-r${round}-restart"
    log "standby-r${round}: OK"
    round=$((round + 1))
  done
}

# Promote Standby → kill a follower → demote back (C43/C45 multi-process).
run_standby_promote_round() {
  local standby_id kill_node via
  standby_id=$((NODES + 1))
  log "=== standby promote/demote ==="
  fetch_all_groups "$SNAPSHOT" || fail "standby-promote: fetch pre"
  values_file <"$SNAPSHOT" >"$VALUES_PRE"

  via="$(post_voter_admin "/admin/promote_standby/0/${standby_id}")" \
    || fail "standby-promote: promote failed"
  log "promote_standby ${standby_id} via node ${via}"
  sleep 0.5

  kill_node="$(pick_live_follower)" || fail "standby-promote: no follower"
  # After promote, majority is among 4 voters; killing one original follower is safe.
  kill_restart_one "$kill_node" "standby-promote-kill"

  via="$(post_voter_admin "/admin/demote_standby/0/${standby_id}")" \
    || fail "standby-promote: demote failed"
  log "demote_standby ${standby_id} via node ${via}"
  sleep 0.3
  readd_standby "$standby_id"

  wait_all_groups_healthy "standby-promote-done" "$AFTER"
  values_file <"$AFTER" >"$VALUES_POST"
  assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "standby-promote"
  log "standby promote/demote OK"
}

# Trigger Standby snapshot, wipe a voter, recover via ads (C42 multi-process).
run_standby_recover_round() {
  local standby_id victim via kill_pid admin_port id port
  standby_id=$((NODES + 1))
  log "=== standby snapshot recover ==="
  fetch_all_groups "$SNAPSHOT" || fail "standby-recover: fetch pre"
  values_file <"$SNAPSHOT" >"$VALUES_PRE"

  # Ensure Standby has applied recent log before snapshot trigger.
  id=1
  while [[ "$id" -le 3 ]]; do
    port=$((BASE_PORT + 100))
    curl -sf -X POST "http://127.0.0.1:${port}/groups/0/inc" \
      -H 'content-type: application/json' -d "{\"delta\":1,\"idem\":null}" \
      >/dev/null 2>&1 || true
    id=$((id + 1))
    sleep 0.2
  done
  sleep 0.5

  via="$(post_voter_admin "/admin/standby_snapshot/0")" \
    || fail "standby-recover: trigger snapshot failed"
  log "standby_snapshot via node ${via}"
  wait_standby_catalog "$standby_id" "standby-recover"

  victim="$(pick_live_follower)" || fail "standby-recover: no follower"
  kill_pid="$(cat "$DATA/node-${victim}.pid" 2>/dev/null || true)"
  [[ -n "$kill_pid" ]] || fail "standby-recover: missing pid ${victim}"
  log "kill -9 wipe voter ${victim} pid=${kill_pid}"
  kill -9 "$kill_pid"
  wait "$kill_pid" 2>/dev/null || true
  rm -rf "$DATA/node-${victim}"
  mkdir -p "$DATA/node-${victim}"

  wait_all_groups_healthy "standby-recover-postkill" "$AFTER"
  values_file <"$AFTER" >"$VALUES_POST"
  assert_values_not_backwards "$VALUES_PRE" "$VALUES_POST" "standby-recover-postkill"
  cp "$VALUES_POST" "$VALUES_PRE"

  start_one_node "$victim"
  # Prefer explicit fetch_url so the wiped node does not need a local ad list.
  replicate_from_standby "$victim" "standby-recover" || true

  wait_node_catchup "$victim" "$VALUES_PRE" "$AFTER" "$VALUES_POST" "standby-recover"
  log "standby snapshot recover OK"
}

run_standby_rounds() {
  run_standby_kill_restart_rounds
  # Recover while Standby is still a clean learner (before promote churn).
  run_standby_recover_round
  log "=== standby: kill_leader with Standby present ==="
  run_kill_leader_rounds
  run_standby_promote_round
}

run_one_scenario() {
  local name="$1"
  log "SCENARIO=${name} ROUNDS=${ROUNDS} NODES=${NODES} GROUPS=${GROUPS} STANDBY=${STANDBY}"
  case "$name" in
    random) run_random_rounds ;;
    kill_leader) run_kill_leader_rounds ;;
    kill_follower) run_kill_follower_rounds ;;
    rolling) run_rolling_rounds ;;
    double_kill) run_double_kill_rounds ;;
    standby) run_standby_rounds ;;
    *) fail "unknown SCENARIO='${name}' (want random|kill_leader|kill_follower|rolling|double_kill|standby|all)" ;;
  esac
  log "SCENARIO=${name} OK"
}

# --- prep ---
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/multiraft-chaos.XXXXXX")"
SNAPSHOT="${WORKDIR}/snap.txt"
AFTER="${WORKDIR}/after.txt"
VALUES_PRE="${WORKDIR}/v_pre.txt"
VALUES_POST="${WORKDIR}/v_post.txt"

case "$SCENARIO" in
  random|kill_leader|kill_follower|rolling|double_kill|standby|all) ;;
  *) fail "unknown SCENARIO='${SCENARIO}'" ;;
esac

log "building + starting multi-process demo (SCENARIO=${SCENARIO} ROUNDS=${ROUNDS} STANDBY=${STANDBY})"
export DATA_DIR="$DATA"
export BASE_PORT GROUPS NODES STANDBY
start_cluster wipe
wait_any_admin

wait_all_groups_healthy "warmup" "$SNAPSHOT"
log "warmup OK"

if [[ "$SCENARIO" == "all" ]]; then
  for name in random kill_leader kill_follower rolling double_kill standby; do
    # standby scenario needs its own cluster with STANDBY=1
    if [[ "$name" == "standby" ]]; then
      STANDBY=1
      MAX_NODE_ID=$((NODES + 1))
      export STANDBY
      start_cluster wipe
      wait_any_admin
      wait_all_groups_healthy "standby-warmup" "$SNAPSHOT"
    fi
    run_one_scenario "$name"
  done
else
  run_one_scenario "$SCENARIO"
fi

log "CHAOS OK"
printf 'CHAOS OK\n'
