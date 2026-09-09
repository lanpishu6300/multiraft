#!/usr/bin/env bash
# Restart one multiraft-demo process (voter or standby).
# Used by chaos.sh, acceptance.sh, and Jepsen nemesis.
#
# Usage: start_one_node.sh NODE_ID [voter|standby]
#
# Env (cluster.json overrides when present):
#   DATA_DIR or DATA, BASE_PORT, GROUPS, NODES, PEER_NODES, STANDBY,
#   JEPSEN, NO_AUTO_PROPOSE, DEMO_BIN, MULTIRAFT_ROOT, DAISY_UPSTREAM
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/cluster_lib.sh
. "$ROOT/scripts/cluster_lib.sh"

id="${1:?node id required}"
role_override="${2:-}"

DATA="$(cluster_data_dir)"
[[ -n "$DATA" ]] || {
  echo "start_one_node: DATA_DIR or DATA required" >&2
  exit 1
}

if cluster_has_descriptor; then
  BASE_PORT="$(cluster_json_field base_port)"
  GROUPS="$(cluster_json_field groups)"
  NODES="$(cluster_json_field voter_count)"
  PEER_NODES="$(cluster_json_field peer_nodes)"
  STANDBY="$(cluster_json_field standby)"
  DEMO_BIN="$(cluster_json_field demo_bin 2>/dev/null || true)"
else
  BASE_PORT="${BASE_PORT:-21000}"
  GROUPS="${GROUPS:-10}"
  NODES="${NODES:-3}"
  PEER_NODES="${PEER_NODES:-$NODES}"
  STANDBY="${STANDBY:-0}"
  DEMO_BIN="${DEMO_BIN:-}"
fi

if [[ -z "$DEMO_BIN" ]]; then
  DEMO_BIN="${MULTIRAFT_ROOT:-$ROOT}/target/debug/multiraft-demo"
fi

node_data="$DATA/node-$id"
mkdir -p "$node_data"

role_val="voter"
if [[ -n "$role_override" ]]; then
  role_val="$role_override"
elif [[ "$STANDBY" == "1" && "$id" -gt "$NODES" ]]; then
  role_val="standby"
fi

extra=""
if [[ "${JEPSEN:-0}" == "1" || "${NO_AUTO_PROPOSE:-0}" == "1" ]]; then
  extra="--no-auto-propose"
fi
if [[ "$role_val" == "standby" ]]; then
  extra="--no-auto-propose"
fi

args=(
  --mode node
  --node-id "$id"
  --nodes "$NODES"
  --peer-nodes "$PEER_NODES"
  --role "$role_val"
  --base-port "$BASE_PORT"
  --groups "$GROUPS"
  --data-dir "$node_data"
)

if [[ -n "$extra" ]]; then
  args+=(--no-auto-propose)
fi

if [[ -n "${DAISY_UPSTREAM:-}" ]]; then
  args+=(--daisy-upstream "$DAISY_UPSTREAM")
fi

# shellcheck disable=SC2086
"$DEMO_BIN" "${args[@]}" >"$DATA/node-$id.log" 2>&1 &
echo $! >"$DATA/node-$id.pid"
