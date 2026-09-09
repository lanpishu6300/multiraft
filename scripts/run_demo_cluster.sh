#!/usr/bin/env bash
# Launch a 3-process MultiRaft demo over gRPC (`--mode node`).
#
# Each OS process is one Raft node. Admin HTTP per node:
#   node N → http://127.0.0.1:(BASE_PORT + 100 + N - 1)
# Raft gRPC:
#   node N → 127.0.0.1:(BASE_PORT + N - 1)
#
# Optional Standby (Learner):
#   STANDBY=1 → also start node 4 as --role standby (StandbyOffload),
#   then curl the leader to add_standby for group 0.
#   STANDBY=2 or DAISY=1 → also start node 5 as daisy Standby that pulls
#   snapshots from node 4 admin (DAISY_UPSTREAM); node 5 is not add_learner'd.
#
# Compatible with macOS Bash 3.2.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_PORT="${BASE_PORT:-21000}"
GROUPS="${GROUPS:-10}"
NODES="${NODES:-3}"
DATA="${DATA_DIR:-$ROOT/.demo-data}"
export DATA_DIR="$DATA"
STANDBY="${STANDBY:-0}"
DAISY="${DAISY:-0}"
if [[ "$STANDBY" == "2" ]]; then
  DAISY=1
  STANDBY=1
fi
if [[ "$DAISY" == "1" && "$STANDBY" != "1" ]]; then
  STANDBY=1
fi
# gRPC peer table must include Standby (and daisy) ids or voters cannot replicate to them.
PEER_NODES="$NODES"
if [[ "$STANDBY" == "1" ]]; then
  PEER_NODES=$((NODES + 1))
fi
if [[ "$DAISY" == "1" ]]; then
  PEER_NODES=$((NODES + 2))
fi

# Jepsen / external clients: disable background propose_loop.
# Set via JEPSEN=1 or NO_AUTO_PROPOSE=1.
# Use a string (not an empty array) so `set -u` + Bash 3.2 does not trip on
# `"${arr[@]}"` when the array is empty.
NO_AUTO_PROPOSE_FLAG=""
if [[ "${JEPSEN:-0}" == "1" || "${NO_AUTO_PROPOSE:-0}" == "1" ]]; then
  NO_AUTO_PROPOSE_FLAG="--no-auto-propose"
fi

export PATH="${HOME}/.cargo/bin:${PATH}"

rm -rf "$DATA"
mkdir -p "$DATA"

cargo build -p multiraft-demo --manifest-path "$ROOT/Cargo.toml"

BIN="$ROOT/target/debug/multiraft-demo"

# When Standby is enabled, voters also use StandbyOffload (no hot FSM dump).
if [[ "$STANDBY" == "1" ]]; then
  export STANDBY=1
fi

id=1
while [[ "$id" -le "$NODES" ]]; do
  NODE_DATA="$DATA/node-$id"
  mkdir -p "$NODE_DATA"
  # shellcheck disable=SC2086
  "$BIN" \
    --mode node \
    --node-id "$id" \
    --nodes "$NODES" \
    --peer-nodes "$PEER_NODES" \
    --role voter \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$NODE_DATA" \
    $NO_AUTO_PROPOSE_FLAG \
    >"$DATA/node-$id.log" 2>&1 &
  echo $! >"$DATA/node-$id.pid"
  # Stagger binds so peers come up cleanly.
  sleep 0.4
  id=$((id + 1))
done

if [[ "$STANDBY" == "1" ]]; then
  STANDBY_ID=$((NODES + 1))
  NODE_DATA="$DATA/node-$STANDBY_ID"
  mkdir -p "$NODE_DATA"
  "$BIN" \
    --mode node \
    --node-id "$STANDBY_ID" \
    --nodes "$NODES" \
    --peer-nodes "$PEER_NODES" \
    --role standby \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$NODE_DATA" \
    --no-auto-propose \
    >"$DATA/node-$STANDBY_ID.log" 2>&1 &
  echo $! >"$DATA/node-$STANDBY_ID.pid"
  sleep 0.6
fi

echo "cluster started (${NODES} OS processes × ${GROUPS} groups, gRPC)"
echo "  data: $DATA"
id=1
while [[ "$id" -le "$NODES" ]]; do
  admin_port=$((BASE_PORT + 100 + id - 1))
  raft_port=$((BASE_PORT + id - 1))
  echo "  node ${id}: pid=$(cat "$DATA/node-$id.pid") raft=127.0.0.1:${raft_port} admin=http://127.0.0.1:${admin_port}/groups/0/value log=$DATA/node-$id.log"
  id=$((id + 1))
done

if [[ "$STANDBY" == "1" ]]; then
  STANDBY_ID=$((NODES + 1))
  admin_port=$((BASE_PORT + 100 + STANDBY_ID - 1))
  raft_port=$((BASE_PORT + STANDBY_ID - 1))
  echo "  standby ${STANDBY_ID}: pid=$(cat "$DATA/node-$STANDBY_ID.pid") raft=127.0.0.1:${raft_port} admin=http://127.0.0.1:${admin_port}/admin/snapshot_ads log=$DATA/node-$STANDBY_ID.log"

  # Wait briefly for leaders, then add_standby on group 0 via each voter admin until one succeeds.
  echo "  adding standby ${STANDBY_ID} to group 0..."
  added=0
  attempt=1
  while [[ "$attempt" -le 30 ]]; do
    id=1
    while [[ "$id" -le "$NODES" ]]; do
      admin_port=$((BASE_PORT + 100 + id - 1))
      if curl -sf -X POST "http://127.0.0.1:${admin_port}/admin/add_standby/0/${STANDBY_ID}" >/dev/null 2>&1; then
        echo "  add_standby ok via node ${id}"
        added=1
        break
      fi
      id=$((id + 1))
    done
    if [[ "$added" -eq 1 ]]; then
      break
    fi
    sleep 0.5
    attempt=$((attempt + 1))
  done
  if [[ "$added" -ne 1 ]]; then
    echo "  warning: add_standby did not succeed (cluster may still be electing); retry manually:"
    echo "    curl -X POST http://127.0.0.1:$((BASE_PORT + 100))/admin/add_standby/0/${STANDBY_ID}"
  fi
  echo "  optional trigger: curl -X POST http://127.0.0.1:$((BASE_PORT + 100))/admin/standby_snapshot/0"
fi

if [[ "$DAISY" == "1" ]]; then
  # Node 4 is the learner Standby; node 5 daisy-pulls snapshots only.
  UPSTREAM_ID=$((NODES + 1))
  DAISY_ID=$((NODES + 2))
  UPSTREAM_ADMIN=$((BASE_PORT + 100 + UPSTREAM_ID - 1))
  NODE_DATA="$DATA/node-$DAISY_ID"
  mkdir -p "$NODE_DATA"
  DAISY_UPSTREAM="http://127.0.0.1:${UPSTREAM_ADMIN}" \
  "$BIN" \
    --mode node \
    --node-id "$DAISY_ID" \
    --nodes "$NODES" \
    --peer-nodes "$PEER_NODES" \
    --role standby \
    --base-port "$BASE_PORT" \
    --groups "$GROUPS" \
    --data-dir "$NODE_DATA" \
    --daisy-upstream "http://127.0.0.1:${UPSTREAM_ADMIN}" \
    --no-auto-propose \
    >"$DATA/node-$DAISY_ID.log" 2>&1 &
  echo $! >"$DATA/node-$DAISY_ID.pid"
  sleep 0.4
  daisy_admin=$((BASE_PORT + 100 + DAISY_ID - 1))
  echo "  daisy standby ${DAISY_ID}: pid=$(cat "$DATA/node-$DAISY_ID.pid") admin=http://127.0.0.1:${daisy_admin}/snapshots/0/latest upstream=http://127.0.0.1:${UPSTREAM_ADMIN} log=$DATA/node-$DAISY_ID.log"
  echo "  (daisy node is NOT add_learner'd; it syncs snapshots from upstream Standby)"
fi

echo "  metrics: http://127.0.0.1:$((BASE_PORT + 100))/metrics/links"

# shellcheck source=scripts/cluster_lib.sh
. "$ROOT/scripts/cluster_lib.sh"
export DEMO_BIN="$BIN"
cluster_write_descriptor
echo "  descriptor: $DATA/cluster.json"
