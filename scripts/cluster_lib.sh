#!/usr/bin/env bash
# Shared cluster descriptor helpers. Source from other scripts; do not execute.
#
# Canonical admin port: base_port + 100 + node_id - 1
# Canonical raft port:  base_port + node_id - 1
#
# run_demo_cluster.sh writes $DATA_DIR/cluster.json; readers fall back to env
# + formulas when the file is missing (e.g. legacy data dirs).
#
# Compatible with macOS Bash 3.2.

cluster_data_dir() {
  echo "${DATA_DIR:-${DATA:-}}"
}

cluster_descriptor() {
  local data
  data="$(cluster_data_dir)"
  if [[ -z "$data" ]]; then
    return 1
  fi
  echo "${data}/cluster.json"
}

cluster_has_descriptor() {
  local desc
  desc="$(cluster_descriptor 2>/dev/null || true)"
  [[ -n "$desc" && -f "$desc" ]]
}

# shellcheck disable=SC2034
cluster_raft_port() {
  local base="$1" id="$2"
  echo $((base + id - 1))
}

cluster_admin_port() {
  local base="$1" id="$2"
  echo $((base + 100 + id - 1))
}

cluster_json_field() {
  local field="$1"
  local desc
  desc="$(cluster_descriptor)"
  python3 - "$desc" "$field" <<'PY'
import json, sys
path, field = sys.argv[1], sys.argv[2]
with open(path) as f:
    doc = json.load(f)
val = doc.get(field)
if val is None:
    sys.exit(1)
if isinstance(val, bool):
    print("1" if val else "0")
else:
    print(val)
PY
}

cluster_json_node_field() {
  local id="$1" field="$2"
  local desc
  desc="$(cluster_descriptor)"
  python3 - "$desc" "$id" "$field" <<'PY'
import json, sys
path, nid, field = sys.argv[1], int(sys.argv[2]), sys.argv[3]
with open(path) as f:
    doc = json.load(f)
for node in doc.get("nodes", []):
    if int(node.get("id", -1)) == nid:
        val = node.get(field)
        if val is None:
            sys.exit(1)
        print(val)
        sys.exit(0)
sys.exit(1)
PY
}

cluster_resolve_base_port() {
  if cluster_has_descriptor; then
    cluster_json_field base_port && return 0
  fi
  echo "${BASE_PORT:-21000}"
}

cluster_admin_url() {
  local id="$1" port base
  if cluster_has_descriptor; then
    if port="$(cluster_json_node_field "$id" admin_port 2>/dev/null)"; then
      echo "http://127.0.0.1:${port}"
      return 0
    fi
  fi
  base="$(cluster_resolve_base_port)"
  port="$(cluster_admin_port "$base" "$id")"
  echo "http://127.0.0.1:${port}"
}

cluster_write_descriptor() {
  # Expect caller to export: DATA_DIR or DATA, BASE_PORT, GROUPS, NODES,
  # PEER_NODES, STANDBY, DAISY, DEMO_BIN (optional), and node id list via
  # CLUSTER_NODE_IDS (space-separated) or derive from NODES/STANDBY/DAISY.
  local data desc base groups voters peers standby daisy demo_bin
  data="$(cluster_data_dir)"
  [[ -n "$data" ]] || return 1
  desc="${data}/cluster.json"
  base="${BASE_PORT:-21000}"
  groups="${GROUPS:-1}"
  voters="${NODES:-3}"
  peers="${PEER_NODES:-$voters}"
  standby="${STANDBY:-0}"
  daisy="${DAISY:-0}"
  demo_bin="${DEMO_BIN:-}"

  python3 - "$desc" "$base" "$groups" "$voters" "$peers" "$standby" "$daisy" "$data" "$demo_bin" <<'PY'
import json, os, sys

desc, base, groups, voters, peers, standby, daisy, data, demo_bin = sys.argv[1:10]
base = int(base)
groups = int(groups)
voters = int(voters)
peers = int(peers)
standby = standby == "1"
daisy = daisy == "1"

ids = []
for i in range(1, voters + 1):
    ids.append((i, "voter"))
if standby:
    ids.append((voters + 1, "standby"))
if daisy:
    ids.append((voters + 2, "standby"))

nodes = []
for nid, role in ids:
    raft_port = base + nid - 1
    admin_port = base + 100 + nid - 1
    node_data = os.path.join(data, f"node-{nid}")
    nodes.append({
        "id": nid,
        "role": role,
        "raft_port": raft_port,
        "admin_port": admin_port,
        "admin_url": f"http://127.0.0.1:{admin_port}",
        "data_dir": node_data,
        "pid_file": os.path.join(data, f"node-{nid}.pid"),
        "log_file": os.path.join(data, f"node-{nid}.log"),
    })

doc = {
    "version": 1,
    "base_port": base,
    "groups": groups,
    "voter_count": voters,
    "peer_nodes": peers,
    "standby": standby,
    "daisy": daisy,
    "data_dir": data,
    "demo_bin": demo_bin,
    "nodes": nodes,
}

with open(desc, "w") as f:
    json.dump(doc, f, indent=2)
    f.write("\n")
PY
}
