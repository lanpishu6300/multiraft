# Jepsen: multiraft (local)

**中文：** [README.zh-CN.md](./README.zh-CN.md)

Local Jepsen suite against the multi-process `multiraft-demo` admin HTTP API.
No remote SSH VMs — nodes are OS processes on `127.0.0.1`.

## Prerequisites

- Java **17+** (Java 22 works with Jepsen 0.3.9 here; if deps fail, use Java 17)
- [Leiningen](https://leiningen.org/) (`lein` on `PATH`)
- Built demo: `cargo build -p multiraft-demo`

## Quick run

From the multiraft repo root:

```bash
./scripts/run_jepsen.sh
```

This builds the demo, starts a 3-node cluster with `NO_AUTO_PROPOSE=1` / `GROUPS=1`,
runs a ~30s counter test with a kill/restart nemesis, then tears the cluster down.

Optional Standby (Learner, not in the Jepsen `:nodes` set):

```bash
STANDBY=1 ./scripts/run_jepsen.sh
```

Clients only accept `"consistency":"linearizable"` reads; `"stale"` / `"local"` are
treated as failures. The nemesis kills/restarts voters `1..NODES` only.

`run_demo_cluster.sh` writes `$DATA_DIR/cluster.json` (admin URLs, ports, roles).
Jepsen client/nemesis and the bash harness scripts read it when present; they
fall back to `BASE_PORT` env + the canonical port formula otherwise.

## Manual

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
export BASE_PORT=23000 GROUPS=1 NODES=3
export DATA_DIR="$PWD/.jepsen-data"
export DEMO_BIN="$PWD/target/debug/multiraft-demo"
export MULTIRAFT_ROOT="$PWD"
export JEPSEN=1 NO_AUTO_PROPOSE=1

./scripts/run_demo_cluster.sh
# wait until admins respond, then:
cd jepsen/multiraft
lein run test -- --time-limit 30 --concurrency 6
```

## Workload

| Op | HTTP |
| --- | --- |
| `:add` | `POST /groups/0/inc` `{"delta":1}` |
| `:read` | `GET /groups/0/value` — only `"consistency":"linearizable"` |

Checker: `jepsen.checker/counter`.

Nemesis: local `kill -9` of `$DATA/node-$id.pid`, restart via `multiraft-demo --no-auto-propose`.

## Admin ports

With `BASE_PORT=23000`:

| Node | Admin |
| --- | --- |
| `"1"` | `http://127.0.0.1:23100` |
| `"2"` | `http://127.0.0.1:23101` |
| `"3"` | `http://127.0.0.1:23102` |
