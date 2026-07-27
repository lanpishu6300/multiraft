# Getting Started

**中文：** [zh/Getting-Started.md](../zh/Getting-Started.md)

For positioning and the hot path / sync=1 design summary, see **[highlights](../../spotlight/2026-07-hotpath-sync1.md)**. This page is the shortest bootstrap checklist.

## Prerequisites

- Rust (recent stable; check `rustc --version`)
- macOS / Linux
- Demo / Jepsen: optional Java **17+** and [Leiningen](https://leiningen.org/) (`lein`)

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
```

## Clone & test

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
cargo test --workspace
```

## Multi-process demo

```bash
./scripts/run_demo_cluster.sh
```

Default `--base-port 21000`:

| Node | Raft gRPC | Admin HTTP |
|------|-----------|------------|
| 1 | `127.0.0.1:21000` | `http://127.0.0.1:21100` |
| 2 | `127.0.0.1:21001` | `http://127.0.0.1:21101` |
| 3 | `127.0.0.1:21002` | `http://127.0.0.1:21102` |

```bash
curl -s http://127.0.0.1:21100/groups/0/value
curl -s -X POST http://127.0.0.1:21100/groups/0/inc \
  -H 'content-type: application/json' -d '{"delta":1}'
```

Optional Standby (`STANDBY=1` → node 4 learner):

```bash
STANDBY=1 ./scripts/run_demo_cluster.sh
curl -s http://127.0.0.1:21100/admin/groups/0/status
curl -s -X POST http://127.0.0.1:21100/admin/standby_snapshot/0
curl -s http://127.0.0.1:21103/admin/catalog/0
curl -s -X POST http://127.0.0.1:21100/admin/replicate_standby_snapshot/0
curl -s http://127.0.0.1:21103/groups/0/stale
```

## Acceptance / chaos / Jepsen

```bash
./scripts/acceptance.sh
SCENARIO=standby ./scripts/chaos.sh
STANDBY=1 ./scripts/run_jepsen.sh          # ~30s smoke
./scripts/test_all.sh            # CHAOS=1 includes chaos.sh
```

Clean `target/` before large rebuilds if disk is tight.

More: [README.md](../../../README.md) · [jepsen.md](../../jepsen.md)
