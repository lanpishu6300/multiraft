# 快速开始

**English：** [en/Getting-Started.md](../en/Getting-Started.md)

产品定位与热路径 / sync=1 技术方案见 **[技术亮点](../../spotlight/2026-07-hotpath-sync1.zh-CN.md)**。本文是最短上手清单。

## 前置

- Rust（建议与姊妹仓对齐的较新 stable；见 `rustc --version`）
- macOS / Linux
- Demo / Jepsen：可选 Java **17+**、[Leiningen](https://leiningen.org/)（`lein`）

```bash
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
```

## 克隆与测试

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
cargo test --workspace
```

## 多进程 Demo

```bash
./scripts/run_demo_cluster.sh
```

默认 `--base-port 21000`：

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

可选 Standby（`STANDBY=1` → 节点 4 Learner）：

```bash
STANDBY=1 ./scripts/run_demo_cluster.sh
curl -s http://127.0.0.1:21100/admin/groups/0/status
curl -s -X POST http://127.0.0.1:21100/admin/standby_snapshot/0
curl -s http://127.0.0.1:21103/admin/catalog/0
curl -s -X POST http://127.0.0.1:21100/admin/replicate_standby_snapshot/0
curl -s http://127.0.0.1:21103/groups/0/stale
```

## 验收 / Chaos / Jepsen

```bash
./scripts/acceptance.sh
SCENARIO=standby ./scripts/chaos.sh
STANDBY=1 ./scripts/run_jepsen.sh          # ~30s smoke
./scripts/test_all.sh            # CHAOS=1 含 chaos.sh
```

大重建前建议清理 `target/` 以释放磁盘。

更多：[README.zh-CN.md](../../../README.zh-CN.md) · [jepsen.md](../../jepsen.md)
