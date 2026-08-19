# Premium 恢复运维剧本（N3c）

**English：** [2026-08-19-premium-recovery-playbook.md](./2026-08-19-premium-recovery-playbook.md)

**分支：** `feature/standby-premium-parity`  
**关联：** [standby-parity](./2026-07-20-aeron-standby-parity-design.zh-CN.md) · [plugin-v1](./2026-08-19-plugin-v1-design.zh-CN.md)

---

## 前置条件

- Demo 集群：`--premium-plugins --admin-token <tok>`（实验环境可为空 token）。
- Admin 基址：`http://127.0.0.1:21100`（按节点调整）。
- CLI：`cargo run -p multiraft-plugin-ops -- --base http://127.0.0.1:21100 --token <tok> …`

---

## 1. Voter 恢复到最新 standby 快照（P0）

```bash
curl -s -H "Authorization: Bearer $TOK" \
  "$BASE/admin/best_snapshot_ad/0" | jq .

curl -s -X POST -H "Authorization: Bearer $TOK" \
  "$BASE/admin/replicate_standby_snapshot/0" | jq .
```

重启后 demo voter 会自动调用 `try_recover_from_standby_ads`。

---

## 2. 按位点导出快照（N3b）

```bash
curl -s -H "Authorization: Bearer $TOK" \
  "$BASE/admin/archive/0/export/120/3" | jq .

curl -s -H "Authorization: Bearer $TOK" \
  "$BASE/admin/archive/0/export/120/3/data" -o snap.bin
```

---

## 3. Promote standby（P1 / A9）

```bash
curl -s -X POST -H "Authorization: Bearer $TOK" \
  "$BASE/admin/promote_standby/0/4" | jq .
```

自动 transition：`--transition-auto --transition-lag-threshold 100`。

---

## 4. Daisy-chain（P2）· 5. Stale 读（P3）

见英文版对应章节；非 Aeron Archive 录制引擎，仅为本地 catalog + HTTP。
