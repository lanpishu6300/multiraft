# Premium recovery playbook (N3c)

**中文：** [2026-08-19-premium-recovery-playbook.zh-CN.md](./2026-08-19-premium-recovery-playbook.zh-CN.md)

**Branch:** `feature/standby-premium-parity`  
**Related:** [standby-parity](./2026-07-20-aeron-standby-parity-design.md) · [plugin-v1](./2026-08-19-plugin-v1-design.md)

---

## Prerequisites

- Demo cluster with `--premium-plugins --admin-token <tok>` (or empty token in lab).
- Admin base URL: `http://127.0.0.1:21100` (adjust per node).
- CLI: `cargo run -p multiraft-plugin-ops -- --base http://127.0.0.1:21100 --token <tok> …`

---

## 1. Recover voter to latest standby snapshot (P0)

```bash
# List standby ads
curl -s -H "Authorization: Bearer $TOK" \
  "$BASE/admin/best_snapshot_ad/0" | jq .

# Pull and install (leader or local voter)
curl -s -X POST -H "Authorization: Bearer $TOK" \
  "$BASE/admin/replicate_standby_snapshot/0" | jq .

# Expected: {"ok":true,...} or RecoverOutcome Installed / SkippedNotNewer
```

After restart, demo voters call `try_recover_from_standby_ads` automatically.

---

## 2. Export snapshot at position (N3b)

```bash
# Manifest (JSON)
curl -s -H "Authorization: Bearer $TOK" \
  "$BASE/admin/archive/0/export/120/3" | jq .

# Binary bytes
curl -s -H "Authorization: Bearer $TOK" \
  "$BASE/admin/archive/0/export/120/3/data" -o snap.bin

# CLI equivalent
cargo run -p multiraft-plugin-ops -- -u "$BASE" --token "$TOK" \
  archive-export --group 0 --index 120 --term 3 --out snap.bin
```

---

## 3. Promote standby (P1 / A9)

```bash
curl -s -X POST -H "Authorization: Bearer $TOK" \
  "$BASE/admin/promote_standby/0/4" | jq .

# Auto transition (lag policy) when demo started with:
#   --premium-plugins --transition-auto --transition-lag-threshold 100
```

---

## 4. Daisy-chain sync (P2)

```bash
curl -s -X POST -H "Authorization: Bearer $TOK" \
  "$BASE/admin/daisy_sync/0" | jq .
```

Standby B with `--daisy-upstream http://standby-a:port` runs background sync.

---

## 5. Stale read on standby (P3)

```bash
curl -s "$BASE/groups/0/stale" | jq .
# 403 when enable_stale_queries=false
```

---

## Expected outcomes

| Action | Success | Benign skip | Failure |
|--------|---------|-------------|---------|
| replicate | FSM value ≥ watermark | SkippedNotNewer | FetchFailed (bad sha256 / URL) |
| promote | node in voter set | already voter | not learner |
| export | sha256 + size match catalog | 404 | auth 401 |

Not Aeron Archive recording/replay — local catalog + HTTP only.
