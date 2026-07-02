# AGENT_CONTEXT: UPGRADE (Couchbase Server Upgrade Subsystem)

## Read Order

| When you need… | Read |
|---|---|
| Test class hierarchy, upgrade loop flow, all params, helpers | [`agents/skills/test-flow-map/upgrade.md`](../../../agents/skills/test-flow-map/upgrade.md) |
| Test file inventory, maintenance rules, `cluster_features` guard table | [`pytests/upgrade/AGENTS.md`](../../../pytests/upgrade/AGENTS.md) |
| Chain keys, `upgrade_chains` dict, `features` dict, adding new chains | [`upgrade-lib.md`](./upgrade-lib.md) |
| Utility APIs (rebalance, backup, upgrade_utils) | [`couchbase_utils/AGENTS.md`](../../../couchbase_utils/AGENTS.md) |
| NS Server / cluster-manager background | [`docs/agent-context/ns-server/AGENTS.md`](../ns-server/AGENTS.md) |

---

## 1. Architecture

**Upgrade models:**
- `online_*` — rolling: one node at a time, cluster serves traffic, mixed-mode exists during transition
- `offline` — stop node → install → rebalance
- `full_offline` — stop all nodes → install all → single rebalance

**Mixed-mode cluster constraints** (while any node runs old version):
- `MAJORITY` SyncWrite may fail — requires all KV nodes at same version
- AES-256 bucket encryption key creation blocked
- 10K collections limit not unlocked until all nodes ≥ 8.1
- Some REST settings rejected (e.g., `eventLogsLimit` blocked until full 7.1+)

**Version chain mechanics:**
```
upgrade_chain param key → upgrade_chains[key] (lib/upgrade_lib/couchbase.py)
                          + [upgrade_version param]  ← final target appended
```
Cluster is upgraded through each intermediate version in order. See [upgrade-lib.md](./upgrade-lib.md) for all chain keys.

---

## 2. Feature Gating

`cluster_features` reflects the **currently installed** version, not the final target. Guards like `if "collections" in self.cluster_features` must not be removed based on target version alone — a chain starting at 6.5 has no collections during the first hop even if the target is 8.1.

| Version ≥ | Features added to `cluster_features` |
|---|---|
| 6.5 | `durability` |
| 7.0 | `collections` |
| 7.1 | `magma`, `system_event_logs` |
| 7.2 | `cdc` |
| 8.0 | `durability_impossible_fallback` |
| 8.1 | `file_based_rebalance`, `10K_collections`, `fusion`, `rate_limiting`, `jwt_auth`, `pitr` |

**Supported upgrade paths to 8.1**
- Direct: 8.0.x → 8.1, 7.6.x → 8.1, 7.2.9 → 8.1
- 3-hop: 7.1.x / 7.0.x / 6.6.x → 7.2.3 → 7.2.9 → 8.1
- 4-hop: 6.5.x → 6.6.5 → 7.2.3 → 7.2.9 → 8.1
- NOT supported direct to 8.1: 7.1.x single-hop, 7.2.0–7.2.8 single-hop

Source: `lib/upgrade_lib/couchbase.py` → `features` dict (cumulative — 7.2 cluster has all 6.5–7.2 features).

**7.6 boundary**: crossing from < 7.6 to ≥ 7.6 triggers `add_system_scope_to_all_buckets()` — adds `_system` scope (`_query`, `_mobile` collections) to all local bucket objects for downstream collection-count validation.

---

## 3. Key Files

| File | Purpose |
|---|---|
| `pytests/upgrade/upgrade_base.py` | `UpgradeBase`: initial install, cluster init, all 8 upgrade strategy methods, `spare_node` rotation |
| `pytests/upgrade/durability_upgrade.py` | `UpgradeTests`: primary test class — KV, SyncWrite, Magma, CDC, GSI, guardrails, storage migration |
| `pytests/upgrade/cbas_upgrade.py` | `UpgradeTests` (CBAS): dataset/index/replica survival; calls `cluster_cleanup()` in tearDown (base doesn't) |
| `pytests/upgrade/e2e_upgrade.py` | `E2EUpgrade`: multi-service (KV+2i+Eventing+N1QL+CBAS); also inherits `BaseSecondaryIndexingTests` |
| `pytests/upgrade/ce_base.py` / `ce_upgrade.py` | CE restriction tests; node-limit enforcement, CE→EE swap |
| `lib/upgrade_lib/couchbase.py` | `upgrade_chains` dict + `features` dict — single source for version registry |
| `couchbase_utils/upgrade_utils/upgrade_util.py` | `CbServerUpgrade`: fetch build URLs, install on nodes, `get_supported_features()` |
| `conf/upgrade/` | Test suite conf files — one per test class (durability, cbas, kv, offline, magma, system_event_logs) |

---

## 4. Non-Obvious Gotchas

Full debugging table: [test-flow-map/upgrade.md § Common Debugging Patterns](../../../agents/skills/test-flow-map/upgrade.md)

These three are the most surprising and not derivable from reading the code:

| Symptom | Root cause |
|---|---|
| Upgrade loop never exits | `fetch_node_to_upgrade()` matches version string exactly. Community builds append `"community"` — `"8.0.0-1000-community" != "8.0.0-1000"` causes infinite loop. |
| CBAS node left dirty after test | `cbas_upgrade.py::UpgradeTests.tearDown` calls `cluster_cleanup()`; `UpgradeBase.tearDown` does **not**. Never rely on base tearDown for CBAS node cleanup. |
| Storage migration silently skips | Only triggers when `migrate_storage_backend=True`. Default is `False`. No error if omitted — test passes but migration never ran. |
