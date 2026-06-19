---
name: upgrade-agent
description: >
  Couchbase Server upgrade test suite — validates online/offline upgrade strategies,
  mixed-mode cluster behavior, storage migration, guardrails, CDC, durability,
  indexing, and analytics across version chains.
model: inherit
---

# pytests/upgrade

Upgrade test suite. Validates all upgrade strategies (swap, incremental, failover, offline)
across multi-hop version chains, with concurrent data load, SyncWrite validation, and
post-upgrade feature verification.

↑ [pytests/](../basetestcase.py) → root [AGENTS.md](../../AGENTS.md)

---

## Files

| File | Class(es) | Purpose |
|---|---|---|
| `upgrade_base.py` | `UpgradeBase` | Shared base — installs initial version, builds upgrade chain, provides all upgrade strategy methods |
| `durability_upgrade.py` | `UpgradeTests` | Primary upgrade test class — KV durability, SyncWrites, Magma, collections, storage migration, guardrails, CDC, GSI |
| `cbas_upgrade.py` | `UpgradeTests` | Analytics (CBAS) upgrade — dataverse/dataset/index survival, replica, CBO samples |
| `kv_upgrade_tests.py` | `KVUpgradeTests` | KV-specific scenarios: sample buckets + failover-upgrade, tombstone + XDCR |
| `luks_upgrade.py` | `LuksUpgrade` | LUKS disk encryption upgrade — mixed-mode SyncWrite failure expectations |
| `mem_compression_upgrade.py` | `MemCompressionUpgradeTests` | Memory compression + Plasma GSI index upgrade |
| `system_event_logs.py` | `SystemEventLogs` | System event log API compatibility across mixed-mode and fully-upgraded cluster |
| `e2e_upgrade.py` | `E2EUpgrade` | End-to-end multi-service upgrade (KV + 2i + Eventing + N1QL + CBAS) with GSI abort validation |
| `ce_base.py` | `CEBaseTest` | CE restriction and upgrade base — version guards, online/offline swap helpers, CE node-limit assertion utilities |
| `ce_upgrade.py` | `CEUpgradeTests` | CE upgrade tests — node-limit enforcement, mixed-mode EE entry, CE→EE restriction removal, 6-node upgrade restriction trigger |

---

## Test Flow Map

Full class hierarchy, all upgrade strategies, `test_upgrade` step-by-step flow,
configuration parameters reference, upgrade chain configuration, `cluster_features`
lifecycle, and debugging patterns:

**→ [`agents/skills/test-flow-map/upgrade.md`](../../agents/skills/test-flow-map/upgrade.md)**

---

## Maintenance Rules

### When adding a new file or test method to this directory

1. **Check the test-flow-map for constraints before writing code.** Key invariants:

   **`cluster_features` guards — do not remove without checking the full chain.**
   Guards like `if "collections" in self.cluster_features` exist because `upgrade_chain`
   may start from a version that predates the feature. Removing them based on the target
   version alone is incorrect — check whether *every* entry in every conf file that uses
   this test is ≥ the feature's introduction version. See the WARNING block in
   [`upgrade.md` — Upgrade Chain Configuration](../../agents/skills/test-flow-map/upgrade.md).

   | Guard | Safe to assume True only when every chain entry is ≥ |
   |---|---|
   | `"collections" in cluster_features` | 7.0 |
   | `"magma" in cluster_features` | 7.1 |
   | `"system_event_logs" in cluster_features` | 7.1 |
   | `"cdc" in cluster_features` | 7.2 |
   | `"durability_impossible_fallback" in cluster_features` | 8.0 |
   | `"10K_collections" in cluster_features` | 8.1 |
   | `"file_based_rebalance" in cluster_features` | 8.1 |

   **Other invariants:**
   - `self.spare_node` is `cluster.servers[nodes_init]` at setUp and rotates after each `online_swap` — new upgrade methods must maintain this rotation.
   - `upgrade_function` dispatch dict maps `upgrade_type` → method — new upgrade strategies must be registered here before `__validate_upgrade_type()` is called in setUp.
   - `validate_encryption_operations(expected_to_fail=True)` must be called inside the per-node upgrade loop (mixed-mode), and `validate_encryption_operations(expected_to_fail=False)` after full upgrade only — do not invert this order.
   - `add_system_scope_to_all_buckets()` is triggered by version crossing 7.6 — new tests that validate bucket schema post-7.6 upgrade must account for the `_system` scope in collection counts.
   - `attempt_10k_collection_creation()` returns `True` only on 8.1+ — asserting `False` during upgrade and `True` after is the correct pattern.
   - CBAS `UpgradeTests` (`cbas_upgrade.py`) calls `cluster_cleanup()` in tearDown to force-clean nodes; KV `UpgradeBase.tearDown` does not — do not rely on base tearDown for CBAS node cleanup.

2. **Update [`agents/skills/test-flow-map/upgrade.md`](../../agents/skills/test-flow-map/upgrade.md)** to reflect:
   - New file → add a row to the Files table and a new section with class, setUp extras, and test method table.
   - New test method → add a row to the relevant class test table with purpose.
   - New helper method in `UpgradeBase` or `UpgradeTests` → add a row to the relevant helper methods table.
   - New configuration param → add a row to the appropriate Configuration Parameters table with default and description.
   - New upgrade strategy → add a row to the upgrade strategy dispatch table and document the mechanism.
   - New `cluster_features` guard → add a row to the guard safety table in both the Upgrade Chain Configuration section and the Debugging Patterns table.
   - New failure pattern or debugging gotcha → add a row to the Common Debugging Patterns table.

### When modifying an existing file

1. Re-read the corresponding section in the flow map to verify the change doesn't violate a documented invariant (especially `cluster_features` guards and `spare_node` rotation).
2. Update the flow map if the flow, params, guards, or helper methods change.
