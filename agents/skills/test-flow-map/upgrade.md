---
name: upgrade-test-flow-map
description: Code map and flow reference for pytests/upgrade/ — covers class hierarchy, upgrade strategies, test inventory, key parameters, and helper dependencies for debugging upgrade tests.
---

# Upgrade Test Package — Code Map

## Package Location
`pytests/upgrade/`

---

## Class Hierarchy

```
unittest.TestCase
  └── BaseTestCase  (pytests/basetestcase.py)
        └── OnPremBaseTest  (pytests/onprem_basetestcase.py)
              └── CollectionBase  (bucket_collections/collections_base.py)
                    └── UpgradeBase  (pytests/upgrade/upgrade_base.py)
                          ├── UpgradeTests         (durability_upgrade.py) ← primary/richest test class
                          ├── UpgradeTests         (cbas_upgrade.py)
                          ├── KVUpgradeTests       (kv_upgrade_tests.py)
                          ├── LuksUpgrade          (luks_upgrade.py)
                          ├── MemCompressionUpgradeTests (mem_compression_upgrade.py)
                          ├── SystemEventLogs      (system_event_logs.py)
                          └── E2EUpgrade           (e2e_upgrade.py)
                                └── (also inherits BaseSecondaryIndexingTests)
```

---

## File Reference

### `upgrade_base.py` — `UpgradeBase`

**Key setUp actions (in order):**
1. Read all input params (upgrade_type, update_nodes, enable_tls, storage, guardrails, etc.)
2. Call `_populate_upgrade_chain()` → builds `self.upgrade_chain` list from `upgrade_chains` dict + `upgrade_version` param
3. Install initial version on `nodes_init` nodes via `CbServerUpgrade.new_install_version_on_all_nodes()`
4. Initialize cluster (add nodes, rebalance)
5. Set `magmaMinMemoryQuota=256` for versions < 8.0
6. Disable auto-failover
7. Deploy buckets from spec file (`CollectionBase.deploy_buckets_from_spec_file`)
8. Load initial data (`CollectionBase.load_data_from_spec_file` with `initial_data_spec`)
9. Store `self.spare_node = cluster.servers[nodes_init]`

**Upgrade strategy dispatch table** (`self.upgrade_function[upgrade_type]`):

| `upgrade_type` param | Method | Mechanism |
|---|---|---|
| `online_swap` | `online_swap(node)` | Install on spare_node, swap-rebalance old↔spare |
| `online_incremental` | `online_incremental(node)` | Rebalance-out, install, rebalance-in same node |
| `online_rebalance_in_out` | `online_rebalance_in_out(node)` | Rebalance-in spare (new ver), rebalance-out old node |
| `online_rebalance_out_in` | `online_rebalance_out_in(node)` | Rebalance-out old node, install spare, rebalance-in spare |
| `failover_delta_recovery` | `failover_recovery(node, "delta")` | Graceful failover, install, delta recovery rebalance |
| `failover_full_recovery` | `failover_recovery(node, "full")` | Graceful failover, install, full recovery rebalance |
| `offline` | `offline(node)` | Stop couchbase, install, rebalance if unbalanced |
| `full_offline` | `full_offline(nodes_list)` | Stop all, install all, rebalance once |

**Node selection**: `fetch_node_to_upgrade(selection_criteria=None)`
- Iterates `cluster_util.get_nodes()`, picks first node whose version != `upgrade_version` and whose services match `update_nodes`
- `prefer_master=True` → tries master node first
- Returns `None` when all nodes are upgraded (signals loop termination)

**Key helper methods:**

| Method | Purpose |
|---|---|
| `load_during_rebalance(data_spec, async_load)` | Runs CRUD spec async or sync during rebalance |
| `perform_collection_ops_load(collections_spec)` | Runs collection CRUD spec (drop/recreate scopes/collections) |
| `run_queries_during_rebalance(upgrade_node)` | Runs N1QL queries in threads during rebalance (requires `include_indexing_query=True`) |
| `validate_encryption_operations(expected_to_fail)` | Creates an AES-256 key, attempts bucket assignment; asserts fail/success by upgrade state |
| `attempt_10k_collection_creation()` | Tests 10K collection limit (should fail pre-8.1, succeed post-8.1) |
| `insert_new_docs_sdk(num_docs, bucket)` | Direct SDK insert of new docs |
| `load_data_cbc_pillowfight(server, bucket, items, doc_size)` | SSH cbc-pillowfight load |
| `check_resident_ratio(cluster)` | Prometheus query for per-bucket RR across kv_nodes |
| `create_bucket_for_large_doc_upgrades()` | Creates bucket for large-doc tests (skips spec file) |
| `verify_and_configure_fbr()` | Checks `dataServiceFileBasedRebalanceEnabled` for 8.1+ |
| `enable_verify_tls(master_node, level)` | Enables TLS + verifies enforcement level |
| `add_system_scope_to_all_buckets()` | Adds `_system` scope/collections to bucket objects (needed post-7.6 upgrade) |
| `PrintStep(msg)` | Prints visual separator in logs |

---

### `durability_upgrade.py` — `UpgradeTests`
Primary upgrade test class. Covers KV durability, SyncWrites, Magma, collection ops, storage migration, guardrails, CDC, indexing, large docs.

**setUp extras:**
- Creates `DurabilityHelper`
- Initializes `verification_dict` with initial `ops_create` count
- Calls `cluster_util.update_cluster_nodes_service_list()`

**tearDown extras:**
- If `range_scan_collections > 0`: stops range scan task and validates results

**Test methods:**

| Test | Purpose |
|---|---|
| `test_upgrade` | **Primary upgrade test.** Iterates all nodes through `upgrade_chain`, runs SyncWrites + data load + collection ops between each node upgrade. Handles: encryption validation, 10K collection check, large docs, range scans, FBR, guardrail migration, storage migration, CDC, indexes, post-upgrade rebalance/failover. |
| `test_upgrade_to_morpheus` | Tests Morpheus (8.0) specific features: memcached bucket removal, durability-impossible fallback |
| `test_upgrade_from_ce_to_ee` | Community→Enterprise upgrade path |
| `test_downgrade` | Downgrade scenario |
| `test_upgrade_with_large_docs` | Docs > 1MB across upgrade |
| `test_upgrade_magma_default` | Magma storage engine upgrade (sets `magma_upgrade=True`) |
| `test_bucket_durability_upgrade` | Bucket-level durability settings across upgrade |
| `test_transaction_doc_isolation` | Transaction isolation during mixed-mode cluster |
| `test_cbcollect_info` | Triggers cbcollect across upgrade steps |

**Core `test_upgrade` flow:**
```
setUp() → load initial docs
  ↓
[optional] create_indexes_pre_upgrade()
  ↓
validate_encryption_operations(expected_to_fail=True)
  ↓
for each version in upgrade_chain:
    node = fetch_node_to_upgrade()
    while node is not None:
        validate_encryption_operations(expected_to_fail=True)
        assertFalse(attempt_10k_collection_creation())
        [optional] start async data load (sub_data_spec)
        upgrade_function[upgrade_type](node)        ← actual upgrade
        [if not online_swap] stop async data load
        run sync_write_spec CRUD (durability=MAJORITY)
        [optional] create indexes for new collections
        [optional] start range scans
        node = fetch_node_to_upgrade()
  ↓
cluster fully upgraded
  ↓
validate_encryption_operations(expected_to_fail=False)
assertTrue(attempt_10k_collection_creation())
[if guardrail test] check_and_set_guardrail_limits()
[if version cross 7.6] add_system_scope_to_all_buckets()
[if migrate_storage_backend] storage migration with chosen migration_procedure
[if magma_upgrade] enable_cdc_and_get_vb_details()
__wait_for_persistence_and_validate()
[if not test_failure] __play_with_collection()
[if magma_upgrade] verify_history_start_sequence_numbers(bucket)
[if guardrail test] post_upgrade_migration_guardrail_validation()
[if include_indexing_query] test_index_file_based_rebalance()
tasks_post_upgrade()   ← rebalance/failover post-upgrade
validate_test_failure()
```

**Key helper methods in `UpgradeTests`:**

| Method | Purpose |
|---|---|
| `tasks_post_upgrade(data_load)` | Runs rebalance/failover operations after full upgrade; controlled by `rebalance_op` param |
| `swap_rebalance_all_nodes()` | Swap-rebalances all nodes in cluster (for storage migration) |
| `swap_rebalance_all_nodes_iteratively()` | Same but one node at a time |
| `failover_recovery_all_nodes()` | Failover+recovery all nodes (for storage migration) |
| `migrate_node_random(node, method)` | Picks swap_rebalance or failover_recovery per node |
| `swap_rebalance_batch_migrate(nodes)` | Batch swap-rebalance for storage migration |
| `verify_storage_key_for_migration(bucket, storage)` | Validates storageBackend key in bucket props |
| `post_upgrade_migration_guardrail_validation()` | Validates guardrail enforcement after migration |
| `check_and_set_guardrail_limits()` | Sets RR and bucket-size guardrail thresholds |
| `enable_cdc_and_get_vb_details()` | Enables CDC on buckets, captures vbucket seqno details |
| `verify_history_start_sequence_numbers(bucket)` | Verifies CDC history seqnos are non-zero |
| `upsert_docs_to_increase_frag_val()` | Upserts all docs to trigger Magma compaction |
| `loading_large_documents(start_num)` | Loads 1000 docs > 1MB using cbc-pillowfight |
| `create_indexes_pre_upgrade()` | Creates GSI + primary indexes before upgrade begins |
| `create_secondary_indexes(partitioned, replica, suffix, field)` | Creates secondary/partitioned GSI indexes |
| `test_index_file_based_rebalance()` | Tests FBR (File-Based Rebalance) for index service |
| `validate_index_count_and_status()` | Checks all indexes are built and item count matches |
| `post_upgrade_validation()` | Validates cluster state after full upgrade |
| `__wait_for_persistence_and_validate()` | Waits for persistence queue to drain + validates doc counts |
| `__play_with_collection()` | SDK-based scope/collection CRUD + spec-driven collection operations |
| `create_new_bucket_post_upgrade_and_load_data()` | Creates a new bucket post upgrade and loads data into it |
| `edit_history_for_collections_existing_bucket()` | Toggles history setting on collections (CDC test) |

---

### `cbas_upgrade.py` — `UpgradeTests`

**setUp extras:**
- Creates `CbasUtil` + `CBASRebalanceUtil`
- Optionally enables N2N encryption
- Discovers CBAS CC (cluster coordinator) node
- Calls `pre_upgrade_setup()` → creates CBAS infra (dataverses, datasets, indexes) via spec

**Test methods:**

| Test | Purpose |
|---|---|
| `test_upgrade` (inherited pattern) | Upgrades nodes one by one; validates CBAS infra pre/post via `post_upgrade_validation()` |

**Key CBAS-specific methods:**

| Method | Purpose |
|---|---|
| `pre_upgrade_setup()` | Creates CBAS dataverses/datasets/indexes from spec (version-aware: ≤6.5 max 8 datasets) |
| `cbas_setup(update_spec, connect_local_link)` | Creates CBAS infra from spec + connects Local links + waits for ingestion |
| `post_upgrade_validation()` | After full upgrade: rebalance to activate CBAS, validate pre-upgrade infra survives, load new docs, create post-upgrade CBAS infra, test CBO samples (7.2+), replica validation (7.1+) |
| `cluster_cleanup()` | Stops rebalance, deletes all buckets, cleans up cluster |

---

### `kv_upgrade_tests.py` — `KVUpgradeTests`

**setUp extras:**
- `nodes_upgrade`, `graceful`, `recovery_type` params

**Test methods:**

| Test | Purpose |
|---|---|
| `test_multiple_sample_bucket_failover_upgrade` | Loads Travel/Beer/Gamesim sample buckets, then failover-upgrades nodes (MB-53493) |
| `test_db_dump_with_empty_body_and_empty_xattr` | Creates tombstone with sys-xattr, evicts it, then upgrades; tests XDCR replication (MB-51373) |

---

### `luks_upgrade.py` — `LuksUpgrade`
LUKS disk encryption upgrade scenario.

**setUp extras:**
- Pre-installs target version on LUKS spare nodes

**Test methods:**

| Test | Purpose |
|---|---|
| `test_upgrade_to_luks_cluster` | Iterates nodes, calls upgrade_function, validates SyncWrite behavior in mixed-mode (expects SyncWrite failure until all nodes upgraded) |

---

### `mem_compression_upgrade.py` — `MemCompressionUpgradeTests`
Memory compression + GSI index upgrade. Also initializes CBAS util.

**Test methods:**

| Test | Purpose |
|---|---|
| `test_upgrade` | Creates indexes, verifies Plasma stats, upgrades cluster, validates index stats and memory compression metrics post-upgrade |

---

### `system_event_logs.py` — `SystemEventLogs`
System event log API upgrade compatibility test.

**Test methods:**

| Test | Purpose |
|---|---|
| `test_upgrade` | Upgrades cluster node by node; in mixed mode verifies `eventLogsLimit` setting is rejected; after full upgrade validates collection create/drop events appear in system event log |

---

### `e2e_upgrade.py` — `E2EUpgrade`
End-to-end upgrade with multi-service cluster (KV + Views + 2i + Eventing + N1QL + CBAS).

**Inherits from:** `UpgradeBase` + `BaseSecondaryIndexingTests`

**Test methods:**

| Test | Purpose |
|---|---|
| `test_index_with_aborts` | Creates GSI indexes, loads docs with SyncWrite aborts, verifies aborted docs not indexed; then loads successful SyncWrites and validates indexing |
| `test_upgrade` | E2E multi-service upgrade (defined but body shown as docstring only — full service validation) |

---

## Configuration Parameters Reference

### Core Upgrade Params

| Param | Default | Description |
|---|---|---|
| `upgrade_chain` | `"7.2.3"` | Key into `upgrade_chains` dict in `lib/upgrade_lib/couchbase.py` |
| `upgrade_version` | `"8.0.0-1000"` | Final target version (appended to chain) |
| `upgrade_type` | `"online_swap"` | One of: `online_swap`, `online_incremental`, `online_rebalance_in_out`, `online_rebalance_out_in`, `failover_delta_recovery`, `failover_full_recovery`, `offline`, `full_offline` |
| `update_nodes` | `"kv"` | Semicolon-separated service names to upgrade (e.g. `"kv;index"`) |
| `prefer_master` | `False` | Upgrade master node first |
| `is_downgrade` | `False` | Downgrade mode |
| `community_upgrade` | `False` | CE→EE upgrade |

### Data Load Params

| Param | Default | Description |
|---|---|---|
| `upgrade_with_data_load` | `True` | Run CRUD ops during upgrade |
| `bucket_spec` | `"single_bucket.default"` | Bucket spec package name |
| `initial_data_spec` | `"initial_load"` | Spec for pre-upgrade data load |
| `sub_data_spec` | `"subsequent_load_magma"` | Spec for async load during upgrade |
| `upsert_data_spec` | `"upsert_load"` | Spec for upsert operations |
| `sync_write_spec` | `"sync_write_magma"` | Spec for SyncWrite validation between upgrades |
| `collection_spec` | `"collections_magma"` | Spec for collection CRUD ops |
| `ops_rate` | `30000` | Target ops/sec |
| `load_large_docs` | `False` | Load docs > 1MB |
| `large_doc_upgrade` | `False` | Use custom bucket for large-doc test |
| `alternate_load` | `False` | Add 10% reads + 10% updates to sub data spec |

### Storage / Migration Params

| Param | Default | Description |
|---|---|---|
| `migrate_storage_backend` | `False` | Trigger storage backend migration after upgrade |
| `preferred_storage_mode` | `"magma"` | Target storage after migration |
| `migration_procedure` | `"swap_rebalance"` | One of: `swap_rebalance`, `swap_rebalance_all`, `failover_recovery`, `randomize_method`, `swap_rebalance_batch` |
| `magma_upgrade` | `False` | Treat as Magma-to-Magma upgrade (enables CDC tests) |
| `test_storage_upgrade` | `False` | Storage engine upgrade test |

### Guardrail Params

| Param | Default | Description |
|---|---|---|
| `test_guardrail_migration` | `False` | Test guardrail enforcement during storage migration |
| `test_guardrail_upgrade` | `False` | Test guardrail enforcement during upgrade |
| `guardrail_type` | `"resident_ratio"` | One of: `resident_ratio`, `bucket_guardrail`, `collection_guardrail` |
| `breach_guardrail` | `False` | Whether test expects guardrail to be breached |

### TLS Params

| Param | Default | Description |
|---|---|---|
| `enable_tls` | `False` | Enable TLS during upgrade |
| `tls_level` | `"all"` | TLS enforcement level: `"all"` or `"strict"` |

### GSI / Indexing Params

| Param | Default | Description |
|---|---|---|
| `include_indexing_query` | `False` | Create GSI indexes and run N1QL queries during upgrade |
| `redistribute_indexes` | `True` | Enable index redistribution post-upgrade |
| `enable_shard_affinity` | `True` | Enable shard affinity for indexes |
| `create_partitioned_indexes` | `True` | Create partitioned indexes |
| `index_quota_mem` | `512` | Index service memory quota (MB) |
| `kv_quota_mem` | `6000` | KV service memory quota (MB) |

### Range Scan Params

| Param | Default | Description |
|---|---|---|
| `range_scan_collections` | `0` | Number of collections for range scan |
| `range_scan_timeout` | `None` | Range scan operation timeout |
| `range_scan_runs_per_collection` | `1` | Range scan iterations per collection |

### Rebalance / Retry Params

| Param | Default | Description |
|---|---|---|
| `auto_retry_rebalance` | `False` | Enable rebalance auto-retry |
| `rebalance_failure_condition` | `None` | Inject rebalance failure condition (e.g. `"backfill_done"`) |
| `delay_time` | `60000` | Delay for rebalance failure injection (ms) |
| `rebalance_op` | `"all"` | Post-upgrade rebalance ops (controls `tasks_post_upgrade`) |
| `retry_get_process_num` | `25` | Retry count for process-num check during swap rebalance |

---

## Upgrade Chain Configuration

**Source:** `lib/upgrade_lib/couchbase.py`

```python
upgrade_chains = {
    "7.2.3": ["7.2.3"],           # single-hop
    "6.6.5_7.2.3": ["6.6.5", "7.2.3"],  # multi-hop
    ...
}
```

**Usage in `_populate_upgrade_chain()`:**
```python
chain_to_test = self.input.param("upgrade_chain", "7.2.3")
upgrade_version = self.input.param("upgrade_version", "8.0.0-1000")
self.upgrade_chain = upgrade_chains[chain_to_test] + [upgrade_version]
# e.g. "7.2.3" + "8.0.0-1000" → ["7.2.3", "8.0.0-1000"]
```

**Feature gating** (`get_supported_features(version)`):
```
6.5 → durability
7.0 → collections
7.1 → magma, system_event_logs
7.2 → cdc
8.0 → durability_impossible_fallback
8.1 → file_based_rebalance, 10K_collections, fusion, rate_limiting, jwt_auth, pitr
```

**`self.cluster_features` lifecycle:**
- Set in `setUp()` from the **initial** chain version: `get_supported_features(self.cluster.version)`
- Updated at the end of each version loop iteration: `self.cluster_features = self.upgrade_helper.get_supported_features(self.cluster.version)`
- During the upgrade loop, it reflects the **currently installed** cluster version, not the final target

**WARNING — do not remove `if "<feature>" in self.cluster_features` guards based on target version alone.**

These guards exist because `upgrade_chain` may start from a version that does not support the feature, even when the final target does. Example:

```
upgrade_chain = ["6.5.0", "7.2.3", "8.0.0"]   ← base version is 6.5
upgrade_version = "8.1.0"
full chain = ["6.5.0", "7.2.3", "8.0.0", "8.1.0"]
```

While upgrading from 6.5→7.2, `cluster_features` does **not** include `"collections"`. Removing `if "collections" in self.cluster_features` would cause collection-scoped operations to run against a pre-7.0 cluster and fail. The minimum supported base for a given target version does **not** determine what features are safe to assume — only the actual `cluster_features` at each step does.

| Guard | Safe to remove only when |
|---|---|
| `if "collections" in self.cluster_features` | Every chain entry in all conf files is ≥ 7.0 |
| `if "magma" in self.cluster_features` | Every chain entry is ≥ 7.1 |
| `if "cdc" in self.cluster_features` | Every chain entry is ≥ 7.2 |
| `if "system_event_logs" in self.cluster_features` | Every chain entry is ≥ 7.1 |
| `if "10K_collections" in self.cluster_features` | Every chain entry is ≥ 8.1 |
| `if "file_based_rebalance" in self.cluster_features` | Every chain entry is ≥ 8.1 |

---

## Key External Dependencies

| Import | Source | Role |
|---|---|---|
| `CbServerUpgrade` | `couchbase_utils/upgrade_utils/upgrade_util.py` | Install builds, fetch build URLs, offline upgrade |
| `upgrade_chains` | `lib/upgrade_lib/couchbase.py` | Version chain lookup dict |
| `ClusterRestAPI` | `cb_server_rest_util/cluster_nodes/cluster_nodes_api.py` | Rebalance, add_node, failover, cluster_details |
| `RebalanceUtil` | `rebalance_utils/rebalance_util.py` | `monitor_rebalance()` blocking wait |
| `RetryRebalanceUtil` | `rebalance_utils/retry_rebalance.py` | Inject/clear/verify retry-rebalance conditions |
| `CollectionBase` | `bucket_collections/collections_base.py` | deploy_buckets_from_spec_file, load_data_from_spec_file, create_clients_for_sdk_pool |
| `Cbstats` | `cb_tools/cbstats.py` | `vbucket_list()` pre/post vbucket verification |
| `RemoteMachineShellConnection` | `shell_util/remote_connection.py` | SSH: stop_couchbase, execute_command |
| `SDKClient` | `sdk_client3.py` | Direct SDK ops for insert/XDCR tests |
| `CbasUtil` | `cbas_utils/cbas_utils.py` | Analytics dataset/index/query management |
| `GsiHelper` | `gsiLib/gsiHelper.py` | GSI index creation helpers |
| `QueryRestAPI` | `cb_server_rest_util/query/query_api.py` | N1QL query execution |
| `IndexRestAPI` | `cb_server_rest_util/index/index_api.py` | Index settings (FBR, shard affinity) |

---

## Common Debugging Patterns

| Symptom / Question | Diagnosis |
|---|---|
| Upgrade loop never terminates | `fetch_node_to_upgrade()` returns `None` only when all nodes match `upgrade_version`. Check: `self.upgrade_version` set from `upgrade_chain[-1]`? Community builds append `"community"` to `node_info["version"]` — mismatch causes infinite loop. |
| SyncWrite fails during upgrade | Expected in mixed-mode; `MAJORITY` requires all KV nodes at target version. Verify `sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = SDKConstants.DurabilityLevel.MAJORITY` and spec param `sync_write_spec` is correct. |
| `spare_node` wrong or stale | Set in `setUp()` as `cluster.servers[nodes_init]`. After each `online_swap`, `spare_node` flips to the just-rebalanced-out node. For `online_swap`, spare must be pre-installed before swap starts. |
| Storage migration fails or skips | Triggered only when `migrate_storage_backend=True`. Flow: `update_bucket_property(storageBackend=preferred_storage_mode)` → cycle each node via `migration_procedure`. Default is `swap_rebalance` → `swap_rebalance_all_nodes_iteratively()`. |
| `validate_encryption_operations` assertion fails | Called with `expected_to_fail=True` during upgrade (mixed-mode blocks AES-256 key creation) and `expected_to_fail=False` after full upgrade. Uses `rest.create_secret()` + `bucket_helper.change_bucket_props(encryptionAtRestKeyId=...)`. If timing is off, cluster may not be fully upgraded yet. |
| `add_system_scope_to_all_buckets()` called unexpectedly | Triggered by `float(upgrade_version[:3]) >= 7.6 and float(initial_version[:3]) < 7.6`. 7.6 added `_system` scope (`_query`, `_mobile` collections) — must be mirrored in local bucket objects for downstream validation to match. |
| `attempt_10k_collection_creation()` wrong result | Returns `True` only on 8.1+ clusters. Asserted `False` during upgrade, `True` after. Failure = cluster not fully at 8.1+ when checked. |
| FBR check fails | `verify_and_configure_fbr()` only called when `float(upgrade_version[:3]) >= 8.1`. Checks `internalSettings.dataServiceFileBasedRebalanceEnabled == True`. On older versions the key is absent — guarded by `if 'dataServiceFileBasedRebalanceEnabled' in content`. |
| `if "feature" in self.cluster_features` check seems redundant but must not be removed | `cluster_features` reflects the **current** cluster version at each step of the loop, not the final target. If `upgrade_chain` starts from a base version that predates the feature (e.g. chain `["6.5", "7.2", "8.0"] + "8.1"`), the guard is necessary during the 6.5→7.2 phase even though the target supports the feature. See "WARNING" block in Upgrade Chain Configuration for the full guard table. |
