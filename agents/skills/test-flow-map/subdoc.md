# subdoc Test Flow Map

↑ [test_debugging.md](../test_debugging.md)

**Directory:** `pytests/subdoc/`
**Purpose:** Sub-document and XATTR functional tests — durability, timeout, failure, SDK xattr operations, and MB regression coverage.

---

## Base Class Capabilities

| Base Class | File | Extends | setUp provides | Key methods (appear in stack traces) |
|---|---|---|---|---|
| `BaseTestCase` | `pytests/basetestcase.py` | `unittest.TestCase` | Cluster ref, `self.log`, `self.input`, `self.task`, `self.task_manager` | `validate_test_failure()`, `log_failure()`, `sleep()` |
| `ClusterSetup` | `pytests/basetestcase.py` | `BaseTestCase` | + `self.cluster` with nodes provisioned, `sdk_client_pool`, `bucket_util`, `cluster_util` | `create_bucket()`, `verify_stats_all_buckets()` |
| `DurabilityTestsBase` | `pytests/epengine/durability_base.py` | `ClusterSetup` | + default bucket created, `simulate_error`, `doc_ops`, `crud_batch_size=100`, `num_nodes_affected`, `durability_helper`, initial doc load | `get_random_node()`, `getTargetNodes()` |
| `SubdocBaseTest` | `pytests/subdoc/subdoc_xattr.py` | `ClusterSetup` | + bucket created via `create_bucket()`, scope/collection if non-default, RBAC user per bucket | — (helper methods only: `generate_json_for_nesting()`) |

**tearDown ownership:**
- Failures accumulated via `log_failure()` → `validate_test_failure()` raises at tearDown time (`BaseTestCase.tearDown`)
- `SubdocXattrDurabilityTest.tearDown` closes `self.client` SDKClient before calling super
- `DurabilityTestsBase.tearDown` delegates to `ClusterSetup.tearDown`

---

## Sub-Doc Success

### `sub_doc_success.py`
**Chain:** `BasicOps` → `DurabilityTestsBase` → `ClusterSetup`

**setUp note:** Calls `DurabilityTestsBase.setUp()` (which creates the bucket and loads `num_items` docs), then sets `self.is_sync_write_enabled` from `DurabilityHelper.is_sync_write_enabled`.

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_basic_ops` | `op_type` (None=INSERT only, `"upsert"`, `"remove"`) | 1. Build `verification_dict` baseline <br>2. `verify_vbucket_details_stats()` initial check <br>3. `async_load_gen_sub_docs` INSERT `num_items` → wait → collect `insert_failures` <br>4. `_wait_for_stats_all_buckets()` + `verify_stats_all_buckets(num_items)` <br>5. If `op_type=upsert`: UPSERT half the docs → wait → read-back → assert expected field values == `{'StateUpdate', 2, 'LastNameUpdate', 'TypeChange', 'CityUpdate', 'FirstNameUpdate'}` <br>5. If `op_type=remove`: REMOVE half → wait → LOOKUP → assert `PathNotFoundException` for all removed keys <br>5. If `op_type=None`: skip mutation phase (no warning since fix at line 220) <br>6. `_wait_for_stats_all_buckets()` + `verify_vbucket_details_stats()` + `verify_stats_all_buckets()` |
| `test_non_overlapping_similar_crud` | `op_type` (`"insert"`, `"upsert"`, `"remove"`) | 1. Load `num_items` extra full docs to support upsert/remove pre-condition <br>2. `num_items *= 2` <br>3. If `insert`: build two non-overlapping sub_doc_gens (halves) <br>3. If `upsert`/`remove`: pre-INSERT all docs then build two non-overlapping edit gens <br>4. task[0]: durability sub_doc op on first half → task[1]: same op on second half → task[2]: full-doc reader across all <br>5. Wait all → `_wait_for_stats_all_buckets()` + `verify_stats_all_buckets()` + `verify_vbucket_details_stats()` |
| `test_non_overlapping_parallel_cruds` | `doc_ops` (semicolon-separated 4 ops, default `"insert;upsert;remove;read"`) | 1. Pre-INSERT sub_docs into `insert_end_index..num_items` range <br>2. Build 4 non-overlapping generators (insert/read/upsert/remove covering different key ranges) <br>3. Start full-doc CREATE + READ tasks <br>4. Start 4 sub_doc tasks: ops[0..1] with `durability_level`, ops[2..3] with `replicate_to`/`persist_to` (non-sync) <br>5. Wait all → `num_items *= 2` → `verify_stats_all_buckets()` + `verify_vbucket_details_stats()` |
| `test_with_persistence_issues` | `simulate_error`, `durability_level` (skipped for PERSIST variants) | 1. `getTargetNodes()` → per node: open shell, `Cbstats`, capture `vbucket_seqno["init"]` + `failover_stats["init"]` <br>2. `CouchbaseError.create(simulate_error)` <br>3. Pre-load sub_docs for upsert/remove range <br>4. Parallel INSERT + READ + UPSERT + REMOVE tasks → wait all → assert `task.fail == {}` for each via `log_failure` <br>5. `error_sim.revert()` + `shell_conn.disconnect()` <br>6. Capture seqno/failover `["afterCrud"]` → assert seqno **changed**, failover **unchanged** <br>7. `_wait_for_stats_all_buckets()` + `verify_stats_all_buckets()` + `validate_test_failure()` |
| `test_with_process_crash` | `simulate_error`, `num_replicas >= 2` | 1. `num_nodes_affected` forced to 1 → `getTargetNodes()` → capture seqno/failover ["init"] + active vb list <br>2. Pre-INSERT sub_docs for first half <br>3. `CouchbaseError.create()` → build 4 doc_gens excluding active vbs of target node <br>4. Parallel INSERT/READ/UPSERT/REMOVE tasks → wait all via `task_manager.get_task_result` <br>5. `error_sim.revert()` <br>6. Read-back `mutated` field for all docs → assert INSERT→1, UPSERT/REMOVE→2 <br>7. Validate failover unchanged + seqno changed <br>8. `validate_test_failure()` |
| `test_expired_sys_xattr_consumed_by_dcp` | `durability_level` | 1. CREATE doc with `SDKClient.crud` + INSERT xattr + UPDATE with `exp=10s` <br>2. `_wait_for_stats_all_buckets()` → `sleep(doc_ttl)` <br>3. Load 1000 docs into same vbucket as expired doc <br>4. `rest.set_indexer_storage_mode()` → CREATE PRIMARY INDEX via N1QL <br>5. Poll `state == "online"` (up to 10 retries) → no crash expected |
| `test_subdoc_lookup_on_locked_document` | — | MB-66015 regression. 1. Upsert doc → capture CAS <br>2. `collection.get_and_lock(key, 120s)` → assert CAS changed <br>3. GET → assert success <br>4. `client.crud("subdoc_read", key, "name", cas=0)` → assert key in `success_dict` + value matches |
| `test_subdoc_multi_max_paths` | — | MB-63734. Steps 1-7: loop over `[17,20,25,40]` paths: (a) assert curr_value succeeds; (b) assert num_paths fails; (c) `manage_global_memcached_setting(subdoc_multi_max_paths=num_paths)` + sleep 15s; (d) assert num_paths now succeeds. Then reset to 16, verify enforcement, delete+recreate bucket, verify default limit persists. Steps 8-10 (offload thresholds, resolved via MB-68830): read+remember original `subdoc_offload_paths_threshold`/`subdoc_offload_size_threshold` via `manage_global_memcached_setting()` (defaults 16/1048576 apply silently if never touched, so fall back with `.get()`); set `subdoc_offload_paths_threshold=4` + sleep 15s → mutate 3 xattr paths (assert `subdoc_offload_count`, summed via `Cbstats.all_stats()` across `get_kv_nodes()`, unchanged) then 8 xattr paths (assert count increases + read-back correct); restore paths threshold; set `subdoc_offload_size_threshold=256` + sleep 15s → repeat below/above check with a small vs. large (`>2×threshold`) doc body; restore both thresholds to originals; close `Cbstats`. Step 11: test invalid `subdoc_multi_max_paths` values `[-1,0,15,"a","test"]` → assert `status=False` + error `"too_small"`/`"invalid"` |

---

## Sub-Doc Failures

### `sub_doc_failures.py`

#### `SubDocTimeouts`
**Chain:** `SubDocTimeouts` → `DurabilityTestsBase` → `ClusterSetup`

**setUp note:** Calls `DurabilityTestsBase.setUp()` then pre-loads `num_items/2` sub-docs via INSERT using `async_load_gen_sub_docs`.

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_timeout_with_successful_crud` | `simulate_error`, `sdk_timeout` | 1. `getTargetNodes()` → per node: shell + `Cbstats` + capture `vb_info["init"]` <br>2. For each op_type in [insert, read, upsert, remove]: <br>&nbsp;&nbsp;a. Start task (`start_task=True` default) <br>&nbsp;&nbsp;b. `error_sim.create()` → sleep 5s → `error_sim.revert()` → `get_task_result()` <br>&nbsp;&nbsp;c. Assert `task.fail == {}` (reads: warning only) → assert seqno changed (non-read) <br>3. Disconnect shells <br>4. Read-back `mutated` field: keys < `num_items//2` → expect 2, keys >= → expect 1 <br>5. `_wait_for_stats_all_buckets()` + `verify_stats_all_buckets()` + `validate_test_failure()` |
| `test_timeout_with_crud_failures` | `simulate_error`, `sdk_timeout` | 1. `getTargetNodes()` → capture active + replica vb lists + `vb_info["init"]` <br>2. Build 4 doc_gens targeting affected vbs; create tasks with `start_task=False` <br>3. `error_sim.create()` → add all tasks → wait all <br>4. Assert non-read failures all have `DurabilityAmbiguousException` <br>5. `error_sim.revert()` → assert `int(time.time()) >= expected_timeout` <br>6. `validate_vb_seqno_stats()` (inner function) with retry up to 3× — assert affected vbs seqno **not updated** <br>7. `validate_test_failure()` <br>8. Retry each failed op → assert retry failures ⊆ initial failures <br>9. Final seqno check — assert eventually updated after retry |

#### `DurabilityFailureTests`
**Chain:** `DurabilityFailureTests` → `DurabilityTestsBase` → `ClusterSetup`

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_crud_failures` | `durability_level` (cluster sized so quorum impossible) | 1. Load `num_items` full docs async (no durability) <br>2. Capture `vb_info["init"]` for all nodes <br>3. Durable INSERT ×2 (MB-34064) → assert all `num_items` in `task.fail` with `DurabilityImpossibleException` → assert seqno unchanged <br>4. `validate_doc_mutated_value(0)` — confirm no writes via read-back <br>5. Async (no durability) INSERT → assert 0 failures → `validate_doc_mutated_value(1)` <br>6. Capture `vb_info["create_stat"]` <br>7. Parallel durable UPSERT + REMOVE → wait → assert all fail with `DurabilityImpossibleException` <br>8. Assert seqno unchanged vs `["create_stat"]` <br>9. `validate_test_failure()` |
| `test_sync_write_in_progress` | `doc_ops` (semicolon pair of doc/subdoc op types), `with_non_sync_writes` | 1. `getTargetNodes()` → capture replica vb lists → intersect across nodes <br>2. Build gens targeting those vbs (doc gen + subdoc gen based on `doc_ops[0]`/`doc_ops[1]`) <br>3. If upsert/remove in either op: pre-INSERT sub_docs <br>4. `error_sim.create()` → sleep 5s → add task_1 (`start_task=False`) → sleep 10s → add task_2 → wait task_2 <br>5. Assert task_2 failures == `crud_batch_size` with `AmbiguousTimeoutException` + `KV_SYNC_WRITE_IN_PROGRESS` (or `DocumentNotFoundException` for create+subdoc_insert combo) <br>6. `error_sim.revert()` → wait task_1 → assert update read-back if doc_ops[0]=UPDATE <br>7. `_wait_for_stats_all_buckets()` + `verify_stats_all_buckets()` + `validate_test_failure()` |

---

## Sub-Doc XATTR

### `subdoc_xattr.py`

#### `SubdocBaseTest` (base class — no test methods)
**Chain:** `SubdocBaseTest` → `ClusterSetup`

**setUp provides:** Bucket created via `create_bucket()`. If `collection_name != _default`, creates a random scope/collection. Adds RBAC user per bucket. Calls `print_cluster_stats` and `print_bucket_stats`.

---

#### `SubdocXattrSdkTest`
**Chain:** `SubdocXattrSdkTest` → `SubdocBaseTest` → `ClusterSetup`

Uses `SDKClient` directly (per test). No batch loading tasks.

| Test Method | Flow Summary |
|---|---|
| `test_basic_functionality` | CREATE doc → `subdoc_insert` xattr → read-back xattr → assert value matches |
| `test_multiple_attrs` | INSERT multiple xattr paths → read each → assert all values present |
| `test_xattr_big_value` | INSERT xattr with large value (> 1KB) → read → assert intact |
| `test_add_to_parent` | INSERT xattr at parent path → INSERT child path → read → assert nested structure |
| `test_key_length_big` | INSERT xattr with max-length key → assert success |
| `test_key_underscore` | Keys with leading/trailing underscores → assert allowed or rejected per spec |
| `test_key_start_characters` | Validate allowed/disallowed xattr key start characters |
| `test_key_inside_characters_negative` | Invalid key inner characters → assert SDK error |
| `test_key_inside_characters_positive` | Valid key inner characters → assert success |
| `test_key_special_characters` | Special chars in xattr key → assert expected outcome |
| `test_deep_nested` | Deep JSON nesting via `generate_json_for_nesting()` → INSERT → read → assert |
| `test_delete_doc_with_xattr` | CREATE + xattr INSERT → DELETE doc → read xattr → assert accessible |
| `test_delete_doc_with_xattr_access_deleted` | DELETE doc with xattr → attempt doc read → assert `DocumentNotFoundException` |
| `test_delete_doc_without_xattr` | DELETE doc (no xattr) → assert clean deletion |
| `test_delete_xattr` | INSERT xattr → DELETE xattr path → read → assert `PathNotFoundException` |
| `test_cas_changed_upsert` | INSERT xattr → UPSERT xattr → assert CAS changed |
| `test_use_cas_changed_upsert` | INSERT → UPSERT with old CAS → assert CAS mismatch error |
| `test_recreate_xattr` | INSERT → DELETE → re-INSERT same key → assert value updated |
| `test_update_xattr` | INSERT → UPDATE same path → read → assert new value |
| `test_delete_child_xattr` | INSERT nested → DELETE child path → assert parent still exists |
| `test_delete_xattr_key_from_parent` | INSERT at parent → DELETE parent → assert both gone |
| `test_delete_xattr_parent` | INSERT nested xattr parent → DELETE parent path → assert children gone |
| `test_xattr_value_none` | INSERT xattr with null value → read → assert null |
| `test_xattr_delete_not_existing` | DELETE non-existent xattr → assert `PathNotFoundException` |
| `test_insert_list` / `test_insert_integer_as_key` / `test_insert_double_as_key` | Non-string key types → assert rejection or handling per spec |
| `test_multiple_xattrs` / `test_multiple_xattrs2` | Multiple xattr keys on same doc → read all → assert all present |
| `test_check_spec_words` | Reserved spec words as xattr keys → assert correct error |
| `test_upsert_nums` / `test_upsert_order` | Numeric xattr values and insertion order semantics |
| `test_xattr_expand_macros_true` / `test_xattr_expand_macros_false` | `expand_macros=True/False` → assert macro expansion (`${Mutation.CAS}`, etc.) |
| `test_virt_*` (5 tests) | Virtual xattr access (`$document`, `$document.exptime`, etc.) — existence/modify/remove |
| `test_default_view_*` / `test_view_*` (10 tests) | MapReduce views over docs with xattrs — validate xattrs not visible in views |
| `test_reboot_node` | Load xattrs → reboot node → assert xattrs survive |
| `test_use_persistence` | Load xattrs with persist-level durability → validate persistence via cbstats |

---

#### `SubdocXattrDurabilityTest`
**Chain:** `SubdocXattrDurabilityTest` → `SubdocBaseTest` → `ClusterSetup`

**setUp note:** Opens `SDKClient(cluster, bucket, scope, collection)` stored as `self.client`. `tearDown` closes it before calling super.

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_durability_impossible` | `durability_level`, `xattr` | 1. `client.crud(CREATE, doc_id, {...})` — no durability <br>2. `client.crud("subdoc_insert", doc_id, [...], xattr=True, durability=durability_level)` <br>3. Assert `failed_items` non-empty + `DurabilityImpossibleException` in error string |
| `test_doc_sync_write_in_progress` | `durability_level` | For each `doc_task` in [CREATE, UPDATE, REPLACE, DELETE]: start durable doc task on target vb → attempt all `basic_ops` (CREATE/UPDATE/subdoc_insert/subdoc_upsert/subdoc_replace/subdoc_delete/DELETE) → assert `AmbiguousTimeoutException` + `KV_SYNC_WRITE_IN_PROGRESS` for all conflicting ops → revert → wait task → verify doc state |
| `test_subdoc_sync_write_in_progress` | `durability_level` | Same pattern as `test_doc_sync_write_in_progress` but with a durable **subdoc** operation in-flight instead of a doc operation |

---

#### `XattrTests`
**Chain:** `XattrTests` → `SubdocBaseTest` → `ClusterSetup`

Cluster-scale xattr storage scenarios — compaction, expiry, metadata purge, rollback, fragmentation, process crashes.

| Test Method | Flow Summary |
|---|---|
| `test_xattributes` | Bulk load docs + xattrs → read-back all → assert all present |
| `test_xattribute_compaction` | Load docs + xattrs → trigger compaction → assert xattrs survive compaction |
| `test_xattribute_deletion` | Load → delete docs → assert xattrs on deleted docs accessible (tombstone xattrs) |
| `test_xattribute_expiry` | Load docs with TTL + xattrs → wait for expiry → assert xattrs on expired docs accessible via `access_deleted` |
| `test_xattribute_metadata_purge` | Load → expire → trigger metadata purge → assert xattrs purged correctly per config |
| `test_xattributes_with_rollback` | Load → kill memcached to trigger rollback → reload → assert xattrs consistent post-rollback |
| `test_xattributes_with_stopped_replicas` | Stop replicas → load with MAJORITY → assert success → restart → assert replicas catch up |
| `test_xattributes_tolerate_1_failure` | Kill 1 node → load xattrs → assert durability still met → rebalance back |
| `test_fragmentation` | Load large xattrs to induce fragmentation → compaction → assert no data loss |
| `test_crashing_processes` | Crash memcached/ns_server during xattr load → reboot → assert consistency |
| `test_transitions` | Transition bucket type or storage mode → assert xattrs survive |
| `test_shadow_fragmentation` | Shadow xattr fragmentation → compaction → verify |
| `test_shadow_metadata_purge` | Shadow xattr metadata purge age validation |

---

## Common Failure Patterns

| Symptom | Layer | Likely cause |
|---|---|---|
| `WARNING Unsupported doc_operation` (pre-fix) / `CRITICAL Unsupported doc_operation: <val>` | `sub_doc_success.py:221` | `op_type` param is not `"upsert"` or `"remove"`. `None` = INSERT-only run (expected). Any other string = conf typo |
| `assertEqual(len(op_failed_tbl.rows), update_failures)` fails | `sub_doc_success.py:169` | Read-back after UPSERT found unexpected values — durability level not honored or SDKClient returned stale data |
| `assertEqual(len(op_failed_tbl.rows), num_item_start_for_crud)` fails | `sub_doc_success.py:217` | LOOKUP after REMOVE found fewer `PathNotFoundException` than expected — some removes didn't land |
| `DurabilityImpossibleException` seen in success tests | `DurabilityTestsBase` layer | Cluster topology doesn't satisfy `durability_level` — check `num_replicas` vs `nodes_init` |
| `AmbiguousTimeoutException` + `KV_SYNC_WRITE_IN_PROGRESS` seen unexpectedly | `SubDocTimeouts` or `DurabilityFailureTests` layer | Error sim not reverted before timeout — check `error_sim.revert()` was called |
| `PathNotFoundException` during LOOKUP when expecting success | test layer | REMOVE completed but LOOKUP ran against wrong key range or wrong sub-doc path |
| `validate_test_failure()` raises at tearDown | `BaseTestCase.tearDown` | One or more `log_failure()` calls accumulated — trace back to which task's `.fail` dict was non-empty |
