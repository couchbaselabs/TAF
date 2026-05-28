# epengine Test Flow Map

↑ [test_debugging.md](../test_debugging.md)

**Directory:** `pytests/epengine/`
**Purpose:** EP Engine (KV data service) functional tests — durability, collections, CAS, OOO returns, expiry, rate limiting, warmup, document keys.

---

## Base Class Capabilities

| Base Class | File | Extends | setUp provides | Key methods (appear in stack traces) |
|---|---|---|---|---|
| `BaseTestCase` | `pytests/basetestcase.py` | `unittest.TestCase` | Cluster ref, `self.log`, `self.input`, `self.task`, `self.task_manager` | `validate_test_failure()`, `log_failure()`, `sleep()` |
| `ClusterSetup` | `pytests/basetestcase.py` | `BaseTestCase` | + `self.cluster` with nodes provisioned, `sdk_client_pool`, `bucket_util`, `cluster_util` | `create_bucket()`, `verify_stats_all_buckets()` |
| `CollectionBase` | `pytests/bucket_collections/collections_base.py` | `ClusterSetup` | + Scope/collection lifecycle loaded from spec, `supported_d_levels`, `durability_helper`, `bucket_durability_level` | `load_data_for_sub_doc_ops()`, `update_verification_dict_from_collection_task()`, `validate_cruds_from_collection_mutation()`, `run_scenario_from_spec()` |
| `DurabilityTestsBase` | `pytests/epengine/durability_base.py` | `ClusterSetup` | + default bucket created, `simulate_error`, `doc_ops`, `crud_batch_size=100`, `num_nodes_affected`, `durability_helper`, initial doc load | `get_random_node()`, `getTargetNodes()` |
| `BucketDurabilityBase` | `pytests/epengine/durability_base.py` | `ClusterSetup` | + `bucket_template`, `vbs_in_node` (shell per KV node), `possible_d_levels`, `d_level_order`, `durability_helper`, `kv_nodes` | `validate_durability_with_crud()`, `get_vbucket_type_mapping()`, `get_bucket_dict()`, `get_supported_durability_for_bucket()`, `cb_stat_verify()`, `init_java_clients()`, `get_cb_stat_verification_dict()`, `getTargetNodes()` |
| `CasBaseTest` | `pytests/epengine/opschangecas.py` (inline) | `ClusterSetup` | + CAS tracking dict, cbstats connections per node, initial doc load | `_check_cas()`, `_load_ops()`, `_check_expiry()` |

**tearDown ownership:**
- Failures accumulated via `log_failure()` → `validate_test_failure()` raises at tearDown time (in `BaseTestCase.tearDown`)
- `BucketDurabilityBase.tearDown` disconnects all `vbs_in_node` shell connections
- `DurabilityTestsBase.tearDown` delegates to `ClusterSetup.tearDown`

---

## Durability

### `durability_base.py`
**Defines base classes only — no test methods.**
See Base Class Capabilities table above for `DurabilityTestsBase` and `BucketDurabilityBase`.

---

### `durability_success.py`
**Chain:** `DurabilitySuccessTests` → `DurabilityTestsBase` → `ClusterSetup`

**What this layer adds:** positive durability path. Errors are induced but CRUDs must still succeed. Test fails if any op returns a failure.

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_with_persistence_issues` | `simulate_error`, `durability_level` (MAJORITY only — skipped for PERSIST variants), `num_nodes_affected` | 1. `getTargetNodes()` → per node: open shell, `Cbstats`, capture `vbucket_seqno["init"]` + `failover_stats["init"]` <br>2. `DiskError.create()` or `CouchbaseError.create(simulate_error)` <br>3. Parallel `async_load_gen_docs` CREATE + UPDATE → wait → assert `task.fail == {}` <br>4. Parallel READ + DELETE tasks → wait → assert `task.fail == {}` <br>5. `error_sim.revert()` + `sleep(10)` <br>6. `verify_stats_all_buckets(num_items)` <br>7. `SDKClient` retry any `task.fail` via `retry_with_no_error()` → assert no retry failures <br>8. Capture `vbucket_seqno["afterCrud"]` + `failover_stats["afterCrud"]` → assert seqno **changed**, failover **unchanged** |
| `test_with_process_crash` | `simulate_error`, `num_replicas >= 2` | 1. `getTargetNodes()` (num_nodes_affected forced to 1) → capture vbucket stats + active vb list <br>2. Build doc_gens excluding active vbuckets of target node <br>3. Queue CRUD tasks (`start_task=False`) → add all → sleep 10s → `CouchbaseError.create()` <br>4. Wait all tasks → assert `task.fail == {}` <br>5. `error_sim.revert()` → ephemeral bucket: retry rebalance up to 2x <br>6. `retry_with_no_error()` on any fails <br>7. Validate failover stats: `KILL_MEMCACHED` → failover **changed**; others + ephemeral + `STOP_MEMCACHED` → failover **unchanged** |
| `test_non_overlapping_similar_crud` | `doc_ops` (create/update/delete) | 1. Build two non-overlapping doc_gens (doc_gen[0], doc_gen[1]) based on `doc_ops` <br>2. Task[0]: sync_write with `durability_level` <br>3. Task[1]: non-sync_write with `replicate_to`/`persist_to` <br>4. Task[2]: generic reader across all docs <br>5. Wait all → `verify_stats_all_buckets(num_items)` |
| `test_non_overlapping_parallel_cruds` | `doc_ops` (semicolon-separated 4 ops) | 1. Build doc_gens for create/update/delete/read (non-overlapping ranges) <br>2. First 2 ops from `doc_ops`: sync_write tasks <br>3. Last 2 ops: non-sync tasks <br>4. Wait all → verify count |
| `test_buffer_ack_during_dcp_commit` | `durability_level` (must be non-empty) | 1. Load `num_items*2` via UPDATE with `durability_level` <br>2. `_wait_for_stats_all_buckets()` → sleep 5s for DCP settle <br>3. For each KV node: `Cbstats.dcp_stats()` → assert all keys ending in `unacked_bytes == 0` |
| `test_ops_on_same_key` | `crud_pattern` (e.g. "async:sync:async"), `durability_level` | 1. CREATE all keys (sync/async per `crud_pattern[0]`) <br>2. DELETE all keys (sync/async per `crud_pattern[1]`) <br>3. CREATE all keys again (sync/async per `crud_pattern[2]`) <br>4. Subdoc `lookup_in(LookupInMacro.rev_id)` → assert `rev_id == 3` for every key |

---

### `durability_failures.py`
**Chains:**
- `DurabilityFailureTests` → `DurabilityTestsBase` → `ClusterSetup`
- `TimeoutTests` → `DurabilityTestsBase` → `ClusterSetup`

**What this layer adds:** negative durability path. Validates expected exceptions and that vbucket seqnos don't advance during error windows.

#### `DurabilityFailureTests`

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_crud_failures` | `durability_level`, cluster sized so quorum impossible | 1. Capture `vbucket_seqno["init"]` for all KV nodes <br>2. Attempt durable CREATE x2 (MB-34064 regression) → assert all `num_items` keys in `task.fail` <br>3. Assert `DurabilityImpossibleException` for all failures via `validate_durability_exception()` <br>4. Assert `vbucket_seqno` unchanged vs ["init"] <br>5. Async CREATE (no durability) → assert 0 failures → capture seqno["create_stat"] <br>6. Durable UPDATE + durable DELETE in parallel → wait → assert all fail with `DurabilityImpossibleException` <br>7. Assert seqno unchanged vs ["create_stat"] |
| `test_sync_write_in_progress` | `doc_ops` (semicolon pair e.g. "create;update"), `simulate_error`, `with_non_sync_writes` | 1. `getTargetNodes()` → open shells → get **replica** vbuckets → intersect across nodes (target_vbuckets) <br>2. Build doc_gens targeting `target_vbuckets` <br>3. `SDKClient` for individual per-key ops <br>4. `CouchbaseError.create()` → sleep 10s <br>5. Start bulk task_1 (`doc_ops[0]`, `start_task=True`) → sleep 10s <br>6. Per key: `client.crud(doc_ops[1])` → assert `AmbiguousTimeoutException` + retry_reason `KV_SYNC_WRITE_IN_PROGRESS` (or `DocumentNotFoundException` for create+delete combo) <br>7. Per key: `client.crud("read")` → assert: CREATE in progress → key not found; other ops → previous value visible <br>8. `error_sim.revert()` → wait task_1 → retry UPDATE ops without error |
| `test_sync_write_in_progress_for_persist_active` | `simulate_error`, `doc_ops[0]` | 1. Single `get_random_node()` → get active + replica vb lists <br>2. Force `durability_level = MAJORITY_AND_PERSIST_TO_ACTIVE` <br>3. `CouchbaseError.create()` → start task_1 → sleep 20s <br>4. Per key: individual crud → if vb in active or replica → assert `DurableWriteInProgressException`; else → assert success <br>5. `error_sim.revert()` → wait task_1 → verify count |
| `test_durability_with_persistence` | `simulate_error`, `durability_level` | 1. Single `get_random_node()` → get active + replica vb lists → capture seqno["init"] <br>2. `CouchbaseError.create()` <br>3. Parallel CREATE/UPDATE/READ/DELETE tasks → wait all <br>4. Capture seqno["withDiskIssue"] → for active vbs: assert every stat **unchanged** <br>5. Validate error type by vb: active vb → `durability_not_possible`; replica vb (1 node affected) → same; unaffected vbs → no error <br>6. `error_sim.revert()` → `SDKClient` retry all failed ops → assert all retry succeed |
| `test_bulk_sync_write_in_progress` | `doc_ops`, `simulate_error`, `with_non_sync_writes` | 1-4. Same as `test_sync_write_in_progress` <br>5. Start task_1 → sleep 10s → start task_2 (targets same vbuckets) → wait task_2 <br>6. Assert exactly `crud_batch_size` docs in `task_2.fail` with `AmbiguousTimeoutException` + `KV_SYNC_WRITE_IN_PROGRESS` <br>7. Revert → wait task_1 → if UPDATE: read-back and assert `mutated == 1` |
| `test_durability_abort` | `durability_level` | 1. For each KV node: open shell + `Cbstats` → get replica vbuckets <br>2. Build doc_gen targeting those vbuckets <br>3. `bucket_util.load_durable_aborts()` → assert no failures <br>4. Verify `sync_write_aborted_count` in vbucket-details stats <br>5. Retry aborted keys on healthy cluster: create → update → delete → create <br>6. After each op: verify `sync_write_committed_count` increments by `crud_batch_size` |

#### `TimeoutTests`

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_timeout_with_successful_crud` | `simulate_error`, `sdk_timeout` | 1. `getTargetNodes()` → capture seqno["init"] <br>2. For each op_type [create, update, read, delete]: <br>&nbsp;&nbsp;a. Create doc_gen → build task (`start_task=False`) <br>&nbsp;&nbsp;b. `error_sim.create()` → `task_manager.add_new_task()` → sleep 5s → `error_sim.revert()` → wait task <br>&nbsp;&nbsp;c. Assert `task.fail == {}` (reads: warning only) <br>&nbsp;&nbsp;d. Assert seqno changed after each op <br>&nbsp;&nbsp;e. `retry_with_no_error()` on any fails |
| `test_timeout_with_successful_crud_for_persist_active` | `simulate_error`, `sdk_timeout` | 1. Single `get_random_node()` → force `MAJORITY_AND_PERSIST_TO_ACTIVE` <br>2. Capture seqno["init"] → `error_sim.create()` <br>3. Parallel CREATE/UPDATE/READ/DELETE tasks → sleep `sdk_timeout - 20s` <br>4. Capture seqno["withinTimeout"] → assert **unchanged** vs ["init"] <br>5. `error_sim.revert()` → wait all tasks → `retry_with_no_error()` on fails <br>6. Capture seqno["afterCrud"] → assert unchanged (persist_active: seqno consistency validated separately) |
| `test_timeout_with_crud_failures` | `simulate_error`, `sdk_timeout` | 1. `getTargetNodes()` → capture seqno["init"], active + replica vb lists <br>2. Build doc_gens for all op types → `error_sim.create()` → sleep 10s → start all tasks <br>3. Wait all → assert `DurabilityAmbiguousException` on all non-read failures <br>4. Assert `int(time.time()) >= expected_timeout` (timeout fired on schedule) <br>5. `error_sim.revert()` <br>6. For each affected node: `validate_vb_seqno_stats()` with retry — assert affected vbuckets seqno **not updated** <br>7. Per failed key: `retry_for_ambiguous_exception()` → assert eventual success |

---

## Collection CRUD

### `collection_crud_success.py`
**Chain:** `CollectionsSuccessTests` → `CollectionBase` → `ClusterSetup`

**What this layer adds:** positive collection-scoped CRUD (doc + subdoc) with optional error injection. Validation via vbucket-details stats and `validate_docs_per_collections_all_buckets`.

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_basic_ops` | `durability_level` | 1. Build `verification_dict` from `collection.num_items` across all scopes <br>2. `verify_vbucket_details_stats()` baseline check <br>3. Async CRUD spec (create 100%, update 25%, delete 25%) → parallel `__perform_collection_crud()` (2 scopes + 10 collections + docs) → wait → validate results <br>4. `validate_docs_per_collections_all_buckets()` <br>5. Update `verification_dict` from task → re-verify vbucket-details → `validate_cruds_from_collection_mutation()` |
| `test_with_persistence_issues` | `simulate_error`, `durability_level` (skipped for PERSIST variants) | 1. `DurabilityHelper.getTargetNodes()` → per node: `Cbstats`, capture `vbucket_seqno["init"]` + `failover_stats["init"]` <br>2. `CouchbaseError/DiskError.create()` <br>3. Async CRUD spec → parallel `__perform_collection_crud(mutation_num=2)` → wait → validate <br>4. `error_sim.revert()` + sleep(10) <br>5. `validate_docs_per_collections_all_buckets()` <br>6. Capture seqno + failover ["afterCrud"] → assert seqno **changed**, failover **unchanged** |
| `test_with_process_crash` | `simulate_error`, `num_replicas >= 2` | 1. Capture seqno + failover ["init"] → get active vbs of target node <br>2. Async CRUD spec (excluding active vbs of target) → sleep 5s → `CouchbaseError.create()` → parallel collection CRUD <br>3. Wait → validate <br>4. `error_sim.revert()` → ephemeral: rebalance retry up to 2x <br>5. Validate failover: `KILL_MEMCACHED` → changed; `STOP_MEMCACHED` → unchanged; ephemeral + other → changed |
| `test_non_overlapping_similar_crud` | `doc_ops`, `durability_level` | 1. Verify baseline vbucket-details stats <br>2. Build async_write spec + sync_write spec for `doc_ops` type <br>3. Run both tasks in parallel → wait both → validate |
| `test_crud_with_transaction` | `transaction_start_time` ("before_collection_crud" or "after_collection_crud"), `num_items` | 1. Build atomicity task (CREATE, `start_task=False`) <br>2. If `before`: add atomicity task first → start CRUD task; if `after`: start CRUD first → then add atomicity <br>3. Wait both → `validate_cruds_from_collection_mutation()` |
| `test_sub_doc_basic_ops` | `durability_level` | 1. Verify baseline vbucket stats <br>2. SubDoc INSERT 100% → wait → verify doc count + vbucket stats <br>3. SubDoc UPSERT 30% + REMOVE 30% → parallel `__perform_collection_crud(mutation_num=3)` → wait <br>4. `validate_docs_per_collections_all_buckets()` |
| `test_sub_doc_non_overlapping_similar_crud` | `op_type` (insert/upsert/remove) | 1. `load_data_for_sub_doc_ops()` <br>2. Build async spec + sync spec for `op_type` subdoc op <br>3. Run both in parallel → wait → validate |
| `test_sub_doc_with_persistence_issues` | `simulate_error`, `durability_level` (MAJORITY only) | Same structure as `test_with_persistence_issues` but load spec uses subdoc INSERT/UPSERT/REMOVE |
| `test_sub_doc_with_process_crash` | `simulate_error`, `num_replicas >= 2` | Same structure as `test_with_process_crash` but load spec uses subdoc ops |

---

### `collection_crud_negative.py`
**Chain:** `CollectionDurabilityTests` → `CollectionBase` → `ClusterSetup`

**What this layer adds:** negative collection-scoped durability — validates exceptions and that vbucket seqno does not advance during failures.

**setUp note:** Pre-populates `self.verification_dict` from existing `collection.num_items`.

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_crud_failures` | `sub_doc_test` (bool) | 1. `__get_random_durability_level()` (excludes NONE) → verify baseline vbucket stats <br>2. Durable CRUD spec x2 (MB-34064) → assert doc count **unchanged** → assert vbucket seqno unchanged <br>3. Async CRUDs → assert success <br>4. Update `verification_dict` → verify vbucket-details |
| `test_durability_abort` | — | 1. `__get_d_level_and_error_to_simulate()` → picks d_level + matching error type <br>2. Per KV node: get target vb type (replica or active for PERSIST_ACTIVE) → `CouchbaseError.create()` → run spec → `revert()` <br>3. Verify `sync_write_aborted_count` in vbucket-details <br>4. `validate_doc_loading_results()` (retry aborted docs) → verify `sync_write_committed_count` |
| `test_sync_write_in_progress` | `doc_ops` (semicolon pair) | 1. `__get_d_level_and_error_to_simulate()` <br>2. `getTargetNodes()` → capture active + replica vb lists <br>3. `CouchbaseError.create()` → start bulk task_1 → sleep 5s <br>4. Per collection per doc: `client.crud(doc_ops[1])` → assert `AmbiguousTimeoutException` + `KV_SYNC_WRITE_IN_PROGRESS` <br>5. Per doc: `client.crud("read")` → assert visibility <br>6. `error_sim.revert()` → wait task_1 → `validate_docs_per_collections_all_buckets()` |
| `test_bulk_sync_write_in_progress` | `doc_ops` | Bulk variant: task_2 targets same docs as task_1 via `async_load_gen_docs`; assert all in task_2.fail with `AmbiguousTimeoutException` |
| `test_sub_doc_sync_write_in_progress` | `doc_ops` (insert/upsert/remove) | 1. `load_data_for_sub_doc_ops()` <br>2. `__get_d_level_and_error_to_simulate()` <br>3. `CouchbaseError.create()` → start doc loading task → sleep 10s <br>4. Per collection per key: attempt all 3 subdoc ops (INSERT/UPSERT/REMOVE) → assert `AmbiguousTimeoutException` + `KV_SYNC_WRITE_IN_PROGRESS` |

---

### `collection_sdk_exceptions.py`
**Chain:** `SDKExceptionTests` → `CollectionBase` → `ClusterSetup`

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_collection_not_exists` | — | Create scope/collection → CRUD → drop collection → CRUD → assert `CollectionNotExists` → recreate → CRUD → assert success |
| `test_collections_not_available` | — | Make collection unavailable → CRUD → assert exception → restore → CRUD → assert success |
| `test_timeout_with_successful_crud` | `simulate_error`, `sdk_timeout` | 1. Capture vbucket seqno baseline <br>2. `CouchbaseError.create()` → start CRUD → `error_sim.revert()` before timeout → wait → assert success <br>3. `validate_vb_seqno_stats()` — assert seqno advanced |
| `test_timeout_with_crud_failures` | `simulate_error`, `sdk_timeout` | 1. `CouchbaseError.create()` → start CRUD → do NOT revert → wait timeout → assert `DurabilityAmbiguousException` <br>2. `compare_vb_stat()` → assert seqno **not** advanced for affected vbs |

---

## Protocol / Stats

### `opschangecas.py`
**Chain:** `OpsChangeCasTests` → `CasBaseTest` → `ClusterSetup`

**What this layer adds:** CAS consistency across individual ops and cluster events (rebalance/failover/restart).

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_cas_set` | — | Set docs → `_check_cas()` validates CAS ≥ `max_cas` in cbstats |
| `test_cas_updates` | — | Update/replace → assert CAS increments per op |
| `test_cas_deletes` | — | Delete → assert CAS updates correctly |
| `test_cas_expiry` | `ttl` | Load with TTL → wait expiry → `_check_expiry()` validates CAS behavior post-expiry |
| `test_cas_touch` | — | Touch → assert CAS value unchanged |
| `test_cas_getMeta` | — | `getMeta` RPC → validate returned CAS matches vbucket max_cas |
| `test_cas_setMeta_lower` | — | `setMeta` with CAS lower than current → validate conflict resolution outcome |
| `test_cas_setMeta_higher` | — | `setMeta` with CAS higher than current → validate CAS accepted |
| `test_cas_deleteMeta` | — | `deleteMeta` → validate CAS tracking |
| `test_cas_skip_conflict_resolution` | — | Write with `skip_conflict` flag → validate CAS bypasses CR |
| `test_revid_conflict_resolution` | — | Insert → delete → re-insert → assert rev_id-based CR wins |
| `test_cas_conflict_resolution` | — | Multi-node write conflict → assert CAS-based CR determines winner |
| `test_meta_rebalance_out` | `nodes_init` | Load → `rebalance(out)` → `_check_cas()` per vbucket |
| `test_meta_failover` | — | Load → hard failover → `_check_cas()` |
| `test_meta_soft_restart` | — | Load → graceful restart → `_check_cas()` |
| `test_meta_hard_restart` | — | Load → hard reboot → `_check_cas()` |
| `test_restart_revid_conflict_resolution` | — | revid CR validated after node restart |
| `test_rebalance_revid_conflict_resolution` | — | revid CR during active rebalance |
| `test_failover_revid_conflict_resolution` | — | revid CR during failover |
| `test_cas_getMeta_empty_vBucket` | — | `getMeta` on empty vbucket → validate response |
| `test_meta_backup` | — | Load → backup → restore → `_check_cas()` |

---

### `ooo_returns.py`
**Chain:** `OutOfOrderReturns` → `ClusterSetup`

**Key internal helpers (appear in stack traces):**

| Method | Purpose |
|---|---|
| `crud()` | CRUD with `test_lock` synchronization; appends op order to `ooo_order` list |
| `__validate_crud_result()` | Validates OOO ordering list after all ops complete |
| `__transaction_runner()` | Runs a transaction with lock-ordered tracking |
| `trans_doc_gen()` | Generates transaction document set |

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_dgm_reads` | `dgm_percent` | Load to DGM → parallel reads via `crud()` → `__validate_crud_result()` |
| `test_with_sync_write` | `simulate_error` | `CouchbaseError.create()` → parallel sync_writes via `crud()` → validate OOO |
| `test_transaction_with_crud` | — | Interleaved `__transaction_runner()` + `crud()` → validate OOO ordering |
| `test_parallel_transactions` | — | Multiple `__transaction_runner()` in parallel → validate OOO |

---

### `rate_limiting_tests.py`
**Chain:** `KVRateLimitingTests` → `ClusterSetup`

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_bucket_limit_crud_and_cli` | `bucket_limit` | Set limit via REST + CLI → `load_docs_to_bucket()` → `get_throttle_stats()` → assert CLI and REST stats match |
| `test_limit_enforcement` | `throttle_limit`, `reject_limit` | Load → `get_throttle_stats()` → assert `throttle_count > 0` and `reject_count > 0` as configured |
| `test_rate_limiting_unavailable_in_ce` | — | CE edition → assert REST returns error when setting rate limit |
| `test_enable_disable_rate_limiting_rest` | — | Enable → load → `get_throttle_stats()` (expect throttle) → disable → load → assert no throttle |
| `test_soft_limit_throttling` | `throttle_limit` | Sustained load above soft limit → assert `throttle_count > 0` |
| `test_hard_limit_rejection` | `reject_limit` | Load beyond hard limit → assert `reject_count > 0` |
| `test_transaction_rate_limiting` | — | Transaction load → validate throttle/reject stats appear correctly |

**Key internal methods:**
- `get_throttle_stats()` — fetches throttle-related stats via `cbstats`
- `load_docs_to_bucket()` — loads docs with configurable concurrency

---

### `secondary_warmup.py`
**Chain:** `SecondaryWarmup` → `BaseTestCase`

**Note:** Does NOT go through `ClusterSetup`. Bucket configured manually from spec in setUp. Reader thread count set explicitly.

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_secondary_warmup` | `warmup_type` ("background" or "blocking"), `num_reader_threads` | 1. `insert_docs()` with TTL → `monitor_secondary_warmup_stats()` baseline <br>2. `kill_memcached_on_nodes()` → trigger warmup <br>3. `trigger_warmup_perform_cruds()` — parallel INSERT/READ/UPSERT during warmup <br>4. `validate_warmup_behavior()` — blocking: reads unavailable until complete; background: reads available during warmup |

**Key internal methods:**

| Method | Purpose |
|---|---|
| `kill_memcached_on_nodes()` | Kills memcached on target nodes to trigger warmup |
| `insert_docs()` | Inserts docs with TTL and verifies |
| `read_docs()` | Reads docs and logs results |
| `upsert_docs()` | Upserts with timeout handling |
| `perform_cruds()` | Executes parallel INSERT/READ/UPSERT with result tracking |
| `monitor_secondary_warmup_stats()` | Monitors warmup state transitions |
| `validate_warmup_behavior()` | Validates read/write availability during warmup |

---

## Basic CRUD & Document Ops

### `basic_ops.py`
**Chain:** Multiple classes → `ClusterSetup`

Common flow: create/load → read → update → delete → `verify_stats_all_buckets()`. Parallel mutations with reader threads. See source for full method list: `grep -n "def test_" pytests/epengine/basic_ops.py`.

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_kv_curr_connections_bucket_metrics` | `nodes_init=1` | For each KV node: (1) `query_prometheus(kv_curr_connections)` → assert bucket label present for every bucket, values ≥ 0, bucket sum ≤ node-level, node-level still exists; (2) `query_prometheus(kv_curr_connections_closing)` → assert bucket label present, each bucket value ≤ 1 |

---

### `expiry_maxttl.py`
**Chain:** `ExpiryMaxTTL` → `ClusterSetup`

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_maxttl_lesser_doc_expiry` | `maxttl`, `doc_ttl < maxttl` | Load with `doc_ttl` → wait `doc_ttl` → run expiry pager → assert count drops at `doc_ttl` |
| `test_maxttl_greater_doc_expiry` | `maxttl`, `doc_ttl > maxttl` | Load → wait `maxttl` → pager → assert count drops at `maxttl` not `doc_ttl` |
| `test_set_maxttl_on_existing_bucket` | — | Load → update bucket `maxTTL` → load more → validate expiry with new maxTTL |
| `test_maxttl_possible_values` | `maxttl` | Attempt bucket create with various maxTTL values → assert valid/invalid responses |
| `test_update_maxttl` | — | Change maxTTL mid-test → validate both old and new TTL docs expire correctly |
| `test_maxttl_with_doc_updates` | — | Load → update docs → assert TTL reset to maxTTL on update |
| `test_maxttl_with_sync_writes` | `durability_level` | Sync_write load with TTL → wait → validate expiry honored |
| `test_maxttl_with_timeout` | `simulate_error` | Induce timeout → load with TTL → revert → validate eventual expiry |
| `test_ttl_less_than_durability_timeout` | `sdk_timeout > ttl` | Race: TTL expires before durability timeout → validate behavior |
| `test_persistent_metadata_purge_age_purge_validation` | `purge_age` | Load → expire → assert metadata purged after `purge_age` |

---

### `documentkeys.py`
**Chain:** `DocumentKeysTests` → `ClusterSetup`

| Test Method | Key Params | Flow Summary |
|---|---|---|
| `test_dockey_whitespace_data_ops` | — | Load with whitespace keys → CREATE/UPDATE/DELETE → `_persist_and_verify()` |
| `test_dockey_binary_data_ops` | — | Binary key chars → CRUD → verify persistence stats |
| `test_dockey_unicode_data_ops` | — | Unicode keys → CRUD → verify |
| `test_dockey_whitespace_views` | — | Whitespace keys → create view → query → `_verify_with_views()` |
| `test_dockey_binary_views` | — | Binary keys → view query |
| `test_dockey_unicode_views` | — | Unicode keys → view query |
| `test_dockey_whitespace_dcp` | — | Whitespace keys → hard failover → DCP validation |
| `test_dockey_binary_dcp` | — | Binary keys → DCP |
| `test_dockey_unicode_dcp` | — | Unicode keys → DCP |

**Key internal methods:**

| Method | Purpose |
|---|---|
| `_persist_and_verify()` | Waits for persistence and validates cbstats |
| `_verify_with_views()` | Creates view, queries it, validates results |
| `_dockey_data_ops()` | Drives CREATE/UPDATE/DELETE with a given key set |
| `_dockey_dcp()` | Triggers failover and validates DCP stream |

---

## Bucket-Level Durability

### `bucket_level_durability.py`
**Chains:**
- `CreateBucketTests` → `BucketDurabilityBase` → `ClusterSetup`
- `BucketDurabilityTests` → `BucketDurabilityBase` → `ClusterSetup`

**What this layer adds:** validates bucket-level durability configuration across bucket types (couchbase/ephemeral) and all durability levels.

**setUp note:** `BucketDurabilityBase.setUp` opens `RemoteMachineShellConnection` per KV node into `vbs_in_node`. `tearDown` **disconnects all of them** — stack frame in `tearDown` at `disconnect()` is normal cleanup, not a failure.

| Test Method | Class | Key Params | Flow Summary |
|---|---|---|---|
| `test_create_bucket_using_cli` | `CreateBucketTests` | `load_docs_using` | For each d_level in [NONE, MAJORITY, MAJORITY_AND_PERSIST_TO_ACTIVE, PERSIST_TO_MAJORITY]: <br>1. `create_default_bucket(bucket_dict)` with that d_level <br>2. `init_java_clients(bucket)` — `reset_java_loader_tasks()` then `create_clients_in_pool()` <br>3. `sleep(10)` — wait for SDK client warmup <br>4. `validate_durability_with_crud(bucket, d_level, verification_dict)` — see `BucketDurabilityBase` row in Base Class Capabilities <br>5. `cb_stat_verify(bucket, verification_dict)` — asserts `ops_create`, `sync_write_aborted_count`, `sync_write_committed_count` match expected from step 4 <br>6. `reset_java_loader_tasks()` <br>7. `delete_bucket(bucket)` <br>**With `sirius_java_sdk`:** see [sirius-java-sdk.md](sirius-java-sdk.md) for the full DocLoader integration flow and known failure signatures |
| `test_create_bucket_using_rest` | `CreateBucketTests` | — | Same as CLI but via REST API |
| `test_durability_with_bucket_level_none` | `BucketDurabilityTests` | `doc_durability` | Bucket d_level=NONE; for each doc d_level: `validate_durability_with_crud()` → `cb_stat_verify()` |
| `test_ops_only_with_bucket_level_durability` | `BucketDurabilityTests` | `bucket_durability` | Doc ops with `doc_durability=""` (inherits bucket level) → `cb_stat_verify()` |
| `test_sub_doc_op_with_bucket_level_durability` | `BucketDurabilityTests` | `bucket_durability` | SubDoc ops inheriting bucket durability → `cb_stat_verify()` |
| `test_higher_durability_level_from_client` | `BucketDurabilityTests` | — | Client d_level > bucket d_level → effective = client → `validate_durability_with_crud()` |
| `test_lower_durability_level_from_client` | `BucketDurabilityTests` | — | Client d_level < bucket d_level → effective = bucket → validate |
| `test_update_durability_level` | `BucketDurabilityTests` | — | CRUD → update bucket d_level → CRUD again → `cb_stat_verify()` |
| `test_update_durability_between_doc_op` | `BucketDurabilityTests` | — | Update bucket d_level mid-CRUD → assert new level applies to subsequent ops |
| `test_sync_write_in_progress` | `BucketDurabilityTests` | `simulate_error` | Same sync-write-in-progress pattern as `DurabilityFailureTests` but at bucket-level durability |
| `test_observe_scenario` | `BucketDurabilityTests` | — | Observe-based persistence check → validate with bucket durability active |
| `test_durability_impossible` | `BucketDurabilityTests` | — | Create bucket with d_level requiring more nodes than available → assert creation fails |
