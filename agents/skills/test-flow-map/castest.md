# castest Test Flow Map

↑ [test_debugging.md](../test_debugging.md)

**Directory:** `pytests/castest/`
**Purpose:** CAS (Compare-And-Swap) value correctness tests — validates CAS semantics across update, delete, expiry, touch, rebalance, failover, restart, backup/restore, and corrupted CAS recovery.

---

## Base Class Capabilities

| Base Class | File | Extends | setUp provides | Key methods (appear in stack traces) |
|---|---|---|---|---|
| `BaseTestCase` | `pytests/basetestcase.py` | `unittest.TestCase` | Cluster ref, `self.log`, `self.input`, `self.task`, `self.task_manager` | `validate_test_failure()`, `log_failure()`, `sleep()` |
| `ClusterSetup` | `pytests/basetestcase.py` | `BaseTestCase` | + `self.cluster` with nodes provisioned, `sdk_client_pool`, `bucket_util`, `cluster_util` | `create_bucket()`, `verify_stats_all_buckets()` |
| `CasBaseTest` | `pytests/castest/cas_base.py` | `ClusterSetup` | + KV mem quota configured via REST, default bucket created (`self.bucket`), SDK client pool initialized | `_modify_bucket()`, `_restart_server()`, `_reboot_server()`, `_check_config()` |

**setUp detail for `CasBaseTest`:**
1. `ClusterSetup.setUp()`
2. Parse params: `doc_size` (default 256), `doc_ops` (semicolon-split), `mutate_times` (default 10), `expire_time` (default 5), `item_flag` (default 0)
3. `ClusterRestAPI(master)` → `node_details()` → `configure_memory(kv_mem_quota)`
4. `bucket_util.create_default_bucket(replica, storage, conflict_resolution)`
5. SDK client pool created if enabled

**tearDown ownership:**
- Failures accumulated via `log_failure()` → `validate_test_failure()` raises at tearDown (`BaseTestCase.tearDown`)
- `CasBaseTest.tearDown` delegates to `ClusterSetup.tearDown`

**Key internal methods (appear in stack traces):**

| Method | File | Purpose |
|---|---|---|
| `_check_config()` | `cas_base.py:188` | REST `get_bucket_json()` → asserts `conflictResolutionType == 'lww'`; called at end of most tests |
| `_modify_bucket()` | `cas_base.py:139` | Changes `timeSynchronization` via REST; expects server to reject the change |
| `_restart_server(servers)` | `cas_base.py:152` | SSH stop + start couchbase per server; sleeps 30s for warmup |
| `_reboot_server()` | `cas_base.py:162` | OS-level reboot (Windows: `shutdown -r`, Linux: `reboot`); flushes iptables post-reboot |
| `_corrupt_max_cas(mcd, key)` | `cas_base.py:284` | Uses `setWithMetaInvalid` to push CAS to -2, then two `set()` calls to drive it to -1 (MB-17517 setup) |

---

## CAS Base Tests

### `cas_base.py`
**Chain:** `CasBaseTest` → `ClusterSetup`

| Test Method | Key Params | Step-by-step Flow |
|---|---|---|
| `test_modify_bucket_params` | — | `_modify_bucket()` → REST `change_bucket_props(timeSynchronization='enabledWithOutDrift')` → assert response contains `'TimeSyncronization not allowed in update bucket'` |
| `test_restart` | — | `_restart_server(servers[:])` (stop + sleep 10s + start per node, sleep 30s) → `_check_config()` → assert `conflictResolutionType == 'lww'` |
| `test_failover` | `num_nodes=1` | `cluster.failover(servers[1:1])` → `cluster.rebalance([], servers[1:1])` → `_check_config()` |
| `test_rebalance_in` | — | `ClusterOperationHelper.add_and_rebalance(servers)` → `_check_config()` |
| `test_backup_same_cluster` | `command_options` | `execute_cluster_backup()` → sleep 5s → `restore_backupFile(buckets)` → `_check_config()` (in `finally`) |
| `test_backup_diff_bucket` | `command_options` | `execute_cluster_backup()` → sleep 5s → `create_bucket("new_bucket")` → `restore_backupFile(["new_bucket"])` → `_check_config()` (in `finally`) |

---

## CAS Change Operations

### `opschangecas.py`
**Chain:** `OpsChangeCasTests` → `CasBaseTest` → `ClusterSetup`

**setUp note:** Calls `CasBaseTest.setUp()` then sets `self.key = 'test_cas_docs'`.

**Note:** `OpsChangeCasTests` has no `test_*` methods — helpers consumed by subclasses or conf entries directly. Core logic: `ops_change_cas()` and `touch_test()`.

| Method | Type | Key Params | What it does |
|---|---|---|---|
| `ops_change_cas()` | Shared helper | `doc_ops` (semicolon-separated: `update`, `touch`, `delete`, `expire`) | 1. Load `num_items` full docs <br>2. Build cbstat + vb_details per node <br>3. For each op in `doc_ops`: call `verify_cas(op, gen)` <br>4. Disconnect cbstats → `_wait_for_stats_all_buckets()` → `validate_test_failure()` |
| `touch_test()` | Shared helper | `active_resident_threshold` | 1. Load `num_items` docs <br>2. Load DGM (drive resident ratio below threshold) <br>3. Touch all initial docs (which are on disk) → assert all `status=True` → `validate_test_failure()` |
| `key_not_exists_test()` | Shared helper | — | MB-21448 regression. Loops 1500×: CREATE → DELETE → READ (assert `DocumentNotFoundException`) → REPLACE with old CAS (assert `DocumentNotFoundException`). Each iteration calls `validate_test_failure()` |
| `corrupt_cas_is_healed_on_rebalance_out_in()` | Shared helper | — | MB-17517. SET key → `_corrupt_max_cas()` (drive to -1) → rebalance out node → capture replica CAS → rebalance in → assert active CAS == replica CAS |
| `corrupt_cas_is_healed_on_reboot()` | Shared helper | — | MB-17517. `_corrupt_max_cas()` → stop server + sleep 30s + start server → `getMeta(key)` → assert `maxCas == 0` |

#### `verify_cas(ops, generator)` — detailed logic

Called per-key from `ops_change_cas()`. Validates CAS semantics at SDK + cbstats level.

| `ops` value | Validation logic |
|---|---|
| `"update"` | For each key × `mutate_times`: READ CAS → REPLACE with old CAS → assert status=True + new CAS ≠ old CAS + `max_cas` in cbstats == new CAS + replica CAS == active CAS (retry up to 5×) |
| `"touch"` | Cycle through `[0, 60, 0, 0]` TTL values: assert CAS changes when TTL differs, stays same when TTL repeated |
| `"delete"` | READ CAS → DELETE → assert `result.cas != 0` → REPLACE with pre-delete CAS → assert failure with `DocumentNotFoundException` + returned CAS == 0 |
| `"expire"` | READ CAS → TOUCH with `expire_time` → assert CAS updated → sleep `expire_time+1` → REPLACE with old CAS → assert failure with `DocumentNotFoundException` |

---

## Common Failure Patterns

| Symptom | Layer | Likely cause |
|---|---|---|
| `AssertionError: CAS old == new` | `opschangecas.py:verify_cas` | REPLACE didn't mutate the document — SDK returned stale CAS or op silently failed |
| `CbStats CAS mismatch` in `log_failure` | `opschangecas.py:118` | `max_cas` from `vbucket_details` cbstat doesn't match SDK-returned CAS — replication lag or cbstat read against wrong node |
| `Replica cas mismatch` in `log_failure` | `opschangecas.py:134` | Replica CAS hasn't caught up; retry logic (5×) exhausted — likely a replication issue under test load |
| `_check_config` assertion fails (`conflictResolutionType != 'lww'`) | `cas_base.py:192` | Bucket conflict resolution was not set to LWW — check `bucket_conflict_resolution_type` param |
| `Touch / replace with cas failed` | `opschangecas.py:92` | SDK `touch` or `replace` returned `status=False` — possible SDK timeout or durability issue |
| `[ERROR] Modify testcase failed` | `cas_base.py:63` | `_modify_bucket()` raised unexpected exception — REST API returned an unexpected response body |
| `validate_test_failure()` raises at tearDown | `BaseTestCase.tearDown` | One or more `log_failure()` calls accumulated — trace back to which `verify_cas` op produced the CAS mismatch |
