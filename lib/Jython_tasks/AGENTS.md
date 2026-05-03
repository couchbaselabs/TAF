---
name: jython-tasks-agent
description: >
  TAF async task framework — base task classes, TaskManager executor, document-loading
  implementations for default Python SDK / Sirius Java SDK / Sirius Go SDK,
  and all background operation tasks (rebalance, failover, validation, index, CBAS, etc.).
model: inherit
---

# lib/Jython_tasks

Async task execution framework. All test-visible document loading, rebalance monitoring,
background operations, and validation tasks are implemented here and scheduled via `TaskManager`.

↑ root [AGENTS.md](../../AGENTS.md)

---

## Files

| File | Key Classes | Purpose |
|---|---|---|
| `task_manager.py` | `TaskManager`, `Task` | Executor (`ThreadPoolExecutor`-backed) + base `Task` class (extends `Future`) |
| `task.py` | 60+ task classes (see below) | All framework task implementations — doc load, rebalance, failover, validation, index, CBAS, stats, views, N1QL |
| `java_loader_tasks.py` | `SiriusJavaDocGen`, `BaseSiriusLoader`, `SiriusCouchbaseLoader`, `SiriusJavaMongoLoader` | Sirius Java SDK (DocLoader) HTTP bridge — REST-based doc loading against a running DocLoader server |
| `sirius_task.py` | `LoadCouchbaseDocs`, `WorkLoadTask`, `DatabaseManagementTask`, `BlobLoadTask`, `MongoUtil`, `CouchbaseUtil` | Sirius Go SDK task implementations via `sirius_client_framework` |
| `async_result.py` | `AsyncResult` | Bundles a Task + TaskManager so callers can retrieve results without holding a TaskManager reference |

---

## `task_manager.py`

### `TaskManager`

Central executor. `max_workers=100` by default (single `ThreadPoolExecutor`).

| Method | Behaviour |
|---|---|
| `add_new_task(task)` | Sirius tasks (`SiriusCouchbaseLoader`, `SiriusJavaMongoLoader`): calls `task.start_task()`, raises on failure. Regular tasks: submits `task.call` to `ThreadPoolExecutor`, stores future in `self.futures[task.thread_name]`. |
| `get_task_result(task)` | Sirius tasks: calls `task.get_task_result()`, logs CRITICAL on `False`. Regular tasks: blocks on `future.result()`, pops from `self.futures`. Returns `bool`. |
| `schedule(task, sleep_time=0)` | Optional sleep then `add_new_task`. |
| `stop_task(task)` | Sirius tasks: calls `task.end_task()`. Regular tasks: polls `future.done()` up to 30s then `future.cancel()`. |
| `shutdown(timeout=5)` | `pool.shutdown()` + 5s sleep + `pool.shutdown(wait=True)`. |
| `abort_all_tasks()` | Cancels all pending futures in `self.futures`. |
| `print_tasks_in_pool()` | Logs WARNING for any future not yet done. |

### `Task` (base, in `task_manager.py`)

Extends `concurrent.futures.Future`. All task classes in `task.py` inherit this.

| Attribute/Method | Purpose |
|---|---|
| `thread_name` | Unique name: `"{class_name}_{serial_number}"` — appears in all log lines and `self.futures` dict key |
| `result` | `bool` — set via `set_result()`; checked by callers after `get_task_result` |
| `fail` | `dict` — populated by loading tasks; maps `key → {error, value, status}` |
| `call()` | Abstract — subclasses implement the task body here |
| `start_task()` / `complete_task()` | Set `started`/`completed` timestamps |
| `set_exception(e)` | Sets exception + calls `complete_task()` + raises |
| `wait_until(getter, condition, timeout)` | Exponential-backoff poll until condition satisfied or timeout (default 300s) |

---

## `task.py` — Task Class Reference

### Document Loading

| Class | Extends | Purpose | Key result attrs |
|---|---|---|---|
| `GenericLoadingTask` | `Task` | Base for all SDK doc-load tasks; manages `SDKClient`, retries, per-key error tracking | `fail` dict, `success_count` |
| `LoadDocumentsTask` | `GenericLoadingTask` | Single-op type (create/update/delete/read) doc load | `fail`, `success_count` |
| `LoadSubDocumentsTask` | `GenericLoadingTask` | Sub-document op (insert/upsert/remove/lookup) load | `fail`, `success_count` |
| `LoadDocumentsGeneratorsTask` | `Task` | Multi-generator, multi-collection doc load from spec; drives `GenericLoadingTask` instances internally | `result` bool |
| `LoadSubDocumentsGeneratorsTask` | `Task` | Same as above for sub-doc ops | `result` bool |
| `ContinuousDocOpsTask` | `Task` | Indefinite doc ops loop; stopped via `task.stop_indefinite_doc_loading_tasks()` | `fail` dict |
| `LoadDocumentsForDgmTask` | `LoadDocumentsGeneratorsTask` | Loads until DGM threshold is hit | `result`, `doc_index` |
| `ValidateDocumentsTask` | `GenericLoadingTask` | Read-back + value validation per key | `fail` dict |
| `DocumentsValidatorTask` | `Task` | Collection-level validator driven from spec | `result` bool |
| `MutateDocsFromSpecTask` | `Task` | CRUD driven from a mutation spec (used by `run_scenario_from_spec`) | `result` bool |
| `ValidateDocsFromSpecTask` | `Task` | Validates doc count and content per spec | `result` bool |
| `Durability` | `Task` | Durable write with abort simulation (used by `load_durable_aborts`) | `result` bool |
| `ContinuousRangeScan` | `Task` | Continuous range scan across collections; stopped via `task.stop_task = True` | `fail_map` dict |
| `RangeScanOnCollection` | `Task` | Single range scan on one collection | `result` bool |
| `Atomicity` | `Task` | Transaction (atomicity) doc ops | `result` bool |

### Rebalance / Topology

| Class | Purpose | Key result attrs |
|---|---|---|
| `RebalanceTask` | On-prem rebalance; monitors via `_check_rebalance()` loop | `result` bool |
| `RebalanceTaskCapella` | Capella rebalance variant | `result` bool |
| `FailoverTask` | Graceful or hard failover | `result` bool |
| `AutoFailoverNodesFailureTask` | Injects node failures to trigger auto-failover | `result` bool |
| `NodeFailureTask` | Single-node failure injection | `result` bool |
| `ConcurrentFailoverTask` | Multiple concurrent failovers | `result` bool |
| `NodeDownTimerTask` | Keeps node down for a specified duration | — |

### Bucket / Cluster Management

| Class | Purpose |
|---|---|
| `BucketCreateTask` | Create a bucket via REST |
| `BucketCreateFromSpecTask` | Create bucket(s) from spec dict |
| `BucketFlushTask` | Flush a bucket |
| `NodeInitializeTask` | Initialize a new node (set paths, credentials) |
| `FunctionCallTask` | Wraps any callable; used for one-off background calls (e.g., `enable_tls`) |
| `TimerTask` | Runs a function after a delay |

### Index / Query

| Class | Purpose |
|---|---|
| `CreateIndexTask` | Create a GSI index |
| `BuildIndexTask` | Build a deferred index |
| `MonitorIndexTask` | Poll until index reaches `online` state |
| `DropIndexTask` | Drop a GSI index |
| `CompareIndexKVData` | Compare index item count vs KV item count |
| `N1QLQueryTask` | Execute a single N1QL query |
| `RunQueriesTask` | Execute multiple N1QL queries concurrently |
| `N1QLTxnQueryTask` | N1QL query within a transaction |

### Stats / Monitoring

| Class | Purpose | Key result attrs |
|---|---|---|
| `StatsWaitTask` | Poll cbstats until a stat reaches an expected value | `result` bool |
| `MonitorActiveTask` | Monitor an active task (e.g., indexer, compaction) | `result` bool |
| `MonitorDBFragmentationTask` | Poll fragmentation until threshold | `result` bool |
| `MonitorBucketCompaction` | Wait for bucket compaction to complete | `result` bool |
| `PrintBucketStats` | Print bucket stats to log | — |
| `MonitorClusterStatsTask` | Continuous cluster stats monitoring | — |
| `MonitorServerlessDatabaseScaling` | Wait for serverless DB scaling to complete | `result` bool |

### Views

| Class | Purpose |
|---|---|
| `ViewCreateTask` | Create a MapReduce view |
| `ViewDeleteTask` | Delete a view |
| `ViewQueryTask` | Query a view and validate results |
| `MonitorViewFragmentationTask` | Monitor view fragmentation |
| `ViewCompactionTask` | Trigger and wait for view compaction |
| `CompactBucketTask` | Trigger bucket compaction |

### CBAS (Analytics)

| Class | Purpose |
|---|---|
| `CBASQueryExecuteTask` | Execute an analytics query |
| `CreateDatasetsTask` | Create CBAS datasets |
| `CreateSynonymsTask` | Create CBAS synonyms |
| `CreateCBASIndexesTask` | Create CBAS indexes |
| `CreateUDFTask` | Create CBAS UDF |
| `DropDatasetsTask` | Drop CBAS datasets |
| `DropSynonymsTask` | Drop CBAS synonyms |
| `DropCBASIndexesTask` | Drop CBAS indexes |
| `DropUDFTask` | Drop CBAS UDF |
| `DropDataversesTask` | Drop CBAS dataverses |
| `ExecuteQueryTask` | Execute arbitrary analytics query |

### Capella / Cloud

| Class | Purpose |
|---|---|
| `DeployColumnarInstance` | Deploy a Columnar instance |
| `ScaleColumnarInstance` | Scale a Columnar instance |
| `DeployDataplane` | Deploy a data plane |
| `DeployCloud` | Deploy a cloud cluster |
| `UpgradeProvisionedCluster` | Upgrade a provisioned Capella cluster |
| `DatabaseCreateTask` | Create a serverless database |
| `CloudHibernationTask` / `HibernationTask` | Hibernate/resume cloud clusters |

---

## `java_loader_tasks.py` — Sirius Java SDK (DocLoader)

### `SiriusJavaDocGen`
Key generator for tests using Sirius Java SDK keys. Fetches key list from DocLoader on init via `SiriusCouchbaseLoader.get_keys_from_sirius()`. Implements `has_next()` / `next()` compatible with standard doc_generator interface.

### `BaseSiriusLoader`
Shared HTTP utilities for all Sirius Java loaders.

| Method | Purpose |
|---|---|
| `_make_api_request(endpoint, data, timeout)` | POST to `{sirius_url}/{endpoint}`; returns `(ok, json_response)` |
| `_make_task_request(task_ids, endpoint, timeout)` | Loops over task_ids calling `_make_api_request` for each |
| `_flatten_param_to_str(value)` | Serializes dict/list to DocLoader JSON string format |
| `_reset_indexes()` | Zeros all operation start/end index attributes |
| `_print_error_table(bucket, scope, collection, failed_dict)` | Logs failed keys as a table via `TableView` |

### `SiriusCouchbaseLoader`
HTTP bridge to the DocLoader REST API for Couchbase document operations.

| Method | DocLoader endpoint | Purpose |
|---|---|---|
| `create_doc_load_task()` | `POST /doc_load` | Registers task in DocLoader; populates `self.task_ids`; returns `{"tasks": [...], "status": true}` |
| `start_task()` | `POST /submit_task` per task_id | Submits registered tasks to DocLoader executor; raises if `status=false` |
| `get_task_result()` | `POST /get_task_result` per task_id | Collects failures into `self.fail`; returns `bool ok` |
| `end_task()` | `POST /end_task` per task_id | Stops running tasks gracefully |
| `cancel_task()` | `POST /cancel_task` per task_id | Cancels tasks immediately |
| `create_clients_in_pool(server, username, password, bucket_name, ...)` | `POST /create_clients` + `POST /reset_task_manager` | Static. Calls `reset_task_manager` first, then creates SDK clients in DocLoader's pool |
| `get_keys_from_sirius(prefix, size, type, start, end, mutate)` | `POST /get_doc_keys` | Fetches the key list DocLoader would generate for a given range |

**Task lifecycle:** `create_doc_load_task()` → `add_new_task()` (calls `start_task()`) → `get_task_result()`

### `SiriusJavaMongoLoader`
Same lifecycle as `SiriusCouchbaseLoader` but targets MongoDB via DocLoader's Mongo endpoint. Methods: `create_doc_load_task()`, `start_task()`, `get_task_result()`, `end_task()`, `cancel_task()`.

**Full execution flow, initialization sequence, failure signatures, subdoc zero-workers bug, known Java bugs, and debugging checklist:**
→ [`agents/skills/test-flow-map/sirius-java-sdk.md`](../../agents/skills/test-flow-map/sirius-java-sdk.md)

---

## `sirius_task.py` — Sirius Go SDK

Uses `sirius_client_framework` (Go-based sirius submodule) via Python bindings.

| Class | Purpose | Result attrs |
|---|---|---|
| `LoadCouchbaseDocs` | Factory object — builds a `WorkLoadTask` from a doc generator + op type + Couchbase connection info | — |
| `WorkLoadTask` | Executes doc ops via `SiriusClient.start_workload()`; maps TAF `DocLoading` op types to `SiriusCodes.DocOps` | `fail` dict, `success_count`, `fail_count`, `resultSeed` |
| `DatabaseManagementTask` | Database-level operations (create/delete DB or collection) via `SiriusClient.database_management()` | `error`, `message`, `data` |
| `BlobLoadTask` | Blob/object storage load via `SiriusClient.start_blob_workload()` | `fail` dict, `success_count`, `fail_count` |
| `MongoUtil` | Helper object for MongoDB operations; wraps `MongoLoader` + creates `WorkLoadTask` instances | — |
| `CouchbaseUtil` | Helper object for Couchbase operations via Sirius Go; wraps `CouchbaseLoader` + creates tasks | — |

**`LoadCouchbaseDocs.create_sirius_task()` mapping:**

| TAF `DocLoading` op | `SiriusCodes.DocOps` |
|---|---|
| `CREATE` | `SiriusCodes.DocOps.CREATE` |
| `UPDATE` | `SiriusCodes.DocOps.UPDATE` |
| `READ` | `SiriusCodes.DocOps.READ` |
| `TOUCH` | `SiriusCodes.DocOps.TOUCH` |
| `REPLACE` | `SiriusCodes.DocOps.REPLACE` |
| `DELETE` | `SiriusCodes.DocOps.DELETE` |

---

## `async_result.py`

| Class | Purpose |
|---|---|
| `AsyncResult(task_manager, task)` | Wraps a scheduled task so callers can call `async_result.get()` instead of holding `task_manager` directly |
| `.get()` | Delegates to `task_manager.get_task_result(task)` |
| `.ready()` | Returns `task.complete` (note: typo in source — attribute is `completed`, not `complete`) |
| `.successful()` | Returns `True` if `task.exception is not None` (note: inverted semantics — present in source as-is) |

---

## Maintenance Rules

### When adding a new task class to `task.py`

1. Extend `Task` (from `task_manager.py`). Set `self.result` before calling `complete_task()`.
2. If the task accumulates per-key failures, store them in `self.fail` (dict format: `{key: {error, value, status}}`) — callers check this attribute by convention.
3. Add a row to the relevant section in the task class reference table above.

### When modifying `task_manager.py`

`add_new_task` and `get_task_result` have Sirius-vs-regular branching logic. Changes here affect every test in the repo that calls `self.task_manager.add_new_task()` or `self.task_manager.get_task_result()`. Verify the epengine, subdoc, castest, and upgrade flow maps still accurately describe the task lifecycle.

### When modifying `java_loader_tasks.py` or adding a new loader class

Check the constraints in [`sirius-java-sdk.md`](../../agents/skills/test-flow-map/sirius-java-sdk.md) before writing:
- `reset_task_manager` must be called between tests (via `create_clients_in_pool`).
- `create_clients` HTTP response is always 400 — read JSON body `status` field only.
- Subdoc-only loads need `create_start/end_index` mirrored from subdoc range (zero-workers workaround).
- `submit_task` `status=false` must raise immediately.

Update [`sirius-java-sdk.md`](../../agents/skills/test-flow-map/sirius-java-sdk.md) if:
- A new DocLoader endpoint is added → update Layer Stack + lifecycle sections.
- A new failure signature appears → add row to Failure Signatures table.
- The subdoc zero-workers fix is merged in DocLoader → remove the Python workaround note.

### When adding a new `sirius_task.py` class

Add a row to the `sirius_task.py` section above. If the class maps new op types, add rows to the op-type mapping table.
