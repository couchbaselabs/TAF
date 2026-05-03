# Sirius Java SDK Integration — Test Flow Map

↑ [test_debugging.md](../test_debugging.md)

**Applies to any test using:** `load_docs_using=sirius_java_sdk`, or any test that directly instantiates `SiriusCouchbaseLoader(...)` (common in Magma and Fusion storage tests that bypass the task framework and drive doc loading inline)

---

**Trigger log patterns:** `CRITICAL Failure during get_task_result of Task_N_0` · `argument of type 'NoneType' is not iterable` · `Docs inserted without honoring the bucket durability level`

---

## Layer Stack

```
Test method (e.g. validate_durability_with_crud)
  └─ task.async_load_gen_docs(..., load_using="sirius_java_sdk")
       └─ cluster.py: SiriusCouchbaseLoader(...)
            └─ create_doc_load_task()  →  POST /doc_load        → DocLoader REST
       └─ task_manager.add_new_task(task)
            └─ task.start_task()       →  POST /submit_task     → DocLoader REST
       └─ task_manager.get_task_result(task)
            └─ task.get_task_result()  →  POST /get_task_result → DocLoader REST
```

**DocLoader** = Spring Boot Java server (`DocLoader/`); started separately or via `--launch_java_doc_loader`. All Python↔DocLoader communication is HTTP POST.

---

## Initialization Flow (`init_java_clients`)

Called once per bucket before any load tasks run. Lives in `BucketDurabilityBase` and
`DurabilityTestsBase` in `pytests/epengine/durability_base.py`.

### Step 1: `POST /reset_task_manager` — Full JVM-Side Sequence

```
POST /reset_task_manager
  → shutdown_taskmanager()
      → abort_all_tasks()                          # cancel running WorkLoadGenerate threads
      → taskManager.shutdown()                     # ExecutorService.shutdownNow()
      → reset_sdk_client_pool()
          → SDKClientPool.shutdown()
              → client.disconnectCluster()          # per client: SharedClusterManager.releaseCluster()
              → clientCache/idleClients/busyClients.clear()
              → SharedClusterManager.shutdownAll()
                  → cluster.disconnect()            # per Cluster in clusterMap
                  → clusterMap.clear()
                  → sharedEnvironment.shutdown()         # TLS ClusterEnvironment
                  → sharedNonTLSEnvironment.shutdown()   # non-TLS ClusterEnvironment
          → new SDKClientPool()                     # fresh empty pool
  → init_taskmanager()
      → new TaskManager(num_workers)               # fresh executor
  → loader_tasks = new ConcurrentHashMap<>()       # clear task registry
```

Each `ClusterEnvironment` owns Netty `EventLoopGroup`s and KV connection pools. If an
environment is not shut down between tests, its threads persist and corrupt the new
environment's KV connections — especially under node disruption (`stop_memcached`).

### Step 2: `POST /create_clients` — SDK Client Bootstrap

```
SDKClientPool.create_clients(bucket, server, n)
  → for each client:
      new SDKClient(server, bucket)
      → SDKClient.initialiseSDK()
          → SharedClusterManager.getCluster(server)
              → initializeSharedEnvironment()           # lazy: creates TLS env if shutdown
              → initializeSharedNonTLSEnvironment()    # lazy: creates non-TLS env if shutdown
              → Cluster.connect(server.ip, options)     # bootstraps to seed node
          → cluster.bucket(bucket)                     # opens bucket
          → bucket.scope(scope).collection(collection) # selects collection
      → idlePool.add(client)
```

`SharedClusterManager` manages two shared `ClusterEnvironment` instances (TLS and non-TLS),
selected by `server.memcached_port` (`"11207"` → TLS, `"11210"` → non-TLS). Clusters are
shared across SDKClient instances via reference counting. Environments are lazily recreated
after shutdown.

**Known Java bug in this step:** `create_clients` always returns `HTTP 400` regardless of
success/failure (`TaskRequest.java:368`). The JSON body carries the real status
(`{"status": true/false}`). Python's `create_clients_in_pool` reads the JSON body's
`status` field (not HTTP status) to determine success.

---

## Doc Load Task Lifecycle

### Phase 1 — Create task: `POST /doc_load`

```
Python: SiriusCouchbaseLoader.create_doc_load_task()
  → POST /doc_load  {bucket, scope, collection, key ranges, percents, durability_level, ops, ...}
  ← HTTP 200  {"tasks": ["Task_N_0", "Task_N_1", ...], "status": true}
```

- DocLoader creates one `WorkLoadGenerate` object per effective worker.
- Stores them in `loader_tasks: ConcurrentHashMap<String, WorkLoadGenerate>`.
- Returns task names; Python stores them in `self.task_ids`.
- **Effective workers** = `min(process_concurrency, ceil(totalDocs / batchSize))`.
  With `process_concurrency=1` and `ops=40000`, `batchSize=40000`, `create_range=1`:
  `ceil(1/40000) = 1` → 1 worker spawned, named `Task_N_0`.

### Phase 2 — Submit task: `POST /submit_task`

```
Python: task.start_task()  →  _make_task_request(task_ids, "submit_task", 10)
  → POST /submit_task  {"task_id": "Task_N_0"}
  ← HTTP 200  {"status": true}   # task submitted to executor
             {"status": false}  # task not found in loader_tasks (silent NPE)
```

Python now checks the JSON `status` field. If `false`, `add_new_task` raises an `Exception`,
surfacing the failure immediately instead of silently proceeding.

Inside DocLoader, `WorkLoadGenerate.run()`:

```
if sdkClientPool != null:
    while sdk == null:                        # wait for available client
        sdk = sdkClientPool.get_client_for_bucket(bucket, scope, collection)
        if sdk == null: sleep(1s)
        # InterruptedException → result=false, return
actual_run()                                  # write/read docs
```

If `create_clients` failed and the pool is empty, this loop runs forever until the thread
is interrupted (e.g., during `reset_task_manager` shutdown).

### Phase 3 — Get results: `POST /get_task_result`

```
Python: task.get_task_result()
  → POST /get_task_result  {"task_id": "Task_N_0"}
  ← HTTP 200  {"status": true/false, "fail": {"key": {"error": "...", "value": "...", "status": ...}}}
  ← HTTP 500  (unhandled Java exception — see known bugs)
```

Python iterates `self.task_ids`, collects failures into `self.fail` dict.
`task_manager.get_task_result` returns False and logs CRITICAL if any task returns `ok=False`.

---

## Failure Signatures and Likely Causes

| Log message | Python file | Root cause |
|-------------|-------------|------------|
| `CRITICAL Failure during get_task_result of Task_N_0` | `task_manager.py:34` | `task.get_task_result()` returned False. **Expected** when error sim is active (durability failures set `task.result=false`). Red flag: CRITICAL + `doc_load_task.fail` empty → infrastructure failure, not real durability result |
| `get_task_result HTTP error for task_id=Task_N_0` | `java_loader_tasks.py` | DocLoader returned HTTP 5xx for `/get_task_result` |
| `Docs inserted without honoring the bucket durability level` | `durability_base.py:343` | `doc_load_task.fail` is empty when it should have entries — either no writes happened, or result retrieval failed |
| `ops_create: 0 != 1` in cbstats | `durability_base.py` | No successful writes reached Couchbase — SDK clients unavailable or DocLoader task never executed |
| `sync_write_aborted_count: 0 != 1` in cbstats | `durability_base.py` | No sync-write aborts recorded — error-sim phase load task did not write any docs |
| `Failed to create Java SDK clients for bucket X` | `durability_base.py:252` | `create_clients` JSON body returned `status=false` — SDK failed to connect to cluster/bucket |
| `Getting task result for ` *(empty thread name)* | `task_manager.py:30` | `task_ids=[]` — DocLoader returned empty `{"tasks":[]}`. For subdoc-only loads: `effectiveWorkers=0` because subdoc ranges excluded from `totalDocsToProcess` (see Subdoc-Only Load section) |

---

## Subdoc-Only Load: Zero Workers Bug

**Symptom:** `[task_manager:get_task_result:30] Getting task result for ` (empty task name)
and `ops_update: 0 != N` / `sync_write_committed_count: M != N` in cbstats.

**Root cause:** DocLoader's `doc_load` handler computes `effectiveWorkers` from:
```java
long totalDocsToProcess = (createEndIndex - createStartIndex)
                        + (updateEndIndex - updateStartIndex)
                        + (readEndIndex - readStartIndex)
                        + (deleteEndIndex - deleteStartIndex)
                        + (expiryEndIndex - expiryStartIndex);
```
Subdoc ranges (`sdInsertEndIndex - sdInsertStartIndex`, etc.) are **not included**.
For subdoc-only loads (`subdoc_percent=100`, all other percents=0) every term is 0 →
`totalDocsToProcess=0` → `effectiveWorkers=0` → zero tasks → `{"tasks": []}` returned →
Python: `self.task_ids=[]`, `self.thread_name=""` → `get_task_result` loop is a no-op →
0 writes, test sees cbstat mismatches.

**Python workaround (applied):** In `SiriusCouchbaseLoader.__init__`, after the SubDocOps
generator branches, mirror `generator.start/end` into `create_start/end_index` when the
load is subdoc-only. `create_percent` stays 0 so no actual create ops run; DocLoader just
needs `createEndIndex - createStartIndex > 0` to count workers correctly.

**DocLoader Java fix** (`TaskRequest.java:683`, `~/gitRepos/couchbaselabs/DocLoader`): subdoc ranges added to `totalDocsToProcess`:
```java
long totalDocsToProcess = ...existing terms...
    + (this.sdInsertEndIndex - this.sdInsertStartIndex)
    + (this.sdUpsertEndIndex - this.sdUpsertStartIndex)
    + (this.sdRemoveEndIndex - this.sdRemoveStartIndex)
    + (this.sdReadEndIndex - this.sdReadStartIndex);
```

---

## Known DocLoader Java Bugs (submodule — requires DocLoader PR to fix)

| Location | Bug | Impact |
|----------|-----|--------|
| `RestServer/TaskRequest.java` | `create_clients` always returns `HTTP 400` even on success | Python reads JSON body instead (workaround) |

---

## Debugging Checklist for `sirius_java_sdk` Failures

| Step | What to check | Signal |
|---|---|---|
| 1 | DocLoader running | Spring Boot startup log present; `curl http://localhost:<port>/` responds |
| 2 | `create_clients` success | `"Failed to create Java SDK clients for bucket"` absent in log; JSON `status=true` returned |
| 3 | `submit_task` success | No exception from `add_new_task`; task was not cleared between `/doc_load` and `/submit_task` |
| 4 | `get_task_result` HTTP | No `"get_task_result HTTP error for task_id="` in log; cross-check DocLoader stdout for Java stack traces |
| 5 | cbstats write evidence | `ops_create > 0` OR `sync_write_aborted_count > 0`; both 0 = SDK clients unavailable or task interrupted |
| 6 | `self.fail` vs `ok` | `self.fail` empty + `ok=False` = result retrieval failed (not a real "no failures") — "Docs inserted without honoring durability" is a false positive from infrastructure failure |
| 7 | Back-to-back failures | Test passes standalone but fails after prior test → `SharedClusterManager` lifecycle; both TLS and non-TLS envs must be shut down and recreated between tests |
