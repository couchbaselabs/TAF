---
name: agooddoctor-volume-agent
description: >
  On-prem volume tests in Hospital.Murphy (SteadyStateVolume, ClusterOpsVolume,
  SystemTestIndexer) and their workload template mode, where a template file
  gives each bucket its own dataset, collections, rates, indexes and queries.
model: inherit
---

# pytests/aGoodDoctor

Volume ("doctor") test suites. This file covers the on-prem `Hospital.Murphy`
tests and, in detail, **workload template mode** (CBQE-9051): the optional
`workload_template` test param, which replaces the single `val_type` workload
with a per-bucket template file.

↑ root [AGENTS.md](../../AGENTS.md)

---

## Files

| File | Class(es) / symbols | Purpose |
|---|---|---|
| `Hospital.py` | `Murphy(BaseTestCase, OPD, WorkloadTemplateMixin)` | On-prem volume tests: `setUp`, `initial_setup`, `SteadyStateVolume`, `ClusterOpsVolume`, `SystemTestIndexer`, the mutation threads (`normal_mutations`, `vector_file_mutations`) |
| `opd.py` | `OPD` (mixin) | Shared helpers: bucket/collection creation, `print_stats`, rebalance/failover, crash helpers, workload pause/resume, backup/restore/PITR. **Also inherited by `hostedOPD.py` (hostedHospital, fusion, goldfish Columnar).** `ColumnarOnPremVolumeTest` calls `create_required_buckets` directly |
| `n1ql.py` | `DoctorN1QL`, `QueryLoad`, `n1ql_identifier()` | GSI index create/build/stats; query load threads. Also used by hostedHospital |
| `workloads.py` | `default`, `Hotel`, `hotel_vector`, `nimbus`, `siftBigANN`, `MSMARCOEmbeddingProduct`, `MSMARCOSiftEmbeddingProduct` | `val_type` workload dicts. They read test params **at import time** |
| `workload_template.py` | `WorkloadTemplateMixin`, plus `template_errors()`, `collection_rate()`, `build_load_defns()` and others | Everything specific to template mode |
| `templates/*.py` | dict literals | The templates: `single_template`, `sparse_dense_500M`, `sparse_dense_1B`, `sparse_bhive_44M` |
| `hosted*.py`, `serverless*.py`, `fusion/`, `goldfish/` | — | Other suites. They share `opd.py` / `n1ql.py`, so changes there must not change their behaviour |

---

## Two ways a run gets its workloads

| | `val_type` (default) | `workload_template=<name or path>` |
|---|---|---|
| Buckets | `num_buckets` buckets named `GleamBookUsers<i>` | One per template key, named after the key; `num_buckets = len(template)` |
| Workload per bucket | **One** `workloads.py` dict, the **same object** for every bucket, so in-place writes hit all buckets | Each bucket's own dict, so nothing is shared |
| Collections | `VolumeCollection<i>`, `collections` of them | One per `collections_defn` entry, named after its `collection_id` |
| Collection → definition | By **position** (see Gotchas) | By **name** |
| Dataset / loader | Global `val_type` picks one loader for all buckets | Per bucket from its `valType`; loaders built in `workload_template.py` |
| Rates | `ops_rate` param for the initial load; `rebl_ops_rate` param overwrites each bucket's `ops` for mutations; every collection gets the full value | Template `ops` / `rebl_ops_rate`, each split across a bucket's collections, with collection-level overrides |

Template mode supports **only** the file-driven vector datasets
(`siftBigANN`, `MSMARCOEmbeddingProduct`, `MSMARCOSiftEmbeddingProduct`), **one
scope** (`_default`) and **GSI** query loads. Different datasets can be mixed
across buckets in one run, e.g. MSMARCO sparse buckets alongside a siftBigANN
dense bucket.

---

## Template mode

### Enabling it

Pass `workload_template=<value>`. The value can be:
- a template name in `templates/`, with or without `.py`, e.g. `sparse_dense_500M`;
- a path, relative to where testrunner runs or absolute.

```bash
python testrunner.py -i <ini> -t aGoodDoctor.Hospital.Murphy.ClusterOpsVolume,workload_template=sparse_dense_500M,iterations=2,threads_to_use=120,load_docs_using=sirius_java_sdk,<cluster/backup/encryption params> --launch_java_doc_loader --sirius_url http://localhost:<port>
```

setUp logs `Using workload template <path>` and one summary line per bucket:
valType, collection count, items, per-collection initial and mutation rates
and pcs, and 2iQPS. It then logs how many DocLoader tasks the initial load
and the mutation waves ask for against `threads_to_use`, as a warning when
some must wait for a thread (see [Process concurrency](#process-concurrency)).
Grep the test log for these.

### Template file format

A template is one Python dict literal, **no assignment and no imports**.
`load_template()` evaluates it with only `range` available, so
`[{} for _ in range(4)]` works but `dict(...)`, `len(...)` and the like do not.
Comments and trailing commas are fine. Top-level keys are bucket names, in run
order.

**Bucket-level keys**

| Key | Required | Meaning |
|---|---|---|
| `valType` | yes | One of the three dataset types. Every collection's `valType` must equal it |
| `key_prefix`, `key_type` | yes (str) | Sent to DocLoader. `trigger_rollback` sets `key_prefix` to `rollback_docs_<n>-`, and afterwards resets it to the `key` param (default `test_docs-`), **not** to the template's value |
| `key_size` | yes (int ≥ 1) | Sent to DocLoader |
| `scopes` | yes, must be `1` | Collections live in `_default` |
| `doc_op_percentages` | yes | An int for each of `create/read/update/delete/expiry`. This is the op mix of every mutation and steady-state wave (the templates use update 100). The initial load always uses create 100 |
| `vecFilePath` | MSMARCO* | MSMARCO vectors file on the **DocLoader host** |
| `baseFilePath` | siftBigANN | SIFT directory. DocLoader reads `bigann_base.bvecs` from it, and `QueryLoad` reads `bigann_query.bvecs` and the ground-truth files from it on the **testrunner host**. All siftBigANN buckets in a template must use the **same** value |
| `ops` | yes, unless every collection sets it | Initial-load rate, split across the bucket's collections |
| `rebl_ops_rate` | yes, unless every collection sets it | Rate of every later load, split across the bucket's collections |
| `process_concurrency` | no (int ≥ 1) | DocLoader workers per step range of each collection's initial load. Falls back to the `pc` param |
| `rebl_process_concurrency` | no (int ≥ 1) | DocLoader workers per step range of each collection's later loads. Falls back to `process_concurrency`, then the `pc` param |
| `2iQPS` | no (default 0) | Number of query threads for this bucket. Each thread cycles through **all** the bucket's queries. Needs ≥ 1 query when > 0 |
| `collections` | no | If given, must equal `len(collections_defn)`. It is overwritten with that value |
| `collections_defn` | yes | Non-empty list of collection definitions (below) |
| `num_items`, `doc_size`, `ftsQPS`, `cbasQPS` | no | Not used by template loads; FTS/CBAS are rejected |
| `model`, `mockVector`, `base64` | no | Passed to DocLoader as-is (`None` when absent), like the framework loaders |

**Collection-level keys** (`collections_defn` entries)

| Key | Required | Meaning |
|---|---|---|
| `collection_id` | yes | **The collection name.** Must match `^[A-Za-z0-9][A-Za-z0-9_]{0,250}$`, and be unique within the bucket. Digit-leading ids (`8M`) are backtick-escaped in N1QL automatically. List order doesn't matter; ids are sorted at load |
| `valType` | yes | Must equal the bucket's |
| `num_items` | yes | 1 to 8,841,823 for MSMARCO* (DocLoader rejects more), 1 to 1,000,000,000 for siftBigANN |
| `2i` | yes | `{"num_indexes": int, "num_queries": int}`. Index `i` uses `indexes[i % len]`, query `q` uses `queries[q % len]` |
| `indexes`, `queries` | yes when the matching count > 0 | Statement templates; see placeholders below |
| `vector` | yes | Non-empty list of dicts with an int `nProbe`. Optional: `similarity`, `quantization`, `description`. Index `i` uses entry `counter % len`, where the counter runs across the whole bucket |
| `dim` | no (DocLoader default 128) | Sent to DocLoader, and stored for each query |
| `ops`, `rebl_ops_rate` | no | Override this collection's share of the bucket rate |
| `process_concurrency`, `rebl_process_concurrency` | no (int ≥ 1) | Override the bucket's value of the same key for this collection |
| `groundTruths` | no | `[(gt_file, label), ...]`, one per query. Without it, siftBigANN queries run but skip recall/accuracy |
| `queryParams` | no | List of dicts, one per query. Ignored for siftBigANN buckets, where `create_indexes` gives each collection one shared `{}` |
| `doc_size` | no (default 256) | Sent to DocLoader |
| `bhive_indexes`, `indexes_base64`, `queries_base64`, `mix_indexes`, `mix_queries` | no | Needed only with the `bhive` / `base64` / `combinational` params (`combinational` falls back to `indexes`/`queries`) |
| `es` | no | Elasticsearch only; not used by Hospital |

**Statement placeholders:**
- Indexes: `{index_name}`, `{bucket}`, `{scope}`, `{collection}`, and `{vector}` for dense indexes. `{vector}` becomes `'dimension', 'description', 'similarity', 'scan_nprobes'` from the collection's `dim` and `vector` entry.
- Queries: `{bucket}`, `{scope}`, `{collection}`, `{nProbe}`, and `{similarity}` for dense queries.
- Literal braces must be doubled (`{{"defer_build":true}}`).

### Rates

| | Initial load | Every later load |
|---|---|---|
| Template key | `ops` | `rebl_ops_rate` |
| pc key ([Process concurrency](#process-concurrency)) | `process_concurrency` | `rebl_process_concurrency` |
| Used by | `template_initial_load`, `template_rollback_load` | mutation thread (`template_vector_mutations`), `template_steady_state_wave` |

The rule is `collection_rate()`:
- A collection-level value wins.
- Otherwise each collection gets `bucket_value // len(collections_defn)`.

Every resolved rate must be **≥ the collection's pc for that load** (see
[Process concurrency](#process-concurrency)). DocLoader paces each of its pc
workers at `ops // pc` docs/s and **loads nothing when that is 0**; see
`boundedBatchSize` in `DocLoader/src/main/java/utils/docgen/WorkLoadSettings.java`.

The `ops_rate` and `rebl_ops_rate` **test params are not used** in template
mode.

**How long a load takes.**
- DocLoader splits each collection's request at its step boundaries and runs pc tasks per step range. MSMARCO has 3 step ranges up to 8,841,823 items; siftBigANN has one per step of 1M/2M/5M/10M/… items.
- Each step range runs at the collection's rate (pc tasks at `rate // pc` docs/s each). So a collection with every range running loads several times faster than its rate until its small ranges finish.
- Only `threads_to_use` tasks (default 60) run at once. So a load takes **at least about Σ(num_items × pc ÷ collection rate) ÷ threads_to_use seconds**, and no less than the runner and cluster allow. [Process concurrency](#process-concurrency) has both limits and the resulting times.
- **Waiting in DocLoader's queue.** Template tasks pass `never_started_timeout=-1`, so they may wait as long as it takes; the `SiriusCouchbaseLoader` default fails a task after 1800s in the queue. The rest of stall detection stays on: an MSMARCO task that starts and then makes no progress for 600s still fails.
- **Don't use `stall_timeout=-1` to avoid stalls.** It turns off the progress polling in `get_task_result()`. What's left is one blocking DocLoader `get_task_result` call per task with a **600s HTTP timeout**, and a timeout is caught, printed and treated as success. So a task still queued or running after 600s counts as finished. siftBigANN tasks already work this way, because `load_sift_data` passes `-1` and `_template_loader` mirrors it: a long siftBigANN load can be reported done before it is.

### Process concurrency

**Which pc a load uses** (`collection_pc()`). For each key, the collection's
value wins over the bucket's.
- Initial and rollback loads: `process_concurrency`, else the `pc` param.
- Every later load: `rebl_process_concurrency`, else `process_concurrency`, else the `pc` param.

**What pc does in DocLoader** (d350d87: `RestServer/TaskRequest.java`,
`couchbase/loadgen/WorkLoadGenerate.java`, `RankedThreadPoolExecutor.java`):
- Each step range gets pc workers (MSMARCO: `min(pc, range size)`). Each worker gets a 1/pc slice of the range, paced at `rate // pc` docs/s. A step range runs at the collection's rate whatever pc is; pc only sets how many threads it takes.
- A worker writes batches of at most 16,384 docs (4 MB ÷ the 256-byte `doc_size` it is sent), back to back until it has done `rate // pc`. It then sleeps 1s minus the **last** batch's time, so above 16,384 docs/s per worker the earlier batches' time is lost. One siftBigANN worker asked for 50,000/s does 27.8k/s; asked for 25,000/s, it does 22.4k/s.
- A siftBigANN worker gets `range size // pc` docs and **the remainder is never loaded**. So a siftBigANN pc must divide every step range of the collection: any divisor of 1,000,000 does for whole-million `num_items`, and setUp rejects a pc that doesn't. An MSMARCO range's last worker takes its remainder.
- The pool starts queued workers in (worker index, submission order) order: every collection's first worker before any second worker. A worker holds its thread until its slice is done, which is hours for an initial-load MSMARCO range and days for a mutation one.

**Shipped values** in `sparse_dense_500M` and `sparse_dense_1B`:

| Bucket | `process_concurrency` | `rebl_process_concurrency` | Why |
|---|---|---|---|
| GleamBookUsers0/1 (MSMARCO) | 1 | 1 | A collection's rate (≤ 5,000/s initial, ≤ 100/s later) is within what one worker holds (~9.5k/s). More workers only take more threads |
| GleamBookUsers2 (siftBigANN) | 4 | 1 | 4 × 12,500/s is the fewest workers that reach the 50,000/s initial rate within the 16,384-doc batch, and 4 divides every step range. 1,000/s later needs 1 |

`single_template` keeps `process_concurrency: 10` for every bucket.
`sparse_bhive_44M` uses `process_concurrency: 2` (a collection's 10,000/s is
just over one MSMARCO worker's ~9.5k/s) and `rebl_process_concurrency: 1`.

**Measured** on the volume runner (24 cores, Xen VM) against its cluster (2 KV
nodes, magma, 1 replica), with the templates' own requests:

| Load | Tasks running | docs/s |
|---|---|---|
| 1 MSMARCO worker asked for 2,500 / 5,000 / 10,000 | 1 | 2,496 / 4,991 / 9,648 |
| 1 siftBigANN worker asked for 12,500 / 25,000 / 50,000 | 1 | 12,734 / 22,352 / 27,814 |
| 1 siftBigANN step range at pc 4 (4 × 12,500) | 4 | 49,304 |
| 20 MSMARCO collections at 2,500/s (1B GleamBookUsers0), pc 10, 60 threads | 60 of 600 | 15,155 |
| The same at pc 1 | 60 | 67,191 |
| Whole 1B initial load at the shipped pcs, 200 threads | 186 | 120,982 (MSMARCO 67,982, siftBigANN 53,001) |
| Whole 500M initial load at the shipped pcs, 120 threads | 107 | 126,363 (MSMARCO 67,103, siftBigANN 59,260) |

- MSMARCO tops out near 67.5k docs/s, however many tasks run.
- siftBigANN gets what is left of the runner's ~243 MB/s of outbound traffic (about 3.2 KB per MSMARCO doc and 0.6 KB per siftBigANN doc).
- The collections read the same MSMARCO records, so the page cache serves at least 98% of the MSMARCO file reads. The NFS share itself reads only 95–145 MB/s.
- KV stayed under 45% CPU. DocLoader's GC took 3.5–5% of the time at pc 1, with Java 8's default 8.4 GB heap.

**Initial load time.** These figures come from a model of the pool above,
using the measured limits:

| Template and pcs | 60 threads | 120 threads | 200 threads |
|---|---|---|---|
| 1B, pc 20 everywhere (the `pc` param's default) | 23.4 h | 12.3 h | 7.8 h |
| 1B, pc 10 everywhere | 12.2 h | 6.7 h | 4.5 h |
| 1B, shipped | 3.1 h | 2.65 h | 2.57 h |
| 500M, pc 20 everywhere | 6.1 h | 3.3 h | 2.1 h |
| 500M, pc 10 everywhere | 3.2 h | 1.8 h | 1.35 h |
| 500M, shipped | 1.05 h | 1.01 h | 1.01 h |

With the shipped values, 200 threads (1B) and 120 (500M) reach the floor.
Until MSMARCO finishes (about 1.9 h into 1B), the runner's throughput sets
it. After that, 1B's siftBigANN [200M, 500M) range runs alone at its 50,000/s
for the last ~40 minutes. Only a higher `ops` for that bucket would shorten
that; pc wouldn't. A siftBigANN pc of 5 or 8 is never faster than 4. A pc of 2
leaves each range at 44.7k/s, and an MSMARCO pc of 2 only adds tasks.

**Mutations.** Mutations run at a few docs/s per worker, so threads limit
them, not throughput. Queued tasks wait behind running ones that take days.
At the start of a 1B mutation wave with 60 threads, at pc 10:
- GleamBookUsers0 runs at 10% of its rate;
- GleamBookUsers1 and the siftBigANN bucket run at 0%. The siftBigANN bucket waits behind every MSMARCO task.

At pc 1 with ≥ 159 threads, every bucket runs at 100%. A pc above 1 only
multiplies the tasks, and `rate // pc` rounds rates down: a GleamBookUsers1
range does 30 docs/s instead of 33 at pc 10.

**Set `threads_to_use`** to at least the larger of the two task counts setUp
logs:

| Template | Initial-load tasks | Tasks per mutation wave | ClusterOpsVolume, SystemTestIndexer | SteadyStateVolume |
|---|---|---|---|---|
| `sparse_dense_1B` | 186 | 159 | 200 | 340 |
| `sparse_dense_500M` | 107 | 83 | 120 | 170 |
| `sparse_bhive_44M` | 30 | 15 | 60 (default) | 60 (default) |

- SteadyStateVolume runs its own wave alongside the mutation thread's, so it needs twice the mutation tasks.
- A wave's siftBigANN tasks count as done after 600s (see `stall_timeout=-1` above), so the next wave can start while the previous wave's [200M, 500M) range is still running. That's one extra task in 1B, which the margins above cover.
- Mutation workers sleep most of each second, so the extra threads cost little.

### What happens in each phase

| Phase | Function(s) | Template behaviour |
|---|---|---|
| Param read | `Murphy.setUp` | `load_workload_template()` resolves, evaluates and validates the template (fails setUp, listing the problems); `build_load_defns()` builds the per-bucket dicts; `num_buckets = len(load_defn)` |
| Buckets | `OPD.create_required_buckets` (unchanged) | Each loadDefn carries `"name"`, so the existing rename hook names the bucket after the template key. RAM is split evenly; `bucket_type` and `compression_mode` are cycled by index as before |
| Collections | `Murphy.create_required_collections`, which calls `create_template_collections` | Created in `_default`, one thread per bucket, named by `collection_id`, in sorted order |
| skip_init | `bind_template_to_existing_buckets` | Binds the server's buckets to template workloads **by name**; fails unless the bucket sets match exactly. `initial_setup` then returns early, as in the val_type path |
| FTS/analytics check | end of `Murphy.setUp` | Fails if the cluster has FTS or analytics nodes |
| Initial load | `initial_setup`, which calls `template_initial_load` | One DocLoader request per collection, create 100, `num_items`, at the collection's `ops` share and `process_concurrency`. Waits for all tasks; fails if DocLoader refuses any |
| Item bookkeeping | `record_template_collection_items` | Sets each collection's own `num_items` / `doc_index` |
| Indexes | `DoctorN1QL.create_indexes`, `build_indexes` | Definition looked up by collection name; the template's `similarity` is used; keyspaces escaped |
| Query load | `QueryLoad`, one per bucket with `2iQPS > 0` | siftBigANN: real vectors from `<baseFilePath>/bigann_query.bvecs`, recall only with `groundTruths`. MSMARCO: random sparse `$vector` for `SPARSE_VECTOR_DISTANCE` queries; scalar queries get no vector |
| Mutation thread | `vector_file_mutations`, which calls `template_vector_mutations` | Waves of every bucket's `doc_op_percentages` at the `rebl_ops_rate` share and `rebl_process_concurrency`, with `mutate += 1` per wave so vectors change. Same pause handshake; `self.loader_tasks` holds the in-flight wave; every task is awaited; refused tasks are logged as critical |
| SteadyStateVolume iteration | `template_steady_state_wave` | One synchronous full pass at the `rebl_ops_rate` share; the mutation thread keeps running alongside |
| Rollback (`rollback=True`) | `OPD.trigger_rollback`, which calls `template_rollback_load` | 1000 create-only docs per collection under `rollback_docs_<n>-`, at the `ops` share and `process_concurrency` |
| print_stats | `OPD.print_stats` | Crud table skipped; those attributes only exist after `generate_docs` |
| ClusterOpsVolume end | end guard | Skips the val_type-only `doc_loading_tm.getAllTaskResult()` |
| Backup / restore / PITR | `restore_with_workloads_paused` and the backup helpers | Unchanged; they work per bucket name |

Apart from ClusterOpsVolume's end guard, ClusterOpsVolume and
SystemTestIndexer contain no template code. They inherit the behaviour through
`initial_setup` and the mutation thread.

### Where template mode branches

All template code is reached through these guards. Without the param, every
guarded block runs exactly the old code.

- `Hospital.py`:
  - `setUp`: `if self.workload_template:` loads the template, and the `skip_init` path uses `elif self.workload_template:`;
  - `setUp`: the FTS/analytics check;
  - the `create_required_collections` override;
  - `initial_setup`: the load branch, the per-collection items loop, and the mutation-thread choice (`self.workload_template or self.val_type in (...)`);
  - `SteadyStateVolume` loop;
  - top of `vector_file_mutations`;
  - `ClusterOpsVolume` end guard (`not self.workload_template and ...`).
- `opd.py`: `print_stats` and `trigger_rollback` use `getattr(self, "workload_template", None)`. OPD's other subclasses don't have the attribute.
- `n1ql.py` checks per bucket with `b.loadDefn.get(TEMPLATE_KEY)` (`TEMPLATE_KEY = "workload_template"`, set by `build_load_defns`):
  - name lookup and similarity in `create_indexes`;
  - `QueryLoad._sift_base_path()`.

These `n1ql.py` changes apply to **both** paths, but don't change val_type
results:
- `n1ql_identifier()` leaves plain identifiers unchanged;
- ground truth is optional (`_gt_label`, `groudtruth is None`);
- a mock `$vector` is attached only when the query text contains `$vector`;
- `read_query_vecs` / `read_gt_vecs` open their files with `'rb'`.

### Validation (setUp fails, listing the problems found)

The template's own problems are reported first. Params are checked only once
the template passes.

**From `template_errors()`** (pure; also usable offline, see below):
- not a non-empty dict;
- a bucket name not matching `^[A-Za-z][A-Za-z0-9_-]{0,99}$`;
- an unsupported or mismatched `valType`;
- missing `key_prefix`, `key_type` or `key_size`;
- `scopes != 1`;
- `doc_op_percentages` without an int for each op;
- a missing `vecFilePath` / `baseFilePath`;
- a bad `2iQPS`;
- an empty `collections_defn`, or a `collections` mismatch;
- an invalid or duplicate `collection_id`;
- `num_items` out of range;
- a bad `2i`, missing statements, or a bad `vector`;
- a `groundTruths` shorter than the queries, or `queryParams` that aren't a list of dicts;
- a missing rate, a non-int rate, or a rate **below its pc**;
- a `process_concurrency` / `rebl_process_concurrency` that isn't an int ≥ 1;
- a siftBigANN pc that doesn't divide every step range of the collection (DocLoader would skip the remainder);
- `2iQPS > 0` with no queries;
- siftBigANN buckets with different `baseFilePath`s.

**From `_template_param_errors()`**, checked only when the template itself is valid:
- `fts_nodes` or `cbas_nodes` set;
- `collection_id` set;
- `coll_order=-1`;
- `base64` / `bhive` without the matching statements (unless `combinational`);
- dense mock `$vector` queries in non-sift collections without `mockVector=True`.

### Test params in template mode

| Status | Params |
|---|---|
| **Must not be passed** (setUp rejects them) | `bhive=true` / `base64=true` (the shipped templates have no BHIVE/base64 statement lists; their statements already say which indexes are BHIVE), `fts_nodes`/`cbas_nodes` > 0, `collection_id`, `coll_order=-1` |
| **Ignored** (the template defines these) | `val_type`, `num_buckets`, `bucket_names`, `collections`, `num_items`, `doc_size`, `key_size`, `ops_rate`, `rebl_ops_rate`, `2iQPS`, `2i_indexes`, `2i_queries`, `nProbe`, `dim`, `vecFilePath`, `baseFilePath`, `similarity` (used only when a template vector has no `similarity`) |
| **Still honoured** | `pc` (DocLoader workers for any load whose template sets no pc; the 500M/1B templates set them all), `threads_to_use` (DocLoader's thread pool; size it with [Process concurrency](#process-concurrency)), `partitions` (adds `num_partition` to dense `{vector}` indexes; without it `PARTITION BY HASH(meta().id)` is stripped from every index of a collection, as before. `PARTITION BY HASH(<field>)` is kept), `mockVector`, `index_replica` and the indexer settings, bucket storage/type/eviction, backup / continuous-backup / encryption params, `iterations`, sleeps, `rebl_services`, `skip_init`, `rollback`, `crashes` |

### Shipped templates

All four use `ops: 50000` and `rebl_ops_rate: 1000` per bucket. `2iQPS` per
bucket: 100 in `sparse_dense_500M` / `sparse_dense_1B`, 10 / 10 / 50 in
`single_template`, 10 in `sparse_bhive_44M`. The MSMARCO scalar queries end in
`LIMIT 100`. The siftBigANN bucket has no `groundTruths`. For the pcs, see
[Process concurrency](#process-concurrency).

| Template | GleamBookUsers0 (MSMARCO sparse) | GleamBookUsers1 (MSMARCO sparse) | GleamBookUsers2 (siftBigANN dense) | Items |
|---|---|---|---|---|
| `single_template` | `8M` (7 indexes/7 queries) | `8M` (7/7) | `1M` (4/4) | 18,683,646 |
| `sparse_dense_500M` | `GB0_C01..GB0_C10` (4/4 each) | `GB1_C01..GB1_C15` (4/4) | `200M` (8/8) | 421,045,575 |
| `sparse_dense_1B` | `GB0_C01..GB0_C20` (3/3) | `GB1_C01..GB1_C30` (4/4) | `500M` (5/5) | 942,091,150 |
| `sparse_bhive_44M` | `GB0_C01..GB0_C05` (5/5) | — | — | 44,209,115 |

`sparse_bhive_44M`: every collection has the same 4 BHIVE sparse vector
indexes on `sparse` (plain, and `INCLUDE (size)`, `(size, color)`,
`(size, country)`) plus a scalar index on `size`, each with its query.

The MSMARCO collections' indexes and matching queries were distributed round
robin from the bucket's original lists: 4 sparse vector pairs and 3 scalar
pairs. Collection `k` (0-based) gets scalar pairs `2k`, `2k+1` mod 3, and:
- in `sparse_dense_1B`'s GleamBookUsers0, one sparse vector pair, `k` mod 4, so each of the 4 is in 5 collections;
- in every other MSMARCO bucket of the 500M/1B templates, two sparse vector pairs, `2k`, `2k+1` mod 4.

In GleamBookUsers1 the `(sparse)` index pairs with the unfiltered query, and
`(size, sparse)` with `size = 5`.

### Runtime environment

- **SIFT data:**
  - The templates point `baseFilePath` at `/mnt/nfsdata/bigann` on the NFS share. It holds `bigann_base.bvecs`, `bigann_query.bvecs` and `idx_{1,2,5,10,20,50,100,200,500,1000}M.ivecs`.
  - The share **squashes root**, so root can read but not write. Any code that opens these files must use `'rb'`, not `'+rb'`. The old `'+rb'` caused `PermissionError` and killed the siftBigANN query thread.
  - If `bigann_base.bvecs` is missing, DocLoader downloads `bigann_base.bvecs.gz` from ftp.irisa.fr (`FileDownload.checkDownload`). That is a very large download.
- **MSMARCO data:** `vecFilePath` (`/mnt/nfsdata/collection.vecs`) must exist on the DocLoader host.
- **DocLoader:** `--launch_java_doc_loader` starts it on the testrunner host with `load_docs_using=sirius_java_sdk`, so "DocLoader host" and "testrunner host" are usually the same machine.
- **cbcontbk restores:**
  - `opd.py` sets `cbcontbk_cmd_timeout = 86400`.
  - That only reaches the SSH read when the checkout includes the `CbContBk` timeout propagation (commit `cef174441`: `cbcontbk.py` passes `timeout=self.cmd_timeout` to `CbCmdBase._execute_cmd`). Without it, `execute_command` falls back to its default of **600s**.
  - A user reached over sudo takes the channel branch of `execute_command_raw`, which hard-codes 900s.

---

## Invariants — do not break

1. **The val_type path stays byte-identical.** Put template behaviour behind `self.workload_template`, `getattr(self, "workload_template", None)` (in `opd.py`) or `loadDefn.get(TEMPLATE_KEY)` (in `n1ql.py`). `OPD` and `n1ql.py` are shared with hostedHospital, fusion and goldfish.
2. **Never pass `buckets=[]`** to `JavaDocLoaderUtils.load_sift_data` / `load_msmarco_data` / `perform_load`. They do `buckets or cluster.buckets`, so an empty list loads **every** bucket.
3. **Don't route template buckets through the `JavaDocLoaderUtils` loaders.**
   - Those loaders map collections to definitions by sorted position, and `_loader_dict` by insertion order, with `_default` taking index 0.
   - They give every collection the bucket's full `ops`.
   - The template path looks definitions up by name and uses per-collection rates.
4. **Keep `_template_loader` in sync** with `load_sift_data` / `load_msmarco_data`. The intended differences are only per-collection `ops` and `process_concurrency`, name lookup, cluster credentials, and `never_started_timeout=-1`. If the framework loaders gain or change arguments, mirror them.
5. **`n1ql_identifier()` must leave plain identifiers unchanged**, so val_type statement text doesn't change. Use it for every keyspace part in any new N1QL built from bucket/scope/collection names.
6. **`QueryLoad.queryVectors` is a process-wide class cache**, which is why all siftBigANN buckets must share one `baseFilePath`.
7. **Vector dataset files are opened read-only** (`'rb'`).
8. **Template loads and mutations never overwrite `loadDefn["ops"]`.** The val_type `vector_file_mutations` does, which is fine there.
9. **Query stats belong to the whole run.** Every query loader (`n1ql.QueryLoad`, `fts.FTSQueryLoad`, `cbas.CBASQueryLoad`, goldfish's `CBASQueryLoad`) builds its stats once, in `__init__`:
   - `n1ql.QueryLoad`: `query_stats` plus the `itertools.count` totals;
   - the other loaders: their counters, and `query_stats` where they have one.

   `start_query_load()` must only restart worker threads. `restart_query_load()` and `resume_query_workload()` rely on that, and the per-query `Count` in "N1QL Performance Stats" only adds up to "N1QL Results Stats"' `Success` while it holds. `Total` and `Success` differ only by the queries in flight.

---

## Gotchas and known issues on the val_type path

These exist at HEAD of the val_type path and are not caused by template mode.
Don't mistake them for template bugs.

- **SteadyStateVolume** (val_type) calls `perform_load(validate_data=True)`, which calls `JavaDocLoaderUtils.data_validation`, which doesn't exist. Every val_type SteadyStateVolume run hits `AttributeError` once a load completes.
- **The ClusterOpsVolume end guard** calls `self.doc_loading_tm.getAllTaskResult()`, which doesn't exist. val_type runs with `track_failures=True` raise `AttributeError` there. Template mode skips it.
- **The old mutation loops** remove tasks from the list they are iterating, so they await only every other task.
- **`restart_query_load`** increments `2iQPS` on the shared val_type dict once per bucket.
- **`build_indexes`** uses the scope of the last index; with one scope this doesn't matter.
- **`drop_indexes`** builds `DROP INDEX x on \`<collection>\``, without the bucket and scope.
- **Positional mapping differs between consumers.**
  - `load_sift_data`, `load_msmarco_data` and `create_indexes` use sorted names without `_default`.
  - `_loader_dict` and `data_validation` use insertion order, with `_default` at index 0.
- **QueryLoad threads are closed-loop.** `2iQPS` is a thread count, not a rate.
- **MSMARCO collections:** `QueryLoad` sets `validate_item_count=False`, so there's no result-count check.

---

## Maintenance rules

### Adding or changing a template

1. Copy an existing template.
2. Keep `scopes: 1`.
3. Set `vecFilePath` / `baseFilePath` for the dataset.
4. Give every collection a unique, valid `collection_id`.
5. Size `num_items` within the dataset limits.
6. Set `ops` / `rebl_ops_rate`. Add collection-level values to override individual shares.
7. Set `process_concurrency` / `rebl_process_concurrency` as in [Process concurrency](#process-concurrency):
   - the fewest workers for which one worker sustains `rate // pc` (MSMARCO: ~9.5k/s; siftBigANN: ≤ 16,384/s);
   - a siftBigANN pc that divides every step range;
   - every rate ≥ its pc.

   Then size `threads_to_use` from the task counts the validation below prints.
8. Keep `2i` counts consistent with the `indexes` / `queries` lists.
9. Validate offline, without a cluster or TAF dependencies, from the repo root:

```bash
python3 - <<'EOF'
import importlib.util, sys, types
# Stub the framework imports; validation needs none of them
for name, attrs in {
        "bucket_utils": {}, "bucket_utils.bucket_ready_functions": {"CollectionUtils": object, "JavaDocLoaderUtils": object},
        "Jython_tasks": {}, "Jython_tasks.java_loader_tasks": {"SiriusCouchbaseLoader": object},
        "py_constants": {}, "py_constants.cb_constants": {},
        "py_constants.cb_constants.CBServer": {"CbServer": object}}.items():
    sys.modules[name] = types.ModuleType(name)
    sys.modules[name].__dict__.update(attrs)
spec = importlib.util.spec_from_file_location("wt", "pytests/aGoodDoctor/workload_template.py")
wt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wt)

PC = 20  # the run's pc param
for name in ("single_template", "sparse_dense_500M", "sparse_dense_1B", "sparse_bhive_44M"):
    template = wt.load_template(wt.resolve_template_path(name))
    errors = wt.template_errors(template, PC)
    print(f"{name}: {'OK' if not errors else errors}")
    if not errors:
        tasks = {wt.INITIAL_RATE: 0, wt.MUTATION_RATE: 0}
        for bucket, workload in template.items():
            defns = workload["collections_defn"]
            for rate_key in tasks:
                tasks[rate_key] += sum(
                    wt.docloader_tasks(workload["valType"], d["num_items"], wt.collection_pc(workload, d, rate_key, PC))
                    for d in defns)
            print(f"  {bucket}: {workload['valType']}, {len(defns)} collections, "
                  f"ops/collection {sorted({wt.collection_rate(workload, d, wt.INITIAL_RATE) for d in defns})}, "
                  f"rebl_ops_rate/collection {sorted({wt.collection_rate(workload, d, wt.MUTATION_RATE) for d in defns})}, "
                  f"pc {sorted({wt.collection_pc(workload, d, wt.INITIAL_RATE, PC) for d in defns})} / "
                  f"{sorted({wt.collection_pc(workload, d, wt.MUTATION_RATE, PC) for d in defns})}")
        print(f"  DocLoader tasks: initial load {tasks[wt.INITIAL_RATE]}, mutation wave {tasks[wt.MUTATION_RATE]}")
EOF
```

This checks only the template itself. The param checks (`_template_param_errors`)
and the FTS/analytics node check run at setUp.

### Changing template-mode code

- **Read first:** `workload_template.py` and the guards listed under "Where template mode branches".
- **Supporting another valType:**
  - add it to `MAX_ITEMS` and the `*_VAL_TYPES` tuples;
  - give `_template_loader` its DocLoader arguments;
  - check `QueryLoad`'s per-valType behaviour (`n1ql.py`).
- **Supporting more than one scope:** `create_template_collections`, `record_template_collection_items` and `submit_template_loads` all assume `_default`. The `C<n>_Q<q>` identifiers in `create_indexes` and `build_indexes`' grouping by collection name would also collide across scopes.
- **Shared code (`opd.py`, `n1ql.py`):** prove the val_type path is unchanged, e.g. compare `DoctorN1QL.create_indexes` output before and after on the `workloads.py` dicts with the same `random.seed`. It sleeps 1s per bucket; stub `time.sleep` to keep the comparison fast.
- **Importing `Hospital.py` / `n1ql.py` offline:**
  - use the repo's Python 3.10 env;
  - set `sys.path` like `testrunner.py`;
  - set `TestInputSingleton.input` to a `TestInput()` whose `test_params` is a dict, because `workloads.py` reads params at import time;
  - stub any missing cloud SDK modules (`azure.*`, `google.cloud.kms`).
