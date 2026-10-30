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
| `templates/*.py` | dict literals | The templates: `single_template`, `sparse_dense_500M`, `sparse_dense_1B` |
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
python testrunner.py -i <ini> -t aGoodDoctor.Hospital.Murphy.ClusterOpsVolume,workload_template=sparse_dense_500M,iterations=2,pc=20,load_docs_using=sirius_java_sdk,<cluster/backup/encryption params> --launch_java_doc_loader --sirius_url http://localhost:<port>
```

setUp logs `Using workload template <path>` and one summary line per bucket:
valType, collection count, items, per-collection initial and mutation rates,
and 2iQPS. Grep the test log for these.

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
| `2iQPS` | no (default 0) | Number of query threads for this bucket. Each thread cycles through **all** the bucket's queries. Needs ≥ 1 query when > 0 |
| `collections` | no | If given, must equal `len(collections_defn)`. It is overwritten with that value |
| `collections_defn` | yes | Non-empty list of collection definitions (below) |
| `num_items`, `doc_size`, `process_concurrency`, `ftsQPS`, `cbasQPS` | no | Not used by template loads. Workers come from the `pc` param; FTS/CBAS are rejected |
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
| Used by | `template_initial_load`, `template_rollback_load` | mutation thread (`template_vector_mutations`), `template_steady_state_wave` |

The rule is `collection_rate()`:
- A collection-level value wins.
- Otherwise each collection gets `bucket_value // len(collections_defn)`.

Every resolved rate must be **≥ the `pc` param**. DocLoader paces each of its
`pc` workers at `ops // pc` docs/s and **loads nothing when that is 0**; see
`boundedBatchSize` in `DocLoader/src/main/java/utils/docgen/WorkLoadSettings.java`.

The `ops_rate` and `rebl_ops_rate` **test params are not used** in template
mode.

### What happens in each phase

| Phase | Function(s) | Template behaviour |
|---|---|---|
| Param read | `Murphy.setUp` | `load_workload_template()` resolves, evaluates and validates the template (fails setUp, listing the problems); `build_load_defns()` builds the per-bucket dicts; `num_buckets = len(load_defn)` |
| Buckets | `OPD.create_required_buckets` (unchanged) | Each loadDefn carries `"name"`, so the existing rename hook names the bucket after the template key. RAM is split evenly; `bucket_type` and `compression_mode` are cycled by index as before |
| Collections | `Murphy.create_required_collections`, which calls `create_template_collections` | Created in `_default`, one thread per bucket, named by `collection_id`, in sorted order |
| skip_init | `bind_template_to_existing_buckets` | Binds the server's buckets to template workloads **by name**; fails unless the bucket sets match exactly. `initial_setup` then returns early, as in the val_type path |
| FTS/analytics check | end of `Murphy.setUp` | Fails if the cluster has FTS or analytics nodes |
| Initial load | `initial_setup`, which calls `template_initial_load` | One DocLoader task per collection, create 100, `num_items`, at the collection's `ops` share. Waits for all tasks; fails if DocLoader refuses any |
| Item bookkeeping | `record_template_collection_items` | Sets each collection's own `num_items` / `doc_index` |
| Indexes | `DoctorN1QL.create_indexes`, `build_indexes` | Definition looked up by collection name; the template's `similarity` is used; keyspaces escaped |
| Query load | `QueryLoad`, one per bucket with `2iQPS > 0` | siftBigANN: real vectors from `<baseFilePath>/bigann_query.bvecs`, recall only with `groundTruths`. MSMARCO: random sparse `$vector` for `SPARSE_VECTOR_DISTANCE` queries; scalar queries get no vector |
| Mutation thread | `vector_file_mutations`, which calls `template_vector_mutations` | Waves of every bucket's `doc_op_percentages` at the `rebl_ops_rate` share, with `mutate += 1` per wave so vectors change. Same pause handshake; `self.loader_tasks` holds the in-flight wave; every task is awaited; refused tasks are logged as critical |
| SteadyStateVolume iteration | `template_steady_state_wave` | One synchronous full pass at the `rebl_ops_rate` share; the mutation thread keeps running alongside |
| Rollback (`rollback=True`) | `OPD.trigger_rollback`, which calls `template_rollback_load` | 1000 create-only docs per collection under `rollback_docs_<n>-`, at the `ops` share |
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
- a missing rate, a non-int rate, or a rate **below `pc`**;
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
| **Still honoured** | `pc` (DocLoader workers; the rate floor), `partitions` (adds `num_partition` to dense `{vector}` indexes; without it `PARTITION BY HASH(meta().id)` is stripped from every index of a collection, as before. `PARTITION BY HASH(<field>)` is kept), `mockVector`, `index_replica` and the indexer settings, bucket storage/type/eviction, backup / continuous-backup / encryption params, `iterations`, sleeps, `rebl_services`, `skip_init`, `rollback`, `crashes` |

### Shipped templates

All three use `ops: 50000` and `rebl_ops_rate: 10000` per bucket, and `2iQPS`
10 / 10 / 50. The MSMARCO scalar queries end in `LIMIT 100`. The siftBigANN
bucket has no `groundTruths`.

| Template | GleamBookUsers0 (MSMARCO sparse) | GleamBookUsers1 (MSMARCO sparse) | GleamBookUsers2 (siftBigANN dense) | Items |
|---|---|---|---|---|
| `single_template` | `8M` (7 indexes/7 queries) | `8M` (7/7) | `1M` (4/4) | 18,683,646 |
| `sparse_dense_500M` | `GB0_C01..GB0_C10` (4/4 each) | `GB1_C01..GB1_C15` (4/4) | `200M` (8/8) | 421,045,575 |
| `sparse_dense_1B` | `GB0_C01..GB0_C20` (4/4) | `GB1_C01..GB1_C30` (4/4) | `500M` (5/5) | 942,091,150 |

In the 500M/1B templates, each MSMARCO collection has 2 sparse vector and 2
scalar indexes with their matching queries. These were distributed round
robin from the bucket's original lists. Collection `k` (0-based) gets vector
pairs `2k`, `2k+1` mod 4 and scalar pairs `2k`, `2k+1` mod 3. In
GleamBookUsers1 the `(sparse)` index pairs with the unfiltered query, and
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
4. **Keep `_template_loader` in sync** with `load_sift_data` / `load_msmarco_data`. The intended differences are only per-collection `ops`, name lookup, and cluster credentials. If the framework loaders gain or change arguments, mirror them.
5. **`n1ql_identifier()` must leave plain identifiers unchanged**, so val_type statement text doesn't change. Use it for every keyspace part in any new N1QL built from bucket/scope/collection names.
6. **`QueryLoad.queryVectors` is a process-wide class cache**, which is why all siftBigANN buckets must share one `baseFilePath`.
7. **Vector dataset files are opened read-only** (`'rb'`).
8. **Template loads and mutations never overwrite `loadDefn["ops"]`.** The val_type `vector_file_mutations` does, which is fine there.

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
6. Set `ops` / `rebl_ops_rate` so every collection's share is ≥ `pc`. Add collection-level values to override individual shares.
7. Keep `2i` counts consistent with the `indexes` / `queries` lists.
8. Validate offline, without a cluster or TAF dependencies, from the repo root:

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
for name in ("single_template", "sparse_dense_500M", "sparse_dense_1B"):
    template = wt.load_template(wt.resolve_template_path(name))
    errors = wt.template_errors(template, PC)
    print(f"{name}: {'OK' if not errors else errors}")
    if not errors:
        for bucket, workload in template.items():
            defns = workload["collections_defn"]
            print(f"  {bucket}: {workload['valType']}, {len(defns)} collections, "
                  f"ops/collection {sorted({wt.collection_rate(workload, d, wt.INITIAL_RATE) for d in defns})}, "
                  f"rebl_ops_rate/collection {sorted({wt.collection_rate(workload, d, wt.MUTATION_RATE) for d in defns})}")
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
