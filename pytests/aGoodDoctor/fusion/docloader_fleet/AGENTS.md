# docloader_fleet

Load data through an EC2 Auto Scaling Group of Java DocLoader REST servers
instead of the single DocLoader on the TAF host. Product-agnostic: works for
any cluster the instances can reach (Capella, on-prem with public endpoints).
Lives under `fusion/` for now; it has no fusion imports, so it can move.

↑ [agents/fusion.md](../../../../agents/fusion.md)

## Files

| File | Contents |
|---|---|
| `loader.py` | `FleetDataLoader` - test-facing facade: builds a fleet + dispatcher from `loader_fleet_*` params, `start()` (background launch), `load_initial_data(clusters)`, `load_chunks(chunks, clusters)`, `teardown()` (never raises) |
| `fleet.py` | `DocLoaderFleet` - ASG lifecycle (`launch()` -> DocLoader URLs, `teardown()`); `is_loader_online(url, server_name)` |
| `dispatcher.py` | `FleetLoadDispatcher` - chunk queue + per-JVM pull workers; `LoadChunk` (+ `LoadChunk.split`) |
| `bake.sh` | AMI recipe: JDK 17, DocLoader at a pinned commit, `docloader@<port>` systemd units |

## Using it from a test

```python
from aGoodDoctor.fusion.docloader_fleet import FleetDataLoader

# setUp / early setup: boot overlaps with cluster/bucket creation
self.fleet_loader = FleetDataLoader.from_test_input(
    self.input, self.log, region="us-east-1", boto3_session=session).start()
# after buckets/collections exist (bucket.loadDefn set, as hostedOPD does)
self.fleet_loader.load_initial_data(clusters)
# tearDown
self.fleet_loader.teardown()
```

Custom ranges: build a `SiriusCouchbaseLoader` template (bucket, scope,
collection, key/value settings) and run
`fleet_loader.load_chunks(LoadChunk.split(cluster, template, start, end, size), [cluster])`.
`cluster` needs `.master` (`ip`, `memcached_port`, `rest_username`,
`rest_password`) and `.buckets`.

## Parameters (`loader_fleet_*`)

| Param | Default | Meaning |
|---|---|---|
| `size` | caller's `size` (VolumeTest: one per cluster across tenants) | ASG instances |
| `launch_template` | none | Pre-baked template (`taf-docloader-fleet` in the `fusion` account, us-east-1). Without it the checkout's `DocLoader/target` is shipped via S3 and a template + SG are created per run |
| `jvms_per_instance` / `base_port` | 4 / 8080 | JVMs on ports base..base+N-1; heap 75%/N each |
| `instance_type` | c8g.8xlarge | Ignored with `launch_template` (template's type wins) |
| `subnet_ids` | default subnets offering the type | CSV; put loaders in the cluster's AZ(s) |
| `aws_profile`, `region` | caller's session / region | Account the fleet launches in |
| `ingress_cidrs` | TAF host public IP /32 | Who may reach DocLoader (it has no auth) |
| `ami`, `key_name`, `s3_bucket` | | Non-template mode only |
| `chunk_size` | auto | Items per chunk. Auto: equal chunks filling all slots in whole waves, each <= 100M (`auto_chunk_size`) |
| `chunks_per_jvm` | 1 | Concurrent chunks (slots) per JVM |
| `workers_per_jvm` | 40 | DocLoader threads per JVM (like the CLI `-workers 40`) |
| `process_concurrency` | `workers_per_jvm` / `chunks_per_jvm` | DocLoader threads per chunk; overrides the bucket's `pc` |
| `ops_per_chunk` | ops_rate split across concurrent chunks | Per-chunk rate cap |
| `chunk_retries` | 3 | Retries per chunk (upsert, so idempotent) |
| `teardown` | True | False keeps the fleet for debugging |

## Behaviour to know

- `load_initial_data` reuses `JavaDocLoaderUtils.generate_docs` + `_loader_dict`, so `val_type`, `doc_size`, `key_*`, `num_items`, `collections`, `ops_rate` mean what they do for the local loader (threads per chunk come from the fleet settings, not `pc`). `ops_rate` stays the bucket's total; it is divided across a collection's concurrently running chunks.
- Completion means every write was acked. On full-eviction (magma) buckets the item count trails acks until the disk queue drains - wait for stats before validating counts.
- The tail: once the queue empties, slots go idle while the last chunks finish. Auto sizing makes the chunk count a whole multiple of the slots so the last wave is full; a fixed `chunk_size` can leave a partial last wave.
- Teardown deletes the ASG, the per-run SG ingress rule or SG, the per-run launch template version or template, and the S3 artifact. Leftovers are tagged `taf-docloader-fleet=<run_id>`.

## VolumeTest

`loader_fleet=True` (or any `loader_fleet_size`) uses the fleet for the Step 2 initial load; size defaults to the number of clusters across all tenants. Typical: `loader_fleet=True,loader_fleet_launch_template=taf-docloader-fleet,loader_fleet_aws_profile=fusion`.

## Known measurements (fusion account, us-east-1, 10-node 8.5 magma cluster)

- 1 x c8g.8xlarge, 3 JVMs x 4 slots x 10 threads: 1B Hotel docs in 24.7 min (~675K/s), 0 failures. Cluster ~72% CPU, at memory high watermark.
- 1 x c8g.8xlarge, 4 JVMs x 1 slot x 40 threads: same load in 22.9 min (~727K/s); cluster disk queue reached ~37M, i.e. the cluster was the limit.
- c8g.8xlarge is not offered in us-east-1e and may lack capacity in us-east-1f.
