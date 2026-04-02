# Volume Testing for Data Service (KV Engine)

**Purpose**: Validate KV engine robustness under sustained high-load across on-prem/cloud via Murphy() classes.

## Key Coverage

**Data Service/KV Engine Volume Scenarios**:
- Murphy() class implementations for sustained high-load validation
- Multi-service integration (N1QL, FTS, CBAS, Eventing, Backup, XDCR)
- On-premise crash injection and failover/recovery scenarios
- Cloud scaling operations (horizontal/vertical/disk/compute)
- Mutation workload patterns and query load integration (2iQPS, ftsQPS, cbasQPS)
- Data integrity validation and statistical monitoring

**For all data-service/KV engine volume testing related queries, refer to this document.**

## Architecture

```
pytests/aGoodDoctor/
├── Hospital.py              → Murphy(BaseTestCase, OPD)
└── hostedHospital.py        → Murphy(BaseTestCase, hostedOPD)
```

**Inheritance**: On-prem (direct cluster mgmt) | Cloud (multi-tenant pod orchestration)

## Core Patterns

### Data Loading
- **Loader**: `JavaDocLoaderUtils` from `bucket_utils/bucket_ready_functions.py`
- **Workloads**: default, Hotel-vector, siftBigANN-vector, Nimbus
- **Ops**: create:update:delete:read:expiry (configurable via `doc_ops`)
- **Threading**: Background mutations via `threading.Thread(target=self.normal_mutations)`
- **Vector Loaders**: siftBigANN (real ANN vectors from `/data/bigann`, configurable dimensions)

### Service Integration
| Service  | Doctor Class               | Pattern                     |
|----------|--------------------------|----------------------------|
| KV Engine| OPD / hostedOPD          | CRUD ops via load_defn     |
| N1QL     | DoctorN1QL               | Index + QueryLoad (2iQPS)  |
| FTS      | DoctorFTS                | FTS index + QueryLoad (ftsQPS) |
| CBAS     | DoctorCBAS               | Dataset + ingestion (cbasQPS)|
| Eventing | DoctorEventing           | Function deployment        |
| Backup   | DoctorBKRS / DoctorHostedBackupRestore | Backup/restore     |
| XDCR     | DoctorXDCR               | Cross-cluster replication  |

## On-Premise (Hospital.py)

### Test Methods

| Method            | Purpose                          | Key Scenarios                                       |
|-------------------|----------------------------------|---------------------------------------------------|
| `SystemTestMagma` | Magma durability stress          | Rebalance+load, crash injection, failover/recovery, replica changes |
| `SystemTestIndexer`| Index durability stress           | Indexer process crash (num_kills=2), pending mutations check during load |
| `ClusterOpsVolume`| Cluster ops under load           | Service rebalance, failover+RebalanceOut, full recovery |
| `SteadyStateVolume`| Extended stability              | Expiry ops, backup/restore validation            |
| `test_rebalance`  | Multi-service rebalance stress    | fts/cbas/index/eventing: IN/OUT/SWAP              |

### Key Parameters
```bash
# Cluster
nodes_init=3 num_buckets=1-1000+ num_collections=1+ num_scopes=1+

# Data
num_items=1000000+ doc_size=variable(SimpleValue/Hotel/siftBigANN)
key_type=SimpleKey/RandomKey doc_ops=create:update:delete:read:expiry

# Service Nodes
kv_nodes=1+ cbas_nodes=0 fts_nodes=0 eventing_nodes=0 index_nodes=0
xdcr_remote_nodes=0

# Mutation
ops_rate=10000 mutation_perc=100 iterations=10 crashes=20

# Vector Workload (siftBigANN)
val_type=siftBigANN vector=true dim=128 similarity=L2_SQUARED nProbe=50
baseFilePath=/data/bigann collection_id=100M

# Index Configuration
partitions=16 bhive=true enableShardAffinity=false
enableInMemoryCompression=true 2iQPS=20 ftsQPS=10 cbasQPS=10
```

## Cloud (hostedHospital.py)

### Test Methods

| Method              | Purpose                           | Key Scenarios                                      |
|---------------------|-----------------------------------|---------------------------------------------------|
| `test_rebalance`    | Cloud scaling durability          | Horizontal (3→27), vertical (disk/compute), combined |
| `test_upgrades`     | Upgrade validation                | Capella upgrade API, post-upgrade integrity       |
| `test_cluster_on_off`| Availability stress             | Cluster stop/start via DoctorHostedOnOff          |

### Cloud-Specific Patterns
```python
# Multi-tenant iteration
for tenant in self.tenants:
    for cluster in tenant.clusters:
        JavaDocLoaderUtils.load_data(cluster=cluster, buckets=cluster.buckets)
        self.restart_query_load(cluster)

# Horizontal scaling
config = self.rebalance_config(service, rebl_nodes)

# Vertical scaling
self.disk[service] = new_size
self.compute[service] = new_tier
```

**Providers**: AWS/GCP/Azure (provider-specific compute/storage lists)

## Common Patterns

### Vector Workload Configuration
```bash
# Vector-specific parameters
val_type=siftBigANN vector=true dim=128
similarity=L2_SQUARED nProbe=50
baseFilePath=/data/bigann collection_id=100M
mockVector=false base64=false

# Index vector config
partitions=16 bhive=true enableShardAffinity=false
enableInMemoryCompression=true
```

### Mutations
```python
# Standard CRUD (background thread)
while self.mutations:
    bucket.loadDefn["ops"] = ops_rate  # e.g., 5000
    JavaDocLoaderUtils.generate_docs(bucket=bucket)
    JavaDocLoaderUtils.perform_load(cluster, buckets, wait_for_load=False)

# Vector (siftBigANN)
JavaDocLoaderUtils.load_sift_data(cluster, buckets,
    overRidePattern={"create": 0, "update": 100, "delete": 0})
```

### Query Load Integration
```python
# N1QL
self.drIndex.create_indexes(buckets, base64, xattr)
self.drIndex.build_indexes(cluster, buckets, wait=True)
if bucket.loadDefn.get("2iQPS", 0) > 0:
    QueryLoad(mockVector, bucket).start_query_load()

# FTS
self.drFTS.create_fts_indexes(cluster, dims)
self.drFTS.wait_for_fts_index_online(cluster, index_timeout)
if bucket.loadDefn.get("ftsQPS", 0) > 0:
    FTSQueryLoad(cluster, bucket, mockVector).start_query_load()
```

### Index Crash Testing (SystemTestIndexer)
```python
# Indexer process crash with mutation validation
self.initial_setup()  # Indexes + query load initialized
self.crash_indexer(num_kills=2, graceful=False)

# During crash: loads continue, pending mutations checked
self.kill_memcached(num_kills=num_kills, graceful=graceful, wait=True)
self.check_index_pending_mutations(self.cluster)
```

### Failover/Recovery
```python
# On-prem
self.rest.fail_over(node.id, graceful=True)
self.rest.set_recovery_type(node.id, recoveryType="full"/"delta")
self.rebalance(nodes_in=0, nodes_out=1)

# Cloud
DoctorHostedOnOff(pod, tenant, cluster).turn_off_cluster()
DoctorHostedOnOff(pod, tenant, cluster).turn_on_cluster()
```

## Validation & Monitoring

### Health Checks
- `wait_for_warmup` post-crash, 1024 vBuckets ready
- Index pending mutations, build completion
- Query data integrity validation

### Data Integrity
```python
# Active vs Replica (on-prem)
active, replica = self.bucket_util.get_and_compare_active_replica_data_set_all(kvs, buckets)

# Failure validation
if track_failures:
    tasks.extend(self.data_validation(cluster))
```

### Stats Monitored
- Cluster: CPU/RAM/disk/ops/sec
- Index: pending mutations, indexer threads
- Query: latency, success/failure
- XDCR: replication lag, doc count

## Execution

### Config
```ini
# node.ini (topology)
[clusters]
1:ip=10.0.0.1:port=8091:user=Admin:password=password

# conf/test.conf (params)
aGoodDoctor.Hospital.Murphy.SystemTestMagma,nodes_init=3,num_items=1000000,iterations=5
```

### Commands
```bash
# On-prem
python testrunner.py -i node.ini -c conf/sanity.conf -t aGoodDoctor.Hospital.Murphy.SystemTestMagma

# Cloud
python testrunner.py -i node.ini -c conf/capella.conf -t aGoodDoctor.hostedHospital.Murphy.test_rebalance

# Index crash test (vector workload with skip_init)
python testrunner.py -i node.ini -t aGoodDoctor.Hospital.Murphy.SystemTestIndexer \
  -p nodes_init=2,index_nodes=2,num_items=100000,val_type=siftBigANN \
  -p vector=true,dim=128,similarity=L2_SQUARED,similarity=L2_SQUARED,2iQPS=20 \
  -p skip_init=True,skip_cluster_reset=True,track_failures=False \
  -p bucket_storage=magma,doc_ops=expiry,ops_rate=120000
```

## Key Differences

| Aspect               | On-Premise              | Cloud                      |
|----------------------|------------------------|----------------------------|
| Cluster mgmt         | Direct orchestration    | Multi-tenant pod           |
| Scaling              | Rebalance IN/OUT        | H/V/Disk/Compute           |
| Backup               | Magma validation        | Capella-managed            |
| Upgrades             | Manual server           | Capella upgrade API        |
| Failure injection    | Process kill            | Cluster turn-off           |

## Best Practices

1. **Resource Planning**: `RAM = doc_size * num_items * replicas`, `disk = 2x expected`
2. **Mutation Rate**: Start conservative (5000), scale to infrastructure limits
3. **Duration**: Min 10 iterations, include warmup, factor rebuild times
4. **Service Isolation**: Test isolated before combined, monitor contention

## Troubleshooting

| Issue                 | Symptom                           | Solution                          | Monitor               |
|-----------------------|-----------------------------------|-----------------------------------|-----------------------|
| DGM                   | Latency spikes, eviction up      | Increase RAM or reduce dataset    | `ep_curr_size` vs `ep_ram_size` |
| Build timeouts        | Index/FTS build exceeds timeout  | Increase timeout/reduce complexity| `indexer.stats`       |
| Mutation throttling   | ops/sec < configured rate         | Check storage threads, CPU, I/O   | `storage_threads`, I/O stats |
| Failover failures     | Active/Replica dataset mismatch   | Verify replica consistency       | `vb_distribution_analysis` |

## References

| File                          | Purpose                             |
|-------------------------------|-------------------------------------|
| [Hospital.py](../../../pytests/aGoodDoctor/Hospital.py) | On-prem implementation               |
| [hostedHospital.py](../../../pytests/aGoodDoctor/hostedHospital.py) | Cloud implementation                 |
| [opd.py](../../../pytests/aGoodDoctor/opd.py) | On-prem workload generation         |
| [hostedOPD.py](../../../pytests/aGoodDoctor/hostedOPD.py) | Cloud workload generation           |
| [AGENTS.md](AGENTS.md) | Core data service context           |
