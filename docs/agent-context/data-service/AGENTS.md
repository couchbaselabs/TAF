# AGENT_CONTEXT: DATA_SERVICE (KV-ENGINE)

## 1. ARCHITECTURE & CORE LOGIC
- Logic: Operations routed via vBucket (CRC hashing). 1024 vBuckets per cluster.
- Engines: [Magma] (High-perf, Write-optimized, CDC support) | [Couchstore] (Legacy/Standard).
- Buckets:
  - [Couchbase]: Persistent (Disk+RAM). Supports DCP/XDCR. Evicts to disk under RAM pressure.
  - [Ephemeral]: Volatile (RAM only). No disk jitter. Faster rebalance. RAM Full: [No-ejection] or [NRU].

## 2. TEST DIRECTORY & SPEC MAP
graph TD
    DS[Data Service Testing] --> CORE[Core KV Engine]
    DS --> ADV[Advanced Scenarios]
    DS --> SPEC[Ops Specs]

    CORE -->|pytests/epengine/| E1[CRUD, Durability, TTL, CAS]
    CORE -->|pytests/castest/| E2[CAS Healing/Failover]

    ADV -->|pytests/bucket_collections/| A1[Scopes, Lifecycle, Network Split]
    ADV -->|pytests/storage/magma/| A2[Magma DGM, Quota Stress]

    SPEC -->|collection_ops_specs/| S1[Initial Load, Sync Write, Drop/Recreate]

    style DS fill:#f9f,stroke:#333,stroke-width:2px
    style CORE fill:#bbf,stroke:#333
    style ADV fill:#dfd,stroke:#333
    style SPEC fill:#ffd,stroke:#333


## 3. PROGRAMMATIC INTERFACES (PYTHON)

### A. Memcached Client (lib/mc_bin_client.py)
```python
client = MemcachedClient(host, port)
client.sasl_auth_plain(user, pwd)
client.bucket_select(name)
# Ops: set(k,v,cas,expiry), get(k), delete(k), incr/decr, get_meta(k), set_with_meta(...)

### B. VBucket-Aware Helper (lib/memcached/helper/data_helper.py)
```python
from memcached.helper.data_helper import VBucketAwareMemcached
obj = VBucketAwareMemcached(rest, bucket)
v_map = obj.request_map(rest, bucket) # Map keys/ops to specific nodes
```

### C. SDK Client (sdk_client3.py)
```python
sdk = SDKClient(bucket, scope, collection, user, pwd)
sdk.upsert(key, value); sdk.read_multi(keys); sdk.remove(key)
```

## 4. CRITICAL CONSTANTS & PARAMS
- Durability: none, majority, majorityAndPersistOnMaster, persistToMajority.
- Eviction: fullEviction (Key+Meta+Value) | valueOnly (Value only).
- Bucket Types: couchbase, ephemeral, memcached (legacy).
- Common Params: nodes_init, num_items, doc_size, vbuckets=1024, load_ratio.

## 5. OPERATIONAL MONITORING (STATS)
- Tool: couchbase_utils/cb_tools/cbstats.py
- Health: wait_for_warmup, wait_for_vbuckets_ready_state.
- DGM Check: If ep_curr_size > ep_ram_size (Items exceed RAM quota).
- Durability Health: Check ep_syncwr_failed and vbucket-durability-state.

## 6. KV ERROR CODES (memcacheConstants)
- ERR_NOT_FOUND (0x01): Key missing.
- ERR_EXISTS (0x02): CAS mismatch/Key exists.
- ERR_NOT_MY_VBUCKET (0x07): Routing error.
- ERR_E2BIG (0x03): Item > 20MB.
- ERR_TMPFAIL (0x86): Engine busy/Warmup.

## RECURRING TEST PATTERNS
- DGM Stress: doc_size * num_items >> ram_quota.
- Rollback: Force process crash during persistence to validate seqno rollback.
- Warmup Validation: Restart memcached and monitor wait_for_warmup before CRUD.

## VOLUME TESTING
For data-service/KV engine volume testing (on-premise and cloud-based stress scenarios), refer to [volume_test.md](volume_test.md).
