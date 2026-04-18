---
name: bucket-utils-agent
description: >
  Utilities for bucket operations including CRUD, scope/collection management,
  document loading, and bucket validation.
model: inherit
---

# bucket_utils

**Bucket management utilities for Couchbase Server.**
Comprehensive helpers for bucket operations, scope/collection management,
document loading, and bucket state validation.

## Files

| File | Purpose |
|---|---|
| `bucket_ready_functions.py` | BucketUtils class with bucket CRUD, scope/collection ops, doc loading |
| `thanosied.py` | Thanos-style bucket operations |

## Key Classes

### BucketUtils
Main utility class for bucket operations.

**Capabilities:**
- Bucket creation/deletion/update
- Scope and collection management
- Document loading (SDK, Sirius, DocLoader)
- Bucket stats and validation
- Compaction and flush operations

**Usage:**
```python
from couchbase_utils.bucket_utils.bucket_ready_functions import BucketUtils

bucket_util = BucketUtils(cluster, task_manager)
bucket_util.create_bucket(bucket_params)
bucket_util.create_scopes_collections(bucket, scope_coll_spec)
bucket_util.load_data(bucket, num_docs, ops_type)
```

## Dependencies

- `cb_server_rest_util.buckets.buckets_api.BucketRestApi`
- `BucketLib.BucketOperations.BucketHelper`
- `sdk_client3.SDKClient` for document operations
- `Jython_tasks` for async document loading
