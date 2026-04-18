---
name: index-utils-agent
description: >
  Utilities for GSI (Global Secondary Index) operations including index
  creation, deferred builds, and Plasma storage statistics.
model: inherit
---

# index_utils

**GSI index utilities for secondary index management.**
Provides helpers for creating and managing GSI indexes with deferred builds.

## Files

| File | Purpose |
|---|---|
| `index_ready_functions.py` | IndexUtils for GSI operations |
| `plasma_stats_util.py` | Plasma storage statistics |

## Key Classes

### IndexUtils
GSI index creation and management.

**Capabilities:**
- Create GSI indexes on collections
- Build deferred indexes
- Replica configuration
- Multiple indexes per collection

**Usage:**
```python
from couchbase_utils.index_utils.index_ready_functions import IndexUtils

index_util = IndexUtils(server_task)
index_util.create_gsi_on_each_collection(
    cluster=cluster,
    buckets=buckets,
    gsi_base_name="idx",
    replica=1,
    defer=True
)
index_util.build_deferred_indexes(cluster, indexes_to_build)
```

## Index Features

| Feature | Description |
|---|---|
| `defer` | Create indexes without immediate build |
| `replica` | Number of index replicas |
| `number_of_indexes_per_coll` | Indexes per collection |

## Dependencies

- `gsiLib.GsiHelper_Rest.GsiHelper`
- `membase.api.rest_client.RestConnection`
- `cb_constants.CbServer`
