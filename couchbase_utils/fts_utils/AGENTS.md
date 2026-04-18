---
name: fts-utils-agent
description: >
  Utilities for Full-Text Search (FTS) operations including index creation,
  query execution, and search configuration.
model: inherit
---

# fts_utils

**Full-Text Search utilities for FTS index and query operations.**
Provides helpers for creating and managing FTS indexes.

## Files

| File | Purpose |
|---|---|
| `fts_ready_functions.py` | FTSUtils for FTS index operations |

## Key Classes

### FTSUtils
Full-text search index management.

**Capabilities:**
- Create FTS indexes on collections
- Index template management
- Search query execution
- Partition configuration

**Usage:**
```python
from couchbase_utils.fts_utils.fts_ready_functions import FTSUtils

fts_util = FTSUtils(cluster, server_task)
fts_util.create_fts_indexes(
    bucket=bucket,
    scope=scope,
    collection=collection,
    name="fts_idx"
)
```

## Index Template

Default FTS index template includes:
- Scorch index type
- Scope.collection.type_field mapping
- Standard analyzer
- Configurable partitions

## Dependencies

- `FtsLib.FtsOperations.FtsHelper`
- `cb_server_rest_util.search.search_api.SearchRestAPI`
