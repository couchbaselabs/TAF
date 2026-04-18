---
name: rebalance-utils-agent
description: >
  Utilities for rebalance operations including status monitoring,
  retry logic, and cluster topology changes.
model: inherit
---

# rebalance_utils

**Rebalance utilities for cluster topology changes.**
Provides helpers for managing rebalance operations with retry logic.

## Files

| File | Purpose |
|---|---|
| `rebalance_util.py` | RebalanceUtil for rebalance operations |
| `retry_rebalance.py` | RetryRebalance for automatic retry logic |

## Key Classes

### RebalanceUtil
Rebalance operation management.

**Capabilities:**
- Get rebalance status
- Stop rebalance operations
- Monitor rebalance progress
- UI log retrieval for diagnostics

**Usage:**
```python
from couchbase_utils.rebalance_utils.rebalance_util import RebalanceUtil

rebalance_util = RebalanceUtil(cluster)
status = rebalance_util.get_rebalance_status()
if status == "running":
    rebalance_util.stop_rebalance()
```

## Status Values

| Status | Meaning |
|---|---|
| `running` | Rebalance in progress |
| `notRunning` | No rebalance active |
| `none` | Finished or not applicable |

## Dependencies

- `cb_server_rest_util.cluster_nodes.cluster_nodes_api.ClusterRestAPI`
- `common_lib.sleep`
- `custom_exceptions.exception.RebalanceFailedException`
