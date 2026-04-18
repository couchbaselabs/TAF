---
name: xdcr-utils-agent
description: >
  Utilities for XDCR (Cross Data Center Replication) operations including
  replication setup, remote cluster management, and topology configuration.
model: inherit
---

# xdcr_utils

**XDCR utilities for cross-datacenter replication.**
Provides helpers for setting up and managing XDCR replications.

## Files

| File | Purpose |
|---|---|
| `xdcr_ready_functions.py` | XDCRUtils for XDCR operations |
| `xdcr_util.py` | Additional XDCR utilities |

## Key Classes

### XDCRUtils
Cross-datacenter replication management.

**Capabilities:**
- Create/delete remote cluster references
- Create/delete replications
- Manage replication settings
- Support various topologies (chain, star, ring, hybrid)

**Usage:**
```python
from couchbase_utils.xdcr_utils.xdcr_ready_functions import XDCRUtils

xdcr_util = XDCRUtils(cluster)
xdcr_util.create_remote_cluster_ref(
    remote_cluster=remote_cluster,
    name="remote_ref"
)
xdcr_util.create_replication(
    source_bucket="default",
    target_bucket="default",
    replication_type="continuous"
)
```

## Replication Topologies

| Topology | Description |
|---|---|
| `CHAIN` | Linear A → B → C |
| `STAR` | Hub and spoke |
| `RING` | Circular A → B → C → A |
| `HYBRID` | Mixed topology |

## Replication Types

| Type | Description |
|---|---|
| `continuous` | Ongoing replication |
| `xmem` | XMEM protocol |
| `capi` | CAPI protocol |

## Dependencies

- `membase.api.rest_client.RestConnection`
- `bucket_utils.bucket_ready_functions.BucketUtils`
- `cluster_utils.cluster_ready_functions.ClusterUtils`
