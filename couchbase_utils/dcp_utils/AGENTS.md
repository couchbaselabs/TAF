---
name: dcp-utils-agent
description: >
  Utilities for DCP (Database Change Protocol) stream operations including
  stream management, failover logging, and DCP client connections.
model: inherit
---

# dcp_utils

**DCP stream utilities for change data capture operations.**
Provides DCP client management and stream operations for replication scenarios.

## Files

| File | Purpose |
|---|---|
| `dcp_ready_functions.py` | DCPUtils for DCP stream operations |

## Key Classes

### DCPUtils
DCP client management and stream operations.

**Capabilities:**
- DCP stream initialization and connection
- VBucket mapping and management
- Failover logging and tracking
- Stream request handling with filters
- Collection-aware streaming

**Usage:**
```python
from couchbase_utils.dcp_utils.dcp_ready_functions import DCPUtils

dcp_util = DCPUtils(
    node=node,
    bucket=bucket,
    start_seq_no_list=start_seqs,
    end_seq_no=end_seq,
    vb_uuid_list=vb_uuids
)
dcp_util.initialise_cluster_connections()
```

## Parameters

| Parameter | Description |
|---|---|
| `start_seq_no_list` | Starting sequence numbers per vbucket |
| `end_seq_no` | End sequence number for stream |
| `vb_uuid_list` | VBucket UUIDs |
| `collections` | Enable collection-aware streaming |
| `xattrs` | Include extended attributes |
| `filter_file` | Path to DCP filter file |

## Dependencies

- `dcp_bin_client.DcpClient` — Binary DCP client
- `memcacheConstants` — Memcached protocol constants
