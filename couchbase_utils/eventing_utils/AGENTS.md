---
name: eventing-utils-agent
description: >
  Utilities for Couchbase Eventing service operations including function
  deployment, lifecycle management, and handler configuration.
model: inherit
---

# eventing_utils

**Eventing service utilities for function deployment and management.**
Provides helpers for creating, deploying, and managing eventing functions.

## Files

| File | Purpose |
|---|---|
| `eventing_utils.py` | EventingUtils for eventing function operations |

## Key Classes

### EventingUtils
Eventing function lifecycle management.

**Capabilities:**
- Create and save function body with configuration
- Deploy/undeploy eventing functions
- Manage source and metadata buckets
- Timer and DCP configuration
- Multi-destination bucket support
- CURL and external API integration

**Usage:**
```python
from couchbase_utils.eventing_utils.eventing_utils import EventingUtils

eventing_util = EventingUtils(
    master=master_node,
    eventing_nodes=eventing_nodes,
    src_bucket_name='src_bucket',
    dst_bucket_name='dst_bucket',
    metadata_bucket_name='metadata'
)

body = eventing_util.create_save_function_body(
    appname="my_function",
    appcode="handler_code.js"
)
eventing_util.deploy_function(body)
```

## Configuration Options

| Setting | Description |
|---|---|
| `checkpoint_interval` | Checkpoint frequency |
| `dcp_stream_boundary` | DCP stream scope |
| `worker_count` | Number of function workers |
| `timer_worker_pool_size` | Timer thread pool size |
| `execution_timeout` | Function execution timeout |
| `log_level` | Logging verbosity |

## Dependencies

- `EventingLib.EventingOperations_Rest.EventingHelper`
- `BucketLib.BucketOperations.BucketHelper`
- `membase.api.rest_client.RestConnection`
