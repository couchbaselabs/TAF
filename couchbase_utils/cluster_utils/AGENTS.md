---
name: cluster-utils-agent
description: >
  Utilities for cluster operations including cluster readiness checks,
  encryption management, and cluster topology management.
model: inherit
---

# cluster_utils

**Cluster-level utilities for Couchbase Server operations.**
Provides cluster readiness functions and encryption key management.

## Files

| File | Purpose |
|---|---|
| `cluster_ready_functions.py` | ClusterUtils for cluster operations |
| `encryption_util.py` | EncryptionUtil for KMS key management |

## Key Classes

### ClusterUtils
Cluster-level operations and validation.

**Capabilities:**
- Cluster health checks and readiness validation
- Node management (add/remove)
- Cluster configuration operations
- Nebula cluster support

**Usage:**
```python
from couchbase_utils.cluster_utils.cluster_ready_functions import ClusterUtils

cluster_util = ClusterUtils(cluster, task_manager)
cluster_util.wait_for_cluster_to_be_ready()
cluster_util.add_nodes(nodes_list, services)
```

### EncryptionUtil
Encryption key management for various KMS providers.

**Capabilities:**
- GCP KMS symmetric key configuration
- Azure KMS key configuration
- AWS KMS key configuration
- Encryption key management

**Usage:**
```python
from couchbase_utils.cluster_utils.encryption_util import EncryptionUtil

params = EncryptionUtil.create_gcp_kms_params(
    name="MyKey",
    keyResourceId="projects/.../keys/...",
    credentialsFile="/path/to/creds.json"
)
```

## Dependencies

- `cb_server_rest_util.cluster_nodes.cluster_nodes_api.ClusterRestAPI`
- `couchbase_cli.CouchbaseCLI`
- `shell_util.remote_connection.RemoteMachineShellConnection`
