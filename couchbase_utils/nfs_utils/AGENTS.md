---
name: nfs-utils-agent
description: >
  Utilities for NFS server operations including validation, mount management,
  and remote storage configuration.
model: inherit
---

# nfs_utils

**NFS utilities for remote storage operations.**
Provides NFS server validation and mount management for Couchbase deployments.

## Files

| File | Purpose |
|---|---|
| `nfs_utils.py` | NfsUtil for NFS server operations |

## Key Classes

### NfsUtil
NFS server validation and management.

**Capabilities:**
- NFS server validation
- Package and service checks
- Export configuration validation
- Mount point management

**Usage:**
```python
from couchbase_utils.nfs_utils.nfs_utils import NfsUtil

nfs_util = NfsUtil(nfs_server_node)
if nfs_util.validate_nfs_server():
    nfs_util.mount_nfs(client_node, mount_point)
```

## Validation Checks

| Check | Description |
|---|---|
| `nfs_package_installed` | NFS server packages present |
| `nfs_service_running` | NFS service active |
| `nfs_service_enabled` | Service enabled on boot |
| `data_dir_exists` | /data directory present |
| `data_in_exports` | /data in /etc/exports |
| `exportfs_active` | Export active in exportfs |

## Dependencies

- `shell_util.remote_connection.RemoteMachineShellConnection`
- `global_vars.logger`
