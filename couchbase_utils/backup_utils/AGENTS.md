---
name: backup-utils-agent
description: >
  Utilities for backup and restore operations using cbbackupmgr and
  cbcollect. Manages backup repositories and archive operations.
model: inherit
---

# backup_utils

**Backup and restore utilities for Couchbase Server clusters.**
Provides high-level helpers for managing backup repositories via REST API.

## Files

| File | Purpose |
|---|---|
| `backup_utils.py` | BackupUtil class for archive/delete repo operations |

## Key Classes

### BackupUtil
Wrapper around `BackupRestApi` for backup repository management.

**Usage:**
```python
from couchbase_utils.backup_utils.backup_utils import BackupUtil

backup_util = BackupUtil(cluster_node)
backup_util.archive_all_repos()
backup_util.delete_all_archive_repos(remove_repository=True)
```

## Dependencies

- `cb_server_rest_util.backup.backup_api.BackupRestApi` — REST wrapper for backup endpoints
