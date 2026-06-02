---
name: backup-utils-agent
description: >
  Utilities for backup and restore operations using cbbackupmgr and
  the Backup REST API. Manages backup repositories, archive operations,
  continuous backup, and restore monitoring.
model: inherit
---

# backup_utils

**Backup and restore utilities for Couchbase Server clusters.**
Three classes covering cbbackupmgr CLI operations, continuous backup (PITR), and REST-based repo management.

## Files

| File | Purpose |
|---|---|
| `backup_utils.py` | `BackupMgrUtil`, `ContinuousBackupUtil`, `BackupUtil` |

## Key Classes

### BackupMgrUtil
Inherits `CbBackupMgr`. Adds pre/post-processing around CLI commands.

| Method | What it does |
|---|---|
| `configure_backup(archive, repo, exclude, include)` | Deletes archive dir, then calls `super().create_repo()` |
| `monitor_restore(bucket_util, cluster, bucket_name, items, timeout)` | Polls item count until restore completes or timeout (default 43200s) |
| `backup(...)` | Inherited from `CbBackupMgr` — call directly |
| `restore(...)` | Inherited from `CbBackupMgr` — call directly |

**Usage:**
```python
from couchbase_utils.backup_utils.backup_utils import BackupMgrUtil

mgr = BackupMgrUtil(shell_conn, username="Administrator", password="password")
mgr.configure_backup("/data/backups", "repo1", exclude=["bucket1.scope1"])
mgr.backup("/data/backups", "repo1")
done = mgr.monitor_restore(bucket_util, cluster, "my_bucket", expected_items)
```

### ContinuousBackupUtil
Orchestrates continuous backup (PITR) enable/verify workflows. Wraps both
`CbBackupMgr` and `CbContBk` internally.

| Method | What it does |
|---|---|
| `enable_continuous_backup(bucket_util, cluster, buckets, location, interval)` | Enables CB on each bucket via `update_bucket_property` |
| `verify_continuous_backup_params(bucket_util, cluster, buckets)` | Asserts `continuousBackupEnabled=true` for each bucket |
| `verify_backup_and_restore(...)` | Polls for backup data, captures PITR timestamp, runs continuous restore per bucket, validates item counts |

**Usage:**
```python
from couchbase_utils.backup_utils.backup_utils import ContinuousBackupUtil

cb_util = ContinuousBackupUtil(shell_conn, username, password)
cb_util.enable_continuous_backup(bucket_util, cluster, buckets, interval=5)
cb_util.verify_backup_and_restore(bucket_util, cluster, buckets)
```

### BackupUtil
Wrapper around `BackupRestApi` for REST-based backup repository lifecycle.

| Method | What it does |
|---|---|
| `archive_all_repos()` | Archives all active repos |
| `delete_all_archive_repos(remove_repository)` | Deletes all archived repos |
| `reset_cluster_node(backup_node)` | Switches the REST target node |

**Usage:**
```python
from couchbase_utils.backup_utils.backup_utils import BackupUtil

backup_util = BackupServiceUtil(cluster, backup_node=cluster.backup_nodes[0])
backup_util.archive_all_repos()
backup_util.delete_all_archive_repos(remove_repository=True)
```

## Dependencies

- `couchbase_utils.cb_tools.cbbackupmgr.CbBackupMgr` — CLI wrapper (`BackupMgrUtil` parent)
- `couchbase_utils.cb_tools.cbcontbk.CbContBk` — continuous backup CLI wrapper
- `cb_server_rest_util.backup.backup_api.BackupRestApi` — REST wrapper for backup endpoints
