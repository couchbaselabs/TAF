---
name: backup-utils-agent
description: >
  Utilities for backup and restore operations using cbbackupmgr and
  the Backup REST API. Manages backup repositories, archive operations,
  continuous backup, and restore monitoring.
model: inherit
---

# backup_utils

- Backup/restore utilities for Couchbase Server clusters.
- Three classes: cbbackupmgr CLI ops, continuous backup (PITR), REST-based repo management.

## Files

| File | Purpose |
|---|---|
| `backup_utils.py` | `BackupMgrUtil`, `ContinuousBackupUtil`, `BackupUtil` |

## BackupMgrUtil

- Inherits `CbBackupMgr`.
- Built from `cb_node` (not raw `shell_conn`) — opens its own `RemoteMachineShellConnection` internally.
- `__init__(cb_node, cloud_provider=None)` — `cloud_provider` (any `couchbase_utils.cloud_provider_utils` provider, e.g. `AWSProvider`) passed straight through to `CbBackupMgr.__init__`; also drives provider-vs-shell branching below.

| Method | What it does |
|---|---|
| `configure_backup(archive, repo, exclude, include, obj_staging_dir=None)` | Provider set → `cloud_provider.cleanup_for_bkrs(archive)`, skips `chown`. No provider → `rm -rf -- {archive}` + `chown -R couchbase:couchbase {archive}`. Either way, calls `super().create_repo(..., obj_staging_dir=obj_staging_dir)` |
| `monitor_restore(bucket_util, cluster, bucket_name, items, timeout)` | Polls item count until restore completes/timeout (default 43200s) — unchanged |
| `collect_backup_logs_on_failure(archive, log_path, obj_staging_dir=None)` | `self.collect_logs()` on Linux nodes; forwards `obj_staging_dir` |
| `merge_all_backups(archive, repo)` | Finds + merges all backups in archive — unchanged |
| `cleanup_archive(archive)` | Provider set → delegates entirely to `cloud_provider.cleanup_for_bkrs(archive)`. No provider → `find`s repos, `cbbackupmgr remove`s each, `rm -rf {archive}/` fallback |
| `backup(...)` / `restore(...)` | Inherited from `CbBackupMgr` |

```python
from couchbase_utils.backup_utils.backup_utils import BackupMgrUtil
from couchbase_utils.cloud_provider_utils.aws_provider import AWSProvider

mgr = BackupMgrUtil(cb_node, cloud_provider=AWSProvider())
mgr.configure_backup("s3://my-bucket/backups", "repo1",
                     exclude=["bucket1.scope1"], obj_staging_dir="/tmp/staging")
mgr.backup("s3://my-bucket/backups", "repo1")
mgr.cleanup_archive("s3://my-bucket/backups")
```

See [cloud_provider_utils/AGENTS.md](../cloud_provider_utils/AGENTS.md), [cb_tools/AGENTS.md](../cb_tools/AGENTS.md).

## ContinuousBackupUtil

- Wraps `CbBackupMgr` (`self.backup_mgr`) + `CbContBk` (`self.cont_bk_mgr`).
- `__init__(..., backupmgr_cloud_provider=None, contbk_cloud_provider=None)` — passed as `cloud_provider` to `CbBackupMgr`/`CbContBk` resp. `contbk_cloud_provider` also kept as `self.contbk_cloud_provider` for `cleanup_continuous_backup()`.

| Method | What it does |
|---|---|
| `enable_continuous_backup(bucket_util, cluster, buckets, location, interval)` | Enables CB per bucket via `update_bucket_property` |
| `verify_continuous_backup_params(bucket_util, cluster, buckets)` | Asserts `continuousBackupEnabled=true` per bucket |
| `verify_backup_and_restore(..., obj_staging_dir=None)` | Polls backup data (`list_backups`), captures PITR timestamp, restores per bucket (`restore`), validates item counts. `obj_staging_dir` forwarded to both calls |
| `_create_restore_bucket(...)` | Recreates bucket for restore verification |
| `monitor_restore(bucket_util, cluster, bucket, items, timeout, tolerance)` | Polls item count within `+/-tolerance` until restore completes/timeout |
| `trigger_restore(cluster, archive, repo, cont_backup_location, staging_dir, timestamp, threads, obj_staging_dir=None)` | Wraps `cont_bk_mgr.restore()`; `obj_staging_dir` forwarded (distinct from `staging_dir` → CLI's local `temp_dir`) |
| `collect_continuous_backup_logs_on_failure(backup_location, obj_staging_dir=None)` | `cont_bk_mgr.collect_logs()` on Linux nodes; forwards `obj_staging_dir` |
| `cleanup_continuous_backup(backup_location)` | `if contbk_cloud_provider is not None` → `cleanup_for_bkrs(backup_location)`; `else` → `rm -rf {backup_location}/*` (Linux only). `if`/`else`, not sequential — exception in provider path is logged, does NOT fall through to `rm -rf` |

```python
from couchbase_utils.backup_utils.backup_utils import ContinuousBackupUtil
from couchbase_utils.cloud_provider_utils.aws_provider import AWSProvider

cb_util = ContinuousBackupUtil(shell_conn, username, password,
                               backupmgr_cloud_provider=AWSProvider(),
                               contbk_cloud_provider=AWSProvider())
cb_util.enable_continuous_backup(bucket_util, cluster, buckets, interval=5)
cb_util.verify_backup_and_restore(bucket_util, cluster, buckets, obj_staging_dir="/tmp/staging")
cb_util.cleanup_continuous_backup("s3://my-bucket/cont-backup-dir")
```

See [cloud_provider_utils/AGENTS.md](../cloud_provider_utils/AGENTS.md), [cb_tools/AGENTS.md](../cb_tools/AGENTS.md).

## BackupUtil

Wrapper around `BackupRestApi` for REST-based backup repository lifecycle.

| Method | What it does |
|---|---|
| `archive_all_repos()` | Archives all active repos |
| `delete_all_archive_repos(remove_repository)` | Deletes all archived repos |
| `reset_cluster_node(backup_node)` | Switches the REST target node |

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
- `couchbase_utils.cloud_provider_utils.*` — optional providers (AWS/Azure/GCP/Localstack) injectable into `BackupMgrUtil` and `ContinuousBackupUtil`
