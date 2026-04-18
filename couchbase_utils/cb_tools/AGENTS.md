---
name: cb-tools-agent
description: >
  CLI tool wrappers for Couchbase Server command-line utilities including
  cbbackupmgr, cbimport, cbstats, cbepctl, and cbcollect.
model: inherit
---

# cb_tools

**Couchbase CLI tool wrappers for server-side operations.**
Provides Python interfaces to Couchbase command-line tools executed via SSH.

## Files

| File | Purpose |
|---|---|
| `cbbackupmgr.py` | CbBackupMgr for backup/restore operations |
| `cbimport.py` | CbImport for bulk data import |
| `cbstats.py` | Cbstats for bucket statistics |
| `cbstats_cmdline.py` | Extended cbstats command-line interface |
| `cbstats_memcached.py` | Memcached stats retrieval |
| `cbepctl.py` | Cbepctl for vbucket and persistence settings |
| `cb_cli.py` | CbCli for general CLI operations |
| `cbcontbk.py` | Container backup utilities |
| `cb_collectinfo.py` | CbCollectInfo for diagnostic collection |
| `mc_stat.py` | Memcached stat utilities |
| `cb_tools_base.py` | Base class for CLI tools |

## Key Classes

### CbBackupMgr
Backup and restore via cbbackupmgr CLI.

**Usage:**
```python
from couchbase_utils.cb_tools.cbbackupmgr import CbBackupMgr

backup_mgr = CbBackupMgr(shell_conn, username, password)
backup_mgr.backup(archive_dir, repo_name, cluster_host)
backup_mgr.restore(archive_dir, repo_name, cluster_host)
```

### CbImport
Bulk data import via cbimport CLI.

### Cbstats
Bucket and vbucket statistics retrieval.

### Cbepctl
Vbucket and persistence parameter management.

## Execution Model

All tools execute commands via `RemoteMachineShellConnection`:
```python
shell = RemoteMachineShellConnection(server)
tool = CbBackupMgr(shell, "Administrator", "password")
```

## Dependencies

- `shell_util.remote_connection.RemoteMachineShellConnection`
- `cb_tools.cb_tools_base.CbCmdBase` — base class with common CLI handling
