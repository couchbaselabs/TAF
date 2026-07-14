---
name: cb-tools-agent
description: >
  CLI tool wrappers for Couchbase Server command-line utilities including
  cbbackupmgr, cbimport, cbstats, cbepctl, and cbcollect.
model: inherit
---

# cb_tools

- Couchbase CLI tool wrappers for server-side operations, executed via SSH.

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
| `cbcontbk.py` | CbContBk — continuous backup (PITR) restore/collect-logs via cbcontbk CLI |
| `cb_collectinfo.py` | CbCollectInfo for diagnostic collection |
| `mc_stat.py` | Memcached stat utilities |
| `cb_tools_base.py` | Base class for CLI tools |

## CbBackupMgr

- Covers `backup`, `create_repo`, `worm`, `info`, `list_backups`, `restore`, `remove`, `generate_docs`, `merge`, `examine`, `collect_logs`.
- `__init__` takes optional `cloud_provider` (any `couchbase_utils.cloud_provider_utils` provider, e.g. `AWSProvider`).
- If set, every method above (except `merge`) appends `cloud_provider.get_cbbackupmgr_flags()`.
- Each of those methods also takes `obj_staging_dir=None` as a **per-call** param (not stored on instance) — `--obj-staging-dir <dir>` added only when both `cloud_provider` is injected AND `obj_staging_dir` is passed.
- `merge()` takes `obj_staging_dir` too but is NOT gated by `cloud_provider` and never appends provider flags.
- No more `aws_region` param / `prepare_command()` / `_normalise_aws_region()` — the old `AWS_DEFAULT_REGION`/`AWS_REGION` env-var-prefix injection was removed in favor of the `cloud_provider` abstraction.

```python
from couchbase_utils.cb_tools.cbbackupmgr import CbBackupMgr
from couchbase_utils.cloud_provider_utils.aws_provider import AWSProvider

backup_mgr = CbBackupMgr(shell_conn, username, password, cloud_provider=AWSProvider())
backup_mgr.backup(archive_dir, repo_name, cluster_host, obj_staging_dir="/tmp/staging")
backup_mgr.restore(archive_dir, repo_name, cluster_host)
```

## CbContBk

- Continuous backup (PITR) restore + log collection via cbcontbk CLI. Covers `restore`, `collect_logs`, `get_cluster_timestamp`.
- Same `cloud_provider` injection pattern as `CbBackupMgr`: `restore()`/`collect_logs()` append `cloud_provider.get_cbconbk_flags()` plus optional per-call `obj_staging_dir` (added only when both provider + `obj_staging_dir` given).

```python
from couchbase_utils.cb_tools.cbcontbk import CbContBk
from couchbase_utils.cloud_provider_utils.azure_provider import AzureProvider

cont_bk = CbContBk(shell_conn, username, password, cloud_provider=AzureProvider())
cont_bk.restore(archive_path, repo_name, location, temp_dir, obj_staging_dir="/tmp/staging")
```

See [cloud_provider_utils/AGENTS.md](../cloud_provider_utils/AGENTS.md) for the provider interface and per-provider env vars.

## CbImport / Cbstats / Cbepctl

- `CbImport` — bulk data import via cbimport CLI.
- `Cbstats` — bucket/vbucket statistics retrieval.
- `Cbepctl` — vbucket and persistence parameter management.

## Execution Model

All tools execute via `RemoteMachineShellConnection`:
```python
shell = RemoteMachineShellConnection(server)
tool = CbBackupMgr(shell, "Administrator", "password")
```

## Dependencies

- `shell_util.remote_connection.RemoteMachineShellConnection`
- `cb_tools.cb_tools_base.CbCmdBase` — base class with common CLI handling
