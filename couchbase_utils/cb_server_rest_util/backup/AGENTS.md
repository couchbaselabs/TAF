---
name: cb-rest-backup-agent
description: >
  REST API methods for Couchbase Backup Service — plans, repositories,
  tasks, and configuration in TAF.
model: inherit
---

# backup/

**Composite class:** `BackupRestApi` (`backup_api.py`)
**Base URL attribute:** `self.backup_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `backup_api.py` | `BackupRestApi` | All below (composite) |
| `backup_plans.py` | `BackupPlanAPIs` | `CBRestConnection` |
| `backup_repo.py` | `BackupRepoAPIs` | `CBRestConnection` |
| `backup_tasks.py` | `BackupTaskAPIs` | `CBRestConnection` |
| `manage_and_config.py` | `BackupManageAndConfigAPIs` | `CBRestConnection` |

## Method Inventory

### backup_plans.py — `BackupPlanAPIs`

| Method | Verb | Path |
|---|---|---|
| `create_plan` | POST | `/plan/{new_plan_id}` |
| `edit_plan` | PUT | `/plan/{existing_plan_id}` |
| `get_plan_info` | GET | `/plan[/{plan_id}]` |
| `delete_plan` | DELETE | `/plan/{plan_id}` |

### backup_repo.py — `BackupRepoAPIs`

| Method | Verb | Path |
|---|---|---|
| `create_repository` | POST | `/cluster/self/repository/active/{repo_name}` |
| `get_repository_information` | GET | `/cluster/self/repository/{active\|imported\|archived}[/{repo_id}[/info]]` |
| `archive_repository` | POST | `/repository/active/{repo_id}/archive` |
| `import_repository` | POST | `/cluster/self/repository/import` |
| `delete_repository` | DELETE | `/cluster/self/repository/archived/{repo_id}[?remove_repository=true]` |

### backup_tasks.py — `BackupTaskAPIs`

| Method | Verb | Path |
|---|---|---|
| `get_task_information` | GET | `/cluster/self/repository/{type}/{repo_name}[/taskHistory]` |
| `pause_task` | POST | `/cluster/self/repository/active/{repo_id}/pause` |
| `resume_task` | POST | `/cluster/self/repository/active/{repo_id}/resume` |
| `examine_backup_data` | POST | `/cluster/self/repository/{type}/{repo_id}/examine` |
| `perform_immediate_backup` | POST | `/cluster/self/repository/active/{repo_id}/backup` |
| `perform_immediate_merge` | POST | `/cluster/self/repository/active/{repo_id}/merge` |
| `restore_data` | POST | `/cluster/self/repository/{type}/{repo_id}/restore` |
| `delete_backup` | DELETE | `/cluster/self/repository/active/{repo_id}/backups/{backup_id}` |

### manage_and_config.py — `BackupManageAndConfigAPIs`

| Method | Verb | Path |
|---|---|---|
| `get_cluster_info` | GET | `/cluster/self` |
| `get_current_config` | GET | `/config` |
| `update_backup_config` | POST | `/config` |

## No Business Logic — Pure Wrappers Only

Every method must be a single REST call that returns `(status, content)`.
**Forbidden in this directory:** response parsing (`content["key"]`), validation,
retries, polling loops, helper/utility functions, wrapping functions, `self.fail()`,
exceptions, or logging beyond a single debug line.
Put all such logic in the caller or a utility helper outside `cb_server_rest_util/`.

## HTTP Library

All HTTP calls must use `self.request()` inherited from `CBRestConnection` (which uses `requests` internally).
**Never** import `http.client`, `urllib.request`, `urllib3`, `httplib2`, `aiohttp`, `httpx`, or any other HTTP library.
If you encounter such an import, warn the user and replace it with `self.request()`.

## Notes

- All paths are relative to `self.backup_url` (Backup Service port), not `self.base_url`.
- `repo_type` parameter accepts `"active"`, `"imported"`, or `"archived"`.
