---
name: cb-rest-server-groups-agent
description: >
  REST API methods for Couchbase server group management and node assignment
  in TAF.
model: inherit
---

# server_groups/

**Composite class:** `ServerGroupsAPI` (`server_groups_api.py`)
**Base URL attribute:** `self.base_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `server_groups_api.py` | `ServerGroupsAPI` | `ManageServerGroups`, `ManageClusterNodes` |
| `manage_groups.py` | `ManageServerGroups` | `CBRestConnection` |
| `manage_cluster_nodes.py` | `ManageClusterNodes` | `CBRestConnection` |

## Method Inventory

### manage_groups.py — `ManageServerGroups`

| Method | Verb | Path |
|---|---|---|
| `get_server_groups_info` | GET | `/pools/default/serverGroups` |
| `create_server_group` | POST | `/pools/default/serverGroups` |
| `rename_server_group` | PUT | `/pools/default/serverGroups/{uuid}` |
| `delete_server_group` | DELETE | `/pools/default/serverGroups/{uuid}` |
| `update_group_membership` | PUT | `/pools/default/serverGroups?rev={rev_num}` |

### manage_cluster_nodes.py — `ManageClusterNodes`

| Method | Verb | Path |
|---|---|---|
| `add_node` | POST | `/pools/default/serverGroups/{uuid}/addNode` |

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

- `update_group_membership` requires the current revision number (`rev`) from `get_server_groups_info` — always fetch it fresh before calling.
