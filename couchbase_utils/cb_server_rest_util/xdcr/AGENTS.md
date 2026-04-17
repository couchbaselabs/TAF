---
name: cb-rest-xdcr-agent
description: >
  REST API methods for Couchbase XDCR — remote cluster references and
  replications in TAF.
model: inherit
---

# xdcr/

**Composite class:** `XdcrRestAPI` (`xdcr_api.py`)
**Base URL attribute:** `self.base_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `xdcr_api.py` | `XdcrRestAPI` | `XdcrReferencesAPI`, `XdcrReplicationAPI` |
| `xdcr_references.py` | `XdcrReferencesAPI` | `CBRestConnection` |
| `xdcr_replications.py` | `XdcrReplicationAPI` | `CBRestConnection` |
| `xdcr_stats.py` | _(empty)_ | — |

## Method Inventory

### xdcr_references.py — `XdcrReferencesAPI`

| Method | Verb | Path |
|---|---|---|
| `get_remote_references` | GET | `/pools/default/remoteClusters` |
| `create_remote_reference` | POST | `/pools/default/remoteClusters` |
| `delete_remote_reference` | POST | `/pools/default/remoteClusters/{target_cluster_name}` |
| `connection_pre_check` | POST | `/xdcr/connectionPreCheck` |

### xdcr_replications.py — `XdcrReplicationAPI`

| Method | Verb | Path |
|---|---|---|
| `create_replication` | POST | `/controller/createReplication` |
| `delete_replication` | DELETE | `/controller/cancelXDCR/{replication_id}` |
| `pause_resume_replication` | POST | `/settings/replications/{settingsURI}` |
| `stop_recovery` | POST | `{stop_uri}` (dynamic URI from task list) |

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

- `delete_remote_reference` uses POST despite being a delete operation — this matches the Couchbase REST API spec.
- `create_replication` and `connection_pre_check` method bodies are incomplete stubs; implement before use.
- `xdcr_stats.py` is empty — XDCR stats endpoints are not yet implemented here.