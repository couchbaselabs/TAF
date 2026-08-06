---
name: cb-rest-fusion-agent
description: >
  REST API methods for Couchbase Fusion storage — enable/disable, status,
  rebalance, and settings in TAF.
model: inherit
---

# fusion/

**Composite class:** `FusionRestAPI` (`fusion_api.py`)
**Base URL attribute:** `self.base_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `fusion_api.py` | `FusionRestAPI` | `FusionFunctions` |
| `fusion_functions.py` | `FusionFunctions` | `CBRestConnection` |

## Method Inventory

### fusion_functions.py — `FusionFunctions`

| Method | Verb | Path |
|---|---|---|
| `get_active_guest_volumes` | GET | `/fusion/activeGuestVolumes` |
| `manage_fusion_settings` | GET/POST | `/settings/fusion` |
| `get_fusion_status` | GET | `/fusion/status` |
| `enable_fusion` | POST | `/fusion/enable` |
| `disable_fusion` | POST | `/fusion/disable` |
| `stop_fusion` | POST | `/fusion/stop` |
| `prepare_rebalance` | POST | `/controller/fusion/prepareRebalance` |
| `sync_log_store` | POST | `/controller/fusion/syncLogStore` |

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

- Fusion endpoints are in active development — docs URLs may not exist yet.
- `manage_fusion_settings` selects GET or POST based on whether mutation params are provided.
