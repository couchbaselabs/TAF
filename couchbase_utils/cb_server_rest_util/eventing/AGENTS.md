---
name: cb-rest-eventing-agent
description: >
  REST API methods for Couchbase Eventing functions lifecycle in TAF.
model: inherit
---

# eventing/

**Composite class:** `EventingRestAPI` (`eventing_api.py`)
**Base URL attribute:** `self.eventing_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `eventing_api.py` | `EventingRestAPI` | `EventingFunctions` |
| `eventing_functions.py` | `EventingFunctions` | `CBRestConnection` |

## Method Inventory

### eventing_functions.py — `EventingFunctions`

| Method | Verb | Path |
|---|---|---|
| `create_function` | POST | `/api/v1/functions/{name}` |
| `update_function` | POST | `/api/v1/functions/{name}` |
| `deploy_eventing_function` | POST | `/api/v1/functions/{name}/deploy` |
| `pause_eventing_function` | POST | `/api/v1/functions/{name}/pause` |
| `resume_eventing_function` | POST | `/api/v1/functions/{name}/resume` |
| `get_function_details` | GET | `/api/v1/functions/{name}` |
| `get_list_of_eventing_functions` | GET | `/api/v1/list/functions` |

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

- All paths are relative to `self.eventing_url`, not `self.base_url`.
- `create_function` and `update_function` share the same verb and path — distinguish by intent at the call site.
- Missing endpoints to add: delete function (`DELETE /api/v1/functions/{name}`), undeploy, export/import.
