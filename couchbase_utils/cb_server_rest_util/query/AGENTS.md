---
name: cb-rest-query-agent
description: >
  REST API methods for Couchbase N1QL query service in TAF.
model: inherit
---

# query/

**Composite class:** `QueryRestAPI` (`query_api.py`)
**Base URL attribute:** `self.query_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `query_api.py` | `QueryRestAPI` | All below (composite) |
| `admin_rest.py` | `AdminRest` | `CBRestConnection` |
| `query_functions.py` | `QueryFunctions` | `CBRestConnection` |
| `query_settings.py` | `QuerySettings` | `CBRestConnection` |

## Method Inventory

### query_functions.py — `QueryFunctions`

| Method | Verb | Path |
|---|---|---|
| `run_query` | POST | `/query/service` |

### admin_rest.py — `AdminRest`

No REST methods implemented yet — placeholder class.

### query_settings.py — `QuerySettings`

No REST methods implemented yet — placeholder class.

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

- This module is sparsely populated. Missing endpoints to add: query settings (`/admin/settings`), active requests, completed requests, prepared statements, UDF management.
- `run_query` accepts either a dict (sent as JSON body) or a string (sent as form data).
