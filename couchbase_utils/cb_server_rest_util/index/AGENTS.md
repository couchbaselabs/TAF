---
name: cb-rest-index-agent
description: >
  REST API methods for Couchbase GSI Index service settings and statistics
  in TAF.
model: inherit
---

# index/

**Composite class:** `IndexRestAPI` (`index_api.py`)
**Base URL attribute:** `self.index_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `index_api.py` | `IndexRestAPI` | `IndexSettings` |
| `index_settings.py` | `IndexSettings` | `CBRestConnection` |

## Method Inventory

### index_settings.py — `IndexSettings`

| Method | Verb | Path |
|---|---|---|
| `get_gsi_settings` | GET | `/settings/indexes` |
| `set_gsi_settings` | POST | `/settings/indexes` |
| `get_node_statistics` | GET | `/api/v1/stats` |
| `get_keyspace_statistics` | GET | `/api/v1/stats/{keyspace}` |
| `get_index_statistics` | GET | `/api/v1/stats/{keyspace}/{index_name}` |

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

- `/settings/indexes` is served on `self.base_url` (port 8091), not `self.index_url` — verify the URL attribute used in the method before adding new endpoints.
- `/api/v1/stats` paths are served on `self.index_url`.
