---
name: cb-rest-search-agent
description: >
  REST API methods for Couchbase Full-Text Search (FTS) index management
  and querying in TAF.
model: inherit
---

# search/

**Composite class:** `SearchRestAPI` (`search_api.py`)
**Base URL attribute:** `self.fts_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `search_api.py` | `SearchRestAPI` | `IndexFunctions` |
| `search_functions.py` | `IndexFunctions` | `CBRestConnection` |

## Method Inventory

### search_functions.py — `IndexFunctions`

| Method | Verb | Path |
|---|---|---|
| `create_fts_index_from_json` | PUT | `/api/index/{index_name}` |
| `run_fts_query` | POST | `/api/index/{index_name}/query` |
| `delete_fts_index` | DELETE | `/api/index/{index_name}` |
| `fts_index_item_count` | GET | `/api/index/{index_name}/count` |

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

- All paths are relative to `self.fts_url`, not `self.base_url`.
- The class is named `IndexFunctions` (not `SearchFunctions`) — use the existing name when adding methods.
- Missing endpoints to add: list all FTS indexes (`GET /api/index`), get index definition (`GET /api/index/{name}`).
