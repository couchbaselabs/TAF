---
name: cb-rest-analytics-agent
description: >
  REST API methods for Couchbase Analytics (CBAS) service in TAF.
model: inherit
---

# analytics/

**Composite class:** `AnalyticsRestAPI` (`analytics_api.py`)
**Base URL attribute:** `self.cbas_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `analytics_api.py` | `AnalyticsRestAPI` | All below (composite) |
| `analytics_settings.py` | `AnalyticsSettingsAPI` | `CBRestConnection` |
| `analytics_functions.py` | `AnalyticsFunctionsAPI` | `CBRestConnection` |
| `analytics_service.py` | `AnalyticsServiceAPI` | `CBRestConnection` |
| `analytics_admin.py` | `AnalyticsAdminAPI` | `CBRestConnection` |
| `analytics_config.py` | `AnalyticsConfigAPI` | `CBRestConnection` |

## Method Inventory

### analytics_settings.py — `AnalyticsSettingsAPI`

| Method | Verb | Path |
|---|---|---|
| `get_analytics_settings` | GET | `/settings/analytics` |
| `update_analytics_settings` | POST | `/settings/analytics` |
| `set_analytics_debug_settings_in_metakv` | PUT | `/_metakv/cbas/debug/settings/` |
| `set_blob_storage_access_key_id` | — | delegates to `set_analytics_debug_settings_in_metakv` |
| `set_blob_storage_secret_access_key` | — | delegates to `set_analytics_debug_settings_in_metakv` |

### analytics_functions.py — `AnalyticsFunctionsAPI`

| Method | Verb | Path |
|---|---|---|
| `execute_statement_on_cbas` | POST | `/analytics/service` |

### analytics_service.py — `AnalyticsServiceAPI`

| Method | Verb | Path |
|---|---|---|
| `submit_service_request` | POST | `/api/v1/request` |
| `get_request_status` | GET | `/api/v1/request/status/{requestID}/{handle}` |
| `get_request_result` | GET | `/api/v1/request/result/{requestID}/{handle}[/{partition}]` |
| `discard_request_result` | DELETE | `/api/v1/request/result/{requestID}/{handle}` |
| `wait_for_request_completion` | — | polling helper (no direct REST call) |

### analytics_admin.py — `AnalyticsAdminAPI`

| Method | Verb | Path |
|---|---|---|
| `cancel_request` | DELETE | `/api/v1/active_requests?request_id={requestID}` |
| `restart_analytics_service` | POST | `/api/v1/service/restart` |

### analytics_config.py — `AnalyticsConfigAPI`

| Method | Verb | Path |
|---|---|---|
| `get_service_config` | GET | `/api/v1/config/service` |
| `update_service_config` | PUT | `/api/v1/config/service` |

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

- `wait_for_request_completion` is a polling helper — it violates the no-business-logic rule and should be migrated to a utility layer over time.
- All direct REST calls use `self.cbas_url` as the base, not `self.base_url`.
