---
name: cbas-utils-agent
description: >
  Utilities for Couchbase Analytics (CBAS) and Columnar service operations
  including queries, datasets, links, and rebalance.
model: inherit
---

# cbas_utils

**Analytics and Columnar service utilities for Couchbase.**
Supports both on-premises Analytics and Capella Columnar deployments.

## Files

| File | Purpose |
|---|---|
| `cbas_utils.py` | Router based on runtype (columnar vs on-prem) |
| `cbas_utils_on_prem.py` | On-premises Analytics operations |
| `cbas_utils_columnar.py` | Capella Columnar operations |

## Key Classes

### CbasUtil
Main utility class for Analytics operations.

**Capabilities:**
- Execute CBAS queries (SQL++)
- Create/drop datasets and indexes
- Manage connect/disconnect links
- Rebalance operations specific to Analytics nodes
- Backup/restore for Analytics data

**Usage:**
```python
from couchbase_utils.cbas_utils.cbas_utils import CbasUtil

cbas_util = CbasUtil()
status, content = cbas_util.execute_statement_on_cbas(
    query="SELECT * FROM `default`.`default`.`dataset`",
    cbas_servers=analytics_nodes
)
```

### Enterprise Analytics helpers (`cbas_utils_columnar.py`)

| Method | Class | Purpose |
|---|---|---|
| `wait_for_sample_to_be_queryable` | `CbasUtil` | Poll until a sample installed via `load_analytics_sample` answers a query. The samples API returns before the data is loaded, and a standalone EA cluster cannot use ns_server's `/sampleBuckets/install` at all (`max_vbuckets=0`). |
| `set_user_roles` | `RBAC_Util` | REPLACE an existing user's role set — a real privilege revocation, not an append. Allow ~5s for the analytics auth cache to pick the change up. |
| `delete_user` | `RBAC_Util` | Remove a user created by a test. Users are cluster RBAC objects, so they outlive statement-level cleanup and must be dropped in `tearDown`. |

Note `execute_statement_on_cbas_util` returns a fixed 6-tuple
`(status, metrics, errors, results, handle, warnings)` — it unpacks the response and
DROPS every other top-level field. Tests that need the full envelope (e.g. `cachedPlan`)
must use `AnalyticsServiceAPI.submit_service_request` with `mode=None`, or the
`ColumnarOnPremBase._analytics_request` helper that wraps it.

## Runtype Selection

The module automatically selects implementation based on `runtype` parameter:
- `default` → `cbas_utils_on_prem.CbasUtil`
- `columnar` or `onprem-columnar` → `cbas_utils_columnar.CbasUtil`

## Task Classes

| Task | Purpose |
|---|---|
| `FlushToDiskTask` | Flush Analytics data to disk |
| `DisconnectConnectLinksTask` | Manage link connections |
| `CBASRebalanceUtil` | Analytics-specific rebalance |
| `BackupUtils` | Analytics backup operations |

## Dependencies

- `cb_server_rest_util.analytics.analytics_api.AnalyticsRestAPI`
- `Jython_tasks` for async operations
