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
