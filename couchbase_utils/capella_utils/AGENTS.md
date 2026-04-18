---
name: capella-utils-agent
description: >
  Utilities for Capella cloud environments including dedicated, serverless,
  and columnar deployments.
model: inherit
---

# capella_utils

**Capella cloud utilities for dedicated, serverless, and columnar environments.**
Provides API wrappers and helpers for Capella-specific operations.

## Files

| File | Purpose |
|---|---|
| `dedicated.py` | CapellaUtils for dedicated cluster operations |
| `serverless.py` | Utilities for serverless deployments |
| `columnar.py` | Utilities for columnar analytics on Capella |
| `common_utils.py` | Shared utilities for all Capella environments |

## Key Classes

### CapellaUtils (dedicated)
Dedicated cluster operations via Capella API.

**Capabilities:**
- Project and cluster provisioning
- Node scaling and configuration
- User management and RBAC
- Cluster health monitoring

**Usage:**
```python
from couchbase_utils.capella_utils.dedicated import CapellaUtils

CapellaUtils.create_project(pod, tenant, "my_project")
CapellaUtils.create_cluster(pod, tenant, cluster_config)
```

## Environment Types

| Type | File | Use Case |
|---|---|---|
| Dedicated | `dedicated.py` | Full control clusters with fixed nodes |
| Serverless | `serverless.py` | Auto-scaling, multi-tenant clusters |
| Columnar | `columnar.py` | Analytics-focused columnar service |

## Dependencies

- `capellaAPI.capella.dedicated.CapellaAPI` — Capella REST client
- Cloud provider constants (AWS, GCP)
