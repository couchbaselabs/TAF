---
name: rbac-utils-agent
description: >
  Utilities for RBAC (Role-Based Access Control) operations including
  user management, role assignment, and permissions.
model: inherit
---

# rbac_utils

**RBAC utilities for user and role management.**
Provides helpers for creating users and assigning roles.

## Files

| File | Purpose |
|---|---|
| `Rbac_ready_functions.py` | RbacUtils for RBAC operations |

## Key Classes

### RbacUtils
User and role management operations.

**Capabilities:**
- Create users with roles
- Grant/revoke roles
- Drop users
- Role enumeration

**Usage:**
```python
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils

rbac_util = RbacUtils(master)
rbac_util._create_user_and_grant_role(
    username="test_user",
    role="bucket_admin[*]",
    password="password"
)
rbac_util._drop_user("test_user")
```

## Available Roles

Standard Couchbase roles include:
- `admin`, `cluster_admin`, `ro_admin`
- `bucket_admin[*]`, `bucket_full_access[*]`
- `data_reader[*]`, `data_writer[*]`
- `fts_admin[*]`, `fts_searcher[*]`
- `query_select[*]`, `query_insert[*]`
- `analytics_admin`, `analytics_reader`

## Dependencies

- `security.rbac_base.RbacBase`
- `membase.api.rest_client.RestConnection`
