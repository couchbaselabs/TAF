---
name: cb-server-rest-util-agent
description: >
  Rules for reading, writing, and migrating code in
  couchbase_utils/cb_server_rest_util — the canonical Couchbase Server REST
  API wrapper for TAF.
model: inherit
---

# cb_server_rest_util

**Canonical REST layer for all Couchbase Server HTTP calls in TAF.**
New endpoints go here. Old `RestConnection` methods migrate here over time.
Never add REST calls to test files, utility helpers, or `RestConnection`.

## Architecture

```
CBRestConnection (connection.py)          ← HTTP primitives only
  └─ Feature classes  (e.g. AutoFailoverAPI, BucketManageAPI)
       └─ Composite class  (e.g. ClusterRestAPI) via multiple inheritance
            └─ RestConnection (rest_client.py)  ← test code entry point
```

Test code uses `RestConnection` attributes only:
```python
rest = RestConnection(server)
rest.cluster.rebalance(...)
rest.activate_service_api(["security"])
rest.security.get_rbac_user(...)
```

## Directory → Composite class

| Directory | Composite | Responsibility |
|---|---|---|
| `cluster_nodes/` | `ClusterRestAPI` | Failover, rebalance, node ops, settings, stats |
| `buckets/` | `BucketRestApi` | Bucket CRUD, scopes, collections, doc ops |
| `security/` | `SecurityRestAPI` | RBAC, certs, auditing, JWT |
| `analytics/` | `AnalyticsRestAPI` | CBAS queries, config, functions |
| `query/` | `QueryRestAPI` | N1QL, settings, UDFs |
| `index/` | `IndexRestAPI` | GSI index settings |
| `search/` | `SearchRestAPI` | Full-text search |
| `eventing/` | `EventingRestAPI` | Functions lifecycle |
| `backup/` | `BackupRestApi` | Plans, repos, tasks |
| `xdcr/` | `XdcrRestAPI` | References, replications |
| `server_groups/` | `ServerGroupsAPI` | Server group management |
| `fusion/` | `FusionRestAPI` | Fusion-specific endpoints |

## Mandatory Standards

### Docstring — HTTP verb + path required; docs URL when publicly available
```python
def create_bucket(self, bucket_params):
    """
    POST :: /pools/default/buckets
    https://docs.couchbase.com/server/current/rest-api/rest-bucket-create.html
    """
```
Omit the URL for unreleased/undocumented endpoints.

### Return `(status, content)` — always a tuple, never raise
```python
status, content, _ = self.request(api, self.GET)
return status, content          # callers decide what to do with status
```

### No business logic — pure wrappers only

**This is the strictest rule in this module.** Every file under `cb_server_rest_util/`
must contain only thin REST wrappers. The following are **forbidden everywhere**
in this directory tree:

| Forbidden | Examples |
|---|---|
| Response parsing | `content["key"]`, `json.loads(content)`, extracting fields |
| Validation | `if not status`, `assert`, checking response values |
| Retries / polling | loops that re-call the endpoint until a condition is met |
| Helper / utility functions | anything that is not a direct one-shot REST call |
| Wrapping functions | functions that call other methods in this module and add logic |
| Logging beyond debug | `self.log.error(...)`, `self.log.warning(...)` |
| `self.fail()` or exceptions | any raise inside a wrapper method |

```python
# CORRECT — one REST call, return the tuple, nothing else
def get_bucket_info(self, bucket_name):
    """GET :: /pools/default/buckets/{bucket_name}"""
    api = self.base_url + f"/pools/default/buckets/{quote(bucket_name, safe='')}"
    status, content, _ = self.request(api, self.GET)
    return status, content

# WRONG — parsing, validation, and logging all violate this rule
def get_bucket_info(self, bucket_name):
    status, content, _ = self.request(...)
    if not status:
        self.log.error("Failed")          # ← forbidden
        raise Exception("...")            # ← forbidden
    return content["name"]               # ← forbidden (parsing)
```

If you need parsing, validation, or retry logic, put it in the **caller**
(test code or a utility helper outside this directory).

### Method naming — HTTP verb drives the prefix
| Verb | Prefix | Example |
|---|---|---|
| GET | `get_` | `get_auto_failover_settings()` |
| POST create | `create_` | `create_bucket()` |
| POST action | descriptive | `rebalance()`, `failover_node()` |
| PUT/PATCH | `update_` / `set_` | `update_auto_failover_settings()` |
| DELETE | `delete_` | `delete_bucket()` |

### URL construction — use endpoint attributes, never hardcode
| Attribute | Service port |
|---|---|
| `self.base_url` | 8091 / 18091 |
| `self.query_url` | Query |
| `self.index_url` | Index |
| `self.fts_url` | Search |
| `self.eventing_url` | Eventing |
| `self.backup_url` | Backup |
| `self.cbas_url` | Analytics |

URL-encode user-supplied path segments: `quote(name, safe='')`.

### Optional params — keyword args, add to dict only when not `None`
```python
params = {"enabled": enabled, "timeout": timeout}
if max_count is not None:
    params["maxCount"] = max_count
```

## Adding Endpoints

**Existing service**: add the method to the right feature file. Multiple
inheritance propagates it to the composite and `RestConnection` automatically.

**New service**: create `<service>/`, feature files inheriting
`CBRestConnection`, a composite `<service>_api.py`, then register in
`RestConnection.__init__` as `self.<service> = None` and initialise inside
`activate_service_api()`.

**Migrating from legacy `RestConnection`**: strip retries/assertions/logging,
rename to follow the verb prefix, add docstring, return `(status, content)`,
then update callers.

## Naming Conventions

| Entity | Pattern | Example |
|---|---|---|
| Feature class | `PascalCase` + `API` | `AutoFailoverAPI` |
| Composite class | `PascalCase` + `RestAPI` | `ClusterRestAPI` |
| Feature file | `snake_case` | `auto_failover.py` |
| Composite file | `<service>_api.py` | `cluster_nodes_api.py` |

## HTTP Library — `requests` only

All HTTP calls **must** use the `requests` library via `CBRestConnection.request()`.
Never import or use any other HTTP library in this module.

**Forbidden imports — warn the user and replace immediately:**
```python
# FORBIDDEN
import http.client
import urllib.request
import urllib3
import httplib2
import aiohttp
import httpx
```

**Correct pattern — always go through the base method:**
```python
# All REST calls must flow through this inherited method
status, content, _ = self.request(api, self.GET)
status, content, _ = self.request(api, self.POST, params=params)
```

If you see any forbidden import in a file under this module, flag it to the user
before proceeding and replace it with the `self.request()` pattern.

## Anti-Patterns

- **Do not add to `RestConnection`** — it is the legacy class being phased out.
- **Do not hardcode `http://{ip}:8091`** — use `self.base_url`.
- **Do not raise or assert** — return `(False, content)` and let callers handle it.
- **Do not skip lazy-init** — new services must be `None` until `activate_service_api()`.
- **Do not parse response content** — return raw `content`; parse in callers.
- **Do not use any HTTP library other than `requests`** — see "HTTP Library" section above.
