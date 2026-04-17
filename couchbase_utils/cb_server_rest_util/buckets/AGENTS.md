---
name: cb-rest-buckets-agent
description: >
  REST API methods for Couchbase bucket management, scopes, collections,
  doc ops, and stats in TAF.
model: inherit
---

# buckets/

**Composite class:** `BucketRestApi` (`buckets_api.py`)
**Base URL attribute:** `self.base_url`

## Files → Classes

| File | Class | Inherits |
|---|---|---|
| `buckets_api.py` | `BucketRestApi` | All below (composite) |
| `manage_bucket.py` | `BucketManageAPI` | `CBRestConnection` |
| `bucket_info.py` | `BucketInfo` | `CBRestConnection` |
| `bucket_stats.py` | `BucketStats` | `CBRestConnection` |
| `doc_ops.py` | `DocOpAPI` | `CBRestConnection` |
| `scope_and_collections.py` | `ScopeAndCollectionsAPI` | `CBRestConnection` |

## Method Inventory

### manage_bucket.py — `BucketManageAPI`

| Method | Verb | Path |
|---|---|---|
| `get_available_sample_buckets` | GET | `/sampleBuckets` |
| `load_sample_bucket` | POST | `/sampleBuckets/install` |
| `create_bucket` | POST | `/pools/default/buckets` |
| `edit_bucket` | POST | `/pools/default/buckets/{bucket_name}` |
| `delete_bucket` | DELETE | `/pools/default/buckets/{bucket_name}` |
| `compact_bucket` | POST | `/pools/default/buckets/{bucket_name}/controller/compactBucket` |
| `cancel_compaction` | POST | `/pools/default/buckets/{bucket_name}/controller/cancelBucketCompaction` |
| `flush_bucket` | POST | `/pools/default/buckets/{bucket_name}/controller/` |
| `enable_bucket_encryption` | POST | `/pools/default/buckets/{bucket}` |
| `disable_bucket_encryption` | POST | `/pools/default/buckets/{bucket}` |
| `set_auto_compaction` | POST | `/pools/default/buckets/{bucket_name}` or `/controller/setAutoCompaction` |

### bucket_info.py — `BucketInfo`

| Method | Verb | Path |
|---|---|---|
| `get_bucket_info` | GET | `/pools/default/buckets[/{bucket_name}]` |

### bucket_stats.py — `BucketStats`

| Method | Verb | Path |
|---|---|---|
| `get_bucket_stats` | GET | `/pools/default/buckets/{bucket_name}/stats` |
| `get_bucket_stats_from_node` | GET | `/pools/default/buckets/{bucket_name}/nodes/{node_ip}:{port}/stats` |
| `get_stats_range` | GET | `/pools/default/stats/range` |

### doc_ops.py — `DocOpAPI`

| Method | Verb | Path |
|---|---|---|
| `get_random_key` | GET | `/pools/default/buckets/{bucket_name}/localRandomKey` |

### scope_and_collections.py — `ScopeAndCollectionsAPI`

| Method | Verb | Path |
|---|---|---|
| `create_scope` | POST | `/pools/default/buckets/{bucket_name}/scopes` |
| `drop_scope` | DELETE | `/pools/default/buckets/{bucket_name}/scopes` |
| `create_collection` | POST | `/pools/default/buckets/{bucket_name}/scopes/{scope_name}/collections` |
| `drop_collection` | DELETE | `/pools/default/buckets/{bucket_name}/scopes/{scope_name}/collections/{collection_name}` |
| `edit_collection` | PATCH | `/pools/default/buckets/{bucket_name}/scopes/{scope_name}/collections/{collection_name}` |
| `list_scope_collections` | GET | `/pools/default/buckets/{bucket_name}/scopes/` |
| `import_collection_using_manifest` | PUT | `/pools/default/buckets/{bucket_name}/scopes` |
| `get_bucket_manifest` | GET | `/pools/default/buckets/{bucket_name}/scopes` |
| `wait_for_collections_warmup` | POST | `/pools/default/buckets/{bucket_name}/scopes/@ensureManifest/{uid}` |

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

- URL-encode `bucket_name` and `scope_name` when used as path segments.
- `set_auto_compaction` with no `bucket_name` targets the global compaction endpoint.
- `wait_for_collections_warmup` is a polling-style call — business logic should be moved to callers over time.
