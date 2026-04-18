---
name: tpch-utils-agent
description: >
  Utilities for TPC-H benchmark operations including bucket creation,
  data loading, and benchmark execution.
model: inherit
---

# tpch_utils

**TPC-H benchmark utilities for performance testing.**
Provides helpers for setting up and running TPC-H benchmarks.

## Files

| File | Purpose |
|---|---|
| `tpch_utils.py` | TPCHUtil for benchmark setup |
| `tpch_helper.py` | Helper functions for TPC-H operations |

## Key Classes

### TPCHUtil
TPC-H benchmark setup and execution.

**Capabilities:**
- Create buckets for TPC-H data
- Load TPC-H dataset via cbimport
- Configure SDK clients for benchmarking
- S3 data operations for dataset retrieval

**Usage:**
```python
from couchbase_utils.tpch_utils.tpch_utils import TPCHUtil

tpch_util = TPCHUtil(basetest_obj)
tpch_util.create_kv_buckets_for_tpch(cluster)
tpch_util.load_tpch_data(cluster, dataset_path)
```

## TPC-H Tables

Standard TPC-H tables include:
- `customer`, `orders`, `lineitem`
- `part`, `partsupp`, `supplier`
- `nation`, `region`

## Dependencies

- `bucket_collections.collections_base.CollectionBase`
- `couchbase_utils.cb_tools.cbimport.CbImport`
- `awsLib.s3_data_helper` for S3 operations
- `sdk_client3.SDKClientPool`
