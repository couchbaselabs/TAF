---
name: delta-lake-util-agent
description: >
  Utilities for Delta Lake integration with Apache Spark supporting
  AWS S3 and Google Cloud Storage (GCS).
model: inherit
---

# delta_lake_util

**Delta Lake utilities for data lake operations.**
Provides Apache Spark session management and Delta Lake table operations
for cloud storage backends.

## Files

| File | Purpose |
|---|---|
| `delta_lake_util.py` | DeltaLakeUtils for Spark/Delta operations |

## Key Classes

### DeltaLakeUtils
Delta Lake session and table management.

**Capabilities:**
- Spark session creation for S3 or GCS
- Delta table creation and management
- Data operations (create, read, update, delete)
- Schema evolution support

**Usage:**
```python
from couchbase_utils.delta_lake_util.delta_lake_util import DeltaLakeUtils

delta_util = DeltaLakeUtils(
    storage_type="s3",
    credentials={
        "access_key": "...",
        "secret_key": "...",
        "region": "us-west-2"
    }
)
spark = delta_util.create_spark_session()
delta_util.create_table(table_path, schema)
```

## Storage Types

| Type | Backend | Required Credentials |
|---|---|---|
| `s3` | AWS S3 | access_key, secret_key, region |
| `gcs` | Google Cloud Storage | service_account_key, project_id |

## Dependencies

- `pyspark` — Apache Spark Python API
- `delta-spark` — Delta Lake Spark integration
- Required JARs for S3/GCS connectivity

## Version Requirements

- delta-spark >= 3.2.1
- pyspark >= 3.5.3
