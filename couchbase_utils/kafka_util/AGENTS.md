---
name: kafka-util-agent
description: >
  Utilities for Kafka connector operations including Confluent, AWS MSK,
  and Mol17 deployments for CDC and non-CDC data streaming.
model: inherit
---

# kafka_util

**Kafka connector utilities for data streaming.**
Provides helpers for deploying and managing Kafka connectors with Couchbase.

## Files

| File | Purpose |
|---|---|
| `kafka_connect_util.py` | KafkaConnectUtil for connector management |
| `confluent_utils.py` | Confluent Cloud utilities |
| `msk_utils.py` | AWS MSK utilities |
| `molo17_utils.py` | Mol17 Kafka utilities |
| `common_utils.py` | Shared Kafka utilities |

## Key Classes

### KafkaConnectUtil
Kafka connector deployment and management.

**Capabilities:**
- Deploy/undeploy connectors
- Connector plugin management
- Configuration templates for various sources

**Usage:**
```python
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil

kafka_util = KafkaConnectUtil()
kafka_util.create_connector(
    connect_url="http://localhost:8083",
    connector_name="mongo-cdc",
    config=ConnectorConfigTemplate.MongoConfigs.debezium["cdc"]
)
```

## Connector Types

| Type | Source | Use Case |
|---|---|
| `debezium` | MongoDB | CDC with change streams |
| `non-cdc` | MongoDB | Direct document streaming |

## Supported Platforms

| Platform | Port | File |
|---|---|---|
| Confluent CDC | 8083 | `confluent_utils.py` |
| Confluent Non-CDC | 8082 | `confluent_utils.py` |
| AWS MSK CDC | 8084 | `msk_utils.py` |
| AWS MSK Non-CDC | 8085 | `msk_utils.py` |

## Dependencies

- `capellaAPI.capella.lib.APIRequests`
- Kafka Connect REST API
