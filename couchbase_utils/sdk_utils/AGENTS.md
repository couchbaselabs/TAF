---
name: sdk-utils-agent
description: >
  Utilities for SDK client configuration including durability options,
  transaction utilities, and SDK-specific settings.
model: inherit
---

# sdk_utils

**SDK client configuration utilities.**
Provides helpers for SDK options, durability, and transaction settings.

## Files

| File | Purpose |
|---|---|
| `sdk_options.py` | SDKOptions for SDK configuration |
| `transaction_util.py` | TransactionUtil for transaction helpers |

## Key Classes

### SDKOptions
SDK client configuration helpers.

**Capabilities:**
- Durability level configuration
- Persist/replicate settings
- Timeout duration helpers

**Usage:**
```python
from couchbase_utils.sdk_utils.sdk_options import SDKOptions

durability = SDKOptions.get_durability_level("MAJORITY")
persist = SDKOptions.get_persist_to(2)
replicate = SDKOptions.get_replicate_to(1)
timeout = SDKOptions.get_duration(30, "seconds")
```

## Durability Levels

| Level | Description |
|---|---|
| `MAJORITY` | Majority of replicas |
| `MAJORITY_AND_PERSIST_TO_ACTIVE` | Majority + active disk |
| `PERSIST_TO_MAJORITY` | Persist to majority |

## Dependencies

- `couchbase.options`
- `couchbase.durability.ServerDurability`, `DurabilityLevel`
- `couchbase.durability.ClientDurability`, `ReplicateTo`, `PersistTo`
