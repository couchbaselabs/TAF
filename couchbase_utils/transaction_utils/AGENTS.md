---
name: transaction-utils-agent
description: >
  Utilities for distributed transaction operations including
  multi-document ACID transactions.
model: inherit
---

# transaction_utils

**Transaction utilities for distributed ACID operations.**
Provides helpers for multi-document transaction execution.

## Files

| File | Purpose |
|---|---|
| `transactions.py` | Transaction class for transaction operations |

## Key Classes

### Transaction
Distributed transaction operations.

**Capabilities:**
- Set collection context
- Create documents in transaction
- Update documents in transaction
- Read documents in transaction
- Delete documents in transaction
- Execute queries in transaction

**Usage:**
```python
from couchbase_utils.transaction_utils.transactions import Transaction

Transaction.set_collection(scope, collection)
Transaction.create(ctx, doc_gen)
Transaction.update(ctx, doc_gen)
Transaction.delete(ctx, doc_gen)
```

## Transaction Operations

| Method | Purpose |
|---|---|
| `set_collection` | Set target scope/collection |
| `create` | Create documents atomically |
| `update` | Update documents atomically |
| `read` | Read documents in transaction |
| `delete` | Delete documents atomically |
| `query` | Execute N1QL in transaction |

## Dependencies

- Couchbase SDK transaction support
