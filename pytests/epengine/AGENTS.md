---
name: epengine-agent
description: >
  EP Engine (KV data service) functional tests — durability, collections,
  CAS, out-of-order returns, expiry, rate limiting, warmup, and document keys.
model: inherit
---

# pytests/epengine

KV data-service test suite. Validates durability semantics, collection-scoped CRUD,
CAS consistency, OOO returns, TTL/expiry, rate limiting, and secondary warmup.

↑ [pytests/](../basetestcase.py) → root [AGENTS.md](../../AGENTS.md)

---

## Files

| File | Class(es) | Purpose |
|---|---|---|
| `durability_base.py` | `DurabilityTestsBase`, `BucketDurabilityBase` | Base classes only — no test methods. Provides target-node selection, error-sim helpers, cbstats wiring, Java SDK client init. |
| `durability_success.py` | `DurabilitySuccessTests` | Positive durability path: errors injected but CRUDs must still succeed |
| `durability_failures.py` | `DurabilityFailureTests`, `TimeoutTests` | Negative durability path: validates expected exceptions + vbucket seqno frozen during error windows |
| `collection_crud_success.py` | `CollectionsSuccessTests` | Positive collection-scoped CRUD with optional error injection |
| `collection_crud_negative.py` | `CollectionDurabilityTests` | Negative collection-scoped durability |
| `collection_sdk_exceptions.py` | `SDKExceptionTests` | Collection-not-found and timeout exception handling |
| `bucket_level_durability.py` | `CreateBucketTests`, `BucketDurabilityTests` | Bucket-level durability config across all durability levels and bucket types |
| `opschangecas.py` | `OpsChangeCasTests` | CAS change tracking across update/delete/expiry/touch/rebalance/failover |
| `ooo_returns.py` | `OutOfOrderReturns` | Out-of-order return ordering validation under sync writes and transactions |
| `rate_limiting_tests.py` | `KVRateLimitingTests` | KV throttle/reject limit enforcement |
| `secondary_warmup.py` | `SecondaryWarmup` | Secondary warmup behavior (background vs blocking) under CRUD load |
| `expiry_maxttl.py` | `ExpiryMaxTTL` | maxTTL semantics, doc TTL vs bucket TTL precedence, metadata purge |
| `documentkeys.py` | `DocumentKeysTests` | CRUD and views/DCP with whitespace, binary, and unicode doc keys |
| `basic_ops.py` | Multiple | General KV CRUD, concurrent mutations, doc-size variations |

---

## Test Flow Map

Full class hierarchy, per-test step-by-step flows, base-class capability table, and failure
pattern reference:

**→ [`agents/skills/test-flow-map/epengine.md`](../../agents/skills/test-flow-map/epengine.md)**

---

## Maintenance Rules

### When adding a new file or test method to this directory

1. **Check the test-flow-map for constraints before writing code.**
   The flow map documents invariants that must be preserved:
   - `DurabilityTestsBase.tearDown` disconnects all `vbs_in_node` shell connections — subclass tearDown must call `super().tearDown()`.
   - `BucketDurabilityBase.tearDown` disconnects per-node shell connections — do not close them earlier.
   - Failures accumulate via `log_failure()` and raise only at `validate_test_failure()` in `BaseTestCase.tearDown` — do not call `self.fail()` mid-test for recoverable errors.
   - `init_java_clients()` must be called per bucket before any `sirius_java_sdk` load task — see [`sirius-java-sdk.md`](../../agents/skills/test-flow-map/sirius-java-sdk.md).
   - `verify_vbucket_details_stats()` requires a baseline `verification_dict` built before any mutations.

2. **Update [`agents/skills/test-flow-map/epengine.md`](../../agents/skills/test-flow-map/epengine.md)** to reflect:
   - New file → add a row to the Files table and a new section with chain, setUp notes, and per-test flow table.
   - New test method → add a row to the relevant test class table with key params and step-by-step flow.
   - New base class or setUp change → update the Base Class Capabilities table.
   - New failure pattern → add a row to the Common Failure Patterns table.

### When modifying an existing file

1. Re-read the corresponding section in the flow map to verify the change doesn't break a documented invariant.
2. Update the flow map if the step-by-step flow, params, or failure patterns change.
