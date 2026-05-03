---
name: castest-agent
description: >
  CAS (Compare-And-Swap) value correctness tests — validates CAS semantics
  across update, delete, expiry, touch, rebalance, failover, restart,
  backup/restore, and corrupted CAS recovery.
model: inherit
---

# pytests/castest

CAS test suite. Validates CAS value semantics at SDK and cbstats level across
mutation types, cluster events, and conflict resolution scenarios.

↑ [pytests/](../basetestcase.py) → root [AGENTS.md](../../AGENTS.md)

---

## Files

| File | Class(es) | Purpose |
|---|---|---|
| `cas_base.py` | `CasBaseTest` | Base class + basic config/restart/failover/backup tests |
| `opschangecas.py` | `OpsChangeCasTests` | CAS change tracking helpers and MB regression tests (no `test_*` methods — helpers called by subclasses or conf entries) |

---

## Test Flow Map

Full class hierarchy, per-test step-by-step flows, `verify_cas()` logic per op type,
`_corrupt_max_cas()` setup, and failure pattern reference:

**→ [`agents/skills/test-flow-map/castest.md`](../../agents/skills/test-flow-map/castest.md)**

---

## Maintenance Rules

### When adding a new file or test method to this directory

1. **Check the test-flow-map for constraints before writing code.**
   The flow map documents invariants that must be preserved:
   - `CasBaseTest.setUp` configures KV mem quota via REST before bucket creation — new subclass setUp must call `super().setUp()` before any bucket operations.
   - `_check_config()` asserts `conflictResolutionType == 'lww'` — tests that create buckets with a different CR type must not call `_check_config()` at teardown without overriding.
   - `OpsChangeCasTests` has **no `test_*` methods** — all logic is in shared helpers (`ops_change_cas`, `touch_test`, `key_not_exists_test`, `corrupt_cas_is_healed_*`). Adding test methods here breaks the class contract; add a new subclass instead.
   - `_corrupt_max_cas()` uses `setWithMetaInvalid` to push CAS to -2 then two `set()` calls to reach -1 (MB-17517 setup). Do not simplify this — the two-step approach is required by the server bug reproduction path.
   - CAS retry in `verify_cas("update", ...)` retries cbstats replica CAS check up to 5× — do not reduce this; replication lag under load is expected.

2. **Update [`agents/skills/test-flow-map/castest.md`](../../agents/skills/test-flow-map/castest.md)** to reflect:
   - New file → add a row to the Files table and a new section with chain, setUp notes, and per-test flow table.
   - New test method or helper → add a row to the relevant class table with key params and flow.
   - New `verify_cas` op type → add a row to the `verify_cas` logic table.
   - New failure pattern → add a row to the Common Failure Patterns table.

### When modifying an existing file

1. Re-read the corresponding section in the flow map to verify the change doesn't break a documented invariant.
2. Update the flow map if the step-by-step flow, params, or failure patterns change.
