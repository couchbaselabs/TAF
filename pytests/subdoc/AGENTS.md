---
name: subdoc-agent
description: >
  Sub-document and XATTR functional tests — durability, timeout, failure,
  SDK xattr operations, and MB regression coverage.
model: inherit
---

# pytests/subdoc

Sub-document and XATTR test suite. Validates subdoc INSERT/UPSERT/REMOVE/READ semantics,
XATTR correctness at SDK and cluster scale, durability under error injection, and timeout behavior.

↑ [pytests/](../basetestcase.py) → root [AGENTS.md](../../AGENTS.md)

---

## Files

| File | Class(es) | Purpose |
|---|---|---|
| `subdoc_xattr.py` | `SubdocBaseTest`, `SubdocXattrSdkTest`, `SubdocXattrDurabilityTest`, `XattrTests` | Base + SDK-level xattr correctness + durability impossibility/in-progress + cluster-scale xattr storage scenarios |
| `sub_doc_success.py` | `BasicOps` | Positive subdoc path: INSERT/UPSERT/REMOVE/READ with and without error injection |
| `sub_doc_failures.py` | `SubDocTimeouts`, `DurabilityFailureTests` | Negative subdoc path: timeout and durability-impossible/in-progress failures |

---

## Test Flow Map

Full class hierarchy, per-test step-by-step flows, base-class capability table, and failure
pattern reference:

**→ [`agents/skills/test-flow-map/subdoc.md`](../../agents/skills/test-flow-map/subdoc.md)**

---

## Maintenance Rules

### When adding a new file or test method to this directory

1. **Check the test-flow-map for constraints before writing code.**
   The flow map documents invariants that must be preserved:
   - `SubdocXattrDurabilityTest.tearDown` closes `self.client` SDKClient before calling `super()` — new subclasses of `SubdocBaseTest` that open an SDKClient must follow the same pattern.
   - `DurabilityTestsBase.tearDown` disconnects all shell connections — subclass tearDown must call `super().tearDown()`.
   - Failures accumulate via `log_failure()` and raise only at `validate_test_failure()` in `BaseTestCase.tearDown` — do not call `self.fail()` mid-test for recoverable errors.
   - `sub_doc_success.py` mutated-value read-back uses specific expected `mutated` values (1 after INSERT, 2 after UPSERT/REMOVE on same doc) — new tests must match this contract.
   - `test_subdoc_multi_max_paths` documents the valid range for `subdoc_multi_max_paths` and expected error strings — changes to the internal setting name or error message must be reflected in both the test and the flow map.

2. **Update [`agents/skills/test-flow-map/subdoc.md`](../../agents/skills/test-flow-map/subdoc.md)** to reflect:
   - New file → add a row to the Files table and a new section with chain, setUp notes, and per-test flow table.
   - New test method → add a row to the relevant test class table with key params and step-by-step flow.
   - New base class or setUp change → update the Base Class Capabilities table.
   - New failure pattern → add a row to the Common Failure Patterns table.

### When modifying an existing file

1. Re-read the corresponding section in the flow map to verify the change doesn't break a documented invariant.
2. Update the flow map if the step-by-step flow, params, or failure patterns change.
