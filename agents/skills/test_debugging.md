---
name: test_debugging
description: Triages and debugs TAF test failures. Phase 1 (highest priority) checks if the test existed and passed on the previous release branch, then classifies the failure as test regression or product bug — never proposing a fix. Phase 2 (on request) maps logs to source, diagnoses root cause, and proposes a fix.
uses:
  - agents/skills/source-attribution/SKILL.md
---

# Test Debugging

## Purpose

Use this skill when you have run logs and need to:
- Find which file owns a given class or test method
- Know the exact flow steps of a test method
- Identify whether a failure is in the test layer or a base class layer
- Understand what state setUp/tearDown creates/destroys
- **Triage whether a failure is a test regression or a product bug**
- Diagnose the root cause of a failure

**Source attribution is applied to all outputs** — every diagnosis cites the exact source file, line, or documentation section it was derived from. See [Output Attribution](#output-attribution) below.

---

## Phase 1 — Release Regression Triage (Run First, Highest Priority)

> **Hard constraint:** This phase never produces a code fix. It produces a classification and reasoning only. The user decides what to do next.

Run these checks before any deep debugging.

### Check 1 — Does the test exist in the previous release/branch?

```bash
# Identify the previous release branch (ask the user if unknown)
git log --oneline --all | grep -E "release|<version>" | head -5

# Check if the test method exists on that branch
git show <prev-branch>:pytests/<component>/<file>.py | grep "def <test_method>"

# Or check full file history
git log --all --follow -- pytests/<component>/<file>.py
```

**Outcomes:**

| Result | Next step |
|---|---|
| Test **does not exist** in prev branch | New test — skip to [Phase 2](#phase-2--debug--fix-workflow) |
| Test **exists** in prev branch | Proceed to Check 2 |

### Check 2 — Was the test passing on the previous branch?

Ask the user directly:

> "Was `<test_method>` passing on `<prev-branch>`? (check CI results, Jenkins history, or run it locally against that branch)"

If CI/Jenkins logs are available, look for the test name and its last known result on the previous branch.

**Outcomes:**

| Result | Next step |
|---|---|
| Was **not passing** on prev branch either | Pre-existing failure — skip to [Phase 2](#phase-2--debug--fix-workflow) |
| Was **passing** on prev branch | Proceed to Check 3 |

### Check 3 — Classify: test regression or product bug?

Compare the test file and its base classes between the two branches:

```bash
# Diff the test file between branches
git diff <prev-branch>..HEAD -- pytests/<component>/<file>.py

# Diff the base class(es)
git diff <prev-branch>..HEAD -- pytests/<component>/durability_base.py
git diff <prev-branch>..HEAD -- pytests/basetestcase.py

# Diff relevant utility files
git diff <prev-branch>..HEAD -- couchbase_utils/<util>.py
```

**Classification signals:**

| Signal | Classification |
|---|---|
| Test logic, assertions, or parameters changed between branches | **Test code regression** — test changed in a way that broke itself |
| Base class setUp/tearDown contract changed | **Test code regression** — framework layer broke the test |
| No test-side code changes but test now fails | **Product bug** — server behavior changed and the test is correctly catching it |
| New server version introduced different error codes / response shapes | **Product bug** |
| Both test code and server behavior changed | **Ambiguous** — report both possibilities, ranked by evidence |

**Output format for this phase:**

```
Triage result: <Test Regression | Product Bug | Ambiguous>

Evidence:
- <what changed in the test/base class, from <file>, line <N>>
- <what server behavior changed, from general context / logs>

Interpretation:
<one paragraph explaining why this is classified as regression vs. product bug>

Next steps (user decides):
- If test regression: review the diff in <file> and determine if the change was intentional
- If product bug: file a bug against <component> with the failure log as evidence
- If ambiguous: <specific question to resolve it>
```

> **Stop here.** Do not proceed to Phase 2 unless the user explicitly asks to go deeper into the code.

---

## Phase 2 — Debug & Fix Workflow

> Run this phase only when: (a) the user explicitly requests it, or (b) Phase 1 determined the test is new or was never passing.
> If Phase 1 classified the failure as **test regression** or **product bug**, report that result and wait — do not proceed here automatically.

When given a scenario description or raw test log, execute these steps in order:

### Step 1 — Parse the log

Extract from the log:
- **Test identifier**: `module.class.method` (e.g., `epengine.durability_success.DurabilitySuccessTests.test_with_persistence_issues`)
- **Error type**: Exception class + message (e.g., `AssertionError: task.fail is not empty`)
- **Stack frames**: list of `(file, line, function)` tuples, deepest first
- **Relevant log lines**: lines containing `ERROR`, `CRITICAL`, `WARN`, or the test's key variable names appearing near the failure

### Step 2 — Map to component and flow

1. Module prefix → find component in the **Component Files** table below
2. Class name → find the class section in the component file
3. Read the **Step-by-step Flow** for the failing test method to know which step produced the failure

### Step 3 — Identify the failure layer

Use the stack frames + Base Class Capabilities table to determine ownership:

| Frame file | Owning layer | What it means |
|---|---|---|
| `testrunner.py` | Test runner | Infra/setup failure, not test logic |
| `basetestcase.py` (`ClusterSetup`) | Cluster setup | Cluster provisioning or teardown failure |
| `basetestcase.py` (`BaseTestCase`) | Test harness | Likely `validate_test_failure()` accumulating failures |
| `durability_base.py` (`DurabilityTestsBase`) | Durability base | Error simulation or post-CRUD stat validation failed |
| `durability_base.py` (`BucketDurabilityBase`) | Bucket durability | vbucket seqno / failover stats mismatch |
| `collections_base.py` (`CollectionBase`) | Collections base | Scope/collection lifecycle or data verification failed |
| Test file itself | Test method | Logic error in the test's own steps |
| Utility file (`bucket_utils`, `cluster_utils`, etc.) | Utility layer | Framework helper bug |
| SDK client (`sdk_client3.py`) | SDK | SDK operation returned unexpected error |
| Sirius/DocLoader | Doc loader | See [sirius-java-sdk.md](test-flow-map/sirius-java-sdk.md) |

### Step 4 — Read the source

Once the layer is identified, read the exact file and method:
- Use the line numbers from the stack trace to read the failing assertion or operation
- Read 20–30 lines of context around the failure point to understand the invariant being checked
- If the error is in a base class, read the base class method fully

**Attribution:** Every finding from this step must be cited as `[from <file-path>, line <N>]`. If the finding comes from a base class method rather than the test itself, cite the base class file.

### Step 5 — Diagnose

Match the failure to a pattern:

**Assertion failures (`AssertionError`, `task.fail != {}`, empty result):**
- Did a doc operation fail? Check if the error was an expected transient (durability timeout, temp_fail) that should have been retried but wasn't `[from agents/skills/test_debugging.md, section: Diagnose]`
- Did a stat mismatch occur? Check seqno/item count expectations vs actual cluster state `[from <source-file>, line <N>]`

**Attribute / `NoneType` errors:**
- Object was not initialized — trace back to setUp to find the missing initialization step `[from <base-class-file>, line <N>]`
- Task result object is None — DocLoader/Sirius response not received `[from agents/skills/test-flow-map/sirius-java-sdk.md]`

**Timeout / hang:**
- `task_manager.get_task_result()` blocked — worker thread threw but result was never set `[from <file>, line <N>]`
- `sleep()` excessive — cluster operation took longer than expected `[from general context]`

**Skip / unexpected pass:**
- `self.skip()` called in setUp for incompatible parameter combo — check `bucket_type`, `durability_level` constraints `[from <test-file>, line <N>]`

**Import / `AttributeError`:**
- Submodule not initialized or wrong version — check `DocLoader/`, `lib/capellaAPI/`, `sirius/` `[from AGENTS.md, section: Hard Constraints]`

**Attribution rule:** Replace the placeholder `[from <file>, line <N>]` citations with the actual file path and line number read in Step 4. Where the pattern comes from this skill's own knowledge (not a live source read), use `[from agents/skills/test_debugging.md, section: Diagnose]`.

### Step 6 — Propose the fix

State the fix as:

```
Root cause: <one sentence> [from <file>, line <N>]

File: <path/to/file.py>, line <N>
Change: <what to change>

Before:                          [from <file>, line <N>]
<old code>

After:
<new code>

Why this fixes it: <one sentence> [from <source — doc, skill, or general context>]

Verification: run `python testrunner.py -i node.ini -t <test_id>` and confirm <expected outcome>
```

If multiple fixes are plausible, rank them by likelihood and label them **Fix 1 (most likely)**, **Fix 2**, etc.

**Attribution rule:** The `[from ...]` on "Root cause" and "Before" must point to the actual source read in Step 4. The `[from ...]` on "Why this fixes it" cites whichever source justified the fix — a doc section, this skill, or general engineering knowledge.

---

## How to Use (Quick Reference)

1. **Have a test name or stack frame from a log?** → Find the class in the component file below.
2. **Have a base class method in a stack trace** (e.g., `durability_base.py`, `collections_base.py`)? → Check the **Base Class Capabilities** table in the relevant component file.
3. **Unsure which component?** → Match the module path prefix:
   - `epengine.*` → [epengine.md](test-flow-map/epengine.md)
   - *(future: cbas, security, storage, upgrade)*

## Log Line → Skill Lookup

A TAF log line typically looks like:
```
Test: epengine.durability_success.DurabilitySuccessTests.test_with_persistence_issues
FAIL: AssertionError
  File "durability_base.py", line 234, in validate_durability_with_crud
  File "durability_success.py", line 89, in test_with_persistence_issues
```

Steps:
1. Module prefix → `epengine` → open `test-flow-map/epengine.md`
2. Class `DurabilitySuccessTests` → find in **durability_success.py** section
3. Stack frame crosses into `durability_base.py` → look up `validate_durability_with_crud` in the **Base Class Capabilities** table
4. Base class table says it's in `DurabilityTestsBase` and it handles error simulation + vbucket seqno → the failure is in the durability validation layer, not the test method itself
5. **Debug step**: Read `durability_base.py` around line 234 → identify the exact assertion → propose fix

---

## Component Files

| Component | File | Classes |
|-----------|------|---------|
| EP Engine / KV | [test-flow-map/epengine.md](test-flow-map/epengine.md) | `DurabilitySuccessTests`, `DurabilityFailureTests`, `TimeoutTests`, `CollectionsSuccessTests`, `CollectionDurabilityTests`, `SDKExceptionTests`, `OpsChangeCasTests`, `OutOfOrderReturns`, `KVRateLimitingTests`, `SecondaryWarmup`, `ExpiryMaxTTL`, `DocumentKeysTests`, `CreateBucketTests`, `BucketDurabilityTests` |
| Sub-Documents & XATTR | [test-flow-map/subdoc.md](test-flow-map/subdoc.md) | `BasicOps`, `SubDocTimeouts`, `DurabilityFailureTests`, `SubdocBaseTest`, `SubdocXattrSdkTest`, `SubdocXattrDurabilityTest`, `XattrTests` |
| CAS Operations | [test-flow-map/castest.md](test-flow-map/castest.md) | `CasBaseTest`, `OpsChangeCasTests` |

## Cross-Cutting Integration Layers

| Layer | File | When to use |
|-------|------|-------------|
| Sirius Java SDK (`load_docs_using=sirius_java_sdk`) | [test-flow-map/sirius-java-sdk.md](test-flow-map/sirius-java-sdk.md) | Any test failing only with `sirius_java_sdk` but passing with `default_loader`; logs show `CRITICAL Failure during get_task_result`, `argument of type 'NoneType' is not iterable`, or `Docs inserted without honoring durability` |

---

## Output Attribution

This skill applies [source-attribution](source-attribution/SKILL.md) to all outputs. Use the citation format below consistently.

### Citation sources in this skill's context

| What you're citing | Citation format |
|---|---|
| A line read from a test or base class file | `[from pytests/<component>/<file>.py, line <N>]` |
| A line read from a utility or library file | `[from lib/<file>.py, line <N>]` or `[from couchbase_utils/<file>.py, line <N>]` |
| A step-flow or base class table in this skill | `[from agents/skills/test_debugging.md, section: <Step N>]` |
| The epengine detail file | `[from agents/skills/test-flow-map/epengine.md]` |
| The Sirius Java SDK detail file | `[from agents/skills/test-flow-map/sirius-java-sdk.md]` |
| TAF framework docs (AGENTS.md, etc.) | `[from AGENTS.md, section: <name>]` |
| General engineering / Python knowledge | `[from general context]` |

### When to cite vs. not

**Always cite:**
- The specific source code line that contains the failing assertion or the buggy logic
- The component detail file (epengine.md, sirius-java-sdk.md) when its step-flow determined the failing step
- Any AGENTS.md or framework doc that constrains the fix (e.g., submodule rules)

**Use `[from general context]`:**
- Generic retry patterns, Python best practices, or standard concurrency reasoning not specific to TAF
- Inferences about cluster behavior that come from Couchbase domain knowledge, not a specific file

### Validation checklist

After producing a diagnosis and fix, verify:
- [ ] "Root cause" sentence has a `[from <file>, line <N>]` citation
- [ ] "Before" code block cites the file it was read from
- [ ] "Why this fixes it" sentence cites its justification source
- [ ] Any reference to a TAF constraint (submodule, test directory, base class contract) cites AGENTS.md or the relevant file
