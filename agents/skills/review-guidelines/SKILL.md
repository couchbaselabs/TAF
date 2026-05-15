---
name: review-guidelines
description: >-
  Structured code review process for TAF changes. Provides step-by-step
  guidance for reviewing Python test code, configuration files, and library
  changes. Use when asked to review a PR, diff, or set of changed files in TAF.
---

# Agent Review Skill

## Purpose

Provide a structured, repeatable code review process tailored to TAF conventions.
Covers correctness, test quality, framework patterns, configuration validity, and
security — producing actionable, prioritized review comments.

---

## When to Use

- "Review these changes"
- "Check my changes before I commit"
- "What's wrong with this code?"
- Reviewing any changed `.py`, `.conf`, `.ini`, or `AGENTS.md` file in TAF

---

## Review Process

### Step 1 — Understand the Scope

Before reading any code:

1. Run `git diff --stat` (or inspect the provided diff) to list changed files.
2. Group files by layer:
   - **Tests** — `pytests/**/*.py`
   - **Libraries** — `lib/**/*.py`, `couchbase_utils/**/*.py`, `platform_utils/**/*.py`
   - **Configuration** — `conf/**/*.conf`, `*.ini`
   - **Documentation** — `agents/**/*.md`, `docs/**/*.md`, `AGENTS.md`
3. Read the relevant `AGENTS.md` for each directory touched before opening `.py` files.
4. State the summary: what feature/fix this change appears to implement.

---

### Step 2 — Correctness Review

For each changed `.py` file:

**Logic & Semantics**
- [ ] Does the code do what the commit message / PR description claims?
- [ ] Are all edge cases handled (empty collections, zero counts, `None` returns)?
- [ ] Are conditional branches exhaustive — no implicit fall-throughs?
- [ ] Are loop bounds correct (off-by-one, infinite loop risk)?

**Error Handling**
- [ ] Exceptions are caught at the right level — not swallowed silently.
- [ ] Timeouts and retries are bounded; no unbounded `while True` without exit.
- [ ] Failures surface a clear error message rather than a cryptic stack trace.

**Data Integrity**
- [ ] No mutable default arguments (`def foo(x=[])`).
- [ ] Shared state (class variables, globals) is not accidentally mutated.
- [ ] Thread/task safety for anything running in parallel task workers.

---

### Step 3 — TAF Pattern Compliance

Check against TAF conventions (from `AGENTS.md`):

**Naming**
- [ ] Functions and variables: `snake_case`
- [ ] Classes: `PascalCase`
- [ ] Constants: `UPPER_SNAKE_CASE`
- [ ] Test methods: `test_` prefix
- [ ] Private helpers: `_leading_underscore`
- Exception: `setUp`, `tearDown`, `setUpClass`, `tearDownClass` are allowed.

**Test Structure**
- [ ] Test class inherits from the correct base (`OnPremBaseTest`, `ColumnarBaseTest`, etc.) based on `runtype`.
- [ ] Parameters accessed via `TestInputSingleton.input.param("name", default)` — no hard-coded values.
- [ ] `tearDown` cleans up all resources created in the test.
- [ ] No `print()` statements — use the framework logger.
- [ ] No hard-coded cluster IPs, credentials, or cloud identifiers.

**Document Loading**
- [ ] If `load_docs_using` is referenced, valid values are `default_loader`, `sirius_java_sdk`, `sirius_go_sdk`.
- [ ] `sirius_java_sdk` usage requires `--launch_java_doc_loader` flag documentation.

**Submodule Boundaries**
- [ ] No edits to `DocLoader/`, `lib/capellaAPI/`, or `sirius/` — these are maintained separately.

---

### Step 4 — Configuration File Review (`.conf` / `.ini`)

For each changed `conf/**/*.conf`:

- [ ] Each test entry follows the format: `module.class.test_method,param1=value1`
- [ ] No duplicate test entries within the same file.
- [ ] Parameter values are valid (no stray spaces, correct types).
- [ ] If a new `test_*` method was added under `pytests/`, a corresponding `.conf` entry exists that exercises it.
- [ ] If a test method was removed from `.py`, its `.conf` entry is also removed.
- [ ] `.conf` entry for a new test is in the correct component conf file (matches the module path of the test class).

For changed `*.ini` files:
- [ ] No credentials or secrets committed.
- [ ] Node topology is consistent (IP counts match role assignments).

---

### Step 5 — Security Review

- [ ] No hard-coded passwords, API keys, tokens, or secrets anywhere.
- [ ] No `shell=True` in `subprocess` calls with user-controlled input (command injection risk).
- [ ] No `eval()` or `exec()` on untrusted data.
- [ ] SSH commands use paramiko abstractions from `platform_utils/ssh_util/` — not raw shell concat.
- [ ] REST API calls use existing `cb_server_rest_util` wrappers; no ad-hoc credential embedding in URLs.

---

### Step 6 — Code Quality & Maintainability

**Readability**
- [ ] Comments explain *why*, not *what* — no comment restates the code.
- [ ] No commented-out dead code blocks left in.
- [ ] No TODO/FIXME/HACK left without a linked ticket or rationale.
- [ ] No trailing whitespace at end of any line (`grep -rn ' $' <file>` to verify).
- [ ] No leftover debug statements — `print()`, `pdb.set_trace()`, `breakpoint()`, or temporary `logging.debug("here")`-style breadcrumbs (`grep -rn 'print\|pdb\|breakpoint' pytests/` to scan).

**Duplication**
- [ ] No copy-paste of existing utility methods — use `couchbase_utils/` helpers.
- [ ] No re-implemented logic already in the base test class.
- [ ] No repeated code blocks — extract to a shared helper in the base class or `couchbase_utils/`.

**Size**
- [ ] Functions are focused — no method doing 5 unrelated things.
- [ ] Test methods test one scenario — split combined tests that assert unrelated outcomes.

**Imports**
- [ ] No unused imports.
- [ ] Imports follow: stdlib → third-party → local framework.
- [ ] No circular import dependencies — if module A imports B and B imports A, refactor shared logic into a third module (`pip install importlab` or `python -m py_compile` to surface cycles).

---

### Step 7 — Documentation & AGENTS.md

- [ ] If a new class or public method was added, its `AGENTS.md` entry is updated (if the directory has one).
- [ ] If a method was renamed or removed, all `AGENTS.md` references are updated.
- [ ] Docstrings (if present) are accurate — not copy-pasted from a different method.
- [ ] `conf/` changes are reflected in the test class's `AGENTS.md` parameter table (if one exists).

---

### Step 8 — Produce Review Output

Structure comments by severity:

| Level | When to use |
|-------|-------------|
| **BLOCKER** | Bug, security issue, data loss, or broken test — must fix before merge |
| **MAJOR** | Violates TAF pattern, missing cleanup, wrong base class — fix strongly recommended |
| **MINOR** | Style, naming, readability — fix preferred but not blocking |
| **NIT** | Cosmetic (whitespace, comment wording) — optional |

**Comment format:**

```
[BLOCKER] pytests/storage/fusion/fusion_base.py:142
tearDown does not call super().tearDown(). Cluster cleanup will be skipped on failure.
Fix: add `super().tearDown()` as the last line.

[MAJOR] conf/fusion/fusion_sanity.conf:17
Test entry references `fusion_sanity.FusionSanity.test_foo` but that method does not
exist in the class. Will silently skip at runtime.
Fix: correct method name to `test_foo_bar` or remove the entry.

[MINOR] pytests/storage/fusion/fusion_sync.py:88
Variable `bucketList` should be `bucket_list` (PEP8 snake_case).

[NIT] pytests/storage/fusion/fusion_sync.py:92
Comment "# loop over buckets" restates the code. Remove.
```

End with a one-line summary:
```
Overall: N blockers, N major, N minor, N nits. [Ready to merge / Needs fixes before merge.]
```

---

## Quick Reference Checklist

```
Scope
  [ ] Changed files identified and grouped by layer
  [ ] Relevant AGENTS.md files read before .py files

Correctness
  [ ] Logic matches intent
  [ ] Edge cases handled
  [ ] No silent exception swallowing

TAF Patterns
  [ ] Naming conventions followed
  [ ] Correct base class
  [ ] Parameters via TestInputSingleton
  [ ] tearDown cleans up resources
  [ ] No hard-coded credentials or IPs
  [ ] Submodule boundaries respected

Configuration
  [ ] Every new test_* method in pytests/ has a .conf entry
  [ ] .conf entries match actual test methods
  [ ] No duplicate entries

Security
  [ ] No secrets committed
  [ ] No shell injection vectors

Quality
  [ ] No dead code or stale comments
  [ ] No leftover debug prints (print, pdb, breakpoint)
  [ ] No trailing whitespace
  [ ] No duplicated utility logic
  [ ] Imports clean (no unused, no circular dependencies)

Documentation
  [ ] AGENTS.md updated if public API changed
```
