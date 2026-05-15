# Skills Index

↑ [agents/AGENTS.md](../AGENTS.md) → root [AGENTS.md](../../AGENTS.md)

All skills live in `agents/skills/`. Each skill has a `.md` file with YAML frontmatter and is registered in `.claude/settings.json`.

`.factory/skills/` contains `@`-reference files pointing into this directory — do not edit files there directly.

## Available Skills

### source-attribution
Adds source citations to responses indicating which TAF documentation files provided the information.

**Usage:** Automatically applied when providing guidance about TAF programming patterns, repository structure, test execution, or architecture.

**Key features:**
- Cites documentation sources (`AGENTS.md`, `docs/*`, `agents/*`)
- Distinguishes TAF-specific from general knowledge
- Provides clear traceability for recommendations

**File:** `agents/skills/source-attribution.md`

---

### test_debugging
Maps `pytests/` test execution flows to aid debugging of run logs. Given a scenario, test name, raw log output, or stack frame, resolves which layer owns the failure, reads the relevant source code, diagnoses the root cause, and proposes a concrete fix.

**Usage:** When debugging a TAF test failure — provide the scenario description or raw logs and this skill returns: which layer owns the failure, the exact step that failed, a root-cause diagnosis, and a concrete before/after code fix.

**Key features:**
- 6-step Debug & Fix workflow: parse logs → map to flow → identify layer → read source → diagnose → propose fix
- Base Class Capabilities table maps stack frames to owning layers without reading source code
- Per-test-method numbered step flows
- Failure pattern matching (assertion errors, NoneType, timeout/hang, skip, import errors)
- Structured fix proposal format with before/after code and verification command
- Covers all `pytests/epengine/` files (14 files, ~50 test methods)

**Files:**
- `agents/skills/test_debugging.md` — main entry + debug workflow + component routing
- `agents/skills/test-flow-map/epengine.md` — epengine detail
- `agents/skills/test-flow-map/sirius-java-sdk.md` — Sirius Java SDK integration layer
- `agents/skills/test-flow-map/upgrade.md` — upgrade package detail

---

### test-flow-map/upgrade
Comprehensive code map and flow reference for `pytests/upgrade/`. Covers class hierarchy, all 8 upgrade strategy methods, per-file test inventory, configuration parameters, upgrade chain config, external dependencies, and common debugging patterns.

**Usage:** Reference when debugging upgrade test failures, understanding upgrade flow, or adding new upgrade tests.

**Key features:**
- Full class hierarchy (8 test classes inheriting UpgradeBase)
- Upgrade strategy dispatch table (online_swap, incremental, failover, offline, etc.)
- `test_upgrade` step-by-step flow diagram
- All configuration parameters with defaults
- Feature-version gating table
- Common debugging patterns (loop termination, SyncWrite failures, spare_node tracking, storage migration, encryption validation)

**File:** `agents/skills/test-flow-map/upgrade.md`

---

### review-guidelines
Structured code review process for TAF changes. Produces prioritized BLOCKER/MAJOR/MINOR/NIT comments across correctness, TAF patterns, configuration, security, and code quality.

**Usage:** When asked to review a PR, diff, branch, or changed files — "review these changes", "check my changes before I commit".

**Key features:**
- 8-step review process: scope → correctness → TAF patterns → config → security → quality → docs → output
- TAF-specific checks: base class, TestInputSingleton params, tearDown cleanup, submodule boundaries
- Enforces `.conf` entry for every new `test_*` method added under `pytests/`
- Catches leftover debug prints (`print`, `pdb`, `breakpoint`) and trailing whitespace
- Severity-tiered output format (BLOCKER / MAJOR / MINOR / NIT) with file:line citations
- Quick reference checklist for full coverage at a glance

**File:** `agents/skills/review-guidelines/SKILL.md`

---

## Adding a New Skill

1. Create `agents/skills/<name>.md` with YAML frontmatter:
   ```yaml
   ---
   name: your-skill-name
   description: What this skill does
   ---
   ```
2. Register in `.claude/settings.json` under `skills`
3. Create `.factory/skills/<name>/SKILL.md` containing: `@agents/skills/<name>.md`
4. Add an entry to this index file
