---
name: upgrade-group-consolidation
description: >
  Audit and consolidate GROUP= tags in conf/upgrade/*.conf files so that
  regression jobs (Jenkins / QE-Test-Suites) can be collapsed into a small,
  fixed set of category- or version-based jobs while still covering the
  maximum number of distinct base versions. Use whenever the user asks to
  reduce/aggregate upgrade jobs, audit an upgrade .conf file against
  lib/upgrade_lib/couchbase.py, or wants "fewer jobs, more base-version
  coverage" for a maintenance-phase upgrade suite.
---

## Why this exists

Upgrade suites (`conf/upgrade/*.conf`) accumulate one line per source
build (`upgrade_chain=X.Y.Z`) over time. Once a release line goes into
maintenance, this creates two distinct kinds of waste that need two
distinct fixes — don't conflate them:

1. **Job/dispatch overhead** — running a separate Jenkins job per exact
   version is wasteful when jobs could dispatch on a coarser `GROUP=`
   token and pick up many versions at once. Fixing this (mode 2 below)
   never changes which tests run or their params — it only touches
   `GROUP=` tokens, so line count and coverage stay identical.
2. **Actual redundant test execution** — some files run the *same full
   set* of scenario variants (e.g. every `upgrade_type`) for *every*
   version, i.e. a full cross-product. In maintenance phase that's
   usually unnecessary: distributing scenarios across versions (each
   version runs one scenario, round-robin) still exercises every
   scenario repeatedly and every version at least once, for a fraction
   of the test count. Fixing this (mode 3 below) *does* change which
   `upgrade_type`/params run against which version, so it changes actual
   coverage shape, not just dispatch tags, and must be confirmed with
   the user before applying.

This skill has three modes:

1. **Audit** — read-only check of a `.conf` file for correctness/consistency
   issues that block or corrupt job consolidation, plus a report on
   whether the file has cross-product redundancy.
2. **Consolidate (dispatch)** — propose (and, if approved, apply) a
   rewrite of the `GROUP=` tags so the file maps cleanly onto a target
   list of jobs. Never touches test params or line count.
3. **Minimize (test count)** — opt-in, only when the audit finds a full
   cross-product. Propose (and, if approved, apply) a round-robin
   distribution of scenario variants across versions, cutting line count
   while preserving both full scenario coverage and full base-version
   coverage. Always confirm with the user first since this changes what
   each version is actually tested with.

## Inputs

- One or more `conf/upgrade/*.conf` files to review.
- The chain registry the file's `upgrade_chain=` values are validated
  against — normally `lib/upgrade_lib/couchbase.py` (`upgrade_chains` dict).
  If the conf file belongs to a different product surface, ask the user
  which registry file applies before assuming `couchbase.py`.
- (Consolidate mode only) the target job list, e.g.:
  `durability_upgrade (GROUP=default)`, `durability_upgrade_with_tls
  (GROUP=tls)`, `durability_upgrade_with_storage_upgrade
  (GROUP=storage_upgrade)`.

## Audit checks

For every `test_*` line with an `upgrade_chain=` param:

1. **Valid chain key** — the `upgrade_chain` value (dots, e.g. `7.6.3`)
   must exist as a key in the registry's chain dict (keys use dots;
   `GROUP` values use underscores — don't confuse the two when matching).
   Flag any value that isn't a dict key.
2. **Comment/chain consistency** — if the line (or the line(s) above it)
   has a `# Upgrade from X.Y.Z build` style comment, the version in the
   comment must match the line's actual `upgrade_chain=` value. Stale
   comments are a strong signal that a version was bumped without
   updating the label — flag every mismatch with `file:line`.
3. **GROUP scheme fit** — collect the distinct `GROUP=` values (split on
   `;`) used in the file. Check whether they map onto a small, finite
   job taxonomy (e.g. one token per intended job name) or whether they
   still encode an extra dimension (like a version tag) that no job
   filters on. Report the dimensionality found (e.g. "2 tokens per line:
   version;category" vs "1 token per line: category only").
4. **Redundant coverage** — within the same `GROUP` value, flag exact
   duplicate `upgrade_chain` versions that don't vary `upgrade_type` or
   another meaningfully distinct scenario param — these burn runtime
   without adding new base-version coverage.
5. **Min/max version alignment (always compute this from the registry —
   don't just eyeball the conf file)** — for each major line the file
   tests (e.g. `7.2`, `7.6`), compute the actual min and max chain keys
   present in the registry for that line (e.g. `couchbase.py` has
   `7.2.0`...`7.2.9` and `7.6.0`...`7.6.12`), and compare against what
   the conf file references:
   - **Max version gap (high priority)** — if the file's highest tested
     version for a line is below the registry's max for that line, flag
     it prominently. The newest patch is the single most representative
     "upgrade from" version for real-world customers on that line, so
     missing it is a coverage hole even if every other version is covered.
   - **Other gaps (lower priority)** — any other registry version in
     range never referenced by the file.
   - **Below-floor versions** — if the file references a version below
     `min_compatible_version` (a module-level constant in
     `couchbase.py`), flag it as possibly-obsolete — worth confirming
     with the user rather than silently dropping.
   - In all cases: report gaps, do not add or remove versions
     unprompted — surface the max-version gap specifically when
     proposing any consolidate/minimize rewrite so the user can decide
     whether to fold it in as part of that same edit.
6. **Full cross-product redundancy** — for each distinct `GROUP` value
   (or version family), list the set of scenario variants used per
   version (the tuple of `upgrade_type` + other scenario-distinguishing
   params like `upgrade_with_data_load`, `doc_size`, `enable_tls`,
   `test_storage_upgrade`). If *every* version in the family runs the
   *identical* full set of variants (a true cross-product — versions ×
   scenarios), flag it: this is the pattern mode 3 can fix. If different
   versions already run different variants (like
   `durability_upgrade.conf`'s design), this is fine — do not flag it.

## Consolidate checks / rewrite rules (dispatch — GROUP tokens only)

Given a target job list:

- If the target jobs dispatch on a **single token** (e.g. `GROUP=7_2` /
  `GROUP=7_6`, or `GROUP=default` / `GROUP=tls` / `GROUP=storage_upgrade`),
  and the current file encodes extra tokens the jobs will never filter on,
  strip the file down to exactly the dispatched token per line. Do not
  invent new tokens — only remove the ones that are dead weight for
  dispatch. (Testrunner's GROUP filter is an AND-match against a test's
  declared group list, so a job requesting a single token already matches
  every line carrying that token regardless of what else used to be
  alongside it.)
- Prefer **spreading distinct base versions across categories/jobs**
  rather than re-testing the same version in every category — this is
  what "test max possible base versions" means in practice: each job
  should pull in as many *different* source versions as it reasonably
  can, not duplicate the same handful everywhere.
- Never change `upgrade_chain=`, `upgrade_type=`, or any other test
  parameter while consolidating — the rewrite is scoped to `GROUP=`
  tokens and stale comments only. If a chain value looks wrong, report it
  in the audit step instead of silently "fixing" it.
- After rewriting, re-run the audit checks above on the result before
  presenting it — the rewrite must not introduce new stale
  comments or GROUP mismatches.
- Always show the resulting distinct `GROUP=` set and a one-line count of
  how many jobs that maps onto, so the user can confirm before you write
  the file.

## Minimize checks / rewrite rules (test count — only after audit check 6 flags cross-product)

Only run this when the user opts in — it changes actual test coverage
shape, unlike GROUP consolidation. Two variants; ask which the user
wants (default to sampling — it's the more aggressive cut and still
hits the registry's endpoints):

**Variant A — round-robin over existing versions.** List the distinct
scenario variants found (e.g. 4: `online_swap` w/o data load,
`online_swap`, `online_incremental`, `online_rebalance_in_out`) and the
ordered list of versions already in the file. Assign one scenario per
version, cycling through the variant list (`variants[i % len(variants)]`).
Every version that existed before still appears exactly once — no
version dropped. This alone does **not** fix a min/max gap (check 5)
since it only reshuffles what's already there — re-check registry
max/min and consider folding a gap fix in via variant B instead of
leaving it standing.

**Variant B — sample across the full registry range (preferred default
for aggressive reduction).** Instead of keeping every version already in
the file, resample directly from the registry's min/max for that major
line:
- Let `N` = number of distinct scenario variants (e.g. 4).
- For a line with registry versions indexed `0..n-1` (oldest to newest),
  pick `N` evenly spaced indices that always include both endpoints:
  `idx[i] = round(i * (n-1) / (N-1))` for `i = 0..N-1`. This guarantees
  the oldest and newest available builds are both covered, plus evenly
  spaced points in between — without testing every patch.
- Assign scenario `i` to sampled version `idx[i]` (one scenario per
  sampled version, no repeats needed when the sample count equals the
  scenario count).
- If `N` doesn't divide evenly into a clean spread, round to nearest
  index and dedupe — prefer keeping the endpoints exact even if a middle
  pick shifts by one.
- This variant deliberately does **not** iterate every available
  version — confirm with the user that trading exhaustive patch coverage
  for a representative min/mid/max spread is acceptable for this file's
  test intent (it was for `system_event_logs.conf`; it would not be for
  a file where every patch matters, e.g. one gating a specific
  regression fix per build).
- Show a before/after line count and the version → scenario mapping
  table before writing the file.
- Re-run audit checks 1–3 on the result (chain validity, comment
  consistency, GROUP scheme) before presenting it.

## Output format

**Audit report:**
```
<file>
  [chain] line N: upgrade_chain=X.Y.Z not found in <registry> upgrade_chains
  [comment] line N: comment says "X.Y.Z" but upgrade_chain=A.B.C
  [group] N distinct GROUP tokens found: <list> (dimensionality: <n>)
  [dup] line N and M: same upgrade_chain=X.Y.Z under GROUP=<g> with identical upgrade_type
  [gap] registry version X.Y.Z never referenced
  [xprod] GROUP=<g>: N versions x M scenario variants, all identical — full cross-product redundancy
```

**Consolidation proposal (dispatch):** unified diff (or Edit-tool diff)
limited to `GROUP=` tokens and comment text, plus the resulting job →
GROUP mapping table.

**Minimization proposal (test count):** before/after line count, the
scenario-variant list, and the version → scenario round-robin mapping
table, presented for confirmation before writing the file.

## Worked example (reference case)

`conf/upgrade/cbas_upgrade.conf` was already in the target shape: every
line carries a single `GROUP=7_2` or `GROUP=7_6` token, comments matched
`upgrade_chain` exactly, and versions 7.2.1–7.2.9 / 7.6.1–7.6.11 all
existed in `couchbase.py`. No rewrite was needed — 2 jobs
(`cbas_upgrade_7.2`, `cbas_upgrade_7.6`) fall directly out of the
existing tags.

`conf/upgrade/durability_upgrade.conf` carried a two-token scheme
(`GROUP=<version>;<category>`, e.g. `7_2;default`, `7_6;tls`) across 6
combinations, plus several stale comments (e.g. a line commented
"Upgrade from 7.6.9 build" that actually had `upgrade_chain=7.6.2`).
Since the target job list dispatches on category only
(`default`/`tls`/`storage_upgrade`), the version token was dropped
(`GROUP=default`, `GROUP=tls`, `GROUP=storage_upgrade`) and every stale
comment was corrected to match its line's real `upgrade_chain` value —
collapsing 6 job combinations down to 3 without touching any test
parameter or dropping any version's coverage.

`conf/upgrade/system_event_logs.conf` had a 3-token scheme
(`GROUP=P0;<major>;<exact_version>`, e.g. `P0;7_2;7_2_0`) — one distinct
token combo per exact build, plus a real bug (a `7.2.6` line tagged
`GROUP=P0;7_2;7_2_4`, a copy-paste leftover). Dispatch collapsed to
`GROUP=7_2`/`GROUP=7_6` (2 jobs), which incidentally fixed the mismatch.
Separately, audit check 6 found every version in each major line running
the identical 4-scenario cross-product (68 lines total). First pass
(variant A) round-robinned the versions already in the file down to 17
lines — but skipped audit check 5, so `couchbase.py`'s `7.2.9` and
`7.6.8`–`7.6.12` (never tested by the file) stayed uncovered. Corrected
with variant B: resampled directly from the registry's min/max per line
(`7.2`: 10 versions available → sampled `7.2.0, 7.2.3, 7.2.6, 7.2.9`;
`7.6`: 13 versions available → sampled `7.6.0, 7.6.4, 7.6.8, 7.6.12`),
one scenario per sampled version — 8 lines total, both range endpoints
covered on each line, all 4 scenarios still exercised, ~8.5x fewer
executions than the original. Lesson: always run check 5 against the
registry *before* picking a minimize variant — variant A alone can't
close a max-version gap, only variant B (or a manual add) can.
