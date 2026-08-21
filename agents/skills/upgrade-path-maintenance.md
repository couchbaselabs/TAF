---
name: upgrade-path-maintenance
description: Runbook for wiring up a new Couchbase Server release's supported upgrade paths across TAF — lib/upgrade_lib/couchbase.py, conf/upgrade/*.conf (and any other conf referencing upgrade_chain=), docs/agent-context/upgrade/upgrade-lib.md, and the QE-Test-Suites dispatcher DB. Use when a new target release (e.g. 8.2) ships and its official "Upgrade Paths" diagram needs to be reflected in TAF's upgrade test infrastructure.
---

# Upgrade Path Maintenance

## Purpose

Every new Couchbase Server release publishes an official upgrade-path diagram (e.g. "Upgrading to 8.5 | Upgrade Paths") that says which base versions can upgrade **directly** to the new target and which need one or more **intermediate hops**. TAF has to mirror that topology in four places, and they drift independently if only some are updated. This skill is the checklist for updating all four together.

## The four places that must stay in sync

1. **`lib/upgrade_lib/couchbase.py`** — the `upgrade_chains` dict. Single source of truth for what `upgrade_chain=<key>` resolves to.
2. **`conf/upgrade/*.conf`** (and stray conf files elsewhere, e.g. `conf/plasma/plasma_upgrade.conf`) — actual test entries that reference `upgrade_chain=<key>`.
3. **`docs/agent-context/upgrade/upgrade-lib.md`** — human/agent-facing reference for the registry.
4. **`QE-Test-Suites` Couchbase bucket** (via `test_infra_runner/mcp_server`, host in `qe-db` MCP config) — Jenkins dispatcher docs keyed by `confFile` + `GROUP`, with `maxVersion` / `implementedIn` fields controlling which base versions still get scheduled.

## Runbook

### 1. Get the topology from the official diagram
For each "From" band in the diagram, record: direct-to-target, or via which intermediate hop(s), ending at the latest patch of that intermediate line (e.g. "7.2.3 → latest 7.2.x").

### 2. Update `lib/upgrade_lib/couchbase.py`
- Direct-to-target bands → single-hop entries: `"A.B.C": ["A.B.C"]`.
- Multi-hop bands → key is every hop underscore-joined, value is the same ordered list: `"A.B.C_X.Y.Z_M.N.P": ["A.B.C", "X.Y.Z", "M.N.P"]`.
- Cross-check `lib/testconstants.py`'s `CB_RELEASE_BUILDS` for the full point-release list per line — don't hand-wave "6.6.x", enumerate every point release that exists (a build number of `"0000"` just means the scraper hasn't recorded a real build yet; the version is still real and enumerable — see `lib/builds/populate_versions.py`).
- **Never rename or remove an existing key without grepping every `conf/**/*.conf` for it first.** A rename silently breaks any conf still using the old key (`KeyError` at runtime, not caught until the job runs). If a key must change shape (e.g. gaining a new intermediate hop), update every conf reference in the same change.

### 3. Update conf files
- Add new `test_upgrade`/`test_upgrade_with_failover` entries for newly-supported base versions, matching the existing row shape for that component.
- If a component is being deprecated or a base version dropped for it, remove the conf lines — but don't delete the corresponding DB dispatcher docs (see step 5).

### 4. Update `docs/agent-context/upgrade/upgrade-lib.md`
- Do **not** paste the full `upgrade_chains` dict literal into the doc — it drifts from the source file. Describe the two key shapes (single-hop / multi-hop) plus a short coverage summary ("every 7.6.x", "every 6.6.x"), and point to the source file as ground truth.
- Update the "Supported upgrade paths" table's From/Via/To topology row set, without repeating exact version ranges already stated in the coverage summary.

### 5. Sync the `QE-Test-Suites` DB (dispatcher docs)
- Connect via the `qe-db` MCP server's underlying `db_utils.py` (its `_get_cluster()` helper) using the same creds it uses — the MCP tool layer itself is **read-only** for `QE-Test-Suites` (`run_custom_query` blocks UPDATE/INSERT/DELETE/UPSERT/MERGE/DROP/CREATE), so writes require a direct Couchbase SDK script, not the MCP tool.
- Query existing docs for the relevant `confFile`: `SELECT META().id, t.* FROM \`QE-Test-Suites\` t WHERE confFile = "<path>"`.
- For base versions whose conf entry was **removed**: do not delete the doc. Add `"maxVersion": "<previous release>"` — the release immediately preceding the one being wired up now (i.e. the last release this entry actually still ran for), not an older/arbitrary cutoff. Only set it if the doc doesn't already have `maxVersion` (some are already capped from an earlier retirement — leave those alone).
- For **newly added** base versions: insert new docs modeled on the nearest existing doc of the same shape, tagging `"implementedIn": "<new target release>"`.
- **Always show the exact before/after payload and get explicit confirmation before writing** — this is a shared QE database other people's jobs depend on.
- Before writing, dump the full before-state of every doc you're about to touch to a local revert file (before-state JSON for updates, doc-id list for inserts) so the change can be undone later.

### 6. If a conf file is deleted entirely
Grep all `*.md` docs for the bare filename and strip references to it — but only the filename references. Leave alone any reference to a still-existing `.py` test class in the same area (removing a `.conf` doesn't mean the test class went away too).

### 7. Report
- If a tracking Jira ticket exists for the release's upgrade-path work, post a comment summarizing the conf changes and DB changes made for each `confFile` touched.
- If tracking ticket not provided ask user permission to create a new ticket and dump the entire data for referencing + commit only using the ticket id

## Known pitfalls (from real incidents)

- Renaming `6.6.4_7.2.3` → `6.6.4_7.2.3_7.2.9` in the registry silently broke `conf/plasma/plasma_upgrade.conf`, which still referenced the old key — caught only by grepping every conf for the old key name before finalizing.
- `testconstants.CB_RELEASE_BUILDS` entries with build `"0000"` are real, manifest-listed releases with an unscraped build number — not placeholders to skip.
- The `qe-db` MCP server has no write tool for `QE-Test-Suites` by design (safety guard in `db_utils.py`). Don't try to work around it with `cb_run_query`; use a direct SDK script with the same read-only-server's credentials instead.

## After finishing this runbook

Once the new target release's upgrade paths are wired up, the conf files
touched in step 3 (and any older base-version lines that just got pushed
further into maintenance by this change) are good candidates for job
consolidation. Ask the user whether they also want to run the
`upgrade-group-consolidation` skill against the conf file(s) just edited,
to minimize the number of tests/jobs kept around for versions now in
maintenance mode.
