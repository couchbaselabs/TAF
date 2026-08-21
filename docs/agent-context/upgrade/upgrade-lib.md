# upgrade-lib reference — `lib/upgrade_lib/couchbase.py`

Single source of truth for supported upgrade chains and feature-version mappings.
Read this when: adding a new chain key, checking valid base versions, or understanding `_populate_upgrade_chain()`.

---

## `features` dict

Cumulative: `get_supported_features("7.2.3")` returns all features from 6.5 through 7.2.

```python
features = {
    "6.5": ["durability"],
    "7.0": ["collections"],
    "7.1": ["magma", "system_event_logs"],
    "7.2": ["cdc"],
    "8.0": ["durability_impossible_fallback"],
    "8.5": ["file_based_rebalance", "10K_collections",
            "fusion", "rate_limiting", "jwt_auth", "pitr"]
}
```

`min_compatible_version = "7.1"` — clusters below this are not guaranteed supported by the test framework.

---

## `upgrade_chains` dict

Keys are the `upgrade_chain` param value. Values are ordered intermediate-hop lists.
`upgrade_version` param is **appended** as the final target by `_populate_upgrade_chain()`.
The live key list is `lib/upgrade_lib/couchbase.py` itself — don't copy it here, it drifts.
Two shapes exist:

```python
# Single-hop — direct upgrade, key == its own value
"7.6.0": ["7.6.0"],

# Multi-hop — key is every hop underscore-joined, value is the same list
"6.5.2_6.6.5_7.2.3_7.2.9": ["6.5.2", "6.6.5", "7.2.3", "7.2.9"],
```

Coverage today (see the source file for the exact key list):
- Single-hop direct-to-target: `7.2.9`, every `7.6.x` (through `7.6.12`), and `8.0.0`–`8.0.2`.
- Multi-hop via `7.2.3` → `7.2.9` (latest 7.2.x): every `7.1.x`, `7.0.0`, every `6.6.x`.
- Multi-hop via `6.6.5` → `7.2.3` → `7.2.9`: every `6.0.x`, every `6.5.x`.

`8.0.x` stops at `8.0.2` (wired for `conf/upgrade/cbas_upgrade.conf`) even though
`CB_RELEASE_BUILDS` (testconstants.py) lists `8.0.3`–`8.0.5` — those exist in the release
manifest but aren't yet wired into the upgrade-chain registry. Add them once they're
actually exercised by a conf file.

---

## How `_populate_upgrade_chain()` assembles the chain

```python
# UpgradeBase.setUp → _populate_upgrade_chain()
chain_to_test   = self.input.param("upgrade_chain", "7.2.3")
upgrade_version = self.input.param("upgrade_version", "8.0.0-1000")
self.upgrade_chain  = upgrade_chains[chain_to_test] + [upgrade_version]
self.upgrade_version = self.upgrade_chain[0]   # initial install version
```

Example: `upgrade_chain=6.6.5_7.2.3`, `upgrade_version=8.5.0-500`
→ `self.upgrade_chain = ["6.6.5", "7.2.3", "8.5.0-500"]`

The outer loop in `test_upgrade` iterates each entry and calls `upgrade_function[upgrade_type](node)` until `fetch_node_to_upgrade()` returns `None`.

---

## Supported upgrade paths to 8.5 (Totoro)

From `upgrade_path.png` — conf files must only contain entries for these paths (exact key
ranges are in the "Coverage today" list above, not repeated here):

| From | Via | To |
|---|---|---|
| 8.0.x | direct | 8.5 |
| 7.6.x | direct | 8.5 |
| 7.2.9 | direct | 8.5 |
| 7.1.x, 7.0.x, 6.6.x | 7.2.3 → 7.2.9 | 8.5 |
| 6.0.x, 6.5.x | 6.6.5 → 7.2.3 → 7.2.9 | 8.5 |

**NOT supported** (direct to 8.5): 7.1.x single-hop, 7.2.0–7.2.8 single-hop.

---

## Adding a new chain key

1. Append to `upgrade_chains` in `lib/upgrade_lib/couchbase.py`.
2. Key naming convention: `"A.B.C"` for single-hop, `"A.B.C_X.Y.Z_M.N.P"` for multi-hop.
3. Build URL resolution: GA format `"7.2.3"` → `/builds/releases/`; pre-release `"7.2.3-1234"` → `/builds/latestbuilds/`. Do **not** strip the build number for pre-release initial versions.
4. Add corresponding test entries to the relevant `conf/upgrade/*.conf` file.
5. Update the supported paths table above if the target major version changes.

> **When a whole new release's upgrade paths need wiring up** (not just one chain key) — new
> base versions, changed hop requirements, or a component's support being dropped — use the
> [`upgrade-path-maintenance`](../../../agents/skills/upgrade-path-maintenance.md) skill. It
> covers this file plus the conf files and the `QE-Test-Suites` dispatcher DB together, since
> they drift if updated separately.
