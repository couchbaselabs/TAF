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
    "8.1": ["file_based_rebalance", "10K_collections",
            "fusion", "rate_limiting", "jwt_auth", "pitr"]
}
```

`min_compatible_version = "7.1"` — clusters below this are not guaranteed supported by the test framework.

---

## `upgrade_chains` dict

Keys are the `upgrade_chain` param value. Values are ordered intermediate-hop lists.
`upgrade_version` param is **appended** as the final target by `_populate_upgrade_chain()`.

```python
upgrade_chains = {
    # Single-hop (no intermediate stop needed)
    "7.1.0": ["7.1.0"],  "7.1.1": ["7.1.1"],  "7.1.2": ["7.1.2"],
    "7.1.3": ["7.1.3"],  "7.1.4": ["7.1.4"],  "7.1.5": ["7.1.5"],  "7.1.6": ["7.1.6"],

    "7.2.0": ["7.2.0"],  "7.2.1": ["7.2.1"],  "7.2.2": ["7.2.2"],
    "7.2.3": ["7.2.3"],  "7.2.4": ["7.2.4"],  "7.2.5": ["7.2.5"],
    "7.2.6": ["7.2.6"],  "7.2.7": ["7.2.7"],  "7.2.8": ["7.2.8"],  "7.2.9": ["7.2.9"],

    "7.6.0": ["7.6.0"],  "7.6.1": ["7.6.1"],  "7.6.2": ["7.6.2"],
    "7.6.3": ["7.6.3"],  "7.6.4": ["7.6.4"],  "7.6.5": ["7.6.5"],
    "7.6.6": ["7.6.6"],  "7.6.7": ["7.6.7"],

    # Multi-hop (intermediate stop mandated by upgrade guide)
    "6.6.4_7.2.3": ["6.6.4", "7.2.3"],
    "6.6.5_7.2.3": ["6.6.5", "7.2.3"],
    "6.6.5_7.2.0": ["6.6.5", "7.2.0"],
    "7.0.0_7.1.0": ["7.0.0", "7.1.0"],
    "7.0.0_7.1.5": ["7.0.0", "7.1.5"],
}
```

---

## How `_populate_upgrade_chain()` assembles the chain

```python
# UpgradeBase.setUp → _populate_upgrade_chain()
chain_to_test   = self.input.param("upgrade_chain", "7.2.3")
upgrade_version = self.input.param("upgrade_version", "8.0.0-1000")
self.upgrade_chain  = upgrade_chains[chain_to_test] + [upgrade_version]
self.upgrade_version = self.upgrade_chain[0]   # initial install version
```

Example: `upgrade_chain=6.6.5_7.2.3`, `upgrade_version=8.1.0-500`
→ `self.upgrade_chain = ["6.6.5", "7.2.3", "8.1.0-500"]`

The outer loop in `test_upgrade` iterates each entry and calls `upgrade_function[upgrade_type](node)` until `fetch_node_to_upgrade()` returns `None`.

---

## Adding a new chain key

1. Append to `upgrade_chains` in `lib/upgrade_lib/couchbase.py`.
2. Key naming convention: `"A.B.C"` for single-hop, `"A.B.C_X.Y.Z"` for multi-hop.
3. Build URL resolution: GA format `"7.2.3"` → `/builds/releases/`; pre-release `"7.2.3-1234"` → `/builds/latestbuilds/`. Do **not** strip the build number for pre-release initial versions.
4. Add corresponding test entries to the relevant `conf/upgrade/*.conf` file.
