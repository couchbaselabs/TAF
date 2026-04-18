---
name: upgrade-utils-agent
description: >
  Utilities for upgrade operations including version migration,
  build retrieval, and node installation.
model: inherit
---

# upgrade_utils

**Upgrade utilities for Couchbase Server version migration.**
Provides helpers for upgrading clusters to new Couchbase versions.

## Files

| File | Purpose |
|---|---|
| `upgrade_util.py` | CbServerUpgrade for upgrade operations |

## Key Classes

### CbServerUpgrade
Cluster upgrade operations.

**Capabilities:**
- Retrieve build URLs and downloads
- Uninstall and install versions
- Version compatibility checks
- Feature detection for versions

**Usage:**
```python
from couchbase_utils.upgrade_utils.upgrade_util import CbServerUpgrade

upgrade_util = CbServerUpgrade(logger, "couchbase-server")
build = upgrade_util.get_build("7.6.0", remote)
upgrade_util.install_version_on_nodes(nodes, "7.6.0")
```

## Upgrade Tasks

Tasks executed in order:
1. `populate_build_url` — Get build URL
2. `check_url_status` — Validate URL
3. `download_build` — Download package
4. `uninstall` — Remove old version
5. `pre_install` — Pre-install steps
6. `install` — Install new version

## Dependencies

- `builds.build_query.BuildQuery`
- `ssh_util.install_util` for installation
- `membase.api.rest_client.RestConnection`
