---
name: couchbase-utils-agent
description: >
  Utilities for Couchbase Server and Capella operations. Each util folder
  contains feature-specific helpers for test automation.
model: inherit
---

# couchbase_utils

**Feature-specific utilities for Couchbase Server and Capella test automation.**
Contains high-level helpers built on top of `cb_server_rest_util` and other
framework components.

## Utils Overview

| Util | Description | AGENTS.md |
|---|---|---|
| `backup_utils/` | Backup/restore operations using cbbackupmgr, cbcollect | [AGENTS.md](backup_utils/AGENTS.md) |
| `bucket_utils/` | Bucket CRUD, scope/collection management, bucket validation | [AGENTS.md](bucket_utils/AGENTS.md) |
| `capella_utils/` | Capella cloud utilities: columnar, dedicated, serverless environments | [AGENTS.md](capella_utils/AGENTS.md) |
| `cb_server_rest_util/` | **Canonical REST API wrapper** — thin HTTP wrappers for all Couchbase REST endpoints | [AGENTS.md](cb_server_rest_util/AGENTS.md) |
| `cb_tools/` | CB CLI tools: cbbackupmgr, cbimport, cbstats, cbepctl, cbcollect | [AGENTS.md](cb_tools/AGENTS.md) |
| `cbas_utils/` | Analytics/Columnar service: CBAS queries, datasets, links | [AGENTS.md](cbas_utils/AGENTS.md) |
| `cluster_utils/` | Cluster operations, encryption, cluster readiness checks | [AGENTS.md](cluster_utils/AGENTS.md) |
| `dcp_utils/` | DCP stream operations, DCP client helpers | [AGENTS.md](dcp_utils/AGENTS.md) |
| `delta_lake_util/` | Delta Lake integration for data lake operations | [AGENTS.md](delta_lake_util/AGENTS.md) |
| `eventing_utils/` | Eventing service: function deployment, lifecycle management | [AGENTS.md](eventing_utils/AGENTS.md) |
| `fts_utils/` | Full-text search: FTS indexes, query operations | [AGENTS.md](fts_utils/AGENTS.md) |
| `index_utils/` | GSI index operations, index stats, Plasma storage | [AGENTS.md](index_utils/AGENTS.md) |
| `kafka_util/` | Kafka connector utilities: Confluent, MSK, Mol17 | [AGENTS.md](kafka_util/AGENTS.md) |
| `nfs_utils/` | NFS mount operations for remote storage | [AGENTS.md](nfs_utils/AGENTS.md) |
| `node_utils/` | Node management: add/remove nodes, node stats | [AGENTS.md](node_utils/AGENTS.md) |
| `rbac_utils/` | RBAC: user/role management, permissions | [AGENTS.md](rbac_utils/AGENTS.md) |
| `rebalance_utils/` | Rebalance operations, retry logic, cluster topology changes | [AGENTS.md](rebalance_utils/AGENTS.md) |
| `sdk_utils/` | SDK client options, transaction utilities | [AGENTS.md](sdk_utils/AGENTS.md) |
| `security_utils/` | Security: TLS, certificates, JWT, audit, encryption | [AGENTS.md](security_utils/AGENTS.md) |
| `storage_utils/` | Storage engine utilities: Magma, storage validation | [AGENTS.md](storage_utils/AGENTS.md) |
| `tpch_utils/` | TPC-H benchmark utilities for performance testing | [AGENTS.md](tpch_utils/AGENTS.md) |
| `transaction_utils/` | Distributed transaction helpers | [AGENTS.md](transaction_utils/AGENTS.md) |
| `upgrade_utils/` | Upgrade operations: version migration, compatibility | [AGENTS.md](upgrade_utils/AGENTS.md) |
| `xdcr_utils/` | XDCR: replication, references, remote clusters | [AGENTS.md](xdcr_utils/AGENTS.md) |

## Architecture

```
couchbase_utils/
├── cb_server_rest_util/    ← Pure REST wrappers (no business logic)
│   └── Used by all other utils
└── <feature>_utils/        ← Feature helpers with validation, retries, logic
    └── May call cb_server_rest_util + add test-ready abstractions
```

**Key distinction:**
- `cb_server_rest_util/` — Thin HTTP wrappers, returns `(status, content)`, no parsing
- Other `*_utils/` — Business logic, validation, retries, test-friendly APIs

## Adding New Utilities

1. Create `<feature>_utils/` directory
2. Add `__init__.py` (empty or exports)
3. Create utility files following `snake_case` naming
4. Add AGENTS.md with agent rules if complex logic exists
5. Update this file with description and link
