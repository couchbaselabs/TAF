# Fusion State × Cluster-Operation Coverage Matrix

Gap analysis for `pytests/storage/fusion/fusion_enable_disable.py`.

**Goal:** for every Fusion lifecycle state (including the transitional ones —
`enabling`, `disabling`, `stopping`), which cluster / bucket / data operations
do we exercise *while the cluster is in that state*, and which are gaps?

The interesting bug surface is operations performed **during a transition**
(enabling / disabling / stopping) and while **stopped**, because that is where
Fusion's log-store, lease, and sync state machine is most fragile. Many
operations are already tested in the steady `enabled` state (or in other fusion
test files) but are **never crossed with a transition state** — those are marked
as gaps here.

---

## Legend

| Mark | Meaning |
|------|---------|
| ✅   | Covered in `fusion_enable_disable.py` (test abbrev. in cell; see key) |
| 🟡   | Operation is tested somewhere in the fusion suite (usually steady `enabled`) but **NOT** crossed with this state/transition |
| ❌   | Gap — not tested in this state |
| ➖   | Not applicable (e.g. fusion rebalance while fusion is disabled) |

**Fusion states (columns):** `disabled` · `enabling` · `enabled` · `disabling` · `stopping` · `stopped`

### Test abbreviation key
| Abbr | Test |
|------|------|
| EM  | test_fusion_enable_midway |
| DM  | test_disable_fusion_midway |
| DEM | test_disable_fusion_during_extent_migration |
| DDR | test_disable_fusion_during_rebalance |
| RWD | test_fusion_rebalance_while_disabling |
| PRE | test_prepare_rebalance_on_empty_bucket |
| PRR | test_prepare_rebalance_racing_bucket_creation |
| PRW | test_prepare_rebalance_during_blocked_warmup |
| DDU | test_disable_fusion_during_upload |
| RMP | test_fusion_remove_delete_permissions_log_store |
| RWE | test_fusion_rebalance_while_enabling |
| DBE | test_delete_buckets_while_enabling_fusion |
| SLE | test_fusion_second_rebalance_in_lease_expiry |
| SM  | test_stop_fusion_midway |
| SEE | test_stop_fusion_while_enabling_and_enable_again |
| SRM | test_stop_fusion_during_rebalance_or_migration |
| SDD | test_stop_or_disable_fusion_during_dcp_rebalance |
| CNB | test_create_new_buckets_after_stopping_or_disabling |
| CST | test_chaos_during_stopping_fusion |
| CSD | test_chaos_during_disabling_fusion |
| ESM | test_fusion_enable_during_storage_migration |

---

## THE MATRIX

### A. Bucket operations
| Operation | disabled | enabling | enabled | disabling | stopping | stopped |
|-----------|:--------:|:--------:|:-------:|:---------:|:--------:|:-------:|
| Create bucket | ✅ PRR/PRW | ❌ | ✅ PRE/CNB | ❌ | ✅ CNB | ✅ CNB |
| Delete / drop bucket | ✅ PRR | ✅ DBE | 🟡 | ❌ | ❌ | ❌ |
| **Flush bucket** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Manual compaction** | ❌ | ❌ | 🟡 (fusion_magma_compaction) | ❌ | ❌ | ❌ |
| Auto-compaction settings change | ❌ | ❌ | 🟡 (fusion_log_cleaning) | ❌ | ❌ | ❌ |
| Edit bucket settings (ramQuota/replica/eviction/durability/maxTTL) | ❌ | ❌ | 🟡 (fusion_replica_update) | ❌ | ❌ | ❌ |
| Storage backend migration (couchstore↔magma) | ✅ ESM (precond) | ✅ ESM | ✅ ESM | ❌ | ❌ | ❌ |
| **Encryption-at-rest enable / DEK rotation** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### B. Scope / Collection operations
| Operation | disabled | enabling | enabled | disabling | stopping | stopped |
|-----------|:--------:|:--------:|:-------:|:---------:|:--------:|:-------:|
| Create scope / collection | ❌ | ❌ | 🟡 | ❌ | ❌ | ❌ |
| **Drop collection** | ❌ | ❌ | 🟡 (fusion_magma_compaction) | ❌ | ❌ | ❌ |
| **Drop scope** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Edit collection (maxTTL / history retention) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

### C. Cluster / topology operations
| Operation | disabled | enabling | enabled | disabling | stopping | stopped |
|-----------|:--------:|:--------:|:-------:|:---------:|:--------:|:-------:|
| Fusion rebalance-in | ➖ | ✅ RWE (attempt) | ✅ EM/SLE | ✅ RWD | ✅ SRM | ✅ SM (opt) |
| Fusion rebalance-out | ➖ | ❌ | 🟡 | ❌ | 🟡 SRM | ❌ |
| Fusion swap rebalance | ➖ | ❌ | ✅ ESM | ❌ | ❌ | ❌ |
| DCP rebalance | 🟡 | ❌ | ✅ DM | ✅ SDD | ➖ | ✅ SM (opt) |
| prepare_rebalance | ✅ PRR/PRW | ✅ RWE | ✅ PRE | ✅ RWD | ❌ | ✅ SM |
| **Stop / cancel rebalance mid-flight** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Retry failed rebalance** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Graceful failover** | ❌ | ❌ | 🟡 (fusion_failover_rebalance) | ❌ | ❌ | ❌ |
| **Hard failover** | ❌ | ❌ | 🟡 (fusion_failover_rebalance) | ❌ | ❌ | ❌ |
| **Delta recovery** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Full recovery** | ❌ | ❌ | 🟡 (via ESM migration) | ❌ | ❌ | ❌ |
| Server-group (rack-zone) rebalance | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Extent migration | ➖ | ❌ | ✅ EM/SLE | ✅ DEM/DDR/CSD | ✅ SRM/CST | ➖ |
| Lease expiry / 2nd rebalance | ➖ | ❌ | ✅ SLE | ❌ | ❌ | ❌ |

### D. Node / process operations
| Operation | disabled | enabling | enabled | disabling | stopping | stopped |
|-----------|:--------:|:--------:|:-------:|:---------:|:--------:|:-------:|
| Kill memcached (chaos) | ❌ | ✅ EM | 🟡 | ✅ CSD | ✅ CST | ❌ |
| Restart couchbase-server (chaos) | ❌ | ✅ EM | 🟡 | ✅ CSD | ✅ CST | ❌ |
| **Node reboot** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Network partition | ❌ | ❌ | 🟡 (fusion_network_partition) | ❌ | ❌ | ❌ |
| Disk full | ❌ | ❌ | 🟡 (fusion_disk_full) | ❌ | ❌ | ❌ |
| Log-store delete-permission removal | ❌ | ❌ | ❌ | ✅ RMP/CSD | ❌ | ❌ |

### E. Data operations
| Operation | disabled | enabling | enabled | disabling | stopping | stopped |
|-----------|:--------:|:--------:|:-------:|:---------:|:--------:|:-------:|
| Doc load (create) | ✅ many | ✅ EM/DDU | ✅ many | 🟡 | ✅ CNB | ✅ SM |
| Update workload | ❌ | 🟡 | ✅ | ❌ | ❌ | 🟡 |
| **Expiry / TTL churn** | ❌ | ❌ | 🟡 (fusion_magma_expiry) | ❌ | ❌ | ❌ |
| **Durability writes** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Subdoc / XATTR ops | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **XDCR replication** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Backup / restore (continuous backup)** | ❌ | ❌ | 🟡 (fusion_backup_restore) | ❌ | ❌ | ❌ |

### F. Settings / storage-tuning operations
| Operation | disabled | enabling | enabled | disabling | stopping | stopped |
|-----------|:--------:|:--------:|:-------:|:---------:|:--------:|:-------:|
| Metadata / tombstone purge-interval change | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Magma history-retention change | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| num_storage_threads change | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Auto-failover settings / trigger | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Coverage summary

- **Well covered:** the transition *actions* themselves (enable/disable/stop),
  `prepare_rebalance` across most states, fusion/DCP rebalance + extent
  migration during transitions, chaos (memcached kill / server restart) during
  enabling/disabling/stopping, bucket create/delete during transitions, and
  storage-backend migration during enable.
- **Thin:** collection/scope operations, bucket-maintenance ops
  (flush/compaction/settings), and failover/recovery — mostly tested only in
  steady `enabled` (or a sibling file), never crossed with a transition state.
- **Absent entirely:** durability, XDCR, subdoc, encryption-at-rest, purge-interval
  and history-retention changes against *any* fusion state.

---

## Prioritized gaps (highest value first)

These are storage/log-store-sensitive operations that are strong candidates for
new tests, ranked by likely bug yield during a Fusion transition. 🔥 = directly
drives log-store file creation/deletion, compaction, or data movement.

1. 🔥 **Bucket flush during `disabling` / `stopping`** — mass file deletion while
   Fusion is trying to drain/sync; likely to race with log-store cleanup and lease release.
2. 🔥 **Drop collection / drop scope during `enabling` / `disabling` / `stopping`** —
   per-collection file deletion concurrent with sync/upload and extent migration.
3. 🔥 **Manual compaction during `enabling` / `disabling`** — compaction rewrites
   files exactly when Fusion is uploading/cleaning them.
4. 🔥 **Graceful & hard failover during `enabling` / `disabling` / `stopping`** —
   ownership/lease change mid-transition; currently only steady-state failover exists.
5. 🔥 **Delta & full node recovery while `enabled` and during transitions** —
   delta recovery resyncs from last mutation point (exercises log-store catch-up);
   delta recovery is untested against Fusion in *any* state.
6. 🔥 **Stop / cancel rebalance mid-flight** for a Fusion rebalance (in any state) —
   interrupts data movement + extent migration; not tested at all.
7. 🔥 **Encryption-at-rest enable + DEK rotation** while `enabled` / during a
   transition — rewrites/re-encrypts data files that Fusion is shipping.
8. **Edit bucket settings during transitions** — replica-count change (drives
   replica build/movement), eviction-policy change, maxTTL change (tombstone churn).
9. **Expiry / TTL churn during `disabling` / `stopping`** — tombstone generation
   feeding log-store cleanup while sync is winding down.
10. **Retry failed rebalance** after a Fusion rebalance failure (in any state).
11. **Purge-interval / magma history-retention change during `enabled`** — directly
    changes what the log store must retain/delete.
12. **Durability writes (persistToMajority) during transitions** — forces fsync
    while sync/upload state is changing.
13. **Node reboot (not just memcached kill) during transitions** — full warmup
    from disk while Fusion re-establishes leases/sync.
14. **Backup / restore & XDCR concurrent with a Fusion transition** — sustained DCP
    read/write streaming layered on top of log shipping.
15. **Server-group (rack-zone) rebalance with Fusion** — replica placement changes
    not tested against Fusion at all.

---

## Notes on methodology
- **States** were derived from `get_fusion_status()["state"]` values referenced in
  the test file: `disabled`, `enabling`, `enabled`, `disabling`, `stopping`, `stopped`.
- **Coverage (✅)** reflects what `fusion_enable_disable.py` actually exercises.
- **🟡** flags operations the wider fusion suite covers (e.g. `fusion_failover_rebalance.py`,
  `fusion_magma_compaction.py`, `fusion_network_partition.py`, `fusion_disk_full.py`,
  `fusion_backup_restore.py`, `fusion_magma_expiry.py`, `fusion_replica_update.py`) but
  **not** while a transition is in progress — that cross-product is the real gap.
- The operation universe was cross-checked against the TAF test framework and the
  official Couchbase 7.x/8.x documentation for completeness.
