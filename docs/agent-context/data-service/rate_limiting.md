# AGENT SPEC: KV Data Service Rate-Limiting

## 🏗SYSTEM CONTEXT
- **Feature:** Per-bucket throttling for KV Data Service.
- **Version:** Available in Couchbase Server 8.1.0+ (Totoro).
- **Scope:** Enterprise Edition Only.
- **Topology:** Per-node configuration with global capacity pool.

---

## ⚙️ PARAMETERS & SCHEMA

### 1. Node-Level (Global)
- `throttleEnabled` (Bool): Master toggle for the node.
- `capacity` (Uint64): Total Bytes/sec pool available for all buckets.
- `default_throttle_reserved_units` (Uint64): Inherited by new buckets if not specified.
- `default_throttle_hard_limit` (Uint64): Inherited by new buckets if not specified.

### 2. Bucket-Level (Local)
- `throttle_reserved` (Uint64): Guaranteed Bytes/sec reserved for the bucket.
  - Range: `0` to `18446744073709551615` ($2^{64}-1$).
- `throttle_hard_limit` (Uint64): Absolute ceiling for the bucket.
  - Range: `0` to `18446744073709551615` ($2^{64}-1$).

---

## 🚦 FEATURE REQUIREMENTS & LOGIC

### 1. Throttling Behavior [P0]
- **Induced Latency:** Primary method of throttling.
- **Back-pressure (Rejection):** If the internal buffer/queue grows too large, ops are rejected with `EWouldThrottle`.
- **SDK Interaction:** SDKs must recognize `EWouldThrottle` and perform automatic retries.

### 2. Client & DCP Filtering [P0]
- **Always Excluded (No Throttle):**
  - Internal DCP: XDCR (`sdk="Goxdcr"`), Backup/Restore (`cbbackupmgr`).
  - Recovery operations (`cbdatarecovery`).
- **Configurable (User Choice):**
  - `cbexport`, `GSI`, `Connectors`.
  - *Note:* `cbimport` uses standard SET/DELETE and is throttled by default if enabled.

### 3. Validation Invariants
Agents must enforce these logical constraints:
- **Hierarchy Constraint**: `throttle_reserved` $\le$ `throttle_hard_limit`.
- **Sum Constraint**: $\sum (\text{all } throttle\_reserved) \le \text{Node } capacity$.
- **Default Constraint**: `default_throttle_reserved_units` $\le$ `capacity`.
- **Creation Logic**: Bucket creation MUST fail if (Current Total Reserved + New Bucket Reserved) > Node capacity.
- **Inheritance**: If params are omitted during create_bucket, defaults are applied from the Node configuration.
- **Persistence**: Bucket configs (including throttle settings) must be preserved during Backup/Restore (`--enable-bucket-config`). [P1]

---

## 💻 AUTOMATION INTERFACES

### REST API (BucketManageAPI)
```python
from couchbase_utils.cb_server_rest_util.buckets.manage_bucket import BucketManageAPI
rest = BucketManageAPI(cluster_node_server_obj)

# Update bucket limits
rest.edit_bucket(bucket.name, {
    "throttle_reserved": 5000,
    "throttle_hard_limit": 6000
})
```

### Couchbase CLI (CbCli)
```python
from couchbase_utils.cb_tools.cb_cli import CbCli
cb_cli = CbCli(RemoteMachineShellConnection(server_obj))

# Creation: --reserved <val>, --hard-limit <val>
cb_cli.create_bucket({
    "name": "bucket1",
    "throttle_reserved": 5000,
    "throttle_hard_limit": 6000
})

# Modification
cb_cli.edit_bucket(bucket.name, throttle_reserved=5000, throttle_hard_limit=6000)
```

---

## 📊 OBSERVABILITY (KV STATS)
- **ru_total**: Counter for Read unit consumption per second.
- **wu_total**: Counter for Write unit consumption per second.
- **throttle_enabled**: Boolean status of feature on the node.
- **throttle_count_total**: Number of requests being throttled.
- **reject_count_total**: Number of requests being rejected.
- **throttle_duration**: Duration throttled commands spent in throttled state.

---

## 🧪 TEST STRATEGY & SUITE
Objective: Validate end-to-end enforcement of rate-limiting across REST, CLI, and KV Engine, ensuring zero impact on critical system services (XDCR/Backup).

### 1. API & Tooling (REST/CLI) [P0]
Goal: Ensure configuration consistency across interfaces.

- REST/CLI Integration: Add to `pytests/buckettests/createbuckettests.py`.
  - Validate throttle_enabled, node_capacity, and unit_sizes schema.
  - Verify bucket creation with --reserved and --hard-limit flags.
  - Validate 400 Bad Request for invalid ranges (>$2^{64}-1$) or logic violations.
  - Test "Community Edition" block: Ensure CLI returns clear error messages on CE.

### 2. Core Functional & "Corner Case" Logic [P0]
Goal: Validate the "Soft vs. Hard" limit mechanics and back-pressure.
- **Soft Limit Enforcement**: Drive load > throttle_reserved but < capacity. Verify no throttling occurs until global pool is empty.
- **Hard Limit Enforcement**: Drive load > throttle_hard_limit. Verify immediate latency/throttling regardless of pool availability.
- **Back-pressure (The P0 Guard)**: Fill the throttling queue until rejection occurs. Verify SDK receives EWouldThrottle.
- **Capacity Over-subscription**: Attempt to create/edit a bucket where ∑reserved>capacity. Verify atomic failure.

### 3. Resilience & Cluster Operations [P1]
Goal: Integrate into existing high-availability test suites.
- **Rebalance/Failover**: Add rate-limiting plugin tests to `pytests/rebalance_new/` and `pytests/failover/`. Perform rebalance/failover while buckets are actively being throttled. Verify limits persist on destination nodes.
- **Upgrade Path**: Mixed Mode (8.0/8.1): Verify feature reports as "Disabled" on 8.1 nodes until cluster-wide upgrade.
  - Verify config persistence post-upgrade.
- **Migration**: Test Couchstore ↔ Magma migration under active throttling.

### 4. Integration & Service Exclusions [P0/P1]
Goal: Protect internal traffic from user-defined limits.
- **Zero-Impact Services**: Explicitly verify Goxdcr, cbbackupmgr, and cbdatarecovery are NOT throttled by default.
- **Configurable Services**: Toggle throttling for GSI, Connectors (Kafka), and cbexport.
- **N1QL Transactions**: Verify transactions rollback/commit gracefully under throttle-induced latency.

---

## 🛠 INTEGRATION MAP

| Test Category | Target File/Location | Action |
|---------------|----------------------|--------|
| REST/CLI Validation | `pytests/buckettests/createbuckettests.py` | Add CRUD validation for throttle params and --reserved/--hard-limit flags |
| Dedicated Throttling Tests | `pytests/epengine/rate_limiting_tests.py` | **NEW FILE**: Complete rate-limiting test suite (P0/P1 requirements) |
| Rebalance Tests | `pytests/rebalance_new/` | Add rate-limiting plugin tests to existing rebalance test files |
| Failover Tests | `pytests/failover/` | Add rate-limiting plugin tests to existing failover test files |
| Upgrade Tests | `pytests/upgrade/kv_upgrade_tests.py` | Add 8.1 feature-gate checks to existing upgrade tests |

## 📊 VALIDATION CHECKLIST

The Agent must ensure every test case includes these verification steps:
- **Metric Check**: Verify `kv_throttle_ops_rejected` > 0 during back-pressure tests
- **SDK Check**: Verify `EWouldThrottle` error code is caught and retried
- **Persistence Check**: Verify bucket properties are retained after `cbbackupmgr --enable-bucket-config`
