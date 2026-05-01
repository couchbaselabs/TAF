# Fusion Billing Test Plan

## Overview

This test plan covers end-to-end billing validation for fusion-enabled (Express Scaling) clusters on
Capella Dedicated. Fusion billing has two independent components:

- **Fixed billing** — charged hourly per active accelerator node; covers SSD storage and per-bucket
  overages above the free tier. Controlled by the feature flag `ff-billing-fusion-enabled`.
- **Variable billing** — charged once per rebalance event; covers per-GiB of data downloaded by
  accelerator nodes onto guest volumes. Triggered when an accelerator node calls `/fusion/complete`.

Tests validate correctness end-to-end: cluster creation → rebalance → node completion → CP database
records → billing job execution → Salesforce variable record. The control plane Couchbase database
is used as the primary verification layer for all billing records.

The control plane runs its own Couchbase Server cluster (separate from any customer cluster) that
stores all Capella metadata including billing documents. All billing verification in this test plan
is done by querying this control plane Couchbase instance directly.

---

## Terminology

| Term | Definition |
|------|-----------|
| **Accelerator node** | Temporary spot instance that downloads shard data from S3 onto a guest volume during a fusion rebalance |
| **PlanUUID** | Unique identifier for a rebalance plan; used as the aggregation key for variable billing |
| **PagerTask** | Couchbase document written to the `billing.pagerTask` collection in the control plane database when an accelerator node completes its download. It is a durable, idempotent billing intent record — it decouples the real-time node completion event from the slower asynchronous Salesforce write. The collection lives in the control plane Couchbase cluster under the `billing` scope, `pagerTask` collection. Documents auto-expire after 1 year via TTL. The same collection is also used by AI workflow billing (not just fusion). |
| **FusionBreakdown** | Struct embedded in a node's `HourlyBillingRecord` containing SSD cost, SSD uplift, EBS list price, and bucket cost |
| **Variable record** | Salesforce-bound document created by the pager billing job; one per `clusterID + planUUID`. Written to the control plane Couchbase database before being forwarded to Salesforce. |
| **CSP uplift** | Region-specific multiplier applied to the variable rate to account for cost differences by AWS region. Factor name pattern: `Accelerator CSP Uplift - AWS {region}` (e.g. `Accelerator CSP Uplift - AWS us-east-1`). One factor must exist per billable AWS region. |
| **Phone home record** | Any billing-related document written to the CP database: HourlyBillingRecord, PagerTask, or variable record. The term "phone home" refers to billing data that the cluster or control plane writes back to the central billing system. |
| **EBSListPrice** | AWS list price per GB/month for a GP3 EBS volume. GP3 is the disk type always used by AWS accelerator nodes regardless of the cluster's own disk configuration. This is the list price before the SSD uplift multiplier is applied. |
| **HourlyBillingRecord** | Couchbase document written to the control plane database once per hour per active node by the fixed billing job. Contains the full `FusionBreakdown` (SSD cost, bucket cost, uplift values) for that billing hour. |
| **Pager billing job** | A batch background job that reads all pending PagerTask documents from `billing.pagerTask`, aggregates them by `clusterID + planUUID`, computes the variable cost, and writes variable records. Runs on a schedule but can be triggered manually in tests. |

---

## Billing Formulas

| Component | Formula |
|-----------|---------|
| SSD hourly cost | `(diskSizeGb × EBSListPrice / 730) × SSDUplift` |
| Bucket hourly cost | `max(0, bucketCount − freeTier) × bucketGlobalPrice` |
| Variable rebalance cost | `totalGiBProcessed × fusionVariablePrice × cspRegionUplift` |
| GiB conversion | `ShardSizeInBytes / (1024 × 1024 × 1024)` |

---

## CP Database Collections

All billing verification is done by querying the control plane Couchbase database directly. The
control plane Couchbase cluster uses a `billing` scope that holds all billing-related collections.
Below are the four collections relevant to fusion billing tests:

| Collection | Scope | Written By | Key / Query | What to verify |
|-----------|-------|-----------|-------------|---------------|
| `billing.pagerTask` | `billing` scope, `pagerTask` collection in the CP Couchbase cluster | `RecordNodeBilling()` called from `/fusion/complete` handler | `fusionDetails.nodeID` (dedup key — one document per accelerator node) | PagerTask created with correct nodeID, planUUID, shardSizeInBytes, tenantID, clusterID, region, provider, downloadCompletedAt |
| `billing.credit_factors` | `billing` scope, `credit_factors` collection in the CP Couchbase cluster | FinOps / billing admin tooling | `category = "Express Scaling (Fusion)"` | All required factors exist with non-zero multipliers and valid effective dates |
| `HourlyBillingRecord` | CP Couchbase cluster | Fixed billing job (runs hourly) | `clusterID + hour` | `FusionEnabled = true`, `FusionBreakdown` contains non-zero SSD cost; bucket cost matches bucket count |
| Variable record | CP Couchbase cluster | Pager billing job (batch, processes PagerTasks) | `tenantID + clusterID + category + region + start` (deterministic ID) | `CreditQuantity`, `BillingBreakdown.Usage` (GiB), `UsageUnit = "GiB"`, `Category = "Fusion 2"`, `ActivationID` |

> **Note on `billing.pagerTask`**: The document key is the PagerTask `ID` (a UUID), but the
> deduplication logic uses `fusionDetails.nodeID` as the uniqueness field — querying by nodeID is
> the correct way to check whether a billing record already exists for an accelerator node. The
> pager billing job reads all unprocessed PagerTask documents, so verifying documents in this
> collection confirms that variable billing will be triggered on the next job run.

---

## Billing Factors (Prerequisites)

Before running any billing tests, verify the following factors exist in `billing.credit_factors`
under `category = "Express Scaling (Fusion)"`:

| Factor Name | Used By |
|-------------|---------|
| `SSD Uplift` | Fixed — disk cost multiplier |
| `Buckets - Free Tier` | Fixed — number of free buckets |
| `Buckets - Global Price` | Fixed — per-bucket credit price |
| `Accelerator Global Rate - per GiB` | Variable — base rate per GiB |
| `Accelerator CSP Uplift - AWS {region}` | Variable — region multiplier for each AWS region (e.g. `Accelerator CSP Uplift - AWS us-east-1`, `Accelerator CSP Uplift - AWS eu-west-1`) |

---

## Test Scenarios

---

### 1. Fixed Billing — SSD Cost

#### 1.1 SSD Cost Billed Correctly on AWS
**Test ID**: `test_fixed_billing_ssd_cost_aws`

**Context**: The fixed billing job runs once per hour and writes an `HourlyBillingRecord` document
to the control plane Couchbase database for every active accelerator node. The SSD cost is the
primary fixed charge: it reflects what Couchbase pays the cloud provider for the EBS volume attached
to the accelerator node, multiplied by an uplift factor. The divisor `730` converts a monthly GB
price to an hourly rate (730 hours/month). AWS accelerator nodes always use GP3 disks regardless of
the cluster's disk configuration.

**Steps**:
1. Create a fusion-enabled cluster on AWS (e.g. `us-east-1`)
2. Trigger a fusion rebalance; allow an accelerator node to complete download and transition to active
3. Confirm feature flag `ff-billing-fusion-enabled` is `true` in the environment
4. Wait for the hourly fixed billing job to run (or trigger manually)
5. Query CP database: retrieve the `HourlyBillingRecord` for the accelerator node for that hour
6. From `billing.credit_factors` (CP database), retrieve the current values of `SSD Uplift` and
   the GP3 EBS list price for `us-east-1`
7. Compute expected SSD cost: `(diskSizeGb × EBSListPrice / 730) × SSDUplift`
8. Compare `HourlyBillingRecord.Debug.FusionCosts.FusionSSDCost` against the computed value

**Expected Result**:
- `HourlyBillingRecord.FusionEnabled = true`
- `FusionBreakdown.FusionSSDCost` matches the computed value (within floating-point tolerance)
- `FusionBreakdown.EBSListPrice` matches the GP3 rate for `us-east-1`
- `FusionBreakdown.FusionSSDUplift` matches the factor value

**CP Database Checks**:
- [ ] HourlyBillingRecord exists for the node and billing hour
- [ ] `FusionEnabled = true`
- [ ] `FusionBreakdown.FusionSSDCost > 0`
- [ ] `FusionBreakdown.EBSListPrice` matches GP3 rate for region
- [ ] `FusionBreakdown.FusionSSDUplift` matches `SSD Uplift` factor

```sql
-- Retrieve the HourlyBillingRecord for the accelerator node
SELECT b.fusionEnabled,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDCost,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDUplift,
       b.debug.fusionCosts.fusionBreakdown.ebsListPrice
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.nodeId = '<nodeID>'
ORDER BY b.billingHour DESC
LIMIT 1;

-- Retrieve the SSD Uplift factor value to compare against
SELECT multiplier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name = 'SSD Uplift';
```

---

#### 1.2 SSD Cost Billed Correctly in a Different AWS Region
**Test ID**: `test_fixed_billing_ssd_cost_aws_region_variance`

**Context**: GP3 EBS pricing varies by AWS region. This test confirms the fixed biller fetches the
rate for the cluster's actual region rather than a hardcoded default.

**Steps**:
1. Create a fusion-enabled cluster on AWS in a region with a different GP3 price to `us-east-1`
   (e.g. `ap-southeast-1`)
2. Trigger a fusion rebalance; allow an accelerator node to complete
3. Run the hourly fixed billing job
4. Query `billing.credit_factors` for the GP3 rate specific to that region
5. Query HourlyBillingRecord; verify `FusionBreakdown.EBSListPrice` matches the region's GP3 rate

**Expected Result**:
- `EBSListPrice` reflects the GP3 rate for `ap-southeast-1`, not `us-east-1`
- SSD cost is computed with the region-correct price

**CP Database Checks**:
```sql
-- Get the GP3 rate for the target region from billing factors
SELECT name, multiplier FROM `capella`.`billing`.`credit_factors`
WHERE name LIKE '%gp3%'
AND region = 'ap-southeast-1';

-- Verify the HourlyBillingRecord uses that rate
SELECT b.debug.fusionCosts.fusionBreakdown.ebsListPrice,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDCost
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.clusterId = '<clusterID>'
ORDER BY b.billingHour DESC
LIMIT 1;
```

---

#### 1.3 Feature Flag Off — No Fixed Billing
**Test ID**: `test_fixed_billing_feature_flag_disabled`

**Context**: The feature flag `ff-billing-fusion-enabled` gates the entire fixed billing calculation.
When off, the fixed billing job still runs and still writes an `HourlyBillingRecord`, but the
`FusionBreakdown` inside it will have all costs set to zero. This allows the flag to be toggled in
production without leaving gaps in the billing record history — the records exist, they just show
zero fusion cost. This is distinct from the variable billing path, which has no such feature flag.

**Steps**:
1. Disable feature flag `ff-billing-fusion-enabled` in the environment
2. Create a fusion-enabled cluster; trigger a rebalance; allow accelerator node to become active
3. Wait for (or trigger) the hourly billing job
4. Query CP database for the HourlyBillingRecord for that node and hour

**Expected Result**:
- `FusionBreakdown.FusionSSDCost = 0`
- `FusionBreakdown.FusionBucketCost = 0`
- No non-zero fusion costs present in the record

**CP Database Checks**:
- [ ] HourlyBillingRecord exists
- [ ] `FusionBreakdown.FusionSSDCost = 0`
- [ ] `FusionBreakdown.FusionBucketCost = 0`

```sql
-- Verify all fusion costs are zero when the feature flag is off
SELECT b.nodeId,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDCost,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketCost
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.nodeId = '<nodeID>'
ORDER BY b.billingHour DESC
LIMIT 1;
```

---

#### 1.4 SSD Cost Accumulates Correctly Over Multiple Hours
**Test ID**: `test_fixed_billing_ssd_cost_multi_hour`

**Steps**:
1. Create a fusion-enabled cluster on AWS; trigger a rebalance
2. Allow the accelerator node to remain active for at least 3 billing hours
3. For each hour, retrieve the HourlyBillingRecord from the CP database
4. Verify SSD cost is consistent across all hours (same disk size, same factors)
5. Verify there are no gaps — every hour the node was active has a billing record

**Expected Result**:
- SSD cost per hour is identical across all hourly records for the same disk configuration
- No hourly records missing for the active period

```sql
-- Retrieve all hourly records for the node ordered by billing hour
SELECT b.billingHour,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDCost,
       b.debug.fusionCosts.fusionBreakdown.ebsListPrice,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDUplift
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.nodeId = '<nodeID>'
ORDER BY b.billingHour ASC;
```

---

### 2. Fixed Billing — Bucket Cost

#### 2.1 No Bucket Charge Within Free Tier
**Test ID**: `test_fixed_billing_bucket_within_free_tier`

**Steps**:
1. Query `billing.credit_factors` in the CP database to get current `Buckets - Free Tier` value (e.g. N)
2. Create a fusion-enabled cluster on AWS with a number of buckets ≤ N
3. Trigger a rebalance; allow the accelerator node to become active
4. Wait for (or trigger) the hourly fixed billing job
5. Query HourlyBillingRecord from CP database

**Expected Result**:
- `FusionBreakdown.FusionBucketCost = 0`
- `FusionBreakdown.FusionBucketFreeAlloc` equals the free tier factor value
- `FusionBreakdown.FusionSSDCost > 0` (SSD still billed independently)

**CP Database Checks**:
- [ ] `FusionBucketCost = 0`
- [ ] `FusionBucketFreeAlloc` matches `Buckets - Free Tier` factor

```sql
-- Get the free tier value
SELECT multiplier AS freeTier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name = 'Buckets - Free Tier';

-- Verify bucket cost is zero in the HourlyBillingRecord
SELECT b.debug.fusionCosts.fusionBreakdown.fusionBucketCost,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketFreeAlloc
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.nodeId = '<nodeID>'
ORDER BY b.billingHour DESC
LIMIT 1;
```

---

#### 2.2 Bucket Charge Applied Above Free Tier
**Test ID**: `test_fixed_billing_bucket_above_free_tier`

**Steps**:
1. Query `billing.credit_factors` in the CP database to get current `Buckets - Free Tier` (N) and
   `Buckets - Global Price` (P)
2. Create a fusion-enabled cluster on AWS with buckets > N (e.g. N + 2)
3. Trigger a rebalance; allow the accelerator node to become active
4. Wait for (or trigger) the hourly fixed billing job
5. Query HourlyBillingRecord; compute expected bucket cost: `(bucketCount - N) × P`

**Expected Result**:
- `FusionBreakdown.FusionBucketCost = (bucketCount - N) × P`
- `FusionBreakdown.FusionBucketFreeAlloc = N`
- `FusionBreakdown.FusionBucketGlobalPrice = P`

**CP Database Checks**:
- [ ] `FusionBucketCost > 0`
- [ ] Value matches `(bucketCount − freeTier) × globalPrice`
- [ ] `FusionBucketFreeAlloc` and `FusionBucketGlobalPrice` populated

```sql
-- Get both bucket factors in one query
SELECT name, multiplier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name IN ['Buckets - Free Tier', 'Buckets - Global Price'];

-- Verify the bucket cost breakdown in the HourlyBillingRecord
SELECT b.debug.fusionCosts.fusionBreakdown.fusionBucketCost,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketFreeAlloc,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketGlobalPrice
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.nodeId = '<nodeID>'
ORDER BY b.billingHour DESC
LIMIT 1;
```

---

#### 2.3 Bucket Cost Updates When Bucket Count Changes
**Test ID**: `test_fixed_billing_bucket_cost_dynamic`

**Steps**:
1. Create a fusion-enabled cluster with buckets below free tier; verify `FusionBucketCost = 0`
2. Add buckets to push the count above the free tier; wait for next billing hour
3. Query the new HourlyBillingRecord; verify `FusionBucketCost > 0` reflects the additional buckets

**Expected Result**:
- Billing correctly reflects the bucket count at the time of each hourly record

---

### 3. Variable Billing — PagerTask Creation

#### 3.1 PagerTask Written on Node Completion
**Test ID**: `test_variable_billing_pager_task_created`

**Context**: When an accelerator node finishes downloading its shard from S3, it calls
`POST /clusters/{clusterId}/nodes/{nodeId}/fusion/complete` on the cp-internal-api. The handler
calls `RecordNodeBilling()` which writes a PagerTask document to the `billing.pagerTask` collection
in the control plane Couchbase database. This is a fire-and-forget write — if it fails, the error
is logged but the `/complete` call still succeeds (billing is best-effort at this stage). The
PagerTask is the only signal the pager billing job has that a rebalance charge is due; if it is
missing, no variable billing occurs for that node.

**Steps**:
1. Create a fusion-enabled cluster; trigger a fusion rebalance
2. Note the accelerator node ID (`nodeID`) and rebalance plan UUID (`planUUID`) from the cluster state
3. Allow the accelerator node to complete its download (calls `POST /fusion/complete`)
4. Query the `billing.pagerTask` collection in the CP Couchbase database for the document with
   `fusionDetails.nodeID = <nodeID>`

**Expected Result**:
- PagerTask document exists
- `fusionDetails.nodeID` matches the accelerator node ID
- `fusionDetails.planUUID` matches the rebalance plan UUID
- `fusionDetails.shardSizeInBytes > 0`
- `fusionDetails.downloadCompletedAt` is non-zero and recent
- `fusionDetails.registeredAt` is before `downloadCompletedAt`
- `tenantID`, `clusterID`, `projectID`, `projectName`, `region` are correct
- `usageCategory = "Fusion 2"`

**CP Database Checks**:
- [ ] PagerTask document exists with correct `nodeID`
- [ ] `planUUID` matches current rebalance
- [ ] `shardSizeInBytes > 0`
- [ ] `downloadCompletedAt` is non-zero
- [ ] `tenantID` and `clusterID` correct
- [ ] `usageCategory = "Fusion 2"`
- [ ] `billingBreakdown.provider` and `billingBreakdown.region` correct

```sql
-- Verify the PagerTask was created for the accelerator node
-- Note: query by fusionDetails.nodeID (the dedup key), not the document key
SELECT p.id,
       p.tenantId,
       p.clusterId,
       p.region,
       p.usageCategory,
       p.fusionDetails.planUUID,
       p.fusionDetails.nodeID,
       p.fusionDetails.shardSizeInBytes,
       p.fusionDetails.registeredAt,
       p.fusionDetails.downloadCompletedAt,
       p.billingBreakdown.provider
FROM `capella`.`billing`.`pagerTask` p
WHERE p.fusionDetails.nodeID = '<nodeID>';
```

---

#### 3.2 PagerTask Is Idempotent — No Duplicate on Repeated Complete
**Test ID**: `test_variable_billing_pager_task_idempotent`

**Context**: The pager infrastructure uses `fusionDetails.nodeID` as a deduplication key when
writing to `billing.pagerTask`. If a document already exists with the same `nodeID`, the write is
a no-op. This protects against double-billing if `/fusion/complete` is retried (e.g. due to a
network timeout where the first call succeeded but the response was lost). The dedup check is a
N1QL query against the collection before the KV write.

**Steps**:
1. Create a fusion-enabled cluster; trigger a rebalance; allow an accelerator node to complete
2. Verify one PagerTask document exists for the `nodeID` (see 3.1)
3. Simulate a second call to `/fusion/complete` for the same `nodeID` (e.g. retry scenario)
4. Query `billing.pagerTask` again for the same `nodeID`

**Expected Result**:
- Exactly one PagerTask document exists — the second call is a no-op
- No duplicate billing record created
- `shardSizeInBytes` is unchanged

**CP Database Checks**:
- [ ] Exactly one document exists for the `nodeID`

```sql
-- Must return exactly 1; any value > 1 means duplicate billing records exist
SELECT COUNT(*) AS taskCount
FROM `capella`.`billing`.`pagerTask`
WHERE fusionDetails.nodeID = '<nodeID>';
```

---

#### 3.3 Multiple Nodes, Single Rebalance — One PagerTask Per Node
**Test ID**: `test_variable_billing_pager_task_per_node`

**Steps**:
1. Create a fusion-enabled cluster; trigger a rebalance that spawns multiple accelerator nodes
   (e.g. 3 nodes across 3 shards, each with a different shard size)
2. Allow all nodes to complete their downloads
3. Query `billing.pagerTask` for all documents where `clusterId = <clusterID>`
   and `fusionDetails.planUUID = <planUUID>`

**Expected Result**:
- One PagerTask per accelerator node (e.g. 3 documents for 3 nodes)
- Each document has a unique `nodeID` and its own `shardSizeInBytes`
- All documents share the same `planUUID`

**CP Database Checks**:
- [ ] Document count matches number of accelerator nodes in the rebalance
- [ ] Each has a unique `nodeID`
- [ ] All share the same `planUUID`
- [ ] Sum of `shardSizeInBytes` across all tasks equals total data moved in the rebalance

```sql
-- Retrieve all PagerTasks for a given rebalance; count and sum shard sizes
SELECT COUNT(*) AS nodeCount,
       SUM(p.fusionDetails.shardSizeInBytes) AS totalShardBytes,
       ARRAY_AGG(p.fusionDetails.nodeID) AS nodeIDs
FROM `capella`.`billing`.`pagerTask` p
WHERE p.clusterId = '<clusterID>'
AND p.fusionDetails.planUUID = '<planUUID>';
```

---

### 4. Variable Billing — Charge Calculation

#### 4.1 Variable Cost Correct for Single-Node Rebalance
**Test ID**: `test_variable_billing_cost_single_node`

**Context**: The pager billing job reads PagerTask documents from `billing.pagerTask`, groups them
by `clusterID + planUUID` (the aggregation key), sums `shardSizeInBytes` across all tasks in the
group, converts bytes to GiB (`÷ 1024³`), and multiplies by the variable price and CSP uplift.
It then writes one variable record per group. The variable record is the document that eventually
gets forwarded to Salesforce for customer invoicing. Both the base price and the uplift are stored
separately in `BillingBreakdown` for auditability.

**Steps**:
1. Create a fusion-enabled cluster on AWS (`us-east-1`); trigger a rebalance with one accelerator node
2. Record `shardSizeInBytes` from the accelerator node state or PagerTask document
3. Allow the pager billing job to run
4. Query CP database for the variable record with `clusterID` and `planUUID`
5. From `billing.credit_factors`, get `Accelerator Global Rate - per GiB` (price) and
   `Accelerator CSP Uplift - AWS us-east-1` (uplift)
6. Compute: `GiB = shardSizeInBytes / (1024^3)`, `expected cost = GiB × price × uplift`

**Expected Result**:
- `variableRecord.CreditQuantity` matches computed cost (within floating-point tolerance)
- `variableRecord.BillingBreakdown.Usage` = GiB value
- `variableRecord.BillingBreakdown.UsageUnit = "GiB"`
- `variableRecord.BillingBreakdown.BasePrice` = fusionVariablePrice factor
- `variableRecord.BillingBreakdown.Uplift` = cspRegionUplift factor
- `variableRecord.Category = "Fusion 2"`

**CP Database Checks**:
- [ ] Variable record exists for `clusterID + planUUID`
- [ ] `CreditQuantity` matches `GiB × price × uplift`
- [ ] `BillingBreakdown.Usage` matches GiB conversion from `shardSizeInBytes`
- [ ] `UsageUnit = "GiB"`
- [ ] `BasePrice` and `Uplift` factors stored separately in breakdown

```sql
-- Get the variable price and AWS uplift factor to use in manual cost calculation
SELECT name, multiplier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name IN ['Accelerator Global Rate - per GiB',
             'Accelerator CSP Uplift - AWS us-east-1'];

-- Retrieve the variable record written by the pager billing job
-- The record ID is deterministic: derived from tenantID + clusterID + category + region + start time
SELECT v.creditQuantity,
       v.activationId,
       v.plan,
       v.category,
       v.billingBreakdown.usage,
       v.billingBreakdown.usageUnit,
       v.billingBreakdown.basePrice,
       v.billingBreakdown.uplift,
       v.billingBreakdown.region,
       v.billingBreakdown.provider
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
```

---

#### 4.2 Variable Cost Aggregated Correctly for Multi-Node Rebalance
**Test ID**: `test_variable_billing_cost_multi_node_aggregation`

**Steps**:
1. Create a fusion-enabled cluster; trigger a rebalance with multiple accelerator nodes (e.g. 3 nodes)
2. Record `shardSizeInBytes` for each node from the PagerTask documents
3. Allow the pager billing job to run
4. Query the variable record for `clusterID + planUUID`
5. Compute expected total GiB: `Σ(shardSizeInBytes[i]) / (1024^3)` across all nodes
6. Compute expected cost: `totalGiB × price × uplift`

**Expected Result**:
- **Exactly one** variable record exists for the `clusterID + planUUID` combination
- `variableRecord.BillingBreakdown.Usage` equals the sum of GiB across all nodes
- `variableRecord.CreditQuantity` = `totalGiB × price × uplift`

**CP Database Checks**:
- [ ] Only one variable record per `clusterID + planUUID`
- [ ] `Usage` = sum of all node GiB values
- [ ] `CreditQuantity` matches total cost formula

```sql
-- Sum shardSizeInBytes across all PagerTasks for this rebalance to derive expected GiB
SELECT SUM(p.fusionDetails.shardSizeInBytes) / (1024 * 1024 * 1024) AS expectedGiB
FROM `capella`.`billing`.`pagerTask` p
WHERE p.clusterId = '<clusterID>'
AND p.fusionDetails.planUUID = '<planUUID>'
AND p.fusionDetails.downloadCompletedAt IS NOT NULL
AND p.fusionDetails.downloadCompletedAt != '';

-- Confirm exactly one variable record exists for this rebalance
SELECT COUNT(*) AS recordCount,
       v.billingBreakdown.usage AS actualGiB,
       v.creditQuantity AS cost
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
```

---

#### 4.3 Multiple Rebalances on Same Cluster — Separate Variable Records
**Test ID**: `test_variable_billing_separate_records_per_rebalance`

**Steps**:
1. Create a fusion-enabled cluster; trigger Rebalance A (record planUUID-A)
2. Allow all nodes to complete; wait for pager billing job to run; verify variable record for planUUID-A
3. Trigger Rebalance B on the same cluster (record planUUID-B); allow nodes to complete; run job
4. Query variable records for `clusterID`

**Expected Result**:
- Two separate variable records exist for the same `clusterID` — one per `planUUID`
- Each record has the correct `Usage` (GiB) for its respective rebalance

**CP Database Checks**:
- [ ] Two variable records for the cluster with distinct `planUUID` values
- [ ] Each record's `Usage` matches only its rebalance's shard data

```sql
-- Retrieve all variable records for a cluster to confirm one per planUUID
SELECT v.billingBreakdown.planUUID,
       v.billingBreakdown.usage AS giB,
       v.creditQuantity AS cost,
       v.usageDate
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
ORDER BY v.usageDate ASC;
```

---

#### 4.4 AWS Regional Uplift Applied Correctly
**Test ID**: `test_variable_billing_aws_regional_uplift`

**Context**: Each AWS region has its own `Accelerator CSP Uplift - AWS {region}` factor. This test
confirms the correct region factor is applied by running two clusters in different AWS regions with
the same shard size and verifying the cost difference matches the ratio of their uplift factors.

**Steps**:
1. Create two fusion-enabled clusters on AWS — one in `us-east-1`, one in `eu-west-1`
2. Trigger a rebalance on each with identical shard sizes
3. From `billing.credit_factors`, retrieve the uplift for each region
4. After the pager job runs, retrieve variable records for both clusters
5. Verify: `cost_eu / cost_us ≈ uplift_eu / uplift_us`

**Expected Result**:
- `BillingBreakdown.Uplift` in each record matches its region's factor value
- Cost ratio between regions equals the uplift ratio (base GiB and price are the same)

**CP Database Checks**:
- [ ] `BillingBreakdown.Uplift` for `us-east-1` record matches `Accelerator CSP Uplift - AWS us-east-1` factor
- [ ] `BillingBreakdown.Uplift` for `eu-west-1` record matches `Accelerator CSP Uplift - AWS eu-west-1` factor

```sql
-- Retrieve uplift factors for both regions
SELECT name, multiplier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name IN ['Accelerator CSP Uplift - AWS us-east-1',
             'Accelerator CSP Uplift - AWS eu-west-1'];

-- Compare variable records for both clusters side by side
SELECT v.databaseId,
       v.billingBreakdown.region,
       v.billingBreakdown.usage AS giB,
       v.billingBreakdown.uplift,
       v.billingBreakdown.basePrice,
       v.creditQuantity AS cost
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId IN ['<clusterID-us-east-1>', '<clusterID-eu-west-1>'];
```

---

### 5. Variable Billing — Edge Cases

#### 5.1 Tenant Without Salesforce Activation — FreeTier Fallback
**Test ID**: `test_variable_billing_no_activation_freetier_fallback`

**Context**: Before writing a variable record, the pager billing job looks up the tenant's current
Salesforce activation from the CP database to determine the billing plan name and `activationID`
to embed in the record. If no activation is found (`ErrNotFound`), the job does **not** skip the
charge — it still writes the variable record but tags it with `plan = FreeTier` and
`activationID = "notFound"`. It also emits a Datadog gauge metric (`billing.fusion.activation.not_found`)
so ops can detect and investigate. This fallback ensures billing intent is preserved even if the
activation lookup is temporarily inconsistent.

**Steps**:
1. Identify a tenant with no current Salesforce activation record in the CP database
2. Create a fusion-enabled cluster under that tenant; trigger a rebalance; allow node to complete
3. Run the pager billing job
4. Query the variable record for `clusterID + planUUID`

**Expected Result**:
- Variable record is still written (billing is not suppressed)
- `variableRecord.Plan` reflects FreeTier naming
- `variableRecord.ActivationID = "notFound"`

**CP Database Checks**:
- [ ] Variable record exists
- [ ] `ActivationID = "notFound"`
- [ ] `Plan` contains FreeTier identifier
- [ ] Datadog metric `billing.fusion.activation.not_found` emitted (verify via Datadog)

```sql
-- Verify the variable record exists and has the FreeTier fallback values
SELECT v.tenantId,
       v.databaseId,
       v.activationId,
       v.plan,
       v.creditQuantity,
       v.billingBreakdown.usage
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
-- Expected: activationId = "notFound", plan contains "FreeTier"
```

---

#### 5.2 Zero ShardSize — PagerTask Written, Zero-Cost Record
**Test ID**: `test_variable_billing_zero_shard_size`

**Steps**:
1. Force an accelerator node to complete with `shardSizeInBytes = 0` (e.g. empty shard scenario)
2. Verify the PagerTask is still written to `billing.pagerTask`
3. Run the pager billing job; query the variable record

**Expected Result**:
- PagerTask exists with `shardSizeInBytes = 0`
- Variable record is written with `CreditQuantity = 0` and `Usage = 0`
- No error logged; zero-cost record is handled gracefully

```sql
-- Confirm PagerTask exists with zero shard size
SELECT p.fusionDetails.shardSizeInBytes,
       p.fusionDetails.downloadCompletedAt
FROM `capella`.`billing`.`pagerTask` p
WHERE p.fusionDetails.nodeID = '<nodeID>';

-- Confirm variable record was written with zero cost
SELECT v.creditQuantity, v.billingBreakdown.usage
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
-- Expected: creditQuantity = 0, usage = 0
```

---

#### 5.3 Accelerator Node Completes but DownloadCompletedAt Is Zero — Skipped
**Test ID**: `test_variable_billing_zero_download_completed_at`

**Context**: The pager billing job checks `fusionDetails.downloadCompletedAt` for each PagerTask
before including it in the aggregation. A zero value (Go zero `time.Time`) means the node never
actually completed its download — for example, if the node registered but then failed before calling
`/fusion/complete`, the PagerTask may still exist but with a zero `downloadCompletedAt`. Skipping
these tasks prevents billing for data that was never successfully transferred.

**Steps**:
1. Force a scenario where a PagerTask exists in `billing.pagerTask` with
   `fusionDetails.downloadCompletedAt` as zero time (simulates an incomplete/failed node)
2. Run the pager billing job
3. Verify no variable record is written for that task

**Expected Result**:
- PagerTask is skipped by the pager job (zero `downloadCompletedAt` is the skip condition)
- No variable record created for that `clusterID + planUUID` from the incomplete task

**CP Database Checks**:
- [ ] No variable record exists for that task
- [ ] Other tasks in the same job batch that have non-zero `downloadCompletedAt` are processed normally

```sql
-- Confirm the PagerTask exists but has zero/null downloadCompletedAt
SELECT p.fusionDetails.nodeID,
       p.fusionDetails.downloadCompletedAt,
       p.fusionDetails.shardSizeInBytes
FROM `capella`.`billing`.`pagerTask` p
WHERE p.fusionDetails.nodeID = '<nodeID>';

-- Confirm no variable record was written for this planUUID
SELECT COUNT(*) AS recordCount
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
-- Expected: recordCount = 0
```

---

#### 5.4 Large Shard Size — GiB Conversion Precision
**Test ID**: `test_variable_billing_large_shard_precision`

**Steps**:
1. Trigger a rebalance where the total shard size is large (e.g. hundreds of GiB)
2. Record exact `shardSizeInBytes` values from each PagerTask
3. Manually compute: `totalGiB = Σ(shardSizeInBytes) / (1024^3)`
4. Compare against `variableRecord.BillingBreakdown.Usage` after the pager job

**Expected Result**:
- No GiB truncation or rounding errors in the CP database record
- `Usage` matches the manual computation

```sql
-- Get exact shardSizeInBytes values and compute GiB in N1QL for cross-check
SELECT p.fusionDetails.nodeID,
       p.fusionDetails.shardSizeInBytes,
       p.fusionDetails.shardSizeInBytes / (1024 * 1024 * 1024) AS gibPerNode
FROM `capella`.`billing`.`pagerTask` p
WHERE p.clusterId = '<clusterID>'
AND p.fusionDetails.planUUID = '<planUUID>';

-- Compare sum against what the variable record stored
SELECT v.billingBreakdown.usage AS storedGiB
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
```

---

### 6. Fixed Billing — Feature Flag and Factor Changes

#### 6.1 Billing Reflects Updated Factor Values
**Test ID**: `test_fixed_billing_factor_update_reflected`

**Steps**:
1. Record current `SSD Uplift` factor value from `billing.credit_factors`
2. Create a fusion-enabled cluster; trigger a rebalance; verify HourlyBillingRecord reflects current uplift
3. Update the `SSD Uplift` factor to a new value
4. Wait for the next billing hour; query the new HourlyBillingRecord

**Expected Result**:
- New HourlyBillingRecord uses the updated `SSD Uplift` factor
- `FusionBreakdown.FusionSSDUplift` reflects the new value
- `FusionSSDCost` is recomputed with the new uplift

---

#### 6.2 Feature Flag Toggled Mid-Cluster-Lifecycle
**Test ID**: `test_fixed_billing_feature_flag_toggled`

**Steps**:
1. Enable `ff-billing-fusion-enabled`; create a fusion cluster; verify HourlyBillingRecord has `FusionSSDCost > 0`
2. Disable `ff-billing-fusion-enabled`
3. Wait for next billing hour; query the new HourlyBillingRecord

**Expected Result**:
- After flag is disabled: `FusionSSDCost = 0` in the new record
- Previous records with non-zero costs are unaffected

---

### 7. Billing Factor Configuration Verification

#### 7.1 All Required Fusion Factors Present and Valid
**Test ID**: `test_billing_factors_fusion_all_present`

**Steps**:
1. Query `billing.credit_factors` where `category = "Express Scaling (Fusion)"`
2. Verify each of the required factor names is present (see Prerequisites section)
3. For each factor, verify:
   - `multiplier > 0`
   - `effectiveDate` is in the past
   - `endDate` is either zero or in the future

**Expected Result**: All required factors exist and are currently effective.

**CP Database Checks**:
- [ ] `SSD Uplift` factor: exists, `multiplier > 0`, effective
- [ ] `Buckets - Free Tier` factor: exists, `multiplier >= 0`, effective
- [ ] `Buckets - Global Price` factor: exists, `multiplier > 0`, effective
- [ ] `Accelerator Global Rate - per GiB` factor: exists, `multiplier > 0`, effective
- [ ] CSP uplift factors exist for every billable AWS region

```sql
-- List all fusion factors with their effectiveness status
SELECT name,
       multiplier,
       effectiveDate,
       endDate,
       CASE
         WHEN multiplier <= 0 THEN 'INVALID: zero or negative multiplier'
         WHEN effectiveDate > NOW_STR() THEN 'INVALID: not yet effective'
         WHEN endDate != '' AND endDate < NOW_STR() THEN 'INVALID: expired'
         ELSE 'OK'
       END AS status
FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
ORDER BY name;
```

---

#### 7.2 AWS CSP Uplift Factor Naming Convention
**Test ID**: `test_billing_factors_aws_csp_uplift_naming`

**Context**: The pager billing job constructs the uplift factor name at runtime as
`"Accelerator CSP Uplift - AWS {region}"`. If the factor name in `billing.credit_factors` does not
match this exact format (including provider capitalisation and spacing), the lookup will fail and the
variable billing job will error for clusters in that region. This test verifies the exact naming
convention is in place for all AWS regions used in tests.

**Steps**:
1. For each AWS region under test, confirm the billing job constructs the lookup key as
   `"Accelerator CSP Uplift - AWS <region>"` (e.g. `"Accelerator CSP Uplift - AWS us-east-1"`)
2. Query `billing.credit_factors` for each region's factor by exact name

**Expected Result**:
- Factor found for each AWS region using the exact naming convention
- If any region's uplift factor is missing, the variable billing job will error for clusters in that region

```sql
-- Check uplift factors exist for all AWS regions under test
SELECT name, multiplier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name LIKE 'Accelerator CSP Uplift - AWS%'
ORDER BY name;

-- Spot-check exact name for a specific region (case-sensitive match matters)
SELECT name, multiplier FROM `capella`.`billing`.`credit_factors`
WHERE category = 'Express Scaling (Fusion)'
AND name = 'Accelerator CSP Uplift - AWS us-east-1';
```

---

### 8. End-to-End Scenarios

#### 8.1 Complete Fusion Billing Lifecycle on AWS
**Test ID**: `test_e2e_fusion_billing_aws`

**Steps**:
1. Create a fusion-enabled cluster on AWS (`us-east-1`) with 3 buckets
2. Load data into the cluster
3. Trigger a fusion rebalance; note all accelerator node IDs and planUUID
4. Wait for all accelerator nodes to complete (each calls `/fusion/complete`)
5. **Verify fixed billing**: query HourlyBillingRecord for each active hour
   - Confirm `FusionSSDCost > 0` and matches formula
   - Confirm `FusionBucketCost = 0` (if bucket count ≤ free tier)
6. **Verify PagerTasks**: query `billing.pagerTask` for all nodeIDs
   - Confirm one task per node with correct `shardSizeInBytes` and `downloadCompletedAt`
7. **Trigger pager billing job** (or wait for scheduled run)
8. **Verify variable record**: query variable record for `clusterID + planUUID`
   - Confirm `Usage` = sum of all shards in GiB
   - Confirm `CreditQuantity` = `totalGiB × fusionVariablePrice × awsUplift`
   - Confirm `ActivationID` is valid (not `"notFound"` for a paying tenant)

**CP Database Checks**:
- [ ] HourlyBillingRecord: `FusionEnabled = true`, `FusionSSDCost > 0`
- [ ] `billing.pagerTask`: one document per accelerator node
- [ ] Variable record: correct `Usage`, `CreditQuantity`, `ActivationID`, `Category = "Fusion 2"`

```sql
-- 1. Verify fixed billing record
SELECT b.fusionEnabled,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDCost,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketCost,
       b.debug.fusionCosts.fusionBreakdown.ebsListPrice,
       b.debug.fusionCosts.fusionBreakdown.fusionSSDUplift
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.clusterId = '<clusterID>'
ORDER BY b.billingHour DESC
LIMIT 1;

-- 2. Verify all PagerTasks were written (one per accelerator node)
SELECT p.fusionDetails.nodeID,
       p.fusionDetails.shardSizeInBytes,
       p.fusionDetails.downloadCompletedAt,
       p.fusionDetails.planUUID
FROM `capella`.`billing`.`pagerTask` p
WHERE p.clusterId = '<clusterID>'
AND p.fusionDetails.planUUID = '<planUUID>';

-- 3. Verify variable record fields after pager job runs
SELECT v.creditQuantity,
       v.activationId,
       v.plan,
       v.category,
       v.billingBreakdown.usage,
       v.billingBreakdown.usageUnit,
       v.billingBreakdown.uplift,
       v.billingBreakdown.basePrice
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
```

---

#### 8.2 Multiple Rebalances — Billing Isolation Per Rebalance
**Test ID**: `test_e2e_fusion_billing_multiple_rebalances`

**Steps**:
1. Create a fusion-enabled cluster; trigger Rebalance A; allow all nodes to complete
2. Run pager billing job; verify variable record for planUUID-A
3. Trigger Rebalance B on the same cluster; allow all nodes to complete
4. Run pager billing job; verify variable record for planUUID-B
5. Confirm both records coexist with correct, independent costs

**Expected Result**:
- Two separate variable records with distinct `planUUID` values
- Each record's `CreditQuantity` reflects only its own rebalance's data

```sql
-- List all variable records for the cluster ordered by rebalance
SELECT v.billingBreakdown.planUUID,
       v.billingBreakdown.usage AS giB,
       v.creditQuantity AS cost,
       v.usageDate
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
ORDER BY v.usageDate ASC;
-- Expected: 2 rows with distinct planUUIDs and independent GiB/cost values
```

---

#### 8.3 Fusion Billing With Bucket Overages
**Test ID**: `test_e2e_fusion_billing_with_bucket_overages`

**Steps**:
1. Retrieve `Buckets - Free Tier` (N) from CP database
2. Create a fusion-enabled cluster with N + 3 buckets
3. Trigger a rebalance; allow node(s) to complete
4. Verify HourlyBillingRecord:
   - `FusionSSDCost > 0`
   - `FusionBucketCost = 3 × bucketGlobalPrice`
5. Verify variable record is also written correctly (independent of bucket cost)

**Expected Result**:
- Fixed billing includes both SSD and bucket overage costs
- Variable billing reflects only GiB processed (no bucket cost component)

```sql
-- Confirm both SSD and bucket costs are non-zero in the HourlyBillingRecord
SELECT b.debug.fusionCosts.fusionBreakdown.fusionSSDCost,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketCost,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketFreeAlloc,
       b.debug.fusionCosts.fusionBreakdown.fusionBucketGlobalPrice
FROM `capella`.`billing`.`hourlyBillingRecord` b
WHERE b.clusterId = '<clusterID>'
ORDER BY b.billingHour DESC
LIMIT 1;

-- Confirm variable record has no bucket cost component (variable billing is GiB only)
SELECT v.creditQuantity, v.billingBreakdown.usage, v.billingBreakdown.usageUnit
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
```

---

### 9. Negative / Error Scenarios

#### 9.1 Missing CSP Uplift Factor — Variable Billing Fails for That Cluster
**Test ID**: `test_variable_billing_missing_csp_uplift`

**Context**: The CSP uplift factor is fetched per aggregation group (per `clusterID + planUUID`)
after the global variable price is fetched. If the uplift factor is missing for a specific region,
the pager billing job uses Go's `MultiError` — it records the error for that group, continues
processing other groups, and returns a combined error at the end. This means a missing uplift for
one region does not prevent billing for clusters in other regions. However, the affected cluster's
variable record is never written, so the charge is permanently lost unless the job is re-run after
the factor is added.

**Steps**:
1. Temporarily rename or disable the CSP uplift factor for a specific region in `billing.credit_factors`
2. Create a fusion-enabled cluster in that region; trigger a rebalance; allow node to complete
3. Run the pager billing job
4. Query CP database for the variable record

**Expected Result**:
- No variable record created for the affected cluster (error for that cluster is logged)
- Other clusters in the same job batch still have their records written
- Error is visible in Datadog logs; billing job does not crash

```sql
-- Confirm no variable record was created for the affected cluster
SELECT COUNT(*) AS recordCount
FROM `capella`.`billing`.`variableRecord` v
WHERE v.databaseId = '<clusterID>'
AND v.billingBreakdown.planUUID = '<planUUID>';
-- Expected: 0

-- Confirm PagerTask still exists (the task itself was not removed)
SELECT p.fusionDetails.nodeID, p.fusionDetails.planUUID
FROM `capella`.`billing`.`pagerTask` p
WHERE p.clusterId = '<clusterID>'
AND p.fusionDetails.planUUID = '<planUUID>';
```

---

#### 9.2 Missing Fusion Variable Price Factor — Billing Job Aborts
**Test ID**: `test_variable_billing_missing_variable_price`

**Context**: Unlike the CSP uplift (which is fetched per cluster group), the `Accelerator Global Rate
- per GiB` factor is fetched **once at the start** of the pager billing job before any PagerTasks
are processed. If this fetch fails, the job returns an error immediately and processes nothing.
This is an intentional fail-fast design — without the global price, no cost can be computed for
any cluster, so there is no point continuing.

**Steps**:
1. Temporarily remove the `Accelerator Global Rate - per GiB` factor from `billing.credit_factors`
2. Trigger a rebalance and node completion
3. Run the pager billing job; observe its behaviour

**Expected Result**:
- Pager billing job returns an error immediately (this factor is fetched first; if missing, no tasks processed)
- No variable records written for any cluster in that job run
- Error surfaced in Datadog; no silent data loss

---

#### 9.3 PagerTask TTL — Record Expires After One Year
**Test ID**: `test_variable_billing_pager_task_ttl`

**Context**: PagerTask documents in the `billing.pagerTask` collection are written with a Couchbase
TTL of 1 year (8760 hours). This means Couchbase automatically deletes them after a year with no
application-level cleanup required. The TTL is set at write time and is visible as the `_expiry`
metadata field on the document. This is important to verify because if the TTL is accidentally not
set, the collection would grow unboundedly over time.

**Steps**:
1. Create a fusion-enabled cluster; trigger a rebalance; allow an accelerator node to complete
2. Query the `billing.pagerTask` collection in the CP Couchbase database for the new PagerTask document
3. Inspect the document's `_expiry` metadata field (available via the Couchbase SDK's `GetMeta` or
   `LookupIn` operation)

**Expected Result**:
- `_expiry` is approximately `createdAt + 365 days` (8760 hours)
- Documents auto-expire and do not grow indefinitely

```sql
-- Retrieve the document metadata to inspect the TTL expiry timestamp
-- META().expiration returns the Unix epoch seconds at which the document will be deleted
SELECT META(p).id AS docKey,
       META(p).expiration AS expiryEpochSecs,
       p.fusionDetails.nodeID
FROM `capella`.`billing`.`pagerTask` p
WHERE p.fusionDetails.nodeID = '<nodeID>';
-- Expected: expiryEpochSecs ≈ (creation time epoch) + (365 * 24 * 3600)
```

---

## Validation Checklists

### After Rebalance Completion

- [ ] PagerTask exists in `billing.pagerTask` for each accelerator `nodeID`
- [ ] `fusionDetails.planUUID` matches the rebalance plan ID
- [ ] `fusionDetails.shardSizeInBytes > 0`
- [ ] `fusionDetails.downloadCompletedAt` is non-zero and after `registeredAt`
- [ ] `tenantID`, `clusterID`, `projectID`, `region`, `provider` are correct
- [ ] `usageCategory = "Fusion 2"`
- [ ] Only one document per `nodeID` (idempotency check)

### After Fixed Billing Job

- [ ] HourlyBillingRecord exists for each active accelerator node hour
- [ ] `FusionEnabled = true`
- [ ] `FusionBreakdown.FusionSSDCost > 0` (when flag is enabled)
- [ ] `FusionBreakdown.EBSListPrice` matches provider disk rate for region
- [ ] `FusionBreakdown.FusionSSDUplift` matches `SSD Uplift` factor
- [ ] `FusionBreakdown.FusionBucketCost` = `max(0, buckets − freeTier) × bucketPrice`
- [ ] `FusionBreakdown.FusionBucketFreeAlloc` matches `Buckets - Free Tier` factor
- [ ] SSD cost formula: `(diskSizeGb × EBSListPrice / 730) × SSDUplift`

### After Pager Billing Job

- [ ] Variable record exists in CP database for each `clusterID + planUUID`
- [ ] `BillingBreakdown.Usage` = sum of GiB across all PagerTasks for that `planUUID`
- [ ] `BillingBreakdown.UsageUnit = "GiB"`
- [ ] `CreditQuantity` = `totalGiB × fusionVariablePrice × cspRegionUplift`
- [ ] `BillingBreakdown.BasePrice` = `Accelerator Global Rate - per GiB` factor value
- [ ] `BillingBreakdown.Uplift` = region-specific CSP uplift factor value
- [ ] `Category = "Fusion 2"`
- [ ] `ActivationID` is set (not empty; `"notFound"` only for tenants without an activation)
- [ ] `Manual` flag set correctly based on whether job was manual or scheduled

---

## Test Configuration Parameters

```ini
# Cluster configuration
fusion_enabled=true
cloud_provider=aws
region=us-east-1    # primary region; set to eu-west-1 or ap-southeast-1 for regional uplift tests

# Data configuration
num_items=100000
doc_size=1024
num_buckets=3       # vary relative to Buckets - Free Tier for bucket cost tests

# Billing job
billing_job_type=fixed       # for fixed billing tests
billing_job_type=variable    # for pager/variable billing tests
manual_billing_trigger=true  # trigger billing jobs manually in tests rather than waiting for schedule

# Control plane Couchbase database
# This is the CP's own internal Couchbase cluster — not the customer's cluster.
# Used to verify all billing phone home records directly.
cp_db_host=<control-plane-couchbase-host>
cp_db_bucket=capella

# Collections within the billing scope of the CP Couchbase cluster:
#   billing.pagerTask        — one document per accelerator node completion event
#                              dedup key: fusionDetails.nodeID
#                              TTL: 1 year (8760 hours)
#   billing.credit_factors   — billing rate factors (SSD uplift, bucket price, variable rate, CSP uplifts)
#                              query by: category = "Express Scaling (Fusion)"
billing_pager_task_collection=billing.pagerTask
billing_credit_factors_collection=billing.credit_factors
```

---

## Test File Location

```
pytests/billing/fusion/
  fusion_fixed_billing.py       # Tests 1.x, 2.x, 6.x
  fusion_variable_billing.py    # Tests 3.x, 4.x, 5.x
  fusion_billing_factors.py     # Tests 7.x
  fusion_billing_e2e.py         # Tests 8.x, 9.x
```

---

## Dependencies

- Capella API for cluster creation and rebalance triggering
- Control plane Couchbase database (direct query access) for verifying:
  - `billing.pagerTask` documents
  - `billing.credit_factors` documents
  - HourlyBillingRecord documents
  - Variable billing records
- Ability to trigger fixed and variable billing jobs manually (or wait for their scheduled run)
- Datadog API or log access for validating billing error events and metrics
- Feature flag management API for toggling `ff-billing-fusion-enabled`
- Jira reference: [AV-94188](https://jira.issues.couchbase.com/browse/AV-94188)
