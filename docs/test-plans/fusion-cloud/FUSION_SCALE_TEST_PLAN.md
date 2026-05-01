# Fusion Multi-Cluster Scale Test Plan

## Document Information

| Field | Value |
|-------|-------|
| Created | 2026-04-10 |
| Status | Draft |
| Scale Target | 10 clusters × 27 nodes × ~22 accelerators/node = ~6000 accelerators |
| Related Files | couchbase-cloud: `internal/clusters/fusion/`, TAF: `pytests/aGoodDoctor/fusion/` |

---

## Executive Summary

This document provides a comprehensive scale test plan for validating multi-tenant/multi-cluster fusion scaling operations. The plan addresses AWS account-level resource limits, API throttling behavior, control plane scalability, and establishes safe operational limits for production deployment.

### Scale Parameters

| Parameter | Value | Calculation |
|-----------|-------|-------------|
| Clusters | 10 | - |
| Nodes per cluster | 27 | - |
| Accelerator slots per node | 22 | `maxSlots` constant |
| Accelerators per cluster | ~594 | 27 nodes × 22 slots |
| **Total accelerators** | **~6000** | 10 clusters × 594 accelerators |
| EBS volumes per cluster | ~594 | One per accelerator |
| Auto Scaling Groups per cluster | ~594 | One per accelerator |

---

## A. Test Architecture

### A.1 Multi-Cluster Test Orchestration

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SCALE TEST ORCHESTRATOR                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                      Test Coordinator (Python)                           ││
│  │  - Cluster provisioning                                                  ││
│  │  - Parallel operation dispatch                                           ││
│  │  - Resource monitoring aggregation                                       ││
│  │  - Failure detection and recovery                                        ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                    │                                         │
│         ┌──────────────────────────┼──────────────────────────┐             │
│         ▼                          ▼                          ▼             │
│  ┌─────────────┐            ┌─────────────┐            ┌─────────────┐      │
│  │  Cluster 1  │            │  Cluster 2  │   ...      │ Cluster 10  │      │
│  │  27 nodes   │            │  27 nodes   │            │  27 nodes   │      │
│  │  ~594 acc   │            │  ~594 acc   │            │  ~594 acc   │      │
│  └─────────────┘            └─────────────┘            └─────────────┘      │
│         │                          │                          │             │
│         ▼                          ▼                          ▼             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                         AWS Account Resources                            ││
│  │  - EC2 instances (~6000+ accelerators + cluster nodes)                   ││
│  │  - EBS volumes (~6000+ guest volumes)                                    ││
│  │  - Auto Scaling Groups (~6000+)                                          ││
│  │  - S3 API calls (log store operations)                                   ││
│  │  - VPC endpoints (S3 access)                                             ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

### A.2 Parallel vs Sequential Operation Strategies

#### Strategy Comparison

| Strategy | Advantages | Disadvantages | Use Case |
|----------|------------|---------------|----------|
| **Fully Parallel** | Maximum throughput, shortest test duration | High throttling risk, resource contention | Stress testing |
| **Staggered (30s offset)** | Reduced API burst, gradual resource allocation | Longer test duration | Production-like testing |
| **Batch Sequential** | Minimal throttling risk, controlled resource usage | Longest test duration | Safe baseline testing |
| **Mixed Strategy** | Balanced approach, configurable aggressiveness | Complex coordination | Recommended approach |

#### Recommended Mixed Strategy

```python
# Phase-based stagger with configurable batch sizes
SCALE_TEST_CONFIG = {
    "phase1_single_cluster": {
        "clusters": 1,
        "operation_mode": "sequential",
        "batch_size": 27,  # All nodes at once
        "stagger_offset_ms": 0
    },
    "phase2_five_clusters": {
        "clusters": 5,
        "operation_mode": "staggered",
        "batch_size": 9,  # 3 batches per cluster
        "stagger_offset_ms": 30000,  # 30 second offset between clusters
        "cluster_start_delay_ms": 60000  # 1 minute between cluster starts
    },
    "phase3_ten_clusters": {
        "clusters": 10,
        "operation_mode": "staggered",
        "batch_size": 9,  # 3 batches per cluster
        "stagger_offset_ms": 30000,
        "cluster_start_delay_ms": 120000  # 2 minutes between cluster starts
    }
}
```

### A.3 Test Coordination Mechanisms

#### Resource Locking

```python
class ScaleTestCoordinator:
    """
    Coordinates multi-cluster scale testing with resource management.
    """
    
    def __init__(self, max_concurrent_api_calls=50, max_concurrent_jobs=20):
        self.api_rate_limiter = TokenBucketLimiter(
            refill_rate=5,  # AWS EC2 modify API rate
            burst_capacity=50,  # AWS burst tokens
            max_concurrent=max_concurrent_api_calls
        )
        self.job_semaphore = threading.Semaphore(max_concurrent_jobs)
        self.cluster_locks = {i: threading.Lock() for i in range(10)}
        self.resource_monitor = AWSResourceMonitor()
        self.stop_event = threading.Event()
```

#### Health Check Integration

```python
# Per-cluster health monitoring threads
def monitor_cluster_health(coordinator, cluster_id, stop_event):
    while not stop_event.is_set():
        health = check_cluster_health(cluster_id)
        if health.status in ["deployment_failed", "rebalance_failed", "scaleFailed"]:
            coordinator.trigger_recovery(cluster_id, health)
        time.sleep(10)
```

### A.4 Resource Allocation Per Cluster

| Resource | Per Cluster | Per 10 Clusters | AWS Default Limit | Headroom |
|----------|-------------|-----------------|-------------------|----------|
| EC2 Instances (accelerators) | ~594 | ~6000 | 5000 (default) | **EXCEEDED** |
| EBS Volumes | ~594 | ~6000 | 5000 (default) | **EXCEEDED** |
| Auto Scaling Groups | ~594 | ~6000 | 500 (default) | **EXCEEDED** |
| Launch Templates | ~1188 (x86+ARM) | ~12000 | 2000 (default) | **EXCEEDED** |
| vCPUs (t3.medium equivalent) | ~594 | ~6000 | Varies by type | Varies |

**Critical Finding:** Default AWS account limits will be exceeded. Pre-test limit increase requests are mandatory.

---

## B. AWS Resource Planning

### B.1 EC2 Instance Limits and vCPU Considerations

#### EC2 Instance Types for Fusion Accelerators

Based on `infra/aws/deployer.go` analysis:

```go
// Accelerator instances use configurable compute types
// Both x86_64 and ARM architectures supported
// Instance type determined by accelerator release configuration
```

#### Default AWS EC2 Limits

| Instance Category | Default Limit | Per-Region | Notes |
|-------------------|---------------|------------|-------|
| Standard On-Demand | 5-20 vCPUs | Varies | t3, m5, c5 families |
| Accelerated Computing | 0-1 instance | Per type | p3, p4 families |
| All F+ instances | 8 vCPUs | Per region | f1 family |
| Total On-Demand | ~1000 instances | Account-wide | Combined limit |

#### Required Limit Increases

For 10 clusters × 27 nodes × 22 accelerators:

| Metric | Required | Default | Increase Needed | Request Type |
|--------|----------|---------|-----------------|--------------|
| On-Demand t3.medium | 6000 vCPUs | 5-20 vCPUs | **~300x increase** | Service Quota |
| Total instances | 6000+ | ~1000 | **6x increase** | Service Quota |
| Running instances | 6000+ | ~500 | **12x increase** | Service Quota |

#### vCPU Calculation for Accelerators

```
Per accelerator: ~1 vCPU (t3.medium equivalent)
Per cluster: 594 vCPUs
Per 10 clusters: 5940 vCPUs

Plus cluster nodes: 10 × 27 × (node vCPUs)
```

### B.2 EBS Volume Limits

#### EBS Volume Types and Limits

Based on `infra/aws/volumes.go`:

```go
// Volume configuration from deployer
minVolumeSize = 50 * 1024 * 1024 * 1024  // 50 GiB minimum
minVolumeIOPS = 3000
minVolumeThroughput = 125  // MiB/s

// Fusion accelerator characteristic
FUSION_ACCELERATOR_IOPS = 16000  // From TAF fusion_cp_resource_monitor.py
```

#### Default AWS EBS Limits

| Metric | Default Limit | Region Scope | Notes |
|--------|---------------|--------------|-------|
| Total volumes | 5000 | Per region | All types combined |
| gp3 volumes | 5000 | Per region | Included in total |
| IOPS (gp3) | 50,000 per volume | Per volume | 16,000 IOPS used |
| Throughput (gp3) | 1,000 MiB/s per volume | Per volume | 125-2000 used |
| Total IOPS | 300,000 | Per region | Combined IOPS limit |
| Total throughput | 5,000 MiB/s | Per region | Combined throughput |

#### EBS Limit Requirements

| Metric | Required | Default | Increase Factor |
|--------|----------|---------|-----------------|
| Volume count | 6000 | 5000 | **1.2x** |
| Total IOPS | 96M (6000×16000) | 300K | **320x** |
| Total throughput | 6M MiB/s | 5,000 | **1,200x** |

**Note:** The IOPS and throughput limits are per-region and may be shared with other workloads. Coordinate with AWS for significant increases.

### B.3 Auto Scaling Group Limits

#### ASG Configuration from Source

Based on `infra/aws/autoscalinggroups.go`:

```go
// ASG created per accelerator node
// Tags include:
// - couchbase-cloud-cluster-id
// - couchbase-cloud-function: fusion-accelerator
// - couchbase-cloud-fusion-rebalance
```

#### Default AWS ASG Limits

| Metric | Default Limit | Per-Region | Notes |
|--------|---------------|------------|-------|
| ASGs per region | 500 | Per region | Hard limit |
| Launch configurations | 500 | Per region | Per account |
| Launch templates | 2000 | Per region | Per account |
| Instances per ASG | 1000 | Per ASG | Soft limit |
| Target groups per ASG | 10 | Per ASG | Hard limit |

#### ASG Limit Requirements

| Metric | Required | Default | Increase Factor |
|--------|----------|---------|-----------------|
| ASGs per region | 6000 | 500 | **12x** |
| Launch templates | 12000 (2 per ASG) | 2000 | **6x** |

### B.4 S3 API Rate Limits

#### S3 Configuration for Fusion

Based on `fusion_monitor_util.py`:

```python
# Fusion log store S3 operations
# - ep_magma_fusion_logstore_uri points to S3 bucket
# - KV/{bucket_uuid}/ prefix for data storage
# - Continuous upload during data operations
# - Deletion during checkpoint advancement
```

#### S3 Rate Limits

| Operation | Default Rate | Burst | Notes |
|-----------|--------------|-------|-------|
| PUT/COPY/POST/DELETE | 3500 req/s | 5500 | Per prefix |
| GET/HEAD | 5500 req/s | 8500 | Per prefix |
| LIST | 800 req/s | 1000 | Per prefix |

#### S3 Scaling Considerations

For 10 clusters with active fusion:
- Each accelerator uploads logs to S3 during download
- Each cluster node uploads logs continuously
- Checkpoint advancement triggers deletions

Estimated S3 operation rate:
```
Upload operations: ~6000 accelerators × ~100 ops/s = 600K ops/s (per cluster batch)
This exceeds per-prefix limits; must use multiple prefixes/buckets
```

**Recommendation:** Use per-cluster S3 buckets to distribute load.

### B.5 VPC Endpoint Limits

#### VPC Endpoint for S3 Access

Based on `infra/aws/networking_vpc_endpoints.go`:

```go
// S3 VPC endpoint created for accelerator S3 access
// Gateway endpoint type for S3
```

#### VPC Endpoint Limits

| Metric | Default Limit | Per-Region |
|--------|---------------|------------|
| Gateway VPC endpoints | 20 | Per VPC |
| Interface VPC endpoints | 50 | Per VPC |
| Endpoint policies | 10 | Per endpoint |

#### Requirement Analysis

For 10 clusters (assuming separate VPCs):
- 10 S3 gateway endpoints needed
- Within default limits (20 per VPC)

### B.6 Network Interface Limits

#### ENI Limits per Instance Type

| Instance Type | Default ENIs | With Accelerators |
|---------------|--------------|-------------------|
| t3.medium | 3 | +1 for guest volume |
| t3.large | 5 | +1 for guest volume |
| m5.xlarge | 4 | +1 for guest volume |

#### ENI Limits per Region

| Metric | Default Limit |
|--------|---------------|
| Total ENIs | 5000 |

For 6000 accelerators, ENI limit may be approached.

---

## C. API Throttling Analysis

### C.1 AWS EC2 API Rate Limits

#### Rate Limiting Constants from Source

Based on `accelerator/accelerator.go`:

```go
const (
    // limiterRefillRatePerSecond mirrors the AWS EC2 modify API refill rate (5 req/s)
    limiterRefillRatePerSecond = rate.Limit(5)
    
    // limiterBurstTokens mirrors the AWS EC2 modify API burst capacity (50 tokens)
    limiterBurstTokens = 50
)
```

#### AWS EC2 API Rate Limits

| API Category | Rate Limit | Burst | Notes |
|--------------|------------|-------|-------|
| Describe* | 100 req/s | 200 | Read operations |
| RunInstances | 100 req/s | 200 | Instance launch |
| ModifyInstanceAttribute | 5 req/s | 50 | **Critical for fusion** |
| CreateVolume | 100 req/s | 200 | Volume creation |
| AttachVolume | 100 req/s | 200 | Volume attachment |
| DetachVolume | 100 req/s | 200 | Volume detachment |
| DeleteVolume | 100 req/s | 200 | Volume deletion |
| CreateLaunchTemplate | 5 req/s | 50 | Template creation |
| CreateAutoScalingGroup | 50 req/s | 100 | ASG creation |

### C.2 Burst Capacity Calculations

#### Token Bucket Model

```
Rate: 5 tokens/second (refill)
Burst: 50 tokens (initial capacity)

Time to replenish from 0 to 50: 50 / 5 = 10 seconds
Maximum sustained throughput: 5 operations/second
Maximum burst throughput: 50 operations (instant)
```

#### Operation Timing Analysis

For 594 accelerators per cluster:

| Operation Type | Operations | Time (No Burst) | Time (With Burst) |
|----------------|------------|-----------------|-------------------|
| CreateVolume | 594 | ~2 min (parallel) | ~10s (burst) |
| CreateLaunchTemplate | 1188 | ~4 min | ~20s (burst) |
| CreateAutoScalingGroup | 594 | ~12 min | ~12s (burst) |
| ModifyVolume (attach) | 594 | ~2 min | ~10s (burst) |

**Critical Finding:** With rate limiting at 5 req/s, a single cluster's deployment takes approximately 20+ minutes without optimization.

### C.3 Backoff and Retry Strategies

#### Retry Configuration from Source

Based on `infra/aws/deployer.go`:

```go
func defaultRetryOptions[T any](shouldRetry retry.ShouldRetryFunc[T]) retry.RetryerOptions[T] {
    return retry.RetryerOptions[T]{
        MinDelay:     time.Second * 1,      // AWS refills buckets every second
        MaxDelay:     time.Second * 30,
        MinJitter:    time.Second,
        MaxJitter:    time.Second * 5,
        MaxRetries:   100,                  // High for long operations
        ShouldRetry: shouldRetry,
    }
}
```

#### Retry Strategy Details

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| MinDelay | 1 second | Matches AWS bucket refill rate |
| MaxDelay | 30 seconds | Prevents excessive wait |
| MinJitter | 1 second | Avoids retry storms |
| MaxJitter | 5 seconds | Flattens retry waves |
| MaxRetries | 100 | Allows extended operation time |

#### Expected Retry Scenarios

1. **Throttling Exception**: Retry with exponential backoff
2. **Resource Conflict**: Retry after resource freed
3. **Transient Failure**: Retry with jitter
4. **Fatal Error**: No retry (marked fatal)

### C.4 Account-Level vs Region-Level Limits

#### Limit Scope Matrix

| Resource Type | Scope | Sharing Behavior |
|---------------|-------|------------------|
| EC2 instances | Region | Shared across all clusters in region |
| EBS volumes | Region | Shared across all clusters in region |
| ASGs | Region | Shared across all clusters in region |
| S3 operations | Bucket/Prefix | Isolated per bucket/prefix |
| API rate limits | Account | **Shared across all regions** |

#### Critical Finding

**AWS EC2 Modify API rate limits (5 req/s) are account-level, not region-level.**

For 10 clusters operating simultaneously:
- All 10 clusters share the same 5 req/s rate limit
- Effective rate per cluster: 0.5 req/s
- Expected throttling: Significant without coordination

---

## D. Control Plane Scalability

### D.1 Concurrent Job Limits

#### Job Limits from Source

Based on `queuer/queuer.go`:

```go
const maxJobs = 2  // Maximum pending/processing jobs per type

func (q *Queuer) assertJobDoesNotExist(...) error {
    // ... 
    if len(fusionJobs) == maxJobs {
        return ErrJobAlreadyExists
    }
}
```

#### Job Type Limits

| Job Type | Max Concurrent | Notes |
|----------|----------------|-------|
| Deploy | 2 per cluster | Pending + Processing |
| TearDown | 2 per cluster | Pending + Processing |
| Purge | 2 per cluster | Pending + Processing |
| EnableFusion | Unlimited | Idempotent, non-blocking |
| DisableFusion | Unlimited | Idempotent, non-blocking |
| ScaleDownVolume | 2 per volume | Per node basis |
| DestroyCompute | 2 per node | After download completion |
| TrackRebalance | 2 per rebalance | Per plan UUID |

#### Implications for Scale Testing

For 10 clusters × 594 accelerators:
- Deploy jobs: 10 clusters × 2 = 20 concurrent maximum
- ScaleDownVolume jobs: 6000 accelerators × 2 = Limited by queue processing
- TearDown jobs: 10 clusters × 2 = 20 concurrent maximum

**Bottleneck Analysis:**
- Job queue processing rate becomes limiting factor
- Each job takes ~5 minutes for timeout
- Effective throughput: ~4 jobs/minute per job type

### D.2 Manifest Management at Scale

#### Manifest Structure

Based on `accelerator/accelerator.go`:

```go
type Manifest struct {
    ID                               DocumentID
    ClusterID                        string
    TenantID                         string
    CorrelationKey                   string
    RebalanceOp                      rebalance.Op
    EstimatedNormalRebalanceDuration time.Duration
    OriginalNodeCount                int
    NewNodeCount                     int
    PrepareDurationMs                int64
    SplitDurationMs                  int64
    AcceleratorNodeCount             int
    Plan                             *Plan
    Status                           Status
    // ... timestamps
}

type Plan struct {
    ID        string
    TotalSize int64
    Parts     map[string]int  // hostname -> shard count
}
```

#### Manifest Storage Considerations

For 10 concurrent clusters:
- 10 manifest documents
- 594 manifest parts per cluster (27 hosts × ~22 shards)
- Total: 5940 manifest part documents

Database operation load:
- Create operations: 5940 (one-time per phase)
- Update operations: Continuous status tracking
- Query operations: Per-accelerator lookup

### D.3 Checkpoint Management

#### Checkpoint Types

Based on `nodes/checkpointer.go`:

| Checkpoint Type | Purpose | Storage |
|-----------------|---------|---------|
| Deploy | Track deployment progress | DB |
| DeployVolume | Track volume creation | DB |
| DeployComputeTemplate | Track launch template | DB |
| DeployScalingGroup | Track ASG creation | DB |

#### Checkpoint Scaling

For 6000 accelerators:
- 4 checkpoint types × 6000 = 24000 checkpoint documents
- Continuous update during operations
- Must support recovery from any point

### D.4 Database Operation Limits

#### Couchbase Bucket Limits

| Metric | Default Limit | Impact |
|--------|---------------|--------|
| Operations/sec | 100K+ | Sufficient for 6000 accelerators |
| Documents | Unlimited | Storage-based |
| XDCR | 32 per bucket | Not applicable |
| Indexes | 20+ per bucket | Sufficient |

#### Document Operation Patterns

```
Per accelerator lifecycle:
1. Create node document (1 op)
2. Create stats document (1 op)
3. Update status (N ops during lifecycle)
4. Delete documents (2 ops at cleanup)

Total ops per accelerator: ~10
Total ops for 6000 accelerators: ~60,000 (well within limits)
```

---

## E. Test Phases

### E.1 Phase 1: Single Cluster Baseline

#### Configuration

| Parameter | Value |
|-----------|-------|
| Clusters | 1 |
| Nodes per cluster | 27 |
| Expected accelerators | ~594 |
| Data size | 1-10 TB |
| Duration target | 30-60 minutes |

#### Test Steps

1. **Setup**
   - Provision single 27-node cluster
   - Enable fusion on cluster
   - Load baseline data (1-10 TB)
   - Verify fusion status = "enabled"

2. **Scale Operation**
   - Trigger horizontal scale (add/remove nodes)
   - Monitor accelerator creation (0 → 594)
   - Monitor guest volume lifecycle
   - Monitor rebalance completion

3. **Validation**
   - Verify accelerator count returns to 0
   - Verify EBS guest volume cleanup (594 → 0)
   - Verify ASG cleanup
   - Verify no errors in memcached logs
   - Verify item count consistency

4. **Metrics Collection**
   - AWS API call count
   - Rate limit wait duration
   - Total operation duration
   - Resource utilization

#### Success Criteria

| Criterion | Threshold |
|-----------|-----------|
| Accelerator creation | All 594 created within 30 min |
| Guest volume cleanup | All cleaned within 60 min |
| AWS API errors | < 5 throttling events |
| Cluster health | Returns to "healthy" |
| Data consistency | Item count matches |

### E.2 Phase 2: Five Clusters Parallel

#### Configuration

| Parameter | Value |
|-----------|-------|
| Clusters | 5 |
| Nodes per cluster | 27 |
| Expected accelerators | ~3000 (5 × 594) |
| Data size per cluster | 1-10 TB |
| Duration target | 60-90 minutes |

#### Test Steps

1. **Setup**
   - Provision 5 × 27-node clusters
   - Enable fusion on all clusters
   - Load data on all clusters
   - Stagger cluster ready state by 60 seconds

2. **Scale Operation**
   - Trigger scale on all 5 clusters simultaneously
   - Apply stagger offset: 60 seconds between cluster operations
   - Monitor per-cluster accelerator creation
   - Track cross-cluster AWS resource contention

3. **Validation**
   - Per-cluster validation (same as Phase 1)
   - Cross-cluster resource isolation verification
   - AWS account-level limit verification

4. **Metrics Collection**
   - Per-cluster metrics (same as Phase 1)
   - Aggregate AWS API call rate
   - Throttling event correlation
   - Resource contention analysis

#### Success Criteria

| Criterion | Threshold |
|-----------|-----------|
| All clusters reach "healthy" | Within 90 min |
| Total accelerator creation | All ~3000 created |
| Throttling events | < 20 total |
| Cross-cluster interference | None observed |
| AWS limit violations | None |

### E.3 Phase 3: Ten Clusters Parallel

#### Configuration

| Parameter | Value |
|-----------|-------|
| Clusters | 10 |
| Nodes per cluster | 27 |
| Expected accelerators | ~6000 (10 × 594) |
| Data size per cluster | 1-10 TB |
| Duration target | 90-120 minutes |

#### Test Steps

1. **Setup**
   - Provision 10 × 27-node clusters
   - Enable fusion on all clusters
   - Load data on all clusters
   - Stagger cluster ready state by 120 seconds

2. **Scale Operation**
   - Trigger scale on all 10 clusters
   - Apply stagger offset: 120 seconds between cluster operations
   - Batch node operations within each cluster (9 nodes per batch)
   - Monitor per-cluster and aggregate progress

3. **Validation**
   - Per-cluster validation (same as Phase 1)
   - AWS resource limit validation
   - Control plane health validation
   - Job queue depth validation

4. **Metrics Collection**
   - All metrics from Phase 2
   - Control plane job processing rate
   - Database operation latency
   - AWS API throttling event count and duration

#### Success Criteria

| Criterion | Threshold |
|-----------|-----------|
| All clusters reach "healthy" | Within 120 min |
| Total accelerator creation | All ~6000 created |
| Throttling events | < 50 total |
| Control plane health | All jobs completed |
| AWS limit violations | None |

### E.4 Phase 4: Stress Testing Beyond Limits

#### Configuration

| Parameter | Value |
|-----------|-------|
| Clusters | 10+ |
| Nodes per cluster | 27+ |
| Expected accelerators | 6000+ |
| Stress mode | Maximum API rate |

#### Test Objectives

1. Identify breaking points
2. Measure recovery capabilities
3. Document failure modes
4. Establish safety margins

#### Stress Test Types

| Test Type | Description | Expected Outcome |
|-----------|-------------|------------------|
| **API Burst** | Remove rate limiting | Throttling errors |
| **Resource Exhaustion** | Exceed AWS limits | Resource creation failures |
| **Job Queue Overflow** | Queue > processing rate | Job backlogs |
| **Failure Cascade** | Induce failures during peak | Recovery or cascade |

---

## F. Monitoring and Validation

### F.1 Resource Utilization Tracking

#### AWS Resource Metrics

```python
class ScaleTestResourceMonitor:
    """
    Monitors AWS resource utilization during scale testing.
    """
    
    def __init__(self, fusion_aws_util, interval_seconds=60):
        self.fusion_aws_util = fusion_aws_util
        self.interval = interval_seconds
        self.metrics = {
            "ec2_instances": [],
            "ebs_volumes": [],
            "asgs": [],
            "api_calls": [],
            "throttling_events": []
        }
    
    def monitor_loop(self, stop_event):
        while not stop_event.is_set():
            for cluster_id in self.cluster_ids:
                # EC2 instances
                instances = self.fusion_aws_util.list_accelerator_instances(
                    filters=[{
                        'Name': 'tag:couchbase-cloud-cluster-id',
                        'Values': [str(cluster_id)]
                    }]
                )
                self.metrics["ec2_instances"].append({
                    "timestamp": time.time(),
                    "cluster_id": cluster_id,
                    "count": len(instances)
                })
                
                # EBS volumes
                volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                    filters={
                        'couchbase-cloud-cluster-id': cluster_id,
                        'couchbase-cloud-function': 'fusion-accelerator'
                    }
                )
                self.metrics["ebs_volumes"].append({
                    "timestamp": time.time(),
                    "cluster_id": cluster_id,
                    "count": len(volumes)
                })
                
                # ASGs
                asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster_id)
                self.metrics["asgs"].append({
                    "timestamp": time.time(),
                    "cluster_id": cluster_id,
                    "count": len(asgs)
                })
            
            time.sleep(self.interval)
```

#### Resource Tracking Table

| Metric | Collection Method | Interval | Storage |
|--------|-------------------|----------|---------|
| EC2 instance count | EC2 DescribeInstances | 60s | In-memory + file |
| EBS volume count | EC2 DescribeVolumes | 60s | In-memory + file |
| ASG count | ASG DescribeAutoScalingGroups | 60s | In-memory + file |
| API call count | CloudTrail | 5 min | CloudTrail |
| Throttling events | CloudTrail + application logs | Real-time | In-memory + file |

### F.2 Throttling Event Detection

#### Throttling Detection Implementation

```python
class ThrottlingEventDetector:
    """
    Detects and records AWS API throttling events.
    """
    
    THROTTLING_CODES = [
        "RequestLimitExceeded",
        "Throttling",
        "ThrottlingException",
        "RequestThrottled",
        "RequestThrottledException"
    ]
    
    def __init__(self, log):
        self.log = log
        self.events = []
    
    def check_response(self, response, operation):
        error_code = response.get("Error", {}).get("Code", "")
        if error_code in self.THROTTLING_CODES:
            event = {
                "timestamp": time.time(),
                "operation": operation,
                "error_code": error_code,
                "message": response.get("Error", {}).get("Message", "")
            }
            self.events.append(event)
            self.log.warning(f"Throttling detected: {event}")
            return True
        return False
    
    def get_summary(self):
        return {
            "total_events": len(self.events),
            "by_operation": self._group_by("operation"),
            "timeline": self._get_timeline()
        }
```

### F.3 Control Plane Health Monitoring

#### Control Plane Metrics

| Metric | Source | Threshold | Alert |
|--------|--------|-----------|-------|
| Job queue depth | Jobs API | > 100 | Warning |
| Job processing latency | Jobs API | > 5 min | Warning |
| Manifest creation failures | Control plane logs | > 0 | Critical |
| Node registration failures | Control plane logs | > 5% | Warning |
| Checkpoint corruption | Control plane logs | > 0 | Critical |

#### Health Check Implementation

```python
def monitor_control_plane_health(clusters, stop_event, log):
    """
    Monitor control plane health for all clusters.
    """
    while not stop_event.is_set():
        for cluster in clusters:
            # Check job queue
            jobs = get_cluster_jobs(cluster.id)
            pending = [j for j in jobs if j.status in ["pending", "processing"]]
            if len(pending) > 100:
                log.warning(f"High job queue depth for {cluster.id}: {len(pending)}")
            
            # Check manifest status
            manifests = get_cluster_manifests(cluster.id)
            for m in manifests:
                if m.status in ["invalidated", "failed"]:
                    log.error(f"Manifest {m.id} in {m.status} state")
        
        time.sleep(30)
```

### F.4 Performance Degradation Detection

#### Performance Baseline

| Metric | Baseline (Phase 1) | Degradation Threshold |
|--------|-------------------|----------------------|
| Accelerator creation rate | ~20/min | < 10/min |
| Volume attachment time | < 2 min | > 5 min |
| Rebalance duration | Cluster-specific | > 2x baseline |
| Job processing rate | ~4/min | < 2/min |

#### Degradation Detection

```python
class PerformanceDegradationDetector:
    """
    Detects performance degradation during scale testing.
    """
    
    def __init__(self, baseline, degradation_factor=2.0):
        self.baseline = baseline
        self.degradation_factor = degradation_factor
        self.measurements = []
    
    def check_degradation(self, metric_name, current_value):
        baseline_value = self.baseline.get(metric_name)
        if baseline_value is None:
            return False
        
        threshold = baseline_value * self.degradation_factor
        if current_value > threshold:
            self.measurements.append({
                "metric": metric_name,
                "baseline": baseline_value,
                "current": current_value,
                "threshold": threshold
            })
            return True
        return False
```

---

## G. Failure Scenarios and Recovery

### G.1 Partial Cluster Failures

#### Failure Types

| Failure Type | Detection | Impact | Recovery |
|--------------|-----------|--------|----------|
| Node failure during scale | Health check | Partial rebalance | ASG replacement |
| Accelerator instance failure | ASG health | Download stall | ASG replacement |
| Volume attachment failure | EC2 state | Accelerator stall | Retry logic |
| Manifest corruption | Control plane | State inconsistency | Manual intervention |

#### Recovery Procedure

```python
def handle_partial_cluster_failure(cluster, failure_type, log):
    """
    Handle partial cluster failures during scale testing.
    """
    log.error(f"Partial failure detected for {cluster.id}: {failure_type}")
    
    if failure_type == "node_failure":
        # Wait for ASG replacement
        wait_for_cluster_health(cluster, timeout=300)
    
    elif failure_type == "accelerator_failure":
        # Accelerator ASG will auto-replace
        # Download resumes from checkpoint
        wait_for_accelerator_recovery(cluster, timeout=600)
    
    elif failure_type == "volume_attachment":
        # Retry attachment with rate limiting
        retry_volume_attachment(cluster, max_retries=5)
    
    elif failure_type == "manifest_corruption":
        # Force teardown and restart
        force_teardown_manifest(cluster)
        trigger_new_rebalance(cluster)
```

### G.2 AWS API Throttling Events

#### Throttling Response Matrix

| Throttling Level | Detection | Response | Recovery Time |
|------------------|-----------|----------|---------------|
| Light (< 10/min) | Log warning | Continue with backoff | None |
| Moderate (10-50/min) | Log warning + alert | Pause new operations | 1-5 min |
| Severe (> 50/min) | Alert + halt | Stop all operations | 5-30 min |
| Persistent (> 5 min severe) | Emergency alert | Manual intervention | Variable |

#### Throttling Recovery

```python
class ThrottlingRecoveryHandler:
    """
    Handles recovery from AWS API throttling events.
    """
    
    def __init__(self, rate_limiter, log):
        self.rate_limiter = rate_limiter
        self.log = log
        self.throttle_count = 0
        self.last_throttle_time = None
    
    def handle_throttling(self, operation):
        self.throttle_count += 1
        self.last_throttle_time = time.time()
        
        # Exponential backoff
        backoff = min(2 ** self.throttle_count, 300)  # Max 5 min
        self.log.warning(f"Throttling detected, backing off for {backoff}s")
        time.sleep(backoff)
        
        # Reduce rate limiter capacity
        self.rate_limiter.reduce_rate(factor=0.5)
    
    def check_recovery(self):
        if self.last_throttle_time is None:
            return True
        
        time_since_throttle = time.time() - self.last_throttle_time
        if time_since_throttle > 300:  # 5 min recovery window
            self.throttle_count = 0
            self.rate_limiter.restore_rate()
            return True
        return False
```

### G.3 Control Plane Overload

#### Overload Symptoms

| Symptom | Detection | Severity |
|---------|-----------|----------|
| Job queue backup | Queue depth > 100 | Warning |
| Job timeout rate | > 10% timeouts | Warning |
| Database latency | > 1s for operations | Warning |
| Memory pressure | > 80% utilization | Critical |
| CPU saturation | > 90% utilization | Critical |

#### Overload Response

```python
def handle_control_plane_overload(metrics, log):
    """
    Respond to control plane overload conditions.
    """
    if metrics.queue_depth > 100:
        log.warning("Job queue backup detected, reducing operation rate")
        reduce_operation_rate(factor=0.5)
    
    if metrics.job_timeout_rate > 0.1:
        log.warning("High job timeout rate, pausing new operations")
        pause_new_operations(duration=60)
    
    if metrics.db_latency > 1.0:
        log.warning("Database latency elevated, throttling queries")
        throttle_database_queries(rate=100)
    
    if metrics.memory_utilization > 0.8:
        log.critical("Control plane memory pressure, initiating cleanup")
        trigger_garbage_collection()
    
    if metrics.cpu_utilization > 0.9:
        log.critical("Control plane CPU saturation, scaling out")
        request_control_plane_scaleout()
```

### G.4 Resource Exhaustion Handling

#### Resource Exhaustion Scenarios

| Resource | Exhaustion Point | Symptom | Recovery |
|----------|-------------------|---------|----------|
| EC2 instance limit | Account limit reached | InstanceLimitExceeded | Wait for termination |
| EBS volume limit | Account limit reached | VolumeLimitExceeded | Wait for cleanup |
| ASG limit | Region limit reached | ASGLimitExceeded | Wait for ASG deletion |
| ENI limit | Region limit reached | NetworkInterfaceLimitExceeded | Wait for ENI release |
| vCPU limit | Region limit reached | VcpuLimitExceeded | Wait for instance termination |

#### Exhaustion Recovery Procedure

```python
def handle_resource_exhaustion(error_code, cluster, log):
    """
    Handle AWS resource exhaustion events.
    """
    log.critical(f"Resource exhaustion detected: {error_code}")
    
    if error_code in ["InstanceLimitExceeded", "VcpuLimitExceeded"]:
        # Accelerate cleanup of completed accelerators
        trigger_accelerated_cleanup(cluster)
        # Wait for resources to free
        wait_for_resource_availability("ec2:instances", timeout=1800)
    
    elif error_code == "VolumeLimitExceeded":
        # Clean up orphaned volumes
        cleanup_orphaned_volumes(cluster)
        wait_for_resource_availability("ec2:volumes", timeout=600)
    
    elif error_code == "ASGLimitExceeded":
        # Clean up completed ASGs
        cleanup_completed_asgs(cluster)
        wait_for_resource_availability("autoscaling:groups", timeout=300)
    
    elif error_code == "NetworkInterfaceLimitExceeded":
        # Clean up detached ENIs
        cleanup_detached_enis(cluster)
        wait_for_resource_availability("ec2:network-interfaces", timeout=300)
```

---

## H. Safe Operating Limits

### H.1 Recommended Maximum Concurrent Operations

#### Single Cluster Limits

| Operation | Maximum Concurrent | Rationale |
|-----------|-------------------|-----------|
| Node additions | 27 | All nodes in parallel |
| Node removals | 27 | All nodes in parallel |
| Accelerator deployments | 594 | Limited by slots |
| Volume creations | 594 | One per accelerator |
| ASG creations | 594 | One per accelerator |

#### Multi-Cluster Limits

| Scale | Max Clusters | Max Accelerators | Recommended Stagger |
|-------|--------------|------------------|---------------------|
| Small | 1 | ~600 | None |
| Medium | 5 | ~3000 | 60s between clusters |
| Large | 10 | ~6000 | 120s between clusters |
| Maximum | 20 | ~12000 | 300s between clusters |

#### Account-Level Limits

| Metric | Safe Limit | Maximum Observed | Buffer |
|--------|------------|------------------|--------|
| API calls/second | 4 | 5 | 20% |
| Concurrent jobs | 50 | 100 | 50% |
| Pending manifests | 20 | 50 | 60% |
| Active accelerators | 5000 | 6000 | 17% |

### H.2 Staggered Operation Strategies

#### Recommended Stagger Configuration

```python
RECOMMENDED_STAGGER_CONFIG = {
    # Minimum time between cluster operations
    "cluster_stagger_min": 60,  # seconds
    
    # Time to wait for each node batch within a cluster
    "node_batch_stagger": 10,  # seconds
    
    # Number of nodes to process per batch
    "node_batch_size": 9,  # 3 batches for 27 nodes
    
    # Maximum parallel clusters per region
    "max_parallel_clusters": 10,
    
    # Maximum parallel regions per account
    "max_parallel_regions": 5,
    
    # Rate limiter configuration
    "rate_limiter": {
        "refill_rate": 4,  # Below AWS limit
        "burst_capacity": 40,  # Below AWS limit
    }
}
```

#### Stagger Timing Calculation

For N clusters:
```
Total time = (N - 1) × cluster_stagger + single_cluster_operation_time

Example for 10 clusters:
- Cluster stagger: 120s
- Single cluster operation: 1800s (30 min)
- Total time: 9 × 120 + 1800 = 1080 + 1800 = 2880s (~48 min)
```

### H.3 Resource Reservation Guidelines

#### Reserved Capacity for Safety

| Resource | Production Load | Reserved Buffer | Total Required |
|----------|-----------------|-----------------|----------------|
| EC2 instances | 5000 | 1000 (20%) | 6000 |
| EBS volumes | 5000 | 1000 (20%) | 6000 |
| ASGs | 500 | 100 (20%) | 600 |
| vCPUs | 5000 | 1000 (20%) | 6000 |
| API burst tokens | 40 | 10 (25%) | 50 |

#### Pre-Test Checklist

```markdown
- [ ] AWS Service Quotas increased for:
  - [ ] EC2 instances: Request limit 10,000
  - [ ] EBS volumes: Request limit 10,000
  - [ ] Auto Scaling Groups: Request limit 2,000
  - [ ] Launch Templates: Request limit 5,000
  
- [ ] Control plane resources verified:
  - [ ] Job queue capacity: > 200
  - [ ] Database capacity: Sufficient
  - [ ] Memory headroom: > 30%
  - [ ] CPU headroom: > 30%
  
- [ ] Monitoring configured:
  - [ ] CloudWatch alarms for throttling
  - [ ] Control plane health checks
  - [ ] Resource utilization dashboards
```

---

## I. Test Configuration

### I.1 Test Parameters for Each Phase

#### Phase Configuration Files

```ini
# phase1_single_cluster.ini
[DEFAULT]
aws_access_key = ${AWS_ACCESS_KEY_ID}
aws_secret_key = ${AWS_SECRET_ACCESS_KEY}
region = us-east-1

[ScaleTest]
clusters = 1
nodes_per_cluster = 27
expected_accelerators = 594
data_size_gb = 1000
operation_type = horizontal_scale
scale_direction = add
scale_count = 5

[Monitoring]
resource_poll_interval = 60
health_check_interval = 30
throttling_detection = True

[Timeouts]
accelerator_creation_timeout = 1800
rebalance_timeout = 3600
cleanup_timeout = 1800
total_test_timeout = 7200
```

```ini
# phase2_five_clusters.ini
[DEFAULT]
aws_access_key = ${AWS_ACCESS_KEY_ID}
aws_secret_key = ${AWS_SECRET_ACCESS_KEY}
region = us-east-1

[ScaleTest]
clusters = 5
nodes_per_cluster = 27
expected_accelerators = 2970
data_size_gb = 1000
operation_type = horizontal_scale
scale_direction = add
scale_count = 5

[Stagger]
cluster_start_delay = 60
node_batch_size = 9
node_batch_delay = 10

[Monitoring]
resource_poll_interval = 60
health_check_interval = 30
throttling_detection = True
cross_cluster_monitoring = True

[Timeouts]
accelerator_creation_timeout = 2400
rebalance_timeout = 5400
cleanup_timeout = 3600
total_test_timeout = 10800
```

```ini
# phase3_ten_clusters.ini
[DEFAULT]
aws_access_key = ${AWS_ACCESS_KEY_ID}
aws_secret_key = ${AWS_SECRET_ACCESS_KEY}
region = us-east-1

[ScaleTest]
clusters = 10
nodes_per_cluster = 27
expected_accelerators = 5940
data_size_gb = 1000
operation_type = horizontal_scale
scale_direction = add
scale_count = 5

[Stagger]
cluster_start_delay = 120
node_batch_size = 9
node_batch_delay = 10

[RateLimiting]
max_api_rate = 4
burst_capacity = 40
enable_adaptive_backoff = True

[Monitoring]
resource_poll_interval = 60
health_check_interval = 30
throttling_detection = True
cross_cluster_monitoring = True
control_plane_monitoring = True

[Timeouts]
accelerator_creation_timeout = 3600
rebalance_timeout = 7200
cleanup_timeout = 5400
total_test_timeout = 14400
```

### I.2 AWS Credential Management

#### Credential Configuration

```python
# credentials_config.py
import os
from typing import Dict

class ScaleTestCredentials:
    """
    Manages AWS credentials for scale testing.
    """
    
    @staticmethod
    def get_credentials() -> Dict[str, str]:
        """
        Get AWS credentials from environment or secrets manager.
        """
        return {
            "access_key": os.environ.get("AWS_ACCESS_KEY_ID"),
            "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "region": os.environ.get("AWS_REGION", "us-east-1"),
            "session_token": os.environ.get("AWS_SESSION_TOKEN")  # Optional
        }
    
    @staticmethod
    def validate_credentials(credentials: Dict[str, str]) -> bool:
        """
        Validate that required credentials are present.
        """
        required = ["access_key", "secret_key"]
        return all(credentials.get(k) for k in required)
```

### I.3 Multi-Cluster Coordination

#### Coordinator Implementation

```python
# scale_test_coordinator.py
import threading
import time
from dataclasses import dataclass
from typing import List, Dict
from queue import Queue

@dataclass
class ClusterConfig:
    cluster_id: str
    nodes: int
    region: str
    start_delay: int

class ScaleTestCoordinator:
    """
    Coordinates multi-cluster scale testing operations.
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.clusters: List[ClusterConfig] = []
        self.operation_queue = Queue()
        self.stop_event = threading.Event()
        self.monitors = []
        self.lock = threading.Lock()
        
    def setup_clusters(self):
        """
        Setup all clusters for testing.
        """
        for i in range(self.config["clusters"]):
            cluster = ClusterConfig(
                cluster_id=f"scale-test-cluster-{i}",
                nodes=self.config["nodes_per_cluster"],
                region=self.config["region"],
                start_delay=i * self.config.get("cluster_start_delay", 60)
            )
            self.clusters.append(cluster)
    
    def dispatch_operations(self):
        """
        Dispatch scale operations with stagger.
        """
        for cluster in self.clusters:
            time.sleep(cluster.start_delay)
            
            # Create operation thread
            op_thread = threading.Thread(
                target=self._execute_cluster_operation,
                args=(cluster,)
            )
            op_thread.start()
            self.monitors.append(op_thread)
    
    def _execute_cluster_operation(self, cluster: ClusterConfig):
        """
        Execute scale operation for a single cluster.
        """
        # Implementation details...
        pass
    
    def wait_for_completion(self, timeout: int = None):
        """
        Wait for all operations to complete.
        """
        for monitor in self.monitors:
            monitor.join(timeout)
    
    def trigger_shutdown(self):
        """
        Signal all operations to stop.
        """
        self.stop_event.set()
```

### I.4 Log Aggregation Strategy

#### Log Collection Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Log Aggregation                              │
│                                                                      │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐           │
│  │  Cluster 1    │  │  Cluster 2    │  │  Cluster N    │           │
│  │  Logs         │  │  Logs         │  │  Logs         │           │
│  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘           │
│          │                  │                  │                    │
│          ▼                  ▼                  ▼                    │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                    Central Log Collector                        ││
│  │  - S3 log ingestion                                             ││
│  │  - CloudTrail aggregation                                       ││
│  │  - Application log parsing                                      ││
│  └─────────────────────────────────────────────────────────────────┘│
│                              │                                       │
│                              ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                    Analysis & Reporting                         ││
│  │  - Error pattern detection                                      ││
│  │  - Performance metric extraction                                ││
│  │  - Throttling event correlation                                 ││
│  │  - Resource utilization trending                                ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

#### Log Collection Implementation

```python
# log_aggregator.py
import boto3
import json
from datetime import datetime
from typing import Dict, List

class ScaleTestLogAggregator:
    """
    Aggregates logs from all scale test clusters.
    """
    
    def __init__(self, log_bucket: str, region: str):
        self.s3 = boto3.client('s3', region_name=region)
        self.log_bucket = log_bucket
        self.logs = []
    
    def collect_accelerator_logs(self, cluster_id: str, rebalance_id: str):
        """
        Collect accelerator logs from S3 for a cluster.
        """
        prefix = f"accelerator-logs/{cluster_id}/{rebalance_id}/"
        response = self.s3.list_objects_v2(
            Bucket=self.log_bucket,
            Prefix=prefix
        )
        
        for obj in response.get('Contents', []):
            log_obj = self.s3.get_object(
                Bucket=self.log_bucket,
                Key=obj['Key']
            )
            content = log_obj['Body'].read().decode('utf-8')
            self.logs.append({
                "cluster_id": cluster_id,
                "rebalance_id": rebalance_id,
                "log_file": obj['Key'],
                "content": content,
                "timestamp": datetime.now().isoformat()
            })
    
    def analyze_throttling_events(self) -> List[Dict]:
        """
        Analyze collected logs for throttling events.
        """
        throttling_events = []
        for log in self.logs:
            if "Throttling" in log["content"] or "RequestLimitExceeded" in log["content"]:
                throttling_events.append({
                    "cluster_id": log["cluster_id"],
                    "log_file": log["log_file"],
                    "timestamp": log["timestamp"]
                })
        return throttling_events
    
    def generate_summary_report(self) -> Dict:
        """
        Generate summary report from aggregated logs.
        """
        return {
            "total_log_files": len(self.logs),
            "throttling_events": len(self.analyze_throttling_events()),
            "clusters_processed": len(set(log["cluster_id"] for log in self.logs)),
            "collection_timestamp": datetime.now().isoformat()
        }
```

---

## J. Validation Checklist

### J.1 Pre-Test Validation

- [ ] AWS Service Quotas increased
  - [ ] EC2 instances: ≥ 10,000
  - [ ] EBS volumes: ≥ 10,000
  - [ ] Auto Scaling Groups: ≥ 2,000
  - [ ] Launch Templates: ≥ 5,000

- [ ] Control plane health verified
  - [ ] Job queue empty
  - [ ] Database connections healthy
  - [ ] No pending manifests
  - [ ] Memory utilization < 50%
  - [ ] CPU utilization < 50%

- [ ] Test environment ready
  - [ ] AWS credentials configured
  - [ ] TAF dependencies installed
  - [ ] Test configurations validated
  - [ ] Monitoring dashboards active

### J.2 During-Test Validation

- [ ] Resource monitoring active
  - [ ] EC2 instance counts tracked
  - [ ] EBS volume counts tracked
  - [ ] ASG counts tracked
  - [ ] API call rates monitored

- [ ] Throttling detection active
  - [ ] CloudTrail events monitored
  - [ ] Application errors tracked
  - [ ] Retry rates measured

- [ ] Control plane health monitored
  - [ ] Job queue depth < 100
  - [ ] No manifest corruption
  - [ ] No checkpoint failures

### J.3 Post-Test Validation

- [ ] All clusters healthy
  - [ ] Status = "healthy"
  - [ ] Fusion status = "enabled"
  - [ ] No rebalance failures

- [ ] Resource cleanup verified
  - [ ] Accelerator instances = 0
  - [ ] EBS guest volumes = 0
  - [ ] ASGs cleaned up

- [ ] Data consistency verified
  - [ ] Item counts match
  - [ ] No data corruption
  - [ ] No orphaned resources

- [ ] Metrics collected
  - [ ] Total duration recorded
  - [ ] Throttling event count
  - [ ] Resource utilization data
  - [ ] Error rates documented

---

## K. Summary of Key Findings

### AWS Resource Limits

| Finding | Impact | Mitigation |
|---------|--------|-------------|
| EC2 instance limit (default 5000) | Will be exceeded at 6000 accelerators | Request limit increase to 10,000+ |
| ASG limit (default 500) | Will be exceeded at 6000 ASGs | Request limit increase to 2,000+ |
| EBS volume limit (default 5000) | Will be exceeded at 6000 volumes | Request limit increase to 10,000+ |
| API rate limit (5 req/s) | Account-level, shared across clusters | Implement staggered operations |

### Control Plane Scalability

| Finding | Impact | Mitigation |
|---------|--------|-------------|
| maxJobs = 2 per type | Limits concurrent operations | Design for queue processing rate |
| Job processing ~4/min | Bottleneck for 6000 accelerators | Allow extended test duration |
| Manifest parts storage | 5940 documents for 10 clusters | Within database capacity |

### Recommended Safe Limits

| Parameter | Safe Limit | Maximum Observed | Notes |
|-----------|------------|------------------|-------|
| Parallel clusters | 10 | 20 | With 120s stagger |
| API rate | 4 req/s | 5 req/s | 20% buffer |
| Job queue depth | 50 | 100 | Before degradation |
| Total accelerators | 5000 | 6000 | With AWS limit increases |

---

## References

### TAF Documentation
- `pytests/aGoodDoctor/fusion/architecture.md` - Architecture documentation
- `pytests/aGoodDoctor/fusion/README.md` - Quick start guide
- `pytests/aGoodDoctor/fusion/TEST_PLAN.md` - Exhaustive test plan
- `AGENTS.md` - Root TAF coding guidelines

### Couchbase-Cloud Source
- `internal/clusters/fusion/accelerator/accelerator.go` - Accelerator orchestration, rate limits
- `internal/clusters/fusion/infra/limiter.go` - Rate limiting implementation
- `internal/clusters/fusion/infra/aws/deployer.go` - AWS deployment, retry configuration
- `internal/clusters/fusion/jobs/jobs.go` - Job types
- `internal/clusters/fusion/jobs/processor.go` - Job processing
- `internal/clusters/fusion/queuer/queuer.go` - Job queuing, maxJobs constant

### AWS Documentation
- [EC2 Service Quotas](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-on-demand-instances.html)
- [EBS Volume Limits](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/volume_constraints.html)
- [Auto Scaling Limits](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-account-limits.html)
- [API Request Rate](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/throttling.html)

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-04-10 | Droid | Initial scale test plan creation |

---

*This scale test plan should be reviewed before each major scale test execution. Update AWS limit requirements based on current account quotas.*
