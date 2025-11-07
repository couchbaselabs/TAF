# Fusion Test Plan - Exhaustive Implementation Roadmap

This document provides an exhaustive test plan for validating Couchbase Capella fusion storage functionality across AWS infrastructure. It is derived from analysis of both the TAF fusion test implementation and the couchbase-cloud fusion deployment source code.

---

## Document Information

| Field | Value |
|-------|-------|
| Created | 2025-04-09 |
| Status | Draft |
| Related Files | TAF: `pytests/aGoodDoctor/fusion/`, `pytests/storage/fusion/` |
| | couchbase-cloud: `internal/clusters/fusion/` |

---

## Executive Summary

This test plan covers **10 major test categories** with **92 individual test cases** designed to validate:

1. Fusion accelerator lifecycle (creation, operation, termination)
2. EBS guest volume management (creation, hydration, cleanup)
3. Horizontal and vertical scaling scenarios
4. S3 log store operations
5. AWS fault injection scenarios
6. Rebalance operations with fusion
7. Data consistency validation
8. Error detection and recovery
9. Control plane integration
10. Performance and rate limiting

---

## A. Fusion Accelerator Lifecycle Tests

### A.1 Accelerator Creation During Rebalance

**Test Description:** Validate that accelerator instances are created correctly during rebalance operations.

**AWS Resources to Validate:**
- EC2 instances with `couchbase-cloud-function: fusion-accelerator` tag
- EBS volumes with 16,000 IOPS (fusion accelerator characteristic)
- Auto Scaling Groups for accelerator instances

**Expected Control Plane Behavior:**
- Accelerator nodes created per shard per host
- Manifest split determines number of accelerators
- Launch templates created for both x86_64 and ARM architectures
- Rate limiting applied (5 req/s refill rate, 50 burst tokens)

**Validation Steps:**
1. Initiate rebalance operation
2. Poll for accelerator instances using `list_accelerator_instances()`
3. Verify instance count matches manifest plan parts
4. Verify instances have correct IOPS (16,000)
5. Verify instances have `couchbase-cloud-fusion-rebalance` tag

**Edge Cases:**
- Parallel rebalances requiring slot management (max 22 slots/host)
- Accelerator creation with batching session for efficiency
- Accelerator creation during ongoing background migration

**Related Source Files:**
- TAF: `fusion_aws_util.py:list_accelerator_instances()`, `fusion_cp_resource_monitor.py:monitor_cluster_accelerator_instances()`
- couchbase-cloud: `accelerator/accelerator.go:accelerateHostShards()`, `infra/aws/deployer.go:Node()`

---

### A.2 Accelerator Instance Type Validation

**Test Description:** Validate that accelerator instances use correct instance types and configurations.

**AWS Resources to Validate:**
- EC2 instance types (validate compute configuration)
- EBS volume types (gp3 with max IOPS/throughput)

**Expected Control Plane Behavior:**
- Accelerator instances use configurable compute types
- EBS volumes created with gp3 type, 16,000 IOPS, 2,000 MiB/s throughput
- Volumes encrypted with customer-provided KMS key

**Validation Steps:**
1. Query accelerator instances
2. Verify instance type matches expected configuration
3. Verify EBS volume configuration (type, IOPS, throughput, encryption)
4. Verify volume size (min 50 GiB, with 10% buffer)

**Edge Cases:**
- Different instance types across availability zones
- Volume size calculation with shard size + 10% buffer
- Encryption key rotation scenarios

**Related Source Files:**
- TAF: `fusion_aws_util.py:list_instances()`, `fusion_cp_resource_monitor.py:log_fusion_guest_volumes_table()`
- couchbase-cloud: `infra/aws/volumes.go:Create()`, `infra/aws/deployer.go:createVolume()`

---

### A.3 Accelerator Termination After Rebalance

**Test Description:** Validate that accelerator instances are properly terminated after rebalance completion.

**AWS Resources to Validate:**
- EC2 instance state transitions
- Auto Scaling Group deletion
- Launch Template deletion
- EBS volume cleanup

**Expected Control Plane Behavior:**
- Accelerator nodes terminated after guest volume detachment
- ASG deleted before launch templates
- Volumes detached and deleted in sequence
- Background migration completed before termination

**Validation Steps:**
1. Complete rebalance operation
2. Monitor accelerator instance count → 0
3. Verify ASG cleanup via `list_cluster_fusion_asg()`
4. Verify no orphaned instances exist
5. Verify rebalance status in manifest

**Edge Cases:**
- Force tear down scenarios (cluster destroyed)
- Tear down during ongoing background migration
- Accelerator node recovery after failure

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py:monitor_fusion_accelerator_nodes_killed_after_rebalance()`, `check_asg_cleanup_after_rebalance()`
- couchbase-cloud: `accelerator/accelerator.go:TearDown()`, `infra/aws/destroyer.go:Node()`

---

### A.4 Accelerator Fallback Scenarios

**Test Description:** Validate accelerator fallback when primary instances fail.

**AWS Resources to Validate:**
- EC2 instance failure handling
- Auto Scaling Group instance replacement
- Volume re-attachment scenarios

**Expected Control Plane Behavior:**
- ASG automatically replaces failed instances
- Volume remains available during instance replacement
- Checkpoint recovery for download progress

**Validation Steps:**
1. Induce accelerator instance failure (stop/terminate)
2. Monitor ASG replacement behavior
3. Verify volume remains attached or available
4. Verify download resumes from checkpoint
5. Verify final rebalance success

**Edge Cases:**
- Failure during download phase
- Failure during volume transfer phase
- Multiple simultaneous failures

**Related Source Files:**
- TAF: `fusion_aws_util.py`, `fusion_cp_resource_monitor.py`
- couchbase-cloud: `accelerator/accelerator.go:manageAcceleratorNode()`, `infra/aws/autoscalinggroups.go`

---

### A.5 Accelerator Registration and Download Completion

**Test Description:** Validate accelerator node registration with control plane and download completion tracking.

**AWS Resources to Validate:**
- EC2 instance status checks
- Accelerator node document in control plane DB

**Expected Control Plane Behavior:**
- Accelerator registers after boot via dp-agent
- Status transitions: Pending → Registered → Complete
- Download completion communicated to control plane

**Validation Steps:**
1. Monitor accelerator node status via REST API
2. Track status transitions
3. Verify download completion notification
4. Verify node ready for volume detachment

**Edge Cases:**
- Registration timeout handling
- Download failure and retry
- Node status inconsistency recovery

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py`
- couchbase-cloud: `accelerator/accelerator.go:completeShardDownload()`, `nodes/service.go`

---

## B. EBS Guest Volume Tests

### B.1 Volume Creation and Attachment Timing

**Test Description:** Validate EBS guest volume creation timing and attachment to cluster nodes.

**AWS Resources to Validate:**
- EBS volume creation latency
- Volume attachment timing
- Volume state transitions (creating → available → in-use)

**Expected Control Plane Behavior:**
- Volume created with idempotency key for retry safety
- Volume detached from accelerator after download
- Volume attached to cluster node at correct device path

**Validation Steps:**
1. Monitor volume creation via `list_volumes_by_cluster_id()`
2. Track volume state transitions
3. Verify attachment timing meets SLA
4. Verify device path mapping

**Edge Cases:**
- Volume creation failure and retry
- Attachment point already in use error
- Volume attachment timeout

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py:monitor_fusion_guest_volumes()`, `fusion_monitor_util.py:get_attached_ebs_volumes_count()`
- couchbase-cloud: `infra/aws/volumes.go:Create()`, `Attach()`, `WaitAvailable()`, `WaitInUse()`

---

### B.2 Volume Hydration Monitoring

**Test Description:** Validate volume hydration process during fusion rebalance.

**AWS Resources to Validate:**
- EBS volume I/O activity
- Guest volume mount status on cluster nodes
- Fusion pending bytes during hydration

**Expected Control Plane Behavior:**
- Guest volumes mounted on cluster nodes
- Hydration progress tracked via FusionRestAPI
- Background migration proceeds until complete

**Validation Steps:**
1. Monitor attached volume count via REST API
2. Track fusion pending bytes
3. Verify hydration completion (attached → 0)
4. Verify data accessible after hydration

**Edge Cases:**
- Hydration failure scenarios
- Partial hydration recovery
- Multiple concurrent hydrations

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py:monitor_fusion_guest_volumes()`, `fusion_monitor_util.py:log_fusion_pending_bytes()`
- couchbase-cloud: `accelerator/accelerator.go:manageGuestVolume()`, `cb/fusion.go`

---

### B.3 Volume Cleanup Verification (N → 0 Volumes)

**Test Description:** Validate complete cleanup of EBS guest volumes after rebalance.

**AWS Resources to Validate:**
- EBS volume count reduction
- Volume deletion confirmation
- No orphaned volumes remaining

**Expected Control Plane Behavior:**
- Volumes detached after background migration
- Volumes deleted after detachment
- Manifest marked as complete

**Validation Steps:**
1. Monitor volume count during cleanup
2. Verify volume count reaches 0
3. Verify no orphaned volumes via tag query
4. Verify manifest completion status

**Edge Cases:**
- Cleanup during cluster destruction
- Volume deletion failure handling
- Orphaned volume detection

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py:monitor_ebs_cleanup()`, `check_ebs_guest_vol_deletion()`
- couchbase-cloud: `infra/aws/destroyer.go:Node()`, `accelerator/accelerator.go:tearDown()`

---

### B.4 Volume IOPS and Size Validation

**Test Description:** Validate EBS volume IOPS and size configuration.

**AWS Resources to Validate:**
- EBS volume IOPS (16,000 for accelerator, scaled down for cluster)
- EBS volume size (min 50 GiB, variable based on shard size)

**Expected Control Plane Behavior:**
- Initial IOPS: 16,000 for fast download
- Post-download IOPS scaled down based on parts per host
- Size = shard_size + 10% buffer (min 50 GiB)

**Validation Steps:**
1. Query volume IOPS and size during creation
2. Verify initial high IOPS configuration
3. Verify IOPS scale-down after attachment to cluster
4. Verify size matches expected value

**Edge Cases:**
- IOPS modification failure (non-blocking)
- Size calculation edge cases
- Volume modification retry logic

**Related Source Files:**
- TAF: `fusion_aws_util.py:list_instances()`, `fusion_cp_resource_monitor.py:log_fusion_guest_volumes_table()`
- couchbase-cloud: `infra/aws/volumes.go:Modify()`, `accelerator/accelerator.go:ScaleDownGuestVolume()`

---

### B.5 Volume Tagging Verification

**Test Description:** Validate correct tagging of EBS volumes for tracking.

**AWS Resources to Validate:**
- Volume tags: `couchbase-cloud-cluster-id`, `couchbase-cloud-function`, `couchbase-cloud-fusion-rebalance`, `key` (node ID)

**Expected Control Plane Behavior:**
- All required tags present on volume creation
- Tags used for volume discovery and tracking
- Fusion rebalance tag enables correlation

**Validation Steps:**
1. Query volumes with expected filters
2. Verify all required tags present
3. Verify tag values match expected
4. Use tags for volume lifecycle tracking

**Edge Cases:**
- Tag propagation delays
- Tag-based volume discovery failures
- Missing tag recovery

**Related Source Files:**
- TAF: `fusion_aws_util.py:list_accelerator_instances()`, `fusion_cp_resource_monitor.py:get_fusion_rebalance_tag()`
- couchbase-cloud: `infra/aws/volumes.go:Create()`, `clusters/tags/fusion.go`

---

### B.6 Volume Slot Management for Parallel Rebalances

**Test Description:** Validate volume slot management when multiple rebalances run in parallel.

**AWS Resources to Validate:**
- Guest volume slot assignment per host
- Slot availability tracking
- Maximum slot enforcement (22 slots/host)

**Expected Control Plane Behavior:**
- Available slots calculated before acceleration
- Non-consecutive slot assignment supported
- MaxSlotsErr returned when insufficient slots

**Validation Steps:**
1. Initiate parallel rebalances
2. Monitor slot assignments
3. Verify no slot conflicts
4. Verify error handling when max slots reached

**Edge Cases:**
- Slot gaps from previous rebalances
- Slot reclamation after rebalance completion
- Slot exhaustion handling

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py`
- couchbase-cloud: `accelerator/accelerator.go:ValidateParallelRebalance()`, `findAvailableShardSlots()`

---

## C. Horizontal Scaling Tests

### C.1 Add Nodes with Fusion Enabled

**Test Description:** Validate adding nodes to cluster with fusion enabled.

**AWS Resources to Validate:**
- New cluster node instances
- Accelerator instances for rebalance
- Guest volumes for data transfer

**Expected Control Plane Behavior:**
- Fusion remains enabled during scaling
- Accelerator instances created for incoming data
- Rebalance uses fusion acceleration

**Validation Steps:**
1. Initiate node addition
2. Verify fusion status remains "enabled"
3. Monitor accelerator lifecycle
4. Verify rebalance completion with fusion

**Edge Cases:**
- Adding nodes during ongoing rebalance
- Adding nodes to cluster with background migration
- Adding multiple node types simultaneously

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()`, `fusion_monitor_util.py:wait_for_fusion_status()`
- couchbase-cloud: `accelerator/accelerator.go:NewAcceleration()`, `rebalance/rebalance.go:OpRebalanceIn`

---

### C.2 Remove Nodes with Fusion Enabled

**Test Description:** Validate removing nodes from cluster with fusion enabled.

**AWS Resources to Validate:**
- Remaining cluster node instances
- Accelerator instances for data transfer
- Guest volumes for data migration

**Expected Control Plane Behavior:**
- Fusion remains enabled during scaling
- Accelerator instances created for outgoing data
- Clean node removal with data preservation

**Validation Steps:**
1. Initiate node removal
2. Verify fusion status remains "enabled"
3. Monitor accelerator lifecycle
4. Verify data preserved on remaining nodes

**Edge Cases:**
- Removing nodes during active workload
- Removing master node
- Graceful vs failover removal

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()`
- couchbase-cloud: `accelerator/accelerator.go:NewAcceleration()`, `rebalance/rebalance.go:OpRebalanceOut`

---

### C.3 Swap Rebalance Scenarios

**Test Description:** Validate swap rebalance with fusion acceleration.

**AWS Resources to Validate:**
- Outgoing node instances
- Incoming node instances
- Accelerator instances for data transfer

**Expected Control Plane Behavior:**
- Simultaneous node removal and addition
- Fusion acceleration for data transfer
- Minimal cluster disruption

**Validation Steps:**
1. Initiate swap rebalance
2. Verify outgoing node removal
3. Verify incoming node addition
4. Verify data consistency

**Edge Cases:**
- Swap with different node types
- Swap during active workload
- Master node swap

**Related Source Files:**
- TAF: `fusion_sanity.py:test_multiple_random_fusion_rebalances()`
- couchbase-cloud: `rebalance/rebalance.go:OpSwapRebalance`

---

### C.4 Multi-Node Scaling in Parallel

**Test Description:** Validate adding/removing multiple nodes simultaneously.

**AWS Resources to Validate:**
- Multiple accelerator instances
- Multiple guest volumes
- ASG capacity management

**Expected Control Plane Behavior:**
- Parallel accelerator deployment
- Rate-limited API calls
- Manifest tracks all parts

**Validation Steps:**
1. Initiate multi-node scaling
2. Verify parallel accelerator creation
3. Monitor rate limiting behavior
4. Verify all nodes integrated successfully

**Edge Cases:**
- Partial failure handling
- Rate limit throttling
- Resource contention

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()`, `fusion_cp_resource_monitor.py:monitor_cluster_accelerator_instances()`
- couchbase-cloud: `accelerator/accelerator.go:distributeAccelerators()`, `infra/limiter.go`

---

## D. Vertical Scaling Tests

### D.1 Disk Scaling with Fusion

**Test Description:** Validate disk scaling with fusion enabled.

**AWS Resources to Validate:**
- EBS volume size changes
- Guest volume behavior during scaling
- Fusion status during operation

**Expected Control Plane Behavior:**
- Disk resize triggers rebalance
- Fusion acceleration for data movement
- Guest volumes used during scaling

**Validation Steps:**
1. Initiate disk scaling
2. Verify rebalance triggered
3. Monitor accelerator lifecycle
4. Verify final disk size

**Edge Cases:**
- Disk scaling during workload
- Multiple disk scaling operations
- Disk scaling with background migration

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()` (v_scaling section)
- couchbase-cloud: `accelerator/accelerator.go`

---

### D.2 Compute Scaling with Fusion

**Test Description:** Validate compute (instance type) scaling with fusion enabled.

**AWS Resources to Validate:**
- EC2 instance type changes
- Guest volume re-attachment
- Accelerator behavior during scaling

**Expected Control Plane Behavior:**
- Compute scaling triggers node replacement
- Guest volumes detached and re-attached
- Fusion data preserved

**Validation Steps:**
1. Initiate compute scaling
2. Verify node replacement process
3. Monitor guest volume lifecycle
4. Verify data consistency

**Edge Cases:**
- Compute scaling to incompatible type
- Compute scaling during workload
- Cross-architecture scaling

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()` (compute section)
- couchbase-cloud: `infra/aws/deployer.go`

---

### D.3 Combined Disk + Compute Scaling

**Test Description:** Validate simultaneous disk and compute scaling.

**AWS Resources to Validate:**
- EBS volume changes
- EC2 instance changes
- Accelerator coordination

**Expected Control Plane Behavior:**
- Combined scaling handled as single operation
- Fusion acceleration for data movement
- All changes applied atomically

**Validation Steps:**
1. Initiate combined scaling
2. Verify both changes applied
3. Monitor accelerator lifecycle
4. Verify final configuration

**Edge Cases:**
- Partial scaling failure
- Scaling order dependencies
- Resource limits

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()` (vh_scaling section)
- couchbase-cloud: `accelerator/accelerator.go`

---

### D.4 Scaling During Ongoing Operations

**Test Description:** Validate scaling during active data operations.

**AWS Resources to Validate:**
- Accelerator instances under load
- Guest volumes during active I/O
- Cluster performance metrics

**Expected Control Plane Behavior:**
- Operations continue during scaling
- Fusion handles concurrent access
- No data loss or corruption

**Validation Steps:**
1. Start continuous workload
2. Initiate scaling operation
3. Monitor data consistency
4. Verify no errors or data loss

**Edge Cases:**
- High-throughput workload during scaling
- Read/write conflicts
- Timeout scenarios

**Related Source Files:**
- TAF: `fusion_volume.py:normal_mutations()`, `fusion_sanity.py:test_fusion_rebalance_at_scale()`
- couchbase-cloud: `accelerator/accelerator.go`

---

## E. S3 Log Store Tests

### E.1 Log Store URI Discovery

**Test Description:** Validate S3 log store URI discovery and configuration.

**AWS Resources to Validate:**
- S3 bucket containing fusion logs
- Bucket policy for accelerator access
- Log store URI format

**Expected Control Plane Behavior:**
- Log store URI derived from storage configuration
- Bucket created with appropriate policies
- URI accessible from accelerators

**Validation Steps:**
1. Query fusion log store URI
2. Verify bucket exists and accessible
3. Verify bucket policy allows accelerator access
4. Verify URI format correctness

**Edge Cases:**
- Cross-region bucket access
- Bucket policy changes
- URI rotation scenarios

**Related Source Files:**
- TAF: `fusion_monitor_util.py:get_fusion_s3_uri()`
- couchbase-cloud: `infra/aws/storage.go`, `networking_bucket_policy.go`

---

### E.2 Log Store Size Consistency

**Test Description:** Validate log store size consistency across cluster nodes.

**AWS Resources to Validate:**
- S3 object sizes
- Fusion log store data size stats
- Storage class distribution

**Expected Control Plane Behavior:**
- Node stats match S3 actual size
- Data distributed across storage classes
- Size calculations accurate

**Validation Steps:**
1. Query fusion log store data size via cbstats
2. Query S3 bucket size
3. Compare values for consistency
4. Track size changes over time

**Edge Cases:**
- Size calculation delays
- Storage class transitions
- Multi-bucket scenarios

**Related Source Files:**
- TAF: `fusion_monitor_util.py:get_fusion_log_store_data_size_on_s3()`, `run_cbstats_on_all_nodes()`
- couchbase-cloud: `accelerator/accelerator.go:splitManifest()`, `fusiontools/`

---

### E.3 Log Store Object Lifecycle

**Test Description:** Validate log file lifecycle in S3 (creation, access, deletion).

**AWS Resources to Validate:**
- S3 object creation patterns
- Object access patterns
- Object deletion patterns

**Expected Control Plane Behavior:**
- Log files created during data operations
- Files accessed during reads
- Files deleted after checkpoint advancement

**Validation Steps:**
1. Monitor S3 object creation
2. Track object access patterns
3. Verify deletion after checkpoint
4. Validate CloudTrail deletion logs

**Edge Cases:**
- Deletion during active read
- Checkpoint rollback scenarios
- Orphaned log files

**Related Source Files:**
- TAF: `fusion_monitor_util.py`, `awslib/cloudtrail_delete_setup.py`
- couchbase-cloud: `fusiontools/`, `agent/downloader/`

---

### E.4 S3 Deletion Verification

**Test Description:** Verify S3 object deletion is happening correctly.

**AWS Resources to Validate:**
- CloudTrail logs for delete operations
- S3 bucket object count
- Deleted object verification

**Expected Control Plane Behavior:**
- Deletions logged in CloudTrail
- Objects removed after checkpoint
- No unauthorized deletions

**Validation Steps:**
1. Setup CloudTrail delete logging
2. Monitor deletion events
3. Verify deletions correspond to checkpoints
4. Verify bucket object count decreases

**Edge Cases:**
- Deletion failures
- Partial deletion scenarios
- CloudTrail gaps

**Related Source Files:**
- TAF: `awslib/cloudtrail_delete_setup.py`
- couchbase-cloud: `fusiontools/`

---

## F. AWS Fault Injection Tests

### F.1 Accelerator Instance Failure

**Test Description:** Validate behavior when accelerator instance fails.

**AWS Resources to Validate:**
- EC2 instance stop/terminate
- ASG replacement behavior
- Volume preservation

**Expected Control Plane Behavior:**
- ASG replaces failed instance
- Volume remains available
- Download resumes from checkpoint

**Validation Steps:**
1. Deploy accelerator instance
2. Induce failure (stop/terminate)
3. Monitor ASG replacement
4. Verify download completion

**Edge Cases:**
- Multiple simultaneous failures
- Failure during volume attachment
- Failure during volume detachment

**Related Source Files:**
- TAF: `awslib/fis_lib.py`
- couchbase-cloud: `infra/aws/autoscalinggroups.go`, `accelerator/accelerator.go`

---

### F.2 EBS Volume Attachment Failure

**Test Description:** Validate behavior when EBS volume attachment fails.

**AWS Resources to Validate:**
- EBS volume state
- Attachment point conflicts
- Error handling

**Expected Control Plane Behavior:**
- Attachment retry on failure
- Fallback to different device path
- Clear error reporting

**Validation Steps:**
1. Attempt volume attachment
2. Induce attachment failure
3. Verify retry behavior
4. Verify eventual success or clear failure

**Edge Cases:**
- All attachment points occupied
- Volume corruption
- Attachment timeout

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py`
- couchbase-cloud: `infra/aws/volumes.go:Attach()`, error handling

---

### F.3 Network Partition Scenarios

**Test Description:** Validate behavior during network partition.

**AWS Resources to Validate:**
- VPC endpoint availability
- Security group rules
- Network connectivity

**Expected Control Plane Behavior:**
- S3 access via VPC endpoint
- Retry on transient failures
- Clear timeout handling

**Validation Steps:**
1. Induce network partition
2. Monitor retry behavior
3. Verify timeout handling
4. Verify recovery after partition resolved

**Edge Cases:**
- Partial partition (some AZs affected)
- Long-duration partitions
- Partition during critical operation

**Related Source Files:**
- TAF: `fusion_network_partition.py`
- couchbase-cloud: `infra/aws/networking_vpc_endpoints.go`, `networking_security_groups.go`

---

### F.4 AZ Failure Simulation

**Test Description:** Validate behavior when an availability zone fails.

**AWS Resources to Validate:**
- Instances in affected AZ
- Volumes in affected AZ
- Cross-AZ failover

**Expected Control Plane Behavior:**
- Operations continue in other AZs
- Accelerator instances in other AZs handle load
- Clear error for AZ-specific resources

**Validation Steps:**
1. Identify AZ with accelerators
2. Simulate AZ failure
3. Verify cross-AZ behavior
4. Verify recovery

**Edge Cases:**
- All accelerators in single AZ
- Primary node in failed AZ
- Storage in failed AZ

**Related Source Files:**
- TAF: `fusion_network_partition.py`
- couchbase-cloud: `accelerator/accelerator.go`, `infra/aws/networking.go`

---

## G. Rebalance Scenario Tests

### G.1 Graceful Rebalance with Fusion

**Test Description:** Validate graceful rebalance with fusion acceleration.

**AWS Resources to Validate:**
- Accelerator instance lifecycle
- Guest volume lifecycle
- Rebalance progress tracking

**Expected Control Plane Behavior:**
- Accelerator instances created
- Guest volumes mounted
- Rebalance completes with fusion

**Validation Steps:**
1. Initiate graceful rebalance
2. Monitor accelerator lifecycle
3. Monitor guest volume lifecycle
4. Verify rebalance completion

**Edge Cases:**
- Rebalance timeout
- Partial completion
- Retry scenarios

**Related Source Files:**
- TAF: `fusion_sanity.py:test_fusion_rebalance()`, `fusion_volume.py`
- couchbase-cloud: `accelerator/accelerator.go:Accelerate()`

---

### G.2 Failover Rebalance with Fusion

**Test Description:** Validate failover rebalance with fusion acceleration.

**AWS Resources to Validate:**
- Failed node handling
- Replica promotion
- Accelerator coordination

**Expected Control Plane Behavior:**
- Failover triggers rebalance
- Fusion accelerates data movement
- Data consistency maintained

**Validation Steps:**
1. Induce node failure
2. Verify failover initiation
3. Monitor fusion acceleration
4. Verify data consistency

**Edge Cases:**
- Multiple simultaneous failures
- Failover during ongoing rebalance
- Failover of accelerator node

**Related Source Files:**
- TAF: `fusion_failover_rebalance.py`
- couchbase-cloud: `accelerator/accelerator.go`

---

### G.3 DCP Rebalance Scenarios

**Test Description:** Validate DCP-based rebalance with fusion.

**AWS Resources to Validate:**
- DCP stream handling
- Accelerator coordination
- Data consistency

**Expected Control Plane Behavior:**
- DCP streams established
- Fusion accelerates backfill
- Stream consistency maintained

**Validation Steps:**
1. Initiate DCP rebalance
2. Monitor DCP streams
3. Verify fusion acceleration
4. Verify data consistency

**Edge Cases:**
- DCP stream failures
- High mutation rate during rebalance
- DCP backpressure

**Related Source Files:**
- TAF: `fusion_dcp_rebalance.py`
- couchbase-cloud: `accelerator/accelerator.go`

---

### G.4 Rebalance During Steady-State Workload

**Test Description:** Validate rebalance during active workload.

**AWS Resources to Validate:**
- Workload continuity
- Accelerator performance
- Data consistency

**Expected Control Plane Behavior:**
- Workload continues during rebalance
- Fusion handles concurrent operations
- No data loss or corruption

**Validation Steps:**
1. Start steady-state workload
2. Initiate rebalance
3. Monitor workload continuity
4. Verify data consistency

**Edge Cases:**
- High-throughput workload
- Read/write conflicts
- Memory pressure

**Related Source Files:**
- TAF: `fusion_sanity.py:test_fusion_rebalance_at_scale()`, `fusion_volume.py:normal_mutations()`
- couchbase-cloud: `accelerator/accelerator.go`

---

## H. Data Consistency Tests

### H.1 Item Count Validation After Rebalance

**Test Description:** Validate item count consistency after fusion rebalance.

**AWS Resources to Validate:**
- Cluster stats
- Bucket item counts
- Collection item counts

**Expected Control Plane Behavior:**
- Item counts preserved after rebalance
- Stats eventually consistent
- No data loss

**Validation Steps:**
1. Record pre-rebalance item counts
2. Perform fusion rebalance
3. Verify post-rebalance item counts
4. Compare with expected values

**Edge Cases:**
- Async replication lag
- Stats update delays
- Multi-collection scenarios

**Related Source Files:**
- TAF: `fusion_sanity.py`, `fusion_volume.py`, `fusion_base.py:validate_fusion_health()`
- couchbase-cloud: `accelerator/accelerator.go`

---

### H.2 Document Read Validation

**Test Description:** Validate document content after fusion rebalance.

**AWS Resources to Validate:**
- Document content verification
- Read performance
- Cache behavior

**Expected Control Plane Behavior:**
- Documents readable after rebalance
- Content preserved
- No corruption

**Validation Steps:**
1. Store documents with known content
2. Perform fusion rebalance
3. Read and verify documents
4. Compare with expected content

**Edge Cases:**
- Large documents
- Binary documents
- Encrypted documents

**Related Source Files:**
- TAF: `fusion_base.py:perform_batch_reads()`, `validate_fusion_health()`
- couchbase-cloud: `accelerator/accelerator.go`

---

### H.3 Fusion Storage vs NFS Storage Consistency

**Test Description:** Compare fusion storage stats with actual S3/NFS storage.

**AWS Resources to Validate:**
- S3 bucket size
- NFS directory size
- Fusion internal stats

**Expected Control Plane Behavior:**
- Internal stats match actual storage
- Size calculations consistent
- Differences explained by metadata

**Validation Steps:**
1. Query fusion internal stats
2. Query actual S3/NFS storage
3. Compare values
4. Analyze any discrepancies

**Edge Cases:**
- Compression effects
- Metadata overhead
- Storage class differences

**Related Source Files:**
- TAF: `fusion_monitor_util.py:get_fusion_log_store_data_size_on_s3()`, `fusion_base.py:log_fusion_storage_stats()`
- couchbase-cloud: `fusiontools/`

---

### H.4 Pending Bytes Monitoring

**Test Description:** Monitor pending bytes during fusion operations.

**AWS Resources to Validate:**
- Fusion pending bytes stats
- Sync session progress
- Background migration progress

**Expected Control Plane Behavior:**
- Pending bytes increase during operations
- Pending bytes decrease as sync progresses
- Eventually reaches zero

**Validation Steps:**
1. Initiate operation that creates pending bytes
2. Monitor pending bytes over time
3. Verify eventual resolution
4. Correlate with sync progress

**Edge Cases:**
- Persistent pending bytes
- Sync failures
- High pending bytes scenarios

**Related Source Files:**
- TAF: `fusion_monitor_util.py:log_fusion_pending_bytes()`
- couchbase-cloud: `cb/fusion.go`, `accelerator/accelerator.go`

---

## I. Error Detection & Recovery Tests

### I.1 Core Dump Detection

**Test Description:** Detect core dumps during fusion operations.

**AWS Resources to Validate:**
- Core dump files on cluster instances
- Crash directory monitoring
- Error correlation

**Expected Control Plane Behavior:**
- No core dumps during normal operations
- Core dumps indicate critical failures
- Recovery mechanisms in place

**Validation Steps:**
1. Monitor crash directories via SSM
2. Scan for core dump files
3. Correlate with operations
4. Report any findings

**Edge Cases:**
- Transient core dumps
- Core dump during rebalance
- Multiple core dumps

**Related Source Files:**
- TAF: `fusion_aws_util.py:scan_logs_for_errors_on_cluster_instances()`, `fusion_cp_resource_monitor.py:scan_memcached_logs_for_errors()`
- couchbase-cloud: Not directly covered (cluster-side)

---

### I.2 CRITICAL Error Scanning

**Test Description:** Scan for CRITICAL errors in memcached logs.

**AWS Resources to Validate:**
- Memcached log files
- Error patterns
- Fusion-specific errors

**Expected Control Plane Behavior:**
- No CRITICAL errors during normal operations
- CRITICAL errors indicate problems
- Error handling in place

**Validation Steps:**
1. Monitor memcached logs via SSM
2. Scan for CRITICAL patterns
3. Filter known non-critical patterns
4. Report findings

**Edge Cases:**
- False positives
- Known error patterns
- Transient errors

**Related Source Files:**
- TAF: `fusion_aws_util.py:scan_logs_for_errors_on_cluster_instances()`
- couchbase-cloud: Not directly covered (cluster-side)

---

### I.3 Hydration Failure Detection

**Test Description:** Detect and handle fusion hydration failures.

**AWS Resources to Validate:**
- Guest volume status
- Hydration progress
- Error patterns

**Expected Control Plane Behavior:**
- Hydration failures detected
- Retry mechanisms in place
- Clear error reporting

**Validation Steps:**
1. Monitor hydration progress
2. Detect failed hydrations
3. Verify retry behavior
4. Verify eventual success or clear failure

**Edge Cases:**
- Partial hydration failure
- Multiple hydration failures
- Recovery scenarios

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py:monitor_fusion_guest_volumes()`
- couchbase-cloud: `accelerator/accelerator.go:manageGuestVolume()`

---

### I.4 Recovery from Transient Failures

**Test Description:** Validate recovery from transient AWS API failures.

**AWS Resources to Validate:**
- AWS API retry behavior
- Checkpoint recovery
- Manifest state recovery

**Expected Control Plane Behavior:**
- Transient failures retried
- Operations resume from checkpoint
- No data loss

**Validation Steps:**
1. Induce transient failure
2. Verify retry behavior
3. Verify checkpoint recovery
4. Verify eventual success

**Edge Cases:**
- Multiple transient failures
- Long-duration failures
- Cascading failures

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py`
- couchbase-cloud: `infra/aws/deployer.go:defaultRetryOptions()`, `accelerator/accelerator.go`

---

## J. Control Plane Integration Tests

### J.1 Manifest Creation and Validation

**Test Description:** Validate fusion rebalance manifest creation and validation.

**AWS Resources to Validate:**
- Manifest document in control plane DB
- Manifest parts storage
- Plan UUID tracking

**Expected Control Plane Behavior:**
- Manifest created with all required fields
- Parts stored for each host/shard
- Status transitions tracked

**Validation Steps:**
1. Initiate fusion rebalance
2. Verify manifest creation
3. Verify parts storage
4. Track status transitions

**Edge Cases:**
- Manifest creation failure
- Parts storage failure
- Manifest invalidation

**Related Source Files:**
- TAF: `fusion_volume.py`, `fusion_cp_resource_monitor.py`
- couchbase-cloud: `manifest/manifest.go`, `manifest/service.go`

---

### J.2 Checkpoint Management

**Test Description:** Validate checkpoint management during fusion operations.

**AWS Resources to Validate:**
- Accelerator node checkpoints
- Volume operation checkpoints
- Tear down checkpoints

**Expected Control Plane Behavior:**
- Checkpoints created for each operation phase
- Checkpoints used for recovery
- Clean checkpoint cleanup

**Validation Steps:**
1. Monitor checkpoint creation
2. Verify checkpoint storage
3. Test recovery from checkpoint
4. Verify checkpoint cleanup

**Edge Cases:**
- Checkpoint corruption
- Missing checkpoints
- Multiple concurrent checkpoints

**Related Source Files:**
- TAF: Not directly covered
- couchbase-cloud: `nodes/checkpointer.go`, `nodes/checkpoints.go`

---

### J.3 Networking (VPC Endpoints, Security Groups)

**Test Description:** Validate networking setup for fusion accelerators.

**AWS Resources to Validate:**
- VPC endpoints for S3 access
- Security groups for accelerator instances
- Network ACLs

**Expected Control Plane Behavior:**
- S3 VPC endpoint created
- Security groups configured correctly
- Network isolation maintained

**Validation Steps:**
1. Verify VPC endpoint creation
2. Verify security group rules
3. Test network connectivity
4. Verify isolation

**Edge Cases:**
- Shared VPC scenarios
- Pre-existing endpoints
- Security group changes

**Related Source Files:**
- TAF: Not directly covered
- couchbase-cloud: `infra/aws/networking_vpc_endpoints.go`, `networking_security_groups.go`

---

### J.4 ASG Cleanup Verification

**Test Description:** Verify Auto Scaling Group cleanup after operations.

**AWS Resources to Validate:**
- ASG deletion
- Launch template deletion
- Instance termination

**Expected Control Plane Behavior:**
- ASGs deleted after rebalance
- Launch templates deleted
- No orphaned resources

**Validation Steps:**
1. Complete fusion operation
2. Verify ASG deletion
3. Verify launch template deletion
4. Verify no orphaned instances

**Edge Cases:**
- ASG deletion failure
- Instance protection scenarios
- Partial cleanup

**Related Source Files:**
- TAF: `fusion_volume.py:check_asg_cleanup_after_rebalance()`, `fusion_aws_util.py:list_cluster_fusion_asg()`
- couchbase-cloud: `infra/aws/destroyer.go:compute()`, `infra/aws/autoscalinggroups.go`

---

### J.5 Invalid Rebalance Plan Handling

**Test Description:** Validate handling of invalid rebalance plans.

**AWS Resources to Validate:**
- Manifest invalidation
- Resource cleanup
- Error reporting

**Expected Control Plane Behavior:**
- Invalid plans detected
- Manifests invalidated
- Resources cleaned up

**Validation Steps:**
1. Induce invalid plan scenario
2. Verify detection
3. Verify invalidation
4. Verify cleanup

**Edge Cases:**
- Detection timing
- Partial invalidation
- Recovery scenarios

**Related Source Files:**
- TAF: Not directly covered
- couchbase-cloud: `accelerator/accelerator.go:HandleInvalidRebalancePlan()`

---

### J.6 Rate Limiting and Throttling

**Test Description:** Validate rate limiting behavior for AWS API calls.

**AWS Resources to Validate:**
- AWS API rate limit handling
- Retry behavior
- Backoff strategies

**Expected Control Plane Behavior:**
- Rate limiting applied (5 req/s, 50 burst)
- Retries with exponential backoff
- No throttling errors

**Validation Steps:**
1. Monitor API call rate
2. Verify rate limiting applied
3. Test burst capacity
4. Verify retry behavior

**Edge Cases:**
- Sustained high rate
- Throttling events
- Rate limit changes

**Related Source Files:**
- TAF: Not directly covered
- couchbase-cloud: `infra/limiter.go`, `accelerator/accelerator.go:limiterRefillRatePerSecond`, `limiterBurstTokens`

---

## K. Performance and Scale Tests

### K.1 Large Data Volume Scaling

**Test Description:** Validate fusion with large data volumes (>1TB).

**AWS Resources to Validate:**
- Accelerator instance count
- Guest volume count
- Operation duration

**Expected Control Plane Behavior:**
- Multiple accelerators deployed
- Parallel data transfer
- Linear scaling behavior

**Validation Steps:**
1. Load large dataset
2. Initiate scaling
3. Monitor accelerator count
4. Measure duration

**Edge Cases:**
- Maximum accelerator limit
- Storage limits
- Duration constraints

**Related Source Files:**
- TAF: `fusion_sanity.py:test_fusion_rebalance_at_scale()`
- couchbase-cloud: `accelerator/accelerator.go:splitManifest()`, `packShards()`

---

### K.2 High Concurrency Rebalance

**Test Description:** Validate fusion with high concurrent operations.

**AWS Resources to Validate:**
- Concurrent accelerator instances
- Concurrent volume operations
- Cluster performance

**Expected Control Plane Behavior:**
- Parallel operations succeed
- No resource contention
- Performance maintained

**Validation Steps:**
1. Configure high concurrency
2. Initiate operations
3. Monitor resource usage
4. Verify success

**Edge Cases:**
- Resource exhaustion
- Deadlocks
- Timeout scenarios

**Related Source Files:**
- TAF: `fusion_volume.py:test_volume_scaling()`
- couchbase-cloud: `accelerator/accelerator.go:distributeAccelerators()`

---

### K.3 Rebalance Report Analysis

**Test Description:** Analyze rebalance reports for performance metrics.

**AWS Resources to Validate:**
- NS Server rebalance report
- Timing breakdown by stage
- Fusion mount vs DCP backfill

**Expected Control Plane Behavior:**
- Report available after rebalance
- Fusion mount time tracked
- Cache transfer percentage calculated

**Validation Steps:**
1. Complete rebalance
2. Fetch rebalance report
3. Parse timing breakdown
4. Calculate performance metrics

**Edge Cases:**
- Missing report
- Incomplete data
- Report parsing errors

**Related Source Files:**
- TAF: `fusion_monitor_util.py:log_rebalance_report()`, `_fetch_rebalance_report()`, `_analyze_rebalance_report()`
- couchbase-cloud: Not directly covered (NS Server)

---

## L. Additional Edge Case Tests

### L.1 Parallel Rebalance Validation

**Test Description:** Validate parallel rebalance behavior with slot management.

**AWS Resources to Validate:**
- Slot assignment per host
- Concurrent manifest handling
- Error handling for max slots

**Expected Control Plane Behavior:**
- Parallel rebalances allowed after initial rebalance
- Slots managed per host
- ErrMaximumVolumeSlotsUsed when exhausted

**Validation Steps:**
1. Start initial rebalance
2. Attempt parallel rebalance
3. Verify slot validation
4. Test max slot scenario

**Edge Cases:**
- Slot conflicts
- Partial parallel rebalance
- Recovery from slot exhaustion

**Related Source Files:**
- TAF: Not directly covered
- couchbase-cloud: `accelerator/accelerator.go:ValidateParallelRebalance()`, `findAvailableShardSlots()`

---

### L.2 Manifest Rebalancing During Background Migration

**Test Description:** Validate behavior when operations occur during background migration.

**AWS Resources to Validate:**
- Manifest status transitions
- Guest volume lifecycle
- Accelerator node behavior

**Expected Control Plane Behavior:**
- Existing accelerators maintained
- New accelerators skipped during background migration
- Guest volumes managed correctly

**Validation Steps:**
1. Start rebalance
2. Induce background migration state
3. Attempt new operation
4. Verify behavior

**Edge Cases:**
- Operation during guest volume transfer
- Multiple concurrent background migrations
- State inconsistency

**Related Source Files:**
- TAF: `fusion_cp_resource_monitor.py`
- couchbase-cloud: `accelerator/accelerator.go:accelerateShard()` (manifestStatus check)

---

### L.3 Tenant/Cluster Destruction During Rebalance

**Test Description:** Validate behavior when cluster is destroyed during active rebalance.

**AWS Resources to Validate:**
- Force tear down behavior
- Resource cleanup
- Manifest handling

**Expected Control Plane Behavior:**
- Force tear down allowed
- Resources cleaned up
- No hanging resources

**Validation Steps:**
1. Start rebalance
2. Trigger cluster destruction
3. Verify force tear down
4. Verify cleanup

**Edge Cases:**
- Partial destruction
- Credentials unavailable
- Resource orphans

**Related Source Files:**
- TAF: Not directly covered
- couchbase-cloud: `accelerator/accelerator.go:TearDown()`, `Purge()`

---

## Implementation Priority

### Priority 1 - Critical Path (Implement First)
1. A.1 - Accelerator Creation During Rebalance
2. A.3 - Accelerator Termination After Rebalance
3. B.1 - Volume Creation and Attachment Timing
4. B.3 - Volume Cleanup Verification
5. C.1 - Add Nodes with Fusion Enabled
6. G.1 - Graceful Rebalance with Fusion
7. H.1 - Item Count Validation After Rebalance
8. I.1 - Core Dump Detection

### Priority 2 - Core Functionality
1. A.4 - Accelerator Fallback Scenarios
2. B.2 - Volume Hydration Monitoring
3. C.2 - Remove Nodes with Fusion Enabled
4. D.1 - Disk Scaling with Fusion
5. E.1 - Log Store URI Discovery
6. J.1 - Manifest Creation and Validation
7. J.4 - ASG Cleanup Verification

### Priority 3 - Edge Cases and Resilience
1. F.1 - Accelerator Instance Failure
2. F.2 - EBS Volume Attachment Failure
3. J.5 - Invalid Rebalance Plan Handling
4. J.6 - Rate Limiting and Throttling
5. L.1 - Parallel Rebalance Validation

### Priority 4 - Performance and Scale
1. K.1 - Large Data Volume Scaling
2. K.2 - High Concurrency Rebalance
3. K.3 - Rebalance Report Analysis

---

## Test Utilities Reference

### Existing TAF Utilities

| Utility | File | Key Methods |
|---------|------|-------------|
| FusionAWSUtil | `fusion_aws_util.py` | `list_accelerator_instances()`, `list_instances()`, `scan_logs_for_errors_on_cluster_instances()`, `list_cluster_fusion_asg()` |
| FusionMonitorUtil | `fusion_monitor_util.py` | `wait_for_fusion_status()`, `get_fusion_s3_uri()`, `log_fusion_pending_bytes()`, `get_fusion_uploader_map()`, `run_cbstats_on_all_nodes()`, `log_rebalance_report()` |
| FusionCPResourceMonitor | `fusion_cp_resource_monitor.py` | `monitor_fusion_guest_volumes()`, `check_ebs_guest_vol_deletion()`, `monitor_ebs_cleanup()`, `monitor_fusion_accelerator_nodes_killed_after_rebalance()`, `scan_memcached_logs_for_errors()`, `parse_accelerator_logs()` |
| EC2Lib | `awslib/ec2_lib.py` | `list_instances()`, `run_shell_command()`, `list_volumes_by_cluster_id()`, `get_ebs_volume_by_id()` |
| S3Lib | `awslib/s3_lib.py` | `list_files_in_bucket()`, `get_bucket_size()`, `delete_file()` |
| FISLib | `awslib/fis_lib.py` | `create_compute_failure_experiment()`, `start_experiment()`, `wait_for_experiment_completion()` |
| CloudTrailSetup | `awslib/cloudtrail_delete_setup.py` | `setup_cloudtrail_delete_obj_s3_logging()`, `teardown_cloudtrail_delete_logging()` |

### Required New Utilities

| Utility | Purpose | Required Methods |
|---------|---------|------------------|
| FusionManifestUtil | Manifest tracking | `get_manifest()`, `track_status_transitions()`, `verify_parts()` |
| FusionCheckpointUtil | Checkpoint management | `get_checkpoint()`, `verify_recovery()`, `track_operations()` |
| FusionNetworkingUtil | Networking validation | `verify_vpc_endpoint()`, `verify_security_groups()`, `test_connectivity()` |
| FusionScaleTestHelper | Scale test utilities | `measure_duration()`, `track_resource_usage()`, `calculate_metrics()` |

---

## Configuration Parameters

### Required Test Parameters

```ini
[DEFAULT]
aws_access_key = <AWS_ACCESS_KEY_ID>
aws_secret_key = <AWS_SECRET_ACCESS_KEY>
region = us-east-1

[FusionVolume]
h_scaling = True
v_scaling = True
vh_scaling = True
iterations = 10
steady_state_workload_sleep = 1800
wait_for_hydration_complete = True
hydration_time = 6400

[FaultInjection]
fault_type = instance_stop,instance_terminate,volume_detach,network_partition
fault_duration = 300
recovery_timeout = 600

[Performance]
data_size_gb = 1024
concurrent_operations = 10
measure_latency = True
```

---

## Success Criteria

### Test Pass Criteria
1. All assertions pass without exceptions
2. No CRITICAL errors in memcached logs
3. No core dumps on cluster instances
4. All accelerator instances cleaned up after operations
5. All EBS guest volumes cleaned up after operations
6. Item counts match expected values
7. Fusion status remains "enabled" during operations
8. No orphaned AWS resources

### Performance Criteria
1. Accelerator creation time < 5 minutes per instance
2. Volume attachment time < 2 minutes
3. Hydration completion within timeout
4. Rebalance duration improvement measurable
5. No throttling errors during normal operations

---

## References

### TAF Documentation
- `pytests/aGoodDoctor/fusion/architecture.md` - Architecture documentation
- `pytests/aGoodDoctor/fusion/README.md` - Quick start guide
- `AGENTS.md` - Root TAF coding guidelines

### Couchbase-Cloud Source
- `internal/clusters/fusion/accelerator/accelerator.go` - Accelerator orchestration
- `internal/clusters/fusion/infra/aws/` - AWS infrastructure
- `internal/clusters/fusion/manifest/` - Manifest management
- `internal/clusters/fusion/nodes/` - Node management
- `internal/clusters/fusion/rebalance/` - Rebalance types

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-04-09 | Droid | Initial exhaustive test plan creation |

---

*This test plan should be reviewed and updated as the fusion implementation evolves. Priority implementation should follow the defined Priority 1-4 order.*
