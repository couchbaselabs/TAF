# AWS Fusion Libraries for TAF

This directory contains comprehensive fusion testing utilities for the TAF (Test Automation Framework) that provide a clean 3-layer architecture for AWS operations, fusion monitoring, and test orchestration.

## Quick Start

### Running Fusion Cloud Tests

```bash
# Basic execution
python testrunner.py -i node.ini -c conf/fusion_volume.conf \
    -p aws_access_key=$AWS_ACCESS_KEY_ID \
    -p aws_secret_key=$AWS_SECRET_ACCESS_KEY \
    -p region=us-east-1

# With specific parameters
python testrunner.py -i node.ini -c conf/fusion_volume.conf \
    -p h_scaling=True \
    -p iterations=3 \
    -p steady_state_workload_sleep=1800
```

### AWS Credentials Setup

```bash
# Export credentials (recommended)
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

## Files

### AWS Libraries (Layer 1)
- `awslib/ec2_lib.py` - EC2 library with instance management, SSM, and shell command execution
- `awslib/s3_lib.py` - S3 library with bucket and file operations
- `awslib/secrets_manager_lib.py` - Secrets Manager library with secret filtering and value retrieval
- `awslib/cloudtrail_delete_setup.py` - CloudTrail logging setup for S3 object deletion tracking
- `awslib/fis_lib.py` - AWS Fault Injection Simulator for fusion accelerator fallback testing (NEW!)
- `awslib/aws_example.py` - Example usage of all AWS libraries
- `awslib/__init__.py` - Package initialization file

### Business Logic Managers (Layer 2)
- `fusion_aws_util.py` - AWS orchestration facade that wraps EC2, S3, and Secrets Manager
- `fusion_monitor_util.py` - Core fusion stats monitoring via cbstats and REST API (NEW!)
- `fusion_cp_resource_monitor.py` - AWS control plane resource monitoring (NEW!)
- `fusion_log_collector.py` - Failure-time log collection (`FusionLogCollector`: cbcollect via internal-support API, dp-agent journals via SSM) plus the `FusionFailureLogsMixin` hook used by the fusion base classes

### Test Orchestration (Layer 3)
- `fusion_volume.py` - Main test class for fusion volume scaling operations (REFACTORED)

### Documentation
- **[architecture.md](architecture.md)** - Comprehensive fusion cloud testing architecture (canonical reference)
- `FUSION-Agent.md` - Code quality guidelines and regression prevention strategies
- `FIS-LIB-README.md` - Detailed FIS library documentation for fallback testing

## Architecture

The fusion codebase uses a clean 3-layer architecture with clear separation of concerns:

- **Layer 1: AWS Libraries (awslib/)** - Low-level AWS client wrappers with clean abstractions
- **Layer 2: Business Logic Utilities** - High-level orchestration and monitoring utilities that wrap AWS libraries with fusion-specific logic
- **Layer 3: Test Orchestration** - Main test classes that coordinate fusion operations using business logic utilities

For detailed architecture information including:
- Layer responsibilities and class diagrams
- Runtime flows (initialization, execution, teardown)
- Threading and event coordination patterns
- Key invariants and assertions
- Extensibility guidelines

Please see the [comprehensive architecture documentation](architecture.md).

## Log Collection on Failure

`_FusionTestBase`, `VolumeTest` and `FusionBackupRestoreBase` include
`FusionFailureLogsMixin` (`fusion_log_collector.py`). When a test fails or
errors, the logs are collected before any `tearDown` runs, so before
scale-in, bucket deletes or cluster destroy. A collection error is logged as a
warning and never replaces the test's own failure. The logs go to
`<logs_folder>/fusion_failure_logs/<cluster_id>/`:

- `collection_summary.log`: the table of what was collected, including the
  per-node cbcollect S3 URLs (CP uploads them to cb-engineering)
- `dp-agent/node_<instance>_dp-agent.log`: the output of
  `journalctl --since "N days ago" -u dp-agent`, which is exactly what Fleet
  Manager's dp-agent-logs endpoint returns. It is pulled over SSM from every
  Couchbase node. Accelerator instances are not queried.
- `accelerator-s3/<plan_uuid>/<node_id>/logs_<shard>/`: the logs accelerators
  upload to `cbc-storage-<cluster>/logs/fusion/<plan_uuid>/`, only for the
  rebalance plans created during the test (plans already present after setUp
  are skipped).
- `accelerator-live/<plan_uuid>/accelerator_<instance>_bundle.log`: pulled over
  SSM from every accelerator still running (plan read from its EC2 user data),
  with the same content the accelerator uploads to S3: stats.json, manifest,
  disk stats, dmesg and the full journal. This covers plans with no S3 logs yet.

Manual collection, outside a test run:

```
python pytests/aGoodDoctor/fusion/collect_fusion_logs.py \
    --cluster-id <cluster_id> --plan-uuid <plan_uuid> --account-id <aws_account_id>
```

`--plan-uuid` is repeatable. Without it, plans with accelerator logs from the
last `--since-hours` (6) are downloaded and every running accelerator is
collected. AWS access is resolved like the tests (`-i <ini>` for its
[capella] account_id, `--account-id`, `--role-arn`, or `--aws-access-key`/
`--aws-secret-key`). The region is detected from the `cbc-storage-<cluster>`
bucket unless `--region` is given. cbcollect is not part of the CLI.

Params: `collect_logs_on_failure` (default True), `force_collect_logs`
(collect even on pass, for verification), `get-cbcollect-info=False` (skip
the cbcollect part only), `failure_logs_dp_agent`, `failure_logs_accelerator_s3`,
`failure_logs_accelerator_live`,
`dp_agent_log_days` (1), `dp_agent_log_units` (`:`-separated, default `dp-agent`),
`failure_logs_s3_bucket` (default `cbc-storage-<cluster>`, `none` = inline SSM only),
`cbcollect_timeout` (1800), `cbcollect_ticket`.

## Code Patterns

The fusion tests follow established patterns for consistency and maintainability:

### Initialization Pattern
```python
def setUp(self):
    # Initialize utilities consistently
    self.fusion_aws_util = FusionAWSUtil(self.aws_access_key, self.aws_secret_key, region=self.aws_region)
    self.fusion_monitor = FusionMonitorUtil(self.log, self.fusion_aws_util)
    self.cp_monitor = FusionCPResourceMonitor(self.log, self.fusion_aws_util)
    self.stop_run_event = threading.Event()
```

### Monitoring Pattern
```python
# Start monitoring with stop event
cleanup_thread = threading.Thread(
    target=self.cp_monitor.check_ebs_guest_vol_deletion,
    kwargs={"tenant": tenant, "cluster": cluster, "stop_run_event": self.stop_run_event}
)
cleanup_thread.start()

# Clean shutdown in tearDown
def tearDown(self):
    self.stop_run_event.set()
    for thread in self.background_threads:
        thread.join()
```

## AWS Library Summary

### EC2Lib
Instance and volume management with tag filtering, SSM command execution, and polling operations. For complete API details, see [architecture.md - Layer 1](architecture.md#layer-1-aws-libraries-awslib).

### S3Lib
Bucket and object operations including listing, deletion, size calculation, and log artifact retrieval. For complete API details, see [architecture.md - AWS Libraries](architecture.md#layer-1-aws-libraries-awslib).

### SecretsManagerLib  
AWS Secrets Manager credential retrieval with pattern-based discovery and JSON secret parsing. For complete API details, see [architecture.md - AWS Libraries](architecture.md#layer-1-aws-libraries-awslib).

### FISLib
AWS Fault Injection Simulator for accelerator fallback testing with architecture-aware instance handling. For complete API details, see [architecture.md - AWS Libraries](architecture.md#layer-1-aws-libraries-awslib).

### Bucket Operations
- **List all buckets** with metadata
- **Get bucket region** information
- **Create/Delete buckets**
- **Get bucket summary** with comprehensive statistics

### File Operations
- **List files in bucket** with optional prefix filtering
- **Get file size** and metadata
- **Search files by name pattern**
- **List files by size range**
- **List large files** above a threshold

### File Management
- **Delete single file**
- **Delete multiple files** in batch
- **Delete files by prefix**
- **Get bucket size** and file count statistics

### Advanced Features
- **Size analysis** by storage class
- **Recent files** identification
- **Largest/smallest file** detection
- **Comprehensive bucket summaries**

## FIS Library Features (NEW!)

The `FISLib` class provides AWS Fault Injection Simulator functionality for fusion accelerator fallback testing.

**Key Features**:
- **Compute Failure Simulation**: Gracefully stop instances to test fallback behavior
- **Architecture-Aware Testing**: Separate ARM and x86 instance handling
- **Sequential Failure Testing**: Test multiple instance failures in sequence
- **Automatic Recovery**: Instance start/stop with state monitoring
- **Comprehensive Logging**: Detailed experiment status and result reporting
- **Template Management**: Create, list, and delete FIS experiment templates

**Usage Example**:
```python
from awslib.fis_lib import FISLib

# Initialize FIS client
fis = FISLib(
    access_key='your-access-key',
    secret_key='your-secret-key',
    region='us-east-1'
)

# Create compute failure experiment
template_id = fis.create_compute_failure_experiment(
    experiment_name='fusion-accelerator-fallback-test',
    instance_ids=['i-1234567890abcdef0', 'i-0987654321fedcba0'],
    stop_duration=60,  # Keep instances stopped for 60 seconds
    tags={'Purpose': 'testing', 'Environment': 'dev'}
)

# Start the experiment
experiment_id = fis.start_experiment(
    template_id=template_id,
    experiment_name='fallback-test-run-1'
)

# Wait for completion
final_status = fis.wait_for_experiment_completion(
    experiment_id=experiment_id,
    timeout=900,  # 15 minutes
    poll_interval=15
)

# Filter instances by architecture
arm_instances = fis.filter_instances_by_architecture(
    instance_ids=all_instances,
    architecture='arm64'
)

# Create architecture-specific experiments
template_ids = fis.create_architecture_fallback_test(
    experiment_name_base='fusion-multi-arch-test',
    all_instance_ids=all_instances,
    stop_duration=90
)
```

**See FIS-LIB-README.md** for comprehensive FIS documentation including test scenarios, monitoring, and troubleshooting.

## Secrets Manager Library Features

The `SecretsManagerLib` class provides the following functionality:

### Secret Discovery
- **List all secrets** with metadata
- **Filter secrets by tags** using key-value pairs
- **Search secrets by name pattern**
- **Get secrets summary** with statistics

### Secret Value Operations
- **Get secret value** by name or ARN
- **Get secret values by tags** for bulk retrieval
- **Parse JSON secrets** automatically
- **Version management** for secret versions

### Secret Management
- **Create secrets** with tags and descriptions
- **Update secret values** and descriptions
- **Delete secrets** with recovery options
- **Tag management** (add/remove tags)

### Advanced Features
- **Secret versioning** support
- **Binary secret** handling
- **Comprehensive error handling**
- **Detailed metadata** for all operations

## Test Orchestration (Layer 3)

### Fusion Volume Test Class (REFACTORED)

The `VolumeTest` class in `fusion_volume.py` provides comprehensive fusion volume scaling tests using the refactored architecture.

**Key Features**:
- **Simplified Setup**: Clean initialization with proper resource management
- **Multi-Threaded Monitoring**: Concurrent monitoring of different fusion aspects
- **Event-Driven Coordination**: Threading events for clean thread lifecycle management
- **Utility Delegation**: Test logic delegates to specialized utility classes
- **Comprehensive Cleanup**: Proper teardown with resource cleanup and thread shutdown
- **Error Handling**: Enhanced error handling with explicit error states

**Architecture Benefits**:
- **Eliminated Duplication**: Monitoring logic moved to utility classes
- **Single Responsibility**: Test class focuses on test orchestration, utilities handle monitoring
- **Reusability**: Utility classes can be used across different test scenarios
- **Maintainability**: Changes to monitoring logic are localized to utility classes

**Key Refactoring Changes**:
- Moved volume monitoring to `FusionCPResourceMonitor.monitor_fusion_guest_volumes()`
- Moved instance monitoring to `FusionCPResourceMonitor.monitor_cluster_accelerator_instances()`
- Moved stats monitoring to `FusionMonitorUtil.log_fusion_pending_bytes()`
- Moved error scanning to `FusionCPResourceMonitor.scan_memcached_logs_for_errors()`
- Added event-driven coordination with `stop_run_event` for background threads

**Test Workflows**:
- **Initial Setup**: Fusion enablement, bucket creation, data loading
- **Horizontal Scaling**: Scale up/down nodes with concurrent monitoring
- **Vertical Scaling**: Scale disk and compute independently and together
- **Validation**: Accelerator cleanup, EBS cleanup, log scanning, log parsing
- **Background Monitoring**: Continuous EBS cleanup and volume availability tracking

## Refactored Architecture Benefits

The 3-layer refactored architecture provides several key benefits:

### 1. Separation of Concerns
- **Layer 1**: Low-level AWS client wrappers with clean abstractions
- **Layer 2**: Business logic with fusion-specific monitoring and orchestration
- **Layer 3**: Test orchestration focusing on test flows and validation

### 2. Code Reusability
- Utility classes can be reused across different test scenarios
- No duplication of monitoring code in test classes
- Consistent monitoring patterns across all fusion tests

### 3. Maintainability
- Changes to AWS operations localized to awslib/
- Changes to monitoring logic localized to utility classes
- Test code remains clean and focused on test flows

### 4. Thread Safety
- Event-driven coordination for clean thread lifecycle
- Background monitoring threads with proper shutdown
- Thread-safe operations using threading.Event

### 5. Enhanced Logging
- PrettyTable-based structured logging throughout all layers
- Consistent log formats and information levels
- Detailed monitoring tables for fusion operations

## Testing

### AWS Libraries Testing

Run the example file to test the AWS libraries:

```bash
cd /Users/ritesh.agarwal/CB/TAF_GF/TAF/pytests/aGoodDoctor/fusion
python awslib/aws_example.py
```

### FIS Library Testing

Run FIS fallback testing:

```bash
cd /Users/ritesh.agarwal/CB/TAF_GF/TAF/pytests/aGoodDoctor/fusion
python test_fis_fallback.py \
    --access_key $AWS_ACCESS_KEY_ID \
    --secret_key $AWS_SECRET_ACCESS_KEY \
    --cluster_id 02a741eb-e978-42fb-86bb-25e097421b84 \
    --test all \
    --stop_duration 60 \
    --cleanup
```

### Fusion Volume Scaling Tests

Run fusion volume scaling tests using the refactored architecture:

```bash
cd /Users/ritesh.agarwal/CB/TAF_GF/TAF
python testrunner.py -i node.ini -c conf/fusion_volume.conf \
    -p aws_access_key=$AWS_ACCESS_KEY_ID,aws_secret_key=$AWS_SECRET_ACCESS_KEY \
    -p region=us-east-1 \
    -p h_scaling=True \
    -p iterations=3
```

Make sure to set your AWS credentials before running any tests.

## Dependencies

The fusion libraries require the following Python packages:

### AWS and External Libraries
- `boto3` - AWS SDK for Python (for all AWS operations)
- `botocore` - Low-level AWS service access
- `paramiko` - SSH client library (for alternative SSH access)

### Standard Library
- `threading` - Thread coordination and background monitoring
- `socket` - Network operations (hostname resolution)
- `ast` - AST parsing (for fusion uploader map parsing)
- `subprocess` - Shell command execution (for log download scripts)
- `os` - File system operations

### TAF Framework Dependencies
- `membase.api.rest_client` - Couchbase REST API client
- `capella_utils.dedicated` - Capella API utilities
- `BucketLib.BucketOperations` - Bucket operations
- `couchbase_utils.cb_server_rest_util.fusion.fusion_api` - Fusion REST API
- `constants.cloud_constants.capella_constants` - Cloud provider constants
- `Jython_tasks.java_loader_tasks` - Document loading tasks

### PrettyTable (for structured logging)
- Used throughout all utility classes for structured logging

These dependencies are already included in the TAF requirements.txt file.


## Support and Contributing

**For fusion-specific questions**: Contact the fusion testing team or refer to internal team documentation.

**Contributing**: When adding new fusion utilities or tests, follow the established 3-layer architecture and patterns documented in [architecture.md](architecture.md).

## Version History

- **v2.0** (2026-03-31): Major refactoring - Added business logic manager layer (FusionMonitorUtil, FusionCPResourceMonitor), integrated FIS library, improved thread coordination patterns, eliminated code duplication
- **v1.0** (2026-03-16): Initial release with AWS libraries (EC2, S3, Secrets Manager) and basic fusion test infrastructure
