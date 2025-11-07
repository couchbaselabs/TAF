# AWS FIS Library for Fusion Accelerator Fallback Testing

## Overview

This library provides comprehensive functionality to test fusion accelerator compute node fallback logic using AWS Fault Injection Simulator (FIS). It enables fault injection experiments to validate that fusion accelerator nodes properly fallback to alternative compute nodes when their primary targets become unavailable.

## Key Features

- **Compute Node Failure Simulation**: Gracefully stop instances to test fallback behavior
- **Architecture-Aware Testing**: Separate ARM and x86 instance handling
- **Sequential Failure Testing**: Test multiple instance failures in sequence
- **Automatic Recovery**: Instance start/stop with state monitoring
- **Comprehensive Logging**: Detailed experiment status and result reporting
- **Template Management**: Create, list, and delete FIS experiment templates

## Architecture Testing Requirements

The fusion accelerator system uses the following fallback logic:

1. **Architecture Selection**: First chooses between ARM or x86 architecture
2. **Compute Node List**: From the selected architecture, chooses from a list of available compute nodes
3. **Fallback Mechanism**: If a compute node isn't available, falls back to the next in the list

This testing library validates this fallback mechanism by simulating compute node unavailability.

## Installation

The FIS library is part of the fusion AWS libraries package and requires no additional installation.

### Prerequisites

- Python 3.7+
- boto3 library
- AWS credentials with appropriate permissions
- IAM role: `AWSServiceRoleForFIS` or equivalent

### Required AWS Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "fis:CreateExperimentTemplate",
                "fis:StartExperiment",
                "fis:GetExperiment",
                "fis:ListExperimentTemplates",
                "fis:DeleteExperimentTemplate",
                "fis:UpdateExperimentTemplate"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInstances",
                "ec2:DescribeInstanceStatus",
                "ec2:StopInstances",
                "ec2:StartInstances",
                "ec2:DescribeInstanceAttribute"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "sts:GetCallerIdentity",
            "Resource": "*"
        }
    ]
}
```

## Configuration

### Environment Setup

```bash
# Set AWS credentials
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

### Initialize FIS Library

```python
from pytests.aGoodDoctor.fusion.awslib.fis_lib import FISLib

# Initialize with credentials
fis = FISLib(
    access_key='your-access-key',
    secret_key='your-secret-key',
    region='us-east-1'
)
```

## Usage Examples

### 1. Basic Compute Failure Experiment

Create a simple experiment to stop instances and test fallback:

```python
# Create experiment template
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

print(f"Experiment completed with status: {final_status['status']}")
```

### 2. Architecture-Specific Testing

Test ARM vs x86 fallback separately:

```python
# Get all cluster instances
instances = ec2.list_instances(
    filters=[{'Name': 'tag:couchbase-cloud-cluster-id', 'Values': ['cluster-123']}]
)
all_instances = [inst['InstanceId'] for inst in instances]

# Filter ARM instances
arm_instances = fis.filter_instances_by_architecture(
    instance_ids=all_instances,
    architecture='arm64'
)

# Create ARM-specific experiment
template_id = fis.create_compute_failure_experiment(
    experiment_name='fusion-arm-fallback-test',
    instance_ids=arm_instances,
    stop_duration=120,
    tags={'Architecture': 'arm64', 'Purpose': 'testing'}
)
```

### 3. Multi-Architecture Fallback Test

Test both ARM and x86 fallback simultaneously:

```python
# Create experiments for both architectures
template_ids = fis.create_architecture_fallback_test(
    experiment_name_base='fusion-multi-arch-test',
    all_instance_ids=all_instances,
    stop_duration=90
)

# Results: {'arm64': 'ext-arm-id', 'x86_64': 'ext-x86-id'}
for arch, template_id in template_ids.items():
    experiment_id = fis.start_experiment(
        template_id=template_id,
        experiment_name=f'multi-arch-{arch}-test'
    )
    # Monitor experiment...
```

### 4. Sequential Instance Termination

Test fallback behavior with sequential failures:

```python
# Stop instances one by one
sequence_count = 3
instances_to_test = all_instances[:sequence_count]

for i, instance_id in enumerate(instances_to_test):
    # Stop current instance
    fis.stop_instances([instance_id])
    
    # Wait for stop
    fis.wait_for_instance_state(
        instance_ids=[instance_id],
        desired_state='stopped',
        timeout=300
    )
    
    # Wait for fallback (60 seconds)
    time.sleep(60)
    
    # Start instance
    fis.start_instances([instance_id])
    
    # Wait for start
    fis.wait_for_instance_state(
        instance_ids=[instance_id],
        desired_state='running',
        timeout=300
    )
```

### 5. Complete Test Suite

Use the comprehensive test harness:

```bash
python pytests/aGoodDoctor/fusion/test_fis_fallback.py \
    --access_key $AWS_ACCESS_KEY_ID \
    --secret_key $AWS_SECRET_ACCESS_KEY \
    --cluster_id 02a741eb-e978-42fb-86bb-25e097421b84 \
    --test all \
    --stop_duration 60 \
    --cleanup
```

## API Reference

### FISLib Class

#### `__init__(access_key, secret_key, session_token=None, region=None, endpoint_url=None)`

Initialize FIS client with AWS credentials.

**Parameters:**
- `access_key` (str): AWS access key
- `secret_key` (str): AWS secret key
- `session_token` (str, optional): AWS session token
- `region` (str, optional): AWS region (default: 'us-east-1')
- `endpoint_url` (str, optional): Custom endpoint URL

#### `create_compute_failure_experiment(experiment_name, instance_ids, stop_duration=60, stop_before_creating=True, tags=None)`

Create an FIS experiment template for compute node termination.

**Parameters:**
- `experiment_name` (str): Name for the experiment template
- `instance_ids` (List[str]): List of instance IDs to target
- `stop_duration` (int): Duration in seconds to keep instances stopped (default: 60)
- `stop_before_creating` (bool): Stop instances before creating experiment (default: True)
- `tags` (Dict[str, str], optional): Tags for the experiment template

**Returns:** `str` - Experiment template ID

#### `start_experiment(template_id, experiment_name=None, tags=None)`

Start an FIS experiment from a template.

**Parameters:**
- `template_id` (str): ID of the experiment template
- `experiment_name` (str, optional): Name for the experiment instance
- `tags` (Dict[str, str], optional): Tags for the experiment

**Returns:** `str` - Experiment ID

#### `get_experiment_status(experiment_id)`

Get the status of an FIS experiment.

**Parameters:**
- `experiment_id` (str): ID of the experiment

**Returns:** `Dict[str, Any]` - Experiment status and details

#### `wait_for_experiment_completion(experiment_id, timeout=900, poll_interval=10)`

Wait for an FIS experiment to complete.

**Parameters:**
- `experiment_id` (str): ID of the experiment
- `timeout` (int): Maximum wait time in seconds (default: 900)
- `poll_interval` (int): Time between checks in seconds (default: 10)

**Returns:** `Dict[str, Any]` - Final experiment status

#### `stop_instances(instance_ids, force=False)`

Stop EC2 instances.

**Parameters:**
- `instance_ids` (List[str]): List of instance IDs to stop
- `force` (bool): Whether to force stop (default: False)

**Returns:** `bool` - True if successful

#### `start_instances(instance_ids)`

Start EC2 instances.

**Parameters:**
- `instance_ids` (List[str]): List of instance IDs to start

**Returns:** `bool` - True if successful

#### `wait_for_instance_state(instance_ids, desired_state, timeout=300, poll_interval=5)`

Wait for instances to reach desired state.

**Parameters:**
- `instance_ids` (List[str]): List of instance IDs to monitor
- `desired_state` (str): Desired state ('running', 'stopped', etc.)
- `timeout` (int): Maximum wait time in seconds (default: 300)
- `poll_interval` (int): Time between checks in seconds (default: 5)

**Returns:** `bool` - True if all instances reached desired state

#### `get_instance_architecture(instance_id)`

Get the architecture type of an EC2 instance.

**Parameters:**
- `instance_id` (str): ID of the instance

**Returns:** `str` - Architecture type ('x86_64', 'arm64', etc.)

#### `filter_instances_by_architecture(instance_ids, architecture='arm64')`

Filter instances by architecture type.

**Parameters:**
- `instance_ids` (List[str]): List of instance IDs to filter
- `architecture` (str): Architecture to filter by (default: 'arm64')

**Returns:** `List[str]` - Filtered instance IDs

#### `create_architecture_fallback_test(experiment_name_base, all_instance_ids, stop_duration=60)`

Create separate experiments for each architecture type.

**Parameters:**
- `experiment_name_base` (str): Base name for experiments
- `all_instance_ids` (List[str]): List of all instance IDs
- `stop_duration` (int): Duration in seconds (default: 60)

**Returns:** `Dict[str, str]` - Architecture to template ID mapping

## Test Scenarios

### 1. ARM Architecture Fallback

**Purpose:** Validate ARM compute node fallback
**Steps:**
1. Identify ARM instances in cluster
2. Stop ARM instances
3. Verify fallback to x86 instances
4. Restart ARM instances
5. Verify system recovery

**Expected Behavior:** System should fallback to x86 compute nodes when ARM nodes unavailable.

### 2. x86 Architecture Fallback

**Purpose:** Validate x86 compute node fallback
**Steps:**
1. Identify x86 instances in cluster
2. Stop x86 instances
3. Verify fallback to ARM instances
4. Restart x86 instances
5. Verify system recovery

**Expected Behavior:** System should fallback to ARM compute nodes when x86 nodes unavailable.

### 3. Multi-Architecture Fallback

**Purpose:** Test fallback across both architecture types
**Steps:**
1. Create separate experiments for ARM and x86
2. Run experiments sequentially
3. Monitor system behavior
4. Verify proper fallback at each step

**Expected Behavior:** System should handle losses in both architecture types correctly.

### 4. Sequential Instance Termination

**Purpose:** Test handling of multiple sequential failures
**Steps:**
1. Stop instance #1, wait 60s, restart
2. Stop instance #2, wait 60s, restart
3. Stop instance #3, wait 60s, restart
4. Monitor fallback at each step

**Expected Behavior:** System should gracefully handle sequential failures without overall service disruption.

## Monitoring and Validation

### Experiment Status Monitoring

```python
# Poll experiment status
status = fis.get_experiment_status(experiment_id)

# Status values can be:
# 'pending', 'running', 'completed', 'stopped', 'failed'
print(f"Current status: {status['status']}")

# Check completion
if status['status'] in ['completed', 'stopped', 'failed']:
    print(f"Experiment ended: {status['state']}")
```

### Instance State Monitoring

```python
# Monitor instance states
instances = ['i-1234567890abcdef0', 'i-0987654321fedcba0']

# Wait for all to be running
fis.wait_for_instance_state(
    instance_ids=instances,
    desired_state='running',
    timeout=300
)

# Verify specific architecture
for instance_id in instances:
    arch = fis.get_instance_architecture(instance_id)
    print(f"Instance {instance_id} is {arch}")
```

## Error Handling

### Common Errors and Solutions

#### 1. Insufficient Permissions

**Error:** `AccessDenied: User is not authorized to perform: fis:CreateExperimentTemplate`

**Solution:** Ensure AWS credentials have the required IAM permissions (see Prerequisites section).

#### 2. Invalid Instance State

**Error:** `InvalidInstanceID.NotFound: The instance ID XXX does not exist`

**Solution:** Verify instance IDs are correct and instances exist in the specified region.

#### 3. Timeout Exceeded

**Error:** `TimeoutError: Experiment did not complete within 900s`

**Solution:** Increase timeout parameter or investigate why instances aren't recovering properly.

#### 4. Service Role Missing

**Error:** `AccessDenied: AWSServiceRoleForFIS does not exist`

**Solution:** Create the FIS service-linked role:
```bash
aws iam create-service-linked-role --aws-service-name fis.amazonaws.com
```

## Best Practices

### 1. Test in Isolation

- Run tests in non-production environments first
- Use dedicated test clusters with minimal impact
- Ensure proper test isolation to avoid affecting production

### 2. Gradual Testing

- Start with single instance tests
- Progress to small groups
- Finally test at scale

### 3. Comprehensive Monitoring

- Monitor system logs during tests
- Track fallback behavior with application metrics
- Verify complete recovery after tests

### 4. Cleanup

- Always clean up experiment templates after testing
- Remove test tags from resources
- Verify all instances are running after tests

### 5. Documentation

- Document test results and findings
- Track any issues discovered during testing
- Share learnings with team

## Limitations and Future Work

### Current Limitations

1. **EBS Mount Failures**: Not yet implemented (placeholder methods exist)
2. **Volume Attachment Failures**: Not yet implemented (placeholder methods exist)

### Future Enhancements

- **EBS Mount Failure Simulation**: Simulate EBS mount failures for comprehensive testing
- **Volume Attachment Failures**: Test attachment/retry logic
- **Network Partition Simulation**: Test network-level fallback scenarios
- **Custom Experiment Templates**: Support for more complex experiment scenarios
- **Automated Regression Testing**: Integrate into CI/CD pipelines

## Integration with Existing Fusion Code

The FIS library follows the same architectural patterns as other fusion AWS libraries:

```python
# Consistent import pattern
from pytests.aGoodDoctor.fusion.awslib.fis_lib import FISLib
from pytests.aGoodDoctor.fusion.awslib.ec2_lib import EC2Lib

# Consistent initialization
fis = FISLib(access_key, secret_key, region='us-east-1')
ec2 = EC2Lib(access_key, secret_key, region='us-east-1')

# Consistent error handling
try:
    template_id = fis.create_compute_failure_experiment(...)
except Exception as e:
    logger.error(f"FIS experiment creation failed: {e}")
```

## Troubleshooting

### Instance Not Recovering

**Problem:** Instances remain stopped after experiment completion

**Solution:**
```python
# Manually start instances
fis.start_instances(instance_ids)

# Wait for running state
fis.wait_for_instance_state(instance_ids, 'running', timeout=300)
```

### Experiment Hanging

**Problem:** Experiment stuck in 'running' state

**Solution:**
- Check CloudWatch logs for FIS experiments
- Verify instances can be stopped/started manually
- Check for IAM permission issues

### Architecture Detection Issues

**Problem:** Incorrect architecture detection

**Solution:**
```python
# Verify architecture manually
for instance_id in instance_ids:
    arch = fis.get_instance_architecture(instance_id)
    print(f"{instance_id}: {arch}")
```

## Support and Resources

- **AWS FIS Documentation**: https://docs.aws.amazon.com/fis/
- **FIS Boto3 Documentation**: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/fis.html
- **Fusion Architecture Docs:** See agent.md and README.md in the fusion directory

## Changelog

### Version 1.0.0 (2026-03-16)
- Initial release of FIS library
- Compute failure simulation
- Architecture-aware testing
- Sequential failure testing
- Comprehensive test harness
- Documentation and examples
