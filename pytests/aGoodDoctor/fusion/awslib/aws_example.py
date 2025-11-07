"""
Example usage of AWS EC2, S3, and Secrets Manager libraries for TAF
This file demonstrates how to use the EC2Lib, S3Lib, and SecretsManagerLib classes.
"""

import os
from ec2_lib import EC2Lib
from s3_lib import S3Lib
from secrets_manager_lib import SecretsManagerLib


def ec2_example():
    """Example usage of EC2Lib"""
    print("=== EC2 Library Example ===")
    
    # Initialize EC2 client
    ec2 = EC2Lib(
        access_key=os.getenv('AWS_ACCESS_KEY_ID', 'your-access-key'),
        secret_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'your-secret-key'),
        region='us-east-1'
    )
    
    # List all instances
    # print("\n1. Listing all instances:")
    # instances = ec2.list_instances()
    # for instance in instances:
    #     print(f"Instance ID: {instance['InstanceId']}, State: {instance['State']['Name']}")
    
    # Filter instances by tags
    print("\n2. Filtering instances by tags:")
    tagged_instances = ec2.get_instances_by_tags({'couchbase-cloud-cluster-id': 'c8b5accb-edc2-4978-9f47-1981d9343138'})
    for instance in tagged_instances:
        print(f"Instance ID: {instance['InstanceId']}, State: {instance['State']['Name']}")
    
    # Poll instances by tags
    print("\n3. Polling instances by tags:")
    poll_results = ec2.poll_instances_by_tag(
        tags={'couchbase-cloud-cluster-id': 'c8b5accb-edc2-4978-9f47-1981d9343138'},
        poll_interval=10,
        max_polls=5,
        target_state='running'
    )
    
    for instance_id, status in poll_results.items():
        print(f"Instance {instance_id}:")
        print(f"  Current State: {status['current_state']}")
        print(f"  Target State: {status['target_state']}")
        print(f"  Success: {status['success']}")
        print(f"  Public IP: {status.get('public_ip', 'N/A')}")
        print(f"  Poll Count: {status['final_poll_count']}")
        if status.get('error'):
            print(f"  Error: {status['error']}")
        print()
    
    # Poll instances by tags with SSM check
    print("\n4. Polling instances by tags with SSM readiness check:")
    ssm_poll_results = ec2.poll_instances_by_tag_with_ssm(
        tags={'couchbase-cloud-cluster-id': 'c8b5accb-edc2-4978-9f47-1981d9343138'},
        poll_interval=10,
        max_polls=5,
        target_state='running',
        ssm_ready=True
    )
    
    for instance_id, status in ssm_poll_results.items():
        print(f"Instance {instance_id}:")
        print(f"  Current State: {status['current_state']}")
        print(f"  Target State: {status['target_state']}")
        print(f"  Success: {status['success']}")
        print(f"  SSM Ready: {status.get('ssm_ready', 'N/A')}")
        print(f"  SSM Message: {status.get('ssm_ready_message', 'N/A')}")
        print(f"  Public IP: {status.get('public_ip', 'N/A')}")
        print(f"  Poll Count: {status['final_poll_count']}")
        if status.get('error'):
            print(f"  Error: {status['error']}")
        print()
    
    # Get instance details
    if tagged_instances:
        instance_id = tagged_instances[0]['InstanceId']
        print(f"\n5. Getting details for instance {instance_id}:")
        
        # Get instance state
        state = ec2.get_instance_state(instance_id)
        print(f"Current state: {state}")
        
        # Get IP addresses
        public_ip = ec2.get_instance_public_ip(instance_id)
        private_ip = ec2.get_instance_private_ip(instance_id)
        print(f"Public IP: {public_ip}")
        print(f"Private IP: {private_ip}")
        
        # Get instance tags
        tags = ec2.get_instance_tags(instance_id)
        print(f"Tags: {tags}")
        
        # Check if instance is SSM ready
        print(f"\n4. Checking SSM readiness for {instance_id}:")
        ssm_ready = ec2.is_instance_ssm_ready(instance_id)
        print(f"SSM Ready: {ssm_ready}")
        
        if ssm_ready:
            # Run a shell command via SSM
            print(f"\n5. Running shell command on {instance_id} via SSM:")
            result = ec2.run_shell_command(
                instance_id=instance_id,
                command='uname -a'
            )
            print(f"Command result: {result}")
            
            # Run multiple commands via SSM
            print(f"\n6. Running multiple commands on {instance_id} via SSM:")
            commands = ['whoami', 'pwd', 'ls -la']
            results = ec2.run_multiple_commands(
                instance_id=instance_id,
                commands=commands
            )
            for result in results:
                print(f"Command: {result['command']}")
                print(f"Success: {result['success']}")
                print(f"Output: {result['stdout']}")
        else:
            print(f"Instance {instance_id} is not ready for SSM commands.")
            print("Please ensure:")
            print("1. SSM agent is installed on the instance")
            print("2. Instance has proper IAM role with SSM permissions")
            print("3. Instance is in a subnet with internet access or VPC endpoint")


def s3_example():
    """Example usage of S3Lib"""
    print("\n\n=== S3 Library Example ===")
    
    # Initialize S3 client
    s3 = S3Lib(
        access_key=os.getenv('AWS_ACCESS_KEY_ID', 'your-access-key'),
        secret_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'your-secret-key'),
        region='us-east-1'
    )
    
    # List all buckets
    print("\n1. Listing all buckets:")
    buckets = s3.list_buckets()
    for bucket in buckets:
        print(f"Bucket: {bucket['Name']}, Region: {bucket['Region']}, Created: {bucket['CreationDate']}")
    
    if buckets:
        bucket_name = buckets[0]['Name']
        print(f"\n2. Working with bucket: {bucket_name}")
        
        # List files in bucket
        print(f"\n3. Listing files in {bucket_name}:")
        files = s3.list_files_in_bucket(bucket_name)
        for file_info in files[:5]:  # Show first 5 files
            print(f"File: {file_info['Key']}, Size: {file_info['Size']} bytes")
        
        # Get bucket size
        print(f"\n4. Bucket size information:")
        size_info = s3.get_bucket_size(bucket_name)
        print(f"Total files: {size_info['file_count']}")
        print(f"Total size: {size_info['total_size_mb']} MB ({size_info['total_size_gb']} GB)")
        print(f"Size by storage class: {size_info['size_by_storage_class']}")
        
        # Search for specific files
        print(f"\n5. Searching for files containing 'log':")
        log_files = s3.search_files_by_name(bucket_name, 'log')
        for file_info in log_files[:3]:  # Show first 3 matches
            print(f"Found: {file_info['Key']}")

        # List large files
        print(f"\n6. Large files (>10MB):")
        large_files = s3.list_large_files(bucket_name, size_threshold_mb=10)
        for file_info in large_files[:3]:  # Show first 3 large files
            print(f"Large file: {file_info['Key']}, Size: {file_info['Size']} bytes")
        
        # Get bucket summary
        print(f"\n7. Bucket summary:")
        summary = s3.get_bucket_summary(bucket_name)
        print(f"Total files: {summary['total_files']}")
        print(f"Total size: {summary['total_size_gb']} GB")
        print(f"Recent files (last 7 days): {summary['recent_files_count']}")
        if summary['largest_file']:
            print(f"Largest file: {summary['largest_file']['Key']} ({summary['largest_file']['Size']} bytes)")
        
        # Example of file operations (commented out to avoid accidental deletion)
        print(f"\n8. File operations (examples - commented out for safety):")
        print("# Delete a specific file:")
        print(f"# s3.delete_file('{bucket_name}', 'path/to/file.txt')")
        print("# Delete multiple files:")
        print(f"# s3.delete_multiple_files('{bucket_name}', ['file1.txt', 'file2.txt'])")
        print("# Delete files by prefix:")
        print(f"# s3.delete_files_by_prefix('{bucket_name}', 'temp/')")


def secrets_manager_example():
    """Example usage of SecretsManagerLib"""
    print("\n\n=== Secrets Manager Library Example ===")
    
    # Initialize Secrets Manager client
    secrets = SecretsManagerLib(
        access_key=os.getenv('AWS_ACCESS_KEY_ID', 'your-access-key'),
        secret_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'your-secret-key'),
        region='us-east-1'
    )
    
    # # List all secrets
    # print("\n1. Listing all secrets:")
    # all_secrets = secrets.list_secrets()
    # for secret in all_secrets[:5]:  # Show first 5 secrets
    #     print(f"Secret: {secret['Name']}")
    #     print(f"  Description: {secret['Description']}")
    #     print(f"  Created: {secret['CreatedDate']}")
    #     print(f"  Tags: {secret['Tags']}")
    #     print()
    
    # # Filter secrets by tags
    # print("\n2. Filtering secrets by tags:")
    # tagged_secrets = secrets.get_secrets_by_tags({'couchbase-cloud-cluster-id': 'c8b5accb-edc2-4978-9f47-1981d9343138'})
    # for secret in tagged_secrets:
    #     print(f"Secret: {secret['Name']}")
    #     print(f"  Tags: {secret['Tags']}")
    #     print()
    
    # # Get secret value
    # if all_secrets:
    #     secret_name = all_secrets[0]['Name']
    #     print(f"\n3. Getting value for secret: {secret_name}")
    #     secret_value = secrets.get_secret_value(secret_name)
    #     if secret_value['success']:
    #         print(f"Secret value: {secret_value['secret_value']}")
    #         if secret_value['secret_json']:
    #             print(f"Secret JSON: {secret_value['secret_json']}")
    #     else:
    #         print(f"Error: {secret_value.get('error', 'Unknown error')}")
    
    # Get secret values by tags
    # print(f"\n4. Getting secret by tag:")
    # secrets_list = secrets.get_secret_by_tag(tag_key='couchbase-cloud-cluster-id', tag_value='c8b5accb-edc2-4978-9f47-1981d9343138')
    # for secret in secrets_list:
    #     print(f"Secret: {secret['Name']}")
    #     print(f"Secret value: {secret['SecretValue']}")
    #     print(f"Secret tags: {secret['tags']}")
    #     print(f"Secret ARN: {secret['arn']}")
    #     print(f"Secret created date: {secret['created_date']}")
    #     print(f"Secret last changed date: {secret['last_changed_date']}")
    #     print(f"Secret last accessed date: {secret['last_accessed_date']}")
    #     print(f"Secret version id: {secret['version_id']}")
    #     print(f"Secret version stages: {secret['version_stages']}")
    #     print(f"Secret deleted date: {secret['deleted_date']}")
    #     print(f"Secret KMS key ID: {secret['kms_key_id']}")
    #     print(f"Secret success: {secret['success']}")
    #     print(f"Secret error: {secret['error']}")
    #     print()
    
    # get secret by name
    print(f"\n5. Getting secret by name:")
    secret = secrets.get_secret_by_name(secret_name='c8b5accb-edc2-4978-9f47-1981d9343138_dp-admin')
    print(f"Secret Name: {secret['Name']}")
    print(f"Secret Value: {secret['SecretValue']}")


def main():
    """Main function to run examples"""
    print("AWS Fusion Libraries Example")
    print("=" * 50)
    
    # Check if AWS credentials are available
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        print("Warning: AWS credentials not found in environment variables.")
        print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("or update the example code with your credentials.")
        return
    
    try:
        # Run EC2 example
        # ec2_example()
        
        # # Run S3 example
        # s3_example()
        
        # Run Secrets Manager example
        secrets_manager_example()
        
    except Exception as e:
        print(f"Error running examples: {e}")


if __name__ == "__main__":
    main()
