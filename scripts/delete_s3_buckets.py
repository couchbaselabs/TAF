import argparse
import boto3
from datetime import datetime
import re
import time


def empty_bucket(s3_resource, bucket_name):
    """
    Deletes all the objects in the bucket.
    :param bucket_name: Bucket whose objects are to be deleted.
    """
    try:
        # Checking whether versioning is enabled on the bucket or not
        response = s3_resource.BucketVersioning(bucket_name).status
        if not response:
            versioning = True
        else:
            versioning = False

        # Create a bucket resource object
        bucket_resource = s3_resource.Bucket(bucket_name)

        # Empty the bucket before deleting it.
        # If versioning is enabled delete all versions of all the objects,
        # otherwise delete all the objects.
        if versioning:
            response = bucket_resource.object_versions.all().delete()
        else:
            response = bucket_resource.objects.all().delete()
        status = True
        for item in response:
            if item["ResponseMetadata"]["HTTPStatusCode"] != 200:
                status = status and False
        return status
    except Exception as e:
        print(str(e))
        return False


def delete_bucket(s3_resource, bucket_name, max_retry=5):
    """
    Deletes a bucket
    :param bucket_name: Bucket to delete
    :param max_retry
    :param retry_attempt
    """
    try:
        retry_attempt = 0
        while retry_attempt < max_retry:
            if empty_bucket(s3_resource, bucket_name):
                response = s3_resource.Bucket(bucket_name).delete()
                if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                    return True
        return False
    except Exception as e:
        print(str(e))
        return False


def check_bucket_in_exclude_list(bucket_name, exclude_buckets_list):
    if exclude_buckets_list:
        for excluded_bucket in exclude_buckets_list:
            if re.search(excluded_bucket, bucket_name):
                return True
        return False
    else:
        return False


def check_bucket_in_include_list(bucket_name, include_buckets_list):
    if include_buckets_list:
        for included_bucket in include_buckets_list:
            if re.search(included_bucket, bucket_name):
                return True
        return False
    else:
        return False


def check_bucket_older_than_specified_time(bucket_timestamp, older_than):
    if isinstance(bucket_timestamp, datetime):
        datetime_obj = bucket_timestamp
    elif isinstance(bucket_timestamp, str):
        datetime_obj = datetime.fromisoformat(bucket_timestamp)
    epoch_time = int(datetime_obj.timestamp())
    if (int(time.time()) - epoch_time) > older_than:
        return True
    else:
        return False


def get_buckets_to_delete(s3_client, include_buckets, exclude_buckets,
                          older_than):
    try:
        buckets_to_delete = list()
        response = s3_client.list_buckets()
        for bucket_info in response['Buckets']:
            if check_bucket_older_than_specified_time(
                    bucket_info["CreationDate"], older_than) and \
                    check_bucket_in_include_list(
                    bucket_info["Name"], include_buckets) and not \
                    check_bucket_in_exclude_list(
                        bucket_info["Name"], exclude_buckets):
                buckets_to_delete.append(bucket_info["Name"])
        return buckets_to_delete
    except Exception as e:
        print(str(e))
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--access_key", help="access key for aws")
    parser.add_argument(
        "--secret_key", help="secret key for aws")
    parser.add_argument(
        "--include_buckets",
        help="Comma separated list of bucket names or bucket name regex")
    parser.add_argument(
        "--exclude_buckets",
        help="Comma separated list of bucket names or bucket name regex")
    parser.add_argument(
        "--older_than",
        help="Time in seconds. Buckets older than this time will be "
             "deleted. Default value is 604800 seconds",
        default=604800)
    args = parser.parse_args()

    session = boto3.Session(aws_access_key_id=args.access_key,
                            aws_secret_access_key=args.secret_key)
    client = session.client("s3")
    resource = session.resource("s3")
    buckets_to_be_deleted = get_buckets_to_delete(
        client, args.include_buckets.split(","),
        args.exclude_buckets.split(","), int(args.older_than))
    print(f"Following S3 bucket will be deleted - {buckets_to_be_deleted}")
    buckets_failed_to_be_deleted = list()
    for bucket in buckets_to_be_deleted:
        if not delete_bucket(resource, bucket):
            buckets_failed_to_be_deleted.append(bucket)
    if buckets_failed_to_be_deleted:
        print(f"Failed to delete following S3 buckets {buckets_failed_to_be_deleted}")
    else:
        print("All S3 buckets deleted successfully")


if __name__ == "__main__":
    main()
