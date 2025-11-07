#!/bin/bash

export AWS_ACCESS_KEY_ID="$1"
export AWS_SECRET_ACCESS_KEY="$2"
export AWS_REGION="$3"


# --- CONFIGURATION ---
SOURCE_BUCKET="$4"
TARGET_BUCKET="fusion-accesslogs-ritesh-agarwal"
TRAIL_NAME="fusion-delete-tracking"
LOG_PREFIX="cloudtrail-deletes"

# Get current Account ID and Region automatically
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)

echo "--- CloudTrail Delete Logging Setup ---"
echo "Source Bucket: $SOURCE_BUCKET"
echo "Target Bucket: $TARGET_BUCKET"
echo "Trail Name:    $TRAIL_NAME"
echo "Account ID:    $ACCOUNT_ID"
echo "Region:        $REGION"
echo "---------------------------------------"

# ==============================================================================
# STEP 1: Apply S3 Bucket Policy (Fixes InsufficientS3BucketPolicyException)
# ==============================================================================
echo "[1/4] Applying permission policy to Target Bucket ($TARGET_BUCKET)..."

cat <<EOF > /tmp/cloudtrail_policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AWSCloudTrailAclCheck",
            "Effect": "Allow",
            "Principal": { "Service": "cloudtrail.amazonaws.com" },
            "Action": "s3:GetBucketAcl",
            "Resource": "arn:aws:s3:::$TARGET_BUCKET"
        },
        {
            "Sid": "AWSCloudTrailWrite",
            "Effect": "Allow",
            "Principal": { "Service": "cloudtrail.amazonaws.com" },
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::$TARGET_BUCKET/$LOG_PREFIX/AWSLogs/$ACCOUNT_ID/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}
EOF

# Apply the policy
aws s3api put-bucket-policy --bucket "$TARGET_BUCKET" --policy file:///tmp/cloudtrail_policy.json
if [ $? -eq 0 ]; then
    echo "      > Policy applied successfully."
else
    echo "      > Error applying policy. Check bucket ownership."
    exit 1
fi

# ==============================================================================
# STEP 2: Create the Trail
# ==============================================================================
echo "[2/4] Creating/Updating CloudTrail..."

# Check if trail exists
aws cloudtrail get-trail-status --name "$TRAIL_NAME" >/dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "      > Trail '$TRAIL_NAME' already exists. Skipping creation."
else
    aws cloudtrail create-trail \
        --name "$TRAIL_NAME" \
        --s3-bucket "$TARGET_BUCKET" \
        --s3-key-prefix "$LOG_PREFIX" \
        --include-global-service-events \
        --region "$AWS_REGION"
    echo "      > Trail created."
fi

# ==============================================================================
# STEP 3: Configure "Delete Only" Logging
# ==============================================================================
echo "[3/4] Configuring filters to log ONLY object deletions..."

cat <<EOF > /tmp/delete_selector.json
[
    {
        "Name": "LogS3DeletesOnly",
        "FieldSelectors": [
            { "Field": "eventCategory", "Equals": ["Data"] },
            { "Field": "resources.type", "Equals": ["AWS::S3::Object"] },
            { "Field": "resources.ARN", "StartsWith": ["arn:aws:s3:::$SOURCE_BUCKET/"] },
            { "Field": "eventName", "Equals": ["DeleteObject", "DeleteObjects"] }
        ]
    }
]
EOF

aws cloudtrail put-event-selectors \
    --trail-name "$TRAIL_NAME" \
    --advanced-event-selectors file:///tmp/delete_selector.json \
    --region "$AWS_REGION"

# ==============================================================================
# STEP 4: Start Logging
# ==============================================================================
echo "[4/4] Starting the trail..."
aws cloudtrail start-logging --name "$TRAIL_NAME" --region "$REGION"

echo "---------------------------------------"
echo "Done! Deletions in '$SOURCE_BUCKET' will be logged to:"
echo "s3://$TARGET_BUCKET/$LOG_PREFIX/AWSLogs/$ACCOUNT_ID/CloudTrail/$AWS_REGION/..."
echo "Note: It takes about 5-15 minutes for logs to appear."

# Cleanup
rm /tmp/cloudtrail_policy.json /tmp/delete_selector.json