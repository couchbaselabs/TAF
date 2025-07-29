#!/bin/bash

# Check if arguments are provided
if [ $# -lt 1 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <kvstore_path> [username] [password]"
    echo "Example: $0 /data/bucket-1/magma.0/kvstore-0/ Administrator password"
    echo "         $0 /data/bucket-1/magma.0/kvstore-0/ (will prompt for credentials)"
    exit 1
fi

KVSTORE_PATH="$1"
USERNAME="${2:-}"
PASSWORD="${3:-}"
HOST="127.0.0.1"

# Remove trailing slash if present
KVSTORE_PATH="${KVSTORE_PATH%/}"

# Extract magma path (remove /kvstore-X part)
MAGMA_PATH=$(dirname "$KVSTORE_PATH")

# Extract kvstore number (get the number after kvstore-)
KVSTORE_NUM=$(basename "$KVSTORE_PATH" | sed 's/kvstore-//')

# Extract bucket name from the path
# Path format: /data/bucket-name/magma.0/kvstore-X
BUCKET_NAME=$(basename "$(dirname "$MAGMA_PATH")")

# Validate kvstore number is numeric
if ! [[ "$KVSTORE_NUM" =~ ^[0-9]+$ ]]; then
    echo "Error: Invalid kvstore number extracted: $KVSTORE_NUM"
    exit 1
fi

# Prompt for credentials if not provided
if [ -z "$USERNAME" ]; then
    read -p "Enter username: " USERNAME
fi

if [ -z "$PASSWORD" ]; then
    read -s -p "Enter password: " PASSWORD
    echo
fi

echo "Analyzing: $KVSTORE_PATH"
echo "Bucket: $BUCKET_NAME, VBucket: $KVSTORE_NUM"
echo

# 7.6 and greater
if ! magma_dump_output=$(/opt/couchbase/bin/magma_dump "$MAGMA_PATH" docs --index local --kvstore "$KVSTORE_NUM" 2>&1); then
	# 7.2 and before
	if ! magma_dump_output=$(/opt/couchbase/bin/magma_dump "$MAGMA_PATH" --docs-by-local --kvstore "$KVSTORE_NUM" 2>&1); then
        echo "ERROR: Could not magma_dump local index."
        exit 1
	fi
fi

VB_STATE=$(echo "$magma_dump_output" | grep '_local/vbstate' | /opt/couchbase/lib/python/interp/bin/python3 -c "
import sys, json
try:
    line = sys.stdin.read().strip()
    data = json.loads(line)
    print(data['Value']['state'])
except Exception as e:
    print('ERROR', file=sys.stderr)
    sys.exit(1)
")
if [ "$VB_STATE" = "ERROR" ] || [ -z "$VB_STATE" ]; then
    echo "ERROR: Could not retrieve vbstate. Please verify kvstore path."
    exit 1
fi


CLUSTER_UID=$(/opt/couchbase/bin/curl -s -k -u "$USERNAME:$PASSWORD" "https://$HOST:18091/pools/default/buckets/$BUCKET_NAME/scopes" | /opt/couchbase/lib/python/interp/bin/python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data['uid'])
except Exception as e:
    print('ERROR', file=sys.stderr)
    sys.exit(1)
")

if [ "$CLUSTER_UID" = "ERROR" ] || [ -z "$CLUSTER_UID" ]; then
    echo "ERROR: Could not retrieve cluster UID. Please verify credentials and bucket name."
    exit 1
fi

LOCAL_UID=$(echo "$magma_dump_output" | grep '_local/collections/manifest' | /opt/couchbase/lib/python/interp/bin/python3 -c "
import sys, json
try:
    line = sys.stdin.read().strip()
    data = json.loads(line)
    print(data['Value']['uid'])
except Exception as e:
    print('ERROR', file=sys.stderr)
    sys.exit(1)
")

if [ "$LOCAL_UID" = "ERROR" ] || [ -z "$LOCAL_UID" ]; then
    echo "ERROR: Could not retrieve local UID. Please verify kvstore path."
    exit 1
fi

echo "DEBUG: clusterUID: $CLUSTER_UID, dataUID: $LOCAL_UID"

if [ "$VB_STATE" = "active" ]; then
    # Compare the UIDs to determine consistency
    if [ "$CLUSTER_UID" = "$LOCAL_UID" ]; then
        STATUS="OK"
        exit_code=0
    else
        STATUS="VULNERABLE"
        exit_code=1
    fi
else
    STATUS="OK"
    exit_code=0
    echo "DEBUG: VBucket is not active. Skipping consistency check."
fi

echo
echo "Status: $STATUS"
if [ "$STATUS" = "VULNERABLE" ]; then
    echo "Please contact Couchbase support for assistance with upgrade and swap rebalance operation"
fi

exit $exit_code 
