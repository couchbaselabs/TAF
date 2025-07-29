#!/bin/bash

MAGMA_DUMP="/opt/couchbase/bin/magma_dump"

if [ $# -eq 0 ]; then
    echo "Error: Data directory path is required"
    echo "Usage: $0 <data_directory_path>"
    exit 1
fi

DATA_DIR="$1"

if [ ! -d "$DATA_DIR" ]; then
    echo "FAILED: Directory '$DATA_DIR' does not exist"
    exit 1
fi

THRESHOLD=2000000000

VULNERABLE_KVSTORES=0
TOTAL_KVSTORES=0
FAILED_KVSTORES=0
GLOBAL_MAX_MAXSN=0

VULNERABLE_KVSTORE_LIST=()
LOCALINDEX_IMPACTED_LIST=()

if ! command -v $MAGMA_DUMP &> /dev/null; then
    echo "FAILED: $MAGMA_DUMP not found"
    exit 1
fi

# Find all magma.* directories
magma_dirs=$(find "$DATA_DIR" -type d -name 'magma.*' 2>/dev/null | sort)


run_magma_dump_treestate() {
    local magma_dir="$1"
    local kvstore_num="$2"

    # 7.6.0
    if magma_raw_output=$($MAGMA_DUMP "$magma_dir/" tree-state --latest --kvstore "$kvstore_num" 2>&1); then
        if echo "$magma_raw_output" | grep -q maxSn; then
            echo "$magma_raw_output"
            return 0
        fi
    fi
    local magma_dump_76_output="$magma_raw_output"

    # < 7.6
    if magma_raw_output=$($MAGMA_DUMP "$magma_dir/" --tree-state-latest --kvstore "$kvstore_num" 2>&1); then
        if echo "$magma_raw_output" | grep -q maxSn; then
            echo "$magma_raw_output"
            return 0
        fi
    fi

    echo "    7.6.0 output: $magma_dump_76_output"
    echo "    Pre-7.6 output: $magma_raw_output"

    return 1
}

for magma_dir in $magma_dirs; do
    echo "Processing: $magma_dir"

    # Find all kvstore-* directories within this magma directory
    kvstore_dirs=$(find "$magma_dir" -maxdepth 1 -type d -name 'kvstore-*' 2>/dev/null | sort -V)

    for kvstore_dir in $kvstore_dirs; do
        kvstore_num=$(basename "$kvstore_dir" | sed 's/kvstore-//')


        TOTAL_KVSTORES=$((TOTAL_KVSTORES + 1))
        magma_raw_output=$(run_magma_dump_treestate "$magma_dir" "$kvstore_num" 2>&1)
        if [ $? -eq 0 ]; then
            highest_maxsn=$(echo "$magma_raw_output" | grep maxSn | tr -d ' ,' | cut -d: -f2 | sort -nr | head -1)

            if [ -n "$highest_maxsn" ]; then
                # Print all relevant information
                echo "DEBUG: kvstore-$kvstore_num:"
                echo "$magma_raw_output" | grep -E 'maxSn|meta|State File' | sed 's/^/    DEBUG: /'

                localindex_maxsn=$(echo "$magma_raw_output" | grep -E 'maxSn|State File' | grep -A1 "localIndex" | grep "maxSn" | sed 's/.*"maxSn": *\([0-9]*\).*/\1/')
                kvstore_path="$magma_dir/kvstore-$kvstore_num"

                # Extract bucket name from magma_dir path
                bucket_name=$(echo "$magma_dir" | sed 's|.*/\([^/]*\)/magma\.[0-9]*$|\1|')
                # Update global maximum
                if [ "$highest_maxsn" -gt "$GLOBAL_MAX_MAXSN" ]; then
                    GLOBAL_MAX_MAXSN=$highest_maxsn
                fi

                if [ "$highest_maxsn" -gt "$THRESHOLD" ]; then
                    echo "  kvstore-$kvstore_num: maxSn=$highest_maxsn (VULNERABLE)"
                    VULNERABLE_KVSTORES=$((VULNERABLE_KVSTORES + 1))

                    # Check if localIndex is specifically impacted
                    if [ -n "$localindex_maxsn" ] && [ "$localindex_maxsn" -gt "$THRESHOLD" ]; then
                        VULNERABLE_KVSTORE_LIST+=("Bucket: $bucket_name, $kvstore_path (maxSn: $highest_maxsn, localIndexMaxSn: $localindex_maxsn)")
                        LOCALINDEX_IMPACTED_LIST+=("Bucket: $bucket_name $kvstore_path")
                    else
                        VULNERABLE_KVSTORE_LIST+=("Bucket: $bucket_name, $kvstore_path (maxSn: $highest_maxsn)")
                    fi
                else
                    echo "  kvstore-$kvstore_num: maxSn=$highest_maxsn (OK)"
                fi
            else
                echo "  kvstore-$kvstore_num: FAILED to extract maxSn"
                FAILED_KVSTORES=$((FAILED_KVSTORES + 1))
            fi
        elif [ -n "$(ls -A "$kvstore_dir")" ]; then
            echo "  kvstore-$kvstore_num: FAILED to run magma_dump"
            echo "    Error: $magma_raw_output"
            FAILED_KVSTORES=$((FAILED_KVSTORES + 1))
        fi
    done
done

echo
echo "========================================"

# Print summary of vulnerable kvstores
if [ ${#VULNERABLE_KVSTORE_LIST[@]} -gt 0 ]; then
    echo "VULNERABLE VBUCKETS SUMMARY:"
    echo "============================"
    for kvstore in "${VULNERABLE_KVSTORE_LIST[@]}"; do
        echo "  $kvstore"
    done
    echo
    
    # Print summary of localIndex impact
    if [ ${#LOCALINDEX_IMPACTED_LIST[@]} -gt 0 ]; then
        echo "METADATA CONSISTENCY CHECK REQUIRED:"
        echo "===================================="
        echo "VBucket metastore may be vulnerable. After performing bucket compaction, run the following commands to check for metadata inconsistency:"
        echo
        for kvstore in "${LOCALINDEX_IMPACTED_LIST[@]}"; do
            # Format is now: "Bucket: bucket_name /path/to/kvstore"
            kvstore_path=$(echo "$kvstore" | awk '{print $NF}')
            echo "  $kvstore"
            echo "  $  ./check_metastore.sh $kvstore_path [username] [password]"
            echo
        done
    fi
fi

STATUS="OK"
exit_code=0

if [ "$FAILED_KVSTORES" -gt 0 ]; then
    STATUS="SCRIPT_FAILED"
    exit_code=1
elif [ "$VULNERABLE_KVSTORES" -gt 0 ]; then
    STATUS="VULNERABLE"
    exit_code=1
fi

echo "========================================"
echo "Status:$STATUS, maxSn:$GLOBAL_MAX_MAXSN, VBucketsImpacted:$VULNERABLE_KVSTORES/$TOTAL_KVSTORES, FailedExecutions:$FAILED_KVSTORES"

exit $exit_code 

