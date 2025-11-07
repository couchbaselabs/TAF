#!/bin/bash
# Script to download fusion logs from S3, unzip them, and display stats
#
# Usage: ./download_accelerator_logs.sh [S3_BUCKET] [LOCAL_DIR] [TIME_FILTER]
#   S3_BUCKET:   S3 bucket name (default: cbc-storage-ea5498)
#   LOCAL_DIR:   Local directory to download to (default: ~/Downloads/fusion_logs)
#   TIME_FILTER: Filter files modified within this time window (e.g., "1h", "30m", "2h")
#                Supports: Nh (hours) or Nm (minutes). Empty = no filter.
#
# Environment variables:
#   REBALANCE_ID: Filter by rebalance ID (UUID in path, e.g., "60435aa6-a135-45c4-bfe4-a6ecf8eacbc9")
#
# Examples:
#   ./download_accelerator_logs.sh                           # All files
#   ./download_accelerator_logs.sh mybucket ~/logs 1h        # Last 1 hour
#   ./download_accelerator_logs.sh mybucket ~/logs 30m       # Last 30 minutes
#   REBALANCE_ID=60435aa6-a135-45c4-bfe4-a6ecf8eacbc9 ./download_accelerator_logs.sh mybucket ~/logs 1h

set -e

# =============================================================================
# Configuration (edit these defaults or pass as arguments)
# =============================================================================
export AWS_ACCESS_KEY_ID="$1"
export AWS_SECRET_ACCESS_KEY="$2"
export AWS_REGION="$3"

S3_BUCKET="${4:-cbc-storage-ea5498}"
# Rebalance ID filter (optional, set via env var)
REBALANCE_ID="${5:-}"
LOCAL_DIR="${6:-./fusion_logs/${REBALANCE_ID}}"
S3_PATH="logs/fusion/${REBALANCE_ID}"
DOWNLOAD_MARKER="${LOCAL_DIR}/.download_complete"
OUTPUT_FILE="${LOCAL_DIR}/fusion_stats_report.txt"
STATS_TEMP="${LOCAL_DIR}/.stats_temp.csv"


echo "=== Fusion Log Downloader ==="
echo "S3 Bucket: ${S3_BUCKET}"
echo "S3 Path:   ${S3_PATH}/"
echo "Local Dir: ${LOCAL_DIR}"
if [ -n "${REBALANCE_ID}" ]; then
    echo "Rebalance ID: ${REBALANCE_ID}"
fi
echo "Output:    ${OUTPUT_FILE}"
echo ""

# Create local directory
mkdir -p "${LOCAL_DIR}"

# Step 1: Download from S3
# Use filtered download when time filter or rebalance ID is set
S3_LIST_FILE="${LOCAL_DIR}/.s3_filtered_list.txt"

# Filters set - list S3, apply filters, download only matching files
echo ">>> Step 1: Listing and filtering S3 files..."

# Build JMESPath query for time filter
# List objects with optional time filter
aws s3api list-objects-v2 \
    --bucket "${S3_BUCKET}" \
    --prefix "${S3_PATH}/" \
    --query "Contents[*].[Key]" \
    --output text > "${S3_LIST_FILE}"

# Apply rebalance ID filter if set
if [ -n "${REBALANCE_ID}" ]; then
    grep "${REBALANCE_ID}" "${S3_LIST_FILE}" > "${S3_LIST_FILE}.tmp" || true
    mv "${S3_LIST_FILE}.tmp" "${S3_LIST_FILE}"
fi

file_count=$(wc -l < "${S3_LIST_FILE}" | tr -d ' ')
filter_desc=""
[ -n "${REBALANCE_ID}" ] && filter_desc="${filter_desc:+$filter_desc, }rebalance ${REBALANCE_ID}"
echo "  Found ${file_count} files matching filters (${filter_desc})"

# Download each file (preserving directory structure, skip existing)
downloaded=0
skipped=0
while IFS= read -r s3_key; do
    [ -z "${s3_key}" ] && continue
    
    # Calculate local path (strip the S3_PATH prefix)
    local_path="${LOCAL_DIR}/${s3_key#${S3_PATH}/}"
    local_dir=$(dirname "${local_path}")
    
    # Skip if file already exists locally
    if [ -f "${local_path}" ]; then
        skipped=$((skipped + 1))
        continue
    fi
    
    # Create directory and download
    mkdir -p "${local_dir}"
    aws s3 cp "s3://${S3_BUCKET}/${s3_key}" "${local_path}" --quiet
    downloaded=$((downloaded + 1))
    
    # Progress indicator every 10 files
    if [ $((downloaded % 10)) -eq 0 ]; then
        echo "  Downloaded ${downloaded} files..."
    fi
done < "${S3_LIST_FILE}"

echo "  Download complete: ${downloaded} new, ${skipped} skipped (already exist)"
rm -f "${S3_LIST_FILE}"

echo ""
echo ">>> Step 2: Unzipping .zip files (idempotent - skips already extracted)..."

# Step 2: Find and unzip all zip files (idempotent - skip if already extracted)
find "${LOCAL_DIR}" -name "*.zip" -type f | while read -r zipfile; do
    unzip_dir="$(dirname "${zipfile}")/$(basename "${zipfile}" .zip)_extracted"
    if [ -d "${unzip_dir}" ]; then
        echo "  Already extracted: $(basename "${zipfile}")"
    else
        echo "  Unzipping: $(basename "${zipfile}")"
        mkdir -p "${unzip_dir}"
        unzip -o -q "${zipfile}" -d "${unzip_dir}" || echo "  Warning: Failed to unzip ${zipfile}"
    fi
done

echo ""
echo ">>> Step 3: Collecting stats for each journal..."
echo ""

# Function to parse syslog timestamp and convert to epoch seconds
# Format: "Jan 16 16:09:14" - assumes year 2026
parse_syslog_time() {
    local timestamp="$1"
    date -j -f "%b %d %H:%M:%S %Y" "${timestamp} 2026" "+%s" 2>/dev/null || echo "0"
}

# Function to format duration in human-readable form
format_duration() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    printf "%02d:%02d:%02d" $hours $minutes $secs
}

# Function to convert bytes to human readable GB
bytes_to_gb() {
    local bytes=$1
    echo "scale=2; ${bytes} / 1073741824" | bc
}

# Clear temp stats file
> "${STATS_TEMP}"

# Step 3: Collect all stats first
# Apply rebalance ID filter if set, otherwise find all journals
JOURNAL_LIST="${LOCAL_DIR}/.journal_list.txt"
find "${LOCAL_DIR}" -name "journal*.gz" -type f | sort -V > "${JOURNAL_LIST}"

while read -r journal_file; do
    # Get directory containing the journal (should have stats.json too)
    journal_dir=$(dirname "${journal_file}")
    stats_file="${journal_dir}/stats.json"
    
    # Get relative path for display
    rel_path=$(echo "${journal_file}" | sed "s|${LOCAL_DIR}/||")
    
    # Decompress journal once and cache content for multiple greps
    journal_content=$(gunzip -c "${journal_file}" 2>/dev/null)
    
    # Extract throughput from journal (e.g., "Throughput: 1.59 GB/s")
    throughput=$(echo "${journal_content}" | grep "Throughput:" | tail -1 | grep -oE '[0-9]+\.[0-9]+ GB/s' || echo "N/A")
    
    # Extract instance type from DMI line (e.g., "DMI: Amazon EC2 c6in.xlarge/, BIOS")
    instance_type=$(echo "${journal_content}" | grep "DMI: Amazon EC2" | tail -1 | grep -oE 'EC2 [^/]+' | sed 's/EC2 //' || echo "N/A")
    
        # Extract download duration from journal (e.g., "Duration: 31.534570069s" or "Duration: 1m33.428s")
        dl_duration_raw=$(echo "${journal_content}" | grep "Duration:" | tail -1 | grep -oE '[0-9]+m[0-9.]+s|[0-9.]+s' || echo "N/A")
        dl_duration="${dl_duration_raw}"
        
        # Convert duration to seconds (handle "1m33.428s" and "33.428s" formats)
        if echo "${dl_duration_raw}" | grep -q 'm'; then
            # Format: 1m33.428s - extract minutes and seconds
            dl_mins=$(echo "${dl_duration_raw}" | grep -oE '^[0-9]+' || echo "0")
            dl_secs=$(echo "${dl_duration_raw}" | grep -oE '[0-9.]+s$' | grep -oE '[0-9.]+' || echo "0")
            dl_duration_num=$(echo "scale=6; ${dl_mins} * 60 + ${dl_secs}" | bc)
        else
            # Format: 33.428s - just seconds
            dl_duration_num=$(echo "${dl_duration_raw}" | grep -oE '[0-9.]+' || echo "0")
        fi
    
    # Extract total files from journal (e.g., "Total files: 93")
    total_files=$(echo "${journal_content}" | grep "Total files:" | tail -1 | grep -oE '[0-9]+$' || echo "0")
    
    # Extract total bytes from journal (e.g., "Total bytes: 53879839266")
    total_bytes=$(echo "${journal_content}" | grep "Total bytes:" | tail -1 | grep -oE '[0-9]+$' || echo "0")
    
    # Extract total retries from journal (e.g., "Total retries: 0")
    retries=$(echo "${journal_content}" | grep "Total retries:" | tail -1 | grep -oE '[0-9]+$' || echo "0")
    
    # Convert bytes to GB
    if [ -n "${total_bytes}" ] && [ "${total_bytes}" != "0" ]; then
        size_gb=$(bytes_to_gb "${total_bytes}")
    else
        size_gb="N/A"
    fi
    
    # Calculate average file size in MB (rounded)
    if [ -n "${total_bytes}" ] && [ "${total_bytes}" != "0" ] && [ -n "${total_files}" ] && [ "${total_files}" != "0" ]; then
        avg_file_mb=$(echo "scale=0; ${total_bytes} / ${total_files} / 1048576" | bc)
    else
        avg_file_mb="0"
    fi
    
    # Calculate runtime: from "Linux version" line to end of journal
    # Get timestamp of line containing "Linux version" (use tail -1 to get the actual boot, not AMI)
    linux_line=$(echo "${journal_content}" | grep "Linux version" | tail -1)
    last_line=$(echo "${journal_content}" | tail -1)
    
    start_ts=$(echo "${linux_line}" | grep -oE '^[A-Z][a-z]{2} [0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2}' || echo "")
    end_ts=$(echo "${last_line}" | grep -oE '^[A-Z][a-z]{2} [0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2}' || echo "")
    
    # Calculate runtime duration
    runtime_secs=0
    if [ -n "${start_ts}" ] && [ -n "${end_ts}" ]; then
        start_epoch=$(parse_syslog_time "${start_ts}")
        end_epoch=$(parse_syslog_time "${end_ts}")
        
        # Handle day rollover (if end < start, add 24 hours)
        if [ "${end_epoch}" -lt "${start_epoch}" ]; then
            end_epoch=$((end_epoch + 86400))
        fi
        
        runtime_secs=$((end_epoch - start_epoch))
        runtime=$(format_duration ${runtime_secs})
    else
        runtime="N/A"
    fi
    
    # Extract GET API calls from stats.json if it exists
    if [ -f "${stats_file}" ]; then
        get_api_calls=$(grep -oE '"GetAPICalls":\s*[0-9]+' "${stats_file}" | grep -oE '[0-9]+' || echo "0")
    else
        get_api_calls="0"
    fi
    
    # Write to temp CSV for summary calculation
    echo "${dl_duration_num},${retries},${avg_file_mb},${get_api_calls},${runtime_secs}" >> "${STATS_TEMP}"
    
    # Print row: Throughput, Size, DL Duration, Runtime, Avg File Size, GET API, Retries, Instance Type, Journal Path
    printf "%-12s | %-10s | %-18s | %-10s | %-12s | %-10s | %-8s | %-14s | %s\n" \
        "${throughput}" "${size_gb}" "${dl_duration}" "${runtime}" "${avg_file_mb}" "${get_api_calls}" "${retries}" "${instance_type}" "${rel_path}"
done < "${JOURNAL_LIST}" > "${LOCAL_DIR}/.report_body.txt"

rm -f "${JOURNAL_LIST}"

# Generate report with summary
{
    echo "================================================================================"
    echo "                        FUSION LOGS STATS REPORT"
    echo "                        Generated: $(date)"
    echo "================================================================================"
    echo ""
    printf "%-12s | %-10s | %-18s | %-10s | %-12s | %-10s | %-8s | %-14s | %s\n" "THROUGHPUT" "SIZE(GB)" "DL DURATION" "RUNTIME" "AVG FILE(MB)" "GET API" "RETRIES" "INSTANCE TYPE" "JOURNAL PATH"
    printf "%s\n" "-------------+------------+--------------------+------------+--------------+------------+----------+----------------+------------------------------------------"
    
    # Print the collected data
    cat "${LOCAL_DIR}/.report_body.txt"
    
    echo ""
    echo "================================================================================"
    echo "                              SUMMARY"
    echo "================================================================================"
    
    # Calculate summary stats from temp file
    if [ -f "${STATS_TEMP}" ] && [ -s "${STATS_TEMP}" ]; then
        # Total entries
        total_entries=$(wc -l < "${STATS_TEMP}" | tr -d ' ')
        
        # DL Duration: min and max (exclude zeros for min)
        dl_sorted="${LOCAL_DIR}/.dl_sorted.tmp"
        cut -d',' -f1 "${STATS_TEMP}" | sort -n > "${dl_sorted}"
        min_dl=$(awk '$1 > 0' "${dl_sorted}" | head -1)
        max_dl=$(tail -1 "${dl_sorted}")
        
        # Calculate percentile indices (1-based, ceiling)
        p50_idx=$(echo "(${total_entries} * 50 + 99) / 100" | bc)
        p90_idx=$(echo "(${total_entries} * 90 + 99) / 100" | bc)
        p95_idx=$(echo "(${total_entries} * 95 + 99) / 100" | bc)
        p99_idx=$(echo "(${total_entries} * 99 + 99) / 100" | bc)
        
        # Get percentile values
        p50_dl=$(sed -n "${p50_idx}p" "${dl_sorted}")
        p90_dl=$(sed -n "${p90_idx}p" "${dl_sorted}")
        p95_dl=$(sed -n "${p95_idx}p" "${dl_sorted}")
        p99_dl=$(sed -n "${p99_idx}p" "${dl_sorted}")
        
        # Count items in each percentile bucket
        # <= p50, p50 < x <= p90, p90 < x <= p95, p95 < x <= p99, > p99
        count_le_p50=$(awk -v threshold="${p50_dl}" '$1 <= threshold {count++} END {print count+0}' "${dl_sorted}")
        count_p50_p90=$(awk -v lo="${p50_dl}" -v hi="${p90_dl}" '$1 > lo && $1 <= hi {count++} END {print count+0}' "${dl_sorted}")
        count_p90_p95=$(awk -v lo="${p90_dl}" -v hi="${p95_dl}" '$1 > lo && $1 <= hi {count++} END {print count+0}' "${dl_sorted}")
        count_p95_p99=$(awk -v lo="${p95_dl}" -v hi="${p99_dl}" '$1 > lo && $1 <= hi {count++} END {print count+0}' "${dl_sorted}")
        count_gt_p99=$(awk -v threshold="${p99_dl}" '$1 > threshold {count++} END {print count+0}' "${dl_sorted}")
        
        # Count journals with zero download duration (incomplete/failed)
        zero_dl_count=$(awk '$1 == 0 || $1 == ""' "${dl_sorted}" | wc -l | tr -d ' ')
        valid_dl_count=$((total_entries - zero_dl_count))
        
        rm -f "${dl_sorted}"
        
        # Max runtime (field 5)
        max_runtime_secs=$(cut -d',' -f5 "${STATS_TEMP}" | sort -n | tail -1)
        max_runtime_fmt=$(format_duration ${max_runtime_secs:-0})
        
        # Retries: max
        max_retries=$(cut -d',' -f2 "${STATS_TEMP}" | sort -n | tail -1)
        
        # Avg File Size: min and max (exclude zeros for min)
        min_avg_file=$(cut -d',' -f3 "${STATS_TEMP}" | sort -n | awk '$1 > 0' | head -1)
        max_avg_file=$(cut -d',' -f3 "${STATS_TEMP}" | sort -n | tail -1)
        
        # Total GET API calls
        total_gets=$(cut -d',' -f4 "${STATS_TEMP}" | awk '{sum+=$1} END {print sum}')
        
        echo ""
        printf "  %-25s %s\n" "Total Journals:" "${total_entries}"
        if [ "${zero_dl_count}" -gt 0 ]; then
            printf "  %-25s %s (excluded %s with zero/missing data)\n" "Valid DL Entries:" "${valid_dl_count}" "${zero_dl_count}"
        fi
        printf "  %-25s %s\n" "Min DL Duration:" "${min_dl}s"
        printf "  %-25s %s\n" "Max DL Duration:" "${max_dl}s"
        printf "  %-25s %s\n" "Max Runtime:" "${max_runtime_fmt}"
        printf "  %-25s %s\n" "Max Retries:" "${max_retries}"
        printf "  %-25s %s MB\n" "Min Avg File Size:" "${min_avg_file}"
        printf "  %-25s %s MB\n" "Max Avg File Size:" "${max_avg_file}"
        printf "  %-25s %s\n" "Total GET API Calls:" "${total_gets}"
        echo ""
        echo "  Download Duration Percentiles:"
        printf "    %-20s %ss (%d items <= p50)\n" "p50 (median):" "${p50_dl}" "${count_le_p50}"
        printf "    %-20s %ss (%d items in p50-p90)\n" "p90:" "${p90_dl}" "${count_p50_p90}"
        printf "    %-20s %ss (%d items in p90-p95)\n" "p95:" "${p95_dl}" "${count_p90_p95}"
        printf "    %-20s %ss (%d items in p95-p99)\n" "p99:" "${p99_dl}" "${count_p95_p99}"
        if [ "${count_gt_p99}" -gt 0 ]; then
            printf "    %-20s %d items > p99\n" "Outliers:" "${count_gt_p99}"
        fi
        echo ""
    fi
    
    echo "================================================================================"
    echo "                              END OF REPORT"
    echo "================================================================================"
} | tee "${OUTPUT_FILE}"

# Cleanup temp files
rm -f "${STATS_TEMP}" "${LOCAL_DIR}/.report_body.txt"

echo ""
echo "=== Done ==="
echo "Report saved to: ${OUTPUT_FILE}"
