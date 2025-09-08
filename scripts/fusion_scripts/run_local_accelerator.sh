#!/bin/bash

set -e  # Exit on any error

# Constants
ACCELERATOR_CLI="/opt/couchbase/bin/fusion/accelerator-cli"
GUEST_STORAGE_PATH="/mnt/nfs/share/guest_storage"
MANIFEST_SOURCE_PATH="/mnt/nfs/share/fusion-manifests"
BASE_URI="/mnt/nfs/share/buckets"

# Validate required argument
if [ -z "$1" ]; then
    echo "Error: hostID argument is required"
    echo "Usage: $0 <hostID>"
    exit 1
fi

HOST_ID="$1"
SKIP_FLAG=""

if [ "$2" == "--skip-file-linking" ]; then
    SKIP_FLAG="-skip-file-linking"
fi

# Create base directories
PRIVATE_PREFIX="$GUEST_STORAGE_PATH/$HOST_ID"

# Cleanup existing guest directories and private storage
echo "Cleaning up existing guest directories and private storage..."
for dir in /guest*; do
    if [ -L "$dir" ]; then
        echo "Removing guest symlink: $dir"
        sudo rm -f "$dir"
    fi
done

if [ -d "$PRIVATE_PREFIX" ]; then
    echo "Removing private storage: $PRIVATE_PREFIX"
    rm -rf "$PRIVATE_PREFIX"
fi

mkdir -p "$PRIVATE_PREFIX"

# Function to create guest directory and symlink
create_guest_directory() {
    local guest_num=$1
    local guest_dir="guest$guest_num"
    local full_guest_path="$PRIVATE_PREFIX/$guest_dir"
    echo "Creating guest directory: $full_guest_path"
    mkdir -p "$full_guest_path"

    # Create symlink in root directory
    echo "Creating symlink: /$guest_dir -> $full_guest_path"
    sudo ln -sfn "$full_guest_path" "/$guest_dir"
}

# Function to process manifest part
process_manifest_part() {
    local part_num=$1
    local manifest_file="$MANIFEST_SOURCE_PATH/$HOST_ID/part$part_num.json"
    local guest_dir="guest$part_num"
    local dest_path="$PRIVATE_PREFIX/$guest_dir"

    if [ ! -f "$manifest_file" ]; then
        echo "Warning: Manifest file not found: $manifest_file"
        return
    fi

    echo "Processing manifest part $part_num"
    echo "Running accelerator-cli download-files for part $part_num"
    (
        "$ACCELERATOR_CLI" download-files \
            -manifest "$manifest_file" \
            -dest "$dest_path" \
            -base-uri "$BASE_URI" \
            $SKIP_FLAG
        chown -R couchbase:couchbase "$dest_path"
    ) &
}

# Find number of parts by counting part files
PARTS_COUNT=$(ls "$MANIFEST_SOURCE_PATH/$HOST_ID"/part*.json 2>/dev/null | wc -l)

if [ "$PARTS_COUNT" -eq 0 ]; then
    echo "Error: No manifest parts found in $MANIFEST_SOURCE_PATH/$HOST_ID"
    exit 1
fi

echo "Found $PARTS_COUNT manifest parts"

# Process each part
for ((i=1; i<=PARTS_COUNT; i++)); do
    # Create guest directory and symlink
    create_guest_directory "$i"

    # Process manifest part in parallel
    process_manifest_part "$i"
done

# Wait for all background processes to complete
echo "Waiting for all accelerator-cli processes to complete..."
wait

echo "Local accelerator setup completed successfully"