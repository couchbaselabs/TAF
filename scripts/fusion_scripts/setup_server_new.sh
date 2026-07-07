#!/bin/bash
# Dynamic NFS share creation script with rolling deletion (max 5 shares)
# Usage: ./create_nfs_share.sh [<client_cidr>]
# Example: ./create_nfs_share.sh 172.23.103.0/24

set -e

MAX_SHARES=15
NFS_CLIENT_CIDR=${1:-*}
BASE_DIR="/data/nfs"
EXPORTS_FILE="/etc/exports"

# Ensure NFS is installed and running
if ! systemctl is-active --quiet nfs-kernel-server; then
  apt install -y nfs-kernel-server
  systemctl enable --now nfs-kernel-server
fi

mkdir -p "$BASE_DIR"

# Serialize concurrent runs on this NFS server. Multiple Fusion jobs can call
# this script against the same server at once; without a lock they would race
# on (a) share-number selection -> both pick the same share, and (b) the
# read-modify-write of /etc/exports below -> one run's mv clobbers the other's
# export line, and `exportfs -ra` then unexports a live share. Holding this
# lock across the whole critical section (share selection through exportfs)
# makes each run see the previous run's committed state. Released on exit.
exec 9>/var/lock/nfs_setup.lock
flock 9

# Get existing share directories
existing_shares=( $(ls -d ${BASE_DIR}/share* 2>/dev/null | sort -V) )

# Determine highest existing number
highest=$(ls -d ${BASE_DIR}/share* 2>/dev/null | grep -oE 'share[0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1)
next=$((highest + 1))
if [ -z "$highest" ]; then
  next=1
fi

SHARE_DIR="${BASE_DIR}/share${next}"

# If more than MAX_SHARES exist, delete the oldest
num_shares=${#existing_shares[@]}
if (( num_shares >= MAX_SHARES )); then
  oldest_share="${existing_shares[0]}"
  echo "Removing oldest share: $oldest_share"

  # Remove from /etc/exports
  sed -i "\|^${oldest_share} |d" "$EXPORTS_FILE"

  # Unexport and delete directory
  echo "Unexporting $oldest_share ..."
  exportfs -u "*:$oldest_share" 2>/dev/null || true
  rm -rf "$oldest_share"
fi

# Create new directory
mkdir -p "$SHARE_DIR"
chown nobody:nogroup "$SHARE_DIR"
chmod 777 "$SHARE_DIR"

# Add export rule
echo "${SHARE_DIR} ${NFS_CLIENT_CIDR}(rw,sync,no_root_squash,no_subtree_check)" >> "$EXPORTS_FILE"

# Reconcile /etc/exports with reality before exporting. On these slaves the
# share directories under /data/nfs can be wiped while /etc/exports survives,
# which leaves the file with (a) duplicate lines and (b) stale entries pointing
# at directories that no longer exist. Both make `exportfs -ra` abort:
#   - duplicates            -> "duplicated export entries"
#   - missing directories   -> "Failed to stat ...: No such file or directory"
# Keep only the first occurrence of each line whose share directory ($1) still
# exists on disk. The share we just created above always survives this filter.
awk '!seen[$0]++ && system("test -d " $1) == 0' "$EXPORTS_FILE" > "${EXPORTS_FILE}.tmp"
mv "${EXPORTS_FILE}.tmp" "$EXPORTS_FILE"

# Apply new exports
exportfs -ra

# Output result
echo "Created and exported new NFS share:"
echo "    Directory : ${SHARE_DIR}"
echo "    Clients   : ${NFS_CLIENT_CIDR}"
echo "NEW_SHARE_DIR=${SHARE_DIR}"
