#!/bin/bash
# Usage: ./setup_nfs_client.sh <NFS_SERVER_IP> <SHARE_NAME>
# Example: ./setup_nfs_client.sh 172.23.103.54 share3

NFS_SERVER_ADDR=${1:-172.23.103.54}
NFS_SHARE_NAME=${2:-share1}

apt install -y nfs-common

mkdir -p /mnt/nfs/share
chown -R couchbase:couchbase /mnt/nfs/share

mount $NFS_SERVER_ADDR:/data/nfs/$NFS_SHARE_NAME /mnt/nfs/share
