#!/bin/bash
#
# Accept server from commandline arg or use default
NFS_SERVER_ADDR=${1:-172.23.103.54}

apt install nfs-common -y
mkdir -p /mnt/nfs/share
mount $NFS_SERVER_ADDR:/data/nfs/share /mnt/nfs/share