#!/bin/bash
apt install nfs-kernel-server -y
mkdir -p /data/nfs/share
chown nobody:nogroup /data/nfs/share
chmod -R 777 /data/nfs/share

echo "/data/nfs/share *(rw,sync,no_root_squash,no_subtree_check)" >> /etc/exports

exportfs -a
exportfs -v
systemctl start nfs-kernel-server