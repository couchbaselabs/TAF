#!/usr/bin/env python3
"""
NFS setup and teardown for Fusion tests, called by executor_script.sh.

Usage:
  python nfs_setup.py setup
      --ini <populated_testexec_ini>
      --nfs-server-ip <ip>
      --local-scripts-path <path_to_fusion_scripts_dir>

  python nfs_setup.py teardown
      --nfs-server-ip <ip>
      --cluster-ips <ip1,ip2,...>
      --client-share-dir <shareN>

The 'setup' command prints "CLIENT_SHARE_DIR=<value>" to stdout so
executor_script.sh can capture it and forward it to testrunner.py.
"""

import argparse
import concurrent.futures
import os
import sys

import paramiko


SSH_USER = "root"
SSH_PASSWORD = "couchbase"
NFS_SCRIPTS_REMOTE_DIR = "/root/nfs_scripts"
FUSION_SCRIPTS_REMOTE_DIR = "/root/fusion"
FUSION_SCRIPT_FILES = ["run_fusion_rebalance.py", "run_local_accelerator.sh", "config.json"]


# ------------------------------------------------------------------ #
# SSH helpers                                                          #
# ------------------------------------------------------------------ #

def ssh_connect(ip):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=SSH_USER, password=SSH_PASSWORD, timeout=30)
    return client


def run_cmd(ssh, cmd):
    _, stdout, stderr = ssh.exec_command(cmd)
    out = stdout.read().decode()
    err = stderr.read().decode()
    return out, err


def sftp_put(ssh, local_path, remote_path):
    with ssh.open_sftp() as sftp:
        sftp.put(local_path, remote_path)


# ------------------------------------------------------------------ #
# Setup                                                                #
# ------------------------------------------------------------------ #

def parse_ini_ips(ini_file):
    """Extract cluster server IPs from lines of the form 'ip:<address>'."""
    ips = []
    with open(ini_file) as f:
        for line in f:
            line = line.strip()
            if line.startswith("ip:"):
                ips.append(line.split(":", 1)[1].strip())
    return sorted(set(ips))


def setup_nfs_server(nfs_server_ip, local_scripts_path):
    print(f"[NFS] Setting up NFS server: {nfs_server_ip}")
    ssh = ssh_connect(nfs_server_ip)

    run_cmd(ssh, f"mkdir -p {NFS_SCRIPTS_REMOTE_DIR}")
    sftp_put(
        ssh,
        os.path.join(local_scripts_path, "setup_server_new.sh"),
        f"{NFS_SCRIPTS_REMOTE_DIR}/setup_server_new.sh",
    )

    out, err = run_cmd(
        ssh,
        f"chmod +x {NFS_SCRIPTS_REMOTE_DIR}/setup_server_new.sh;"
        f" bash {NFS_SCRIPTS_REMOTE_DIR}/setup_server_new.sh",
    )
    print(out)
    if err:
        print(f"[NFS server stderr]: {err}", file=sys.stderr)
    ssh.close()

    for line in out.splitlines():
        if line.startswith("NEW_SHARE_DIR="):
            new_share_dir = line.split("=", 1)[1].strip()
            return os.path.basename(new_share_dir)

    raise RuntimeError("Could not determine NEW_SHARE_DIR from NFS server output")


def setup_nfs_client(server_ip, nfs_server_ip, client_share_dir, local_scripts_path):
    print(f"[NFS] Configuring client: {server_ip}")
    ssh = ssh_connect(server_ip)

    run_cmd(ssh, f"mkdir -p {NFS_SCRIPTS_REMOTE_DIR} {FUSION_SCRIPTS_REMOTE_DIR}")

    sftp_put(
        ssh,
        os.path.join(local_scripts_path, "setup_client_mount_new.sh"),
        f"{NFS_SCRIPTS_REMOTE_DIR}/setup_client_mount_new.sh",
    )
    out, err = run_cmd(
        ssh,
        f"chmod +x {NFS_SCRIPTS_REMOTE_DIR}/setup_client_mount_new.sh;"
        f" bash {NFS_SCRIPTS_REMOTE_DIR}/setup_client_mount_new.sh"
        f" {nfs_server_ip} {client_share_dir}",
    )
    if out.strip():
        print(f"[NFS] {server_ip}: {out.strip()}")
    if err.strip():
        print(f"[NFS] {server_ip} stderr: {err.strip()}", file=sys.stderr)

    for script in FUSION_SCRIPT_FILES:
        remote_path = f"{FUSION_SCRIPTS_REMOTE_DIR}/{script}"
        sftp_put(ssh, os.path.join(local_scripts_path, script), remote_path)
        run_cmd(ssh, f"chmod +x {remote_path}")

    ssh.close()
    print(f"[NFS] Client configured: {server_ip}")


def cmd_setup(args):
    cluster_ips = parse_ini_ips(args.ini)
    print(f"[NFS] Cluster server IPs: {cluster_ips}")

    client_share_dir = setup_nfs_server(args.nfs_server_ip, args.local_scripts_path)
    print(f"[NFS] New share: {client_share_dir}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                setup_nfs_client, ip, args.nfs_server_ip, client_share_dir, args.local_scripts_path
            )
            for ip in cluster_ips
        ]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    print("[NFS] All clients configured")
    # Parsed by executor_script.sh to forward as a testrunner.py parameter
    print(f"CLIENT_SHARE_DIR={client_share_dir}")


# ------------------------------------------------------------------ #
# Teardown                                                             #
# ------------------------------------------------------------------ #

def teardown_nfs_client(server_ip):
    print(f"[NFS] Unmounting: {server_ip}")
    ssh = ssh_connect(server_ip)
    run_cmd(ssh, "umount -lf /mnt/nfs/share 2>/dev/null || true; rm -rf /mnt/nfs")
    ssh.close()
    print(f"[NFS] Unmounted: {server_ip}")


def cmd_cleanup_clients(args):
    """Unmount NFS on all cluster servers from the ini file.

    Called at the START of a new fusion job to evict any stale mounts left
    behind by a previously aborted job.  Server-side share cleanup is handled
    separately by setup_server_new.sh via MAX_SHARES.
    """
    cluster_ips = parse_ini_ips(args.ini)
    print(f"[NFS] Pre-setup client cleanup on: {cluster_ips}")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(teardown_nfs_client, ip) for ip in cluster_ips]
        for f in concurrent.futures.as_completed(futures):
            try:
                f.result()
            except Exception as e:
                # Non-fatal: machine may simply not have been mounted before
                print(f"[NFS] Cleanup warning: {e}", file=sys.stderr)
    print("[NFS] Client cleanup complete")

def cmd_teardown(args):
    cluster_ips = [ip.strip() for ip in args.cluster_ips.split(",") if ip.strip()]

    print("[NFS] Unmounting NFS clients in parallel")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(teardown_nfs_client, ip) for ip in cluster_ips]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    share = args.client_share_dir
    print(f"[NFS] Removing server share: {share}")
    ssh = ssh_connect(args.nfs_server_ip)
    run_cmd(
        ssh,
        f"sed -i '\\|^/data/nfs/{share} |d' /etc/exports;"
        f" exportfs -ra;"
        f" rm -rf /data/nfs/{share}",
    )
    ssh.close()
    print("[NFS] Teardown complete")


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #

def main():
    print("New python standalone script for Fusion NFS SetUp and TearDown")
    parser = argparse.ArgumentParser(description="NFS setup/teardown for Fusion tests")
    sub = parser.add_subparsers(dest="action", required=True)

    p_setup = sub.add_parser("setup")
    p_setup.add_argument("--ini", required=True, help="Path to the populated testexec ini file")
    p_setup.add_argument("--nfs-server-ip", required=True)
    p_setup.add_argument("--local-scripts-path", required=True,
                         help="Local path to scripts/fusion_scripts/")

    p_cleanup = sub.add_parser("cleanup_clients")
    p_cleanup.add_argument("--ini", required=True,
                           help="Path to the populated testexec ini file")

    p_tear = sub.add_parser("teardown")
    p_tear.add_argument("--nfs-server-ip", required=True)
    p_tear.add_argument("--cluster-ips", required=True,
                        help="Comma-separated cluster server IPs")
    p_tear.add_argument("--client-share-dir", required=True)

    args = parser.parse_args()
    if args.action == "setup":
        cmd_setup(args)
    elif args.action == "cleanup_clients":
        cmd_cleanup_clients(args)
    elif args.action == "teardown":
        cmd_teardown(args)


if __name__ == "__main__":
    main()
