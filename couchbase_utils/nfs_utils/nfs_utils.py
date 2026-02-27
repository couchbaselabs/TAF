from global_vars import logger
from shell_util.remote_connection import RemoteMachineShellConnection


class NfsUtil(object):
    def __init__(self):
        self.log = logger.get("test")

    def validate_nfs_server(self, nfs_server_node):
        """
        Validates that the given node is a properly configured NFS server.
        Checks: NFS packages installed, NFS service running, /data exported,
        /data directory exists with correct permissions, and exportfs active.
        Returns True if all checks pass, raises Exception otherwise.
        """
        shell = RemoteMachineShellConnection(nfs_server_node)
        try:
            checks = {
                "nfs_package_installed": {
                    "cmd": "dpkg -l nfs-kernel-server 2>/dev/null | grep -q '^ii' && echo 'OK' || "
                           "(rpm -q nfs-utils 2>/dev/null && echo 'OK' || echo 'FAIL')",
                    "error": "NFS server packages not installed"
                },
                "nfs_service_running": {
                    "cmd": "systemctl is-active nfs-kernel-server 2>/dev/null || "
                           "systemctl is-active nfs-server 2>/dev/null || echo 'FAIL'",
                    "error": "NFS server service is not running"
                },
                "nfs_service_enabled": {
                    "cmd": "systemctl is-enabled nfs-kernel-server 2>/dev/null || "
                           "systemctl is-enabled nfs-server 2>/dev/null || echo 'FAIL'",
                    "error": "NFS server service is not enabled on boot"
                },
                "data_dir_exists": {
                    "cmd": "test -d /data && echo 'OK' || echo 'FAIL'",
                    "error": "/data directory does not exist"
                },
                "data_dir_permissions": {
                    "cmd": "stat -c '%a' /data 2>/dev/null || stat -f '%Lp' /data 2>/dev/null",
                    "error": "/data directory permissions could not be read"
                },
                "data_in_exports": {
                    "cmd": "grep -q '/data' /etc/exports && echo 'OK' || echo 'FAIL'",
                    "error": "/data is not listed in /etc/exports"
                },
                "exportfs_active": {
                    "cmd": "exportfs -v 2>/dev/null | grep -q '/data' && echo 'OK' || echo 'FAIL'",
                    "error": "/data is not actively exported (exportfs)"
                },
                "rpcbind_running": {
                    "cmd": "systemctl is-active rpcbind 2>/dev/null || echo 'FAIL'",
                    "error": "rpcbind service is not running"
                }
            }

            failed = []
            for check_name, check_info in checks.items():
                output, error = shell.execute_command(check_info["cmd"])
                result = output[0].strip() if output else "FAIL"
                if result == "FAIL":
                    self.log.error("NFS check '%s' failed: %s"
                                   % (check_name, check_info["error"]))
                    failed.append(check_name)
                elif check_name == "data_dir_permissions":
                    if result != "777":
                        self.log.error("/data permissions are %s, expected 777"
                                       % result)
                        failed.append(check_name)
                    else:
                        self.log.debug("NFS check '%s' passed" % check_name)
                else:
                    self.log.info("NFS check '%s' passed" % check_name)

            if failed:
                raise Exception(
                    "NFS server validation failed on %s. "
                    "Failed checks: %s" % (nfs_server_node.ip,
                                           ", ".join(failed)))

            self.log.info("All NFS server checks passed for %s"
                          % nfs_server_node.ip)
            return True
        finally:
            shell.disconnect()

    def validate_nfs_client(self, client_node, nfs_server_node,
                            mount_point="/mnt/nfs_data"):
        """
        Validates that the given client node is a properly configured NFS
        client connected to the specified NFS server. Checks: NFS client
        packages installed, rpcbind running, NFS server reachable,
        server:/data mounted, mount point accessible and writable,
        fstab entry for persistence.
        Returns True if all checks pass, raises Exception otherwise.
        """
        shell = RemoteMachineShellConnection(client_node)
        try:
            checks = {
                "nfs_client_package_installed": {
                    "cmd": "dpkg -l nfs-common 2>/dev/null | grep -q '^ii' && echo 'OK' || "
                           "(rpm -q nfs-utils 2>/dev/null && echo 'OK' || echo 'FAIL')",
                    "error": "NFS client packages (nfs-common/nfs-utils) not installed"
                },
                "rpcbind_running": {
                    "cmd": "systemctl is-active rpcbind 2>/dev/null || echo 'FAIL'",
                    "error": "rpcbind service is not running on client"
                },
                "nfs_server_reachable": {
                    "cmd": "ping -c 1 -W 3 %s >/dev/null 2>&1 && echo 'OK' || echo 'FAIL'"
                           % nfs_server_node.ip,
                    "error": "NFS server %s is not reachable from client"
                             % nfs_server_node.ip
                },
                "showmount_server": {
                    "cmd": "showmount -e %s 2>/dev/null | grep -q '/data' && echo 'OK' || echo 'FAIL'"
                           % nfs_server_node.ip,
                    "error": "/data not listed in NFS server exports (showmount)"
                },
                "mount_point_exists": {
                    "cmd": "test -d %s && echo 'OK' || echo 'FAIL'"
                           % mount_point,
                    "error": "Mount point %s does not exist" % mount_point
                },
                "nfs_mounted": {
                    "cmd": "mount | grep -q '%s:/data.*%s' && echo 'OK' || "
                           "(mount | grep -q '%s.*nfs' && echo 'OK' || echo 'FAIL')"
                           % (nfs_server_node.ip, mount_point, mount_point),
                    "error": "%s:/data is not mounted at %s"
                             % (nfs_server_node.ip, mount_point)
                },
                "mount_type_nfs": {
                    "cmd": "df -T %s 2>/dev/null | grep -qE 'nfs|nfs4' && echo 'OK' || echo 'FAIL'"
                           % mount_point,
                    "error": "%s is not an NFS mount" % mount_point
                },
                "mount_writable": {
                    "cmd": "touch %s/.nfs_client_test 2>/dev/null && rm -f %s/.nfs_client_test "
                           "&& echo 'OK' || echo 'FAIL'"
                           % (mount_point, mount_point),
                    "error": "Mount point %s is not writable" % mount_point
                },
                "mount_readable": {
                    "cmd": "ls %s >/dev/null 2>&1 && echo 'OK' || echo 'FAIL'"
                           % mount_point,
                    "error": "Mount point %s is not readable" % mount_point
                },
                "fstab_entry": {
                    "cmd": "grep -q '%s:/data' /etc/fstab && echo 'OK' || echo 'FAIL'"
                           % nfs_server_node.ip,
                    "error": "No fstab entry for %s:/data "
                             "(mount not persistent across reboots)"
                             % nfs_server_node.ip
                },
                "nfs_statd_running": {
                    "cmd": "systemctl is-active nfs-client.target 2>/dev/null || "
                           "systemctl is-active nfs-lock 2>/dev/null || "
                           "systemctl is-active rpc-statd 2>/dev/null || echo 'FAIL'",
                    "error": "NFS client services "
                             "(nfs-client.target/rpc-statd) not active"
                }
            }

            failed = []
            for check_name, check_info in checks.items():
                output, error = shell.execute_command(check_info["cmd"])
                result = output[0].strip() if output else "FAIL"
                if result == "FAIL":
                    self.log.error("NFS client check '%s' failed: %s"
                                   % (check_name, check_info["error"]))
                    failed.append(check_name)
                else:
                    self.log.debug("NFS client check '%s' passed"
                                   % check_name)

            if failed:
                raise Exception(
                    "NFS client validation failed on %s. "
                    "Failed checks: %s" % (client_node.ip,
                                           ", ".join(failed)))

            self.log.info("All NFS client checks passed for %s "
                          "(connected to server %s)"
                          % (client_node.ip, nfs_server_node.ip))
            return True
        finally:
            shell.disconnect()

    def setup_nfs_client(self, client_node, nfs_server_ip="172.23.104.203",
                         nfs_share="/data", mount_point="/mnt/nfs_data"):
        """
        Sets up NFS client on the given node. Installs nfs-common, verifies
        server exports, creates mount point, mounts the NFS share, adds
        fstab entry for persistence, and verifies the mount.
        """
        shell = RemoteMachineShellConnection(client_node)
        try:
            self.log.info("Installing NFS client packages on %s"
                          % client_node.ip)
            output, error = shell.execute_command("apt-get update -qq")
            if error:
                self.log.warning("apt-get update warnings: %s" % error)
            output, error = shell.execute_command(
                "apt-get install -y nfs-common")
            if error:
                self.log.warning("nfs-common install warnings: %s" % error)

            self.log.info("Verifying NFS exports from %s" % nfs_server_ip)
            output, error = shell.execute_command(
                "showmount -e %s" % nfs_server_ip)
            if error:
                raise Exception("Cannot reach NFS server %s: %s"
                                % (nfs_server_ip, error))
            self.log.info("NFS exports: %s" % output)

            self.log.info("Creating mount point %s" % mount_point)
            shell.execute_command("mkdir -p %s" % mount_point)

            self.log.info("Mounting %s:%s to %s"
                          % (nfs_server_ip, nfs_share, mount_point))
            output, error = shell.execute_command(
                "mount %s:%s %s" % (nfs_server_ip, nfs_share, mount_point))
            if error:
                raise Exception("Failed to mount %s:%s on %s: %s"
                                % (nfs_server_ip, nfs_share,
                                   client_node.ip, error))

            fstab_entry = ("%s:%s  %s  nfs  defaults,_netdev  0  0"
                           % (nfs_server_ip, nfs_share, mount_point))
            output, _ = shell.execute_command(
                "grep -qF '%s:%s' /etc/fstab && echo 'EXISTS' "
                "|| echo 'MISSING'" % (nfs_server_ip, nfs_share))
            if output and output[0].strip() == "MISSING":
                self.log.info("Adding fstab entry for persistence")
                shell.execute_command(
                    "echo '%s' >> /etc/fstab" % fstab_entry)
            else:
                self.log.info("fstab entry already exists, skipping")

            self.log.info("Verifying mount on %s" % client_node.ip)
            output, error = shell.execute_command("df -h | grep nfs")
            if not output:
                raise Exception("NFS mount verification failed on %s"
                                % client_node.ip)
            self.log.info("NFS client setup complete on %s. "
                          "Mounted at: %s" % (client_node.ip, mount_point))
        finally:
            shell.disconnect()
