from global_vars import logger
from platform_utils.ssh_util.install_util.test_input import TestInputServer
from shell_util.remote_connection import RemoteMachineShellConnection
from testconstants import PITR_NFS_SERVER


class NfsUtil(object):
    # Defaults for the shared NFS export used by the PITR / continuous backup
    # suites. Server side setup scripts live at
    # https://github.com/couchbaselabs/test_infra_runner/tree/master/scripts/pitr_scripts
    # -- setup_nfs_server() / setup_nfs_client() are ports of them.
    NFS_SHARE = "/data"
    MOUNT_POINT = "/mnt/nfs_data"

    def __init__(self, nfs_server_node, setup_server=False):
        """
        :param setup_server: let ensure_nfs_clients() set up a server that
                             fails validation, instead of raising. Off by
                             default: setting up a server adds to its
                             /etc/exports, which a run must not do to the
                             shared PITR server unasked.
        """
        self.log = logger.get("test")
        self.nfs_server_node = nfs_server_node
        self.setup_server = setup_server

    @classmethod
    def for_pitr_tests(cls, test_input):
        """
        Build an NfsUtil for the shared PITR NFS server, letting a run point
        at a different server or credentials via the pitr_nfs_* params, and
        opt in to setting that server up via pitr_nfs_setup_server=True.

        :param test_input: TestInput instance (i.e. the test's `self.input`)
        """
        nfs_server_node = TestInputServer()
        nfs_server_node.ip = test_input.param("pitr_nfs_server",
                                              PITR_NFS_SERVER)
        nfs_server_node.ssh_username = test_input.param("pitr_nfs_username",
                                                        "root")
        nfs_server_node.ssh_password = test_input.param("pitr_nfs_password",
                                                        "couchbase")
        return cls(nfs_server_node,
                   setup_server=test_input.param("pitr_nfs_setup_server",
                                                 False))

    def ensure_nfs_clients(self, client_nodes, nfs_share=NFS_SHARE,
                           mount_point=MOUNT_POINT):
        """
        Validate the NFS server, then make sure every node in `client_nodes`
        has `nfs_share` mounted at `mount_point`, setting up the client on the
        fly for any node that is not mounted yet. With `setup_server`, a
        server that fails validation is set up on the fly the same way,
        exporting `nfs_share` to `client_nodes`.

        Raises if the server is unhealthy (after the setup attempt, with
        `setup_server`), or if a node still fails client validation after the
        setup attempt.
        """
        self.log.info("Validating NFS server at %s" % self.nfs_server_node.ip)
        try:
            self.validate_nfs_server()
        except Exception as e:
            if not self.setup_server:
                raise
            self.log.warning(f"NFS server validation failed: {e}. "
                             f"Attempting to set up NFS server.")
            self.setup_nfs_server(client_nodes, nfs_share)
            self.validate_nfs_server()

        for node in client_nodes:
            self.log.info("Validating NFS client at %s connected to server %s"
                          % (node.ip, self.nfs_server_node.ip))
            try:
                self.validate_nfs_client(node, mount_point=mount_point)
            except Exception as e:
                self.log.warning("NFS client validation failed: %s. "
                                 "Attempting to set up NFS client." % e)
                self.setup_nfs_client(node, nfs_share, mount_point)
                self.validate_nfs_client(node, mount_point=mount_point)

    def validate_nfs_server(self):
        """
        Validates that the given node is a properly configured NFS server.
        Checks: NFS packages installed, NFS service running, /data exported,
        /data directory exists with correct permissions, and exportfs active.
        Returns True if all checks pass, raises Exception otherwise.
        """
        shell = RemoteMachineShellConnection(self.nfs_server_node)
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
                    "Failed checks: %s" % (self.nfs_server_node.ip,
                                           ", ".join(failed)))

            self.log.info("All NFS server checks passed for %s"
                          % self.nfs_server_node.ip)
            return True
        finally:
            shell.disconnect()

    def validate_nfs_client(self, client_node,
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
                           % self.nfs_server_node.ip,
                    "error": "NFS server %s is not reachable from client"
                             % self.nfs_server_node.ip
                },
                "showmount_server": {
                    "cmd": "showmount -e %s 2>/dev/null | grep -q '/data' && echo 'OK' || echo 'FAIL'"
                           % self.nfs_server_node.ip,
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
                           % (self.nfs_server_node.ip, mount_point, mount_point),
                    "error": "%s:/data is not mounted at %s"
                             % (self.nfs_server_node.ip, mount_point)
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
                           % self.nfs_server_node.ip,
                    "error": "No fstab entry for %s:/data "
                             "(mount not persistent across reboots)"
                             % self.nfs_server_node.ip
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
                          % (client_node.ip, self.nfs_server_node.ip))
            return True
        finally:
            shell.disconnect()

    def setup_nfs_server(self, client_nodes, nfs_share=NFS_SHARE):
        """
        Sets up the NFS server: installs nfs-kernel-server, makes `nfs_share`
        world-writable and owned by couchbase, exports it with every client
        write squashed to the couchbase user, and starts the service.

        Unlike setup_nfs_server.sh, `nfs_share` is exported only to
        `client_nodes`, not to every host. Safe to rerun: nothing under
        `nfs_share` is touched, and an existing /etc/exports entry for it is
        left as is.
        """
        server_ip = self.nfs_server_node.ip
        shell = RemoteMachineShellConnection(self.nfs_server_node)
        try:
            self.log.info(f"Installing NFS server packages on {server_ip}")
            output, error = shell.execute_command("apt-get update -qq")
            if error:
                self.log.warning(f"apt-get update warnings: {error}")
            output, error = shell.execute_command(
                "apt-get install -y nfs-kernel-server")
            if error:
                self.log.warning(f"nfs-kernel-server install warnings: {error}")

            # Every client write lands as the couchbase user (all_squash
            # below), which a host without Couchbase installed lacks
            shell.execute_command(
                "id -u couchbase || useradd -r -s /bin/false couchbase")
            output, _ = shell.execute_command("id -u couchbase")
            cb_uid = output[0].strip()
            output, _ = shell.execute_command("id -g couchbase")
            cb_gid = output[0].strip()

            self.log.info(f"Preparing shared directory {nfs_share}")
            for cmd in (f"mkdir -p {nfs_share}",
                        f"chmod 777 {nfs_share}",
                        f"chown couchbase:couchbase {nfs_share}"):
                output, error = shell.execute_command(cmd)
                if error:
                    raise Exception(f"'{cmd}' failed on NFS server "
                                    f"{server_ip}: {error}")

            output, _ = shell.execute_command(
                f"grep -qE '^{nfs_share}[[:space:]]' /etc/exports "
                f"&& echo 'EXISTS' || echo 'MISSING'")
            if output and output[0].strip() == "MISSING":
                export_opts = (f"rw,sync,no_subtree_check,all_squash,"
                               f"anonuid={cb_uid},anongid={cb_gid}")
                clients = " ".join(f"{node.ip}({export_opts})"
                                   for node in client_nodes)
                export_line = f"{nfs_share} {clients}"
                self.log.info(f"Adding export entry: {export_line}")
                shell.execute_command(f"echo '{export_line}' >> /etc/exports")
            else:
                self.log.info(f"Entry for {nfs_share} already exists in "
                              f"/etc/exports, skipping")

            output, error = shell.execute_command("exportfs -ra")
            if error:
                self.log.warning(f"exportfs warnings: {error}")
            output, error = shell.execute_command(
                "systemctl enable --now rpcbind nfs-kernel-server")
            if error:
                self.log.warning(f"systemctl warnings: {error}")

            output, _ = shell.execute_command("exportfs -v")
            self.log.info(f"NFS server setup complete on {server_ip}. "
                          f"Exports: {output}")
        finally:
            shell.disconnect()

    def setup_nfs_client(self, client_node,
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

            self.log.info("Verifying NFS exports from %s" % self.nfs_server_node.ip)
            output, error = shell.execute_command(
                "showmount -e %s" % self.nfs_server_node.ip)
            if error:
                raise Exception("Cannot reach NFS server %s: %s"
                                % (self.nfs_server_node.ip, error))
            self.log.info("NFS exports: %s" % output)

            self.log.info("Creating mount point %s" % mount_point)
            shell.execute_command("mkdir -p %s" % mount_point)

            self.log.info("Mounting %s:%s to %s"
                          % (self.nfs_server_node.ip, nfs_share, mount_point))
            output, error = shell.execute_command(
                "mount %s:%s %s" % (self.nfs_server_node.ip, nfs_share, mount_point))
            if error:
                raise Exception("Failed to mount %s:%s on %s: %s"
                                % (self.nfs_server_node.ip, nfs_share,
                                   client_node.ip, error))

            fstab_entry = ("%s:%s  %s  nfs  defaults,_netdev  0  0"
                           % (self.nfs_server_node.ip, nfs_share, mount_point))
            output, _ = shell.execute_command(
                "grep -qF '%s:%s' /etc/fstab && echo 'EXISTS' "
                "|| echo 'MISSING'" % (self.nfs_server_node.ip, nfs_share))
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
