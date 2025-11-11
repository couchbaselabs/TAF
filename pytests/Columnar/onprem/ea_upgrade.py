"""
Created on 19-Nov-2025

@author: himanshu.jain@couchbase.com

Enterprise Analytics Upgrade Test
Setup: 2-node cluster (node1 and node2) on version 2.0.0-1069, node3 as spare
Swap Rebalance:
1. Upgrade node3 to 2.1; Swap node1 ↔ node3; Rebalance
2. Upgrade node1 to 2.1; Swap node2 ↔ node1; Rebalance
Result: node1 and node3 cluster with upgraded versions
"""

from pytests.Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from platform_utils.ssh_util.shell_util.remote_connection import RemoteMachineShellConnection
from cb_constants.CBServer import CbServer
from cb_constants.ClusterRun import ClusterRun
from awsLib.S3 import S3
from custom_exceptions.exception import RebalanceFailedException
import time


class EnterpriseAnalyticsUpgrade(ColumnarOnPremBase):
    """
    Test class for Enterprise Analytics upgrade from 2.0 to 2.1
    using swap rebalance
    """

    # Hardcoded Enterprise Analytics build URLs and constants
    EA_DOWNLOAD_SERVER = "latestbuilds.service.couchbase.com"
    EA_BASE_URL_PATH = "builds/latestbuilds/enterprise-analytics"
    EA_VERSION_MAP = {
        "2.1": "phoenix",  # Map version to build path
    }

    # Enterprise Analytics service and paths
    EA_SERVICE_NAME = "enterprise-analytics"
    EA_INSTALL_DIR = "/opt/enterprise-analytics"
    EA_DOWNLOAD_DIR = "/tmp"

    COMMANDS = {
        "uninstall": [
            ("rm -rf /tmp/tmp* ; rm -rf /tmp/cbbackupmgr-staging; rm -rf /tmp/entbackup* || true",
             0, "Clean up temporary directories"),
            ("systemctl -q stop {}.service || true".format(EA_SERVICE_NAME),
             0, "Stop Enterprise Analytics service"),
            ("umount -a -t nfs,nfs4 -f -l || true",
             0, "Unmount NFS mounts"),
            ("service ntp restart || true",
             0, "Restart NTP service"),
            ("systemctl unmask {}.service || true".format(EA_SERVICE_NAME),
             0, "Unmask Enterprise Analytics service"),
            ("dpkg -l | grep enterprise-analytics || echo 'not_installed'",
             0, "Check current installation status"),
            ("apt-get purge -y 'enterprise-analytics*' > /dev/null 2>&1 || true",
             10, "Purge Enterprise Analytics packages using apt-get"),
            ("dpkg --purge $(dpkg -l | grep enterprise-analytics | awk '{print $2}' | xargs echo) 2>&1 || true",
             10, "Additional cleanup using dpkg --purge"),
            ("rm -f /var/lib/dpkg/info/enterprise-analytics* || true",
             10, "Remove dpkg info files"),
            ("ps -ef | egrep enterprise-analytics || echo 'no_processes'",
             0, "Check Enterprise Analytics processes"),
            ("kill -9 `ps -ef | egrep enterprise-analytics | cut -f3 -d' '` 2>&1 || true",
             0, "Kill remaining Enterprise Analytics processes"),
            ("rm -rf {} > /dev/null 2>&1 && echo 1 || echo 0".format(EA_INSTALL_DIR),
             0, "Remove install directory"),
            ("rm -rf {}/* > /dev/null 2>&1 && echo 1 || echo 0".format(EA_DOWNLOAD_DIR),
             0, "Remove download directory"),
            ("dpkg -P {} 2>&1 || true".format(EA_SERVICE_NAME),
             0, "Explicit dpkg purge"),
            ("rm -rf /var/lib/dpkg/info/enterprise-analytics* || true",
             0, "Remove dpkg info files (second pass)"),
            ("du -ch /data | grep total || echo 'no_data_dir'",
             0, "Check data directory size"),
            ("rm -rf /data/* || true",
             0, "Remove data directory contents"),
            ("dpkg --configure -a || true",
             0, "Configure dpkg"),
            ("apt-get update || true",
             0, "Update apt"),
            ("journalctl --vacuum-size=100M || true",
             0, "Clean up journal logs (size)"),
            ("journalctl --vacuum-time=10d || true",
             0, "Clean up journal logs (time)"),
            ("grep 'kernel.dmesg_restrict=0' /etc/sysctl.conf || (echo 'kernel.dmesg_restrict=0' >> /etc/sysctl.conf && service procps restart) || true",
             0, "Set kernel.dmesg_restrict if needed"),
            ("rm -rf {} || true".format(EA_INSTALL_DIR),
             0, "Final cleanup of install directory"),
            ("dpkg -l | grep enterprise-analytics || echo 'not_installed'",
             0, "Verify uninstallation"),
        ]}

    def setUp(self):
        super(EnterpriseAnalyticsUpgrade, self).setUp()
        self.upgrade_version = self.input.param(
            "upgrade_version", "2.1.0-1367")
        self.nodes_init = self.input.param("nodes_init", 2)
        self.spare_node = self.cluster.servers[self.nodes_init]
        # Track buckets created during upgrade for cleanup
        self.created_buckets = []  # List of (bucket_name, s3_obj) tuples

    def tearDown(self):
        self.log_setup_status(
            self.__class__.__name__, "Started", stage=self.tearDown.__name__
        )

        # Delete all buckets created during upgrade
        if hasattr(self, 'created_buckets') and self.created_buckets:
            for bucket_name, s3_obj in self.created_buckets:
                if not s3_obj.delete_bucket(bucket_name):
                    self.log.error("AWS bucket failed to delete - {}".format(
                        bucket_name))
                self.log.info(
                    "Successfully deleted bucket - {}".format(bucket_name))

        super(EnterpriseAnalyticsUpgrade, self).tearDown()

        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.tearDown.__name__
        )

    def _get_build_url(self, version, build_number=None):

        # Extract build number from version if present (e.g., "2.1.0-1234" -> "2.1.0" and "1234")
        original_version = version
        if "-" in version:
            version, build_number = version.split("-", 1)
            self.log.debug("Extracted version: {}, build_number: {} from input: {}"
                           .format(version, build_number, original_version))
        else:
            self.log.debug("Using provided version: {}, build_number: {}"
                           .format(version, build_number))

        if not build_number:
            self.log.error(
                "Build number required for version {}".format(version))
            self.fail("Build number required for version {}. "
                      "Use format '2.1.0-1234' or provide build_number parameter"
                      .format(version))

        version_parts = version.split(".")
        main_version = ".".join(version_parts[:2])  # e.g., "2.1" from "2.1.0"
        self.log.debug(
            "Main version (for path mapping): {}".format(main_version))

        if main_version not in self.EA_VERSION_MAP:
            self.log.error("Version {} not in EA_VERSION_MAP. Supported: {}"
                           .format(main_version, list(self.EA_VERSION_MAP.keys())))
            self.fail("Version {} not supported. Supported versions: {}"
                      .format(main_version, list(self.EA_VERSION_MAP.keys())))

        arch_suffix = "amd64"

        # Construct URL
        version_path = self.EA_VERSION_MAP[main_version]
        self.log.debug("Version path from map: {}".format(version_path))

        package_name = "enterprise-analytics_{}-{}-linux_{}.deb".format(
            version, build_number, arch_suffix)
        self.log.debug("Package name: {}".format(package_name))

        url = "https://{}/{}/{}/{}/{}".format(
            self.EA_DOWNLOAD_SERVER,
            self.EA_BASE_URL_PATH,
            version_path,
            build_number,
            package_name)

        self.log.info("Constructed build URL: {}".format(url))
        return url, package_name

    def _download_build(self, shell, build_url, package_name):
        download_path = "{}/{}".format(self.EA_DOWNLOAD_DIR, package_name)
        self.log.debug("Download path: {}".format(download_path))

        # Check if file already exists
        self.log.debug(
            "Checking if package already exists at: {}".format(download_path))
        cmd = "test -f {} && echo 'exists' || echo 'not_exists'".format(
            download_path)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "File existence check - output: {}, error: {}".format(output, error))

        if output and len(output) > 0 and output[0].strip() == "exists":
            self.log.debug("Package already exists at {}, skipping download"
                           .format(download_path))
            # Verify file size
            cmd = "ls -lh {} | awk '{{print $5}}'".format(download_path)
            size_output, _ = shell.execute_command(cmd)
            if size_output:
                self.log.debug("Existing package size: {}".format(
                    size_output[0] if size_output else "unknown"))
            return download_path

        # Download using wget or curl
        self.log.debug("Package not found, starting download...")
        self.log.debug("Using download directory: {}".format(
            self.EA_DOWNLOAD_DIR))
        cmd = "cd {} && wget -q {} -O {} || curl -L {} -o {}".format(
            self.EA_DOWNLOAD_DIR, build_url, package_name,
            build_url, package_name)
        self.log.debug("Executing download command: {}".format(cmd))
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Download command output: {}, error: {}".format(output, error))

        if error and len(error) > 0:
            self.log.error("Download error: {}".format(error))
            self.fail("Failed to download build: {}".format(build_url))

        # Verify download
        self.log.debug("Verifying downloaded file exists...")
        cmd = "test -f {} && echo 'exists' || echo 'not_exists'".format(
            download_path)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Verification check - output: {}, error: {}".format(output, error))

        if not output or len(output) == 0 or output[0].strip() != "exists":
            self.log.error(
                "Downloaded file not found at {}".format(download_path))
            self.fail("Downloaded file not found at {}".format(download_path))

        # Get file size
        cmd = "ls -lh {} | awk '{{print $5}}'".format(download_path)
        size_output, _ = shell.execute_command(cmd)
        if size_output:
            self.log.debug(
                "Downloaded package size: {}".format(size_output[0]))

        self.log.info("Successfully downloaded: {}".format(download_path))
        return download_path

    def _uninstall_enterprise_analytics(self, shell):
        # Execute commands in sequence
        for cmd, sleep_seconds, description in self.COMMANDS["uninstall"]:
            self.log.debug("{}...".format(description))
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "{} - output: {}, error: {}".format(description, output, error))

            # Sleep if specified
            if sleep_seconds > 0:
                self.sleep(sleep_seconds, "Wait after {}".format(description))
        self.log.debug("Uninstallation completed")

    def _install_enterprise_analytics(self, shell, package_path):
        """Install Enterprise Analytics from deb package"""

        # Verify package file exists before installation
        self.log.debug("Verifying package file exists...")
        cmd = "test -f {} && echo 'exists' || echo 'not_exists'".format(
            package_path)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Package file check - output: {}, error: {}".format(output, error))
        if output and "not_exists" in output[0]:
            self.log.error(
                "Package file not found at: {}".format(package_path))
            self.fail("Package file not found at: {}".format(package_path))

        # Get package file size
        cmd = "ls -lh {} | awk '{{print $5}}'".format(package_path)
        size_output, _ = shell.execute_command(cmd)
        if size_output:
            self.log.debug("Package file size: {}".format(size_output[0]))

        # Follow pattern from install_constants.py: use apt-get install -f
        # This handles dependencies automatically
        self.log.debug(
            "Installing using apt-get (following install_constants.py pattern)...")
        cmd = "DEBIAN_FRONTEND='noninteractive' apt-get -y -f install {} > /dev/null 2>&1 && echo 1 || echo 0".format(
            package_path)
        self.log.debug(
            "Executing: DEBIAN_FRONTEND='noninteractive' apt-get -y -f install {}".format(package_path))
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "apt-get install - output: {}, error: {}".format(output, error))

        # Check if installation succeeded
        if not output or len(output) == 0 or output[0] != '1':
            self.log.warn(
                "apt-get install returned: {}, error: {}".format(output, error))
            # Fallback to dpkg if apt-get fails
            self.log.debug("Trying dpkg installation as fallback...")
            cmd = "dpkg -i {} 2>&1 || true".format(package_path)
            self.log.debug("Executing: dpkg -i {}".format(package_path))
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "dpkg install - output: {}, error: {}".format(output, error))

            # Fix any dependency issues
            self.log.debug("Fixing dependencies...")
            cmd = "apt-get update && apt-get install -f -y > /dev/null 2>&1"
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "Fix dependencies - output: {}, error: {}".format(output, error))

        # Verify installation
        self.log.debug("Verifying installation...")
        cmd = "dpkg -l | grep enterprise-analytics"
        output, error = shell.execute_command(cmd)
        if not output or len(output) == 0:
            self.log.error(
                "Enterprise Analytics installation verification failed - no packages found")
            self.fail("Enterprise Analytics installation verification failed")

        self.log.info("Installation completed successfully")

    def _start_enterprise_analytics(self, shell):
        """Start Enterprise Analytics service"""
        # Check service status before starting
        self.log.debug("Checking service status before starting...")
        cmd = "systemctl status {}.service || true".format(
            self.EA_SERVICE_NAME)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Initial service status - output: {}, error: {}".format(output, error))

        # Unmask the service first (in case it was masked during uninstall)
        self.log.debug("Unmasking Enterprise Analytics service...")
        cmd = "systemctl unmask {}.service || true".format(
            self.EA_SERVICE_NAME)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Unmask service - output: {}, error: {}".format(output, error))

        # Reload systemd daemon to pick up any changes
        self.log.debug("Reloading systemd daemon...")
        cmd = "systemctl daemon-reload"
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "daemon-reload - output: {}, error: {}".format(output, error))

        # Enable the service
        self.log.debug("Enabling Enterprise Analytics service...")
        cmd = "systemctl enable {}.service || true".format(
            self.EA_SERVICE_NAME)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Enable service - output: {}, error: {}".format(output, error))

        # Start the service
        self.log.debug("Starting Enterprise Analytics service...")
        cmd = "systemctl start {}.service".format(self.EA_SERVICE_NAME)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Start service - output: {}, error: {}".format(output, error))

        if error and len(error) > 0:
            self.log.error("Error starting service: {}".format(error))
            # Check service status
            self.log.debug("Checking detailed service status after error...")
            cmd = "systemctl status {}.service".format(self.EA_SERVICE_NAME)
            output, error = shell.execute_command(cmd)
            self.log.debug("Service status details: {}".format(output))

            # Retry starting
            self.log.debug("Retrying service start...")
            cmd = "systemctl start {}.service".format(self.EA_SERVICE_NAME)
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "Retry start - output: {}, error: {}".format(output, error))

        # Check if service is active
        cmd = "systemctl is-active {}.service".format(self.EA_SERVICE_NAME)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Service active check - output: {}, error: {}".format(output, error))

        # Wait a bit for service to start
        self.sleep(10, "Wait for service to start")

        cmd = "systemctl status {}.service --no-pager | head -10".format(
            self.EA_SERVICE_NAME)
        output, error = shell.execute_command(cmd)
        self.log.debug("Final service status: {}".format(output))

    def _is_service_running(self, shell, max_wait=60):
        """Check if Enterprise Analytics service is running"""
        self.log.info(
            "Checking if Enterprise Analytics service is running (max_wait={}s)...".format(max_wait))
        for i in range(max_wait // 5):
            attempt = i + 1
            self.log.debug(
                "Service check attempt {}/{}...".format(attempt, max_wait // 5))
            cmd = "systemctl is-active {}.service".format(self.EA_SERVICE_NAME)
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "Service status check - output: {}, error: {}".format(output, error))

            if output and len(output) > 0 and "active" in output[0].lower():
                self.log.info("Service is active!")
                return True

            self.log.debug("Service not active yet, waiting 5 seconds...")
            self.sleep(5, "Wait for service to become active")

        self.log.warn(
            "Service did not become active within {} seconds".format(max_wait))
        return False

    def _initialize_cluster_for_upgrade(self, node):
        """
        Initialize cluster for upgraded Enterprise Analytics node
        This configures compute storage and initializes the node
        """
        self.log.debug(
            "Initializing cluster for upgraded node: {}".format(node.ip))

        # Configure compute storage if needed (creating new bucket for upgrade)
        if hasattr(self, 'analytics_compute_storage_separation') and self.analytics_compute_storage_separation:
            self.log.debug(
                "Configuring compute storage for node {}...".format(node.ip))
            # Create new bucket for upgrade process
            aws_access_key = getattr(self, 'columnar_aws_access_key', None) or getattr(
                self, 'aws_access_key', None)
            aws_secret_key = getattr(self, 'columnar_aws_secret_key', None) or getattr(
                self, 'aws_secret_key', None)
            aws_bucket_region = getattr(self, 'columnar_aws_region', None) or getattr(
                self, 'aws_region', 'us-west-1')
            aws_endpoint = getattr(self, 'columnar_aws_endpoint', None) or getattr(
                self, 'aws_endpoint', None)

            if not aws_access_key or not aws_secret_key:
                self.log.error(
                    "Missing compute storage configuration parameters")
                self.fail(
                    "Cannot setup compute storage: missing AWS credentials")

            # Create new S3 bucket for upgrade
            columnar_s3_obj = S3(aws_access_key, aws_secret_key,
                                 region=aws_bucket_region,
                                 endpoint_url=aws_endpoint)

            # Generate new bucket name with timestamp
            new_bucket_name = "columnar-build-sanity-" + str(int(time.time()))
            bucket_created = False
            for i in range(5):
                try:
                    bucket_created = columnar_s3_obj.create_bucket(
                        new_bucket_name, aws_bucket_region)
                    if bucket_created:
                        break
                except Exception as e:
                    self.log.error(
                        "Creating S3 bucket - {0} in region {1}. "
                        "Failed.".format(new_bucket_name, aws_bucket_region))
                    self.log.error(str(e))

            if not bucket_created:
                self.fail("Unable to create new S3 bucket for upgrade.")

            aws_bucket_name = new_bucket_name
            self.log.info(
                "Successfully created new bucket: {}".format(aws_bucket_name))

            # Track bucket for cleanup in teardown
            if not hasattr(self, 'created_buckets'):
                self.created_buckets = []
            self.created_buckets.append((aws_bucket_name, columnar_s3_obj))

            # Configure compute storage - following base class pattern
            # After node reset, we should be able to set up topology fresh
            status = self.configure_compute_storage_separation_for_analytics(
                server=node,
                aws_access_key=aws_access_key,
                aws_secret_key=aws_secret_key,
                aws_bucket_name=aws_bucket_name,
                aws_bucket_region=aws_bucket_region)

            if not status:
                self.fail(
                    "Failed to put aws credentials to analytics, request error")

        # Call /clusterInit API explicitly - equivalent to curl command
        self.log.debug(
            "Calling /clusterInit API for node {}...".format(node.ip))

        rest = ClusterRestAPI(node)

        # Prepare cluster init parameters matching curl command
        # curl: clusterName=EA, hostname=127.0.0.1, username=Administrator,
        #       password=password, port=8091, memoryQuota=100
        cluster_init_params = {
            "hostname": node.ip,
            "username": node.rest_username,
            "password": node.rest_password,
            "port": "8091",
            "cluster_name": "EA",
            "memory_quota": 100,  # Hardcoded as requested
            "services": ""  # Empty string for Enterprise Analytics
        }

        self.log.debug("Cluster init parameters: hostname={}, username={}, port={}, "
                       "memoryQuota={}, services={}".format(
                           node.ip, node.rest_username, cluster_init_params["port"],
                           cluster_init_params["memory_quota"], cluster_init_params["services"]))

        # Call initialize_cluster which calls /clusterInit endpoint
        status, content = rest.initialize_cluster(**cluster_init_params)

        if not status:
            self.log.error(
                "Cluster initialization (/clusterInit) failed: {}".format(content))
            self.fail(
                "Cluster initialization (/clusterInit) failed: {}".format(content))

        self.log.info(
            "Cluster initialization (/clusterInit) completed successfully")

    def _get_non_master_node_with_older_version(self, nodes_to_upgrade):
        """
        Get a node from nodes_to_upgrade to upgrade.
        First tries to pick a non-master node that is on older version (not on upgrade_version).
        If no such node exists, picks any node from nodes_to_upgrade.
        Returns None if nodes_to_upgrade is empty.
        """
        if not nodes_to_upgrade:
            return None

        master_ip = self.cluster.master.ip

        # First, try to find a non-master node on older version
        for node in nodes_to_upgrade:
            if node.ip != master_ip:
                # Check if node is on older version
                try:
                    _, node_info = ClusterRestAPI(node).node_details()
                    node_version = node_info.get("version", "")
                    # If upgrade_version is not in node_version, it's on older version
                    if self.upgrade_version not in node_version:
                        self.log.debug("Found non-master node {} on older version: {}"
                                       .format(node.ip, node_version))
                        return node
                    else:
                        self.log.debug("Node {} is already on upgrade version: {}"
                                       .format(node.ip, node_version))
                except Exception as e:
                    self.log.warn(
                        "Failed to get version for node {}: {}".format(node.ip, str(e)))
                    # Continue to next node
                    continue

        # If no non-master older version node found, pick any node from nodes_to_upgrade
        if nodes_to_upgrade:
            selected_node = nodes_to_upgrade[0]
            self.log.debug("No non-master older version node found. Picking any node from nodes_to_upgrade: {}"
                           .format(selected_node.ip))
            return selected_node

        return None

    def _collect_cbcollect_logs_on_failure(self):
        """
        Collect cbcollect logs when rebalance fails and display zip URLs
        Based on rebalance_base.py cbcollect_info method
        """
        self.log.error("=" * 80)
        self.log.error("REBALANCE FAILED - Collecting cbcollect logs")
        self.log.error("=" * 80)

        try:
            rest = ClusterRestAPI(self.cluster.master)
            nodes = self.cluster_util.get_nodes(self.cluster.master)

            # Trigger cbcollect
            self.log.info("Triggering cbcollect on cluster...")
            status = self.cluster_util.trigger_cb_collect_on_cluster(
                rest, nodes)
            if not status:
                self.log.error(
                    "Failed to trigger cbcollect: API returned False")
                return

            # Wait for cbcollect to complete (with shorter timeout for failure case)
            self.log.info("Waiting for cbcollect to complete...")
            status = self.cluster_util.wait_for_cb_collect_to_complete(
                # 40 minutes max (120 * 20 seconds)
                self.cluster, retry_count=120)

            if not status:
                self.log.error("cbcollect timed out or did not complete")
                return

            # Get cbcollect response to extract URLs
            self.log.info("Retrieving cbcollect task information...")
            cb_collect_response = self.cluster_util.get_cluster_tasks(
                self.cluster.master, "clusterLogsCollection")

            if not cb_collect_response:
                self.log.error("Failed to get cbcollect task information")
                return

            # Extract and display URLs
            self.log.error("=" * 80)
            self.log.error("CBCOLLECT LOG DOWNLOAD URLs:")
            self.log.error("=" * 80)

            zip_urls = []
            if 'perNode' in cb_collect_response:
                per_node_data = cb_collect_response['perNode']
                for node_id, node_data in per_node_data.items():
                    node_status = node_data.get('status', 'unknown')

                    # Check for uploaded URL
                    if 'url' in node_data and node_data['url']:
                        zip_url = node_data['url']
                        zip_urls.append((node_id, zip_url))
                        self.log.error("Node {}: {}".format(node_id, zip_url))
                    # Check for local path if no URL
                    elif 'path' in node_data and node_data['path']:
                        local_path = node_data['path']
                        self.log.warn("Node {}: Local path only (not uploaded): {}".format(
                            node_id, local_path))
                        self.log.warn("  Status: {}".format(node_status))
                    else:
                        self.log.warn("Node {}: No URL or path available. Status: {}".format(
                            node_id, node_status))

            if zip_urls:
                self.log.error("=" * 80)
                self.log.error("SUMMARY - Download cbcollect logs from:")
                for node_id, url in zip_urls:
                    self.log.error("  {} -> {}".format(node_id, url))
                self.log.error("=" * 80)
            else:
                self.log.warn(
                    "No downloadable URLs found. Check local paths above.")

        except Exception as e:
            self.log.error(
                "Exception while collecting cbcollect logs: {}".format(str(e)))
            import traceback
            self.log.error(traceback.format_exc())

    def _rebalance_cluster_manually(self, eject_nodes=None):
        """
        Rebalance cluster manually using REST API
        Can be called after adding or removing nodes

        Parameters:
            eject_nodes: Optional list of OTP node IDs to eject during rebalance

        Returns:
            bool: True if rebalance completed successfully, False otherwise
        """
        self.log.debug("Rebalancing cluster manually using REST API...")
        if eject_nodes:
            self.log.debug("Ejecting nodes (OTP IDs): {}".format(eject_nodes))
        else:
            self.log.debug("Rebalancing to incorporate newly added node")

        rest = ClusterRestAPI(self.cluster.master)

        # Get all OTP nodes
        nodes = self.cluster_util.get_otp_nodes(self.cluster.master)
        known_nodes = [node.id for node in nodes]
        self.log.debug("Known nodes (OTP IDs): {}".format(known_nodes))

        # Start rebalance using REST API
        self.log.debug("Starting rebalance via REST API...")
        status, content = rest.rebalance(
            known_nodes=known_nodes,
            eject_nodes=eject_nodes)

        if not status:
            self.log.error("Failed to start rebalance: {}".format(content))
            return False

        self.log.debug("Rebalance started successfully")

        # Wait for rebalance to complete
        self.log.debug("Waiting for rebalance to complete...")
        try:
            rebalance_completed = self.cluster_util.rebalance_reached(
                self.cluster,
                percentage=100,
                wait_step=5,
                num_retry=240,  # 20 minutes max (240 * 5 seconds)
                validate_bucket_ranking=False)
        except RebalanceFailedException as e:
            # Rebalance failed with exception - collect logs
            self.log.error(
                "Rebalance failed with RebalanceFailedException: {}".format(str(e)))
            self._collect_cbcollect_logs_on_failure()
            raise
        except Exception as e:
            # Catch any other unexpected exceptions
            self.log.error(
                "Rebalance failed with unexpected exception: {}".format(str(e)))
            self._collect_cbcollect_logs_on_failure()
            raise

        if not rebalance_completed:
            self.log.error("Rebalance did not complete successfully")
            # Collect cbcollect logs on failure
            self._collect_cbcollect_logs_on_failure()
            return False

        self.log.info("Rebalance completed successfully")

        # Update cluster service lists after rebalance
        self.log.debug(
            "Updating cluster nodes service list after rebalance...")
        self.cluster_util.update_cluster_nodes_service_list(
            self.cluster,
            inactive_added=True,
            inactive_failed=True)

        return True

    def _add_node_to_cluster(self, node_to_add):
        """
        Add a node to the cluster and rebalance
        Steps 1 and 2: Add node + Rebalance

        Parameters:
            node_to_add: TestInputServer object - should NOT be in cluster.nodes_in_cluster (spare node)

        Returns:
            bool: True if node was added and rebalanced successfully, False otherwise
        """

        self.log.info(
            "Step 1: Adding node {} to cluster".format(node_to_add.ip))
        rest = ClusterRestAPI(self.cluster.master)
        status, content = rest.add_node(
            node_to_add.ip,
            username=node_to_add.rest_username,
            password=node_to_add.rest_password,
            services=["kv,cbas"])

        if not status:
            self.log.error("Failed to add node {} to cluster: {}".format(
                node_to_add.ip, content))
            return False

        self.log.debug(
            "Node {} added to cluster successfully".format(node_to_add.ip))

        # Update cluster service lists to include the newly added node
        self.log.debug("Updating cluster nodes service list...")
        self.cluster_util.update_cluster_nodes_service_list(
            self.cluster,
            inactive_added=True,
            inactive_failed=True)

        # Add the new node to nodes_in_cluster
        if node_to_add not in self.cluster.nodes_in_cluster:
            self.cluster.nodes_in_cluster.append(node_to_add)
            self.log.info(
                "Added {} to nodes_in_cluster".format(node_to_add.ip))

        # Step 2: Rebalance to incorporate the newly added node
        self.log.info(
            "Step 2: Rebalancing cluster to incorporate newly added node")

        if not self._rebalance_cluster_manually(eject_nodes=None):
            self.log.error(
                "Rebalance failed after adding node {}".format(node_to_add.ip))
            return False

        return True

    def _update_master_node(self, upgraded_node):
        """
        Update cluster.master to the upgraded node.
        Orchestrator is automatically managed by Couchbase, so we only update cluster.master.

        Parameters:
            upgraded_node: TestInputServer object - the upgraded node that should become master

        Returns:
            bool: True if master was updated successfully, False otherwise
        """
        self.log.info(
            f"Updating cluster.master to upgraded node: {upgraded_node.ip}")

        # Find the upgraded node in cluster.nodes_in_cluster or cluster.servers
        # Compare by IP address (ports may differ)
        target_ip = upgraded_node.ip
        updated = False

        # First try to find in nodes_in_cluster
        for node in self.cluster.nodes_in_cluster:
            if node.ip == target_ip:
                self.cluster.master = node
                updated = True
                break

        # If not found in nodes_in_cluster, try cluster.servers
        if not updated:
            for server in self.cluster.servers:
                if server.ip == target_ip:
                    self.cluster.master = server
                    updated = True
                    break

        if not updated:
            self.log.error("Could not find upgraded node {} in cluster.nodes_in_cluster or cluster.servers"
                           .format(target_ip))
            self.log.error("Current master: {}:{}".format(
                self.cluster.master.ip, self.cluster.master.port))
            self.log.error("Nodes in cluster: {}".format(
                [(n.ip, n.port) for n in self.cluster.nodes_in_cluster]))
            return False

        # Verify master was updated correctly
        if self.cluster.master.ip == target_ip:
            self.log.debug("Successfully updated cluster.master to upgraded node {}:{}"
                           .format(self.cluster.master.ip, self.cluster.master.port))
            return True
        else:
            self.log.error("Master update failed. Master IP: {}, Expected: {}"
                           .format(self.cluster.master.ip, target_ip))
            return False

    def _remove_node_from_cluster(self, node_to_remove):
        """
        Remove node from cluster and rebalance
        Steps 4 and 5: Remove node + Rebalance

        Parameters:
            node_to_remove: TestInputServer object - node to remove from cluster

        Returns:
            bool: True if node was successfully removed, False otherwise
        """
        self.log.info(
            f"Step 1: Removing node from cluster: {node_to_remove.ip}")

        # Validate node_to_remove is in cluster
        if node_to_remove not in self.cluster.nodes_in_cluster:
            self.log.warn("Node {} is not in cluster.nodes_in_cluster, skipping removal"
                          .format(node_to_remove.ip))
            return True

        # Get all OTP nodes
        nodes = self.cluster_util.get_otp_nodes(self.cluster.master)

        # Find OTP ID of node to remove
        ejected_nodes = []
        use_hostnames = getattr(self.task, 'use_hostnames', False) if hasattr(
            self, 'task') else False
        for node in nodes:
            if ClusterRun.is_enabled:
                if int(node_to_remove.port) == int(node.port):
                    ejected_nodes.append(node.id)
                    self.log.debug(
                        "Node to remove {} -> OTP ID: {}".format(node_to_remove.ip, node.id))
            else:
                if use_hostnames:
                    if hasattr(node_to_remove, 'hostname') and node_to_remove.hostname == node.ip \
                            and int(node_to_remove.port) == int(node.port):
                        ejected_nodes.append(node.id)
                        self.log.debug(
                            "Node to remove {} -> OTP ID: {}".format(node_to_remove.ip, node.id))
                elif node_to_remove.ip == node.ip and int(node_to_remove.port) == int(node.port):
                    ejected_nodes.append(node.id)
                    self.log.debug(
                        "Node to remove {} -> OTP ID: {}".format(node_to_remove.ip, node.id))

        if not ejected_nodes:
            self.log.error(
                "Could not find OTP ID for node to remove: {}".format(node_to_remove.ip))
            return False

        # Step 5: Rebalance with ejected node
        self.log.info(
            f"Step 2: Rebalancing cluster to remove node: {node_to_remove.ip}")

        if not self._rebalance_cluster_manually(eject_nodes=ejected_nodes):
            self.log.error(
                "Rebalance failed while removing node {}".format(node_to_remove.ip))
            return False

        # Update nodes_in_cluster - remove the old node
        if node_to_remove in self.cluster.nodes_in_cluster:
            self.cluster.nodes_in_cluster.remove(node_to_remove)
            self.log.debug(
                "Removed {} from cluster.nodes_in_cluster".format(node_to_remove.ip))

        # Verify node is no longer in cluster
        nodes_after = self.cluster_util.get_otp_nodes(self.cluster.master)
        node_found = False
        for node in nodes_after:
            if node.id in ejected_nodes:
                node_found = True
                break

        if node_found:
            self.log.warn(
                "Node {} still appears in cluster OTP nodes".format(node_to_remove.ip))
        else:
            self.log.debug(
                "Node {} successfully removed from cluster".format(node_to_remove.ip))

        return True

    def _swap_rebalance_node(self, node_to_remove, node_to_add):
        """
        Perform swap rebalance: remove one node, add another
        Steps: add node (includes rebalance), remove node (includes rebalance), update master

        Parameters:
            node_to_remove: TestInputServer object - must be in cluster.nodes_in_cluster
            node_to_add: TestInputServer object - should NOT be in cluster.nodes_in_cluster (spare node)
        """
        # Step 1: Add node and rebalance (handled by _add_node_to_cluster)
        if not self._add_node_to_cluster(node_to_add):
            self.fail("Failed to add node {} to cluster".format(node_to_add.ip))

        # Step 2: Remove node and rebalance (handled by _remove_node_from_cluster)
        if not self._remove_node_from_cluster(node_to_remove):
            self.fail("Failed to remove node {} from cluster".format(
                node_to_remove.ip))

        # # Add the new node to nodes_in_cluster
        # if node_to_add not in self.cluster.nodes_in_cluster:
        #     self.cluster.nodes_in_cluster.append(node_to_add)
        #     self.log.info("Added {} to cluster.nodes_in_cluster".format(node_to_add.ip))

        # Step 3: Update cluster.master to upgraded node
        if not self._update_master_node(node_to_add):
            self.log.warn("Master update may have failed, but continuing...")

        self.log.info("Swap rebalance completed: {} removed, {} added"
                      .format(node_to_remove.ip, node_to_add.ip))
        self.log.info("Master node after swap: {}".format(
            self.cluster.master.ip))
        self.cluster_util.print_cluster_stats(self.cluster)

    def _upgrade_and_prepare_node(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            # Step 0: Uninstall existing Enterprise Analytics before upgrade
            self.log.info(
                "Step 0/5: Uninstalling existing Enterprise Analytics...")
            self._uninstall_enterprise_analytics(shell)

            # Step 1: Get build URL
            self.log.info("Step 1/5: Getting build URL...")
            build_url, package_name = self._get_build_url(self.upgrade_version)

            # Step 2: Download build
            self.log.info("Step 2/5: Downloading build...")
            package_path = self._download_build(shell, build_url, package_name)

            # Step 3: Install new version
            self.log.info("Step 3/5: Installing new version...")
            self._install_enterprise_analytics(shell, package_path)

            # Step 4: Start service
            self.log.info("Step 4/5: Starting service...")
            self._start_enterprise_analytics(shell)

        finally:
            shell.disconnect()

        self.sleep(30, "Wait after installation on node {}".format(node.ip))

        # Verify node is running
        self.log.debug(
            "Verifying service is running on node {}...".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        try:
            if not self._is_service_running(shell, max_wait=60):
                self.log.error(
                    "Node {} service not running after upgrade".format(node.ip))
                self.fail(
                    "Node {} service not running after upgrade".format(node.ip))
        finally:
            shell.disconnect()

        # Verify REST API is accessible
        self.log.debug(
            "Verifying REST API is accessible on node {}...".format(node.ip))
        is_running = self.cluster_util.is_ns_server_running(node, 30)
        self.assertTrue(
            is_running,
            "Node {} REST API not accessible after upgrade".format(node.ip))

        # Step 5: Initialize cluster after upgrade
        self.log.info("Step 5/5: Initializing cluster after upgrade...")
        self._initialize_cluster_for_upgrade(node)

        self.log.info("Node {} upgraded and initialized successfully to version {}"
                      .format(node.ip, self.upgrade_version))

    def test_swap_rebalance_upgrade(self):
        """
        Test swap rebalance upgrade from 2.0.0-1069 to 2.1.0:
        Setup: 2-node cluster + 1 spare node
        Iteration pattern:
        1. Pick non-master node from cluster
        2. Upgrade spare node
        3. Swap rebalance: remove non-master, add upgraded spare
        4. Update cluster.master to upgraded node (orchestrator is auto-managed by Couchbase)
        5. Repeat for next node in cluster
        """
        self.log.info("Starting swap rebalance upgrade test")

        # Verify initial setup: nodes_init nodes in cluster
        self.assertEqual(
            len(self.cluster.nodes_in_cluster), self.nodes_init,
            "Expected {} nodes in cluster, found {}"
            .format(self.nodes_init, len(self.cluster.nodes_in_cluster)))
        self.assertEqual(
            self.spare_node, self.cluster.servers[self.nodes_init],
            "Expected spare node to be the node at index {}, found {}"
            .format(self.nodes_init, self.spare_node.ip))

        # initail cluster state
        self.log.info("=" * 60)
        self.log.info("Initial cluster state")
        self.log.info("=" * 60)
        self.cluster_util.print_cluster_stats(self.cluster)

        nodes_to_upgrade = list(self.cluster.nodes_in_cluster)
        spare_node = self.spare_node
        iteration = 1
        while nodes_to_upgrade:
            self.log.info("=" * 60)
            self.log.info("Iteration {}".format(iteration))
            self.log.info("=" * 60)
            self.log.info("Nodes in cluster: {}".format(
                [n.ip for n in nodes_to_upgrade]))
            self.log.info("Current master node: {}:{}".format(
                self.cluster.master.ip, self.cluster.master.port))
            self.log.info("Spare node: {}".format(spare_node.ip))

            # Step 1: Pick a node to upgrade (non-master node on older version)
            currect_node_to_upgrade = self._get_non_master_node_with_older_version(
                nodes_to_upgrade)
            if not currect_node_to_upgrade and not nodes_to_upgrade:
                # No node to upgrade and nodes_to_upgrade is empty - upgrade is complete
                self.log.info("=" * 60)
                self.log.info(
                    "No non-master nodes on older version found and nodes_to_upgrade is empty. Upgrade completed!")
                self.log.info("=" * 60)
                break

            self.log.info("Selected node to upgrade: {} (non-master: {})"
                          .format(currect_node_to_upgrade.ip,
                                  currect_node_to_upgrade.ip != self.cluster.master.ip))

            # Step 2: Upgrade spare node
            self.log.info("Upgrading spare node {} to version {}"
                          .format(spare_node.ip, self.upgrade_version))
            self._upgrade_and_prepare_node(spare_node)

            # Step 3: Swap rebalance: Swap currect_node_to_upgrade <-> spare_node ; Rebalance
            self.log.info("Swap rebalance: removing {} and adding {}"
                          .format(currect_node_to_upgrade.ip, spare_node.ip))
            self._swap_rebalance_node(currect_node_to_upgrade, spare_node)

            # Step 4: Remove currect_node_to_upgrade from nodes_to_upgrade and make it the new spare
            if currect_node_to_upgrade in nodes_to_upgrade:
                nodes_to_upgrade.remove(currect_node_to_upgrade)
            spare_node = currect_node_to_upgrade

            self.log.info("=" * 60)
            self.log.info("Iteration {} completed. Remaining nodes to upgrade: {}"
                          .format(iteration, [n.ip for n in nodes_to_upgrade]))
            self.log.info("=" * 60)
            iteration += 1

        # Verify final state
        self.log.info("=" * 60)
        self.log.info("Final cluster state")
        self.log.info("=" * 60)
        self.cluster_util.print_cluster_stats(self.cluster)

        # Verify all nodes in cluster are running upgraded version
        self.log.info("Verifying node versions in final cluster")
        for node in self.cluster.nodes_in_cluster:
            _, node_info = ClusterRestAPI(node).node_details()
            node_version = node_info.get("version", "")
            if self.upgrade_version in node_version:
                self.log.info("✓ Node {} is on upgraded version {}"
                              .format(node.ip, node_version))
            else:
                self.log.warn("Node {} version {} does not contain {}"
                              .format(node.ip, node_version, self.upgrade_version))

        self.log.info("=" * 60)
        self.log.info("Swap rebalance upgrade test completed successfully")
        final_nodes = [n.ip for n in self.cluster.nodes_in_cluster]
        self.log.info("Final cluster nodes: {}"
                      .format(", ".join(final_nodes)))
        self.log.info("=" * 60)
