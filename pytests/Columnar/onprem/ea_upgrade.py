"""
Created on 19-Nov-2025

@author: himanshu.jain@couchbase.com

Enterprise Analytics Upgrade Test
Setup: 2-node cluster (node1 and node2) on version 2.0.0-1069, node3 as spare
Swap Rebalance:
1. Upgrade node3 to 2.1; Swap node1 ↔ node3; Rebalance
2. Upgrade node1 to 2.1; Swap node2 ↔ node1; Rebalance
Result: node1 and node3 cluster with upgraded versions

Failover Upgrade Test
Setup: 3-node cluster, no spare node needed
Failover:
1. Failover node (settle-rebalances it out); Upgrade node in place;
   Add it back in + Rebalance
2. Repeat for each remaining node (master rerouted before it's failed over)
Result: same 3 nodes, all on the upgraded version

Offline Upgrade Test
Setup: 3-node cluster, no spare node needed
Auto-failover is disabled for the whole upgrade:
1. Upgrade each node in place (stop, remove old package, install new build
   without starting, restore its saved state, start); the node rejoins the
   cluster by itself - no failover, rebalance or add-node needed
2. Verify all nodes are active + healthy and the cluster is balanced
3. Re-enable auto-failover once every node is upgraded
Result: same 3 nodes, all on the upgraded version
"""

from pytests.Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from cb_server_rest_util.analytics.analytics_settings import \
    LEGACY_ANALYTICS_SETTINGS_PATH
from platform_utils.ssh_util.shell_util.remote_connection import RemoteMachineShellConnection
from cb_constants.CBServer import CbServer
from cb_constants.ClusterRun import ClusterRun
from awsLib.S3 import S3
from custom_exceptions.exception import RebalanceFailedException
from TestInput import TestInputSingleton
from global_vars import logger
from common_lib import sleep as common_sleep
import time
import os


class EnterpriseAnalyticsUpgrade(ColumnarOnPremBase):
    """
    Test class for Enterprise Analytics upgrade from 2.0 to 2.1
    using swap rebalance
    """

    EA_DOWNLOAD_SERVER = "latestbuilds.service.couchbase.com"
    EA_VERSION_MAP = {
        "2.1": "phoenix",  # Map version to build path
        "2.2": "lumina",
        "3.0": "helios"
    }

    # The product was renamed from "Enterprise Analytics" to "Operational
    # Insights" starting with the 3.0 release - download path, package
    # name prefix, install dir, and service/launcher name all changed
    # together. Versions before 3.0 (e.g. 2.1, 2.2) keep the old naming;
    # 3.0+ uses the new one. See _get_product_info().
    PRODUCT_RENAME_MAIN_VERSION = (3, 0)
    PRODUCT_INFO = {
        "old": {
            "url_path": "builds/latestbuilds/enterprise-analytics",
            "package_prefix": "enterprise-analytics",
            "service_name": "enterprise-analytics",
            "install_dir": "/opt/enterprise-analytics",
        },
        "new": {
            "url_path": "builds/latestbuilds/operational-insights",
            "package_prefix": "operational-insights",
            "service_name": "operational-insights",
            "install_dir": "/opt/couchbase",
        },
    }

    EA_DOWNLOAD_DIR = "/tmp"

    def _get_product_info(self, version):
        """
        Returns the PRODUCT_INFO entry (url path, package prefix, service
        name, install dir) matching the given version string (e.g.
        "3.0.0-1010", "2.2.1-1404"), based on the Enterprise Analytics ->
        Operational Insights rename that shipped in 3.0.
        """
        main_version = version.split("-", 1)[0]
        main_version = tuple(
            int(part) for part in main_version.split(".")[:2])
        if main_version >= self.PRODUCT_RENAME_MAIN_VERSION:
            return self.PRODUCT_INFO["new"]
        return self.PRODUCT_INFO["old"]

    @staticmethod
    def _get_uninstall_commands(product_info):
        service_name = product_info["service_name"]
        install_dir = product_info["install_dir"]
        package_prefix = product_info["package_prefix"]
        return [
            ("rm -rf /tmp/tmp* ; rm -rf /tmp/cbbackupmgr-staging; rm -rf /tmp/entbackup* || true",
             0, "Clean up temporary directories"),
            ("systemctl -q stop {}.service || true".format(service_name),
             0, "Stop {} service".format(service_name)),
            ("umount -a -t nfs,nfs4 -f -l || true",
             0, "Unmount NFS mounts"),
            ("service ntp restart || true",
             0, "Restart NTP service"),
            ("systemctl unmask {}.service || true".format(service_name),
             0, "Unmask {} service".format(service_name)),
            ("dpkg -l | grep {} || echo 'not_installed'".format(package_prefix),
             0, "Check current installation status"),
            ("apt-get purge -y '{}*' > /dev/null 2>&1 || true".format(package_prefix),
             10, "Purge {} packages using apt-get".format(package_prefix)),
            ("dpkg --purge $(dpkg -l | grep {} | awk '{{print $2}}' | xargs echo) 2>&1 || true".format(package_prefix),
             10, "Additional cleanup using dpkg --purge"),
            ("rm -f /var/lib/dpkg/info/{}* || true".format(package_prefix),
             10, "Remove dpkg info files"),
            ("ps -ef | egrep {} || echo 'no_processes'".format(package_prefix),
             0, "Check {} processes".format(package_prefix)),
            ("kill -9 `ps -ef | egrep {} | cut -f3 -d' '` 2>&1 || true".format(package_prefix),
             0, "Kill remaining {} processes".format(package_prefix)),
            ("rm -rf {} > /dev/null 2>&1 && echo 1 || echo 0".format(install_dir),
             0, "Remove install directory"),
            ("rm -rf {}/* > /dev/null 2>&1 && echo 1 || echo 0".format(
                EnterpriseAnalyticsUpgrade.EA_DOWNLOAD_DIR),
             0, "Remove download directory"),
            ("dpkg -P {} 2>&1 || true".format(service_name),
             0, "Explicit dpkg purge"),
            ("rm -rf /var/lib/dpkg/info/{}* || true".format(package_prefix),
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
            ("rm -rf {} || true".format(install_dir),
             0, "Final cleanup of install directory"),
            ("dpkg -l | grep {} || echo 'not_installed'".format(package_prefix),
             0, "Verify uninstallation"),
        ]

    def setUp(self):
        # Minimal bootstrap (input/log/sleep) so the pre-cluster-formation
        # reinstall below can run: CouchbaseBaseTest.setUp() (invoked via
        # super().setUp() further down) assigns these same three
        # identically a moment later, so this is a harmless early peek at
        # framework state, not a second, divergent initialization path.
        self.input = TestInputSingleton.input
        self.log = logger.get("test")
        self.sleep = common_sleep

        self.upgrade_version = self.input.param(
            "post_upgrade_version", "2.1.0-1367")
        self.pre_upgrade_version = self.input.param(
            "pre_upgrade_version", "2.0.0-1069")
        self.nodes_init = self.input.param("nodes_init", 2)
        self.reset_to_pre_upgrade_version = self.input.param(
            "reset_to_pre_upgrade_version", True)

        if self.reset_to_pre_upgrade_version:
            # Reinstall the initial nodes_init servers with
            # pre_upgrade_version BEFORE OnPremBaseTest/ClusterSetup
            # (invoked by super().setUp() below) clusters them on
            # whatever version test_infra_runner actually installed -
            # see _reset_servers_to_pre_upgrade_version().
            self._reset_servers_to_pre_upgrade_version(
                self.input.servers[:self.nodes_init], self.pre_upgrade_version)

        self.analytics_settings_path = LEGACY_ANALYTICS_SETTINGS_PATH
        super(EnterpriseAnalyticsUpgrade, self).setUp()

        # super().setUp() re-reads/overwrites some of the params above
        # with its own (different) defaults - re-assert ours.
        self.upgrade_version = self.input.param(
            "post_upgrade_version", "2.1.0-1367")
        self.pre_upgrade_version = self.input.param(
            "pre_upgrade_version", "2.0.0-1069")
        self.nodes_init = self.input.param("nodes_init", 2)
        # Not every test provisions a spare node (e.g. failover-based
        # upgrade needs none) - guard against IndexError in that case.
        if len(self.cluster.servers) > self.nodes_init:
            self.spare_node = self.cluster.servers[self.nodes_init]
        else:
            self.spare_node = None

        # COPY INTO / ANALYZE COLLECTION params, shared by both tests
        self.copy_into_path_pre_upgrade = self.input.param(
            "copy_into_path_pre_upgrade",
            "level_1_folder_1/level_2_folder_1/level_3_folder_1")
        self.copy_into_path_post_upgrade = self.input.param(
            "copy_into_path_post_upgrade", "level_1_folder_1")
        self.analyze_sample_size = self.input.param(
            "analyze_sample_size", "high")
        self.analyze_sample_seed = self.input.param(
            "analyze_sample_seed", 1000)
        self.post_upgrade_retry_count = self.input.param(
            "post_upgrade_retry_count", 3)

        # Track buckets created during upgrade for cleanup
        self.created_buckets = []  # List of (bucket_name, s3_obj) tuples
        # Standalone collection used to validate that ANALYZE COLLECTION
        # sample statistics survive the upgrade. Populated by
        # _create_ea_upgrade_infra().
        self.analyze_collection_name = None
        self.analyze_collection_full_name = None

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

        product_info = self._get_product_info(original_version)
        self.log.debug("Product info for version {}: {}".format(
            original_version, product_info))

        package_name = "{}_{}-{}-linux_{}.deb".format(
            product_info["package_prefix"], version, build_number, arch_suffix)
        self.log.debug("Package name: {}".format(package_name))

        url = "https://{}/{}/{}/{}/{}".format(
            self.EA_DOWNLOAD_SERVER,
            product_info["url_path"],
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

    def _uninstall_enterprise_analytics(self, shell, product_info):
        # Execute commands in sequence
        for cmd, sleep_seconds, description in self._get_uninstall_commands(
                product_info):
            self.log.debug("{}...".format(description))
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "{} - output: {}, error: {}".format(description, output, error))

            # Sleep if specified
            if sleep_seconds > 0:
                self.sleep(sleep_seconds, "Wait after {}".format(description))
        self.log.debug("Uninstallation completed")

    def _install_enterprise_analytics(self, shell, package_path, product_info,
                                      start_server=True):
        """Install Enterprise Analytics / Operational Insights from deb package"""
        env_prefix = "" if start_server else "INSTALL_DONT_START_SERVER=1 "

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
        cmd = "{}DEBIAN_FRONTEND='noninteractive' apt-get -y -f install {} > /dev/null 2>&1 && echo 1 || echo 0".format(
            env_prefix, package_path)
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
            cmd = "{}dpkg -i {} 2>&1 || true".format(env_prefix, package_path)
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
        cmd = "dpkg -l | grep {}".format(product_info["package_prefix"])
        output, error = shell.execute_command(cmd)
        if not output or len(output) == 0:
            self.log.error(
                "{} installation verification failed - no packages found"
                .format(product_info["package_prefix"]))
            self.fail("{} installation verification failed"
                      .format(product_info["package_prefix"]))

        self.log.info("Installation completed successfully")

    def _start_enterprise_analytics(self, shell, product_info):
        """Start the Enterprise Analytics / Operational Insights service"""
        service_name = product_info["service_name"]

        # Check service status before starting
        self.log.debug("Checking service status before starting...")
        cmd = "systemctl status {}.service || true".format(service_name)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Initial service status - output: {}, error: {}".format(output, error))

        # Unmask the service first (in case it was masked during uninstall)
        self.log.debug("Unmasking {} service...".format(service_name))
        cmd = "systemctl unmask {}.service || true".format(service_name)
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
        self.log.debug("Enabling {} service...".format(service_name))
        cmd = "systemctl enable {}.service || true".format(service_name)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Enable service - output: {}, error: {}".format(output, error))

        # Start the service
        self.log.debug("Starting {} service...".format(service_name))
        cmd = "systemctl start {}.service".format(service_name)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Start service - output: {}, error: {}".format(output, error))

        if error and len(error) > 0:
            self.log.error("Error starting service: {}".format(error))
            # Check service status
            self.log.debug("Checking detailed service status after error...")
            cmd = "systemctl status {}.service".format(service_name)
            output, error = shell.execute_command(cmd)
            self.log.debug("Service status details: {}".format(output))

            # Retry starting
            self.log.debug("Retrying service start...")
            cmd = "systemctl start {}.service".format(service_name)
            output, error = shell.execute_command(cmd)
            self.log.debug(
                "Retry start - output: {}, error: {}".format(output, error))

        # Check if service is active
        cmd = "systemctl is-active {}.service".format(service_name)
        output, error = shell.execute_command(cmd)
        self.log.debug(
            "Service active check - output: {}, error: {}".format(output, error))

        # Wait a bit for service to start
        self.sleep(10, "Wait for service to start")

        cmd = "systemctl status {}.service --no-pager | head -10".format(
            service_name)
        output, error = shell.execute_command(cmd)
        self.log.debug("Final service status: {}".format(output))

    def _is_service_running(self, shell, product_info, max_wait=60):
        """Check if the Enterprise Analytics / Operational Insights service is running"""
        service_name = product_info["service_name"]
        self.log.info(
            "Checking if {} service is running (max_wait={}s)...".format(
                service_name, max_wait))
        for i in range(max_wait // 5):
            attempt = i + 1
            self.log.debug(
                "Service check attempt {}/{}...".format(attempt, max_wait // 5))
            cmd = "systemctl is-active {}.service".format(service_name)
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

    def _initialize_cluster_for_upgrade(self, node, target_version=None):
        """
        Initialize cluster for upgraded Enterprise Analytics node
        This configures compute storage and initializes the node
        """
        target_version = target_version or self.upgrade_version
        self.log.debug(
            "Initializing cluster for upgraded node: {} (version: {})"
            .format(node.ip, target_version))

        # Configure compute storage if needed (creating new bucket for upgrade)
        if hasattr(self, 'analytics_compute_storage_separation') and self.analytics_compute_storage_separation:
            self.log.debug(
                "Configuring compute storage for node {}...".format(node.ip))
            # Create new bucket for upgrade process
            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", None)
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
            aws_session_token = os.getenv("AWS_SESSION_TOKEN", None)
            aws_bucket_region = self.input.param("aws_region", "us-east-1")
            aws_endpoint = self.input.param("aws_endpoint", None)

            if not aws_access_key or not aws_secret_key or not aws_session_token:
                self.log.error(
                    "Missing compute storage configuration parameters")
                self.fail(
                    "Cannot setup compute storage: missing AWS credentials")

            # Create new S3 bucket for upgrade
            columnar_s3_obj = S3(aws_access_key, aws_secret_key,
                                 aws_session_token,
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
                aws_bucket_region=aws_bucket_region,
                path=getattr(self, "analytics_settings_path",
                             LEGACY_ANALYTICS_SETTINGS_PATH))

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

    def _get_prod_compat_version(self, node=None):
        """
        Retrieve prodCompatVersion for every node in the cluster via GET /pools/default.
        Uses cluster_details() which is already present in ClusterRestAPI.

        Returns:
            dict: {hostname: prodCompatVersion} for every node, or None on failure.
        """
        if node is None:
            node = self.cluster.master
        rest = ClusterRestAPI(node)
        status, content = rest.cluster_details()
        if not status or not content:
            self.log.error(
                "Failed to get cluster details from node {}".format(node.ip))
            return None
        nodes = content.get("nodes", [])
        if not nodes:
            self.log.error("No nodes found in cluster details response")
            return None
        versions = {}
        for node_info in nodes:
            hostname = node_info.get("hostname", "unknown")
            prod_compat = node_info.get("prodCompatVersion", "unknown")
            versions[hostname] = prod_compat
        return versions

    def _validate_prod_compat_version(self, expected_version, label="", node=None):
        """
        Assert that every node in the cluster reports the expected prodCompatVersion.

        Parameters:
            expected_version: str  - e.g. "2.1.0" (build number must be stripped)
            label:            str  - prefix used in log lines for easier grepping
            node:             server object to query (defaults to cluster.master)

        Returns:
            bool: True when every node matches, False otherwise.
        """
        if node is None:
            node = self.cluster.master
        versions = self._get_prod_compat_version(node)
        if versions is None:
            self.log.error(
                "[{}] Failed to retrieve prodCompatVersion".format(label))
            return False
        self.log.info(
            "[{}] prodCompatVersion per node:".format(label))
        all_match = True
        for hostname, version in versions.items():
            match = (version == expected_version)
            marker = "✓" if match else "✗"
            self.log.info("  {} {} -> {} (expected: {})".format(
                marker, hostname, version, expected_version))
            if not match:
                all_match = False
        if all_match:
            self.log.info(
                "[{}] All nodes are on prodCompatVersion {}".format(
                    label, expected_version))
        else:
            self.log.warn(
                "[{}] One or more nodes are NOT on prodCompatVersion {}".format(
                    label, expected_version))
        return all_match

    def _create_ea_upgrade_infra(self):
        """
        Creates (via a columnar spec) a standalone collection and an S3
        external link to self.s3_source_bucket, used to verify ANALYZE
        COLLECTION (and its SAMPLE statistics) across the upgrade.
        Data is loaded separately via COPY INTO (see
        _copy_into_analyze_collection), not by direct upsert.
        """
        # Fail fast with a clear message instead of silently creating an
        # S3 link with blank credentials, which later manifests as a
        # confusing "COPY INTO succeeded but ingested 0 docs" symptom
        # (0 matched files from a broken/anonymous link is not an error).
        if not self.aws_access_key or not self.aws_secret_key:
            self.fail(
                "AWS credentials not found (AWS_ACCESS_KEY_ID / "
                "AWS_SECRET_ACCESS_KEY env vars are empty/unset) - "
                "required to create the S3 external link used to load "
                "ANALYZE COLLECTION test data from {}".format(
                    self.s3_source_bucket))

        self.log.info("Creating standalone collection for ANALYZE COLLECTION")
        self.input.test_params["num_external_links"] = "1"
        columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec("full_template"))
        columnar_spec["standalone_dataset"]["num_of_standalone_coll"] = 1
        columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.cluster, cbas_spec=columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False)
        if not result:
            self.fail(msg)

        standalone_collection = self.cbas_util.get_all_dataset_objs("standalone")[
            0]
        self.analyze_collection_name = standalone_collection.name
        self.analyze_collection_full_name = standalone_collection.full_name

        # S3 external link created above via the spec, used by COPY INTO
        # to load/upsert sample data from self.s3_source_bucket into the
        # standalone collection created above.
        external_link = self.cbas_util.get_all_link_objs("s3")[0]
        self.analyze_collection_link_name = external_link.full_name

    def _copy_into_analyze_collection(self, path):
        """
        Runs COPY INTO command
        """
        cmd = self.cbas_util.generate_copy_from_cmd(
            self.analyze_collection_name, self.s3_source_bucket,
            self.analyze_collection_link_name, "Default", "Default",
            files_to_include=["*/file_1.json"], file_format="json",
            path_on_aws_bucket=(path))
        self.log.info(
            "Running COPY INTO on {} from S3 bucket {} via link {}: "
            "{}".format(
                self.analyze_collection_full_name, self.s3_source_bucket,
                self.analyze_collection_link_name, cmd))
        status, metrics, errors, results, _, warnings = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, cmd, timeout=1200, analytics_timeout=1200)
        if warnings:
            self.log.warn("COPY INTO warnings for {}: {}".format(
                self.analyze_collection_full_name, warnings))
        if status != "success":
            self.fail("COPY INTO failed for {}: {}".format(
                self.analyze_collection_full_name, errors))

        doc_count = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cluster, self.analyze_collection_full_name,
            timeout=300, analytics_timeout=300)
        self.log.info("{} now has {} docs after COPY INTO".format(
            self.analyze_collection_full_name, doc_count))
        if doc_count == 0:
            self.fail(
                "COPY INTO on {} ingested 0 docs even though the "
                "statement reported success (0 matched files is not an "
                "error). Check: (1) AWS credentials - aws_access_key is "
                "{}, aws_secret_key is {} (empty/None means "
                "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are not set in "
                "this environment); (2) whether PATH "
                "'level_1_folder_1/level_2_folder_1/level_3_folder_1' + "
                "include '*/file_1.json' still matches a file in S3 "
                "bucket {}. Warnings: {}".format(
                    self.analyze_collection_full_name,
                    "set" if self.aws_access_key else "empty/None",
                    "set" if self.aws_secret_key else "empty/None",
                    self.s3_source_bucket, warnings))

    def _run_analyze_collection(self, sample_size="high", sample_seed=1000,
                                sample_method=None):
        """
        Runs ANALYZE COLLECTION on self.analyze_collection_full_name with
        the given sample settings.
        """
        self.log.info(
            "Running ANALYZE COLLECTION (sample={}, sample_method={}, "
            "sample_seed={}) on {}".format(
                sample_size, sample_method, sample_seed,
                self.analyze_collection_full_name))
        if not self.cbas_util.create_sample_for_analytics_collections(
                self.cluster, self.analyze_collection_full_name,
                sample_size=sample_size, sample_seed=sample_seed,
                sample_method=sample_method, analytics=False):
            self.fail("ANALYZE COLLECTION failed for {}".format(
                self.analyze_collection_full_name))

    def _verify_sample_metadata_index(self, sample_size="high", sample_seed=1000,
                                      sample_method=None):
        """
        Verifies the SAMPLE statistics for self.analyze_collection_name
        are present in Metadata.Index and match the given expected
        sample settings.
        """
        self.log.info(
            "Verifying SAMPLE statistics (sample={}, sample_method={}, "
            "sample_seed={}) are present in Metadata.Index for {}".format(
                sample_size, sample_method, sample_seed,
                self.analyze_collection_name))
        self.sleep(
            10, "Wait for Metadata.Index to be updated after ANALYZE COLLECTION")
        if not self.cbas_util.verify_sample_present_in_Metadata(
                self.cluster, self.analyze_collection_name, "Default",
                sample_method=sample_method, sample_size=sample_size,
                sample_seed=sample_seed):
            self.fail(
                "SAMPLE statistics not found/mismatched in Metadata.Index "
                "for {} (sample={}, sample_method={}, sample_seed={})".format(
                    self.analyze_collection_name, sample_size, sample_method,
                    sample_seed))
        self.log.info("SAMPLE statistics verified in Metadata.Index for {}"
                      .format(self.analyze_collection_name))

    def _install_version_on_node(self, node, target_version):
        """
        Uninstall whatever is currently running on 'node' (detected via
        REST rather than assumed - see callers) and install+start
        target_version on it. Does NOT touch cluster membership/REST
        cluster-init: the node is left factory-fresh, still standalone,
        for the caller to decide what to do with next. Safe to call
        before self.cluster/self.cluster_util exist (e.g. from setUp(),
        before super().setUp() has formed a cluster) - only uses
        self.log/self.fail/self.sleep, all of which setUp() below
        assigns before this can be reached either way.
        """
        _, node_info = ClusterRestAPI(node).node_details()
        current_version = node_info.get("version", self.pre_upgrade_version)
        old_product_info = self._get_product_info(current_version)
        new_product_info = self._get_product_info(target_version)

        shell = RemoteMachineShellConnection(node)
        try:
            # Step 0: Uninstall whatever is currently installed
            self.log.info(
                "Step 0/4: Uninstalling existing Enterprise Analytics "
                "(detected version: {})...".format(current_version))
            self._uninstall_enterprise_analytics(shell, old_product_info)

            # Step 1: Get build URL
            self.log.info("Step 1/4: Getting build URL...")
            build_url, package_name = self._get_build_url(target_version)

            # Step 2: Download build
            self.log.info("Step 2/4: Downloading build...")
            package_path = self._download_build(shell, build_url, package_name)

            # Step 3: Install new version
            self.log.info("Step 3/4: Installing new version...")
            self._install_enterprise_analytics(
                shell, package_path, new_product_info)

            # Step 4: Start service
            self.log.info("Step 4/4: Starting service...")
            self._start_enterprise_analytics(shell, new_product_info)

        finally:
            shell.disconnect()

        self.sleep(30, "Wait after installation on node {}".format(node.ip))

        # Verify node is running
        self.log.debug(
            "Verifying service is running on node {}...".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        try:
            if not self._is_service_running(
                    shell, new_product_info, max_wait=60):
                self.log.error(
                    "Node {} service not running after install".format(node.ip))
                self.fail(
                    "Node {} service not running after install".format(node.ip))
        finally:
            shell.disconnect()

        return new_product_info

    def _upgrade_and_prepare_node(self, node, target_version=None):
        """
        Install target_version (default: self.upgrade_version) on 'node'
        via _install_version_on_node(), then clusterInit it as a fresh
        standalone single-node cluster.
        """
        target_version = target_version or self.upgrade_version
        self._install_version_on_node(node, target_version)

        # Verify REST API is accessible
        self.log.debug(
            "Verifying REST API is accessible on node {}...".format(node.ip))
        is_running = self.cluster_util.is_ns_server_running(node, 30)
        self.assertTrue(
            is_running,
            "Node {} REST API not accessible after upgrade".format(node.ip))

        # Step 5: Initialize cluster after upgrade
        self.log.info("Step 5/5: Initializing cluster after upgrade...")
        self._initialize_cluster_for_upgrade(
            node, target_version=target_version)

        self.log.info("Node {} upgraded and initialized successfully to version {}"
                      .format(node.ip, target_version))

    def _reset_servers_to_pre_upgrade_version(self, servers, pre_upgrade_version):
        """
        Reinstall pre_upgrade_version on each of 'servers' (raw
        TestInputServer objects, not yet part of any TAF cluster object).

        Called from setUp() BEFORE super().setUp() - infra provisioning
        (ini + test_infra_runner) installs whatever version the ini's
        per-node 'version:' field says, which for a regression-suite-
        triggered run is the suite-wide target build, not
        pre_upgrade_version. Doing the reinstall here, before
        OnPremBaseTest/ClusterSetup (invoked via super().setUp() right
        after this returns) ever clusters these nodes, means the base
        class's own cluster-formation logic (compute storage separation,
        clusterInit, rebalance-in) runs exactly once, against nodes
        already on the right starting version - instead of running twice:
        once for a 3.0-build cluster that's immediately thrown away, then
        again after a manual uninstall/reinstall/re-cluster dance.

        Each node is left factory-fresh (uninstalled+reinstalled+started,
        no clusterInit) via _install_version_on_node() - ClusterSetup's
        normal __initial_rebalance()/initialize_cluster() then clusters
        them exactly like it would nodes provisioned correctly to begin
        with.
        """
        self.log.info("=" * 60)
        self.log.info("Resetting {} server(s) to pre_upgrade_version {} "
                      "before initial cluster formation"
                      .format(len(servers), pre_upgrade_version))
        self.log.info("=" * 60)

        for server in servers:
            self.log.info("Reinstalling {} with pre_upgrade_version {}"
                          .format(server.ip, pre_upgrade_version))
            self._install_version_on_node(server, pre_upgrade_version)

        self.log.info(
            "All {} server(s) reset to pre_upgrade_version {}".format(
                len(servers), pre_upgrade_version))

    def _get_otp_id_for_node(self, node):
        """
        Resolve a TestInputServer to its current OTP node id.
        Returns None if the node is not currently known to the cluster.
        """
        use_hostnames = getattr(self.task, 'use_hostnames', False) if hasattr(
            self, 'task') else False
        for otp_node in self.cluster_util.get_otp_nodes(self.cluster.master):
            if ClusterRun.is_enabled:
                if int(node.port) == int(otp_node.port):
                    return otp_node.id
            elif use_hostnames:
                if hasattr(node, 'hostname') and node.hostname == otp_node.ip \
                        and int(node.port) == int(otp_node.port):
                    return otp_node.id
            elif node.ip == otp_node.ip and int(node.port) == int(otp_node.port):
                return otp_node.id
        return None

    def _failover_node(self, node):
        """
        Fail 'node' out of the cluster and wait for it to be fully gone.

        REST sequence mirrors a manually-captured failover (HAR) against
        this product: perform_graceful_failover() - on this product the
        node drops out of pools/default as soon as failover completes, no
        separate recovery/eject step is needed - followed by a plain
        settle rebalance with the remaining known nodes.

        Parameters:
            node: TestInputServer object - must be in cluster.nodes_in_cluster

        Returns:
            bool: True if the node was failed over and the cluster settled
        """
        otp_id = self._get_otp_id_for_node(node)
        if not otp_id:
            self.log.error(
                "Could not find OTP id for node to fail over: {}".format(node.ip))
            return False

        rest = ClusterRestAPI(self.cluster.master)
        self.log.info("Gracefully failing over node {} ({})"
                      .format(node.ip, otp_id))
        status, content = rest.perform_graceful_failover([otp_id])
        if not status:
            self.log.error(
                "Failover of {} failed: {}".format(node.ip, content))
            return False

        # Failover runs as a short rebalance-style task internally - wait
        # for it the same way _rebalance_cluster_manually waits for an
        # ordinary rebalance.
        try:
            failover_completed = self.cluster_util.rebalance_reached(
                self.cluster,
                percentage=100,
                wait_step=5,
                num_retry=240,
                validate_bucket_ranking=False)
        except RebalanceFailedException as e:
            self.log.error(
                "Failover of {} did not complete: {}".format(node.ip, str(e)))
            self._collect_cbcollect_logs_on_failure()
            raise

        if not failover_completed:
            self.log.error(
                "Failover of node {} did not complete successfully".format(node.ip))
            self._collect_cbcollect_logs_on_failure()
            return False

        self.log.info("Node {} failed over successfully".format(node.ip))
        self.cluster_util.update_cluster_nodes_service_list(
            self.cluster, inactive_added=True, inactive_failed=True)

        if node in self.cluster.nodes_in_cluster:
            self.cluster.nodes_in_cluster.remove(node)

        # Settle rebalance, matching the captured flow (plain rebalance
        # over the remaining known nodes, no eject/recovery needed).
        if not self._rebalance_cluster_manually(eject_nodes=None):
            self.log.error(
                "Settle rebalance failed after failing over {}".format(node.ip))
            return False

        return True

    def _set_auto_failover(self, settings=None):
        """
        Disable (settings=None) or restore (settings=<dict returned by an
        earlier call>) auto-failover on the cluster, and verify the change
        took effect.

        Returns:
            dict: the auto-failover settings that were in effect before this
            call, to be passed back in later to restore them.
        """
        rest = ClusterRestAPI(self.cluster.master)
        status, previous = rest.get_auto_failover_settings()
        self.assertTrue(
            status, "Failed to read auto-failover settings: {}".format(previous))

        if settings is None:
            enabled = False
            timeout = previous.get("timeout", 120)
            max_count = None
        else:
            enabled = settings.get("enabled", True)
            timeout = settings.get("timeout", 120)
            max_count = settings.get("maxCount")

        self.log.info("Setting auto-failover enabled={} (timeout={})"
                      .format(enabled, timeout))
        status, content = rest.update_auto_failover_settings(
            enabled="true" if enabled else "false",
            timeout=timeout,
            max_count=max_count)
        self.assertTrue(
            status, "Failed to update auto-failover settings: {}".format(content))

        status, updated = rest.get_auto_failover_settings()
        self.assertTrue(
            status and updated.get("enabled") == enabled,
            "Auto-failover enabled expected to be {}, got: {}"
            .format(enabled, updated))
        return previous

    def _verify_cluster_nodes_active_healthy(self, timeout=300):
        """
        Wait until every node in cluster.nodes_in_cluster is reported by
        /pools/default as clusterMembership=active and status=healthy, and
        the cluster reports balanced=true. Fails the test on timeout.
        """
        rest = ClusterRestAPI(self.cluster.master)
        expected_nodes = len(self.cluster.nodes_in_cluster)
        end_time = time.time() + timeout
        summary = None
        while True:
            status, details = rest.cluster_details()
            if status:
                nodes = details.get("nodes", [])
                summary = [(n.get("hostname"), n.get("clusterMembership"),
                            n.get("status")) for n in nodes]
                all_active_healthy = len(nodes) == expected_nodes and all(
                    n.get("clusterMembership") == "active"
                    and n.get("status") == "healthy" for n in nodes)
                if all_active_healthy and details.get("balanced", False):
                    self.log.info(
                        "All {} nodes are active and healthy, cluster is "
                        "balanced: {}".format(expected_nodes, summary))
                    return
            if time.time() >= end_time:
                break
            self.sleep(5, "Wait for all nodes to be active and healthy")
        self.fail("Cluster did not become active/healthy/balanced within {}s "
                  "(expected {} nodes), last seen (host, membership, status): "
                  "{}".format(timeout, expected_nodes, summary))

    def _offline_upgrade_node(self, node):
        """
        Upgrade 'node' in place, keeping its cluster identity and data:
        back up its state (config, excluding data/logs), remove the old
        package, install the new build without starting it, restore the
        state, and start the service. The node then rejoins the cluster by
        itself - no failover/rebalance/add-node is involved.
        """
        _, node_info = ClusterRestAPI(node).node_details()
        current_version = node_info.get("version") or self.pre_upgrade_version
        node_uuid = node_info.get("nodeUUID")
        data_path = node_info["storage"]["hdd"][0]["path"]
        old_product_info = self._get_product_info(current_version)
        new_product_info = self._get_product_info(self.upgrade_version)

        old_var_dir = old_product_info["install_dir"] + "/var"
        state_dir = old_var_dir + "/lib/couchbase"
        backup_dir = "/var/tmp/ea-state-backup"

        shell = RemoteMachineShellConnection(node)

        def run(cmd, description):
            output, error = shell.execute_command(
                cmd + " && echo __ok__ || echo __fail__")
            self.log.debug("{} - output: {}, error: {}"
                           .format(description, output, error))
            if not output or output[-1].strip() != "__ok__":
                self.fail("{} failed on node {}: {}"
                          .format(description, node.ip, output))

        try:
            self.log.info("Step 1/5: Downloading build {} on {}"
                          .format(self.upgrade_version, node.ip))
            build_url, package_name = self._get_build_url(self.upgrade_version)
            package_path = self._download_build(shell, build_url, package_name)

            self.log.info("Step 2/5: Backing up node state to {}"
                          .format(backup_dir))
            run("rm -rf {b} && mkdir -p {b} && tar -C {s} -cf - --exclude=./data "
                "--exclude=./logs . | tar -C {b} -xf - && "
                "test -f {b}/config/config.dat"
                .format(b=backup_dir, s=state_dir), "Backup node state")

            self.log.info("Step 3/5: Removing old package {}"
                          .format(old_product_info["package_prefix"]))
            shell.execute_command("systemctl stop {}.service || true"
                                  .format(old_product_info["service_name"]))
            run("DEBIAN_FRONTEND=noninteractive apt-get remove -y {} "
                "> /dev/null 2>&1".format(old_product_info["package_prefix"]),
                "Remove old package")

            self.log.info("Step 4/5: Installing {} without starting it"
                          .format(self.upgrade_version))
            self._install_enterprise_analytics(
                shell, package_path, new_product_info, start_server=False)

            self.log.info(
                "Step 5/5: Restoring node state and starting service")
            run("mkdir -p {s} && tar -C {b} -cf - . | tar -C {s} -xf - && "
                "chown -R couchbase:couchbase {v}"
                .format(b=backup_dir, s=state_dir, v=old_var_dir),
                "Restore node state")
            if new_product_info["install_dir"] != old_product_info["install_dir"]:
                run("rm -rf {n}/var && ln -s {v} {n}/var"
                    .format(n=new_product_info["install_dir"], v=old_var_dir),
                    "Link new install var dir to the restored state")
            self._start_enterprise_analytics(shell, new_product_info)
            if not self._is_service_running(
                    shell, new_product_info, max_wait=60):
                self.fail("Node {} service not running after upgrade"
                          .format(node.ip))
            run("test -d {}".format(data_path),
                "Verify data dir {} is still present".format(data_path))
        finally:
            shell.disconnect()

        self.assertTrue(
            self.cluster_util.is_ns_server_running(node, 120),
            "Node {} REST API not accessible after upgrade".format(node.ip))
        _, new_node_info = ClusterRestAPI(node).node_details()
        self.assertEqual(
            new_node_info.get("nodeUUID"), node_uuid,
            "nodeUUID of {} changed across upgrade".format(node.ip))
        new_version = new_node_info.get("version", "")
        self.assertIn(
            self.upgrade_version, new_version,
            "Node {} reports version {}, expected {}"
            .format(node.ip, new_version, self.upgrade_version))
        new_data_path = new_node_info["storage"]["hdd"][0]["path"]
        self.assertEqual(
            new_data_path, data_path,
            "Data path of {} changed across upgrade".format(node.ip))
        self.log.info("Node {} upgraded in place to {}"
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

        # Initial cluster state
        self.log.info("=" * 60)
        self.log.info("Initial cluster state")
        self.log.info("=" * 60)
        self.cluster_util.print_cluster_stats(self.cluster)

        # Validate prodCompatVersion before upgrade
        pre_upgrade_version_base = self.pre_upgrade_version.split("-")[0]
        self._validate_prod_compat_version(
            pre_upgrade_version_base, label="Pre-upgrade")

        # Create infra
        self._create_ea_upgrade_infra()

        # Run COPY INTO to insert data (~10k docs) before upgrade
        self._copy_into_analyze_collection(
            path=self.copy_into_path_pre_upgrade)

        # Run ANALYZE COLLECTION before upgrade
        self._run_analyze_collection(sample_size=self.analyze_sample_size,
                                     sample_seed=self.analyze_sample_seed)
        self._verify_sample_metadata_index(
            sample_size=self.analyze_sample_size, sample_seed=self.analyze_sample_seed)

        nodes_to_upgrade = list(self.cluster.nodes_in_cluster)
        spare_node = self.spare_node
        iteration = 1
        while nodes_to_upgrade:
            self.log.info("=" * 60)
            self.log.info("Iteration {}".format(iteration))
            self.log.info("=" * 60)
            self.log.info("Nodes in cluster: {}".format(
                [n.ip for n in self.cluster.nodes_in_cluster]))
            self.log.info("Remaining nodes in cluster to be upgraded: {}".format(
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
        self.sleep(
            30, "Wait after final swap rebalance to allow cluster to settle")
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

        # Validate prodCompatVersion after upgrade
        post_upgrade_version = self.upgrade_version.split("-")[0]
        prod_compat_valid = self._validate_prod_compat_version(
            post_upgrade_version, label="Post-upgrade")
        self.assertTrue(
            prod_compat_valid,
            "prodCompatVersion validation failed: one or more nodes are not "
            "reporting prodCompatVersion={}".format(post_upgrade_version))

        # Verify pre-upgrade SAMPLE statistics survived the upgrade
        self._verify_sample_metadata_index(
            sample_size=self.analyze_sample_size, sample_seed=self.analyze_sample_seed)

        # Trigger FLUSH
        maxRetry = self.post_upgrade_retry_count
        retry = 0
        while (retry < maxRetry):
            try:
                # Run COPY INTO to upsert data post-upgrade, before
                # re-running ANALYZE COLLECTION, so the re-analyze has
                # fresh data
                self.log.info(
                    "Running COPY INTO to upsert data post-upgrade (retry {}/{})".format(retry + 1, maxRetry))
                self._copy_into_analyze_collection(
                    path=self.copy_into_path_post_upgrade)

                # Re-run ANALYZE COLLECTION post-upgrade with
                # sample-method=random and verify it took effect
                self._run_analyze_collection(sample_size=self.analyze_sample_size,
                                             sample_seed=self.analyze_sample_seed,
                                             sample_method="random")
                self._verify_sample_metadata_index(sample_size=self.analyze_sample_size,
                                                   sample_seed=self.analyze_sample_seed,
                                                   sample_method="random")
                break
            except Exception as e:
                retry += 1
                if retry >= maxRetry:
                    self.fail(
                        "COPY INTO/ANALYZE COLLECTION/verify failed after "
                        "{} attempts: {}".format(maxRetry, e))
                self.log.warn(
                    "COPY INTO/ANALYZE COLLECTION/verify failed on "
                    "attempt {}/{}: {} - retrying".format(
                        retry, maxRetry, e))

        self.log.info("=" * 60)
        self.log.info("Swap rebalance upgrade test completed successfully")
        final_nodes = [n.ip for n in self.cluster.nodes_in_cluster]
        self.log.info("Final cluster nodes: {}"
                      .format(", ".join(final_nodes)))
        self.log.info("=" * 60)

    def test_failover_upgrade(self):
        """
        Test failover upgrade from 2.0.0-1069 to 2.1.0:
        Setup: 3-node cluster, no spare node required
        Iteration pattern (per node, non-master first, master last):
        1. Pick non-master node on older version (master rerouted to an
           already-upgraded node first, if it's the last one left)
        2. Gracefully failover the node and settle-rebalance the
           cluster down to the remaining nodes
        3. Upgrade the failed-over node in place (same as the spare node
           path in test_swap_rebalance_upgrade)
        4. Add the upgraded node back in; rebalance; update cluster.master
        5. Repeat for next node in cluster
        """
        self.log.info("Starting failover upgrade test")

        # Verify initial setup: nodes_init nodes in cluster, no spare needed
        self.assertEqual(
            len(self.cluster.nodes_in_cluster), self.nodes_init,
            "Expected {} nodes in cluster, found {}"
            .format(self.nodes_init, len(self.cluster.nodes_in_cluster)))

        # Initial cluster state
        self.log.info("=" * 60)
        self.log.info("Initial cluster state")
        self.log.info("=" * 60)
        self.cluster_util.print_cluster_stats(self.cluster)

        # Validate prodCompatVersion before upgrade
        pre_upgrade_version_base = self.pre_upgrade_version.split("-")[0]
        self._validate_prod_compat_version(
            pre_upgrade_version_base, label="Pre-upgrade")

        # Create infra
        self._create_ea_upgrade_infra()

        # Run COPY INTO to insert data before upgrade
        self._copy_into_analyze_collection(
            path=self.copy_into_path_pre_upgrade)

        # Run ANALYZE COLLECTION before upgrade
        self._run_analyze_collection(sample_size=self.analyze_sample_size,
                                     sample_seed=self.analyze_sample_seed)
        self._verify_sample_metadata_index(
            sample_size=self.analyze_sample_size, sample_seed=self.analyze_sample_seed)

        nodes_to_upgrade = list(self.cluster.nodes_in_cluster)
        iteration = 1
        while nodes_to_upgrade:
            self.log.info("=" * 60)
            self.log.info("Iteration {}".format(iteration))
            self.log.info("=" * 60)
            self.log.info("Nodes in cluster: {}".format(
                [n.ip for n in self.cluster.nodes_in_cluster]))
            self.log.info("Remaining nodes in cluster to be upgraded: {}".format(
                [n.ip for n in nodes_to_upgrade]))
            self.log.info("Current master node: {}:{}".format(
                self.cluster.master.ip, self.cluster.master.port))

            # Step 1: Pick a node to upgrade (non-master node on older version)
            current_node_to_upgrade = self._get_non_master_node_with_older_version(
                nodes_to_upgrade)
            if not current_node_to_upgrade and not nodes_to_upgrade:
                self.log.info("=" * 60)
                self.log.info(
                    "No non-master nodes on older version found and nodes_to_upgrade is empty. Upgrade completed!")
                self.log.info("=" * 60)
                break

            # If the only node left to upgrade is the current master, move
            # cluster.master off it first so it can be failed over safely
            # and so subsequent REST calls keep working.
            if current_node_to_upgrade.ip == self.cluster.master.ip:
                reroute_target = None
                for node in self.cluster.nodes_in_cluster:
                    if node.ip != self.cluster.master.ip:
                        reroute_target = node
                        break
                if reroute_target:
                    self.log.info(
                        "Rerouting cluster.master to {} before failing over "
                        "the current master {}".format(
                            reroute_target.ip, current_node_to_upgrade.ip))
                    self._update_master_node(reroute_target)

            self.log.info("Selected node to upgrade: {} (was master: {})"
                          .format(current_node_to_upgrade.ip,
                                  current_node_to_upgrade.ip == self.cluster.master.ip))

            # Step 2: Failover the node and settle the cluster
            if not self._failover_node(current_node_to_upgrade):
                self.fail("Failed to failover node {}"
                          .format(current_node_to_upgrade.ip))

            # Step 3: Upgrade the failed-over node in place
            self.log.info("Upgrading failed-over node {} to version {}"
                          .format(current_node_to_upgrade.ip, self.upgrade_version))
            self._upgrade_and_prepare_node(current_node_to_upgrade)

            # Step 4: Add the upgraded node back in and update master
            if not self._add_node_to_cluster(current_node_to_upgrade):
                self.fail("Failed to add node {} back to cluster"
                          .format(current_node_to_upgrade.ip))
            if not self._update_master_node(current_node_to_upgrade):
                self.log.warn(
                    "Master update may have failed, but continuing...")

            # Step 5: Remove upgraded node from nodes_to_upgrade
            if current_node_to_upgrade in nodes_to_upgrade:
                nodes_to_upgrade.remove(current_node_to_upgrade)

            self.log.info("=" * 60)
            self.log.info("Iteration {} completed. Remaining nodes to upgrade: {}"
                          .format(iteration, [n.ip for n in nodes_to_upgrade]))
            self.log.info("=" * 60)
            iteration += 1

        # Verify final state
        self.sleep(
            30, "Wait after final swap rebalance to allow cluster to settle")
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
                self.log.info("\u2713 Node {} is on upgraded version {}"
                              .format(node.ip, node_version))
            else:
                self.log.warn("Node {} version {} does not contain {}"
                              .format(node.ip, node_version, self.upgrade_version))

        # Validate prodCompatVersion after upgrade
        post_upgrade_version = self.upgrade_version.split("-")[0]
        prod_compat_valid = self._validate_prod_compat_version(
            post_upgrade_version, label="Post-upgrade")
        self.assertTrue(
            prod_compat_valid,
            "prodCompatVersion validation failed: one or more nodes are not "
            "reporting prodCompatVersion={}".format(post_upgrade_version))

        # Verify pre-upgrade SAMPLE statistics survived the upgrade
        self._verify_sample_metadata_index(
            sample_size=self.analyze_sample_size, sample_seed=self.analyze_sample_seed)

        # Trigger FLUSH
        maxRetry = self.post_upgrade_retry_count
        retry = 0
        while (retry < maxRetry):
            try:
                # Run COPY INTO to upsert data post-upgrade, before
                # re-running ANALYZE COLLECTION, so the re-analyze has
                # fresh data
                self.log.info(
                    "Running COPY INTO to upsert data post-upgrade (retry {}/{})".format(retry + 1, maxRetry))
                self._copy_into_analyze_collection(
                    path=self.copy_into_path_post_upgrade)

                # Re-run ANALYZE COLLECTION post-upgrade with
                # sample-method=random and verify it took effect
                self._run_analyze_collection(sample_size=self.analyze_sample_size,
                                             sample_seed=self.analyze_sample_seed,
                                             sample_method="random")

                self._verify_sample_metadata_index(sample_size=self.analyze_sample_size,
                                                   sample_seed=self.analyze_sample_seed,
                                                   sample_method="random")
                break
            except Exception as e:
                retry += 1
                if retry >= maxRetry:
                    self.fail(
                        "COPY INTO/ANALYZE COLLECTION/verify failed after "
                        "{} attempts: {}".format(maxRetry, e))
                self.log.warn(
                    "COPY INTO/ANALYZE COLLECTION/verify failed on "
                    "attempt {}/{}: {} - retrying".format(
                        retry, maxRetry, e))

        self.log.info("=" * 60)
        self.log.info("Failover upgrade test completed successfully")
        final_nodes = [n.ip for n in self.cluster.nodes_in_cluster]
        self.log.info("Final cluster nodes: {}"
                      .format(", ".join(final_nodes)))
        self.log.info("=" * 60)

    def test_offline_upgrade(self):
        """
        Test offline upgrade from 2.0.0-1069 to 2.1.0:
        Setup: 3-node cluster, no spare node required
        Auto-failover is disabled for the whole upgrade and restored at the end.
        Iteration pattern (per node, non-master first, master last):
        1. Pick a node on the older version (cluster.master rerouted to
           another node first, if it's the node picked)
        2. Upgrade the node in place: stop, remove old package, install
           new build without starting it, restore its saved state, start
        3. The node rejoins the cluster by itself; verify all nodes are
           active + healthy and the cluster is balanced (no failover,
           no rebalance, no add-node)
        4. Repeat for next node in cluster
        """
        self.log.info("Starting offline upgrade test")

        # Verify initial setup: nodes_init nodes in cluster, no spare needed
        self.assertEqual(
            len(self.cluster.nodes_in_cluster), self.nodes_init,
            "Expected {} nodes in cluster, found {}"
            .format(self.nodes_init, len(self.cluster.nodes_in_cluster)))

        # Initial cluster state
        self.log.info("=" * 60)
        self.log.info("Initial cluster state")
        self.log.info("=" * 60)
        self.cluster_util.print_cluster_stats(self.cluster)

        # Validate prodCompatVersion before upgrade
        pre_upgrade_version_base = self.pre_upgrade_version.split("-")[0]
        self._validate_prod_compat_version(
            pre_upgrade_version_base, label="Pre-upgrade")

        # Create infra
        self._create_ea_upgrade_infra()

        # Run COPY INTO to insert data before upgrade
        self._copy_into_analyze_collection(
            path=self.copy_into_path_pre_upgrade)

        # Run ANALYZE COLLECTION before upgrade
        self._run_analyze_collection(sample_size=self.analyze_sample_size,
                                     sample_seed=self.analyze_sample_seed)
        self._verify_sample_metadata_index(
            sample_size=self.analyze_sample_size, sample_seed=self.analyze_sample_seed)

        # Auto-failover stays disabled until every node is upgraded
        saved_auto_failover = self._set_auto_failover()
        try:
            nodes_to_upgrade = list(self.cluster.nodes_in_cluster)
            iteration = 1
            while nodes_to_upgrade:
                self.log.info("=" * 60)
                self.log.info("Iteration {}".format(iteration))
                self.log.info("=" * 60)
                self.log.info("Nodes in cluster: {}".format(
                    [n.ip for n in self.cluster.nodes_in_cluster]))
                self.log.info("Remaining nodes in cluster to be upgraded: {}".format(
                    [n.ip for n in nodes_to_upgrade]))
                self.log.info("Current master node: {}:{}".format(
                    self.cluster.master.ip, self.cluster.master.port))

                # Step 1: Pick a node to upgrade (non-master node on older version)
                current_node_to_upgrade = self._get_non_master_node_with_older_version(
                    nodes_to_upgrade)

                # If the node being upgraded is the current master, move
                # cluster.master off it first so REST calls keep working
                # while it is down.
                if current_node_to_upgrade.ip == self.cluster.master.ip:
                    reroute_target = None
                    for node in self.cluster.nodes_in_cluster:
                        if node.ip != self.cluster.master.ip:
                            reroute_target = node
                            break
                    if reroute_target:
                        self.log.info(
                            "Rerouting cluster.master to {} before upgrading "
                            "the current master {}".format(
                                reroute_target.ip, current_node_to_upgrade.ip))
                        self._update_master_node(reroute_target)

                self.log.info("Selected node to upgrade: {}"
                              .format(current_node_to_upgrade.ip))

                # Step 2: Upgrade the node in place
                self._offline_upgrade_node(current_node_to_upgrade)

                # Step 3: Node rejoins by itself - verify active + healthy
                self._verify_cluster_nodes_active_healthy()

                # Step 4: Remove upgraded node from nodes_to_upgrade
                if current_node_to_upgrade in nodes_to_upgrade:
                    nodes_to_upgrade.remove(current_node_to_upgrade)

                self.log.info("=" * 60)
                self.log.info("Iteration {} completed. Remaining nodes to upgrade: {}"
                              .format(iteration, [n.ip for n in nodes_to_upgrade]))
                self.log.info("=" * 60)
                iteration += 1
        finally:
            self._set_auto_failover(saved_auto_failover)

        # Verify final state
        self.sleep(30, "Wait after final node upgrade to allow cluster to settle")
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

        # Validate prodCompatVersion after upgrade
        post_upgrade_version = self.upgrade_version.split("-")[0]
        prod_compat_valid = self._validate_prod_compat_version(
            post_upgrade_version, label="Post-upgrade")
        self.assertTrue(
            prod_compat_valid,
            "prodCompatVersion validation failed: one or more nodes are not "
            "reporting prodCompatVersion={}".format(post_upgrade_version))

        # Verify pre-upgrade SAMPLE statistics survived the upgrade
        self._verify_sample_metadata_index(
            sample_size=self.analyze_sample_size, sample_seed=self.analyze_sample_seed)

        # Trigger FLUSH
        maxRetry = self.post_upgrade_retry_count
        retry = 0
        while (retry < maxRetry):
            try:
                # Run COPY INTO to upsert data post-upgrade, before
                # re-running ANALYZE COLLECTION, so the re-analyze has
                # fresh data
                self.log.info(
                    "Running COPY INTO to upsert data post-upgrade (retry {}/{})".format(retry + 1, maxRetry))
                self._copy_into_analyze_collection(
                    path=self.copy_into_path_post_upgrade)

                # Re-run ANALYZE COLLECTION post-upgrade with
                # sample-method=random and verify it took effect
                self._run_analyze_collection(sample_size=self.analyze_sample_size,
                                             sample_seed=self.analyze_sample_seed,
                                             sample_method="random")
                self._verify_sample_metadata_index(sample_size=self.analyze_sample_size,
                                                   sample_seed=self.analyze_sample_seed,
                                                   sample_method="random")
                break
            except Exception as e:
                retry += 1
                if retry >= maxRetry:
                    self.fail(
                        "COPY INTO/ANALYZE COLLECTION/verify failed after "
                        "{} attempts: {}".format(maxRetry, e))
                self.log.warn(
                    "COPY INTO/ANALYZE COLLECTION/verify failed on "
                    "attempt {}/{}: {} - retrying".format(
                        retry, maxRetry, e))

        self.log.info("=" * 60)
        self.log.info("Offline upgrade test completed successfully")
        final_nodes = [n.ip for n in self.cluster.nodes_in_cluster]
        self.log.info("Final cluster nodes: {}"
                      .format(", ".join(final_nodes)))
        self.log.info("=" * 60)
