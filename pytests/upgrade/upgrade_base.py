import re

from basetestcase import BaseTestCase
from builds.build_query import BuildQuery
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from scripts.old_install import InstallerJob
from testconstants import CB_REPO, COUCHBASE_VERSIONS, CB_VERSION_NAME, \
    COUCHBASE_MP_VERSION, MV_LATESTBUILD_REPO


class UpgradeBase(BaseTestCase):
    def setUp(self):
        super(UpgradeBase, self).setUp()
        self.log.info("=== UpgradeBase setUp started ===")
        self.released_versions = ["2.0.0-1976-rel", "2.0.1", "2.5.0", "2.5.1",
                                  "2.5.2", "3.0.0", "3.0.1",
                                  "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                  "3.1.0", "3.1.0-1776", "3.1.1", "3.1.1-1807",
                                  "3.1.2", "3.1.2-1815", "3.1.3", "3.1.3-1823",
                                  "4.0.0", "4.0.0-4051", "4.1.0", "4.1.0-5005",
                                  "4.5.0", "4.5.0-2601", "4.5.1", "4.5.1-2817",
                                  "4.6.0", "4.6.0-3573", '4.6.2', "4.6.2-3905"]

        self.creds = self.input.membase_settings
        self.key = "update_docs"
        self.initial_version = self.input.param("initial_version",
                                                "6.0.1-2037")
        self.upgrade_version = self.input.param("upgrade_version",
                                                "6.5.0-3939")
        self.upgrade_type = self.input.param("upgrade_type", "online_swap")
        self.prefer_master = self.input.param("prefer_master", False)
        self.update_nodes = self.input.param("update_nodes", "kv").split(";")
        self.is_downgrade = self.input.param('downgrade', False)
        self.upgrade_with_data_load = \
            self.input.param("upgrade_with_data_load", True)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        self.sync_write_abort_pattern = \
            self.input.param("sync_write_abort_pattern", "all_aborts")

        # Works only for versions > 1.7 release
        self.product = "couchbase-server"

        if self.initial_version == "same_version":
            self.initial_version = self.upgrade_version

        self.cluster_supports_sync_write = \
            float(self.initial_version[:3]) >= 6.5

        self.installer_job = InstallerJob()

        # Dict to map upgrade_type to action functions
        self.upgrade_function = dict()
        self.upgrade_function["online_swap"] = self.online_swap
        self.upgrade_function["online_incremental"] = self.online_incremental
        self.upgrade_function["online_rebalance_in_out"] = \
            self.online_rebalance_in_out
        self.upgrade_function["online_rebalance_out_in"] = \
            self.online_rebalance_out_in
        self.upgrade_function["failover_delta_recovery"] = \
            self.failover_delta_recovery
        self.upgrade_function["failover_full_recovery"] = \
            self.failover_full_recovery
        self.upgrade_function["offline"] = self.offline

        self.__validate_upgrade_type()

        self.log.info("Installing initial version %s on servers"
                      % self.initial_version)
        self.install_version_on_node(
            self.cluster.servers[0:self.nodes_init],
            self.initial_version)

        # Get service list to initialize the cluster
        if self.services_init:
            self.services_init = self.cluster_util.get_services(
                self.cluster.master, self.services_init, 0)

        # Initialize first node in cluster
        master_rest = RestConnection(self.cluster.servers[0])
        master_rest.init_node()

        # Initialize cluster using given nodes
        for index, server \
                in enumerate(self.cluster.servers[1:self.nodes_init]):
            node_service = None
            if self.services_init and len(self.services_init) > index:
                node_service = self.services_init[index+1].split(',')
            master_rest.add_node(remoteIp=server.ip,
                                 services=node_service)

        self.task.rebalance(self.cluster.servers[0:self.nodes_init], [], [])
        self.cluster.nodes_in_cluster.extend(
            self.cluster.servers[0:self.nodes_init])
        self.cluster_util.print_cluster_stats()

        # Disable auto-failover to avaid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Create default bucket and add rbac user
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas,
            compression_mode=self.compression_mode,
            ram_quota=self.bucket_size,
            bucket_type=self.bucket_type, storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            bucket_durability=self.bucket_durability_level)
        self.bucket_util.add_rbac_user()
        self.bucket = self.bucket_util.buckets[0]

        # Load initial async_write docs into the cluster
        self.gen_load = doc_generator(self.key, 0, self.num_items,
                                      doc_size=self.doc_size)
        async_load_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, self.gen_load, "create",
            active_resident_threshold=self.active_resident_threshold,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(async_load_task)

        # Update num_items in case of DGM run
        if self.active_resident_threshold != 100:
            self.num_items = async_load_task.doc_index

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_for_bucket(
            self.bucket,
            self.num_items,
            timeout=60)
        self.bucket_util.print_bucket_stats()
        self.spare_node = self.cluster.servers[self.nodes_init]

    def tearDown(self):
        super(UpgradeBase, self).tearDown()

    def __validate_upgrade_type(self):
        """
        Validates input param 'upgrade_type' and
        fails the test in-case of unsupported type.
        :return:
        """
        if self.upgrade_type not in self.upgrade_function.keys():
            self.fail("Unsupported upgrade_type: %s" % self.upgrade_type)

    def fetch_node_to_upgrade(self):
        """
        :return cluster_node: TestServer node to be upgraded.
                              If 'None', no more nodes requires upgrade.
        """

        def check_node_runs_service(node_services):
            for target_service in self.update_nodes:
                if target_service in node_services:
                    return True
            return False

        cluster_node = None
        self.cluster.update_master_using_diag_eval()

        if self.prefer_master:
            node_info = RestConnection(self.cluster.master).get_nodes_self(10)
            if self.upgrade_version not in node_info.version \
                    and check_node_runs_service(node_info["services"]):
                cluster_node = self.cluster.master

        if cluster_node is None:
            for node in self.cluster_util.get_nodes(self.cluster.master):
                node_info = RestConnection(node).get_nodes_self(10)
                if self.upgrade_version not in node_info.version \
                        and check_node_runs_service(node_info.services):
                    cluster_node = node
                    break

        # Fetch TestServer object from 'Node' object
        if cluster_node is not None:
            cluster_node = self.__getTestServerObj(cluster_node)

        return cluster_node

    def install_version_on_node(self, nodes, version):
        """
        Installs required Couchbase-server version on the target nodes.

        :param nodes: List of nodes to install the cb 'version'
        :param version: Version to install on target 'nodes'
        :return:
        """
        install_params = dict()
        install_params['num_nodes'] = len(nodes)
        install_params['product'] = "cb"
        install_params['version'] = version
        install_params['vbuckets'] = [self.cluster_util.vbuckets]
        install_params['init_nodes'] = False
        install_params['debug_logs'] = False
        self.installer_job.parallel_install(nodes, install_params)

    def __getTestServerObj(self, node_obj):
        for node in self.cluster.servers:
            if node.ip == node_obj.ip:
                return node

    @staticmethod
    def __get_otp_node(rest, target_node):
        """
        Get the OtpNode for the 'target_node'

        :param rest: RestConnection object
        :param target_node: Node going to be upgraded
        :return: OtpNode object of the target_node
        """
        nodes = rest.node_statuses()
        for node in nodes:
            if node.ip == target_node.ip:
                return node

    def __get_rest_node(self, node_to_upgrade):
        """
        Fetch node not going to be involved in upgrade

        :param node_to_upgrade: Node going to be upgraded
        :return: RestConnection object of node
        """
        target_node = None
        for node in self.cluster_util.get_nodes(self.cluster.master):
            if node.ip != node_to_upgrade.ip:
                target_node = node
                break

        return RestConnection(self.__getTestServerObj(target_node))

    def __get_build(self, version, remote, is_amazon=False, info=None):
        if info is None:
            info = remote.extract_remote_info()
        build_repo = CB_REPO
        if version[:5] in COUCHBASE_VERSIONS:
            if version[:3] in CB_VERSION_NAME:
                build_repo = CB_REPO + CB_VERSION_NAME[version[:3]] + "/"
            elif version[:5] in COUCHBASE_MP_VERSION:
                build_repo = MV_LATESTBUILD_REPO
        builds, changes = BuildQuery().get_all_builds(
            version=version,
            timeout=self.wait_timeout * 5,
            deliverable_type=info.deliverable_type,
            architecture_type=info.architecture_type,
            edition_type="couchbase-server-enterprise",
            repo=build_repo,
            distribution_version=info.distribution_version.lower())

        if re.match(r'[1-9].[0-9].[0-9]-[0-9]+$', version):
            version = version + "-rel"
        if version[:5] in self.released_versions:
            appropriate_build = BuildQuery(). \
                find_couchbase_release_build(
                    '%s-enterprise' % self.product,
                    info.deliverable_type,
                    info.architecture_type,
                    version.strip(),
                    is_amazon=is_amazon,
                    os_version=info.distribution_version)
        else:
            appropriate_build = BuildQuery(). \
                find_build(builds,
                           '%s-enterprise' % self.product,
                           info.deliverable_type,
                           info.architecture_type,
                           version.strip())

        if appropriate_build is None:
            self.log.info("Builds are: %s \n. Remote is %s, %s. Result is: %s"
                          % (builds, remote.ip, remote.username, version))
            raise Exception("Build %s not found" % version)
        return appropriate_build

    def _upgrade(self, upgrade_version, server, queue=None, skip_init=False,
                 info=None, save_upgrade_config=False, fts_query_limit=None,
                 debug_logs=False):
        try:
            remote = RemoteMachineShellConnection(server)
            appropriate_build = self.__get_build(server, upgrade_version,
                                                 remote, info=info)
            self.assertTrue(appropriate_build.url,
                            msg="unable to find build {0}"
                            .format(upgrade_version))
            self.assertTrue(remote.download_build(appropriate_build),
                            "Build wasn't downloaded!")
            o, e = remote.couchbase_upgrade(
                appropriate_build,
                save_upgrade_config=save_upgrade_config,
                forcefully=self.is_downgrade,
                fts_query_limit=fts_query_limit,
                debug_logs=debug_logs)
            self.log.info("upgrade {0} to version {1} is completed"
                          .format(server.ip, upgrade_version))
            if 5.0 > float(self.initial_version[:3]) and self.is_centos7:
                remote.execute_command("systemctl daemon-reload")
                remote.start_server()
            self.rest = RestConnection(server)
            if self.is_linux:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 4, wait_if_warmup=True)
            else:
                self.wait_node_restarted(server, wait_time=testconstants.NS_SERVER_TIMEOUT * 10, wait_if_warmup=True, check_service=True)
            if not skip_init:
                self.rest.init_cluster(self.rest_settings.rest_username, self.rest_settings.rest_password)
            self.sleep(self.sleep_time)
            remote.disconnect()
            self.sleep(10)
            return o, e
        except Exception, e:
            self.log.error(e)
            if queue is not None:
                queue.put(False)
                if not self.is_linux:
                    remote = RemoteMachineShellConnection(server)
                    output, error = remote.execute_command(
                        "cmd /c schtasks /Query /FO LIST /TN removeme /V")
                    remote.log_command_output(output, error)
                    output, error = remote.execute_command(
                        "cmd /c schtasks /Query /FO LIST /TN installme /V")
                    remote.log_command_output(output, error)
                    output, error = remote.execute_command(
                        "cmd /c schtasks /Query /FO LIST /TN upgrademe /V")
                    remote.log_command_output(output, error)
                    remote.disconnect()
                raise e
        if queue is not None:
            queue.put(True)

    def failover_recovery(self, node_to_upgrade, recovery_type):
        rest = self.__get_rest_node(node_to_upgrade)
        otp_node = self.__get_otp_node(rest, node_to_upgrade)
        self.log.info("Failing over the node %s" % otp_node.id)
        success = rest.fail_over(otp_node.id, graceful=True)
        if not success:
            self.log_failure("Failover unsuccessful")
            return

        # Monitor failover rebalance
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

        shell = RemoteMachineShellConnection(node_to_upgrade)
        appropriate_build = self.__get_build(self.upgrade_version,
                                             shell)
        self.assertTrue(appropriate_build.url,
                        msg="Unable to find build %s" % self.upgrade_version)
        self.assertTrue(shell.download_build(appropriate_build),
                        "Failed while downloading the build!")

        self.log.info("Starting node upgrade")
        upgrade_success = shell.couchbase_upgrade(appropriate_build,
                                                  save_upgrade_config=False,
                                                  forcefully=self.is_downgrade)
        shell.disconnect()
        if not upgrade_success:
            self.log_failure("Upgrade failed")
            return

        rest.add_back_node("ns_1@" + otp_node.ip)
        self.sleep(5, "Wait after add_back_node")
        rest.set_recovery_type(otp_node.id, recoveryType=recovery_type)

        delta_recovery_buckets = list()
        if recovery_type == "delta":
            delta_recovery_buckets.append(self.bucket.name)

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       deltaRecoveryBuckets=delta_recovery_buckets)

    def online_swap(self, node_to_upgrade, version,
                    install_on_spare_node=True):
        vb_details = dict()
        vb_verification = dict()
        vb_types = ["active", "replica"]

        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + node_to_upgrade.port)]

        # Record vbuckets in swap_node
        if "kv" in services_on_target_node:
            shell = RemoteMachineShellConnection(node_to_upgrade)
            cbstats = Cbstats(shell)
            for vb_type in vb_types:
                vb_details[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
            shell.disconnect()

        if install_on_spare_node:
            # Install target version on spare node
            self.install_version_on_node([self.spare_node], version)

        # Fetch node not going to be involved in upgrade
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      self.spare_node.ip,
                      self.spare_node.port,
                      services=services_on_target_node)
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=[eject_otp_node.id])
        self.sleep(5, "Wait for rebalance to start")
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Swap rebalance failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # VBuckets shuffling verification
        if "kv" in services_on_target_node:
            # Fetch vbucket stats after swap rebalance for verification
            shell = RemoteMachineShellConnection(self.spare_node)
            cbstats = Cbstats(shell)
            for vb_type in vb_types:
                vb_verification[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
            shell.disconnect()

            # Check vbuckets are shuffled or not
            for vb_type in vb_types:
                if vb_details[vb_type].sort() \
                        != vb_verification[vb_type].sort():
                    self.log_failure("%s vbuckets shuffled post swap_rebalance"
                                     % vb_type)
                    self.log.error("%s vbuckets before vs after: %s != %s"
                                   % (vb_type,
                                      vb_details[vb_type],
                                      vb_verification[vb_type]))

        # Update master node
        self.cluster.master = self.spare_node

        # Update spare_node to rebalanced-out node
        self.spare_node = node_to_upgrade

    def online_rebalance_out_in(self, node_to_upgrade, version,
                                install_on_spare_node=True):
        """
        cluster --OUT--> Node with previous version
        cluster <--IN-- Node with latest_build
        """

        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + node_to_upgrade.port)]

        # Rebalance-out the target_node
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of %s"
                             % node_to_upgrade.ip)
            return

        # Install target version on spare node
        if install_on_spare_node:
            self.install_version_on_node([self.spare_node], version)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      self.spare_node.ip,
                      self.spare_node.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Print cluster status
        self.cluster_util.print_cluster_stats()

        # Update master node
        self.cluster.master = self.spare_node

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade

    def online_rebalance_in_out(self, node_to_upgrade, version,
                                install_on_spare_node=True):
        """
        cluster <--IN-- Node with latest_build
        cluster --OUT--> Node with previous version
        """
        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + node_to_upgrade.port)]

        if install_on_spare_node:
            # Install target version on spare node
            self.install_version_on_node([self.spare_node], version)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      self.spare_node.ip,
                      self.spare_node.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Print cluster status
        self.cluster_util.print_cluster_stats()

        # Rebalance-out the target_node
        rest = self.__get_rest_node(self.spare_node)
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Update master node
        self.cluster.master = self.spare_node

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade

    def online_incremental(self, node_to_upgrade, version):
        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + node_to_upgrade.port)]
        # Rebalance-out the target_node
        rest = self.__get_rest_node(node_to_upgrade)
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Install the required version on the node
        self.install_version_on_node([node_to_upgrade], version)

        # Rebalance-in the target_node again
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      node_to_upgrade.ip,
                      node_to_upgrade.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

    def failover_delta_recovery(self, node_to_upgrade):
        self.failover_recovery(node_to_upgrade, "delta")

    def failover_full_recovery(self, node_to_upgrade):
        self.failover_recovery(node_to_upgrade, "full")

    def offline(self, node_to_upgrade, version):
        self.fail("Yet to be implemented")
