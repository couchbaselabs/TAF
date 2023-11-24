from Cb_constants import CbServer
from basetestcase import BaseTestCase
import Jython_tasks.task as jython_tasks
from collections_helper.collections_spec_constants import MetaConstants, MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from pytests.ns_server.enforce_tls import EnforceTls
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from bucket_collections.collections_base import CollectionBase
from BucketLib.bucket import Bucket
import testconstants
from upgrade_lib.couchbase import upgrade_chains
from upgrade_lib.upgrade_helper import CbServerUpgrade


class UpgradeBase(BaseTestCase):
    def setUp(self):
        super(UpgradeBase, self).setUp()
        self.log.info("=== UpgradeBase setUp started ===")

        self.creds = self.input.membase_settings
        self.key = "update_docs"
        self.disk_location_data = self.input.param("data_location",
                                                   testconstants.COUCHBASE_DATA_PATH)
        self.disk_location_index = self.input.param("index_location",
                                                    testconstants.COUCHBASE_DATA_PATH)
        self.test_storage_upgrade = \
            self.input.param("test_storage_upgrade", False)
        self.upgrade_type = self.input.param("upgrade_type", "online_swap")
        self.prefer_master = self.input.param("prefer_master", False)
        self.update_nodes = self.input.param("update_nodes", "kv").split(";")
        self.is_downgrade = self.input.param('downgrade', False)
        self.enable_tls = self.input.param('enable_tls', False)
        self.tls_level = self.input.param('tls_level', "all")
        self.upgrade_with_data_load = \
            self.input.param("upgrade_with_data_load", True)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        self.sync_write_abort_pattern = \
            self.input.param("sync_write_abort_pattern", "all_aborts")
        self.migrate_storage_backend = self.input.param("migrate_storage_backend", False)
        self.preferred_storage_mode = self.input.param("preferred_storage_mode",
                                                      Bucket.StorageBackend.magma)
        self.range_scan_timeout = self.input.param("range_scan_timeout",
                                                   None)
        self.range_scan_collections = self.input.param("range_scan_collections", None)
        self.rest = RestConnection(self.cluster.master)
        self.server_index_to_fail = self.input.param("server_index_to_fail",
                                                     None)
        self.key_size = self.input.param("key_size", 8)
        self.range_scan_task = self.input.param("range_scan_task", None)
        self.expect_range_scan_exceptions = self.input.param("expect_range_scan_exceptions",
                                                             ["com.couchbase.client.core.error.FeatureNotAvailableException: "
                                                              "The cluster does not support the scan operation (Only supported"
                                                              " with Couchbase Server 7.5 and later)."])
        self.skip_range_scan_collection_mutation = self.input.param(
            "skip_range_scan_collection_mutation", True)
        self.range_scan_runs_per_collection = self.input.param(
            "range_scan_runs_per_collection", 1)
        self.migration_procedure = self.input.param("migration_procedure", "swap_rebalance")
        self.test_guardrail_migration = self.input.param("test_guardrail_migration", False)
        self.test_guardrail_upgrade = self.input.param("test_guardrail_upgrade", False)
        self.guardrail_type = self.input.param("guardrail_type", "resident_ratio")
        self.breach_guardrail = self.input.param("breach_guardrail", False)

        self.cluster_profile = self.input.param("cluster_profile", None)

        #### Spec File Parameters ####

        self.spec_name = self.input.param("bucket_spec", "single_bucket.default")
        self.initial_data_spec = self.input.param("initial_data_spec", "initial_load")
        self.sub_data_spec = self.input.param("sub_data_spec", "subsequent_load_magma")
        self.upsert_data_spec = self.input.param("upsert_data_spec", "upsert_load")
        self.sync_write_spec = self.input.param("sync_write_spec", "sync_write_magma")
        self.collection_spec = self.input.param("collection_spec","collections_magma")
        self.load_large_docs = self.input.param("load_large_docs", False)
        self.collection_operations = self.input.param("collection_operations", True)
        ####
        self.rebalance_op = self.input.param("rebalance_op", "all")
        self.dur_level = self.input.param("dur_level", "default")
        self.alternate_load = self.input.param("alternate_load", False)
        self.magma_upgrade = self.input.param("magma_upgrade", False)
        self.perform_collection_ops = self.input.param("perform_collection_ops", False)
        self.collection_ops_iterations = self.input.param("collection_ops_iterations", 1)

        # Works only for versions > 1.7 release
        self.product = "couchbase-server"
        community_upgrade = self.input.param("community_upgrade", False)

        self.upgrade_helper = CbServerUpgrade(self.log, self.product)
        self._populate_upgrade_chain()

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
        self.upgrade_function["full_offline"] = self.full_offline

        self.__validate_upgrade_type()

        if community_upgrade:
            build_type = "community"
            CbServer.enterprise_edition = False
            self.upgrade_chain[0] = self.upgrade_version
        else:
            build_type = "enterprise"

        self.PrintStep("Installing initial version {0} on servers"
                       .format(self.upgrade_chain[0]))
        self.cluster.version = self.upgrade_chain[0]
        self.upgrade_helper.install_version_on_nodes(
            nodes=self.cluster.servers[0:self.nodes_init],
            version=self.upgrade_chain[0],
            vbuckets=self.cluster.vbuckets,
            build_type=build_type,
            cluster_profile=self.cluster_profile)
        for node in self.cluster.servers[0:self.nodes_init]:
            self.assertTrue(
                RestConnection(node).is_ns_server_running(30),
                "{} - Server REST endpoint unreachable after 30 seconds"
                .format(node.ip))

        if self.disk_location_data == testconstants.COUCHBASE_DATA_PATH and \
                self.disk_location_index == testconstants.COUCHBASE_DATA_PATH:

            # Construct dict of mem. quota percent / mb per service
            mem_quota_percent = dict()
            # Construct dict of mem. quota percent per service
            if self.kv_mem_quota_percent:
                mem_quota_percent[CbServer.Services.KV] = \
                    self.kv_mem_quota_percent
            if self.index_mem_quota_percent:
                mem_quota_percent[CbServer.Services.INDEX] = \
                    self.index_mem_quota_percent
            if self.cbas_mem_quota_percent:
                mem_quota_percent[CbServer.Services.CBAS] = \
                    self.cbas_mem_quota_percent
            if self.fts_mem_quota_percent:
                mem_quota_percent[CbServer.Services.FTS] = \
                    self.fts_mem_quota_percent
            if self.eventing_mem_quota_percent:
                mem_quota_percent[CbServer.Services.EVENTING] = \
                    self.eventing_mem_quota_percent

            if not mem_quota_percent:
                mem_quota_percent = None

            for cluster_name, cluster in self.cb_clusters.items():
                if not self.skip_cluster_reset:
                    self.initialize_cluster(
                        cluster_name, cluster, services=None,
                        services_mem_quota_percent=mem_quota_percent)
                else:
                    self.quota = ""
            else:
                self.quota = ""

        self.cluster = self.cb_clusters.values()[0]
        if self.services_init:
            self.services_init = self.cluster_util.get_services(
                [self.cluster.master], self.services_init, 0)

        # Initialize first node in cluster
        master_node = self.cluster.servers[0]
        if self.services_init:
            master_node.services = self.services_init[0]
        master_rest = RestConnection(master_node)
        master_rest.init_node()

        # Initialize cluster using given nodes
        for index, server \
                in enumerate(self.cluster.servers[1:self.nodes_init]):
            node_service = None
            if self.services_init and len(self.services_init) > index:
                node_service = self.services_init[index + 1].split(',')
            RestConnection(self.cluster.master).add_node(
                user=server.rest_username, password=server.rest_password,
                remoteIp=server.ip, port=server.port, services=node_service)

        self.task.rebalance(self.cluster, [], [])
        self.cluster.nodes_in_cluster.extend(
            self.cluster.servers[0:self.nodes_init])
        self.cluster_util.print_cluster_stats(self.cluster)

        self.cluster_features = \
            self.upgrade_helper.get_supported_features(self.cluster.version)
        self.set_feature_specific_params()

        # Disable auto-failover to avoid failover of nodes
        if not community_upgrade:
            status = RestConnection(self.cluster.master) \
                .update_autofailover_settings(False, 120, False)
            self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Creating buckets from spec file
        CollectionBase.deploy_buckets_from_spec_file(self)

        self.spec_bucket = self.bucket_util.get_bucket_template_from_package(
            self.spec_name)
        if "buckets" not in self.spec_bucket:
            self.items_per_col = self.spec_bucket[MetaConstants.NUM_ITEMS_PER_COLLECTION]
        else:
            self.items_per_col = self.spec_bucket["buckets"]["bucket-0"][
                                                MetaConstants.NUM_ITEMS_PER_COLLECTION]

        # Adding RBAC user
        self.bucket_util.add_rbac_user(self.cluster.master)
        self.bucket = self.cluster.buckets[0]

        if self.enable_tls:
            self.enable_verify_tls(self.cluster.master)
            if self.tls_level == "strict":
                for node in self.cluster.servers:
                    #node.memcached_port = CbServer.ssl_memcached_port (MB-47567)
                    node.port = CbServer.ssl_port

        # Create clients in SDK client pool
        CollectionBase.create_clients_for_sdk_pool(self)

        if self.dur_level == "majority":
            for bucket in self.cluster.buckets:
                if bucket.name == "bucket-1":
                    self.bucket_util.update_bucket_property(self.cluster.master,
                                    bucket,
                                    bucket_durability=Bucket.DurabilityMinLevel.MAJORITY)

        # Load initial async_write docs into the cluster
        self.PrintStep("Initial doc generation process starting...")
        CollectionBase.load_data_from_spec_file(self, self.initial_data_spec,
                                                validate_docs=True)
        self.log.info("Initial doc generation completed")

        # Verify initial doc load count
        if "collections" in self.cluster_features:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)
        else:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)
        self.sleep(30, "Wait for num_items to get reflected")

        self.bucket_util.print_bucket_stats(self.cluster)
        self.spare_node = self.cluster.servers[self.nodes_init]

        self.gen_load = doc_generator(self.key, 0, self.num_items,
                                      randomize_doc_size=True,
                                      randomize_value=True,
                                      randomize=True)

    def tearDown(self):
        super(UpgradeBase, self).tearDown()

    def _populate_upgrade_chain(self):
        chain_to_test = self.input.param("upgrade_chain", "7.2.3")
        upgrade_version = self.input.param("upgrade_version", "8.0.0-1000")
        self.upgrade_chain = upgrade_chains[chain_to_test] + [upgrade_version]
        self.upgrade_version = self.upgrade_chain[0]

    def set_feature_specific_params(self):
        if "magma" in self.cluster_features:
            RestConnection(self.cluster.master).set_internalSetting(
                "magmaMinMemoryQuota", 256)

    def enable_verify_tls(self, master_node, level=None):
        if not level:
            level = self.tls_level
        task = jython_tasks.FunctionCallTask(
            self.node_utils._enable_tls, [master_node, level])
        self.task_manager.schedule(task)
        self.task_manager.get_task_result(task)
        self.assertTrue(EnforceTls.get_encryption_level_on_node(
            master_node) == level)
        if level == "strict":
            status = self.cluster_util.check_if_services_obey_tls(
                self.cluster.nodes_in_cluster)
            self.assertTrue(status, "Services did not honor enforce tls")
            CbServer.use_https = True
            CbServer.n2n_encryption = True

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
        self.cluster_util.find_orchestrator(self.cluster)

        if self.prefer_master:
            node_info = RestConnection(self.cluster.master).get_nodes_self(10)
            if (self.upgrade_version not in node_info.version or "community" in node_info.version) \
                    and check_node_runs_service(node_info["services"]):
                cluster_node = self.cluster.master

        if cluster_node is None:
            for node in self.cluster_util.get_nodes(self.cluster.master):
                node_info = RestConnection(node).get_nodes_self(10)
                if (self.upgrade_version not in node_info.version or "community" in node_info.version) \
                        and check_node_runs_service(node_info.services):
                    cluster_node = node
                    break

        # Fetch TestServer object from 'Node' object
        if cluster_node is not None:
            cluster_node = self.__getTestServerObj(cluster_node)

        return cluster_node

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

    def failover_recovery(self, node_to_upgrade, recovery_type, graceful=True):
        rest = self.__get_rest_node(node_to_upgrade)
        otp_node = self.__get_otp_node(rest, node_to_upgrade)
        self.log.info("Failing over the node %s" % otp_node.id)
        success = rest.fail_over(otp_node.id, graceful=graceful)
        if not success:
            self.log_failure("Failover unsuccessful")
            return

        self.cluster_util.print_cluster_stats(self.cluster)

        # Monitor failover rebalance
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

        shell = RemoteMachineShellConnection(node_to_upgrade)
        appropriate_build = self.upgrade_helper.get_build(
            self.upgrade_version, shell)
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

        rest.set_recovery_type(otp_node.id,
                               recoveryType=recovery_type)

        delta_recovery_buckets = list()
        if recovery_type == "delta":
            delta_recovery_buckets=[bucket.name for bucket in self.cluster.buckets]

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       deltaRecoveryBuckets=delta_recovery_buckets)

        self.perform_collection_ops_load(self.collection_spec)
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

    def online_swap(self, node_to_upgrade, version,
                    install_on_spare_node=True):
        vb_details = dict()
        vb_verification = dict()
        vb_types = ["active", "replica"]

        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]

        # Record vbuckets in swap_node
        if CbServer.Services.KV in services_on_target_node:
            cbstats = Cbstats(node_to_upgrade)
            for vb_type in vb_types:
                vb_details[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
        if install_on_spare_node:
            # Install target version on spare node
            self.upgrade_helper.install_version_on_nodes(
                nodes=[self.spare_node], version=version,
                vbuckets=self.cluster.vbuckets,
                cluster_profile=self.cluster_profile)
            self.assertTrue(
                RestConnection(self.spare_node).is_ns_server_running(30),
                "{} - REST endpoint unreachable after 30 seconds"
                .format(self.spare_node.ip))

        # Perform swap rebalance for node_to_upgrade <-> spare_node
        self.log.info("Swap Rebalance starting...")
        rebalance_passed = self.task.async_rebalance(
            self.cluster,
            to_add=[self.spare_node],
            to_remove=[node_to_upgrade],
            check_vbucket_shuffling=False,
            services=[",".join(services_on_target_node)],
        )

        if self.upgrade_with_data_load:
            self.load_during_rebalance(self.sub_data_spec)

        self.perform_collection_ops_load(self.collection_spec)
        self.task_manager.get_task_result(rebalance_passed)
        if rebalance_passed.result is True:
            self.log.info("Swap Rebalance passed")
        else:
            self.log.info("Swap Rebalance failed")

        # VBuckets shuffling verification
        if CbServer.Services.KV in services_on_target_node:
            # Fetch vbucket stats after swap rebalance for verification
            cbstats = Cbstats(self.spare_node)
            for vb_type in vb_types:
                vb_verification[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)

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
        # self.cluster.master = self.spare_node

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
                                            + str(node_to_upgrade.port))]

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
            self.upgrade_helper.install_version_on_nodes(
                [self.spare_node], version)

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
        self.cluster_util.print_cluster_stats(self.cluster)

        # Update master node
        self.cluster.master = self.spare_node
        self.cluster.nodes_in_cluster.append(self.spare_node)

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade
        self.cluster.nodes_in_cluster.remove(node_to_upgrade)

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
                                            + str(node_to_upgrade.port))]

        if install_on_spare_node:
            # Install target version on spare node
            self.upgrade_helper.install_version_on_nodes(
                [self.spare_node], version)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.creds.rest_username,
                      self.creds.rest_password,
                      self.spare_node.ip,
                      self.spare_node.port,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])

        self.perform_collection_ops_load(self.collection_spec)
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Print cluster status
        self.cluster_util.print_cluster_stats(self.cluster)

        # Rebalance-out the target_node
        rest = self.__get_rest_node(self.spare_node)
        eject_otp_node = self.__get_otp_node(rest, node_to_upgrade)
        otp_nodes = [node.id for node in rest.node_statuses()]
        rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_otp_node.id])

        self.perform_collection_ops_load(self.collection_spec)
        rebalance_passed = rest.monitorRebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Update master node
        self.cluster.master = self.spare_node
        self.cluster.nodes_in_cluster.append(self.spare_node)

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade
        self.cluster.nodes_in_cluster.remove(node_to_upgrade)

    def online_incremental(self, node_to_upgrade, version):
        # Fetch active services on node_to_upgrade
        rest = self.__get_rest_node(node_to_upgrade)
        services = rest.get_nodes_services()
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]
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
        self.upgrade_helper.install_version_on_nodes(
            [node_to_upgrade], version)

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

    def failover_full_recovery(self, node_to_upgrade, graceful=True):
        self.failover_recovery(node_to_upgrade, "full", graceful)

    def offline(self, node_to_upgrade, version, rebalance_required=True):
        rest = RestConnection(node_to_upgrade)
        shell = RemoteMachineShellConnection(node_to_upgrade)
        appropriate_build = self.upgrade_helper.get_build(version, shell)
        self.assertTrue(appropriate_build.url,
                        msg="Unable to find build %s" % version)
        self.assertTrue(shell.download_build(appropriate_build),
                        "Failed while downloading the build!")

        self.log.info("Starting node upgrade")
        upgrade_success = shell.couchbase_upgrade(
            appropriate_build, save_upgrade_config=False,
            forcefully=self.is_downgrade)
        shell.disconnect()
        if not upgrade_success:
            self.log_failure("Upgrade failed")
            return

        self.log.info("Wait for ns_server to accept connections")
        if not rest.is_ns_server_running(timeout_in_seconds=120):
            self.log_failure("Server not started post upgrade")
            return

        self.log.info("Validate the cluster rebalance status")
        if not rest.cluster_status()["balanced"]:
            if rebalance_required:
                otp_nodes = [node.id for node in rest.node_statuses()]
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_passed = rest.monitorRebalance()
                if not rebalance_passed:
                    self.log_failure(
                        "Rebalance failed post node upgrade of {0}"
                        .format(node_to_upgrade))
                    return
            else:
                self.log_failure("Cluster reported (/pools/default) balanced=false")
                return

    def full_offline(self, nodes_to_upgrade, version):
        for node in nodes_to_upgrade:
            rest = RestConnection(node)
            shell = RemoteMachineShellConnection(node)

            appropriate_build = self.upgrade_helper.get_build(version, shell)
            self.assertTrue(appropriate_build.url,
                            msg="Unable to find build %s" % version)
            self.assertTrue(shell.download_build(appropriate_build),
                            "Failed while downloading the build!")

            self.log.info("Starting node upgrade")
            upgrade_success = shell.couchbase_upgrade(
                appropriate_build, save_upgrade_config=False,
                forcefully=self.is_downgrade)
            shell.disconnect()

            if upgrade_success:
                self.log.info("Upgrade of {0} completed".format(node))

            self.log.info("Wait for ns_server to accept connections")
            if not rest.is_ns_server_running(timeout_in_seconds=120):
                self.log_failure("Server not started post upgrade")
                return


        self.cluster_util.print_cluster_stats(self.cluster)

        rest = RestConnection(self.cluster.master)
        balanced = rest.cluster_status()["balanced"]

        if not balanced:
            self.log.info("Cluster not balanced. Rebalance starting...")
            otp_nodes = [node.id for node in rest.node_statuses()]
            rebalance_task = rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
            if rebalance_task:
                self.log.info("Rebalance successful")
            else:
                self.log.info("Rebalance failed")

            self.cluster_util.print_cluster_stats(self.cluster)

    def perform_collection_ops_load(self, collections_spec):
        if "collections" not in self.cluster_features \
                or self.perform_collection_ops is False:
            return
        iter = self.collection_ops_iterations
        spec_collection = self.bucket_util.get_crud_template_from_package(
            collections_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, spec_collection)
        CollectionBase.set_retry_exceptions(spec_collection, self.durability_level)

        spec_collection["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

        self.log.info("Performing collection ops during rebalance...")
        for iterations in range(iter):
            collection_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                spec_collection,
                mutation_num=0,
                batch_size=500,
                process_concurrency=4)

            if collection_task.result is True:
                self.log.info("Iteration {0} of collection ops done".format(iterations+1))
                if iterations < iter - 1:
                    self.sleep(5, "Wait for 5 seconds before starting with the next iteration")
                else:
                    self.log.info("Collection ops load done")

    def load_during_rebalance(self, data_spec, async_load=False):

        sub_load_spec = self.bucket_util.get_crud_template_from_package(data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self,sub_load_spec)
        CollectionBase.set_retry_exceptions(sub_load_spec, self.durability_level)

        data_load_task = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.cluster.buckets,
            sub_load_spec,
            mutation_num=0,
            async_load=async_load,
            batch_size=500,
            process_concurrency=4)

        return data_load_task

    def check_resident_ratio(self, cluster):
        """
        This function returns a dictionary which contains resident ratios
        of all the buckets in the cluster across all nodes.
        key = bucket name
        value = list of resident ratios of the bucket on different nodes
        Ex:  bucket_rr = {'default': [100, 100], 'bucket-1': [45.8, 50.1]}
        """
        bucket_rr = dict()
        for server in cluster.kv_nodes:
            kv_ep_max_size = dict()
            _, res = RestConnection(server).query_prometheus("kv_ep_max_size")
            for item in res["data"]["result"]:
                bucket_name = item["metric"]["bucket"]
                kv_ep_max_size[bucket_name] = float(item["value"][1])

            _, res = RestConnection(server).query_prometheus("kv_logical_data_size_bytes")
            for item in res["data"]["result"]:
                if item["metric"]["state"] == "active":
                    bucket_name = item["metric"]["bucket"]
                    logical_data_bytes = float(item["value"][1])
                    resident_ratio = (kv_ep_max_size[bucket_name] / logical_data_bytes) * 100
                    resident_ratio = min(resident_ratio, 100)
                    if bucket_name not in bucket_rr:
                        bucket_rr[bucket_name] = [resident_ratio]
                    else:
                        bucket_rr[bucket_name].append(resident_ratio)

        return bucket_rr

    def check_bucket_data_size_per_node(self, cluster):

        bucket_data_size = dict()

        for server in cluster.kv_nodes:
            _, res = RestConnection(server).query_prometheus("kv_logical_data_size_bytes")

            for item in res["data"]["result"]:
                if item["metric"]["state"] == "active":
                    bucket_name = item["metric"]["bucket"]
                    logical_data_bytes = float(item["value"][1])
                    logical_data_bytes_in_tb = logical_data_bytes / float(1000000000000)

                    if bucket_name not in bucket_data_size:
                        bucket_data_size[bucket_name] = [logical_data_bytes_in_tb]
                    else:
                        bucket_data_size[bucket_name].append(logical_data_bytes_in_tb)

        return bucket_data_size

    def insert_new_docs_sdk(self, num_docs, bucket, doc_key="new_docs"):
        result = []
        self.document_keys = []
        self.log.info("Creating SDK client for inserting new docs")
        self.sdk_client = SDKClient([self.cluster.master],
                                    bucket,
                                    scope=CbServer.default_scope,
                                    collection=CbServer.default_collection)
        new_docs = doc_generator(key=doc_key, start=0,
                                end=num_docs,
                                doc_size=1024,
                                doc_type=self.doc_type,
                                vbuckets=self.cluster.vbuckets,
                                key_size=self.key_size,
                                randomize_value=True)
        self.log.info("Inserting {0} documents into bucket: {1}".format(num_docs, bucket))
        for i in range(num_docs):
            key_obj, val_obj = new_docs.next()
            self.document_keys.append(key_obj)
            res = self.sdk_client.insert(key_obj, val_obj)
            result.append(res)

        self.sdk_client.close()
        return result

    def PrintStep(self, msg=None):
        print "\n"
        print "\t", "#"*60
        print "\t", "#"
        print "\t", "#  %s" % msg
        print "\t", "#"
        print "\t", "#"*60
        print "\n"
