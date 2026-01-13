import threading
import json

from basetestcase import BaseTestCase
from cb_constants import CbServer
import Jython_tasks.task as jython_tasks
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from pytests.ns_server.enforce_tls import EnforceTls
from BucketLib.bucket import Collection, Scope
from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from rebalance_utils.rebalance_util import RebalanceUtil
from rebalance_utils.retry_rebalance import RetryRebalanceUtil
from sdk_client3 import SDKClient
from bucket_collections.collections_base import CollectionBase
from BucketLib.bucket import Bucket
import testconstants
from shell_util.remote_connection import RemoteMachineShellConnection
from upgrade_lib.couchbase import upgrade_chains
from upgrade_utils.upgrade_util import CbServerUpgrade


class UpgradeBase(BaseTestCase):
    def setUp(self):
        super(UpgradeBase, self).setUp()
        self.log.info("=== UpgradeBase setUp started ===")

        self.creds = self.input.membase_settings
        self.key = "update_docs"
        self.disk_location_data = \
            self.input.param("data_location",
                             testconstants.COUCHBASE_DATA_PATH)
        self.disk_location_index = \
            self.input.param("index_location",
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
        self.retry_get_process_num = \
            self.input.param("retry_get_process_num", 25)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        self.sync_write_abort_pattern = \
            self.input.param("sync_write_abort_pattern", "all_aborts")
        self.migrate_storage_backend = \
            self.input.param("migrate_storage_backend", False)
        self.preferred_storage_mode = \
            self.input.param("preferred_storage_mode",
                             Bucket.StorageBackend.magma)
        self.range_scan_timeout = self.input.param("range_scan_timeout", None)
        self.range_scan_collections = \
            self.input.param("range_scan_collections", 0)
        self.rest = ClusterRestAPI(self.cluster.master)
        self.server_index_to_fail = \
            self.input.param("server_index_to_fail", None)
        self.key_size = self.input.param("key_size", None)
        self.range_scan_task = self.input.param("range_scan_task", None)
        self.expect_range_scan_exceptions = \
            self.input.param("expect_range_scan_exceptions",
                             ["com.couchbase.client.core.error.FeatureNotAvailableException: "
                              "The cluster does not support the scan operation (Only supported"
                              " with Couchbase Server 7.5 and later)."])
        self.skip_range_scan_collection_mutation = self.input.param(
            "skip_range_scan_collection_mutation", True)
        self.range_scan_runs_per_collection = self.input.param(
            "range_scan_runs_per_collection", 1)
        self.migration_procedure = \
            self.input.param("migration_procedure", "swap_rebalance")
        self.test_guardrail_migration = \
            self.input.param("test_guardrail_migration", False)
        self.test_guardrail_upgrade = \
            self.input.param("test_guardrail_upgrade", False)
        self.guardrail_type = \
            self.input.param("guardrail_type", "resident_ratio")
        self.breach_guardrail = self.input.param("breach_guardrail", False)

        self.cluster_profile = self.input.param("cluster_profile", None)

        # Spec File Parameters
        self.spec_name = \
            self.input.param("bucket_spec", "single_bucket.default")
        self.initial_data_spec = \
            self.input.param("initial_data_spec", "initial_load")
        self.sub_data_spec = \
            self.input.param("sub_data_spec", "subsequent_load_magma")
        self.upsert_data_spec = \
            self.input.param("upsert_data_spec", "upsert_load")
        self.sync_write_spec = \
            self.input.param("sync_write_spec", "sync_write_magma")
        self.collection_spec = \
            self.input.param("collection_spec","collections_magma")
        self.load_large_docs = \
            self.input.param("load_large_docs", False)
        self.collection_operations = \
            self.input.param("collection_operations", True)
        self.ops_rate = self.input.param("ops_rate", 30000)
        ####
        self.rebalance_op = self.input.param("rebalance_op", "all")
        self.dur_level = self.input.param("dur_level", "default")
        self.alternate_load = self.input.param("alternate_load", False)
        self.magma_upgrade = self.input.param("magma_upgrade", False)
        self.perform_collection_ops = \
            self.input.param("perform_collection_ops", False)
        self.collection_ops_iterations = \
            self.input.param("collection_ops_iterations", 1)

        self.include_indexing_query = \
            self.input.param("include_indexing_query", False)
        self.redistribute_indexes = \
            self.input.param("redistribute_indexes", True)
        self.enable_shard_affinity = \
            self.input.param("enable_shard_affinity", True)
        self.create_partitioned_indexes = \
            self.input.param("create_partitioned_indexes", True)
        self.index_quota_mem = self.input.param("index_quota_mem", 512)
        self.kv_quota_mem = self.input.param("kv_quota_mem", 6000)

        # Input params for large doc upgrade tests
        self.large_doc_upgrade = self.input.param("large_doc_upgrade", False)
        self.bucket_replicas = self.input.param("bucket_replicas", 0)
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)

        # Works only for versions > 1.7 release
        self.product = "couchbase-server"
        community_upgrade = self.input.param("community_upgrade", False)

        self.enable_auto_retry_rebalance = \
            self.input.param("auto_retry_rebalance", False)
        self.rebalance_failure_condition = \
            self.input.param("rebalance_failure_condition", None)
        self.delay_time = self.input.param("delay_time", 60000)

        self.upgrade_helper = CbServerUpgrade(self.log, self.product)
        self._populate_upgrade_chain()

        if community_upgrade:
            self.upgrade_version = self.upgrade_chain[-1]

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

        # Setting non_ssl ports before initial installation
        for server in self.input.servers:
            self.set_ports_for_server(server, "non_ssl")
        for server in self.cluster.servers:
            self.set_ports_for_server(server, "non_ssl")

        CbServer.use_https = False
        CbServer.n2n_encryption = False

        self.PrintStep("Installing initial version {0} on servers"
                       .format(self.upgrade_chain[0].split("-")[0]))
        self.cluster.version = self.upgrade_chain[0]
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=self.cluster.servers[0:self.nodes_init],
            version=self.upgrade_chain[0].split("-")[0],
            edition=build_type)

        for node in self.cluster.servers[0:self.nodes_init]:
            self.assertTrue(
                self.cluster_util.is_ns_server_running(node, 30),
                "{} - Server REST endpoint unreachable after 30 seconds"
                .format(node.ip))

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
            self.initialize_cluster(
                cluster_name, cluster, services=None,
                services_mem_quota_percent=mem_quota_percent)

        self.cluster = list(self.cb_clusters.values())[0]
        if self.services_init:
            self.services_init = self.cluster_util.get_services(
                [self.cluster.master], self.services_init, 0)

        # Initialize first node in cluster
        master_node = self.cluster.servers[0]
        master_node.services = "kv"
        if self.services_init:
            master_node.services = self.services_init[0]

        # Initialize cluster using given nodes
        for index, server \
                in enumerate(self.cluster.servers[1:self.nodes_init]):
            node_service = None
            if self.services_init and len(self.services_init) > index:
                node_service = self.services_init[index + 1].split(',')
                node_service = ",".join(node_service)
            ClusterRestAPI(self.cluster.master).add_node(
                server.ip, username=server.rest_username,
                password=server.rest_password,
                services=node_service)

        self.task.rebalance(self.cluster, [], [])
        self.cluster.nodes_in_cluster.extend(
            self.cluster.servers[0:self.nodes_init])
        self.cluster_util.print_cluster_stats(self.cluster)

        # Set magmaMinMemoryQuota for versions < 8.0
        status, node_info = ClusterRestAPI(self.cluster.master).node_details()
        if status and node_info:
            version = node_info.get("version", "")
            if version and float(version[:3]) < 8.0:
                ClusterRestAPI(self.cluster.master).set_internal_settings(
                    setting_name="magmaMinMemoryQuota", setting_value=256)

        self.cluster_features = \
            self.upgrade_helper.get_supported_features(self.cluster.version)

        # Disable auto-failover to avoid failover of nodes
        if not community_upgrade:
            status = ClusterRestAPI(self.cluster.master) \
                .update_auto_failover_settings("false")
            self.assertTrue(status,
                            msg="Failure during disabling auto-failover")

        if self.enable_auto_retry_rebalance:
            after_time_period = self.input.param("afterTimePeriod", 100)
            max_attempts = self.input.param("maxAttempts", 3)
            body = dict()
            body["enabled"] = "true"
            body["afterTimePeriod"] = after_time_period
            body["maxAttempts"] = max_attempts
            rest = RestConnection(self.cluster.master)
            res = rest.set_retry_rebalance_settings(body)
            self.log.info("Rebalance retry settings = {}".format(res))

        # Creating buckets from spec file
        if not self.large_doc_upgrade:
            CollectionBase.deploy_buckets_from_spec_file(self)
        else:
            self.create_bucket_for_large_doc_upgrades()

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
            for server in self.cluster.servers:
                self.set_ports_for_server(server, "ssl")
            CbServer.use_https = True
            CbServer.n2n_encryption = True

        # Create clients in SDK client pool
        CollectionBase.create_clients_for_sdk_pool(self)

        if self.dur_level == "majority":
            for bucket in self.cluster.buckets:
                if bucket.name == "bucket-1":
                    self.bucket_util.update_bucket_property(
                        self.cluster.master, bucket,
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
        if self.include_indexing_query:
            self.log.info(f"Setting kv mem quota={self.kv_quota_mem} MB")
            self.log.info(f"Setting index mem quota={self.index_quota_mem} MB")
            ClusterRestAPI(self.cluster.master).configure_memory(
                {CbServer.Settings.KV_MEM_QUOTA: self.kv_quota_mem,
                 CbServer.Settings.INDEX_MEM_QUOTA: self.index_quota_mem})

        self.retry_rebalance_util = RetryRebalanceUtil()

    def tearDown(self):
        super(UpgradeBase, self).tearDown()

    def validate_encryption_operations(self, expected_to_fail=True):
        """Validate encryption key creation and assignment."""
        rest = RestConnection(self.cluster.master)

        self.log.info(
            "Starting encryption validation, expected_to_fail=%s",
            expected_to_fail)

        # Try creating a secret
        bucket_helper = BucketHelper(self.cluster.master)
        params = bucket_helper.create_secret_params(
            secret_type="cb-server-managed-aes-key-256",
            name="TestSecretAutoRotationOn",
            usage=[f"bucket-encryption-{self.cluster.buckets[0].name}"],
            autoRotation=True,
            rotationIntervalInSeconds=60,
            port=None
        )
        self.log.info("Sending request to create secret with params: %s",
                      params)
        status, response = rest.create_secret(params)
        self.log.info("Secret creation status=%s, response=%s", status,
                      response)

        if expected_to_fail:
            self.assertFalse(status,
                             "Encryption key creation should fail before full upgrade")
        else:
            self.assertTrue(status,
                            "Encryption key creation should succeed after upgrade")

        if status:
            response_dict = json.loads(response)
            secret_id = response_dict.get('id')
            self.log.info("Created secret ID=%s", secret_id)

            bucket_helper = BucketHelper(self.cluster.master)
            bucket = self.cluster.buckets[0]

            self.log.info(
                "Attempting to assign encryption key %s to bucket %s",
                secret_id, bucket.name)
            status = bucket_helper.change_bucket_props(bucket,
                                                       encryptionAtRestKeyId=secret_id)
            self.log.info("Encryption key assignment status=%s", status)

            if expected_to_fail:
                self.assertFalse(status,
                                 "Should not be able to set encryption before full upgrade")
            else:
                self.assertTrue(status,
                                "Should be able to set encryption after upgrade")

            self.log.info("Deleting created secret ID=%s", secret_id)
            rest.delete_secret(secret_id)
            self.log.info("Secret ID=%s deleted successfully", secret_id)

        self.log.info(
            "Encryption validation completed, expected_to_fail=%s",
            expected_to_fail)

    def add_system_scope_to_all_buckets(self):
        for bucket in self.cluster.buckets:
            scope = Scope({"name": CbServer.system_scope})
            if CbServer.system_scope not in bucket.scopes:
                bucket.scopes[CbServer.system_scope] = scope
            for c_name in [CbServer.query_collection,
                        CbServer.mobile_collection]:
                collection = Collection({"name": c_name, "maxTTL": 0})
                if c_name not in bucket.scopes[CbServer.system_scope].collections:
                    bucket.scopes[CbServer.system_scope].collections[c_name] = collection

    def _populate_upgrade_chain(self):
        chain_to_test = self.input.param("upgrade_chain", "7.2.3")
        upgrade_version = self.input.param("upgrade_version", "8.0.0-1000")
        self.upgrade_chain = upgrade_chains[chain_to_test] + [upgrade_version]
        self.upgrade_version = self.upgrade_chain[0]

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

    def fetch_node_to_upgrade(self, selection_criteria=None):
        """
        :param selection_criteria: Criteria for selecting nodes to be upgraded.
        selection_criteria = {
            "cbas" : {selection criteria},
            "fts" : {selection criteria},
            "kv" : {selection criteria},
        }
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

        if selection_criteria:
            if CbServer.Services.CBAS in selection_criteria:
                for node in self.cluster_util.get_nodes(self.cluster.master):
                    _, node_info = ClusterRestAPI(node).node_details()
                    if (self.upgrade_version not in node_info["version"]
                            or "community" in node_info["version"]) \
                            and check_node_runs_service(node_info["services"]):
                        if "exclude_node" in selection_criteria[
                                CbServer.Services.CBAS]:
                            if selection_criteria[CbServer.Services.CBAS][
                                    "exclude_node"].ip == node.ip:
                                continue
                            else:
                                cluster_node = node
                                break
                        elif "select_node" in selection_criteria[
                            CbServer.Services.CBAS]:
                            if selection_criteria[CbServer.Services.CBAS][
                                    "select_node"].ip == node.ip:
                                cluster_node = node
                                break
                            else:
                                continue
                        else:
                            cluster_node = node
                            break
        else:
            if self.prefer_master:
                _, node_info = ClusterRestAPI(self.cluster.master).node_details()
                if (self.upgrade_version not in node_info["version"]
                        or "community" in node_info["version"]) \
                        and check_node_runs_service(node_info["services"]):
                    cluster_node = self.cluster.master

            if cluster_node is None:
                for node in self.cluster_util.get_nodes(self.cluster.master):
                    _, node_info = ClusterRestAPI(node).node_details()
                    if (self.upgrade_version not in node_info["version"]
                            or "community" in node_info["version"]) \
                            and check_node_runs_service(node_info["services"]):
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

    def __get_otp_node(self, rest_node, target_node):
        """
        Get the OtpNode for the 'target_node'

        :param rest: RestConnection object
        :param target_node: Node going to be upgraded
        :return: OtpNode object of the target_node
        """
        nodes = self.cluster_util.get_nodes(rest_node)
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

        return target_node, ClusterRestAPI(self.__getTestServerObj(target_node))

    def failover_recovery(self, node_to_upgrade, recovery_type, graceful=True):
        rest_node, rest = self.__get_rest_node(node_to_upgrade)
        otp_node = self.__get_otp_node(rest_node, node_to_upgrade)
        self.log.info("Failing over the node %s" % otp_node.id)
        if graceful:
            success, _ = rest.perform_graceful_failover(otp_node.id)
        else:
            success, _ = rest.perform_hard_failover(otp_node.id)
        if not success:
            self.log_failure("Failover unsuccessful")
            return

        self.cluster_util.print_cluster_stats(self.cluster)

        # Monitor failover rebalance
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

        install_tasks = ["populate_build_url", "check_url_status",
                         "download_build", "install"]
        # Install target version on spare node
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=[node_to_upgrade], version=self.upgrade_version,
            cluster_profile=self.cluster_profile,
            install_tasks=install_tasks)
        self.sleep(30, "Wait after installation on the spare node")

        rest.set_failover_recovery_type(otp_node.id, recovery_type)
        self.sleep(5, "Wait after setting failover recovery type")

        delta_recovery_buckets = list()
        if recovery_type == "delta":
            delta_recovery_buckets = [bucket.name
                                      for bucket in self.cluster.buckets]

        known_nodes=[node.id for node in self.cluster_util.get_nodes(rest_node, inactive_added=True)]
        _, _ = rest.rebalance(known_nodes=known_nodes,
                            delta_recovery_buckets=delta_recovery_buckets)

        self.perform_collection_ops_load(self.collection_spec)
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Graceful failover rebalance failed")
            return

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

    def online_swap(self, node_to_upgrade, install_on_spare_node=True):
        rebalance_fail = None
        vb_details = dict()
        vb_verification = dict()
        vb_types = ["active", "replica"]

        # Fetch active services on node_to_upgrade
        services = self.cluster_util.get_nodes_services(node_to_upgrade)
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]

        # Record vbuckets in swap_node
        if CbServer.Services.KV in services_on_target_node:
            cbstats = Cbstats(node_to_upgrade)
            for vb_type in vb_types:
                vb_details[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
            cbstats.disconnect()
        if install_on_spare_node:
            install_tasks = ["populate_build_url", "check_url_status",
                             "download_build", "uninstall", "install"]
            # Install target version on spare node
            self.upgrade_helper.new_install_version_on_all_nodes(
                nodes=[self.spare_node], version=self.upgrade_version,
                cluster_profile=self.cluster_profile,
                install_tasks=install_tasks)
            self.sleep(60, "Wait after installation on the spare node")

            # Initialize paths on the spare node
            status, content = ClusterRestAPI(self.spare_node).initialize_node(
                self.spare_node.rest_username,
                self.spare_node.rest_password,
                data_path=self.spare_node.data_path,
                index_path=self.spare_node.index_path,
                cbas_path=self.spare_node.cbas_path,
                eventing_path=self.spare_node.eventing_path)
            self.assertTrue(status, f"Init node failed: {content}")

        if self.rebalance_failure_condition is not None:
            nodes_to_induce = self.cluster.nodes_in_cluster + [self.spare_node]
            self.retry_rebalance_util.induce_rebalance_test_condition(
                nodes_to_induce,
                self.rebalance_failure_condition,
                self.cluster.buckets[0].name,
                delay_time=self.delay_time)
            rebalance_fail = False
            if self.rebalance_failure_condition in \
                    ["backfill_done", "verify_replication",
                     "after_apply_delta_recovery", "rebalance_start"]:
                rebalance_fail = True
        self.sleep(10)
        # Perform swap rebalance for node_to_upgrade <-> spare_node
        self.log.info("Swap Rebalance starting...")
        rebalance_passed = self.task.async_rebalance(
            self.cluster,
            to_add=[self.spare_node],
            to_remove=[node_to_upgrade],
            check_vbucket_shuffling=False,
            services=[",".join(services_on_target_node)],
            retry_get_process_num=self.retry_get_process_num
        )
        self.sleep(10)

        if self.upgrade_with_data_load:
            update_task = self.load_during_rebalance(
                self.sub_data_spec, async_load=True)
            if self.include_indexing_query:
                self.run_queries_during_rebalance(node_to_upgrade)
            self.task_manager.get_task_result(update_task)

        self.perform_collection_ops_load(self.collection_spec)
        self.task_manager.get_task_result(rebalance_passed)
        if rebalance_passed.result is True:
            self.log.info("Swap Rebalance passed")
        elif rebalance_passed.result is False \
                and self.rebalance_failure_condition is None:
            self.fail("Swap rebalance failed")
        elif rebalance_passed.result is False \
                and self.rebalance_failure_condition is not None \
                and not rebalance_fail:
            self.fail("Swap rebalance failed even though test condition was {}"
                      .format(self.rebalance_failure_condition))
        else:
            delete_condition_nodes = self.cluster.nodes_in_cluster + [self.spare_node]
            self.retry_rebalance_util.delete_rebalance_test_condition(
                delete_condition_nodes, self.rebalance_failure_condition)
            self.sleep(30, "Wait for 30 seconds before retrying rebalance")
            status = self.retry_rebalance_util.check_retry_rebalance_succeeded(
                self.cluster)
            self.assertTrue(status, "Retry rebalance didn't succeed")

        # VBuckets shuffling verification
        if CbServer.Services.KV in services_on_target_node:
            # Fetch vbucket stats after swap rebalance for verification
            cbstats = Cbstats(self.spare_node)
            for vb_type in vb_types:
                vb_verification[vb_type] = \
                    cbstats.vbucket_list(self.bucket.name, vb_type)
            cbstats.disconnect()

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

    def online_rebalance_out_in(self, node_to_upgrade,
                                install_on_spare_node=True):
        """
        cluster --OUT--> Node with previous version
        cluster <--IN-- Node with latest_build
        """

        # Fetch active services on node_to_upgrade
        rest_node, rest = self.__get_rest_node(node_to_upgrade)
        services = self.cluster_util.get_nodes_services(node_to_upgrade)
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]

        # Rebalance-out the target_node
        eject_otp_node = self.__get_otp_node(rest_node, node_to_upgrade)
        otp_nodes = [node.id for node in \
                     self.cluster_util.get_nodes(rest_node)]
        rest.rebalance(known_nodes=otp_nodes, eject_nodes=[eject_otp_node.id])
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of %s"
                             % node_to_upgrade.ip)
            return

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(
            self.cluster, removed_nodes=[node_to_upgrade])

        # Install target version on spare node
        if install_on_spare_node:
            self.upgrade_helper.new_install_version_on_all_nodes(
                nodes=[self.spare_node], version=self.upgrade_version,
                cluster_profile=self.cluster_profile)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.spare_node.ip,
                      self.creds.rest_username,
                      self.creds.rest_password,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in \
                     self.cluster_util.get_nodes(rest_node)]

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

        rest.rebalance(known_nodes=otp_nodes, eject_nodes=[])
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Print cluster status
        self.cluster_util.print_cluster_stats(self.cluster)

        # Update master node
        self.cluster.master = self.spare_node

        # Update spare node to rebalanced_out node
        self.spare_node = node_to_upgrade

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

    def online_rebalance_in_out(self, node_to_upgrade,
                                install_on_spare_node=True):
        """
        cluster <--IN-- Node with latest_build
        cluster --OUT--> Node with previous version
        """
        # Fetch active services on node_to_upgrade
        rest_node, rest = self.__get_rest_node(node_to_upgrade)
        services = self.cluster_util.get_nodes_services(node_to_upgrade)
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]

        if install_on_spare_node:
            # Install target version on spare node
            self.upgrade_helper.new_install_version_on_all_nodes(
                nodes=[self.spare_node], version=self.upgrade_version,
                cluster_profile=self.cluster_profile)

        # Rebalance-in spare node into the cluster
        rest.add_node(self.spare_node.ip,
                      self.creds.rest_username,
                      self.creds.rest_password,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in \
                     self.cluster_util.get_nodes(rest_node)]

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

        rest.rebalance(known_nodes=otp_nodes, eject_nodes=[])

        self.perform_collection_ops_load(self.collection_spec)
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

        # Print cluster status
        self.cluster_util.print_cluster_stats(self.cluster)

        # Rebalance-out the target_node
        rest_node, rest = self.__get_rest_node(self.spare_node)
        eject_otp_node = self.__get_otp_node(rest_node, node_to_upgrade)
        otp_nodes = [node.id for node in
                     self.cluster_util.get_nodes(rest_node)]
        rest.rebalance(known_nodes=otp_nodes, eject_nodes=[eject_otp_node.id])

        self.perform_collection_ops_load(self.collection_spec)
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
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

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

    def online_incremental(self, node_to_upgrade, version):
        # Fetch active services on node_to_upgrade
        rest_node, rest = self.__get_rest_node(node_to_upgrade)
        services = self.cluster_util.get_nodes_services(node_to_upgrade)
        services_on_target_node = services[(node_to_upgrade.ip + ":"
                                            + str(node_to_upgrade.port))]
        # Rebalance-out the target_node
        rest_node, rest = self.__get_rest_node(node_to_upgrade)
        eject_otp_node = self.__get_otp_node(rest_node, node_to_upgrade)
        otp_nodes = [node.id for node in \
                     self.cluster_util.get_nodes(rest_node)]
        rest.rebalance(known_nodes=otp_nodes, eject_nodes=[eject_otp_node.id])
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-out failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Install the required version on the node
        self.upgrade_helper.install_version_on_nodes(
            [node_to_upgrade], version)

        # Wait for ns_server to be ready on the upgraded node
        self.log.info("Wait for ns_server to accept connections on upgraded node")
        if not self.cluster_util.is_ns_server_running(
                node_to_upgrade, timeout_in_seconds=120):
            self.log_failure("Server not started post upgrade on {0}"
                            .format(node_to_upgrade))
            return

        # Rebalance-in the target_node again
        rest.add_node(node_to_upgrade.ip,
                      username=self.creds.rest_username,
                      password=self.creds.rest_password,
                      services=services_on_target_node)
        otp_nodes = [node.id for node in \
                     self.cluster_util.get_nodes(rest_node)]

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

        rest.rebalance(known_nodes=otp_nodes, eject_nodes=[])
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.log_failure("Rebalance-in failed during upgrade of {0}"
                             .format(node_to_upgrade))
            return

        # Validate orchestrator selection
        self.cluster_util.validate_orchestrator_selection(self.cluster)

    def failover_delta_recovery(self, node_to_upgrade):
        self.failover_recovery(node_to_upgrade, "delta")

    def failover_full_recovery(self, node_to_upgrade, graceful=True):
        self.failover_recovery(node_to_upgrade, "full", graceful)

    def offline(self, node_to_upgrade, rebalance_required=True):
        rest = ClusterRestAPI(node_to_upgrade)
        self.log.info("Stopping couchbase server on node %s" % node_to_upgrade.ip)
        shell = RemoteMachineShellConnection(node_to_upgrade)
        shell.stop_couchbase()
        shell.disconnect()

        install_tasks = ["populate_build_url", "check_url_status",
                         "download_build", "install"]
        # Install target version on the node
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=[node_to_upgrade], version=self.upgrade_version,
            cluster_profile=self.cluster_profile,
            install_tasks=install_tasks)
        self.sleep(30, "Wait after installation on the node")

        self.log.info("Wait for ns_server to accept connections")
        if not self.cluster_util.is_ns_server_running(node_to_upgrade,
                                                      timeout_in_seconds=120):
            self.log_failure("Server not started post upgrade")
            return

        self.log.info("Validate the cluster rebalance status")
        status, cluster_info = rest.cluster_details()
        if not status or not cluster_info.get("balanced", False):
            if rebalance_required:
                otp_nodes = [node.id for node in \
                             self.cluster_util.get_nodes(node_to_upgrade)]
                rest.rebalance(known_nodes=otp_nodes, eject_nodes=[])
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
                if not rebalance_passed:
                    self.log_failure(
                        "Rebalance failed post node upgrade of {0}"
                        .format(node_to_upgrade))
                    return
            else:
                self.log_failure("Got /pools/default::balanced=false")
                return

    def full_offline(self, nodes_to_upgrade):

        self.log.info("Stopping couchbase server on all nodes")
        for node in nodes_to_upgrade:
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()

        install_tasks = ["populate_build_url", "check_url_status",
                         "download_build", "install"]
        # Install target version on all the nodes
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=nodes_to_upgrade, version=self.upgrade_version,
            cluster_profile=self.cluster_profile,
            install_tasks=install_tasks)
        self.sleep(30, "Wait after installation on all nodes")

        for node in nodes_to_upgrade:
            self.log.info("Wait for ns_server to accept connections")
            if not self.cluster_util.is_ns_server_running(
                    node, timeout_in_seconds=120):
                self.log_failure("Server not started post upgrade")
                return

        self.cluster_util.print_cluster_stats(self.cluster)

        rest = ClusterRestAPI(self.cluster.master)
        balanced = rest.cluster_details()[1]["balanced"]

        if not balanced:
            self.log.info("Cluster not balanced. Rebalance starting...")
            otp_nodes = [node.id for node in
                         self.cluster_util.get_nodes(self.cluster.master)]
            rebalance_task, _ = rest.rebalance(known_nodes=otp_nodes,
                                               eject_nodes=[])
            self.log.info("Rebalance successful") if rebalance_task \
                else self.log.info("Rebalance failed")

            self.cluster_util.print_cluster_stats(self.cluster)

    def perform_collection_ops_load(self, collections_spec):
        if "collections" not in self.cluster_features \
                or self.perform_collection_ops is False:
            return
        iter = self.collection_ops_iterations
        spec_collection = self.bucket_util.get_crud_template_from_package(
            collections_spec)
        CollectionBase.over_ride_doc_loading_template_params(
            self, spec_collection)
        CollectionBase.set_retry_exceptions(
            spec_collection, self.durability_level)

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
                self.log.info(f"Itr {iterations+1} of collection ops done")
                if iterations < iter - 1:
                    self.sleep(5, "Sleep before starting the next iteration")
                else:
                    self.log.info("Collection ops load done")

    def load_during_rebalance(self, data_spec, async_load=False):
        sub_load_spec = self.bucket_util.get_crud_template_from_package(
            data_spec)
        CollectionBase.over_ride_doc_loading_template_params(
            self, sub_load_spec)
        CollectionBase.set_retry_exceptions(
            sub_load_spec, self.durability_level)

        return self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.cluster.buckets,
            sub_load_spec,
            mutation_num=0,
            async_load=async_load,
            batch_size=500,
            process_concurrency=4,
            load_using=self.load_docs_using,
            ops_rate=self.ops_rate)

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
        self.sdk_client = SDKClient(self.cluster, bucket,
                                    scope=CbServer.default_scope,
                                    collection=CbServer.default_collection)
        new_docs = doc_generator(key=doc_key, start=0, end=num_docs,
                                 doc_size=1024, doc_type=self.doc_type,
                                 vbuckets=self.cluster.vbuckets,
                                 key_size=self.key_size,
                                 randomize_value=True)
        self.log.info(f"Inserting {num_docs} documents into bucket: {bucket}")
        for i in range(num_docs):
            key_obj, val_obj = new_docs.next()
            self.document_keys.append(key_obj)
            res = self.sdk_client.insert(key_obj, val_obj)
            result.append(res)

        self.sdk_client.close()
        return result

    def run_queries_during_rebalance(self, upgrade_node):
        global success_query_count, failed_queries
        bucket = self.cluster.buckets[0]
        thread_array = []
        query_node = None
        success_query_count = 0
        total_queries = 0
        failed_queries = dict()

        for node in self.cluster.query_nodes:
            if node.ip != upgrade_node.ip:
                query_node = node
                break

        self.query_client = RestConnection(query_node)

        def run_query_thread(query, query_client, iter=30):
            count = 0
            local_success_count = 0

            while count < iter:
                result = query_client.query_tool(query)
                if result["status"] == "success":
                    local_success_count += 1
                else:
                    global failed_queries
                    if query not in failed_queries:
                        failed_queries[query] = [result]
                    else:
                        failed_queries[query].append(result)
                count += 1

            global success_query_count
            success_query_count += local_success_count

        self.log.info("Running queries during rebalance...")
        for scope in self.bucket_util.get_active_scopes(bucket):
            for col in self.bucket_util.get_active_collections(
                    bucket,  scope.name):
                index_name = f"{bucket.name}_{scope.name}_{col.name}_index"
                if index_name in self.indexes:
                    query = \
                        "SELECT name from " \
                        f"`{bucket.name}`.`{scope.name}`.`{col.name}` " \
                        f"USE INDEX(`{index_name}`) limit 1000"
                    self.log.info("Query = {}".format(query))
                    total_queries += 750
                    t = threading.Thread(target=run_query_thread, args=[query, self.query_client, 750])
                    t.start()
                    thread_array.append(t)

                mutation_index_name = "{0}_{1}_{2}_sec_mutation_index"\
                    .format(bucket.name, scope.name, col.name)
                if mutation_index_name in self.indexes:
                    query = \
                        "SELECT mutation_type, count(*) from " \
                        f"`{bucket.name}`.`{scope.name}`.`{col.name}` " \
                        f"USE INDEX(`{mutation_index_name}`)" \
                        " group by mutation_type limit 10000"
                    self.log.info("Query = {}".format(query))
                    total_queries += 250
                    t = threading.Thread(target=run_query_thread, args=[query, self.query_client, 250])
                    t.start()
                    thread_array.append(t)

        complex_queries = [
            'select name from `bucket-0`.`myscope`.`mycoll` '
            'where age between 30 and 50 limit 10;',

            'select age, count(*) from `bucket-0`.`myscope`.`mycoll` '
            'where marital = "M" group by age order by age limit 10;',

            'select v.name, animal from `bucket-0`.`myscope`.`mycoll` as v '
            'unnest animals as animal where v.attributes.hair="Burgundy" '
            'limit 10;',

            'SELECT v.name, ARRAY hobby.name FOR hobby '
            'IN v.attributes.hobbies END FROM `bucket-0`.`myscope`.`mycoll` '
            'as v WHERE v.attributes.hair = "Burgundy" and gender = "F" '
            'and ANY hobby IN v.attributes.hobbies '
            'SATISFIES hobby.type="Music" END limit 10;',

            'select name, ROUND'
            '(attributes.dimensions.weight/attributes.dimensions.height,2) '
            'from `bucket-0`.`myscope`.`mycoll` '
            'WHERE gender is not MISSING limit 10;']

        for complex_query in complex_queries:
            self.log.info("Complexy query = {}".format(complex_query))
            total_queries += 200
            t = threading.Thread(target=run_query_thread,
                                 args=[complex_query, self.query_client, 200])
            t.start()
            thread_array.append(t)

        for th in thread_array:
            th.join()

        self.log.info("Number of successful queries during rebalance = {}"
                      .format(success_query_count))
        self.log.info("Number of failed queries = {}"
                      .format(total_queries - success_query_count))
        self.log.info("Failed queries = {}"
                      .format(failed_queries))

    def create_bucket_for_large_doc_upgrades(self):
        rest = ClusterRestAPI(self.cluster.master)
        pools_info = rest.cluster_details()[1]
        kv_quota = pools_info["memoryQuota"]
        bucket_spec = self.bucket_util.get_bucket_template_from_package(
            self.spec_name)
        bucket_spec[MetaConstants.USE_SIMPLE_NAMES] = True
        bucket_spec[Bucket.replicaNumber] = self.bucket_replicas
        if self.bucket_ram_quota is None:
            bucket_spec[Bucket.ramQuotaMB] = kv_quota - 2000
        else:
            bucket_spec[Bucket.ramQuotaMB] = self.bucket_ram_quota

        CollectionBase.over_ride_bucket_template_params(
            self, self.bucket_storage, bucket_spec)
        self.bucket_util.create_buckets_using_json_data(
            self.cluster, bucket_spec)

        if float(self.upgrade_chain[0][:3]) < 7.6:
            for bucket in self.cluster.buckets:
                for coll in bucket.scopes[CbServer.system_scope].collections:
                    bucket.scopes[CbServer.system_scope].collections.pop(coll)
                bucket.scopes.pop(CbServer.system_scope)
        self.bucket_util.wait_for_collection_creation_to_complete(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

    def load_data_cbc_pillowfight(self, server, bucket, items, doc_size,
                                  key_prefix="test_docs", threads=1,
                                  ops_rate=None):
        shell = RemoteMachineShellConnection(server)

        self.log.info(f"Loading {items} items of doc size: {doc_size} "
                      f"into the bucket with cbc-pillowfight")
        pillowfight_base_cmd = \
            "/opt/couchbase/bin/cbc-pillowfight -U {0}/{1} " \
            "-u Administrator -P password -I {2}" \
            " -t {3} -m {4} -M {4} --populate-only --random-body " \
            "--key-prefix={5} -Dtimeout=10"

        cmd = pillowfight_base_cmd.format(server.ip, bucket.name, items,
                                          threads, doc_size, key_prefix)

        if ops_rate is not None:
            cmd += " --rate-limit {}".format(ops_rate)

        self.log.info("Executing pillowfight command = {}".format(cmd))
        o, e = shell.execute_command(cmd)
        self.sleep(30, "Wait after executing pillowfight command")
        shell.disconnect()

    def PrintStep(self, msg=None):
        print("\n")
        print("\t", "#"*60)
        print("\t", "#")
        print("\t", "#  %s" % msg)
        print("\t", "#")
        print("\t", "#"*60)
        print("\n")
