# -*- coding: utf-8 -*-
import time
from threading import Thread

from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.task import AutoFailoverNodesFailureTask, NodeDownTimerTask
from basetestcase import ClusterSetup
from cb_constants import DocLoading, CbServer
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.server_groups.server_groups_api import ServerGroupsAPI
from cb_tools.cb_cli import CbCli
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from bucket_utils.bucket_ready_functions import CollectionUtils

from pytests.bucket_collections.collections_base import CollectionBase
from rebalance_utils.rebalance_util import RebalanceUtil
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase


class AutoFailoverBaseTest(ClusterSetup, FusionBase):
    MAX_FAIL_DETECT_TIME = 120
    ORCHESTRATOR_TIMEOUT_BUFFER = 60

    def setUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self._get_params()
        self.rest = ClusterRestAPI(self.orchestrator)

        # Verify FBR (File-Based Rebalance) setting is enabled
        status, content = self.rest.set_internal_settings()
        if not status:
            self.fail(f"Failed to get internalSettings: {content}")
        if not isinstance(content, dict):
            self.fail(f"Expected dict from internalSettings, got {type(content)}")
        file_based_backfill_enabled = content.get('dataServiceFileBasedRebalanceEnabled', False)
        self.assertTrue(file_based_backfill_enabled,
                       "dataServiceFileBasedRebalanceEnabled should be True by default in Couchbase 8.5")
        self.log.info("FBR (dataServiceFileBasedRebalanceEnabled) is enabled as expected")

        # Check if FBR should be disabled for DCP fallback testing
        if self.disable_file_based_rebalance:
            self.cluster_util.set_file_based_rebalance(
                self.cluster.master, enabled=False)

        self.spec_name = self.input.param("bucket_spec", None)
        self.auto_reprovision = self.input.param("auto_reprovision", False)
        self.skip_collections_during_data_load = self.input.param(
            "skip_col_dict", None)

        self.range_scan_timeout = self.input.param("range_scan_timeout",
                                                   None)
        self.expect_range_scan_exceptions = self.input.param(
            "expect_range_scan_exceptions",
            ["com.couchbase.client.core.error.CouchbaseException: "
             "The range scan internal partition UUID could not be found on the server"])
        self.range_scan_collections = self.input.param("range_scan_collections", None)
        self.server_index_to_fail = self.input.param("server_index_to_fail",
                                                     None)
        self.key_size = self.input.param("key_size", None)
        self.range_scan_task = self.input.param("range_scan_task", None)
        self.skip_range_scan_collection_mutation = self.input.param(
            "skip_range_scan_collection_mutation", True)
        self.new_replica = self.input.param("new_replica", None)
        self.replica_update_during = self.input.param("replica_update_during",
                                                      None)
        self.include_prefix_scan = self.input.param("include_prefix_scan",
                                                    True)
        self.include_range_scan = self.input.param("include_range_scan",
                                                   True)
        self.failover_ephemeral_no_replicas = self.input.param("failover_ephemeral_no_replicas", False)

        self.bucket_num_vb = self.input.param("bucket_num_vb", 128)
        if self.server_index_to_fail is None:
            self.server_to_fail = self._servers_to_fail()
        else:
            self.server_to_fail = \
                [self.cluster.servers[self.server_index_to_fail]]
        self.servers_to_add = \
            self.cluster.servers[self.nodes_init:self.nodes_init
                                 + self.nodes_in]
        self.servers_to_remove = \
            self.cluster.servers[self.nodes_init
                                 - self.nodes_out:self.nodes_init]
        self.retry_get_process_num = \
            self.input.param("retry_get_process_num", 200)
        self.disk_optimized_thread_settings = \
            self.input.param("disk_optimized_thread_settings", False)
        if self.disk_optimized_thread_settings:
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")

        if self.fusion_test and self.fusion_test:
            self.configure_fusion()
            self.enable_fusion()

        if self.spec_name is not None:
            try:
                self.collection_setup()
                CollectionBase.setup_collection_history_settings(self)
            except Exception as exception:
                self.handle_setup_exception(exception)
        else:
            self.initial_load_gen = doc_generator(self.key,
                                                  0,
                                                  self.num_items,
                                                  key_size=self.key_size,
                                                  doc_size=self.doc_size,
                                                  doc_type=self.doc_type)
            self.update_load_gen = doc_generator(self.key,
                                                 0,
                                                 self.update_items,
                                                 key_size=self.key_size,
                                                 doc_size=self.doc_size,
                                                 doc_type=self.doc_type)
            self.delete_load_gen = doc_generator(self.key,
                                                 self.update_items,
                                                 self.delete_items,
                                                 key_size=self.key_size,
                                                 doc_size=self.doc_size,
                                                 doc_type=self.doc_type)
            if self.auto_reprovision:
                self.bucket_type = Bucket.Type.EPHEMERAL
            self.bucket_util.create_default_bucket(
                self.cluster,
                replica=self.num_replicas,
                bucket_type=self.bucket_type,
                ram_quota=self.bucket_size,
                storage=self.bucket_storage,
                vbuckets=self.bucket_num_vb)
            self.sleep(5, "Wait for bucket to accept SDK connections")

            if self.load_docs_using == "sirius_java_sdk":
                for bucket in self.cluster.buckets:
                    self.log.info(f"Creating Java SDK pool for {bucket.name}")
                    SiriusCouchbaseLoader.create_clients_in_pool(
                        self.cluster.master,
                        self.cluster.master.rest_username,
                        self.cluster.master.rest_password,
                        bucket.name,
                        req_clients=self.sdk_pool_capacity)
            elif self.cluster.sdk_client_pool:
                self.log.info("Creating SDK clients for client_pool")
                for bucket in self.cluster.buckets:
                    self.cluster.sdk_client_pool.create_clients(
                        self.cluster, bucket,
                        req_clients=self.sdk_pool_capacity,
                        compression_settings=self.sdk_compression)

            self.load_all_buckets(self.initial_load_gen,
                                  DocLoading.Bucket.DocOps.CREATE, 0,
                                  load_using=self.load_docs_using)

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.servers), self.durability_level)
        self.active_vb_in_failover_nodes = list()
        self.replica_vb_in_failover_nodes = list()
        self.get_vbucket_info_from_failover_nodes()
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

    def bareSetUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self.spec_name = self.input.param("bucket_spec", None)
        self._get_params()
        self.rest = ClusterRestAPI(self.orchestrator)
        if self.spec_name is None:
            self.initial_load_gen = doc_generator(self.key,
                                                  0,
                                                  self.num_items,
                                                  key_size=self.key_size,
                                                  doc_size=self.doc_size,
                                                  doc_type=self.doc_type)
            self.update_load_gen = doc_generator(self.key,
                                                 0,
                                                 self.update_items,
                                                 key_size=self.key_size,
                                                 doc_size=self.doc_size,
                                                 doc_type=self.doc_type)
            self.delete_load_gen = doc_generator(self.key,
                                                 self.update_items,
                                                 self.delete_items,
                                                 key_size=self.key_size,
                                                 doc_size=self.doc_size,
                                                 doc_type=self.doc_type)
        self.server_to_fail = self._servers_to_fail()
        self.servers_to_add = \
            self.cluster.servers[self.nodes_init:self.nodes_init
                                 + self.nodes_in]
        self.servers_to_remove = \
            self.cluster.servers[self.nodes_init
                                 - self.nodes_out:self.nodes_init]
        self.get_vbucket_info_from_failover_nodes()

    def tearDown(self):
        self.log.info("============AutoFailoverBaseTest teardown============")
        if self.range_scan_task is not None:
            self.range_scan_task.stop_task = True
            self.task.jython_task_manager.get_task_result(self.range_scan_task)
            result = CollectionUtils.get_range_scan_results(
                self.range_scan_task.fail_map, self.range_scan_task.expect_range_scan_failure, self.log)
            self.assertTrue(result, "unexpected failures in range scans")
        self.bucket_util.print_bucket_stats(self.cluster)
        self._get_params()
        self.server_to_fail = self._servers_to_fail()
        self.start_couchbase_server()
        self.sleep(10)
        self.server_to_fail = self.cluster.servers[:self.nodes_init]
        self.disable_firewall()
        self.rest = ClusterRestAPI(self.orchestrator)
        self.rest.reset_auto_failover_count()
        self.disable_autofailover()
        if self.disk_optimized_thread_settings:
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                num_writer_threads="default",
                num_reader_threads="default",
                num_storage_threads="default")
        super(AutoFailoverBaseTest, self).tearDown()

    def collection_setup(self):
        CollectionBase.deploy_buckets_from_spec_file(self)

        if self.fusion_test:
            # Override Fusion default settings
            self.override_fusion_settings()
            # Set Rate Limits
            status, content = ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                                fusion_sync_rate_limit=self.fusion_sync_rate_limit,
                                fusion_migration_rate_limit=self.fusion_migration_rate_limit)
            self.log.info(f"Status = {status}, Content = {content}")

        CollectionBase.create_clients_for_sdk_pool(self)
        CollectionBase.load_data_from_spec_file(self, "initial_load")
        if isinstance(self.range_scan_collections, int) \
                and self.range_scan_collections > 0:
            CollectionBase.range_scan_load_setup(self)

    def stop_continuous_loadgen(self):
        """
        End the indefinite load tasks _loadgen() started.

        They never finish on their own, so waiting on one blocks for ever.
        Safe to call more than once, and safe when the load was a fixed
        batch instead.
        :return: Nothing
        """
        for task in getattr(self, "loadgen_tasks", None) or []:
            if hasattr(task, "end_task"):
                task.end_task()

    def _loadgen(self):
        tasks = []
        if self.atomicity:
            # A continuous update load rather than a fixed batch, because
            # the disk failure has to be injected into a node that is
            # actively writing. failoverOnDataDiskIssues fires on disk
            # errors KV reports, and KV only reports errors on I/O it
            # actually issues. This branch previously did nothing at all -
            # it referenced a run_time_create_load_gen that exists nowhere
            # in the tree - so the suite injected a disk failure into an
            # idle node: on 172.23.104.173 the kernel logged 246 I/O
            # errors while memcached logged none, and no disk failover
            # ever fired.
            update_gen = doc_generator(self.key, 0, self.num_items,
                                       key_size=self.key_size,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type)
            for bucket in self.cluster.buckets:
                tasks.append(self.task.async_continuous_doc_ops(
                    self.cluster, bucket, update_gen, op_type="update",
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    load_using=self.load_docs_using))
        else:
            subsequent_load_gen = doc_generator(self.key,
                                                self.num_items,
                                                self.num_items*2,
                                                key_size=self.key_size,
                                                doc_size=self.doc_size,
                                                doc_type=self.doc_type)
            tasks = self.async_load_all_buckets(
                subsequent_load_gen, "create", 0,
                load_using=self.load_docs_using)
        return tasks

    def get_vbucket_info_from_failover_nodes(self):
        """
        Fetch active/replica vbucket list from the
        nodes which are going to be failed over
        """
        if not len(self.cluster.buckets):
            return
        bucket = self.cluster.buckets[0]
        # Reset the values
        self.active_vb_in_failover_nodes = list()
        self.replica_vb_in_failover_nodes = list()

        # Fetch new vbucket list
        for node in self.server_to_fail:
            cbstat = Cbstats(node)
            self.active_vb_in_failover_nodes += cbstat.vbucket_list(
                bucket.name, "active")
            self.replica_vb_in_failover_nodes += cbstat.vbucket_list(
                bucket.name, "replica")
            cbstat.disconnect()

    def async_load_all_buckets_atomicity(self, kv_gen, op_type, exp=0,
                                         batch_size=20):

        task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets, kv_gen, op_type, exp,
            batch_size=batch_size, process_concurrency=8,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            transaction_timeout=self.transaction_timeout,
            commit=self.transaction_commit, durability=self.durability_level)
        return task

    def load_all_buckets_atomicity(self, kv_gen, op_type, exp, batch_size=20):
        task = self.async_load_all_buckets(kv_gen, op_type, exp, batch_size)
        self.task_manager.get_task_result(task)

    def async_load_all_buckets(self, kv_gen, op_type, exp, batch_size=20,
                               load_using="default_loader"):
        tasks = []
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, kv_gen, op_type, exp,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                batch_size=batch_size, timeout_secs=self.sdk_timeout,
                process_concurrency=8, retries=self.sdk_retries,
                durability=self.durability_level, load_using=load_using)
            tasks.append(task)
        return tasks

    def load_all_buckets(self, kv_gen, op_type, exp, batch_size=20,
                         load_using="default_loader"):
        tasks = self.async_load_all_buckets(kv_gen, op_type, exp, batch_size,
                                            load_using=load_using)
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1.
        Rebalance the nodes in the end.
        Nodes are divided into groups iteratively
        i.e. 1st node in Group 1, 2nd in Group 2, 3rd in Group 1 and so on,
        when zone=2.
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        server_group_rest = ServerGroupsAPI(self.orchestrator)
        nodes = self.cluster_util.get_nodes(self.orchestrator,
                                            inactive_added=True)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [node for node in nodes
                                     if node.ip == self.orchestrator.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                okay = self.cluster_util.is_zone_exists(self.orchestrator,
                                                        zones[i])
                if not okay:
                    server_group_rest.create_server_group(zones[i])
                nodes_in_zone[zones[i]] = []
            # Divide the nodes between zones.
            nodes_in_cluster = \
                [node.ip for node in self.cluster_util.get_nodes_in_cluster(
                    self.cluster)]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.cluster.servers)):
                if self.cluster.servers[i].ip in nodes_in_cluster \
                        and self.cluster.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(
                        [node for node in nodes
                         if node.ip == self.cluster.servers[i].ip][0])
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = [node.ip for node in list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in self.cluster_util.get_nodes_in_zone(self.orchestrator, zones[i])]))]
                moved_nodes = []
                for otp_node in self.cluster_util.get_nodes(
                        self.orchestrator, inactive_added=True):
                    if otp_node.ip in node_in_zone:
                        moved_nodes.append(otp_node)
                self.cluster_util.shuffle_nodes_in_zones(
                    self.orchestrator, moved_nodes, zones[0], zones[i])
        self.zones = nodes_in_zone
        nodes = self.cluster_util.get_nodes(self.orchestrator,
                                            inactive_added=True)
        otpnodes = [node.id for node in nodes]
        nodes_to_remove = [node.id for node in nodes
                           if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        rest = ClusterRestAPI(self.orchestrator)
        started, _ = rest.rebalance(known_nodes=otpnodes,
                                    eject_nodes=nodes_to_remove)
        if started:
            result = RebalanceUtil(self.cluster).monitor_rebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))

    def enable_autofailover(self):
        """
        Enable the autofailover setting with the given timeout.
        :return: True If the setting was set with the timeout, else return
        False
        """
        status, st = self.rest.update_auto_failover_settings(
            'true', self.timeout, max_count=self.max_count, allow_ephemeral_failover_with_no_replicas=self.failover_ephemeral_no_replicas)
        return status

    def enable_autoreprovision(self):
        """
        Enable the enable_autoreprovision setting with num node failures
        :return: True If the setting was set, else return
        False
        """
        status, _ = self.rest.update_auto_reprovision_settings(
            'true', self.num_node_failures)
        return status

    def disable_autofailover(self):
        """
        Disable the autofailover setting.
        :return: True If the setting was disabled, else return
        False
        """
        status, _ = self.rest.update_auto_failover_settings('false')
        return status

    def disable_autoreprovision(self):
        """
        Disable the autoreprovision setting.
        :return: True If the setting was disabled, else return
        False
        """
        status, _ = self.rest.update_auto_reprovision_settings('false')
        return status

    def enable_autofailover_and_validate(self):
        """
        Enable autofailover with given timeout and then validate if the
        settings.
        :return: Nothing
        """
        status = self.enable_autofailover()
        self.assertTrue(status, "Failed to enable autofailover_settings!")
        self.sleep(5)
        _, settings = self.rest.get_auto_failover_settings()
        self.assertTrue(settings["enabled"], "Failed to enable "
                                             "autofailover_settings!")
        self.assertEqual(self.timeout, settings["timeout"],
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}"
                         .format(self.timeout, settings["timeout"]))

    def disable_autofailover_and_validate(self):
        """
        Disable autofailover setting and then validate if the setting was
        disabled.
        :return: Nothing
        """
        status = self.disable_autofailover()
        self.assertTrue(status, "Failed to change autofailover_settings!")
        _, settings = self.rest.get_auto_failover_settings()
        self.assertFalse(settings["enabled"],
                         "Failed to disable autofailover_settings!")

    def enable_firewall(self):
        """
        Enable firewall on the nodes to fail in the tests.
        :return: Nothing
        """
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "enable_firewall", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def disable_firewall(self):
        """
        Disable firewall on the nodes to fail in the tests
        :return: Nothing
        """
        self.time_start = time.time()
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "disable_firewall", self.timeout,
            self.pause_between_failover_action, False,
            self.timeout_buffer, False, auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def restart_couchbase_server(self):
        """
        Restart couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, node.port)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "restart_couchbase", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        # self.sleep(30, "Waiting for couchbase-server to come up")
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def stop_couchbase_server(self):
        """
        Stop couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, node.port)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "stop_couchbase", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))
        finally:
            self.start_couchbase_server()

    def start_couchbase_server(self):
        """
        Start the couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "start_couchbase", self.timeout, 0, False, self.timeout_buffer,
            False, auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def stop_restart_network(self):
        """
        Stop and restart network for said timeout period on the nodes to
        fail in the tests
        :return: Nothing
        """

        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "restart_network", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def restart_machine(self):
        """
        Restart the nodes to fail in the tests
        :return: Nothing
        """

        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "restart_machine", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))
        finally:
            for node in self.server_to_fail:
                for _ in range(0, 6):
                    try:
                        shell = RemoteMachineShellConnection(node)
                        o, r = shell.execute_command("/sbin/iptables -F")
                        _, _ = shell.execute_command("nft flush ruleset")
                        self.log.debug("Output: %s, Err: %s" % (o, r))
                        shell.disconnect()
                        break
                    except:
                        self.log.info("Unable to connect to the host. "
                                      "Machine has not restarted")
                        self.sleep(60, "Sleep for another minute and try "
                                       "again")

    def stop_memcached(self):
        """
        Stop the memcached on the nodes to fail in the tests
        :return: Nothing
        """
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip, 11211)
            node_down_timer_tasks.append(node_failure_timer_task)
        self.timeout_buffer += 3
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "stop_memcached", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))
        finally:
            task = AutoFailoverNodesFailureTask(
                self.task_manager, self.orchestrator, self.server_to_fail,
                "start_memcached", self.timeout, 0, False, 0,
                check_for_failover=False,
                auto_reprovision=self.auto_reprovision)
            self.task_manager.add_new_task(task)
            self.task_manager.get_task_result(task)
            # self.sleep(60)

    def split_network(self):
        """
        Split the network in the cluster. Stop network traffic from few
        nodes while allowing the traffic from rest of the cluster.
        :return: Nothing
        """
        self.time_start = time.time()
        if self.server_to_fail.__len__() < 2:
            self.fail("Need atleast 2 servers to fail")
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "network_split", self.timeout, self.pause_between_failover_action,
            False, self.timeout_buffer, auto_reprovision=self.auto_reprovision)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))
        self.disable_firewall()

    def bring_back_failed_nodes_up(self):
        """
        Bring back the failed nodes.
        :return: Nothing
        """
        if self.failover_action == "firewall":
            self.disable_firewall()
        elif self.failover_action == "stop_server":
            self.start_couchbase_server()

    def _servers_to_fail(self):
        """
        Select the nodes to be failed in the tests.
        :return: Nothing
        """
        if self.failover_orchestrator:
            servers_to_fail = self.cluster.servers[0:self.num_node_failures]
        else:
            servers_to_fail = self.cluster.servers[1:self.num_node_failures+1]
        return servers_to_fail

    def _get_params(self):
        """
        Initialize the test parameters.
        :return:  Nothing
        """
        self.timeout = self.input.param("timeout", 300)
        self.max_count = self.input.param("maxCount", 1)
        self.failover_action = self.input.param("failover_action",
                                                "stop_server")
        self.failover_orchestrator = self.input.param("failover_orchestrator",
                                                      False)
        self.multiple_node_failure = self.input.param("multiple_nodes_failure",
                                                      False)
        self.key_size = self.input.param("key_size", None)
        self.num_items = self.input.param("num_items", 1000000)
        self.update_items = self.input.param("update_items", 100000)
        self.delete_items = self.input.param("delete_items", 100000)
        self.add_back_node = self.input.param("add_back_node", True)
        self.recovery_strategy = self.input.param("recovery_strategy",
                                                  "delta")
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.services = self.input.param("services", None)
        self.zone = self.input.param("zone", 1)
        self.multi_services_node = self.input.param("multi_services_node",
                                                    False)
        self.pause_between_failover_action = self.input.param(
            "pause_between_failover_action", 0)
        self.remove_after_failover = self.input.param(
            "remove_after_failover", False)
        self.timeout_buffer = 120 if self.failover_orchestrator else 10
        # A disk failure is not observed the way the other actions are.
        # The injection unmounts with 'umount -l', which is lazy, so
        # memcached keeps writing through its open file descriptors and
        # ns_server is told of no disk errors at all; what eventually
        # fails the node over is ns_server wedging on the missing data
        # path, reported as 'The cluster manager did not respond' and
        # measured at ~57s from injection on 172.23.104.173 - against a
        # 20s window with disk_timeout=5. The other failure actions take
        # the node down at once and keep their tighter budget, so this is
        # deliberately a separate value rather than a wider one for all.
        self.disk_failover_buffer = self.input.param(
            "disk_failover_buffer", self.timeout_buffer + 180)
        failover_not_expected = (self.max_count == 1
                                 and self.num_node_failures > 1
                                 and self.pause_between_failover_action < self.timeout
                                 or self.num_replicas < 1)
        failover_not_expected = failover_not_expected or \
            (1 < self.max_count < self.num_node_failures and
             self.pause_between_failover_action < self.timeout or
             self.num_replicas < self.max_count)
        self.failover_expected = not failover_not_expected
        if self.failover_action == "restart_server":
            self.num_items *= 100
        self.orchestrator = self.cluster.servers[0] if not \
            self.failover_orchestrator else self.cluster.servers[
            self.num_node_failures]

    def reset_cluster(self):
        try:
            for node in self.cluster.servers:
                # Reset node
                ClusterRestAPI(node).reset_node()
                # If Ipv6 update dist_cfg file post server restart
                # to change distribution to IPv6
                shell = RemoteMachineShellConnection(node)
                if '.com' in node.ip or ':' in node.ip:
                    self.log.info("Updating dist_cfg for IPv6 Machines")
                    shell.update_dist_type()
                shell.disconnect()
            self.sleep(10)
        except Exception as ex:
            self.log.info(ex)

    failover_actions = {
        "firewall": enable_firewall,
        "stop_server": stop_couchbase_server,
        "restart_server": restart_couchbase_server,
        "restart_machine": restart_machine,
        "restart_network": stop_restart_network,
        "stop_memcached": stop_memcached,
        "network_split": split_network
    }

    def _auto_failover_message_present_in_logs(self, ipaddress):
        return any("Rebalance interrupted due to auto-failover of nodes ['ns_1@{0}']."
                   .format(ipaddress) in d.values()
                   for d in self.cluster_util.get_ui_logs(self.cluster.master, 10))

    def wait_for_failover_or_assert(self, expected_failover_count, timeout):
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            self.sleep(20)
        time_end = time.time()
        self.assertTrue(actual_failover_count == expected_failover_count,
                        "{0} nodes failed over, expected : {1}"
                        .format(actual_failover_count,
                                expected_failover_count))
        self.log.info("{0} nodes failed over as expected in {1} seconds"
                      .format(actual_failover_count, time_end - time_start))

    def get_failover_count(self):
        # Return number of nodes in failed_over state
        return len(self.cluster_util.get_nodes(
            self.cluster.master, active=False,
            inactive_failed=True))

    def validate_loadgen_tasks(self):
        def validate_durability_for_bucket(index, bucket):
            # Fetch all the failed docs from the tasks
            failed_docs = dict()
            failed_docs["create"] = self.loadgen_tasks[0].fail
            failed_docs["update"] = self.loadgen_tasks[1].fail
            failed_docs["delete"] = self.loadgen_tasks[2].fail
            try:
                failed_docs["read"] = self.loadgen_tasks[3].fail
            except:
                pass

            # Detect whether durability is going to succeed or not
            # based on the fail_over type and num_server to fail
            durability_success = self.durability_helper.durability_succeeds(
                bucket.name, self.cluster.master,
                induced_error=self.failover_action,
                failed_nodes=self.server_to_fail)

            no_error_ops = ["read"]
            error_expected_ops = []

            if durability_success:
                no_error_ops += ["create", "update", "delete"]
                # If durability is expected to pass,
                # we should not see any failure in create/delete/update
                self.assertTrue(len(failed_docs["create"]) == 0,
                                msg="Unexpected exception during 'create' {0}"
                                .format(failed_docs["create"]))
                self.assertTrue(len(failed_docs["update"]) == 0,
                                msg="Unexpected exception during 'update' {0}"
                                .format(failed_docs["update"]))
                self.assertTrue(len(failed_docs["delete"]) == 0,
                                msg="Unexpected exception during 'delete' {0}"
                                .format(failed_docs["delete"]))
            else:
                error_expected_ops += ["create", "update", "delete"]
                # If durability is expected to fail,
                # we should see atleast one failure in create/delete/update
                self.assertTrue(len(failed_docs["create"]) != 0,
                                msg="No exceptions in 'create' operation")
                self.assertTrue(len(failed_docs["update"]) != 0,
                                msg="No exceptions in 'update' operation")
                self.assertTrue(len(failed_docs["delete"]) != 0,
                                msg="No exceptions in 'delete' operation")

            # Fail the cases, if the errors seen in the unexpected op_type
            for op_type in no_error_ops:
                # Verification of CRUD failures
                self.assertTrue(len(failed_docs[op_type] == 0),
                                msg="Read operation failed: {0}"
                                .format(failed_docs["read"]))

            # Create SDK client for doc CRUD retry
            client = SDKClient(self.cluster, bucket)

            # Loop over all failed docs as per their op_types
            for op_type in error_expected_ops:
                for failed_doc in failed_docs[op_type]:
                    # Failures should be observed only in the vbuckets
                    # placed in the failure nodes
                    key = failed_doc["key"]
                    vb_num = self.bucket_util.get_vbucket_num_for_key(
                        key, bucket.numVBuckets)
                    msg = "Key '{0}' not found in failed nodes' vbucket list" \
                        .format(key)
                    err_msg = "Invalid exception {0} for vb failure"
                    key_in_active_vb = vb_num in self.active_vb_in_failover_nodes
                    key_in_replica_vb = vb_num in self.replica_vb_in_failover_nodes

                    # Checks whether the error happened on target vbucket only
                    self.assertTrue(key_in_active_vb or key_in_replica_vb,
                                    msg=msg)

                    # Validate the received exception for CRUD failure
                    if key_in_active_vb:
                        result = "ambiguous abort" in failed_doc["error"]
                        self.assertTrue(result,
                                        msg=err_msg.format(failed_doc["error"],
                                                           "active"))
                    elif key_in_replica_vb:
                        result = "ambiguous abort" in failed_doc["error"]
                        self.assertTrue(result,
                                        msg=err_msg.format(failed_doc["error"],
                                                           "active"))

                # Retry failed docs to verify it succeeds with no failure
                self.durability_helper.retry_with_no_error(
                    client, failed_docs[op_type], op_type,
                    timeout=self.sdk_timeout)

            # Closing the SDK client
            client.close()

        # Wait for all tasks to complete
        for task in self.loadgen_tasks:
            self.task_manager.get_task_result(task)

        # Validate the doc_errors only if durability is set
        if self.durability_level:
            for b_index, bucket_obj in enumerate(self.cluster.buckets):
                validate_durability_for_bucket(b_index, bucket_obj)

    def data_load_after_autofailover(self):
        self.bucket = self.cluster.buckets[0]
        gen_create = doc_generator(self.key, self.num_items*2,
                                   self.num_items * 3,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.bucket.numVBuckets)
        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                gen_create, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                transaction_timeout=self.transaction_timeout,
                commit=self.transaction_commit,
                durability=self.durability_level, sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
        else:
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_create, "create", 0,
                batch_size=10, replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)
            # Verify there is not failed docs in the task
            if len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed after autofailover")


class DiskAutoFailoverBasetest(AutoFailoverBaseTest):
    def setUp(self):
        super(DiskAutoFailoverBasetest, self).bareSetUp()
        self.log.info("=========Starting Diskautofailover base setup=========")
        self.original_data_path = self.cluster_util.fetch_data_path(self.orchestrator)
        self.reset_cluster()
        self.disk_location = self.input.param("data_location", "/data")
        self.disk_location_size = self.input.param("data_location_size", 5120)
        self.data_location = "{0}/data".format(self.disk_location)
        self.skip_collections_during_data_load = self.input.param(
            "skip_col_dict", None)
        self.range_scan_timeout = self.input.param("range_scan_timeout",
                                                   None)
        self.expect_range_scan_exceptions = self.input.param(
            "expect_range_scan_exceptions",
            ["com.couchbase.client.core.error.CouchbaseException: "
             "The range scan internal partition UUID could not be found on the server "])
        self.range_scan_collections = self.input.param(
            "range_scan_collections", None)
        self.key_size = self.input.param("key_size", None)
        self.range_scan_task = self.input.param("range_scan_task", None)
        self.skip_range_scan_collection_mutation = self.input.param(
            "skip_range_scan_collection_mutation", True)
        self.disk_timeout = self.input.param("disk_timeout", 120)
        self.read_loadgen = self.input.param("read_loadgen", False)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 200)

        self.log.info("Cleanup the cluster and set the data location "
                      "to the one specified by the test.")
        self.original_data_devices = {}
        # list.append is atomic under the GIL, so the worker threads can
        # record into this without further locking.
        self.node_setup_errors = list()
        threads = list()
        for server in self.cluster.servers:
            threads.append(Thread(target=self.__per_node_new_mount_partition,
                                  args=(server,)))
            threads[-1].start()

        for t in threads:
            t.join()

        # Every node is joined before failing, so one bad node does not
        # hide the state of the others.
        if self.node_setup_errors:
            self.fail(f"Failed to set the data location to "
                      f"{self.data_location} on: "
                      f"{'; '.join(self.node_setup_errors)}")

        self.services = self.cluster_util.get_services(
            self.cluster.servers[:self.nodes_init], None)
        self.task.rebalance(self.cluster,
                            self.cluster.servers[1:self.nodes_init], [],
                            services=self.services,
                            retry_get_process_num=self.retry_get_process_num)

        self.enable_tls_on_nodes()

        # Enable diag/eval on non-local hosts for all servers
        self.log.info("Enabling diag/eval on non-local hosts for all servers")
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()

        # Creating encryption keys
        encryption_result = self.encryption_util.setup_encryption_at_rest(
            cluster_master=self.cluster.master,
            bypass_encryption_func=self.bypass_encryption_setting,
            create_KMIP_secret=self.create_KMIP_secret,
            enable_encryption_at_rest=self.enable_encryption_at_rest,
            enable_config_encryption_at_rest=self.enable_config_encryption_at_rest,
            enable_log_encryption_at_rest=self.enable_log_encryption_at_rest,
            enable_audit_encryption_at_rest=self.enable_audit_encryption_at_rest,
            secret_rotation_interval=self.secret_rotation_interval,
            kmip_key_uuid=self.kmip_key_uuid,
            client_certs_path=self.client_certs_path,
            KMIP_pkcs8_file_name=self.KMIP_pkcs8_file_name,
            KMIP_cert_file_name=self.KMIP_cert_file_name,
            private_key_passphrase=self.private_key_passphrase,
            kmip_host_name=self.kmip_host_name,
            KMIP_for_config_encryption=self.KMIP_for_config_encryption,
            config_dekLifetime=self.config_dekLifetime,
            config_dekRotationInterval=self.config_dekRotationInterval,
            KMIP_for_log_encryption=self.KMIP_for_log_encryption,
            log_dekLifetime=self.log_dekLifetime,
            log_dekRotationInterval=self.log_dekRotationInterval,
            KMIP_for_audit_encryption=self.KMIP_for_audit_encryption,
            audit_dekLifetime=self.audit_dekLifetime,
            audit_dekRotationInterval=self.audit_dekRotationInterval
        )

        # Set the returned IDs back to self
        self.encryption_util.set_encryption_ids(self, encryption_result)

        self.auto_reprovision = self.input.param("auto_reprovision", False)
        self.bucket_util.add_rbac_user(self.cluster.master)
        self.disk_optimized_thread_settings = \
            self.input.param("disk_optimized_thread_settings", False)
        if self.disk_optimized_thread_settings:
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
        ClusterRestAPI(self.cluster.master).set_internal_settings(
            setting_name="magmaMinMemoryQuota", setting_value=256)
        if self.spec_name is None:
            if self.read_loadgen:
                self.bucket_size = self.input.param("bucket_size", 256)
            # create_default_bucket() defaults storage to magma, so
            # bucket_storage was parsed and then silently dropped: the
            # bucket came up magma however the test was invoked.
            self.bucket_util.create_default_bucket(self.cluster,
                                                   ram_quota=self.bucket_size,
                                                   replica=self.num_replicas,
                                                   storage=self.bucket_storage)
            self.load_all_buckets(self.initial_load_gen, "create", 0)
        else:
            try:
                self.collection_setup()
            except Exception as exception:
                self.handle_setup_exception(exception)

        # If updated, update in 'DurabilityHelper.durability_succeeds' as well
        self.failover_actions['disk_failure'] = self.fail_disk_via_disk_failure
        self.failover_actions['disk_full'] = self.fail_disk_via_disk_full

        self.loadgen_tasks = []
        self.log.info("=========Finished Diskautofailover base setup=========")

    def wait_for_ns_server_reachable(self, server, wait_time=300):
        """
        Wait until ns_server on 'server' answers REST.

        node-init only needs the node to be reachable; it does not need it
        to be healthy. is_ns_server_running() returns True only for status
        'healthy', but a node whose data directory has just been restored
        comes back in 'warmup' while it reloads its buckets - so waiting
        for 'healthy' fails on exactly the node the test wiped, while
        ns_server is up and serving REST perfectly well. Measured on
        172.23.104.173, which sat in 'warmup' through a 30s and then a
        120s wait and was answering /pools/default with 200 throughout.
        :param server: Node to wait for
        :param wait_time: Seconds to wait before giving up
        :return: True once ns_server answers, False if it never does
        """
        end_time = time.time() + wait_time
        while time.time() < end_time:
            try:
                status, content = ClusterRestAPI(server).node_details()
                if status:
                    self.log.debug(
                        f"{server.ip}: ns_server reachable, node status="
                        f"{content.get('status')}")
                    return True
            except Exception as e:
                # Building the REST client probes the node, so an
                # unreachable one raises here rather than returning.
                self.log.debug(f"{server.ip}: ns_server not reachable "
                               f"yet: {e}")
            self.sleep(5, f"Waiting for ns_server on {server.ip}")
        return False

    def prepare_data_location(self, shell, original_device, data_paths):
        """
        Give couchbase back the directories a restore has just uncovered.

        restore_partition() unmounts the loopback and, when there was no
        dedicated device to put back, leaves disk_location as the bare
        directory 'mkdir -p' created in create_new_partition(): root
        owned and empty. ns_server cannot start on a data path that does
        not exist, so the node never comes back and every later
        node-init gets connection-refused. Waiting longer for ns_server
        is not a substitute - it simply never comes up.

        A shell 'mkdir -p' is used rather than create_directory(): that
        one stats over sftp and raises EACCES on a root-owned mountpoint.
        :param shell: Open connection to the node
        :param original_device: Device restore_partition() put back, or
                                None when there was none. Recorded for the
                                log; the paths are recreated either way.
        :param data_paths: Paths couchbase must find on restart
        :return: Nothing
        """
        if original_device:
            # A real device is back, but not necessarily with the data
            # directories on it: create_new_partition() unmounts that
            # device and then removes and recreates disk_location, so the
            # paths couchbase is configured with are gone from the real
            # volume too. mkdir -p and the chown below are idempotent, so
            # doing this unconditionally costs nothing when they do exist.
            self.log.debug(f"{original_device} is back at the data location; "
                           f"ensuring {data_paths} exist on it")
        # An immutable flag on any component fails the mkdir below even as
        # root, which leaves the node with no data path at all.
        shell.clear_immutable(*data_paths)
        for path in data_paths:
            if not path:
                continue
            try:
                output, error = shell.execute_command(f"mkdir -p {path}")
                shell.log_command_output(output, error)
                shell.give_directory_permissions_to_couchbase(path)
            except Exception as e:
                self.log.error(f"Failed to recreate {path} on {shell.ip}: "
                               f"{e}")

    def tearDown(self):
        self.log.info("=========Starting Diskautofailover teardown ==========")
        # The test body stops these itself, but it may have failed before
        # reaching that point, and an indefinite task blocks the task
        # manager from shutting down.
        self.stop_continuous_loadgen()
        self.bucket_util.print_bucket_stats(self.cluster)
        self.targetMaster = True
        restore_errors = []
        if hasattr(self, "original_data_path"):
            self.bring_back_failed_nodes_up()
            # Before the restore removes the data directory, not after.
            # A node's configuration lives in
            # /opt/couchbase/var/lib/couchbase/config, not under
            # disk_location, so wiping the data directory of a node that
            # still owns a bucket leaves it believing it has vbucket files
            # that no longer exist: it comes back in 'warmup' and stays
            # there. super().tearDown() deletes the buckets far too late
            # to prevent that. Measured on 172.23.104.173.
            try:
                self.bucket_util.delete_all_buckets(self.cluster)
            except Exception as e:
                restore_errors.append(f"delete_all_buckets: {e}")
                self.log.error(f"Failed to delete buckets before restoring "
                               f"{self.disk_location}: {e}")
            for server in self.cluster.servers:
                shell = RemoteMachineShellConnection(server)
                shell.stop_couchbase()
                try:
                    shell.restore_partition(
                        self.disk_location,
                        self.original_data_devices.get(server.ip))
                except Exception as e:
                    restore_errors.append("{0}: {1}".format(server.ip, e))
                    self.log.error(
                        "Failed to restore {0} on {1}: {2}"
                        .format(self.disk_location, server.ip, e))
                finally:
                    # Before couchbase restarts, not after: it cannot
                    # start at all on a data path the restore removed.
                    self.prepare_data_location(
                        shell, self.original_data_devices.get(server.ip),
                        (self.disk_location, self.data_location,
                         self.original_data_path))
                    shell.start_couchbase()
                    shell.disconnect()
                self._initialize_node_with_new_data_location(
                    server, self.original_data_path)
        super(DiskAutoFailoverBasetest, self).tearDown()
        if restore_errors:
            self.fail(
                "Failed to restore original data partition on: {0}"
                .format("; ".join(restore_errors)))

    def __per_node_new_mount_partition(self, server):
        # Nothing may escape this method. An exception raised in a thread
        # never reaches the code that started it - Thread.join() returns
        # normally and the test goes on to pass - so self.fail() and
        # assertTrue() in here are silent unless the failure is carried
        # back to the caller by hand. Record it and let setUp raise it on
        # the main thread once every node has been joined.
        try:
            self._create_data_locations(server)
            if server == self.cluster.master:
                master_services = self.cluster_util.get_services(
                    self.cluster.servers[:1], self.services_init,
                    start_node=0)
            else:
                master_services = None
            if master_services:
                master_services = master_services[0].split(",")
            self._initialize_node_with_new_data_location(
                server, self.data_location, master_services)
            if self.use_https:
                self.set_ports_for_server(server, "ssl")
        except Exception as e:
            # AssertionError is an Exception, so this catches self.fail()
            # and assertTrue() from the helpers as well as a raised
            # ServerUnavailableException from a node that is still down.
            self.node_setup_errors.append(f"{server.ip}: {e}")
            self.log.error(f"Failed to set up the data location on "
                           f"{server.ip}: {e}")

    def enable_disk_autofailover(self):
        if self.disk_timeout < 5:
            shell = RemoteMachineShellConnection(self.orchestrator)
            curl_addr = "localhost:%s" % self.orchestrator.port
            if CbServer.use_https:
                curl_addr = " -k https://%s" % curl_addr

            shell.execute_command(
                "curl %s/diag/eval -u %s:%s -d 'ns_config:set("
                "{menelaus_web_auto_failover,"
                " min_data_disk_issues_timeperiod}, 1).'"
                % (curl_addr,
                   self.orchestrator.rest_username,
                   self.orchestrator.rest_password))
            shell.disconnect()
        status, _ = self.rest.update_auto_failover_settings(
            "true", self.timeout, fo_on_disk_issue="true",
            fo_on_disk_timeout=self.disk_timeout)
        return status

    def enable_disk_autofailover_and_validate(self):
        status = self.enable_disk_autofailover()
        self.assertTrue(status,
                        "Failed to enable disk autofailover for the cluster")
        self.sleep(5)
        _, settings = self.rest.get_auto_failover_settings()
        self.assertTrue(settings["enabled"],
                        "Failed to enable autofailover_settings!")
        self.assertEqual(self.timeout, settings["timeout"],
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}"
                         .format(self.timeout, settings["timeout"]))
        self.assertTrue(settings["failoverOnDataDiskIssues"]["enabled"],
                        "Failed to enable disk autofailover for the cluster")
        self.assertEqual(
            self.disk_timeout, settings["failoverOnDataDiskIssues"]["timePeriod"],
            f"Incorrect timeout period for disk failover set. "
            f"Expected Timeout: {self.disk_timeout}, "
            f"Actual timeout: {settings['failoverOnDataDiskIssues']['timePeriod']}")

    def disable_disk_autofailover(self, disable_autofailover=False):
        status, _ = self.rest.update_auto_failover_settings(
            "true" if not disable_autofailover else "false", self.timeout,
            fo_on_disk_issue="false", fo_on_disk_timeout=self.disk_timeout)
        return status

    def disable_disk_autofailover_and_validate(self,
                                               disable_autofailover=False):
        status = self.disable_disk_autofailover(disable_autofailover)
        self.assertTrue(status, "Failed to update autofailover settings. "
                                "Failed to disable disk failover settings")
        _, settings = self.rest.get_auto_failover_settings()
        self.assertEqual(not disable_autofailover, settings["enabled"],
                         "Failed to update autofailover settings.")
        self.assertFalse(settings["failoverOnDataDiskIssues"]["enabled"],
                         "Failed to disable disk autofailover for the cluster")

    def _create_data_locations(self, server):
        shell = RemoteMachineShellConnection(server)
        current_device = shell.get_mount_source(self.disk_location)
        # A '/dev/loop*' here is this suite's own leftover from a run that
        # did not restore itself, not a real data disk. Recording it makes
        # the teardown try to mount back a loop device it has just
        # detached and whose backing file it has deleted, which fails on
        # every node with "expected device '/dev/loopN', found 'None'".
        # There is nothing pristine behind it, so there is nothing to
        # restore.
        if shell.is_suite_device(current_device, self.disk_location):
            self.log.warning(
                f"{server.ip}: {self.disk_location} is already on "
                f"{current_device}, a leftover loopback from an earlier "
                f"run. Treating it as having no dedicated device.")
            current_device = None
        # Provisional only. create_new_partition() re-samples after it has
        # drained this suite's leftovers, so on a node an interrupted run
        # left dirty it sees the real device where this sees the leftover
        # loopback and records None. Whatever it actually unmounts is what
        # has to be put back, so its return value replaces this below.
        self.original_data_devices[server.ip] = current_device
        # Couchbase has to be down first. create_new_partition() unmounts
        # disk_location and detaches the loop device behind it, and while
        # memcached still holds files open on that filesystem the unmount
        # is only lazy and the detach fails with EBUSY however often it is
        # retried - 'losetup -d' merely arms autoclear, so the device frees
        # itself minutes later once couchbase lets go, long after setUp has
        # given up. restore_partition() documents the same requirement for
        # the reverse direction.
        shell.stop_couchbase()
        try:
            unmounted = shell.create_new_partition(self.disk_location,
                                                   self.disk_location_size)
            if unmounted:
                # It took a real device out of the way; that is the device
                # teardown must remount, whatever was recorded above.
                if unmounted != current_device:
                    self.log.info(
                        f"{server.ip}: {unmounted} was mounted at "
                        f"{self.disk_location} underneath a leftover from "
                        f"an earlier run; recording it as the device to "
                        f"restore instead of {current_device}")
                self.original_data_devices[server.ip] = unmounted
            shell.create_directory(self.data_location)
            shell.give_directory_permissions_to_couchbase(self.data_location)
        finally:
            shell.start_couchbase()
        shell.disconnect()

    def _initialize_node_with_new_data_location(self, server, data_location,
                                                services=None):
        init_tasks = []
        # Note: This won't support cluster_run
        init_port = server.port or CbServer.port
        if init_port == CbServer.ssl_port:
            init_port = CbServer.port

        shell_conn = RemoteMachineShellConnection(server)
        if not self.wait_for_ns_server_reachable(server, wait_time=30):
            shell_conn.start_couchbase()
            self.assertTrue(
                self.wait_for_ns_server_reachable(server, wait_time=300),
                f"{server.ip}: ns_server not reachable, cannot set the "
                f"data location to {data_location}")

        ClusterRestAPI(server).reset_node()
        self.assertTrue(
            self.wait_for_ns_server_reachable(server, wait_time=300),
            f"{server.ip}: ns_server not reachable after resetting the node")
        cb_cli = CbCli(shell_conn)
        output, error = cb_cli.node_init(node_init_data_path=data_location)
        cb_cli.disconnect()
        self.log.info(output)
        if error or "ERROR" in output:
            self.log.info(error)
            self.fail("Failed to set new data location. Check error message.")
        init_tasks.append(self.task.async_init_node(
            server, self.disabled_consistent_view,
            self.rebalanceIndexWaitingDisabled,
            self.rebalanceIndexPausingDisabled, self.maxParallelIndexers,
            self.maxParallelReplicaIndexers, init_port, self.quota_percent,
            services=services, gsi_type=self.gsi_type))
        for task in init_tasks:
            self.task.jython_task_manager.get_task_result(task)

    def fail_disk_via_disk_failure(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "disk_failure", self.timeout,
            self.pause_between_failover_action, self.failover_expected,
            self.disk_failover_buffer, failure_timers=node_down_timer_tasks,
            disk_timeout=self.disk_timeout, disk_location=self.disk_location,
            disk_size=self.disk_location_size)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))
        # The load exists to make the failure observable, and the verdict
        # is now in, so it is ended here rather than in each caller: seven
        # tests in this suite start it through _loadgen() and then wait on
        # it, and get_task_result() on an indefinite task never returns.
        # Ended before any self.fail() below, so a failing verdict stops
        # the load too.
        self.stop_continuous_loadgen()
        # AutoFailoverNodesFailureTask reports a missed failover through
        # set_warn(), which records task.exception but - unlike
        # set_exception() - does not raise, so get_task_result() returns
        # normally and a run where the failover never happened was
        # reported as a pass. The recorded exception is the whole verdict
        # of the task, so it has to be acted on here.
        if task.exception:
            self.fail(f"disk_failure injection on "
                      f"{[node.ip for node in self.server_to_fail]}: "
                      f"{task.exception}")

    def fail_disk_via_disk_full(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "disk_full", self.timeout, self.pause_between_failover_action,
            self.failover_expected, self.disk_failover_buffer,
            failure_timers=node_down_timer_tasks,
            disk_timeout=self.disk_timeout, disk_location=self.disk_location,
            disk_size=self.disk_location_size)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))
        # The load exists to make the failure observable, and the verdict
        # is now in, so it is ended here rather than in each caller: seven
        # tests in this suite start it through _loadgen() and then wait on
        # it, and get_task_result() on an indefinite task never returns.
        # Ended before any self.fail() below, so a failing verdict stops
        # the load too.
        self.stop_continuous_loadgen()
        # See fail_disk_via_disk_failure(): set_warn() records the verdict
        # without raising, so it has to be acted on explicitly.
        if task.exception:
            self.fail(f"disk_full injection on "
                      f"{[node.ip for node in self.server_to_fail]}: "
                      f"{task.exception}")

    def bring_back_failed_nodes_up(self):
        if self.failover_action == "disk_failure":
            task = AutoFailoverNodesFailureTask(
                self.task_manager, self.orchestrator, self.server_to_fail,
                "recover_disk_failure", self.timeout,
                self.pause_between_failover_action, expect_auto_failover=False,
                timeout_buffer=self.timeout_buffer, check_for_failover=False,
                disk_timeout=self.disk_timeout,
                disk_location=self.disk_location,
                disk_size=self.disk_location_size)
            self.task_manager.add_new_task(task)
            try:
                self.task_manager.get_task_result(task)
            except Exception as e:
                self.fail("Exception: {}".format(e))
        elif self.failover_action == "disk_full":
            task = AutoFailoverNodesFailureTask(
                self.task_manager, self.orchestrator, self.server_to_fail,
                "recover_disk_full_failure", self.timeout,
                self.pause_between_failover_action, expect_auto_failover=False,
                timeout_buffer=self.timeout_buffer, check_for_failover=False,
                disk_timeout=self.disk_timeout,
                disk_location=self.disk_location,
                disk_size=self.disk_location_size)
            self.task_manager.add_new_task(task)
            try:
                self.task_manager.get_task_result(task)
            except Exception as e:
                self.fail("Exception: {}".format(e))
        else:
            super(DiskAutoFailoverBasetest, self).bring_back_failed_nodes_up()
