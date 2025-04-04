# -*- coding: utf-8 -*-
import time

from BucketLib.bucket import Bucket
from Jython_tasks.task import AutoFailoverNodesFailureTask, NodeDownTimerTask
from basetestcase import ClusterSetup
from cb_constants import DocLoading, CbServer
from cb_tools.cbstats import Cbstats
from couchbase_cli import CouchbaseCLI
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from bucket_utils.bucket_ready_functions import CollectionUtils

from pytests.bucket_collections.collections_base import CollectionBase
from sdk_client3 import SDKClient

from java.lang import Exception as Java_base_exception


class AutoFailoverBaseTest(ClusterSetup):
    MAX_FAIL_DETECT_TIME = 120
    ORCHESTRATOR_TIMEOUT_BUFFER = 60

    def setUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self.spec_name = self.input.param("bucket_spec", None)
        self.auto_reprovision = self.input.param("auto_reprovision", False)
        self.skip_collections_during_data_load = self.input.param(
            "skip_col_dict", None)
        self.afo_delay_time = self.input.param("afo_delay_time", None)
        self._get_params()
        self.range_scan_timeout = self.input.param("range_scan_timeout",
                                                   None)
        self.expect_range_scan_exceptions = self.input.param(
            "expect_range_scan_exceptions",
            ["com.couchbase.client.core.error.CouchbaseException: "
             "The range scan internal partition UUID could not be found on the server"])
        self.range_scan_collections = self.input.param("range_scan_collections", None)
        self.rest = RestConnection(self.orchestrator)
        self.server_index_to_fail = self.input.param("server_index_to_fail",
                                                     None)
        self.key_size = self.input.param("key_size", 8)
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
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
        if self.spec_name is not None:
            try:
                self.collection_setup()
                CollectionBase.setup_collection_history_settings(self)
            except Java_base_exception as exception:
                self.handle_setup_exception(exception)
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
                ram_quota=self.bucket_size)
            self.sleep(5, "Wait for bucket to accept SDK connections")

            if self.cluster.sdk_client_pool:
                self.log.info("Creating SDK clients for client_pool")
                for bucket in self.cluster.buckets:
                    self.cluster.sdk_client_pool.create_clients(
                        self.cluster, bucket,
                        req_clients=self.sdk_pool_capacity,
                        compression_settings=self.sdk_compression)

            self.load_all_buckets(self.initial_load_gen,
                                  DocLoading.Bucket.DocOps.CREATE, 0)

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
        self.rest = RestConnection(self.orchestrator)
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

    def apply_io_throttling(self, read_limit, write_limit, device="/dev/xvda"):
        override_content = "[Service]\nIOReadBandwidthMax={0} {1}\nIOWriteBandwidthMax={0} {2}\n".format(
            device, read_limit, write_limit)
        commands = [
            "mkdir -p /etc/systemd/system/couchbase-server.service.d",
            "echo '{0}' | tee /etc/systemd/system/couchbase-server.service.d/override.conf".format(
                override_content),
            "systemctl daemon-reload",
            "systemctl restart couchbase-server.service",
            "ls -l /etc/systemd/system/couchbase-server.service.d"
        ]
        for node in self.server_to_fail:
            shell = RemoteMachineShellConnection(node)
            try:
                for command in commands:
                    self.log.info("Executing command: {0}".format(command))
                    output, error = shell.execute_command(command)
                    self.log.debug("Output: {0}".format(output))
                    self.log.debug("Error: {0}".format(error))
                    if command.startswith("echo"):
                        # Check if the file was created
                        check_file = "ls -l /etc/systemd/system/couchbase-server.service.d/override.conf"
                        self.log.debug("Checking file: {0}".format(check_file))
                        check_output, check_error = shell.execute_command(check_file)
                        self.log.debug("Check Output: {0}".format(check_output))
                        self.log.debug("Check Error: {0}".format(check_error))
            finally:
                shell.disconnect()

    def remove_io_throttling(self, device="/dev/xvda"):
        commands = [
            "rm -f /etc/systemd/system/couchbase-server.service.d/override.conf",
            "systemctl daemon-reload",
            "systemctl restart couchbase-server.service"
        ]
        for node in self.server_to_fail:
            self.log.info("Processing node: {0}".format(node.ip))
            shell = RemoteMachineShellConnection(node)
            try:
                for command in commands:
                    self.log.info("Executing command: {0}".format(command))
                    output, error = shell.execute_command(command)
                    self.log.debug("Output: {0}".format(output))
                    self.log.debug("Error: {0}".format(error))
            except Exception as e:
                self.log.error("Failed to execute command: {0}. Error: "
                               "1}".format(command, e))
            finally:
                shell.disconnect()

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
        self.rest = RestConnection(self.orchestrator)
        self.rest.reset_autofailover()
        self.disable_autofailover()
        if self.disk_optimized_thread_settings:
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="default",
                num_reader_threads="default",
                num_storage_threads="default")
        super(AutoFailoverBaseTest, self).tearDown()

    def collection_setup(self):
        CollectionBase.deploy_buckets_from_spec_file(self)
        CollectionBase.create_clients_for_sdk_pool(self)
        CollectionBase.load_data_from_spec_file(self, "initial_load")
        if self.range_scan_collections > 0:
            CollectionBase.range_scan_load_setup(self)

    def _loadgen(self):
        tasks = []
        if self.atomicity:
            # tasks.append(self.async_load_all_buckets_atomicity(
            #     self.run_time_create_load_gen, "create", 0))
            pass
        else:
            subsequent_load_gen = doc_generator(self.key,
                                                self.num_items,
                                                self.num_items*2,
                                                key_size=self.key_size,
                                                doc_size=self.doc_size,
                                                doc_type=self.doc_type)
            tasks = self.async_load_all_buckets(subsequent_load_gen,
                                                "create", 0)
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

    def async_load_all_buckets(self, kv_gen, op_type, exp, batch_size=20):
        tasks = []
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, kv_gen, op_type, exp,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                batch_size=batch_size, timeout_secs=self.sdk_timeout,
                process_concurrency=8, retries=self.sdk_retries,
                durability=self.durability_level)
            tasks.append(task)
        return tasks

    def load_all_buckets(self, kv_gen, op_type, exp, batch_size=20):
        tasks = self.async_load_all_buckets(kv_gen, op_type, exp, batch_size)
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
        rest = RestConnection(self.orchestrator)
        nodes = rest.get_nodes(inactive_added=True)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [node for node in nodes
                                     if node.ip == self.orchestrator.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                if not rest.is_zone_exist(zones[i]):
                    rest.add_zone(zones[i])
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
                                    set([node for node in rest.get_nodes_in_zone(zones[i])]))]
                moved_nodes = []
                for otp_node in rest.node_statuses():
                    if otp_node.ip in node_in_zone:
                        moved_nodes.append(otp_node)
                rest.shuffle_nodes_in_zones(moved_nodes, zones[0], zones[i])
        self.zones = nodes_in_zone
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses()
                           if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started, _ = rest.rebalance(otpNodes=otpnodes, ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))

    def enable_autofailover(self):
        """
        Enable the autofailover setting with the given timeout.
        :return: True If the setting was set with the timeout, else return
        False
        """
        status = self.rest.update_autofailover_settings(
            True, self.timeout, maxCount=self.max_count,
            allow_ephemeral_failover_with_no_replicas=self.allow_ephemeral_failover_with_no_replicas)
        return status

    def enable_autoreprovision(self):
        """
        Enable the enable_autoreprovision setting with num node failures
        :return: True If the setting was set, else return
        False
        """
        status = self.rest.update_autoreprovision_settings(True, self.num_node_failures)
        return status

    def disable_autofailover(self):
        """
        Disable the autofailover setting.
        :return: True If the setting was disabled, else return
        False
        """
        status = self.rest.update_autofailover_settings(False, 120)
        return status

    def disable_autoreprovision(self):
        """
        Disable the autoreprovision setting.
        :return: True If the setting was disabled, else return
        False
        """
        status = self.rest.update_autoreprovision_settings(False)
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
        settings = self.rest.get_autofailover_settings()
        self.assertTrue(settings.enabled, "Failed to enable "
                                          "autofailover_settings!")
        self.assertEqual(self.timeout, settings.timeout,
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}"
                         .format(self.timeout, settings.timeout))

    def disable_autofailover_and_validate(self):
        """
        Disable autofailover setting and then validate if the setting was
        disabled.
        :return: Nothing
        """
        status = self.disable_autofailover()
        self.assertTrue(status, "Failed to change autofailover_settings!")
        settings = self.rest.get_autofailover_settings()
        self.assertFalse(settings.enabled, "Failed to disable "
                                           "autofailover_settings!")

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
        self.allow_ephemeral_failover_with_no_replicas = self.input.param(
            "allow_ephemeral_failover_with_no_replicas", None)
        self.failover_expected_override =  self.input.param(
            "failover_expected_override", None)
        self.max_count = self.input.param("maxCount", 1)
        self.failover_action = self.input.param("failover_action",
                                                "stop_server")
        self.failover_orchestrator = self.input.param("failover_orchestrator",
                                                      False)
        self.multiple_node_failure = self.input.param("multiple_nodes_failure",
                                                      False)
        self.doc_size = self.input.param("doc_size", 10)
        self.key_size = self.input.param("key_size", 0)
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
        failover_not_expected = (self.max_count == 1
                                 and self.num_node_failures > 1
                                 and self.pause_between_failover_action < self.timeout
                                 or self.num_replicas < 1)
        failover_not_expected = failover_not_expected or \
            (1 < self.max_count < self.num_node_failures and
             self.pause_between_failover_action < self.timeout or
             self.num_replicas < self.max_count)
        self.failover_expected = not failover_not_expected
        if self.failover_expected_override is not None:
            self.failover_expected = self.failover_expected_override
        if self.failover_action is "restart_server":
            self.num_items *= 100
        self.orchestrator = self.cluster.servers[0] if not \
            self.failover_orchestrator else self.cluster.servers[
            self.num_node_failures]

    def reset_cluster(self):
        try:
            for node in self.cluster.servers:
                # Reset node
                rest = RestConnection(node)
                rest.reset_node()

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
                   .format(ipaddress) in d.values() for d in self.rest.get_logs(10))

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
        rest = RestConnection(self.cluster.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

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
                        key, bucket.num_vbuckets)
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
        gen_create = doc_generator(self.key, self.num_items*2,
                                   self.num_items * 3,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)
        self.bucket = self.cluster.buckets[0]
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
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)
            # Verify there is not failed docs in the task
            if len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed after autofailover")


class DiskAutoFailoverBasetest(AutoFailoverBaseTest):
    def setUp(self):
        super(DiskAutoFailoverBasetest, self).bareSetUp()
        self.log.info("=========Starting Diskautofailover base setup=========")
        self.original_data_path = self.rest.get_data_path()
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
        self.key_size = self.input.param("key_size", 8)
        self.range_scan_task = self.input.param("range_scan_task", None)
        self.skip_range_scan_collection_mutation = self.input.param(
            "skip_range_scan_collection_mutation", True)
        self.disk_timeout = self.input.param("disk_timeout", 120)
        self.read_loadgen = self.input.param("read_loadgen", False)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 200)

        self.log.info("Cleanup the cluster and set the data location "
                      "to the one specified by the test.")
        for server in self.cluster.servers:
            self._create_data_locations(server)
            if server == self.cluster.master:
                master_services = self.cluster_util.get_services(
                    self.cluster.servers[:1], self.services_init, start_node=0)
            else:
                master_services = None
            if master_services:
                master_services = master_services[0].split(",")
            self._initialize_node_with_new_data_location(
                server, self.data_location, master_services)
            if self.use_https:
                self.set_ports_for_server(server, "ssl")

        self.services = self.cluster_util.get_services(
            self.cluster.servers[:self.nodes_init], None)
        self.task.rebalance(self.cluster,
                            self.cluster.servers[1:self.nodes_init], [],
                            services=self.services,
                            retry_get_process_num=self.retry_get_process_num)

        self.enable_tls_on_nodes()

        self.auto_reprovision = self.input.param("auto_reprovision", False)
        self.bucket_util.add_rbac_user(self.cluster.master)
        self.disk_optimized_thread_settings = \
            self.input.param("disk_optimized_thread_settings", False)
        if self.disk_optimized_thread_settings:
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
        RestConnection(self.cluster.master).set_internalSetting("magmaMinMemoryQuota", 256)
        if self.spec_name is None:
            if self.read_loadgen:
                self.bucket_size = self.input.param("bucket_size", 256)
            self.bucket_util.create_default_bucket(self.cluster,
                                                   ram_quota=self.bucket_size,
                                                   replica=self.num_replicas)
            self.load_all_buckets(self.initial_load_gen, "create", 0)
        else:
            try:
                self.collection_setup()
            except Java_base_exception as exception:
                self.handle_setup_exception(exception)
            except Exception as exception:
                self.handle_setup_exception(exception)

        # If updated, update in 'DurabilityHelper.durability_succeeds' as well
        self.failover_actions['disk_failure'] = self.fail_disk_via_disk_failure
        self.failover_actions['disk_full'] = self.fail_disk_via_disk_full

        self.loadgen_tasks = []
        self.log.info("=========Finished Diskautofailover base setup=========")

    def tearDown(self):
        self.log.info("=========Starting Diskautofailover teardown ==========")
        self.bucket_util.print_bucket_stats(self.cluster)
        self.targetMaster = True
        if hasattr(self, "original_data_path"):
            self.bring_back_failed_nodes_up()
            for server in self.cluster.servers:
                self._initialize_node_with_new_data_location(
                    server, self.original_data_path)
        super(DiskAutoFailoverBasetest, self).tearDown()

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
        status = self.rest.update_autofailover_settings(
            True, self.timeout, enable_disk_failure=True,
            disk_timeout=self.disk_timeout)
        return status

    def enable_disk_autofailover_and_validate(self):
        status = self.enable_disk_autofailover()
        self.assertTrue(status,
                        "Failed to enable disk autofailover for the cluster")
        self.sleep(5)
        settings = self.rest.get_autofailover_settings()
        self.assertTrue(settings.enabled, "Failed to enable "
                                          "autofailover_settings!")
        self.assertEqual(self.timeout, settings.timeout,
                         "Incorrect timeout set. Expected timeout : {0} "
                         "Actual timeout set : {1}"
                         .format(self.timeout, settings.timeout))
        self.assertTrue(settings.failoverOnDataDiskIssuesEnabled,
                        "Failed to enable disk autofailover for the cluster")
        self.assertEqual(
            self.disk_timeout, settings.failoverOnDataDiskIssuesTimeout,
            "Incorrect timeout period for disk failover set. "
            "Expected Timeout: {0} Actual timeout: {1}"
            .format(self.disk_timeout,
                    settings.failoverOnDataDiskIssuesTimeout))

    def disable_disk_autofailover(self, disable_autofailover=False):
        status = self.rest.update_autofailover_settings(
            not disable_autofailover, self.timeout, enable_disk_failure=False,
            disk_timeout=self.disk_timeout)
        return status

    def disable_disk_autofailover_and_validate(self,
                                               disable_autofailover=False):
        status = self.disable_disk_autofailover(disable_autofailover)
        self.assertTrue(status, "Failed to update autofailover settings. "
                                "Failed to disable disk failover settings")
        settings = self.rest.get_autofailover_settings()
        self.assertEqual(not disable_autofailover, settings.enabled,
                         "Failed to update autofailover settings.")
        self.assertFalse(settings.failoverOnDataDiskIssuesEnabled,
                         "Failed to disable disk autofailover for the cluster")

    def _create_data_locations(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.create_new_partition(self.disk_location, self.disk_location_size)
        shell.create_directory(self.data_location)
        shell.give_directory_permissions_to_couchbase(self.data_location)
        shell.disconnect()

    def _initialize_node_with_new_data_location(self, server, data_location,
                                                services=None):
        init_tasks = []
        # Note: This won't support cluster_run
        init_port = server.port or CbServer.port
        if init_port == CbServer.ssl_port:
            init_port = CbServer.port
        cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
        cli.hostname = "%s:%s" % (server.ip, init_port)
        output, error, _ = cli.node_init(data_location, None, None)
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
            self.timeout_buffer, failure_timers=node_down_timer_tasks,
            disk_timeout=self.disk_timeout, disk_location=self.disk_location,
            disk_size=self.disk_location_size)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def fail_disk_via_disk_full(self):
        node_down_timer_tasks = []
        for node in self.server_to_fail:
            node_failure_timer_task = NodeDownTimerTask(node.ip)
            node_down_timer_tasks.append(node_failure_timer_task)
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "disk_full", self.timeout, self.pause_between_failover_action,
            self.failover_expected, self.timeout_buffer,
            failure_timers=node_down_timer_tasks,
            disk_timeout=self.disk_timeout, disk_location=self.disk_location,
            disk_size=self.disk_location_size)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

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
