import time

from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_cli import CouchbaseCLI
from couchbase_helper.documentgenerator import DocumentGenerator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from Jython_tasks.task import AutoFailoverNodesFailureTask, NodeDownTimerTask


class AutoFailoverBaseTest(BaseTestCase):
    MAX_FAIL_DETECT_TIME = 120
    ORCHESTRATOR_TIMEOUT_BUFFER = 60

    def setUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self._get_params()
        self.rest = RestConnection(self.orchestrator)
        self.initial_load_gen = self.get_doc_generator(0, self.num_items)
        self.update_load_gen = self.get_doc_generator(0, self.update_items)
        self.delete_load_gen = self.get_doc_generator(self.update_items,
                                                      self.delete_items)
        self.set_up_cluster()
        self.load_all_buckets(self.initial_load_gen, "create", 0)
        self.server_index_to_fail = self.input.param("server_index_to_fail",
                                                     None)
        self.new_replica = self.input.param("new_replica", None)
        self.replica_update_during = self.input.param("replica_update_during",
                                                      None)
        if self.server_index_to_fail is None:
            self.server_to_fail = self._servers_to_fail()
        else:
            self.server_to_fail = [self.cluster.servers[self.server_index_to_fail]]
        self.servers_to_add = self.cluster.servers[self.nodes_init:self.nodes_init +
                                                   self.nodes_in]
        self.servers_to_remove = self.cluster.servers[self.nodes_init -
                                                      self.nodes_out:self.nodes_init]
        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.servers), self.durability_level)
        self.active_vb_in_failover_nodes = list()
        self.replica_vb_in_failover_nodes = list()
        self.get_vbucket_info_from_failover_nodes()
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

    def bareSetUp(self):
        super(AutoFailoverBaseTest, self).setUp()
        self._get_params()
        self.rest = RestConnection(self.orchestrator)
        self.initial_load_gen = self.get_doc_generator(0, self.num_items)
        self.update_load_gen = self.get_doc_generator(0, self.update_items)
        self.delete_load_gen = self.get_doc_generator(self.update_items,
                                                      self.delete_items)
        self.server_to_fail = self._servers_to_fail()
        self.servers_to_add = self.cluster.servers[self.nodes_init:self.nodes_init +
                                                   self.nodes_in]
        self.servers_to_remove = self.cluster.servers[self.nodes_init -
                                                      self.nodes_out:self.nodes_init]
        self.get_vbucket_info_from_failover_nodes()

    def tearDown(self):
        self.log.info("============AutoFailoverBaseTest teardown============")
        self._get_params()
        self.server_to_fail = self._servers_to_fail()
        self.start_couchbase_server()
        self.sleep(10)
        self.disable_firewall()
        self.rest = RestConnection(self.orchestrator)
        self.rest.reset_autofailover()
        self.disable_autofailover()
        self.cluster_util.check_for_panic_and_mini_dumps(self.servers)
        self._cleanup_cluster()
        super(AutoFailoverBaseTest, self).tearDown()

    def _loadgen(self):
        tasks = []
        if self.atomicity:
            tasks.append(self.async_load_all_buckets_atomicity(self.run_time_create_load_gen,
                                                 "create", 0))

        else:
            tasks.extend(self.async_load_all_buckets(self.run_time_create_load_gen,
                                                     "create", 0))
            tasks.extend(self.async_load_all_buckets(self.update_load_gen,
                                                     "update", 0))
            tasks.extend(self.async_load_all_buckets(self.delete_load_gen,
                                                 "delete", 0))
        return tasks


    def set_up_cluster(self):
        nodes_init = self.cluster.servers[1:self.nodes_init] \
                     if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(replica=self.num_replicas)
        self.bucket_util.add_rbac_user()
        self.sleep(10)

    def get_vbucket_info_from_failover_nodes(self):
        """
        Fetch active/replica vbucket list from the
        nodes which are going to be failed over
        """
        if not len(self.bucket_util.buckets):
            return
        bucket = self.bucket_util.buckets[0]
        # Reset the values
        self.active_vb_in_failover_nodes = list()
        self.replica_vb_in_failover_nodes = list()

        # Fetch new vbucket list
        for node in self.server_to_fail:
            shell_conn = RemoteMachineShellConnection(node)
            cbstat = Cbstats(shell_conn)
            self.active_vb_in_failover_nodes += cbstat.vbucket_list(
                bucket.name, "active")
            self.replica_vb_in_failover_nodes += cbstat.vbucket_list(
                bucket.name, "replica")
            shell_conn.disconnect()

    def get_doc_generator(self, start, end):
        age = range(5)
        first = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = '{{ "age": {0}, "first_name": "{1}", "body": "{2}"}}'
        generator = DocumentGenerator(self.key, template, age, first, body,
                                      start=start, end=end)
        return generator

    def async_load_all_buckets_atomicity(self, kv_gen, op_type, exp=0, batch_size=20):

        task = self.task.async_load_gen_docs_atomicity(
                        self.cluster,self.bucket_util.buckets, kv_gen, op_type,exp,
                        batch_size=batch_size,process_concurrency=8,timeout_secs=self.sdk_timeout,retries=self.sdk_retries,
                        transaction_timeout=self.transaction_timeout, commit=self.transaction_commit,durability=self.durability_level)
        return task


    def load_all_buckets_atomicity(self, kv_gen, op_type, exp, batch_size=20):
        task = self.async_load_all_buckets(kv_gen, op_type, exp, batch_size)
        self.task_manager.get_task_result(task)

    def async_load_all_buckets(self, kv_gen, op_type, exp, batch_size=20):
        tasks = []
        for bucket in self.bucket_util.buckets:
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
        serverinfo = self.orchestrator
        rest = RestConnection(serverinfo)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [serverinfo.ip]}
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
            nodes_in_cluster = [node.ip for node in self.cluster_util.get_nodes_in_cluster()]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.cluster.servers)):
                if self.cluster.servers[i].ip in nodes_in_cluster \
                        and self.cluster.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(self.cluster.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        self.zones = nodes_in_zone
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses()
                           if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes, ejectedNodes=nodes_to_remove)
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
            True, self.timeout, canAbortRebalance=self.can_abort_rebalance,
            maxCount=self.max_count,
            enableServerGroup=self.server_group_failover)
        return status

    def disable_autofailover(self):
        """
        Disable the autofailover setting.
        :return: True If the setting was disabled, else return
        False
        """
        status = self.rest.update_autofailover_settings(False, 120, False)
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
        self.assertEqual(self.can_abort_rebalance, settings.can_abort_rebalance,
                         "Incorrect can_abort_rebalance set. Expected can_abort_rebalance : {0} "
                         "Actual can_abort_rebalance set : {1}"
                         .format(self.can_abort_rebalance, settings.can_abort_rebalance))

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
            self.timeout_buffer, failure_timers=node_down_timer_tasks)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
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
            self.timeout_buffer, False)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
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
            self.timeout_buffer, failure_timers=node_down_timer_tasks)
        self.task_manager.add_new_task(task)
        self.sleep(30, "Waiting for couchbase-server to come up")
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
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
            self.timeout_buffer, failure_timers=node_down_timer_tasks)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception as e:
            self.fail("Exception: {}".format(e))

    def start_couchbase_server(self):
        """
        Start the couchbase server on the nodes to fail in the tests
        :return: Nothing
        """
        task = AutoFailoverNodesFailureTask(
            self.task_manager, self.orchestrator, self.server_to_fail,
            "start_couchbase", self.timeout, 0, False, self.timeout_buffer,
            False)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
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
            self.timeout_buffer, failure_timers=node_down_timer_tasks)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
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
            self.timeout_buffer, failure_timers=node_down_timer_tasks)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:

            self.fail("Exception: {}".format(e))
        finally:
            self.sleep(120, "Sleeping for 2 min for the machines to restart")
            for node in self.server_to_fail:
                for _ in range(0, 2):
                    try:
                        shell = RemoteMachineShellConnection(node)
                        shell.disconnect()
                        break
                    except Exception:
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
            self.timeout_buffer, failure_timers=node_down_timer_tasks)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
            self.fail("Exception: {}".format(e))
        finally:
            task = AutoFailoverNodesFailureTask(
                self.task_manager, self.orchestrator, self.server_to_fail,
                "start_memcached", self.timeout, 0, False, 0,
                check_for_failover=False)
            self.task_manager.add_new_task(task)
            self.task_manager.get_task_result(task)

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
            False, self.timeout_buffer)
        self.task_manager.add_new_task(task)
        try:
            self.task_manager.get_task_result(task)
        except Exception, e:
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
        self.timeout = self.input.param("timeout", 60)
        self.max_count = self.input.param("maxCount", 1)
        self.server_group_failover = self.input.param("serverGroupFailover",
                                                      False)
        self.failover_action = self.input.param("failover_action",
                                                "stop_server")
        self.failover_orchestrator = self.input.param("failover_orchestrator",
                                                      False)
        self.multiple_node_failure = self.input.param("multiple_nodes_failure",
                                                      False)
        self.doc_size = self.input.param("doc_size", 10)
        self.key_size = self.input.param("key_size", 0)
        self.key = 'test_docs'.rjust(self.key_size, '0')
        self.num_items = self.input.param("num_items", 1000000)
        self.update_items = self.input.param("update_items", 100000)
        self.delete_items = self.input.param("delete_items", 100000)  
        self.add_back_node = self.input.param("add_back_node", True)
        self.recovery_strategy = self.input.param("recovery_strategy",
                                                  "delta")
        self.multi_node_failures = self.input.param("multi_node_failures",
                                                    False)
        self.can_abort_rebalance = self.input.param("can_abort_rebalance",
                                                    True)
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
        if self.failover_action is "restart_server":
            self.num_items *= 100
        self.orchestrator = self.cluster.servers[0] if not \
            self.failover_orchestrator else self.cluster.servers[
            self.num_node_failures]

    def _cleanup_cluster(self):
        """
        Cleaup the cluster. Delete all the buckets in the nodes and remove
        the nodes from any cluster that has been formed.
        :return:
        """
        self.bucket_util.delete_all_buckets(self.cluster.servers)

    def reset_cluster(self):
        try:
            for node in self.cluster.servers:
                shell = RemoteMachineShellConnection(node)
                # Start node
                rest = RestConnection(node)
                data_path = rest.get_data_path()
                # Stop node
                shell.stop_server()
                # Delete Path
                shell.cleanup_data_config(data_path)
                shell.start_server()

                # If Ipv6 update dist_cfg file post server restart
                # to change distribution to IPv6
                if '.com' in node.ip or ':' in node.ip:
                    self.log.info("Updating dist_cfg for IPv6 Machines")
                    shell.update_dist_type()
                shell.disconnect()
            self.sleep(10)
        except Exception, ex:
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
            time.sleep(20)
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
                bucket.name, induced_error=self.failover_action,
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
            client = SDKClient(RestConnection(self.cluster.master), bucket)

            # Loop over all failed docs as per their op_types
            for op_type in error_expected_ops:
                for failed_doc in failed_docs[op_type]:
                    # Failures should be observed only in the vbuckets
                    # placed in the failure nodes
                    key = failed_doc["key"]
                    vb_num = self.bucket_util.get_vbucket_num_for_key(key)
                    msg = "Key '{0}' not found in failed nodes' vbucket list" \
                        .format(key)
                    err_msg = "Invalid exception {0} for vb failure"
                    key_in_active_vb = vb_num in self.active_vb_in_failover_nodes
                    key_in_replica_vb = vb_num in self.replica_vb_in_failover_nodes

                    # Checks whether the error happened on target vbucket only
                    self.assetTrue(key_in_active_vb or key_in_replica_vb,
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
            for b_index, bucket_obj in enumerate(self.bucket_util.buckets):
                validate_durability_for_bucket(b_index, bucket_obj)


class DiskAutoFailoverBasetest(AutoFailoverBaseTest):
    def setUp(self):
        super(DiskAutoFailoverBasetest, self).bareSetUp()
        self.log.info("=========Starting Diskautofailover base setup=========")
        self.original_data_path = self.rest.get_data_path()
        self.reset_cluster()
        self.disk_location = self.input.param("data_location", "/data")
        self.disk_location_size = self.input.param("data_location_size", 5120)
        self.data_location = "{0}/data".format(self.disk_location)
        self.disk_timeout = self.input.param("disk_timeout", 120)
        self.read_loadgen = self.input.param("read_loadgen", False)
        self.log.info("Cleanup the cluster and set the data location to the one specified by the test.")
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
        self.services = self.cluster_util.get_services(self.cluster.servers[:self.nodes_init], None)
        self.task.rebalance(self.cluster.servers[:1],
                            self.cluster.servers[1:self.nodes_init],
                            [], services=self.services)
        self.bucket_util.add_rbac_user()
        if self.read_loadgen:
            self.bucket_size = 100
        self.bucket_util.create_default_bucket(ram_quota=self.bucket_size,
                                               replica=self.num_replicas)
        self.load_all_buckets(self.initial_load_gen, "create", 0)

        # If updated, update in 'DurabilityHelper.durability_succeeds' as well
        self.failover_actions['disk_failure'] = self.fail_disk_via_disk_failure
        self.failover_actions['disk_full'] = self.fail_disk_via_disk_full

        self.loadgen_tasks = []
        self.log.info("=========Finished Diskautofailover base setup=========")

    def tearDown(self):
        self.log.info("=========Starting Diskautofailover teardown ==========")
        self.targetMaster = True
        if hasattr(self, "original_data_path"):
            self.bring_back_failed_nodes_up()
            self.reset_cluster()
            for server in self.cluster.servers:
                self._initialize_node_with_new_data_location(server, self.original_data_path)
        self.log.info("=========Finished Diskautofailover teardown ==========")

    def enable_disk_autofailover(self):
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

    def disable_disk_autofailover_and_validate(self, disable_autofailover=False):
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
        init_port = server.port or '8091'
        init_tasks = []
        cli = CouchbaseCLI(server, server.rest_username, server.rest_password)
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
            services=services, index_quota_percent=self.index_quota_percent,
            gsi_type=self.gsi_type))
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
        except Exception, e:
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
        except Exception, e:
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
            except Exception, e:
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
            except Exception, e:
                self.fail("Exception: {}".format(e))
        else:
            super(DiskAutoFailoverBasetest, self).bring_back_failed_nodes_up()
