# -*- coding: utf-8 -*-
from couchbase_helper.documentgenerator import doc_generator
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from membase.api.exception import RebalanceFailedException, \
    ServerUnavailableException
from membase.api.rest_client import RestConnection


class MultiNodeAutoFailoverTests(AutoFailoverBaseTest):
    def setUp(self):
        super(MultiNodeAutoFailoverTests, self).setUp()
        self.data_load_spec = self.input.param("data_load_spec",
                                               "volume_test_load")
        self.master = self.servers[0]

    def tearDown(self):
        super(MultiNodeAutoFailoverTests, self).tearDown()

    def _is_failover_expected(self, failure_node_number):
        failover_not_expected = (
                self.max_count == 1 and failure_node_number > 1 and
                self.pause_between_failover_action <
                self.timeout or self.num_replicas < 1)
        failover_not_expected = failover_not_expected or (
                1 < self.max_count < failure_node_number and
                self.pause_between_failover_action < self.timeout or
                self.num_replicas < failure_node_number)
        return not failover_not_expected

    def _multi_node_failover(self):
        servers_to_fail = self.server_to_fail
        for i in range(self.max_count):
            self.server_to_fail = [servers_to_fail[i]]
            self.failover_expected = self._is_failover_expected(i + 1)
            self.failover_actions[self.failover_action](self)
            self.sleep(self.timeout)

    def test_autofailover(self):
        """
        Test the basic autofailover for different failure scenarios.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required.
        3. Disable autofailover and validate.
        :return: Nothing
        """
        self.enable_autofailover_and_validate()
        self.sleep(5)
        tasks = self.subsequent_load_gen()
        self._multi_node_failover()
        if self.spec_name is None:
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(tasks)
        self.disable_autofailover_and_validate()

    def _get_server_group_nodes(self, server_group):
        servers_in_group = self.zones[server_group]
        server_group_nodes = []
        for server in self.servers:
            if server.ip in servers_in_group:
                server_group_nodes.append(server)
        return server_group_nodes

    def test_autofailover_for_server_group(self):
        self.enable_autofailover_and_validate()
        self.shuffle_nodes_between_zones_and_rebalance()
        self.sleep(30,"waiting")
        self.server_to_fail = self._get_server_group_nodes("Group 2")
        self.failover_expected = True
        tasks = self.subsequent_load_gen()
        try:
            self.failover_actions[self.failover_action](self)
        except:
            result = self._check_for_autofailover_initiation_for_server_group_failover(self.server_to_fail)
            self.assertTrue(result,
                            "Server group failover msg was not seen in logs")
        finally:
            self.sleep(300)
            self.start_couchbase_server()
            if self.spec_name is None:
                for task in tasks:
                    self.task.jython_task_manager.get_task_result(task)
            else:
                self.wait_for_async_data_load_to_complete(tasks)

    def test_autofailover_during_rebalance(self):
        """
        Test autofailover for different failure scenarios while
        rebalance
        of nodes in progress
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing nodes.
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.

        :return: Nothing
        """
        self.enable_autofailover_and_validate()
        self.sleep(5)
        rebalance_task = self.task.async_rebalance(self.servers,
                                                   self.servers_to_add,
                                                   self.servers_to_remove)
        self.sleep(5)
        tasks = self.subsequent_load_gen()
        self._multi_node_failover()
        try:
            rebalance_task.result()
        except RebalanceFailedException:
            pass
        except ServerUnavailableException:
            pass
        except Exception:
            pass
        else:
            self.fail("Rebalance should fail since a node went down")
        finally:
            if self.spec_name is None:
                for task in tasks:
                    self.task.jython_task_manager.get_task_result(task)
            else:
                self.wait_for_async_data_load_to_complete(tasks)
            self.disable_autofailover_and_validate()

    def test_autofailover_after_rebalance(self):
        """
        Test autofailover for different failure scenarios after
        rebalance
        of nodes
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing
        nodes and
        wait for the rebalance to be completed
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.
        :return: Nothing
        """
        self.enable_autofailover_and_validate()
        self.sleep(5)
        rebalance_success = self.task.rebalance(self.servers,
                                                self.servers_to_add,
                                                self.servers_to_remove)
        if not rebalance_success:
            self.disable_firewall()
            self.fail("Rebalance failed. Check logs")
        tasks = self.subsequent_load_gen()
        self._multi_node_failover()
        if self.spec_name is None:
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(tasks)
        self.disable_autofailover_and_validate()

    def test_rebalance_after_autofailover(self):
        """
        Test autofailover for different failure scenarios and then
        rebalance
        nodes
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing
        nodes and
        wait for the rebalance to be completed
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.
        :return: Nothing
        """
        self.enable_autofailover_and_validate()
        self.sleep(5)
        tasks = self.subsequent_load_gen()
        self._multi_node_failover()
        for node in self.servers_to_add:
            self.rest.add_node(user=self.orchestrator.rest_username,
                               password=self.orchestrator.rest_password,
                               remoteIp=node.ip)
        nodes = self.rest.node_statuses()
        nodes_to_remove = [node.id for node in nodes if
                           node.ip in [t.ip for t in
                                       self.servers_to_remove]]
        nodes = [node.id for node in nodes]
        started = self.rest.rebalance(nodes, nodes_to_remove)
        rebalance_success = False
        if started:
            rebalance_success = self.rest.monitorRebalance()
        if (not rebalance_success or not started) and not \
                self.failover_expected:
            self.fail("Rebalance failed. Check logs")
        if self.spec_name is None:
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(tasks)

    def test_autofailover_and_addback_of_node(self):
        """
        Test autofailover of nodes and then addback of the node after
        failover
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Addback node and validate that the addback was successful.
        :return: Nothing
        """
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        self.enable_autofailover_and_validate()
        self.sleep(5)
        tasks = self.subsequent_load_gen()
        self._multi_node_failover()
        self.server_to_fail = self._servers_to_fail()
        self.bring_back_failed_nodes_up()
        self.sleep(30)
        self.nodes = self.rest.node_statuses()
        for node in self.server_to_fail:
            self.rest.add_back_node("ns_1@{}".format(node.ip))
            self.rest.set_recovery_type("ns_1@{}".format(node.ip),
                                        self.recovery_strategy)
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while recovering failover nodes {0}" \
              .format(self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)
        if self.spec_name is None:
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(tasks)

    def test_autofailover_and_remove_failover_node(self):
        """
        Test autofailover of nodes and remove the node via rebalance
        after
        the failover.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Rebalance of node if failover was successful and validate.
        :return:
        """
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        tasks = self.subsequent_load_gen()
        self.enable_autofailover_and_validate()
        self.sleep(5)
        self._multi_node_failover()
        self.nodes = self.rest.node_statuses()
        self.remove_after_failover = True
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while removing failover nodes {0}" \
              .format(self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True),
                        msg)
        if self.spec_name is None:
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(tasks)

    def _check_for_autofailover_initiation_for_server_group_failover(
            self, failed_over_nodes):
        rest = RestConnection(self.master)
        ui_logs = rest.get_logs(10)
        ui_logs_text = [t["text"] for t in ui_logs]
        ui_logs_time = [t["serverTime"] for t in ui_logs]
        expected_log = "Starting failing over ['ns_1@{}','ns_1@{}']".format(
            failed_over_nodes[0].ip, failed_over_nodes[1].ip)
        self.log.info("ui_logs_text: {0}".format(ui_logs_text))
        if expected_log in ui_logs_text:
            failed_over_time = ui_logs_time[ui_logs_text.index(expected_log)]
            return True, failed_over_time
        return False, None

    def subsequent_load_gen(self, async_load=True):
        if self.spec_name is None:
            subsequent_load_gen = doc_generator(self.key,
                                                self.num_items,
                                                self.num_items*2,
                                                key_size=self.key_size,
                                                doc_size=self.doc_size,
                                                doc_type=self.doc_type)
            tasks = self.async_load_all_buckets(
                subsequent_load_gen, "create", 0)
            return tasks
        else:
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                self.data_load_spec)
            tasks = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                async_load=async_load)
            return tasks

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
