# -*- coding: utf-8 -*-
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from failover.AutoFailoverBaseTest import DiskAutoFailoverBasetest
from membase.api.exception import \
    RebalanceFailedException, \
    ServerUnavailableException


class DiskAutofailoverTests(DiskAutoFailoverBasetest):
    def setUp(self):
        super(DiskAutofailoverTests, self).setUp()
        self.data_load_spec = self.input.param("data_load_spec",
                                               "volume_test_load")
        self.skip_validations = self.input.param("skip_validations", True)
        if self.spec_name is None:
            if self.atomicity:
                self.run_time_create_load_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items*2,
                    key_size=self.key_size,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type)
            else:
                self.run_time_create_load_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items*10,
                    key_size=self.key_size,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type)

    def tearDown(self):
        super(DiskAutofailoverTests, self).tearDown()

    def data_load_from_spec(self, async_load=False):
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.data_load_spec)
        if self.durability_level:
            doc_loading_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                self.durability_level
        tasks = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.bucket_util.buckets,
            doc_loading_spec,
            mutation_num=0,
            async_load=async_load)
        return tasks

    def data_validation_collection(self):
        if not self.skip_validations:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.validate_docs_per_collections_all_buckets()

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                self.fail("Doc_loading failed")

    def test_disk_autofailover_rest_api(self):
        disk_timeouts = self.input.param("disk_failover_timeouts",
                                         "5,10,30,60,120")
        disk_timeouts = disk_timeouts.split(",")
        for disk_timeout in disk_timeouts:
            self.disk_timeout = disk_timeout
            self.enable_disk_autofailover_and_validate()
            self.sleep(10)
            self.disable_disk_autofailover_and_validate()

    def test_disk_autofailover_rest_api_negative(self):
        self.enable_disk_autofailover_and_validate()

    def test_disk_failure_for_writes(self):
        self.enable_disk_autofailover_and_validate()
        self.loadgen_tasks = self._loadgen()
        self.failover_expected = True
        self.failover_actions[self.failover_action]()
        if self.atomicity:
            for task in self.loadgen_tasks:
                self.task_manager.get_task_result(task)
        else:
            self.validate_loadgen_tasks()
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_disk_failure_for_reads(self):
        self.enable_disk_autofailover_and_validate()
        tasks = []
        tasks.extend(self.async_load_all_buckets(self.initial_load_gen,
                                                 "read", 0))
        self.loadgen_tasks = tasks
        self.failover_expected = (not self.failover_action == "disk_full")
        self.failover_actions[self.failover_action]()
        if self.atomicity:
            for task in self.loadgen_tasks:
                self.task_manager.get_task_result(task)
        else:
            self.validate_loadgen_tasks()
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_disk_failure_for_read_and_writes(self):
        task = None
        self.enable_disk_autofailover_and_validate()
        if self.spec_name is None:
            self.loadgen_tasks = self._loadgen()
        else:
            task = self.data_load_from_spec(async_load=True)
        self.failover_expected = True
        self.failover_actions[self.failover_action]()
        if self.spec_name is None:
            if self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
            else:
                self.validate_loadgen_tasks()
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_disk_autofailover_during_rebalance(self):
        """
        Test autofailover for different failure scenarios while rebalance
        of nodes in progress
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing nodes.
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.

        :return: None
        """
        task = None
        self.enable_disk_autofailover_and_validate()
        self.sleep(5)
        if self.spec_name is None:
            self.loadgen_tasks = self._loadgen()
            self.loadgen_tasks.extend(self.bucket_util._async_load_all_buckets(
                self.cluster.master, self.initial_load_gen, "read", 0))
        else:
            task = self.data_load_from_spec(async_load=True)
        rebalance_task = self.cluster.async_rebalance(self.servers,
                                                      self.servers_to_add,
                                                      self.servers_to_remove)
        self.sleep(5)
        self.failover_expected = True
        self.failover_actions[self.failover_action]()
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
        if self.spec_name is None:
            if not self.atomicity:
                self.validate_loadgen_tasks()
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_disk_autofailover_after_rebalance(self):
        """
        Test autofailover for different failure scenarios after rebalance
        of nodes
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing nodes and
        wait for the rebalance to be completed
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.
        :return: None
        """
        task = None
        self.enable_disk_autofailover_and_validate()
        self.sleep(5)
        if self.spec_name is None:
            self.loadgen_tasks = self._loadgen()
            self.loadgen_tasks.extend(self.bucket._async_load_all_buckets(
                self.cluster.master, self.initial_load_gen, "read", 0))
        else:
            task = self.data_load_from_spec(async_load=True)
        rebalance_success = self.task.rebalance(self.servers,
                                                self.servers_to_add,
                                                self.servers_to_remove)
        if not rebalance_success:
            self.disable_firewall()
            self.fail("Rebalance failed. Check logs")
        self.failover_actions[self.failover_action]()
        if self.spec_name is None:
            if not self.atomicity:
                self.validate_loadgen_tasks()
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_rebalance_after_disk_autofailover(self):
        """
        Test autofailover for different failure scenarios and then rebalance
        nodes
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing nodes and
        wait for the rebalance to be completed
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.
        :return: None
        """
        task = None
        self.enable_disk_autofailover_and_validate()
        self.sleep(5)
        if self.spec_name is None:
            self.loadgen_tasks = self._loadgen()
            self.loadgen_tasks.extend(self.bucket_util._async_load_all_buckets(
                self.cluster.master, self.initial_load_gen, "read", 0))
        else:
            task = self.data_load_from_spec(async_load=True)
        self.failover_actions[self.failover_action]()
        count = 0
        while self.get_failover_count() != self.num_node_failures \
                and count < 5:
            self.sleep(30)
            count += 1
        for node in self.servers_to_add:
            self.rest.add_node(user=self.orchestrator.rest_username,
                               password=self.orchestrator.rest_password,
                               remoteIp=node.ip)
        nodes = self.rest.node_statuses()
        nodes_to_remove = [node.id for node in nodes if
                           node.ip in [t.ip for t in self.servers_to_remove]]
        nodes = [node.id for node in nodes]
        started = self.rest.rebalance(nodes, nodes_to_remove)
        rebalance_success = False
        if started:
            rebalance_success = self.rest.monitorRebalance()
        if (not rebalance_success or not started) and not \
                self.failover_expected:
            self.fail("Rebalance failed. Check logs")
        if self.spec_name is None:
            if not self.atomicity:
                self.validate_loadgen_tasks()
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_disk_autofailover_and_addback_of_node(self):
        """
        Test autofailover of nodes and then addback of the node after failover
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Addback node and validate that the addback was successful.
        :return: None
        """
        task = None
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        self.enable_disk_autofailover_and_validate()
        self.sleep(5)
        if self.spec_name is None:
            self.loadgen_tasks = self._loadgen()
            self.loadgen_tasks.extend(self.bucket._async_load_all_buckets(
                self.cluster.master, self.initial_load_gen, "read", 0))
        else:
            task = self.data_load_from_spec(async_load=True)
        self.failover_actions[self.failover_action]()
        self.bring_back_failed_nodes_up()
        self.sleep(30)
        self.log.info(self.server_to_fail[0])
        self.nodes = self.rest.node_statuses()
        self.log.info(self.nodes[0].id)
        self.rest.add_back_node("ns_1@{}".format(self.server_to_fail[0].ip))
        self.rest.set_recovery_type("ns_1@{}".format(self.server_to_fail[
                                                         0].ip),
                                    self.recovery_strategy)
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while recovering failover nodes {0}".format(
            self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)
        if self.spec_name is None:
            if not self.atomicity:
                self.validate_loadgen_tasks()
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()

    def test_disk_autofailover_and_remove_failover_node(self):
        """
        Test autofailover of nodes and remove the node via rebalance after
        the failover.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Rebalance of node if failover was successful and validate.
        :return: None
        """
        task = None
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        self.enable_disk_autofailover_and_validate()
        self.sleep(5)
        if self.spec_name is None:
            self.loadgen_tasks = self._loadgen()
            self.loadgen_tasks.extend(self.bucket_util._async_load_all_buckets(
                self.cluster.master, self.initial_load_gen, "read", 0))
        else:
            task = self.data_load_from_spec(async_load=True)
        self.failover_actions[self.failover_action]()
        self.nodes = self.rest.node_statuses()
        self.remove_after_failover = True
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[])
        msg = "rebalance failed while removing failover nodes {0}".format(
            self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)
        if self.spec_name is None:
            if not self.atomicity:
                self.validate_loadgen_tasks()
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.disable_disk_autofailover_and_validate()
        self.disable_autofailover_and_validate()
