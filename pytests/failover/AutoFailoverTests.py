from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from membase.api.exception import RebalanceFailedException, \
    ServerUnavailableException


class AutoFailoverTests(AutoFailoverBaseTest):
    def setUp(self):
        super(AutoFailoverTests, self).setUp()
        if self.atomicity:
            self.run_time_create_load_gen = self.get_doc_generator(self.num_items, self.num_items * 2)
        else:
            self.run_time_create_load_gen = self.get_doc_generator(self.num_items, self.num_items * 10)
        

    def tearDown(self):
        super(AutoFailoverTests, self).tearDown()

    def test_autofailover(self):
        """
        Test the basic autofailover for different failure scenarios.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required.
        3. Disable autofailover and validate.
        :return: None
        """
        self.enable_autofailover_and_validate()
        self.sleep(5)

        # Start load_gen, if it is durability_test
        if self.durability_level or self.atomicity:
            self.loadgen_tasks = self._loadgen()

        self.failover_actions[self.failover_action](self)
        if self.atomicity:
            # Wait for all tasks to complete
            for task in self.loadgen_tasks:
                self.task_manager.get_task_result(task)
        else:
            self.validate_loadgen_tasks()
        self.disable_autofailover_and_validate()

    def test_autofailover_during_rebalance(self):
        """
        Test autofailover for different failure scenarios while rebalance
        of nodes in progress
        1. Enable autofailover and validate
        2. Start rebalance of nodes by either adding or removing nodes.
        3. Fail a node and validate if node is failed over if required.
        4. Disable autofailover and validate.

        :return: None
        """
        self.enable_autofailover_and_validate()
        self.sleep(5)

        # Start load_gen, if it is durability_test
        if self.durability_level or self.atomicity:
            self.loadgen_tasks = self._loadgen()

        rebalance_task = self.task.async_rebalance(self.servers,
                                                   self.servers_to_add,
                                                   self.servers_to_remove)
        self.sleep(5)
        self.failover_actions[self.failover_action](self)
        try:
            self.task.jython_task_manager.get_task_result(rebalance_task)
        except RebalanceFailedException:
            pass
        except ServerUnavailableException:
            pass
        except Exception:
            pass
        else:
            self.fail("Rebalance should fail since a node went down")
        if self.atomicity:
            # Wait for all tasks to complete
            for task in self.loadgen_tasks:
                self.task_manager.get_task_result(task)
        else:
            self.validate_loadgen_tasks()
        self.disable_autofailover_and_validate()

    def test_autofailover_after_rebalance(self):
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
        self.enable_autofailover_and_validate()
        self.sleep(5)

        # Start load_gen, if it is durability_test
        if self.durability_level:
            self.loadgen_tasks = self._loadgen()

        rebalance_success = self.task.rebalance(self.servers,
                                                self.servers_to_add,
                                                self.servers_to_remove)
        if not rebalance_success:
            self.disable_firewall()
            self.fail("Rebalance failed. Check logs")
        self.failover_actions[self.failover_action](self)
        self.validate_loadgen_tasks()
        self.disable_autofailover_and_validate()

    def test_rebalance_after_autofailover(self):
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
        self.enable_autofailover_and_validate()
        self.sleep(5)

        # Start load_gen, if it is durability_test
        if self.durability_level or self.atomicity:
            self.loadgen_tasks = self._loadgen()

        self.failover_actions[self.failover_action](self)

        # Update replica before rebalance due to failover
        if self.replica_update_during == "before_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.new_replica)

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

        # Update replica after rebalance due to failover
        if self.replica_update_during == "after_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.new_replica)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
            msg = "rebalance failed while updating replica from {0} -> {1}" \
                .format(self.replicas, self.new_replica)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        if self.atomicity:
            # Wait for all tasks to complete
            for task in self.loadgen_tasks:
                self.task_manager.get_task_result(task)
        else:
            self.validate_loadgen_tasks()
        self.disable_autofailover_and_validate()

    def test_autofailover_and_addback_of_node(self):
        """
        Test autofailover of nodes and then addback of the node after failover
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Addback node and validate that the addback was successful.
        :return: None
        """
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        self.enable_autofailover_and_validate()
        self.sleep(5)

        # Start load_gen, if it is durability_test
        if self.durability_level or self.atomicity:
            self.loadgen_tasks = self._loadgen()

        self.failover_actions[self.failover_action](self)

        # Update replica before rebalance due to failover
        if self.replica_update_during == "before_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.new_replica)

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

        # Update replica after rebalance due to failover
        if self.replica_update_during == "after_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.new_replica)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
            msg = "rebalance failed while updating replica from {0} -> {1}" \
                .format(self.replicas, self.new_replica)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        if self.atomicity:
            # Wait for all tasks to complete
            for task in self.loadgen_tasks:
                self.task_manager.get_task_result(task)
        else:
            self.validate_loadgen_tasks()
        self.disable_autofailover_and_validate()

    def test_autofailover_and_remove_failover_node(self):
        """
        Test autofailover of nodes and remove the node via rebalance after
        the failover.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Rebalance of node if failover was successful and validate.
        :return: None
        """
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        bucket = self.bucket_util.buckets[0]
        self.enable_autofailover_and_validate()
        self.sleep(5)

        # Start load_gen, if it is durability_test
        if self.durability_level:
            self.loadgen_tasks = self._loadgen()

        self.failover_actions[self.failover_action](self)
        self.nodes = self.rest.node_statuses()
        self.remove_after_failover = True

        # Update replica before rebalance due to failover
        if self.replica_update_during == "before_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.new_replica)

        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while removing failover nodes {0}".format(
            self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        # Update replica after rebalance due to failover
        if self.replica_update_during == "after_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.new_replica)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
            msg = "rebalance failed while updating replica from {0} -> {1}" \
                  .format(self.replicas, self.new_replica)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        self.validate_loadgen_tasks()
        self.disable_autofailover_and_validate()
