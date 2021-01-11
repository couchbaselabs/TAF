from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest

from sdk_exceptions import SDKException


class AutoFailoverTests(AutoFailoverBaseTest):
    def setUp(self):
        super(AutoFailoverTests, self).setUp()
        self.skip_validations = self.input.param("skip_validations", True)
        self.data_load_spec = self.input.param("data_load_spec",
                                               "volume_test_load")
        if self.spec_name is None:
            if self.atomicity:
                self.run_time_create_load_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items * 2,
                    key_size=self.key_size,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type)
            else:
                self.run_time_create_load_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items * 10,
                    key_size=self.key_size,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type)

    def tearDown(self):
        self.log.info("Printing bucket stats before teardown")
        self.bucket_util.print_bucket_stats()
        super(AutoFailoverTests, self).tearDown()

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def data_load_from_spec(self, async_load=False):
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.data_load_spec)
        if self.durability_level:
            doc_loading_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                self.durability_level
        self.set_retry_exceptions(doc_loading_spec)
        tasks = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.bucket_util.buckets,
            doc_loading_spec,
            mutation_num=0,
            async_load=async_load,
            validate_task=self.skip_validations)
        return tasks

    def data_validation_collection(self):
        if not self.skip_validations:
            if self.durability_level:
                self.bucket_util._wait_for_stats_all_buckets()
                self.bucket_util.validate_docs_per_collections_all_buckets()
            else:
                # No data validation for doc loading without durability level
                pass

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
            if self.durability_level and task.result is False:
                self.fail("Doc_loading failed")

    def test_autofailover(self):
        """
        Test the basic autofailover for different failure scenarios.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required.
        3. Disable autofailover and validate.
        :return: None
        """
        task = None
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            # this is for collections, so load from spec
            task = self.data_load_from_spec(async_load=True)

        self.failover_actions[self.failover_action](self)
        self.sleep(300)
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.sleep(60)
        result_nodes = [node for node in self.cluster.servers[:self.nodes_init]
                        if node.ip != self.server_to_fail[0].ip]
        self.cluster.nodes_in_cluster = result_nodes
        self.cluster.master = self.master = self.cluster.nodes_in_cluster[0]
        if self.spec_name is None:
            if self.durability_level:
                self.data_load_after_autofailover()
        else:
            self.data_load_from_spec(async_load=False)
            self.data_validation_collection()
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
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
        task = None
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task = self.data_load_from_spec(async_load=True)

        rebalance_task = self.task.async_rebalance(self.servers,
                                                   self.servers_to_add,
                                                   self.servers_to_remove)
        self.sleep(5)
        self.failover_actions[self.failover_action](self)
        self.sleep(300)
        self.task.jython_task_manager.get_task_result(rebalance_task)
        self.assertFalse(rebalance_task.result,
                         "Rebalance should fail since a node went down")
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)

        self.sleep(60)
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        result_nodes = [node for node in self.cluster.servers[:self.nodes_init]
                        if node.ip != self.server_to_fail[0].ip]
        self.cluster.nodes_in_cluster = result_nodes
        self.cluster.master = self.master = self.cluster.nodes_in_cluster[0]
        if self.spec_name is None:
            if self.durability_level:
                self.data_load_after_autofailover()
        else:
            self.data_load_from_spec(async_load=False)
            self.data_validation_collection()
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
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
        task = None
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task = self.data_load_from_spec(async_load=True)

        rebalance_task = self.task.async_rebalance(
            self.servers,
            self.servers_to_add,
            self.servers_to_remove,
            check_vbucket_shuffling=False)
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.disable_firewall()
            self.fail("Rebalance failed. Check logs")
        self.failover_actions[self.failover_action](self)
        self.sleep(300)
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
        self.sleep(60)
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        result_nodes = [node for node in self.cluster.servers[:self.nodes_init]
                        if node.ip != self.server_to_fail[0].ip]
        self.cluster.nodes_in_cluster = result_nodes
        self.cluster.master = self.master = self.cluster.nodes_in_cluster[0]
        if self.spec_name is None:
            if self.durability_level:
                self.data_load_after_autofailover()
        else:
            self.data_load_from_spec(async_load=False)
            self.data_validation_collection()
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
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
        task = None
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task = self.data_load_from_spec(async_load=True)

        self.failover_actions[self.failover_action](self)
        self.sleep(300)

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
                .format(self.num_replicas, self.new_replica)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
        init_nodes = self.cluster.servers[:self.nodes_init]
        if self.failover_orchestrator:
            init_nodes = self.cluster.servers[1:self.nodes_init]
        rebalance = self.task.async_rebalance(
            init_nodes, [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        result_nodes = [node for node in self.cluster.servers[:self.nodes_init]
                        if node.ip != self.server_to_fail[0].ip]
        self.cluster.nodes_in_cluster = result_nodes
        self.cluster.master = self.master = self.cluster.nodes_in_cluster[0]
        if self.spec_name is None:
            if self.durability_level:
                self.data_load_after_autofailover()
        else:
            self.data_load_from_spec(async_load=False)
            self.data_validation_collection()
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
            self.disable_autofailover_and_validate()

    def test_autofailover_and_addback_of_node(self):
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
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task = self.data_load_from_spec(async_load=True)

        self.failover_actions[self.failover_action](self)
        self.sleep(300)

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
                .format(self.num_replicas, self.new_replica)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        result_nodes = [node for node in self.cluster.servers[:self.nodes_init]
                        if node.ip != self.server_to_fail[0].ip]
        self.cluster.nodes_in_cluster = result_nodes
        self.cluster.master = self.master = self.cluster.nodes_in_cluster[0]
        if self.spec_name is None:
            if self.durability_level:
                self.data_load_after_autofailover()
        else:
            self.data_load_from_spec(async_load=False)
            self.data_validation_collection()
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
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
        task = None
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task = self.data_load_from_spec(async_load=True)

        self.failover_actions[self.failover_action](self)
        self.sleep(300)
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
                .format(self.num_replicas, self.new_replica)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        result_nodes = [node for node in self.cluster.servers[:self.nodes_init]
                        if node.ip != self.server_to_fail[0].ip]
        self.cluster.nodes_in_cluster = result_nodes
        self.cluster.master = self.master = self.cluster.nodes_in_cluster[0]
        if self.spec_name is None:
            if self.durability_level:
                self.data_load_after_autofailover()
        else:
            self.data_load_from_spec(async_load=False)
            self.data_validation_collection()
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
            self.disable_autofailover_and_validate()
