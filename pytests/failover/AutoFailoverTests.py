# -*- coding: utf-8 -*-
from bucket_collections.collections_base import CollectionBase
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from cb_constants import constants, CbServer


class AutoFailoverTests(AutoFailoverBaseTest):
    def setUp(self):
        super(AutoFailoverTests, self).setUp()
        self.skip_validations = self.input.param("skip_validations", True)
        self.data_load_spec = self.input.param("data_load_spec",
                                               "volume_test_load")
        self.failover_ephemeral_no_replicas = self.input.param("failover_ephemeral_no_replicas", False)
        if self.failover_ephemeral_no_replicas:
            shell = RemoteMachineShellConnection(self.cluster.master)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
            self.rest.update_failover_ephemeral_no_replicas()

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
        if self.failover_ephemeral_no_replicas:
            shell = RemoteMachineShellConnection(self.cluster.master)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
            self.rest.update_failover_ephemeral_no_replicas(value="false")
        self.bucket_util.print_bucket_stats(self.cluster)
        super(AutoFailoverTests, self).tearDown()

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        retry_exceptions.append(SDKException.ServerOutOfMemoryException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def data_load_from_spec(self, async_load=False):
        # History retention doc_loading. Returns 'None' in non-dedupe runs
        cont_doc_load = CollectionBase.start_history_retention_data_load(
            self, async_load)
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.data_load_spec)
        if self.durability_level:
            doc_loading_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                self.durability_level
        self.set_retry_exceptions(doc_loading_spec)
        tasks = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.cluster.buckets,
            doc_loading_spec,
            mutation_num=0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            async_load=async_load,
            validate_task=self.skip_validations)
        return [tasks, cont_doc_load]

    def data_validation_collection(self):
        if not self.skip_validations:
            if self.durability_level:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                self.bucket_util.validate_docs_per_collections_all_buckets(
                    self.cluster)
            else:
                # No data validation for doc loading without durability level
                pass

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(self.cluster, task)
            if self.durability_level and task.result is False:
                self.fail("Doc_loading failed")

    def enable_logic(self, sleep_after_enabling=5):
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(sleep_after_enabling, "Wait after enabling auto-failover/auto-reprovision")

    def disable_logic(self):
        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
            self.disable_autofailover_and_validate()

    def test_autofailover(self):
        """
        Test the basic autofailover for different failure scenarios.
        1. Enable autofailover/autoreprovision and validate
        2. Induce failure
        3. Rebalance-out the failed-over-nodes
        4. Disable AF/Auto-reprovision
        """
        task = cont_load_task = None
        self.enable_logic()
        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            # this is for collections, so load from spec
            task, cont_load_task = self.data_load_from_spec(async_load=True)

        self.log.info("Inducing failure {0} on nodes {1}".format(self.failover_action,
                                                                 self.server_to_fail))
        self.failover_actions[self.failover_action](self)
        self.sleep(300, "Wait after inducing failure")
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
            CollectionBase.wait_for_cont_doc_load_to_complete(
                self, cont_load_task)

        rebalance = self.task.async_rebalance(self.cluster, [], [],
                                              retry_get_process_num=self.retry_get_process_num)
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
        self.disable_logic()

    def test_autofailover_during_rebalance(self):
        """
        Test autofailover for different failure scenarios during rebalance
        1. Enable autofailover/autoreprovision and validate
        2. Start rebalance of nodes by either adding or removing nodes.
        3. While step2 is going on, induce failure in a node and assert that the rebalance fails.
        4. Do another rebalance in order to remove the failed-over nodes
        5. Disable AF/Auto-reprovision
        """
        task = cont_load_task = None
        self.enable_logic()
        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            # this is for collections, so load from spec
            task, cont_load_task = self.data_load_from_spec(async_load=True)

        rebalance_task = self.task.async_rebalance(self.cluster,
                                                   self.servers_to_add,
                                                   self.servers_to_remove)
        self.sleep(5, "Wait for rebalance to make progress")
        self.log.info("Inducing failure {0} on nodes {1}".format(self.failover_action,
                                                                 self.server_to_fail))
        self.failover_actions[self.failover_action](self)
        self.sleep(300, "Wait after inducing failure")
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
            CollectionBase.wait_for_cont_doc_load_to_complete(
                self, cont_load_task)

        self.sleep(60, "Wait before starting another rebalance")
        rebalance = self.task.async_rebalance(self.cluster, [], [],
                                              retry_get_process_num=self.retry_get_process_num)
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
        self.disable_logic()

    def test_autofailover_after_rebalance(self):
        """
        Test autofailover for different failure scenarios after rebalance
        1. Enable autofailover/autoreprovision and validate
        2. Start rebalance of nodes by either adding or removing nodes and
        wait for the rebalance to be completed
        3. Induce failure in a node(s)
        4. Rebalance the cluster
        5. Disable AF/Auto-reprovision
        """
        task = cont_load_task = None
        self.enable_logic()
        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task, cont_load_task = self.data_load_from_spec(async_load=True)

        rebalance_task = self.task.async_rebalance(
            self.cluster, self.servers_to_add, self.servers_to_remove,
            check_vbucket_shuffling=False,
            retry_get_process_num=self.retry_get_process_num)
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.disable_firewall()
            self.fail("Rebalance failed. Check logs")
        self.log.info("Inducing failure {0} on nodes {1}".format(self.failover_action,
                                                                 self.server_to_fail))
        self.failover_actions[self.failover_action](self)
        self.sleep(300, "Wait after inducing failure")
        if self.spec_name is None:
            if self.durability_level or self.atomicity:
                for task in self.loadgen_tasks:
                    self.task_manager.get_task_result(task)
        else:
            self.wait_for_async_data_load_to_complete(task)
            CollectionBase.wait_for_cont_doc_load_to_complete(
                self, cont_load_task)
        self.sleep(60, "Wait before starting another rebalance")
        rebalance = self.task.async_rebalance(self.cluster, [], [],
                                              retry_get_process_num=self.retry_get_process_num)
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
        self.disable_logic()

    def test_rebalance_after_autofailover(self):
        """
        Test autofailover for different failure scenarios and then rebalance
        nodes
        1. Enable autofailover/autoreprovision and validate
        2. Induce failure in node(s)
        3. Start rebalance of nodes by either adding or removing nodes and
        wait for the rebalance to be completed
        4. Start another rebalance
        5. Disable AF/Auto-reprovision
        """
        task = cont_load_task = None
        self.enable_logic()
        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task, cont_load_task = self.data_load_from_spec(async_load=True)

        self.log.info("Inducing failure {0} on nodes {1}".format(self.failover_action,
                                                                 self.server_to_fail))
        self.failover_actions[self.failover_action](self)
        self.sleep(300, "Wait after inducing failure")

        # Update replica before rebalance due to failover
        if self.replica_update_during == "before_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        self.new_replica)

        for node in self.servers_to_add:
            self.log.info("Adding node {0}".format(node.ip))
            self.rest.add_node(user=self.orchestrator.rest_username,
                               password=self.orchestrator.rest_password,
                               remoteIp=node.ip,
                               port=CbServer.ssl_port)
        nodes = self.rest.node_statuses()
        self.log.info("Marking {0} for removal".format(self.servers_to_remove))
        nodes_to_remove = [node.id for node in nodes if
                           node.ip in [t.ip for t in self.servers_to_remove]]
        nodes = [node.id for node in nodes]
        started, _ = self.rest.rebalance(nodes, nodes_to_remove)
        rebalance_success = False
        if started:
            rebalance_success = self.rest.monitorRebalance()
        if (not rebalance_success or not started) and not \
                self.failover_expected:
            self.fail("Rebalance failed. Check logs")

        # Update replica after rebalance due to failover
        if self.replica_update_during == "after_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        self.new_replica)
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
            CollectionBase.wait_for_cont_doc_load_to_complete(
                self, cont_load_task)
        rebalance = self.task.async_rebalance(
            self.cluster, [], [], retry_get_process_num=self.retry_get_process_num)
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
        self.disable_logic()

    def test_autofailover_and_addback_of_node(self):
        """
        Test autofailover of nodes and then addback of the node after failover
        1. Enable autofailover/autoreprovision and validate
        2. Induce failure in node(s)
        3. Delta/full recover the failed-over-nodes to add them back and rebalance
        4. Start another rebalance
        5. Disable AF/Auto-reprovision
        """
        task = cont_load_task = None
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        self.enable_logic()
        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task, cont_load_task = self.data_load_from_spec(async_load=True)

        self.log.info("Inducing failure {0} on nodes {1}".format(self.failover_action,
                                                                 self.server_to_fail))
        self.failover_actions[self.failover_action](self)
        self.sleep(300, "Wait after inducing failure")

        # Update replica before rebalance due to failover
        if self.replica_update_during == "before_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        self.new_replica)

        self.bring_back_failed_nodes_up()
        self.sleep(30, "Wait after removing failures")
        self.nodes = self.rest.node_statuses()
        self.log.info("Adding back {0} using {1} recovery".format(self.server_to_fail[0].ip,
                                                                  self.recovery_strategy))
        self.rest.set_recovery_type("ns_1@{}".format(self.server_to_fail[
                                                         0].ip),
                                    self.recovery_strategy)
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while recovering failover nodes {0}".format(
            self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        # Update replica after rebalance due to failover
        if self.replica_update_during == "after_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        self.new_replica)
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
            CollectionBase.wait_for_cont_doc_load_to_complete(
                self, cont_load_task)
        rebalance = self.task.async_rebalance(self.cluster, [], [],
                                              retry_get_process_num=self.retry_get_process_num)
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
        self.disable_logic()

    def test_autofailover_and_remove_failover_node(self):
        """
        Test autofailover of nodes and remove the node via rebalance after
        the failover.
        1. Enable autofailover and validate
        2. Fail a node and validate if node is failed over if required
        3. Rebalance of node if failover was successful and validate.
        """
        task = cont_load_task = None
        if not self.failover_expected:
            self.log.info("Since no failover is expected in the test, "
                          "skipping the test")
            return
        self.enable_logic()
        self.cluster.master = self.master = self.orchestrator
        if self.spec_name is None:
            # Start load_gen, if it is durability_test
            if self.durability_level or self.atomicity:
                self.loadgen_tasks = self._loadgen()
        else:
            task, cont_load_task = \
                self.data_load_from_spec(async_load=True)

        self.log.info("Inducing failure {0} on nodes {1}".format(self.failover_action,
                                                                 self.server_to_fail))
        self.failover_actions[self.failover_action](self)
        self.sleep(300, "Wait after inducing failure")
        self.nodes = self.rest.node_statuses()
        self.remove_after_failover = True

        # Update replica before rebalance due to failover
        if self.replica_update_during == "before_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        self.new_replica)

        self.rest.rebalance(otpNodes=[node.id for node in self.nodes])
        msg = "rebalance failed while removing failover nodes {0}".format(
            self.server_to_fail[0])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg)

        # Update replica after rebalance due to failover
        if self.replica_update_during == "after_rebalance":
            self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                        self.new_replica)
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
            CollectionBase.wait_for_cont_doc_load_to_complete(
                self, cont_load_task)
        rebalance = self.task.async_rebalance(
            self.cluster, [], [], retry_get_process_num=self.retry_get_process_num)
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
        self.disable_logic()
