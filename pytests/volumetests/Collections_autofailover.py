from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from collections_helper.collections_spec_constants import MetaCrudParams
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton

from failover.AutoFailoverBaseTest import AutoFailoverBaseTest

from sdk_exceptions import SDKException


class volume(AutoFailoverBaseTest):
    # will add the __init__ functions after the test has been stabilised
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(volume, self).setUp()
        self.bucket_util._expiry_pager(val=5)
        self.rest = RestConnection(self.servers[0])
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.iterations = self.input.param("iterations", 1)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.data_load_spec = self.input.param("data_load_spec", "volume_test_load_with_CRUD_on_collections")
        self.spec_name_eph = self.input.param("bucket_spec_eph", "volume_templates.ephemeral_buckets_for_volume_test")
        self.skip_validations = self.input.param("skip_validations", True)
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]

    def tearDown(self):
        # Do not call the base class's teardown, as we want to keep the cluster intact after the volume run
        self.log.info("Printing bucket stats before teardown")
        self.bucket_util.print_bucket_stats()
        if self.collect_pcaps:
            self.start_fetch_pcaps()
        result, core_msg, stream_msg, asan_msg = self.check_coredump_exist(self.servers,
                                                                           force_collect=True)
        if not self.crash_warning:
            self.assertFalse(result, msg=core_msg + stream_msg + asan_msg)
        if self.crash_warning and result:
            self.log.warn(core_msg + stream_msg + asan_msg)

    def servers_to_fail(self):
        """
        Select the nodes to be failed in the tests.
        :return: Nothing
        """
        if self.failover_orchestrator:
            servers_to_fail = []
            servers_to_fail.append(self.cluster.master)
            servers_to_fail.extend(self.nodes_in_cluster[0:self.num_node_failures])
        else:
            servers_to_fail = self.nodes_in_cluster[1:self.num_node_failures + 1]
        return servers_to_fail

    def custom_induce_and_remove_failure(self):
        """
        1. Induce failure
        2. sleep for timeout amount of time
        3. remove the failure, so that autoreprovision kicks in
        :return: None
        """
        if self.failover_action == "stop_server":
            self.cluster_util.stop_server(self.server_to_fail[0])
            self.sleep(self.timeout, "keeping the failure")
            self.cluster_util.start_server(self.server_to_fail[0])
        elif self.failover_action == "firewall":
            self.cluster_util.start_firewall_on_node(self.server_to_fail[0])
            self.sleep(self.timeout, "keeping the failure")
            self.cluster_util.stop_firewall_on_node(self.server_to_fail[0])
        elif self.failover_action == "stop_memcached":
            self.cluster_util.stop_memcached_on_node(self.server_to_fail[0])
            self.sleep(self.timeout, "keeping the failure")
            self.cluster_util.start_memcached_on_node(self.server_to_fail[0])
        else:
            # for restart server, machine restart, and network restart use base libraries
            self.failover_actions[self.failover_action](self)

    def rebalance_after_autofailover(self):
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
        # Enable autofailover/autoreprovision
        if self.auto_reprovision:
            self.disable_autofailover_and_validate()
            self.enable_autoreprovision()
        else:
            self.enable_autofailover_and_validate()
        self.sleep(5)

        # start async data load
        # TODO: Decide whether to do data load after autofailover kicks in?
        task = self.data_load_collection(async_load=True)

        # Initiate failure, and let the autofailover/autoreprovison kick-in
        self.log.info("Inducing failure {0} on nodes: {1}".format(self.failover_action, self.server_to_fail))
        if self.auto_reprovision:
            self.custom_induce_and_remove_failure()
        else:
            self.failover_actions[self.failover_action](self)

        self.sleep(300, "keep the cluster with the failure(autofailover test)/warmup to complete(autoreprovision test)")

        # Wait for async data load to complete
        self.wait_for_async_data_load_to_complete(task)
        self.data_validation_collection()

        # Bring back the node up in the case of autofailover.
        # In the case of autoreprovision we would have done it already in function: custom_induce_and_remove_failure
        if not self.auto_reprovision:
            self.bring_back_failed_nodes_up()
            self.sleep(30)

        if self.auto_reprovision:
            # Rebalance the cluster
            rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], [],
                                                       retry_get_process_num=100)
            self.wait_for_rebalance_to_complete(rebalance_task)
        else:
            # Either RebalanceOut or Recovery
            if self.subsequent_action == "RebalanceOut":
                # RebalanceOut
                rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], self.server_to_fail,
                                                           retry_get_process_num=100)
                self.wait_for_rebalance_to_complete(rebalance_task)
                # Add the node back to cluster
                rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, self.server_to_fail, [],
                                                           retry_get_process_num=100)
                self.wait_for_rebalance_to_complete(rebalance_task)
            else:
                if self.subsequent_action == "DeltaRecovery":
                    self.rest.set_recovery_type(otpNode='ns_1@' + self.server_to_fail[0].ip, recoveryType="delta")
                else:
                    self.rest.set_recovery_type(otpNode='ns_1@' + self.server_to_fail[0].ip, recoveryType="full")
                rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], [], retry_get_process_num=100)
                self.wait_for_rebalance_to_complete(rebalance_task)

        if self.auto_reprovision:
            self.disable_autoreprovision()
        else:
            self.disable_autofailover_and_validate()

    def wait_for_rebalance_to_complete(self, task, wait_step=120):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = []
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def data_load_collection(self, async_load=True, skip_read_success_results=True):
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(self.data_load_spec)
        doc_loading_spec[MetaCrudParams.SKIP_READ_SUCCESS_RESULTS] = skip_read_success_results
        self.set_retry_exceptions(doc_loading_spec)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        task = self.bucket_util.run_scenario_from_spec(self.task,
                                                       self.cluster,
                                                       self.bucket_util.buckets,
                                                       doc_loading_spec,
                                                       mutation_num=0,
                                                       async_load=async_load)
        return task

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                self.fail("Doc_loading failed")

    def data_validation_collection(self):
        retry_count = 0
        while retry_count < 10:
            try:
                self.bucket_util._wait_for_stats_all_buckets()
            except:
                retry_count = retry_count + 1
                self.log.info("ep-queue hasn't drained yet. Retry count: {0}".format(retry_count))
            else:
                break
        if retry_count == 10:
            self.log.info("Attempting last retry for ep-queue to drain")
            self.bucket_util._wait_for_stats_all_buckets()
        if not self.skip_validations:
            self.bucket_util.validate_docs_per_collections_all_buckets()
        else:
            pass

    def test_volume_taf(self):
        self.log.info("Finished steps 1-4 successfully in setup")
        #########################################################################################################################
        failure_conditions = ["restart_machine", "stop_server", "restart_server", "firewall", "restart_network",
                              "stop_memcached"]
        step_count = 4
        for failover_action in failure_conditions:
            self.failover_action = failover_action
            step_count = step_count + 1
            if self.failover_action == "restart_server" or self.failover_action == "restart_machine":
                self.timeout = 5
            else:
                self.timeout = 60
            if step_count % 2 == 0:
                self.failover_orchestrator = False
                self.subsequent_action = "RebalanceOut"
            else:
                # TODO: 1. Enable failover_orchaestror for odd step number 2. Enable full recovery for some steps
                self.failover_orchestrator = False
                self.subsequent_action = "DeltaRecovery"
            self.log.info("Step {0}: {1} -> Autofailover -> {2}".format(step_count, self.failover_action,
                                                                        self.subsequent_action))
            self.server_to_fail = self.servers_to_fail()
            self.rebalance_after_autofailover()
            self.bucket_util.print_bucket_stats()
        #########################################################################################################################
        step_count = step_count + 1
        self.log.info("Step {0}: Deleting all buckets".format(step_count))
        self.bucket_util.delete_all_buckets()
        #########################################################################################################################
        step_count = step_count + 1
        self.log.info("Step {0}: Creating ephemeral buckets".format(step_count))
        # Create bucket(s) and add rbac user
        self.bucket_util.add_rbac_user()
        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            self.spec_name_eph)
        self.bucket_util.create_buckets_using_json_data(buckets_spec)
        self.bucket_util.wait_for_collection_creation_to_complete()
        # Prints bucket stats before doc_ops
        self.bucket_util.print_bucket_stats()
        #########################################################################################################################
        step_count = step_count + 1
        self.log.info("Step {0}: Initial data data load into ephemeral buckets".format(step_count))
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        self.over_ride_doc_loading_template_params(doc_loading_spec)

        # TODO: remove this once the bug is fixed
        self.sleep(120, "MB-38497")

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")

        self.cluster_util.print_cluster_stats()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        #########################################################################################################################
        self.auto_reprovision = True
        failure_conditions = ["restart_machine", "stop_server", "restart_server", "firewall", "restart_network",
                              "stop_memcached"]
        for failover_action in failure_conditions:
            self.failover_action = failover_action
            step_count = step_count + 1
            if self.failover_action == "restart_server" or self.failover_action == "restart_machine":
                self.timeout = 5
            else:
                self.timeout = 60
            if step_count % 2 == 0:
                self.failover_orchestrator = False
            else:
                # ToDo: 1. Enable failover_orchaestror for odd step number
                self.failover_orchestrator = False
            self.log.info("Step {0}: {1} -> Autoreprovision -> Rebalance".format(step_count, self.failover_action))
            self.server_to_fail = self.servers_to_fail()
            self.rebalance_after_autofailover()
            self.bucket_util.print_bucket_stats()
        self.log.info("Volume test run complete!")
