import json
import random

from rebalance_base import RebalanceBaseTest

from collections_helper.collections_spec_constants import MetaCrudParams
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection

from sdk_exceptions import SDKException


class AutoRetryFailedRebalance(RebalanceBaseTest):
    def setUp(self):
        super(AutoRetryFailedRebalance, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.sleep_time = self.input.param("sleep_time", 15)
        self.enabled = self.input.param("enabled", True)
        self.afterTimePeriod = self.input.param("afterTimePeriod", 300)
        self.maxAttempts = self.input.param("maxAttempts", 1)
        self.log.info("Changing the retry rebalance settings ....")
        self.change_retry_rebalance_settings(
            enabled=self.enabled,
            afterTimePeriod=self.afterTimePeriod,
            maxAttempts=self.maxAttempts)
        self.rebalance_operation = self.input.param("rebalance_operation",
                                                    "rebalance_out")
        self.disable_auto_failover = self.input.param("disable_auto_failover",
                                                      True)
        self.auto_failover_timeout = self.input.param("auto_failover_timeout",
                                                      120)
        if self.disable_auto_failover:
            self.rest.update_autofailover_settings(False, 120)
        else:
            self.rest.update_autofailover_settings(True,
                                                   self.auto_failover_timeout)
        self.cb_collect_failure_nodes = dict()
        # To support data load during auto retry op
        self.data_load = self.input.param("data_load", False)
        self.rebalance_failed_msg = "Rebalance failed as expected"

    def tearDown(self):
        self.reset_retry_rebalance_settings()
        self.cbcollect_info()
        # Reset to default value
        super(AutoRetryFailedRebalance, self).tearDown()
        rest = RestConnection(self.servers[0])
        zones = rest.get_zone_names()
        for zone in zones:
            if zone != "Group 1":
                rest.delete_zone(zone)

    def __update_cbcollect_expected_node_failures(self, nodes, reason):
        for node in nodes:
            self.cb_collect_failure_nodes[node.ip] = reason

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def async_data_load(self):
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            "volume_test_load")
        self.set_retry_exceptions(doc_loading_spec)
        tasks = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.bucket_util.buckets,
            doc_loading_spec,
            mutation_num=0,
            async_load=True)
        return tasks

    def data_validation(self, tasks):
        self.task.jython_task_manager.get_task_result(tasks)
        self.bucket_util.validate_doc_loading_results(tasks)
        if tasks.result is False:
            self.fail("Doc_loading failed")

        self.cluster_util.print_cluster_stats()
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.bucket_util.print_bucket_stats()

    def test_auto_retry_of_failed_rebalance_where_failure_happens_before_rebalance(self):
        tasks = None
        before_rebalance_failure = self.input.param("before_rebalance_failure",
                                                    "stop_server")
        # induce the failure before the rebalance starts
        self._induce_error(before_rebalance_failure)
        self.sleep(self.sleep_time)

        try:
            rebalance = self._rebalance_operation(self.rebalance_operation)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, self.rebalance_failed_msg)
        except Exception as e:
            self.log.info("Rebalance failed with: {0}".format(str(e)))
            # Trigger cbcollect after rebalance failure
            self.cbcollect_info(trigger=True, validate=False,
                                known_failures=self.cb_collect_failure_nodes)
            # Recover from the error
            self._recover_from_error(before_rebalance_failure)
            if self.data_load:
                tasks = self.async_data_load()
            self.check_retry_rebalance_succeeded()
            # Validate cbcollect result after rebalance retry
            self.cbcollect_info(trigger=False, validate=True,
                                known_failures=self.cb_collect_failure_nodes)
            if self.data_load:
                self.data_validation(tasks)
        else:
            self.fail("Rebalance did not fail as expected. "
                      "Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.cluster_util.start_server(self.servers[1])
            self.cluster_util.stop_firewall_on_node(self.servers[1])

    def test_auto_retry_of_failed_rebalance_where_failure_happens_during_rebalance(self):
        tasks = None
        during_rebalance_failure = self.input.param("during_rebalance_failure",
                                                    "stop_server")
        try:
            rebalance = self._rebalance_operation(self.rebalance_operation)
            self.sleep(self.sleep_time)
            # induce the failure during the rebalance
            self._induce_error(during_rebalance_failure)
            self.task.jython_task_manager.get_task_result(rebalance)
            # Trigger cbcollect after rebalance interrupted
            self.cbcollect_info(trigger=True, validate=False,
                                known_failures=self.cb_collect_failure_nodes)
            self.assertTrue(rebalance.result, self.rebalance_failed_msg)
        except Exception as e:
            self.log.info("Rebalance failed with: {0}".format(str(e)))
            # Recover from the error
            self._recover_from_error(during_rebalance_failure)
            if self.data_load:
                tasks = self.async_data_load()
            self.check_retry_rebalance_succeeded()
            if self.data_load:
                self.data_validation(tasks)
        else:
            # This is added as the failover task is not throwing exception
            if self.rebalance_operation == "graceful_failover":
                # Recover from the error
                self._recover_from_error(during_rebalance_failure)
                if self.data_load:
                    tasks = self.async_data_load()
                self.check_retry_rebalance_succeeded()
                if self.data_load:
                    self.data_validation(tasks)
            else:
                self.fail("Rebalance did not fail as expected. "
                          "Hence could not validate auto-retry feature..")
        finally:
            # Validate cbcollect results
            self.cbcollect_info(trigger=False, validate=True,
                                known_failures=self.cb_collect_failure_nodes)
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.cluster_util.start_server(self.servers[1])
            self.cluster_util.stop_firewall_on_node(self.servers[1])

    def test_auto_retry_of_failed_rebalance_does_not_get_triggered_when_rebalance_is_stopped(self):
        _ = self._rebalance_operation(self.rebalance_operation)
        reached = RestHelper(self.rest).rebalance_reached(30)
        self.assertTrue(reached, "Rebalance failed or did not reach 30%")
        # Trigger cbcollect before interrupting the rebalance
        self.cbcollect_info(trigger=True, validate=False)
        self.rest.stop_rebalance(wait_timeout=self.sleep_time)
        result = json.loads(self.rest.get_pending_rebalance_info())
        self.log.info(result)
        # Validate cbcollect results
        self.cbcollect_info(trigger=False, validate=True)
        retry_rebalance = result["retry_rebalance"]
        if retry_rebalance != "not_pending":
            self.fail("Auto-retry succeeded even when Rebalance was stopped by user")

    def test_negative_auto_retry_of_failed_rebalance_where_rebalance_will_be_cancelled(self):
        during_rebalance_failure = self.input.param("during_rebalance_failure",
                                                    "stop_server")
        post_failure_operation = self.input.param("post_failure_operation",
                                                  "cancel_pending_rebalance")
        try:
            rebalance = self._rebalance_operation(self.rebalance_operation)
            self.sleep(self.sleep_time)
            # induce the failure during the rebalance
            self._induce_error(during_rebalance_failure)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, self.rebalance_failed_msg)
        except Exception as e:
            self.log.info("Rebalance failed with: %s" % e)
            # Start cbcollect after rebalance failure
            self.cbcollect_info(trigger=True, validate=False,
                                known_failures=self.cb_collect_failure_nodes)
            # Recover from the error
            self._recover_from_error(during_rebalance_failure)
            # TODO : Data load at this stage fails;
            # if self.data_load:
            #     tasks = self.async_data_load()
            result = json.loads(self.rest.get_pending_rebalance_info())
            # if self.data_load:
            #     self.data_validation(tasks)
            self.log.info(result)
            retry_rebalance = result["retry_rebalance"]
            rebalance_id = result["rebalance_id"]
            if retry_rebalance != "pending":
                # Wait for cbcollect to complete before asserting
                self.cbcollect_info(
                    trigger=False, validate=True,
                    known_failures=self.cb_collect_failure_nodes)
                self.fail("Auto-retry of failed rebalance is not triggered")
            if post_failure_operation == "cancel_pending_rebalance":
                # cancel pending rebalance
                self.log.info("Cancelling rebalance-id: %s" % rebalance_id)
                self.rest.cancel_pending_rebalance(rebalance_id)
            elif post_failure_operation == "disable_auto_retry":
                # disable the auto retry of the failed rebalance
                self.log.info("Disable the auto retry of the failed rebalance")
                self.change_retry_rebalance_settings(enabled=False)
            elif post_failure_operation == "retry_failed_rebalance_manually":
                # retry failed rebalance manually
                self.log.info("Retrying failed rebalance id %s" % rebalance_id)
                self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            else:
                self.fail("Invalid post_failure_operation option")
            # Now check and ensure retry won't happen
            result = json.loads(self.rest.get_pending_rebalance_info())
            self.log.info(result)
            # Validate cbcollect results
            self.cbcollect_info(trigger=False, validate=True,
                                known_failures=self.cb_collect_failure_nodes)
            retry_rebalance = result["retry_rebalance"]
            if retry_rebalance != "not_pending":
                self.fail("Auto-retry of failed rebalance is not cancelled")
        else:
            self.fail("Rebalance did not fail as expected. "
                      "Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.cluster_util.start_server(self.servers[1])
            self.cluster_util.stop_firewall_on_node(self.servers[1])

    def test_negative_auto_retry_of_failed_rebalance_where_rebalance_will_not_be_cancelled(self):
        during_rebalance_failure = self.input.param("during_rebalance_failure",
                                                    "stop_server")
        post_failure_operation = self.input.param("post_failure_operation",
                                                  "create_delete_buckets")
        zone_name = "Group_{0}_{1}".format(random.randint(1, 1000000000),
                                           self._testMethodName)
        zone_name = zone_name[0:60]
        default_zone = "Group 1"
        moved_node = [self.servers[1].ip]
        try:
            rebalance = self._rebalance_operation(self.rebalance_operation)
            self.sleep(self.sleep_time)
            # induce the failure during the rebalance
            self._induce_error(during_rebalance_failure)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, self.rebalance_failed_msg)
        except Exception as e:
            self.log.info("Rebalance failed with : {0}".format(str(e)))
            # Recover from the error
            self._recover_from_error(during_rebalance_failure)
            result = json.loads(self.rest.get_pending_rebalance_info())
            self.log.info(result)
            retry_rebalance = result["retry_rebalance"]
            if retry_rebalance != "pending":
                self.fail("Auto-retry of failed rebalance is not triggered")
            # if post_failure_operation == "create_delete_buckets":
            #     # delete buckets and create new one
            #     BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
            #     self.sleep(self.sleep_time)
            #     BucketOperationHelper.create_bucket(self.master, test_case=self)

            # Start cbcollect only if auto-retry of rebalance is triggered
            self.cbcollect_info(trigger=True, validate=False)
            if post_failure_operation == "change_replica_count":
                # change replica count
                self.log.info("Changing replica count of buckets")
                for bucket in self.bucket_util.buckets:
                    self.bucket_util.update_bucket_property(bucket,
                                                            replica_number=2)
            elif post_failure_operation == "change_server_group":
                # change server group
                self.log.info("Creating new zone " + zone_name)
                self.rest.add_zone(zone_name)
                self.log.info("Moving {0} to new zone {1}".format(moved_node,
                                                                  zone_name))
                _ = self.rest.shuffle_nodes_in_zones(moved_node,
                                                     default_zone,
                                                     zone_name)
            else:
                self.fail("Invalid post_failure_operation option")
            # In these failure scenarios while the retry is pending,
            # then the retry will be attempted but fail
            try:
                self.check_retry_rebalance_succeeded()
                # Validate cbcollect results
                self.cbcollect_info(trigger=False, validate=True)
            except Exception as e:
                self.log.info(e)
                # Wait for cbstat to complete before asserting
                self.cbcollect_info(trigger=False, validate=True)
                if "Retrying of rebalance still did not help. All the retries exhausted" not in str(e):
                    self.fail("Auto retry of failed rebalance succeeded when it was expected to fail")
        else:
            self.fail("Rebalance did not fail as expected. Hence could not validate auto-retry feature..")
        finally:
            if post_failure_operation == "change_server_group":
                status = self.rest.shuffle_nodes_in_zones(moved_node,
                                                          zone_name,
                                                          default_zone)
                self.log.info("Shuffle the node back to default group. "
                              "Status: %s" % status)
                self.sleep(self.sleep_time)
                self.log.info("Deleting new zone " + zone_name)
                try:
                    self.rest.delete_zone(zone_name)
                except:
                    self.log.info("Errors in deleting zone")
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.cluster_util.start_server(self.servers[1])
            self.cluster_util.stop_firewall_on_node(self.servers[1])

    def test_auto_retry_of_failed_rebalance_with_rebalance_test_conditions(self):
        tasks = None
        test_failure_condition = self.input.param("test_failure_condition")
        # induce the failure before the rebalance starts
        self._induce_rebalance_test_condition(test_failure_condition)
        self.sleep(self.sleep_time)
        # Start cbcollect before rebalance starts
        self.cbcollect_info(trigger=True, validate=False)
        try:
            rebalance = self._rebalance_operation(self.rebalance_operation)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, self.rebalance_failed_msg)
        except Exception as e:
            self.log.info("Rebalance failed with: %s" % e)
            # Validate cbcollect result
            self.cbcollect_info(trigger=False, validate=True)
            # Restart cbcollect to test with error recovery
            self.cbcollect_info(trigger=True, validate=False)
            # Delete the rebalance test condition to recover from the error
            self._delete_rebalance_test_condition(test_failure_condition)
            if self.data_load:
                tasks = self.async_data_load()
            self.check_retry_rebalance_succeeded()
            # Validate cbcollect result
            self.cbcollect_info(trigger=False, validate=True)
            if self.data_load:
                self.data_validation(tasks)
        else:
            # Wait for cbcollect to complete
            self.cbcollect_info(trigger=False, validate=True)
            self.fail("Rebalance did not fail as expected. "
                      "Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self._delete_rebalance_test_condition(test_failure_condition)

    def test_auto_retry_of_failed_rebalance_with_autofailvoer_enabled(self):
        before_rebalance_failure = self.input.param("before_rebalance_failure",
                                                    "stop_server")
        # induce the failure before the rebalance starts
        self._induce_error(before_rebalance_failure)
        try:
            rebalance = self._rebalance_operation(self.rebalance_operation)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, self.rebalance_failed_msg)
        except Exception as e:
            self.log.info("Rebalance failed with: {0}".format(str(e)))
            self.cbcollect_info(trigger=True, validate=False,
                                known_failures=self.cb_collect_failure_nodes)
            if self.auto_failover_timeout < self.afterTimePeriod:
                self.sleep(self.auto_failover_timeout)
                result = json.loads(self.rest.get_pending_rebalance_info())
                self.log.info(result)
                retry_rebalance = result["retry_rebalance"]
                if retry_rebalance != "not_pending":
                    # Wait for cbcollect to complete before asserting
                    self.cbcollect_info(
                        trigger=False, validate=True,
                        known_failures=self.cb_collect_failure_nodes)
                    self.fail("Auto-failover did not cancel pending retry "
                              "of the failed rebalance")
            else:
                try:
                    self.check_retry_rebalance_succeeded()
                except Exception as e:
                    expected_msg = "Retrying of rebalance still did not help"
                    if expected_msg not in str(e):
                        self.fail("Retry rebalance succeeded "
                                  "even without failover")
                    self.sleep(self.auto_failover_timeout)
                    self.cluster.rebalance(self.servers[:self.nodes_init],
                                           [], [])
                finally:
                    self.cbcollect_info(
                        trigger=False, validate=True,
                        known_failures=self.cb_collect_failure_nodes)
        else:
            self.fail("Rebalance did not fail as expected. "
                      "Hence could not validate auto-retry feature..")
        finally:
            if self.disable_auto_failover:
                self.rest.update_autofailover_settings(True, 120)
            self.cluster_util.start_server(self.servers[1])
            self.cluster_util.stop_firewall_on_node(self.servers[1])

    def test_cbcollect_with_rebalance_delay_condition(self):
        test_failure_condition = self.input.param("test_failure_condition")
        vb_num = self.input.param("target_vb")
        delay_milliseconds = self.input.param("delay_time", 60) * 1000
        # induce the failure before the rebalance starts
        self._induce_rebalance_test_condition(test_failure_condition,
                                              vb_num=vb_num,
                                              delay_time=delay_milliseconds)
        self.sleep(self.sleep_time,
                   "Wait for rebalance_test_condition to take effect")
        rebalance = self._rebalance_operation(self.rebalance_operation)
        # Start and validate cbcollect with rebalance delay
        self.cbcollect_info(trigger=True, validate=True)
        self.task.jython_task_manager.get_task_result(rebalance)
        if self.disable_auto_failover:
            self.rest.update_autofailover_settings(True, 120)
        self._delete_rebalance_test_condition(test_failure_condition)
        if rebalance.result is False:
            self.fail("Rebalance failed with test_condition: %s"
                      % test_failure_condition)

    def _rebalance_operation(self, rebalance_operation):
        operation = None
        self.log.info("Starting rebalance operation of type: %s"
                      % rebalance_operation)
        if rebalance_operation == "rebalance_out":
            operation = self.task.async_rebalance(
                self.cluster_util.get_nodes_in_cluster(self.cluster.master),
                [], self.cluster.servers[1:])
            self.__update_cbcollect_expected_node_failures(
                self.cluster.servers[1:], "out_node")
        elif rebalance_operation == "rebalance_in":
            operation = self.task.async_rebalance(
                self.cluster_util.get_nodes_in_cluster(self.cluster.master),
                [self.cluster.servers[self.nodes_init]], [])
            self.__update_cbcollect_expected_node_failures(
                [self.cluster.servers[self.nodes_init]], "in_node")
        elif rebalance_operation == "swap_rebalance":
            self.rest.add_node(self.cluster.master.rest_username,
                               self.cluster.master.rest_password,
                               self.cluster.servers[self.nodes_init].ip,
                               self.cluster.servers[self.nodes_init].port)
            operation = self.task.async_rebalance(
                self.cluster_util.get_nodes_in_cluster(self.cluster.master),
                [], [self.cluster.servers[self.nodes_init - 1]])
            self.__update_cbcollect_expected_node_failures(
                [self.cluster.servers[self.nodes_init]], "in_node")
            self.__update_cbcollect_expected_node_failures(
                [self.cluster.servers[self.nodes_init - 1]], "out_node")
        elif rebalance_operation == "graceful_failover":
            # TODO : retry for graceful failover is not yet implemented
            operation = self.task.async_failover(
                [self.cluster.master],
                failover_nodes=[self.cluster.servers[1]],
                graceful=True, wait_for_pending=120)
        return operation

    def _induce_error(self, error_condition):
        cb_collect_err_str = None
        if error_condition == "stop_server":
            cb_collect_err_str = "failed"
            self.cluster_util.stop_server(self.servers[1])
        elif error_condition == "enable_firewall":
            cb_collect_err_str = "failed"
            self.cluster_util.start_firewall_on_node(self.servers[1])
        elif error_condition == "kill_memcached":
            self.cluster_util.kill_server_memcached(self.servers[1])
        elif error_condition == "reboot_server":
            # cb_collect_err_str = "failed"
            shell = RemoteMachineShellConnection(self.servers[1])
            shell.reboot_node()
        elif error_condition == "kill_erlang":
            # cb_collect_err_str = "failed"
            shell = RemoteMachineShellConnection(self.servers[1])
            shell.kill_erlang()
            self.sleep(self.sleep_time * 3)
        else:
            self.fail("Invalid error induce option")

        if cb_collect_err_str:
            self.__update_cbcollect_expected_node_failures(
                [self.servers[1]], cb_collect_err_str)

    def _recover_from_error(self, error_condition):
        if error_condition == "stop_server" \
                or error_condition == "kill_erlang":
            self.cluster_util.start_server(self.servers[1])
        elif error_condition == "enable_firewall":
            self.cluster_util.stop_firewall_on_node(self.servers[1])
        elif error_condition == "reboot_server":
            self.sleep(self.sleep_time * 4)
            # wait till node is ready after warm-up
            # ClusterOperationHelper.wait_for_ns_servers
            # or assert([self.servers[1]], self)

    def _induce_rebalance_test_condition(self, test_failure_condition,
                                         bucket_name="default",
                                         vb_num=1,
                                         delay_time=60000):
        if test_failure_condition == "verify_replication":
            set_command = 'testconditions:set(verify_replication, ' \
                          '{fail, "%s"})' % bucket_name
        elif test_failure_condition == "backfill_done":
            set_command = 'testconditions:set(backfill_done, ' \
                          '{for_vb_move, "%s", %s , fail})' \
                          % (bucket_name, vb_num)
        elif test_failure_condition == "delay_rebalance_start":
            set_command = 'testconditions:set(rebalance_start, {delay, %s}).' \
                          % delay_time
        elif test_failure_condition == "delay_verify_replication":
            set_command = 'testconditions:set(verify_replication, ' \
                          '{delay, "%s", %s})' % (bucket_name, delay_time)
        elif test_failure_condition == "delay_backfill_done":
            set_command = 'testconditions:set(backfill_done, ' \
                          '{for_vb_move, "%s", %s, {delay, %s}})' \
                          % (bucket_name, vb_num, delay_time)
        else:
            set_command = "testconditions:set(%s, fail)" \
                          % test_failure_condition
        get_command = "testconditions:get(%s)" % test_failure_condition
        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(set_command)
            self.log.debug("Set Command: %s. Return: %s"
                           % (set_command, content))
            shell.disconnect()

        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(get_command)
            self.log.info("Command: %s, Return: %s" % (get_command, content))

    def _delete_rebalance_test_condition(self, test_failure_condition):
        if test_failure_condition.startswith("delay_"):
            test_failure_condition = test_failure_condition[6:]

        delete_command = "testconditions:delete(%s)" % test_failure_condition
        get_command = "testconditions:get(%s)" % test_failure_condition
        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(delete_command)
            self.log.info("Command: %s, Return: %s"
                          % (delete_command, content))

        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(get_command)
            self.log.info("Command: %s, Return: %s" % (get_command, content))
