from couchbase_utils.cbas_utils.cbas_utils import CBASRebalanceUtil, CbasUtil
from pytests.bucket_collections.collections_base import CollectionBase
from pytests.serverless.serverless_onprem_basetest import \
    ServerlessOnPremBaseTest
from Jython_tasks.task import ConcurrentFailoverTask
from membase.api.rest_client import RestConnection
from cb_constants import CbServer

import json
import time
from common_lib import sleep
from shell_util.remote_connection import RemoteMachineShellConnection


class Hibernation(ServerlessOnPremBaseTest):
    def setUp(self):
        super(Hibernation, self).setUp()

        self.region = self.input.param('region', None)
        self.s3_path = self.input.param('s3_path',None)
        self.pause_resume_timeout = self.input.param("pause_resume_timeout", 300)
        self.autofailover_timeout = self.input.param("autofailover_timeout", 100)
        self.autofailover_max_count = self.input.param("autofailover_maxCount", 1)
        self.rate_limit = int(self.input.param('rate_limit', 1024))
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.nodes_out = self.input.param("nodes_out", 0)
        self.nodes_in = self.input.param("nodes_in", 0)
        self.recovery_strategy = self.input.param("recovery_strategy", "remove")
        self.failure_type = self.input.param("failure_type", "stop_memcached")
        self.current_fo_strategy = self.input.param("current_fo_strategy",
                                                    "auto")
        self.pick_zone_wise = self.input.param("pick_zone_wise", False)
        self.rebalance_type = self.input.param("rebalance_type", "rebalance_in")
        self.spec_name = self.input.param("bucket_spec", None)
        self.run_pause_resume_first = self.input.param("run_pause_resume_first",
                                                       False)
        self.aws_access_key_id = self.input.param("aws_access_key_id", None)
        self.aws_secret_access_key = self.input.param("aws_secret_access_key",
                                                      None)

        if not self.aws_access_key_id:
            self.log.info("Getting credentials from json file")
            aws_cred_file = open('./pytests/serverless/aws_cred.json', 'r')
            aws_cred = json.load(aws_cred_file)
            self.aws_access_key_id = aws_cred['aws_access_key_id']
            self.aws_secret_access_key = aws_cred['aws_secret_access_key']
        self.log.info("Configuring aws credentials on all nodes")
        self.configure_aws_on_nodes()

        self.rest = RestConnection(self.cluster.master)
        self.servers_to_fail = self.cluster.servers[1:self.num_node_failures
                                                          + 1]
        self.zone_affected = dict()
        self.zone_map = dict()
        self.get_zone_map()

        #creating bucket from spec file
        if self.spec_name:
            CollectionBase.deploy_buckets_from_spec_file(self)
            self.create_sdk_clients()

        self.cbas_util = CbasUtil(self.task)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, True,
            self.cbas_util)

        self.rebalance_util.data_load_collection(
            self.cluster, "initial_load", False, async_load=False)

        self.sleep(60, "Wait for data for load")
        self.bucket = self.cluster.buckets[0]
        self.bucket2 = self.cluster.buckets[1]

    def tearDown(self):
        super(Hibernation, self).tearDown()

    def create_sdk_clients(self, buckets=None):
        if not buckets:
            buckets = self.cluster.buckets
        CollectionBase.create_sdk_clients(
            self.cluster,
            self.task_manager.number_of_threads,
            self.cluster.master,
            buckets,
            self.sdk_compression)

    def test_basic_pause_resume(self):
        self.log.info("Pausing bucket")
        remote_path = self.s3_path +  "/" + self.bucket.name
        pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                   remote_path, self.region,
                                                   self.rate_limit,
                                                   timeout=self.pause_resume_timeout)
        if not pause_task.result:
            self.fail("Pause of bucket failed")
        self.log.info("Buckets after pausing: {0}".format(self.cluster.buckets))
        self.log.info("Resuming paused bucket")
        self.sleep(10, "Wait before resuming bucket")
        resume_task = self.bucket_util.resume_bucket(self.cluster, self.bucket,
                                                    remote_path, self.region,
                                                    self.rate_limit,
                                                    timeout=self.pause_resume_timeout)
        if not resume_task.result:
            self.fail("Resume of bucket failed")

    def test_stop_pause(self):
        self.log.info("Pausing bucket")
        remote_path = self.s3_path +  "/" + self.bucket.name
        self.bucket_util.pause_bucket(self.cluster, self.bucket, remote_path,
                                      self.region, self.rate_limit,
                                      timeout=self.pause_resume_timeout,
                                      wait_for_bucket_deletion=False)
        self.sleep(5, "Wait for pause to start")
        self.log.info("Invoke stop pause")
        result, content = self.bucket_util.stop_pause(self.cluster, self.bucket)
        if not result:
            self.fail("Failed to stop pause of bucket")

    def test_stop_resume(self):
        self.log.info("Pausing bucket")
        remote_path = self.s3_path + "/" + self.bucket.name
        pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                   remote_path, self.region,
                                                   self.rate_limit,
                                                   timeout=self.pause_resume_timeout)
        if not pause_task.result:
            self.fail("Pause of bucket failed")
        self.log.info("Try to resume bucket")
        self.bucket_util.resume_bucket(self.cluster, self.bucket, remote_path,
                                       self.region, self.rate_limit,
                                       timeout=self.pause_resume_timeout,
                                       wait_for_bucket_creation=False)
        self.sleep(5, "Wait for resume to start")
        result, content = self.bucket_util.stop_resume(self.cluster, self.bucket)
        if not result:
            self.fail("Failed to stop resume of bucket")

    def get_zone_map(self):
        for zones in self.rest.get_zone_names():
            servers = []
            nodes = self.rest.get_nodes_in_zone(zones)
            for server in self.cluster.servers:
                if server.ip in nodes:
                    self.zone_map[server] = zones
                    servers.append(server)
            if self.pick_zone_wise and self.cluster.master.ip not in nodes \
                    and len(self.servers_to_fail) == 0:
                self.servers_to_fail = servers

    def __update_server_obj(self, nodes_out = []):
        temp_data = nodes_out
        self.servers_to_fail = dict()
        self.log.info(nodes_out)
        for node_obj in temp_data:
            if self.nodes_in > 0:
                if self.zone_map[node_obj] in self.zone_affected:
                    self.zone_affected[self.zone_map[node_obj]] += 1
                else:
                    self.zone_affected[self.zone_map[node_obj]] = 1
            self.servers_to_fail[node_obj] = self.failure_type
            self.log.info("zone affected: {0}".format(self.zone_affected))
            self.log.info("Zone map: {0}".format(self.zone_map))

    def auto_failover_task(self):
        rest_nodes = self.rest.get_nodes()
        self.__update_server_obj(self.servers_to_fail)
        self.log.info("Updating autofailover settings")
        status = self.rest.update_autofailover_settings(
            True, self.autofailover_timeout, maxCount=self.autofailover_max_count)
        self.assertTrue(status, "Auto-failover enable failed")
        failover_task = ConcurrentFailoverTask(
            task_manager=self.task_manager, master=self.cluster.master,
            servers_to_fail=self.servers_to_fail,
            expected_fo_nodes=self.num_node_failures,
            task_type="induce_failure")
        self.task_manager.add_new_task(failover_task)
        self.task_manager.get_task_result(failover_task)
        if failover_task.result is False:
            self.fail("Failure during concurrent failover procedure")

        self.sleep(5, "Wait before reverting")

        self.rest.update_autofailover_settings(enabled=False,
                                               timeout=self.autofailover_timeout,
                                               maxCount=self.autofailover_max_count)

        if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
            self.log.info("Reverting failure")
            failover_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.cluster.master,
                servers_to_fail=self.servers_to_fail,
                task_type="revert_failure")
            self.task_manager.add_new_task(failover_task)
            self.task_manager.get_task_result(failover_task)
            if failover_task.result is False:
                self.fail("Failure during reverting failover operation")

    def failover_task(self):
        rest_nodes = self.rest.get_nodes()
        self.__update_server_obj(self.servers_to_fail)
        self.log.info("servers to fail: {0}".format(self.servers_to_fail))
        if self.current_fo_strategy == CbServer.Failover.Type.GRACEFUL:
            self.rest.monitorRebalance()
            for node in self.servers_to_fail:
                node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                status = self.rest.fail_over(node.id, graceful=True)
                if status is False:
                    self.fail("Graceful failover failed for %s" % node)
                self.sleep(5, "Wait for failover to start")
                self.rest.monitorRebalance()
        elif self.current_fo_strategy == CbServer.Failover.Type.FORCEFUL:
            self.rest.monitorRebalance()
            for node in self.servers_to_fail:
                node = [t_node for t_node in rest_nodes
                        if t_node.ip == node.ip][0]
                status = self.rest.fail_over(node.id, graceful=False)
                if status is False:
                    self.fail("Hard failover failed for %s" % node)
                self.sleep(5, "Wait for failover to start")
                self.rest.monitorRebalance()

    def wait_for_pause_resume_to_complete(self):
        start = time.time()
        timeout_in_seconds = 300
        result = False
        while (time.time() - start) <= timeout_in_seconds:
                pause_resume_task = self.rest.ns_server_tasks(task_type="hibernation")
                if not pause_resume_task:
                    return result

                status = pause_resume_task['status']
                self.log.info("Hibernation status: {0}".format(status))
                if status == "running":
                    sleep(2)
                elif status == "completed":
                    result = True
                    break
                else:
                    return result
        return result

    def test_pause_with_rebalance(self):
        rebalance_task = None
        pause_task = None
        remote_path = self.s3_path +  "/" + self.bucket.name
        if self.run_pause_resume_first:
            pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                       remote_path, self.region,
                                                       self.rate_limit,
                                                       timeout=self.pause_resume_timeout,
                                                       wait_for_bucket_deletion=False)
            self.sleep(5, "Wait for pause operation to start")
        if self.rebalance_type == "rebalance_in":
            nodes_in = self.cluster.servers[
                    self.nodes_init:self.nodes_init + self.nodes_in]

            rebalance_task = self.task.async_rebalance(self.cluster,
                                    nodes_in, [],
                                    retry_get_process_num=2000)
            if not self.run_pause_resume_first:
                self.task_manager.get_task_result(rebalance_task)
                self.distribute_servers_across_available_zones()
                for bucket in self.cluster.buckets:
                    self.bucket_util.update_bucket_property(self.cluster.master,
                                                            bucket, bucket_width=2)
                rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                           retry_get_process_num=2000)
                self.sleep(5, "Wait for rebalance to start")
        elif self.rebalance_type == "swap_rebalance":
            nodes_in = self.cluster.servers[
                self.nodes_init:self.nodes_init + self.nodes_in]
            nodes_out = self.cluster.servers[
                1:self.nodes_out + 1]
            self.__update_server_obj(nodes_out)
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in, nodes_out,
                                                       retry_get_process_num=2000,
                                                       add_nodes_server_groups =
                                                       self.zone_affected)
            if not self.run_pause_resume_first:
                self.sleep(30, "Wait for rebalance to start")

        if self.run_pause_resume_first:
            self.task_manager.get_task_result(rebalance_task)
            if rebalance_task:
                error_msg = rebalance_task.rebalance_api_response
                expected_error_msg = "Cannot rebalance when another bucket " + \
                                      "is pausing/resuming."
                self.assertEqual(error_msg, expected_error_msg, "Did not get expected"
                                "error for rebalance, Expected:{0}, Actual:{1}".
                                format(expected_error_msg, error_msg))
            self.task_manager.get_task_result(pause_task)
            if pause_task:
                if not pause_task.result:
                    self.fail("Pause task did not complete as expected, Reason: {0}".
                              format(pause_task.failure_reason))
        else:
            pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                       remote_path, self.region,
                                                       self.rate_limit,
                                                       timeout=self.pause_resume_timeout)
            if pause_task.result:
                self.fail("Pause did not fail as expected")
            error = pause_task.failure_reason
            self.assertEqual(error, "rebalance_running", "Did not get expected error \
                            for pause, Expected: {0}, Actual: {1}".format("rebalance_running",
                                                                          error))
            self.task_manager.get_task_result(rebalance_task)

    def test_resume_with_rebalance(self):
        #pause bucket
        remote_path = self.s3_path +  "/" + self.bucket.name
        pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                   remote_path, self.region,
                                                   self.rate_limit,
                                                   timeout=self.pause_resume_timeout)
        if not pause_task.result:
            self.fail("Pause of bucket failed, Reason: {0}".format(
                                                            pause_task.failure_reason))
        resume_task = None
        if self.run_pause_resume_first:
            resume_task = self.bucket_util.resume_bucket(self.cluster, self.bucket,
                                                         remote_path, self.region,
                                                         self.rate_limit,
                                                         timeout=self.pause_resume_timeout,
                                                         wait_for_bucket_creation=False)
            self.sleep(5, "Wait for resume operation to start")
        rebalance_task = None
        if self.rebalance_type == "rebalance_in":
            nodes_in = self.cluster.servers[
                    self.nodes_init:self.nodes_init + self.nodes_in]

            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in, [],
                                                       retry_get_process_num=2000,
                                                       check_vbucket_shuffling=False)
            if not self.run_pause_resume_first:
                self.task_manager.get_task_result(rebalance_task)
                self.distribute_servers_across_available_zones()
                for bucket in self.cluster.buckets[1:]:
                    self.bucket_util.update_bucket_property(self.cluster.master,
                                                            bucket, bucket_width=2)
                rebalance_task = self.task.async_rebalance(self.cluster, [], [],
                                                           retry_get_process_num=2000,
                                                           check_vbucket_shuffling=False)
                self.sleep(3, "Wait for rebalance to start")
        elif self.rebalance_type == "swap_rebalance":
            nodes_in = self.cluster.servers[
                self.nodes_init:self.nodes_init + self.nodes_in]
            nodes_out = self.cluster.servers[
                1:self.nodes_out + 1]
            self.__update_server_obj(nodes_out)
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in, nodes_out,
                                                       retry_get_process_num=2000,
                                                       add_nodes_server_groups =
                                                       self.zone_affected,
                                                       check_vbucket_shuffling=False)
            if not self.run_pause_resume_first:
                self.sleep(15, "Wait for rebalance to start")

        if self.run_pause_resume_first:
            self.task_manager.get_task_result(rebalance_task)
            if rebalance_task:
                error_msg = rebalance_task.rebalance_api_response
                expected_error_msg = "Cannot rebalance when another bucket " + \
                                      "is pausing/resuming."
                self.assertEqual(error_msg, expected_error_msg, "Did not get expected"
                                "error for rebalance, Expected:{0}, Actual:{1}".
                                format(expected_error_msg, error_msg))
            self.task_manager.get_task_result(resume_task)
            if resume_task:
                if not resume_task.result:
                    self.fail("Resume task did not complete as expected, Reason : {0}".
                              format(resume_task.failure_reason))
        else:
            resume_task = self.bucket_util.resume_bucket(self.cluster, self.bucket,
                                                         remote_path, self.region,
                                                         self.rate_limit,
                                                         timeout=self.pause_resume_timeout)
            if resume_task.result:
                self.fail("Resume did not fail as expected")
            error = resume_task.failure_reason
            self.assertEqual(error, "rebalance_running", "Did not get expected error \
                            for resume, Expected: {0}, Actual: {1}".format("rebalance_running",
                                                                            error))
            self.task_manager.get_task_result(rebalance_task)

    def test_pause_with_failover(self):
        remote_path = self.s3_path +  "/" + self.bucket.name
        pause_task = None
        if self.run_pause_resume_first:
            pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                       remote_path, self.region,
                                                       self.rate_limit,
                                                       timeout=self.pause_resume_timeout,
                                                       wait_for_bucket_deletion=False)
            self.sleep(10, "Wait for pause to start")
        failover_error = None
        try:
            self.failover_task()
        except Exception as e:
            failover_error = str(e)
        if self.run_pause_resume_first:
            if failover_error:
                expected_error = "Unexpected server error: in_bucket_hibernation"
                error_result = (expected_error in failover_error)
                self.assertTrue(error_result, "Unexpected exception: {0}".format(
                                failover_error))
                self.task_manager.get_task_result(pause_task)
            else:
                self.fail("Node failover did not fail as expected")
        else:
            if failover_error:
                self.fail(failover_error)
            pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                       remote_path, self.region,
                                                       self.rate_limit,
                                                       timeout=self.pause_resume_timeout)
            if pause_task.result:
                self.fail("Pause did not fail as expected")
            error = pause_task.failure_reason
            self.assertEqual(error, "requires_rebalance", "Did not get expected error \
                            for pause, Expected: {0}, Actual: {1}".format("requires_rebalance",
                                                                            error))

    def test_resume_with_failover(self):
        remote_path = self.s3_path +  "/" + self.bucket.name
        pause_task = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                   remote_path, self.region,
                                                   self.rate_limit,
                                                   timeout=self.pause_resume_timeout)
        if not pause_task.result:
            self.fail("Pause of bucket failed, Reason: {0}".format(
                        pause_task.failure_reason))
        resume_task = None
        if self.run_pause_resume_first:
            resume_task = self.bucket_util.resume_bucket(self.cluster, self.bucket,
                                                         remote_path, self.region,
                                                         self.rate_limit,
                                                         timeout=self.pause_resume_timeout,
                                                         wait_for_bucket_creation=False)
            self.sleep(5, "Wait for resume to start")
        failover_error = None
        try:
            self.failover_task()
        except Exception as e:
            failover_error = str(e)
        if self.run_pause_resume_first:
            if failover_error:
                expected_error = "Unexpected server error: in_bucket_hibernation"
                error_result = (expected_error in failover_error)
                self.assertTrue(error_result, "Unexpected exception: {0}".format(
                                failover_error))
                self.task_manager.get_task_result(resume_task)
            else:
                self.fail("Node failover did not fail as expected")
        else:
            if failover_error:
                self.fail(failover_error)
            resume_task = self.bucket_util.resume_bucket(self.cluster, self.bucket,
                                                         remote_path, self.region,
                                                         self.rate_limit,
                                                         timeout=self.pause_resume_timeout)
            error = resume_task.failure_reason
            expected_error_msg = "Need more space in availability zones [{0}]."
            av_zones = ""
            for node_obj in self.servers_to_fail:
                av_zone = self.zone_map[node_obj]
                av_zone_msg = ",<<\"{0}\">>".format(av_zone)
                av_zones += av_zone_msg
            av_zones = av_zones[1:]
            expected_error_msg = expected_error_msg.format(av_zones)
            self.assertEqual(error, expected_error_msg, "Did not get expected error \
                            for pause, Expected: {0}, Actual: {1}".format(expected_error_msg,
                                                                        error))

    def test_pause_with_autofailover(self):
        remote_path = self.s3_path +  "/" + self.bucket.name
        result, content = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                        remote_path, self.region,
                                                        self.rate_limit,
                                                        timeout=self.pause_resume_timeout,
                                                        wait_for_bucket_deletion=False)
        self.auto_failover_task()
        self.wait_for_pause_resume_to_complete()

    def test_resume_with_autofailover(self):
        remote_path = self.s3_path +  "/" + self.bucket.name
        result, content = self.bucket_util.pause_bucket(self.cluster, self.bucket,
                                                        remote_path, self.region,
                                                        self.rate_limit,
                                                        timeout=self.pause_resume_timeout)
        if not result:
            self.fail("Pause of bucket failed")

        resut, content = self.bucket_util.resume_bucket(self.cluster, self.bucket,
                                                        remote_path, self.region,
                                                        self.rate_limit,
                                                        timeout=self.pause_resume_timeout,
                                                        wait_for_bucket_creation=False)
        self.auto_failover_task()
        self.wait_for_pause_resume_to_complete()

    def configure_aws_on_nodes(self):
        COUCHBASE_AWS_HOME = '/home/couchbase/.aws'
        aws_cred_file = ('[default]\n' +
                         'aws_access_key_id={0}\n'.format(self.aws_access_key_id) +
                         'aws_secret_access_key={0}'.format(self.aws_secret_access_key))
        aws_conf_file = ('[default]\n' +
                         'region={0}\n'.format(self.region) +
                         'output=json')
        for node in self.servers:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command("rm -rf {0}".format(COUCHBASE_AWS_HOME))
            shell.execute_command("mkdir -p {0}".format(COUCHBASE_AWS_HOME))

            shell.create_file(remote_path='{0}/credentials'.format(COUCHBASE_AWS_HOME),
                              file_data=aws_cred_file)
            shell.create_file(remote_path='{0}/config'.format(COUCHBASE_AWS_HOME),
                              file_data=aws_conf_file)
