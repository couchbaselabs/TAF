import time
import urllib

from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from collections_helper.collections_spec_constants import MetaCrudParams
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
import random
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from error_simulation.cb_error import CouchbaseError
from bucket_collections.collections_base import CollectionBase


class volume(CollectionBase):
    # will add the __init__ functions after the test has been stabilised
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(volume, self).setUp()
        self.bucket_util._expiry_pager(val=5)
        self.rest = RestConnection(self.servers[0])
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.iterations = self.input.param("iterations", 2)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.data_load_spec = self.input.param("data_load_spec", "volume_test_load_for_volume_test")
        self.contains_ephemeral = self.input.param("contains_ephemeral", True)
        # the stage at which CRUD for collection level/ document level take place.
        # "before" - start and finish before rebalance/failover starts at each step
        # "during" - during rebalance/failover at each step
        self.data_load_stage = self.input.param("data_load_stage", "during")
        self.doc_and_collection_ttl = self.input.param("doc_and_collection_ttl", False)  # For using doc_ttl + coll_ttl
        self.skip_validations = self.input.param("skip_validations", True)

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

    # Stopping and restarting the memcached process
    def stop_process(self):
        target_node = self.servers[2]
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log, remote)
        error_to_simulate = "stop_memcached"
        # Induce the error condition
        error_sim.create(error_to_simulate)
        self.sleep(20, "Wait before reverting the error condition")
        # Revert the simulated error condition and close the ssh session
        error_sim.revert(error_to_simulate)
        remote.disconnect()

    def rebalance(self, nodes_in=0, nodes_out=0):
        servs_in = random.sample(self.available_servers, nodes_in)

        self.nodes_cluster = self.cluster.nodes_in_cluster[:]
        self.nodes_cluster.remove(self.cluster.master)
        servs_out = random.sample(self.nodes_cluster, nodes_out)

        if nodes_in == nodes_out:
            self.vbucket_check = False

        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, servs_out, check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=100)

        self.available_servers = [servs for servs in self.available_servers if servs not in servs_in]
        self.available_servers += servs_out

        self.cluster.nodes_in_cluster.extend(servs_in)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        return rebalance_task

    def wait_for_rebalance_to_complete(self, task, wait_step=120):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def data_load_collection(self, async_load=True, skip_read_success_results=True):
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(self.data_load_spec)
        doc_loading_spec[MetaCrudParams.SKIP_READ_SUCCESS_RESULTS] = skip_read_success_results
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
        if self.doc_and_collection_ttl:
            self.bucket_util._expiry_pager(val=5)
            self.sleep(400, "wait for doc/collection maxttl to finish")
            items = 0
            self.bucket_util._wait_for_stats_all_buckets()
            for bucket in self.bucket_util.buckets:
                items = items + self.bucket_helper_obj.get_active_key_count(bucket)
            if items != 0:
                self.fail("doc count!=0, TTL + rebalance failed")
        else:
            if not self.skip_validations:
                self.bucket_util.validate_docs_per_collections_all_buckets()
            else:
                pass

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=7200):
        # Timeout is kept large for graceful failover
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        if actual_failover_count != expected_failover_count:
            self.log.info(self.rest.print_UI_logs())
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

    def test_volume_taf(self):
        self.loop = 0
        # self.cluster_utils.set_metadata_purge_interval()
        while self.loop < self.iterations:
            self.log.info("Finished steps 1-4 successfully in setup")
            self.log.info("Step 5: Rebalance in with Loading of docs")
            if self.data_load_stage == "before":
                task = self.data_load_collection(async_load=False)
                if task.result is False:
                    self.fail("Doc loading failed")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            if self.data_load_stage == "during":
                task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            if self.data_load_stage == "during":
                self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.bucket_util.print_bucket_stats()
            #########################################################################################################################
            self.log.info("Step 6: Rebalance Out with Loading of docs")
            if self.data_load_stage == "before":
                task = self.data_load_collection(async_load=False)
                if task.result is False:
                    self.fail("Doc loading failed")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)
            if self.data_load_stage == "during":
                task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            if self.data_load_stage == "during":
                self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.bucket_util.print_bucket_stats()
            #######################################################################################################################
            self.log.info("Step 7: Rebalance In_Out with Loading of docs")
            if self.data_load_stage == "before":
                task = self.data_load_collection(async_load=False)
                if task.result is False:
                    self.fail("Doc loading failed")
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1)
            if self.data_load_stage == "during":
                task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            if self.data_load_stage == "during":
                self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.bucket_util.print_bucket_stats()
            ########################################################################################################################
            self.log.info("Step 8: Swap with Loading of docs")
            if self.data_load_stage == "before":
                task = self.data_load_collection(async_load=False)
                if task.result is False:
                    self.fail("Doc loading failed")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)
            if self.data_load_stage == "during":
                task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            if self.data_load_stage == "during":
                self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            ########################################################################################################################
            self.log.info("Step 9: Updating the bucket replica to 2")
            if self.data_load_stage == "before":
                task = self.data_load_collection(async_load=False)
                if task.result is False:
                    self.fail("Doc loading failed")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=2)
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            if self.data_load_stage == "during":
                task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            if self.data_load_stage == "during":
                self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.bucket_util.print_bucket_stats()
            ########################################################################################################################
            if self.contains_ephemeral:
                self.log.info("No Memcached kill for ephemeral bucket")
            else:
                self.log.info("Step 10: Stopping and restarting memcached process")
                if self.data_load_stage == "before":
                    task = self.data_load_collection(async_load=False)
                    if task.result is False:
                        self.fail("Doc loading failed")
                rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [], retry_get_process_num=100)
                if self.data_load_stage == "during":
                    task = self.data_load_collection()
                self.wait_for_rebalance_to_complete(rebalance_task)
                self.stop_process()
                if self.data_load_stage == "during":
                    self.wait_for_async_data_load_to_complete(task)
                self.data_validation_collection()
                self.bucket_util.print_bucket_stats()
            ########################################################################################################################
            step_count = 10
            for failover in ["Graceful", "Hard"]:
                for action in ["RebalanceOut", "FullRecovery", "DeltaRecovery"]:
                    step_count = step_count + 1
                    self.log.info(
                        "Step {0}: {1} Failover a node and {2} that node with data load in parallel".format(step_count,
                                                                                                            failover,
                                                                                                            action))
                    if self.data_load_stage == "before":
                        task = self.data_load_collection(async_load=False)
                        if task.result is False:
                            self.fail("Doc loading failed")

                    self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
                    std = self.std_vbucket_dist or 1.0

                    prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.nodes_in_cluster,
                                                                              self.bucket_util.buckets)
                    prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.nodes_in_cluster,
                                                                             self.bucket_util.buckets)
                    self.sleep(10)

                    disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
                        self.cluster.nodes_in_cluster, self.bucket_util.buckets, path=None)

                    self.rest = RestConnection(self.cluster.master)
                    self.nodes = self.cluster_util.get_nodes(self.cluster.master)
                    self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

                    if self.data_load_stage == "during":
                        task = self.data_load_collection()
                    # Mark Node for failover
                    if failover == "Graceful":
                        self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=True)
                    else:
                        self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)

                    self.sleep(300)
                    self.wait_for_failover_or_assert(1)

                    # Perform the action
                    if action == "RebalanceOut":
                        self.nodes = self.rest.node_statuses()
                        self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[self.chosen[0].id])
                        # self.sleep(600)
                        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=False), msg="Rebalance failed")
                        servs_out = [node for node in self.cluster.servers if node.ip == self.chosen[0].ip]
                        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
                        self.available_servers += servs_out
                        self.sleep(10)
                    else:
                        if action == "FullRecovery":
                            if self.success_failed_over:
                                self.rest.set_recovery_type(otpNode=self.chosen[0].id, recoveryType="full")
                        elif action == "DeltaRecovery":
                            if self.success_failed_over:
                                self.rest.set_recovery_type(otpNode=self.chosen[0].id, recoveryType="delta")

                        rebalance_task = self.task.async_rebalance(
                            self.cluster.servers[:self.nodes_init], [], [], retry_get_process_num=100)
                        self.wait_for_rebalance_to_complete(rebalance_task)
                        self.sleep(10)

                    if self.data_load_stage == "during":
                        self.wait_for_async_data_load_to_complete(task)
                    self.data_validation_collection()

                    self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.nodes_in_cluster,
                                                            self.bucket_util.buckets)
                    self.sleep(10)

                    self.bucket_util.data_analysis_active_replica_all(
                        disk_active_dataset, disk_replica_dataset,
                        self.cluster.servers[:self.nodes_in + self.nodes_init],
                        self.bucket_util.buckets, path=None)
                    nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
                    self.bucket_util.vb_distribution_analysis(
                        servers=nodes, buckets=self.bucket_util.buckets,
                        num_replicas=2,
                        std=std, total_vbuckets=self.cluster_util.vbuckets)
                    self.sleep(10)
                    self.tasks = []
                    # Bring back the rebalance out node back to cluster for further steps
                    if action == "RebalanceOut":
                        self.sleep(120)
                        rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
                        # self.sleep(600)
                        self.wait_for_rebalance_to_complete(rebalance_task)
                    self.bucket_util.print_bucket_stats()
            ########################################################################################################################
            self.log.info("Step 17: Updating the bucket replica to 1")
            if self.data_load_stage == "before":
                task = self.data_load_collection(async_load=False)
                if task.result is False:
                    self.fail("Doc loading failed")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=1)
            rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [], retry_get_process_num=100)
            if self.data_load_stage == "during":
                task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            if self.data_load_stage == "during":
                self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            ########################################################################################################################
            self.log.info("Step 18: Flush the bucket and start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                self.bucket_util.flush_all_buckets(self.cluster.master)
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                    self.nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(self.nodes_cluster,
                                              int(len(self.cluster.nodes_in_cluster) - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out, retry_get_process_num=100)
                    self.wait_for_rebalance_to_complete(rebalance_task)
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
            else:
                self.log.info("Volume Test Run Complete")
        ############################################################################################################################
