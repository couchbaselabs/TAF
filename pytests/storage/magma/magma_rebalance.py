from math import ceil
import os
import threading
import time

from BucketLib.BucketOperations import BucketHelper
from cb_constants.CBServer import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from magma_base import MagmaBaseTest
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClientPool
from sdk_exceptions import SDKException
from upgrade_lib.upgrade_helper import CbServerUpgrade


class MagmaRebalance(MagmaBaseTest):
    def setUp(self):
        super(MagmaRebalance, self).setUp()
        self.bucket_util._expiry_pager(self.cluster)
        self.bucket = self.cluster.buckets[0]
        self.data_load_stage = self.input.param("data_load_stage", "before")
        self.num_collections_to_drop = self.input.param("num_collections_to_drop", 0)
        self.nodes_swap = self.input.param("nodes_swap", 1)
        self.nodes_failover = self.input.param("nodes_failover", 1)
        self.failover_ops = ["graceful_failover_rebalance_out", "hard_failover_rebalance_out",
                             "graceful_failover_recovery", "hard_failover_recovery"]
        self.step_count = self.input.param("step_count", -1)
        self.recovery_type = self.input.param("recovery_type", "full")
        self.compaction = self.input.param("compaction", False)
        if self.compaction:
            self.disable_auto_compaction()
        self.warmup = self.input.param("warmup", False)
        self.update_replica = self.input.param("update_replica", False)  # for replica + rebalance tests
        self.updated_num_replicas = self.input.param("updated_num_replicas",
                                                     1)  # for replica + rebalance tests, forced hard failover
        self.forced_hard_failover = self.input.param("forced_hard_failover", False)  # for forced hard failover tests
        self.change_ram_quota_cluster = self.input.param("change_ram_quota_cluster",
                                                         False)  # To change during rebalance
        self.skip_validations = self.input.param("skip_validations", True)
        if self.compaction:
            self.compaction_tasks = list()
        self.dgm_test = self.input.param("dgm_test", False)
        # Init sdk_client_pool if not initialized before
        if self.cluster.sdk_client_pool is None:
            self.cluster.sdk_client_pool = SDKClientPool()

        # Create clients in SDK client pool
        self.log.info("Creating required SDK clients for client_pool")
        bucket_count = len(self.cluster.buckets)
        max_clients = self.task_manager.number_of_threads
        clients_per_bucket = int(ceil(max_clients / bucket_count))
        for bucket in self.cluster.buckets:
            self.cluster.sdk_client_pool.create_clients(
                self.cluster,
                bucket,
                [self.cluster.master],
                clients_per_bucket,
                compression_settings=self.sdk_compression)

    def tearDown(self):
        super(MagmaRebalance, self).tearDown()

    def disable_auto_compaction(self):
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for bucket in buckets:
            if bucket.bucketType == "couchbase":
                self.bucket_util.disable_compaction(self.cluster,
                                                    bucket=bucket.name)

    def compact_all_buckets(self):
        self.sleep(10, "wait for rebalance to start")
        self.log.info("Starting compaction for each bucket")
        for bucket in self.cluster.buckets:
            self.compaction_tasks.append(self.task.async_compact_bucket(
                self.cluster.master, bucket))

    def warmup_node(self, node):
        self.log.info("Warmuping up node...")
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        self.sleep(30)
        shell.start_couchbase()
        shell.disconnect()
        self.log.info("Done warming up...")

    def set_ram_quota_cluster(self):
        self.sleep(45, "Wait for rebalance have some progress")
        self.log.info("Changing cluster RAM size")
        status = self.rest.set_service_mem_quota(
            {CbServer.Settings.KV_MEM_QUOTA: 2500})
        self.assertTrue(status, "RAM quota wasn't changed")

    def set_retry_exceptions(self):
        #retry_exceptions = []
        ##if self.data_load_stage == "during" or (self.data_load_stage == "before" and self.data_load_type == "async"):
        #    retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        #    retry_exceptions.append(SDKException.TimeoutException)
        #    retry_exceptions.append(SDKException.RequestCanceledException)
        if self.durability_level:
            self.retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            self.retry_exceptions.append(SDKException.DurabilityImpossibleException)
        #doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def get_active_resident_threshold(self, bucket_name):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket_name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        return dgm

    def load_to_dgm(self, threshold=100):
        # load data until resident % goes below 100
        bucket_name = self.cluster.buckets[0].name
        curr_active = self.get_active_resident_threshold(bucket_name)
        while curr_active >= threshold:
            self.subsequent_data_load(data_load_spec="dgm_load")
            curr_active = self.get_active_resident_threshold(bucket_name)
            self.log.info("curr_active resident {0} %".format(curr_active))
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
        self.log.info("Initial dgm load done. Resident {0} %".format(curr_active))

    def data_load_after_failover(self):
        self.log.info("Starting a sync data load after failover")
        self.subsequent_data_load()  # sync data load
        # Until we recover/rebalance-out, we can't call - self.bucket_util.validate_docs_per_collections_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=180):
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

    def forced_failover_operation(self, known_nodes=None, failover_nodes=None, wait_for_pending=120):
        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
        self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                    self.updated_num_replicas)
        failover_count = 0
        for failover_node in failover_nodes:
            failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                    graceful=False, wait_for_pending=wait_for_pending)
            failover_count = failover_count + 1
            self.wait_for_failover_or_assert(failover_count)
        operation = self.task.async_rebalance(known_nodes, [], failover_nodes,
                                              retry_get_process_num=120)
        self.data_load_after_failover()
        return operation

    def rebalance_operation(self, rebalance_operation, known_nodes=None, add_nodes=None, remove_nodes=None,
                            failover_nodes=None, wait_for_pending=120, tasks=None):
        self.log.info("Starting rebalance operation of type : {0}".format(rebalance_operation))
        step_count = self.step_count
        if rebalance_operation == "rebalance_out":
            if step_count == -1:
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                          retry_get_process_num=120)
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.cluster.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                bucket, servers=[node]))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                              retry_get_process_num=120)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(
                            self.cluster, self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats(self.cluster)
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                          retry_get_process_num=120)
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                remove_list = []
                for i in range(0, len(remove_nodes), step_count):
                    if i + step_count >= len(remove_nodes):
                        remove_list.append(remove_nodes[i:])
                    else:
                        remove_list.append(remove_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_remove_nodes in remove_list:
                    operation = self.task.async_rebalance(known_nodes, [], new_remove_nodes,
                                                          retry_get_process_num=120)
                    known_nodes = [node for node in known_nodes if node not in new_remove_nodes]
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(remove_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "rebalance_in":
            if step_count == -1:
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [],
                                                          retry_get_process_num=120)
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.cluster.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                bucket, servers=[node]))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(known_nodes + add_nodes, [], [],
                                                              retry_get_process_num=120)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(
                            self.cluster, self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats(self.cluster)
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [],
                                                          retry_get_process_num=120)
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                add_list = []
                for i in range(0, len(add_nodes), step_count):
                    if i + step_count >= len(add_nodes):
                        add_list.append(add_nodes[i:])
                    else:
                        add_list.append(add_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_add_nodes in add_list:
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, [],
                                                          retry_get_process_num=120)
                    known_nodes.append(new_add_nodes)
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(add_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "swap_rebalance":
            if (step_count == -1):
                if self.warmup:
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(self.cluster, [], remove_nodes,
                                                          check_vbucket_shuffling=False,
                                                          retry_get_process_num=120)
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.cluster.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                bucket, servers=[node]))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(self.cluster, [], remove_nodes,
                                                              retry_get_process_num=120)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(
                            self.cluster, self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats(self.cluster)
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    operation = self.task.async_rebalance(self.cluster, [], remove_nodes,
                                                          check_vbucket_shuffling=False,
                                                          retry_get_process_num=120)
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                add_list = []
                remove_list = []
                for i in range(0, len(add_nodes), step_count):
                    if i + step_count >= len(add_nodes):
                        add_list.append(add_nodes[i:])
                        remove_list.append(remove_nodes[i:])
                    else:
                        add_list.append(add_nodes[i:i + step_count])
                        remove_list.append(remove_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_add_nodes, new_remove_nodes in zip(add_list, remove_list):
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, new_remove_nodes,
                                                          check_vbucket_shuffling=False,
                                                          retry_get_process_num=120)
                    known_nodes = [node for node in known_nodes if node not in new_remove_nodes]
                    known_nodes.extend(new_add_nodes)
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(add_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "rebalance_in_out":
            if self.warmup:
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                node = known_nodes[-1]
                self.warmup_node(node)
                operation = self.task.async_rebalance(self.cluster, [], remove_nodes,
                                                      retry_get_process_num=120)
                self.task.jython_task_manager.get_task_result(operation)
                if not operation.result:
                    self.log.info("rebalance was failed as expected")
                    for bucket in self.cluster.buckets:
                        self.assertTrue(self.bucket_util._wait_warmup_completed(
                            bucket, servers=[node]))
                    self.log.info("second attempt to rebalance")
                    self.sleep(60, "wait before starting rebalance after warmup")
                    operation = self.task.async_rebalance(self.cluster, [], remove_nodes,
                                                          retry_get_process_num=120)
                    self.wait_for_rebalance_to_complete(operation)
                self.sleep(60)
            else:
                if self.update_replica:
                    self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                    self.bucket_util.update_all_bucket_replicas(
                        self.cluster, self.updated_num_replicas)
                    self.bucket_util.print_bucket_stats(self.cluster)
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                operation = self.task.async_rebalance(self.cluster, [], remove_nodes,
                                                      retry_get_process_num=120)
                if self.compaction:
                    self.compact_all_buckets()
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
        elif rebalance_operation == "graceful_failover_rebalance_out":
            if step_count == -1:
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=True, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes,
                                                      retry_get_process_num=120)
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and rebalance them out
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=True, wait_for_pending=wait_for_pending)
                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes,
                                                          retry_get_process_num=120)
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_rebalance_out":
            if step_count == -1:
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=False, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes,
                                                      retry_get_process_num=120)
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and rebalance them out
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=False, wait_for_pending=wait_for_pending)
                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes,
                                                          retry_get_process_num=120)
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "graceful_failover_recovery":
            if (step_count == -1):
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=True, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                self.data_load_after_failover()
                # Mark the failover nodes for recovery
                for failover_node in failover_nodes:
                    self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
                if self.compaction:
                    self.compact_all_buckets()
                # Rebalance all the nodes
                operation = self.task.async_rebalance(known_nodes, [], [],
                                                      retry_get_process_num=120)
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and recover
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=True, wait_for_pending=wait_for_pending)

                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [],
                                                          retry_get_process_num=120)
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_recovery":
            if (step_count == -1):
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=False, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                self.data_load_after_failover()
                # Mark the failover nodes for recovery
                for failover_node in failover_nodes:
                    self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
                if self.compaction:
                    self.compact_all_buckets()
                # Rebalance all the nodes
                operation = self.task.async_rebalance(known_nodes, [], [],
                                                      retry_get_process_num=120)
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and recover
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=False, wait_for_pending=wait_for_pending)

                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [],
                                                          retry_get_process_num=120)
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        else:
            self.fail("rebalance_operation is not defined")
        return operation

    def subsequent_data_load(self, async_load=False, data_load_spec=None):
        if data_load_spec is None:
            data_load_spec = self.data_load_spec
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(data_load_spec)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        self.set_retry_exceptions(doc_loading_spec)
        if self.dgm_test:
            if data_load_spec == "dgm_load":
                # pre-load to dgm
                doc_loading_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 2
            else:
                # Do only deletes during dgm + rebalance op
                doc_loading_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        if self.forced_hard_failover and self.spec_name == "multi_bucket.buckets_for_rebalance_tests_more_collections":
            # create collections, else if other bucket_spec - then just "create" ops
            doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 20
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                        self.cluster.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size,
                                                        validate_task=(not self.skip_validations))
        return tasks

    def async_data_load(self):
        tasks = self.subsequent_data_load(async_load=True)
        return tasks

    def sync_data_load(self):
        self.subsequent_data_load()

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(self.cluster, task)
            if task.result is False:
                self.fail("Doc_loading failed")

    def wait_for_compaction_to_complete(self):
        for task in self.compaction_tasks:
            self.task_manager.get_task_result(task)
            self.assertTrue(task.result, "Compaction failed for bucket: %s" %
                            task.bucket.name)


    def wait_for_rebalance_to_complete(self, task, wait_step=120):
        self.task.jython_task_manager.get_task_result(task)
        if self.dgm_test and (not task.result):
            fail_flag = True
            for bucket in self.cluster.buckets:
                result = self.get_active_resident_threshold(bucket.name)
                if result < 20:
                    fail_flag = False
                    self.log.error("DGM less than 20")
                    break
            self.assertFalse(fail_flag, "rebalance failed")
        else:
            self.assertTrue(task.result, "Rebalance Failed")
        if self.compaction:
            self.wait_for_compaction_to_complete()

    def data_validation_collection(self):
        if not self.skip_validations:
            self.log.info("entering validation")
            #if self.data_load_spec == "ttl_load" or self.data_load_spec == "ttl_load1":
            if "expriy" in self.doc_ops:
                self.bucket_util._expiry_pager(self.cluster)
                self.sleep(400, "wait for maxttl to finish")
                items = 0
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)
                for bucket in self.cluster.buckets:
                    items = items + self.bucket_helper_obj.get_active_key_count(bucket)
                if items != 0:
                    self.fail("TTL + rebalance failed")
            elif self.forced_hard_failover:
                pass
            else:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)
                self.bucket_util.validate_docs_per_collections_all_buckets(
                    self.cluster)

    def load_collections_with_rebalance(self, rebalance_operation):
        tasks = None
        rebalance = None

        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.log.info("Initial loading with new loader starts")
        self.new_loader(wait=True)
        self.compute_docs_ranges()
        self.create_perc = self.input.param("create_perc", 0)
        self.read_perc = self.input.param("read_perc", 0)
        self.update_perc = self.input.param("update_perc", 0)
        self.delete_perc = self.input.param("delete_perc", 0)
        self.expiry_perc = self.input.param("expiry_perc", 0)
        collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
        self.log.info("collections list is {}".format(collections))
        if self.num_collections_to_drop > 0:
            self.collections.remove(CbServer.default_collection)
            collections = self.collections[(self.num_collections_to_drop):]
            collections_to_drop = self.collections[:self.num_collections_to_drop]
            self.collections.append(CbServer.default_collection)
            self.log.debug("collections list after dropping collections {}".format(collections))
            self.log.debug("collections_to_drop {}".format(collections_to_drop))

        if self.data_load_stage == "before":
            self.log.info("Data loading before rebalance stage")
            loader_tasks = self.new_loader(scopes=self.scopes, collections=collections)

            if self.num_collections_to_drop > 0:
                self.log.info("Starting to drop collections")
                for collection in collections_to_drop:
                    for scope_name in self.scopes:
                        self.log.info("scope = {}, Collection to be dropped {}".format(scope_name, collection))
                        for bucket in self.cluster.buckets:
                            self.bucket_util.drop_collection(self.cluster.master, bucket,
                                                             scope_name=scope_name,
                                                             collection_name=collection)
                            self.cluster.buckets[self.cluster.buckets.index(bucket)].scopes[scope_name].collections.pop(collection)
                    self.collections.remove(collection)
                    self.log.info("collection count in scope {}is {}".format(scope_name, len(self.collections)))
                self.num_collections = len(self.collections)
                self.log.info("Num collections after dropping collections are {}".format(self.num_collections))
                #self.collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
                self.log.debug("collections list after dropping collections is {}".format(self.collections))

        if rebalance_operation == "rebalance_in":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 tasks=tasks)

        elif rebalance_operation == "rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 tasks=tasks)
        elif rebalance_operation == "swap_rebalance":
            rebalance = self.rebalance_operation(rebalance_operation="swap_rebalance",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_swap],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_swap:],
                                                 tasks=tasks)
        elif rebalance_operation == "rebalance_in_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 tasks=tasks)
        elif rebalance_operation == "graceful_failover_rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="graceful_failover_rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "hard_failover_rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="hard_failover_rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "graceful_failover_recovery":
            rebalance = self.rebalance_operation(rebalance_operation="graceful_failover_recovery",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "hard_failover_recovery":
            rebalance = self.rebalance_operation(rebalance_operation="hard_failover_recovery",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "forced_hard_failover_rebalance_out":
            rebalance = self.forced_failover_operation(known_nodes=self.cluster.servers[:self.nodes_init],
                                                       failover_nodes=self.cluster.servers[:self.nodes_init]
                                                       [-self.nodes_failover:]
                                                       )
        if self.data_load_stage == "during":
            self.sleep(10, "wait for rebalance to start")
            self.log.info("Data loading during rebalance")
            loader_tasks = self.new_loader(scopes=self.scopes, collections=collections)
            if self.num_collections_to_drop > 0:
                self.log.info("Starting to drop collections")
                for collection in collections_to_drop:
                    for scope_name in self.scopes:
                        self.log.info("scope = {}, Collection to be dropped {}".format(scope_name, collection))
                        for bucket in self.cluster.buckets:
                            self.bucket_util.drop_collection(self.cluster.master, bucket,
                                                     scope_name=scope_name,
                                                     collection_name=collection)
                        self.cluster.buckets[self.cluster.buckets.index(bucket)].scopes[scope_name].collections.pop(collection)
                    self.collections.remove(collection)
                    self.log.info("collection count in scope {}is {}".format(scope_name, len(self.collections)))
                self.num_collections = len(self.collections)
                #self.collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
                self.log.debug("collections list after dropping collections is {}".format(self.collections))
        if not self.warmup:
            self.wait_for_rebalance_to_complete(rebalance)
        if self.data_load_stage == "after":
            self.log.info("Data loading after rebalance")
            loader_tasks = self.new_loader(scopes=self.scopes, collections=collections)
            if self.num_collections_to_drop > 0:
                self.log.info("Starting to drop collections")
                for collection in collections_to_drop:
                    for scope_name in self.scopes:
                        self.log.info("Collection to be dropped {}".format(collection))
                        for bucket in self.cluster.buckets:
                            self.bucket_util.drop_collection(self.cluster.master, bucket,
                                                             scope_name=scope_name,
                                                             collection_name=collection)
                        self.cluster.buckets[self.cluster.buckets.index(bucket)].scopes[scope_name].collections.pop(collection)
                    self.collections.remove(collection)
                self.log.info("collection count in scope {}is {}".format(scope_name, len(self.collections)))
                self.num_collections = len(self.collections)
                #self.collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
                self.log.debug("collections list after dropping collections is {}".format(self.collections))
        self.doc_loading_tm.getAllTaskResult()
        self.retry_failures(loader_tasks)
        self.sleep(30, "sleep before validation")
        self.data_validation(scopes=self.scopes, collections=collections)

    def test_data_load_collections_with_rebalance_in(self):
        self.log.info("====test_data_load_collections_with_rebalance_in starts====")
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in")

    def test_data_load_collections_with_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_out")

    def test_data_load_collections_with_swap_rebalance(self):
        self.load_collections_with_rebalance(rebalance_operation="swap_rebalance")

    def test_data_load_collections_with_rebalance_in_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in_out")

    def test_data_load_collections_with_graceful_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="graceful_failover_rebalance_out")

    def test_data_load_collections_with_hard_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="hard_failover_rebalance_out")

    def test_data_load_collections_with_graceful_failover_recovery(self):
        self.load_collections_with_rebalance(rebalance_operation="graceful_failover_recovery")

    def test_data_load_collections_with_hard_failover_recovery(self):
        self.load_collections_with_rebalance(rebalance_operation="hard_failover_recovery")

    def test_data_load_collections_with_forced_hard_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="forced_hard_failover_rebalance_out")

    def test_rebalance_large_batches(self):
        # Number of docs of different sizes
        num_2mb_docs = self.input.param("num_2mb_docs", 1000)
        num_4mb_docs = self.input.param("num_4mb_docs", 1000)
        num_8mb_docs = self.input.param("num_8mb_docs", 1000)
        num_16mb_docs = self.input.param("num_16mb_docs", 500)
        num_20mb_docs = self.input.param("num_20mb_docs", 500)
        num_small_docs = self.input.param("num_small_docs", 5000000)
        small_doc_size = self.input.param("small_doc_size", 2048)

        multi_bucket_test = self.input.param("multi_bucket_test", False)
        upsert_iterations = self.input.param("upsert_iterations", 0)
        rebalance_data_load = self.input.param("rebalance_data_load", False)

        spare_node1 = self.cluster.servers[1]
        spare_node2 = self.cluster.servers[2]
        bucket = self.cluster.buckets[0]
        pillowfight_thread = None

        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(
            self.cluster, self.cluster.nodes_in_cluster)

        # Loading docs of sizes 2MB - 20MB (large docs) using cbc-pillowfight
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_2mb_docs, 2548000, "2mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_4mb_docs, 4548000, "4mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_8mb_docs, 8548000, "8mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_16mb_docs, 16548000, "16mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_20mb_docs, 20548000, "20mbbig_docs")

        self.sleep(20, "Wait for num_items to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)

        total_upsert_iterations = upsert_iterations
        while upsert_iterations > 0:
            self.log.info("Upserting docs iteration: {}"
                          .format(total_upsert_iterations-upsert_iterations+1))
            self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                           num_2mb_docs, 2548000,
                                           "2mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                           num_4mb_docs, 4548000,
                                           "4mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                           num_8mb_docs, 8548000,
                                           "8mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                           num_16mb_docs, 16548000,
                                           "16mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                           num_20mb_docs, 20548000,
                                           "20mbbig_docs")
            upsert_iterations -= 1

        if multi_bucket_test:
            bucket2 = self.cluster.buckets[1]
            self.load_data_cbc_pillowfight(
                self.cluster.master, bucket2,
                num_small_docs, small_doc_size, threads=4)

            self.sleep(20, "Wait for num_items to get reflected")
            self.bucket_util.print_bucket_stats(self.cluster)

        for bucket in self.cluster.buckets:
            self.fetch_vbucket_size(bucket)

        if rebalance_data_load:
            self.log.info("Starting data load during rebalance..")
            pillowfight_thread = threading.Thread(
                target=self.load_data_cbc_pillowfight,
                args=[self.cluster.master, bucket, 1000, 10548000,
                      "10mbbig_docs1", 1, 10])
            pillowfight_thread.start()

        # Rebalancing-in a node
        self.log.info("Rebalancing-in the node: {}".format(spare_node1.ip))
        rebalance_in_res = self.task.rebalance(self.cluster,
                                               to_add=[spare_node1],
                                               to_remove=[],
                                               retry_get_process_num=500)
        if not rebalance_in_res:
            self.log.info("Rebalance-in failed")
        self.cluster_util.print_cluster_stats(self.cluster)

        if rebalance_data_load:
            pillowfight_thread.join()

        if rebalance_data_load:
            self.log.info("Starting data load during rebalance..")
            pillowfight_thread = threading.Thread(
                target=self.load_data_cbc_pillowfight,
                args=[self.cluster.master, bucket, 1000, 10548000,
                      "10mbbig_docs2", 1, 10])
            pillowfight_thread.start()

        # Swap rebalance of a spare node
        self.log.info("Swap rebalance..")
        rebalance_swap_res = self.task.rebalance(self.cluster,
                                                 to_add=[spare_node2],
                                                 to_remove=[spare_node1],
                                                 retry_get_process_num=500)
        if not rebalance_swap_res:
            self.log.info("Swap Rebalance failed")
        self.cluster_util.print_cluster_stats(self.cluster)

        if rebalance_data_load:
            pillowfight_thread.join()

        if rebalance_data_load:
            self.log.info("Starting data load during rebalance..")
            pillowfight_thread = threading.Thread(
                target=self.load_data_cbc_pillowfight,
                args=[self.cluster.master, bucket, 1000, 10548000,
                      "10mbbig_docs3", 1, 10])
            pillowfight_thread.start()

        # Rebalance-out of a node
        self.log.info("Rebalancing-out the node: {}".format(spare_node2.ip))
        rebalance_out_res = self.task.rebalance(self.cluster,
                                                to_add=[],
                                                to_remove=[spare_node2],
                                                retry_get_process_num=500)
        if not rebalance_out_res:
            self.log.info("Rebalance failed")
        self.cluster_util.print_cluster_stats(self.cluster)

        if rebalance_data_load:
            pillowfight_thread.join()
