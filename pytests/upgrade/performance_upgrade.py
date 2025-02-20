import random
import math
import time
import json
import os
import csv

from BucketLib.bucket import Bucket
from cb_constants import CbServer
from membase.api.rest_client import RestConnection
from collections_helper.collections_spec_constants import MetaConstants
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from remote.remote_util import RemoteMachineShellConnection
from gsiLib.gsiHelper import GsiHelper
from table_view import TableView

from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.docgen import DocumentGenerator

from upgrade.upgrade_base import UpgradeBase
from bucket_collections.collections_base import CollectionBase

class PerformanceUpgradeTests(UpgradeBase):

    def setUp(self):
        super(PerformanceUpgradeTests, self).setUp()
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.spec_name = self.input.param("bucket_spec", "upgrade_test.magma_spec")

        # A hiphen separated list of rebalance tasks to be performed post upgrade
        self.rebalance_tasks_post_upgrade = self.input.param("rebalance_tasks_post_upgrade", None)

        self.ops_rate = self.input.param("ops_rate", 1000)
        self.log_path = self.input.param("logs_folder", "/tmp")
        self.index_profile_during_workloads = self.input.param("index_profile_during_workloads", True)
        self.index_profile_during_rebalances = self.input.param("index_profile_during_rebalances", True)

        if self.index_profile_during_workloads or self.index_profile_during_rebalances:
            self.index_profile_tasks = []

        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        self.doc_loading_tm = TaskManager(self.process_concurrency)

        self.log.info("Setting kv mem quota to {} MB".format(self.kv_quota_mem))
        self.log.info("Setting index mem quota to {} MB".format(self.index_quota_mem))
        RestConnection(self.cluster.master).set_service_mem_quota(
            {
                CbServer.Settings.KV_MEM_QUOTA: self.kv_quota_mem,
                CbServer.Settings.INDEX_MEM_QUOTA: self.index_quota_mem
            })

        self.index_count = 0
        self.bucket_load_map = {}
        self.comparison_map = {}

        self.cluster.sdk_client_pool = None

        self.spare_nodes = self.cluster.servers[self.nodes_init:]

    def tearDown(self):
        super(PerformanceUpgradeTests, self).tearDown()

    def set_comparison_queries(self):
        query_workload_time = self.input.param("query_workload_time", 600)
        queries = []
        while len(queries) < query_workload_time:
            queries = queries + self.fetch_queries()

        self.comparison_queries = queries

    def spill_stats_to_disk(self, monitored_stats, title, workload):
        file_name = os.path.join(self.log_path, "{}_{}.json".format(title, workload))
        with open(file_name, "w") as f:
            json.dump(monitored_stats, f)

        return file_name

    def check_index_pending_mutations(self, timeout=18000):
        def print_index_data():
            for node in self.cluster.index_nodes:
                stats = GsiHelper(node, self.log).get_bucket_index_stats()
                for bucket in self.cluster.buckets:
                    for index in stats[bucket.name]:
                        if index == CbServer.system_scope:
                            continue

                        self.log.debug("node: {} bucket : {} index : {}".format(node.ip, bucket, index))

        print_index_data()
        end_time = time.time() + timeout

        check = True
        while check:
            check = False
            for node in self.cluster.index_nodes:
                stats = GsiHelper(node, self.log).get_bucket_index_stats()
                for bucket in self.cluster.buckets:
                    bucket = bucket.name
                    for index in stats[bucket]:
                        if index == CbServer.system_scope:
                            continue

                        self.log.debug("node: {} bucket : {} index : {} num_docs_pending : {}".format(node.ip, bucket, index, stats[bucket][index]["num_docs_pending"]))
                        if stats[bucket][index]["num_docs_pending"] > 0:
                            check = True

            if time.time() > end_time and check:
                self.fail("Index mutations have not been processed after {} seconds".format(timeout))

            if check:
                self.sleep(30, "Wait for index mutations pending")

        print_index_data()

    def start_index_profiling(self):
        tasks = []
        for index_node in self.cluster.index_nodes:
            cpu_task = self.task.async_periodic_profile([index_node], timeout=None, period=0, tasks=["cpu_profile"], profile_dir=self.log_path)
            heap_task = self.task.async_periodic_profile([index_node], timeout=None, period=30, tasks=["heap_profile"], profile_dir=self.log_path)
            go_routines_task = self.task.async_periodic_profile([index_node], timeout=None, period=30, tasks=["go_routines_dump"], profile_dir=self.log_path)
            tasks = tasks + [cpu_task, heap_task, go_routines_task]

        self.index_profile_tasks = tasks

    def stop_index_profiling(self):
        for task in self.index_profile_tasks:
            task.end_task()

    def rebalance_post_upgrade(self, task_name):
        rebalance_tasks = [
            "rebalance_in_out",
            "rebalance_out_in",
            "swap_rebalance",
            "failover_add_back_delta_recovery",
            "failover_add_back_full_recovery",
            "replica_update_rebalance"
            ]
        if task_name not in rebalance_tasks:
            self.fail("Invalid rebalance task name: {}".format(task_name))

        num_nodes_rebalance = self.input.param("num_nodes_rebalance", 1)
        if num_nodes_rebalance > len(self.spare_nodes):
            self.fail("Number of nodes to rebalance is greater than the number of spare nodes")

        if task_name == "rebalance_in_out":
            # First do a rebalance in-out kv nodes, then index-query nodes
            to_add = self.spare_nodes[:num_nodes_rebalance]

            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_add)
            result = self.task.rebalance(self.cluster, to_add, [],
                                         services=["kv"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_add)
            result = self.task.rebalance(self.cluster, [], to_add,
                                         services=["kv"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            # First do a rebalance in-out kv nodes, then index-query nodes
            to_add = self.spare_nodes[:num_nodes_rebalance]

            self.cluster_util.print_cluster_stats(self.cluster)
            self.rebalance_workload(to_add)
            result = self.task.rebalance(self.cluster, to_add, [],
                                         services=["index,n1ql"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_add)
            result = self.task.rebalance(self.cluster, [], to_add,
                                         services=["index,n1ql"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

        elif task_name == "rebalance_out_in":
            # First do a rebalance out-in kv nodes, then index-query nodes
            to_remove = self.cluster.kv_nodes[:num_nodes_rebalance]

            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_remove)
            result = self.task.rebalance(self.cluster, [], to_remove,
                                         services=["kv"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_remove)
            result = self.task.rebalance(self.cluster, to_remove, [],
                                         services=["kv"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            # First do a rebalance in-out kv nodes, then index-query nodes
            to_remove = self.cluster.index_nodes[:num_nodes_rebalance]

            self.cluster_util.print_cluster_stats(self.cluster)
            self.rebalance_workload(to_remove)
            result = self.task.rebalance(self.cluster, [], to_remove,
                                         services=["index,n1ql"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_remove)
            result = self.task.rebalance(self.cluster, to_remove, [],
                                         services=["index,n1ql"] * num_nodes_rebalance,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

        elif task_name == "swap_rebalance":
            # First do a rebalance out-in kv nodes, then index-query nodes
            to_add = self.spare_nodes[:num_nodes_rebalance]
            to_remove = self.cluster.kv_nodes[:num_nodes_rebalance]

            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_remove)
            result = self.task.rebalance(self.cluster, to_add, to_remove,
                                         services=["kv"] * num_nodes_rebalance,
                                         check_vbucket_shuffling=False,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_add)
            result = self.task.rebalance(self.cluster, to_remove, to_add,
                                         services=["kv"] * num_nodes_rebalance,
                                         check_vbucket_shuffling=False,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            # First do a rebalance in-out kv nodes, then index-query nodes
            to_add = self.spare_nodes[:num_nodes_rebalance]
            to_remove = self.cluster.index_nodes[:num_nodes_rebalance]

            self.cluster_util.print_cluster_stats(self.cluster)
            self.rebalance_workload(to_remove)
            result = self.task.rebalance(self.cluster, to_add, to_remove,
                                         services=["index,n1ql"] * num_nodes_rebalance,
                                         check_vbucket_shuffling=False,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

            self.rebalance_workload(to_add)
            result = self.task.rebalance(self.cluster, to_remove, to_add,
                                         services=["index,n1ql"] * num_nodes_rebalance,
                                         check_vbucket_shuffling=False,
                                         validate_bucket_ranking=True,
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")
            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)

        elif task_name == "failover_add_back_delta_recovery":
            node_to_failover = self.cluster.kv_nodes[0]
            rest = RestConnection(self.cluster.master)
            nodes = rest.node_statuses()
            for node in nodes:
                if node.ip == node_to_failover.ip:
                    otp_node = node
                    break

            self.rebalance_workload([node_to_failover])

            self.log.info("Failing over the node {0}".format(otp_node.ip))
            failover_task = rest.fail_over(otp_node.id, graceful=True)

            if(failover_task):
                self.log.info("Graceful Failover of the node successful")
            else:
                self.log.info("Failover failed")

            rebalance_passed = rest.monitorRebalance(progress_count=3600)

            if(rebalance_passed):
                self.log.info("Failover rebalance passed")
                self.cluster_util.print_cluster_stats(self.cluster)

            # Validate orchestrator selection
            self.cluster_util.validate_orchestrator_selection(self.cluster)

            self.wait_for_rebalance_workload_completion()

            rest.set_recovery_type(otp_node.id,
                                   recoveryType="delta")

            # Validate orchestrator selection
            self.cluster_util.validate_orchestrator_selection(self.cluster)

            delta_recovery_buckets = [bucket.name for bucket in self.cluster.buckets]

            self.rebalance_workload([node_to_failover])

            self.log.info("Rebalance starting...")
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                           deltaRecoveryBuckets=delta_recovery_buckets)
            rebalance_passed = rest.monitorRebalance(progress_count=3600)

            if(rebalance_passed):
                self.log.info("Rebalance after recovery completed")
                self.cluster_util.print_cluster_stats(self.cluster)

            # Validate orchestrator selection
            self.cluster_util.validate_orchestrator_selection(self.cluster)

            self.wait_for_rebalance_workload_completion()

        elif task_name == "failover_add_back_full_recovery":
            node_to_failover = self.cluster.kv_nodes[0]
            rest = RestConnection(self.cluster.master)
            nodes = rest.node_statuses()
            for node in nodes:
                if node.ip == node_to_failover.ip:
                    otp_node = node
                    break

            self.rebalance_workload([node_to_failover])

            self.log.info("Failing over the node {0}".format(otp_node.ip))
            failover_task = rest.fail_over(otp_node.id, graceful=False)

            if(failover_task):
                self.log.info("Hard Failover of the node successful")
            else:
                self.log.info("Failover failed")

            rebalance_passed = rest.monitorRebalance(progress_count=3600)

            if(rebalance_passed):
                self.log.info("Failover rebalance passed")
                self.cluster_util.print_cluster_stats(self.cluster)

            # Validate orchestrator selection
            self.cluster_util.validate_orchestrator_selection(self.cluster)

            self.wait_for_rebalance_workload_completion()

            rest.set_recovery_type(otp_node.id, recoveryType="full")

            # Validate orchestrator selection
            self.cluster_util.validate_orchestrator_selection(self.cluster)

            self.rebalance_workload([node_to_failover])

            self.log.info("Rebalance starting...")
            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()])
            rebalance_passed = rest.monitorRebalance(progress_count=3600)

            if(rebalance_passed):
                self.log.info("Rebalance after recovery completed")
                self.cluster_util.print_cluster_stats(self.cluster)

            # Validate orchestrator selection
            self.cluster_util.validate_orchestrator_selection(self.cluster)

            self.wait_for_rebalance_workload_completion()

        elif task_name == "replica_update_rebalance":
            self.bucket_util.update_bucket_property(
                self.cluster.master, self.cluster.buckets[0],
                replica_number=self.cluster.buckets[0].replicaNumber + 1)

            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            self.rebalance_workload([])

            result = self.task.rebalance(self.cluster, [], [],
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")

            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            self.bucket_util.update_bucket_property(
                self.cluster.master, self.cluster.buckets[0],
                replica_number=self.cluster.buckets[0].replicaNumber - 1)

            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            self.rebalance_workload([])
            result = self.task.rebalance(self.cluster, [], [],
                                         retry_get_process_num=500)
            self.assertTrue(result, "Rebalance failed")

            self.wait_for_rebalance_workload_completion()
            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

    def rebalance_workload(self, nodes_out):
        create_perc = random.choice(range(0,101, 2))
        delete_perc = create_perc // 2
        create_perc = create_perc // 2

        read_perc = random.choice(range(0, 101 - (create_perc + delete_perc)))
        update_perc = 100 - (create_perc + delete_perc + read_perc)

        query_nodes = []
        for node in self.cluster.query_nodes:
            flag = True
            for node1 in nodes_out:
                if node.ip == node1.ip:
                    flag = False
                    break
            if flag:
                query_nodes.append(node)

        if len(query_nodes) == 0:
            self.fail("Query node not found")

        tasks = self.use_java_doc_loader(create_perc=create_perc, read_perc=read_perc, update_perc=update_perc, delete_perc=delete_perc,
                                         ops_rate=self.ops_rate, validate_data=False, async_load=True)

        queries = self.fetch_queries()

        query_tasks = []

        for query_node in query_nodes:
            query_task = self.task.async_execute_queries(query_node, queries)
            query_tasks.append(query_task)

        self.rebalance_query_tasks = query_tasks
        self.rebalance_workload_tasks = tasks

    def wait_for_rebalance_workload_completion(self):
        self.log.info("Waiting for Rebalance Workload to complete")

        for task in self.rebalance_query_tasks:
            task.end_task()
            self.task_manager.get_task_result(task)

        self.wait_for_java_doc_loader_completion(self.rebalance_workload_tasks)

        self.check_index_pending_mutations()

    def comparison_workload(self, title):
        self.comparison_map[title] = {}
        create_perc = self.input.param("create_perc", 20)
        read_perc = self.input.param("read_perc", 20)
        update_perc = self.input.param("update_perc", 40)
        delete_perc = self.input.param("delete_perc", 20)
        query_workload_time = self.input.param("query_workload_time", 600)
        idle_workload_time = self.input.param("idle_workload_time", 600)
        wait_time_between_workloads = self.input.param("wait_time_between_workloads", 180)
        period_of_comparison = self.input.param("period_of_comparison", 1)
        workloads_to_run = self.input.param("workloads_to_run", None) # A hiphen separated list of workloads to run

        all_workloads = ["CRUD", "Query", "CRUD+Query", "Idle"]

        if workloads_to_run is None:
            workloads_to_run = all_workloads
        else:
            workloads_to_run = workloads_to_run.split("-")
            for workload in workloads_to_run:
                if workload not in all_workloads:
                    self.fail("Invalid workload name: {}".format(workload))

        if "CRUD" in workloads_to_run:

            if self.index_profile_during_workloads:
                self.start_index_profiling()

            ### WORKLOAD 1 - CRUD Workload - x% create, x% deletes, y% update, z% reads
            tasks = self.use_java_doc_loader(create_perc=create_perc, read_perc=read_perc, update_perc=update_perc, delete_perc=delete_perc,
                                             ops_rate=self.ops_rate, validate_data=False, async_load=True)

            monitor_task = self.task.async_monitor_cluster_stats(self.cluster, store_csv=True, csv_dir=self.log_path, period=period_of_comparison)

            self.wait_for_java_doc_loader_completion(tasks)

            monitor_task.end_task()
            monitored_stats = self.task_manager.get_task_result(monitor_task)

            workload = "CRUD Workload"
            self.comparison_map[title][workload] = self.spill_stats_to_disk(monitored_stats, title, workload)

            self.PrintStep("self.comparison_map[{}]['CRUD Workload'] ---> {}".format(title, self.comparison_map[title]["CRUD Workload"]))

            self.check_index_pending_mutations()

            if self.index_profile_during_workloads:
                self.stop_index_profiling()

            self.sleep(wait_time_between_workloads, "Sleeping for {} seconds before running the next workload".format(wait_time_between_workloads))

        if "Query" in workloads_to_run:

            if self.index_profile_during_workloads:
                self.start_index_profiling()

            ### WORKLOAD 2 - Query Workload - Run queries for query_workload_time seconds

            query_tasks = []
            for query_node in self.cluster.query_nodes:
                query_task = self.task.async_execute_queries(query_node, self.comparison_queries, timeout=query_workload_time, shuffle_queries=False)
                query_tasks.append(query_task)
            monitor_task = self.task.async_monitor_cluster_stats(self.cluster, store_csv=True, csv_dir=self.log_path, period=period_of_comparison)

            for query_task in query_tasks:
                self.task_manager.get_task_result(query_task)

            monitor_task.end_task()
            monitored_stats = self.task_manager.get_task_result(monitor_task)
            workload = "Query Workload"
            self.comparison_map[title][workload] = self.spill_stats_to_disk(monitored_stats, title, workload)

            self.PrintStep("self.comparison_map[{}]['Query Workload'] ---> {}".format(title, self.comparison_map[title]["Query Workload"]))

            self.check_index_pending_mutations()

            if self.index_profile_during_workloads:
                self.stop_index_profiling()

            self.sleep(wait_time_between_workloads, "Sleeping for {} seconds before running the next workload".format(wait_time_between_workloads))

        if "CRUD+Query" in workloads_to_run:

            if self.index_profile_during_workloads:
                self.start_index_profiling()

            ### WORKLOAD 3 - CRUD (x% create, x% deletes, y% update, z% reads) + Query Workload
            tasks = self.use_java_doc_loader(create_perc=create_perc, read_perc=read_perc, update_perc=update_perc, delete_perc=delete_perc,
                                            ops_rate=self.ops_rate, validate_data=False, async_load=True)

            query_tasks = []
            for query_node in self.cluster.query_nodes:
                query_task = self.task.async_execute_queries(query_node, self.comparison_queries, shuffle_queries=False)
                query_tasks.append(query_task)

            monitor_task = self.task.async_monitor_cluster_stats(self.cluster, store_csv=True, csv_dir=self.log_path, period=period_of_comparison)

            self.wait_for_java_doc_loader_completion(tasks)

            for query_task in query_tasks:
                query_task.end_task()
                self.task_manager.get_task_result(query_task)

            monitor_task.end_task()
            monitored_stats = self.task_manager.get_task_result(monitor_task)
            workload = "Query + CRUD Workload"
            self.comparison_map[title][workload] = self.spill_stats_to_disk(monitored_stats, title, workload)

            self.PrintStep("self.comparison_map[{}]['Query + CRUD Workload'] ---> {}".format(title, self.comparison_map[title]["Query + CRUD Workload"]))

            self.check_index_pending_mutations()

            if self.index_profile_during_workloads:
                self.stop_index_profiling()

            self.sleep(wait_time_between_workloads, "Sleeping for {} seconds before running the next workload".format(wait_time_between_workloads))

        if "Idle" in workloads_to_run:

            if self.index_profile_during_workloads:
                self.start_index_profiling()

            ### WORKLOAD 4 - Idle workload for idle_workload_time seconds
            monitor_task = self.task.async_monitor_cluster_stats(self.cluster, store_csv=True, csv_dir=self.log_path, period=period_of_comparison)

            self.sleep(idle_workload_time, "Running idle workload for {} seconds".format(idle_workload_time))

            monitor_task.end_task()
            monitored_stats = self.task_manager.get_task_result(monitor_task)

            workload = "Idle Workload"
            self.comparison_map[title][workload] = self.spill_stats_to_disk(monitored_stats, title, workload)

            if self.index_profile_during_workloads:
                self.stop_index_profiling()

            self.PrintStep("self.comparison_map[{}]['Idle Workload'] ---> {}".format(title, self.comparison_map[title]["Idle Workload"]))

    def initialize_bucket_load_map_for_bucket(self, bucket):
        if bucket.name not in self.bucket_load_map:
            self.bucket_load_map[bucket.name] = {}

        for scope in bucket.scopes.keys():
            if scope == CbServer.system_scope:
                continue

            if scope not in self.bucket_load_map[bucket.name]:
                self.bucket_load_map[bucket.name][scope] = {}

            for collection in bucket.scopes[scope].collections.keys():
                if collection not in self.bucket_load_map[bucket.name][scope]:
                    self.bucket_load_map[bucket.name][scope][collection] = {
                            "start": 0,
                            "end": 0,
                            "val_type": None,
                            "key_type": None
                    }

    def create_hotel_dataset_bucket(self):
        self.PrintStep("Creating new buckets for index query comparisons")

        num_scopes = self.input.param("num_scopes", 1)
        num_collections = self.input.param("num_collections", 1)
        bucket_ram_quota = self.input.param("bucket_ram_quota", 2048)
        num_docs = self.input.param("num_docs", 1000000)

        hotel_bucket_spec = self.bucket_util.get_bucket_template_from_package(self.spec_name)
        hotel_bucket_spec["buckets"] = {}

        hotel_bucket_spec[MetaConstants.USE_SIMPLE_NAMES] = True
        hotel_bucket_spec[MetaConstants.NUM_BUCKETS] = 1
        hotel_bucket_spec["buckets"]["hotel-bucket"] = {
            MetaConstants.NUM_SCOPES_PER_BUCKET: num_scopes,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: num_collections,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000,
            Bucket.ramQuotaMB: bucket_ram_quota,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
        }

        CollectionBase.over_ride_bucket_template_params(self, self.bucket_storage,
                                                        hotel_bucket_spec)
        self.bucket_util.create_buckets_using_json_data(self.cluster, hotel_bucket_spec)

        for bucket in self.cluster.buckets:
            if bucket.name == "hotel-bucket":
                self.hotel_bucket = bucket

        if hasattr(self, "upgrade_chain") and \
            float(self.upgrade_chain[0][:3]) < 7.6:
            for coll in self.hotel_bucket.scopes[CbServer.system_scope].collections:
                self.hotel_bucket.scopes[CbServer.system_scope].collections.pop(coll)
            self.hotel_bucket.scopes.pop(CbServer.system_scope)

        if not (hasattr(self, "upgrade_chain")
                and int(self.upgrade_chain[0][0]) < 7):
            self.bucket_util.wait_for_collection_creation_to_complete(self.cluster)

        self.bucket_util.print_bucket_stats(self.cluster)

        self.initialize_bucket_load_map_for_bucket(self.hotel_bucket)

        self.PrintStep("Loading hotel dataset into all collections")

        validate_data=True
        self.use_java_doc_loader(create_perc=100,
                                 ops_rate=self.ops_rate,
                                 validate_data=validate_data,
                                 async_load=False,
                                 key_type="RandomKey",
                                 value_type="Hotel",
                                 num_items=num_docs)

        self.bucket_util.print_bucket_stats(self.cluster)

        self.PrintStep("Upserting docs to increase fragmentation value")

        self.use_java_doc_loader(update_perc=100,
                                 ops_rate=self.ops_rate,
                                 validate_data=False,
                                 async_load=False)

        for server in self.cluster.kv_nodes:
            frag_val = self.bucket_util.get_fragmentation_kv(self.cluster, bucket, server)
            self.log.debug("Current Fragmentation for node {} is {}".format(server.ip, frag_val))

    def create_new_indexes_for_hotel_dataset(self):
        self.PrintStep("Creating new indexes for the hotel dataset")
        indexes = [
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(price) USING GSI  WITH {{'defer_build': False}}",
            "CREATE PRIMARY INDEX `{}` ON `{}`.`{}`.`{}` USING GSI",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(free_breakfast,avg_rating) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(free_breakfast,type,free_parking,array_count(public_likes),price,country) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews when r.ratings.Cleanliness < 4 END,country,email,free_parking) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(city INCLUDE MISSING DESC,avg_rating,country) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(name) PARTITION BY HASH (name) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(price, All ARRAY v.ratings.Overall FOR v IN reviews END) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(price, All ARRAY v.ratings.Rooms FOR v IN reviews END) USING GSI  WITH {{'defer_build': False}}",
            "CREATE  INDEX `{}` ON `{}`.`{}`.`{}`(country,DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count(`public_likes`),array_count(`reviews`) DESC,`type`,phone,price,email,address,name,url) USING GSI  WITH {{'defer_build': False}}"
        ]

        self.query_client = RestConnection(self.cluster.query_nodes[0])

        idx_prefix = "hotel_index"
        count = 0
        self.log.info("Creating indexes for the new collection")
        for scope in self.hotel_bucket.scopes.keys():
            if scope == CbServer.system_scope:
                continue
            for collection in self.hotel_bucket.scopes[scope].collections.keys():
                for idx_query in indexes:
                    idx_name = idx_prefix + str(count)
                    idx_query = idx_query.format(idx_name, self.hotel_bucket.name,
                                                scope, collection)
                    self.log.info("Index query = {}".format(idx_query))
                    result = self.query_client.query_tool(idx_query, timeout=3600)
                    self.log.info("Result for creation of {} = {}".format(idx_name, result))
                    if result['status'] == 'success':
                        self.index_count += 1
                    count += 1

    def wait_for_java_doc_loader_completion(self, tasks=[]):
        self.log.info("Waiting for Java Doc Loader to complete")
        DocLoaderUtils.wait_for_doc_load_completion(self.doc_loading_tm, tasks)

    def use_java_doc_loader(self, create_perc=0, read_perc=0, update_perc=0, mutated=0, delete_perc=0,
                            ops_rate=1000, validate_data=True, async_load=True,
                            key_type="SimpleKey", value_type="SimpleValue",
                            buckets_map={}, num_items=0):
            """
            The template of buckets_map
            buckets_map = {
                "bucket_1" : {
                    "scope_1" : ["collection_1", "collection_2"],
                    "scope_2" : ["collection_3", "collection_4"],
                },
                "bucket_2" : {
                    "scope_3" : ["collection_5", "collection_6"],
                    "scope_4" : ["collection_7", "collection_8"],
                }
            }
            """

            if create_perc + read_perc + update_perc + delete_perc != 100:
                self.fail("Invalid workload settings")

            if len(buckets_map) == 0:
                for bucket in self.cluster.buckets:
                    buckets_map[bucket.name] = {}
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
                        buckets_map[bucket.name][scope] = bucket.scopes[scope].collections.keys()

            loader_map = dict()
            buckets = []

            for bucket in self.cluster.buckets:
                if bucket.name not in self.bucket_load_map:
                    self.fail("Bucket {} not found in bucket_load_map".format(bucket.name))

                if bucket.name not in buckets_map:
                    continue

                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue

                    if scope not in self.bucket_load_map[bucket.name]:
                        self.fail("Scope {}.{} not found in bucket_load_map".format(bucket.name, scope))

                    if scope not in buckets_map[bucket.name]:
                        continue

                    for collection in bucket.scopes[scope].collections.keys():
                        if collection not in self.bucket_load_map[bucket.name][scope]:
                            self.fail("Collection {}.{}.{} not found in bucket_load_map".format(bucket.name, scope, collection))

                        if collection not in buckets_map[bucket.name][scope]:
                            continue

                        if self.bucket_load_map[bucket.name][scope][collection]["end"] == 0:
                            if update_perc != 0 or delete_perc != 0 or read_perc != 0:
                                self.fail("Invalid workload settings. Only create workload is allowed")

                            self.bucket_load_map[bucket.name][scope][collection]["val_type"] = value_type
                            self.bucket_load_map[bucket.name][scope][collection]["key_type"] = key_type

                            create_start = 0
                            create_end = num_items * create_perc // 100
                            read_start = 0
                            read_end = 0
                            update_start = 0
                            update_end = 0
                            delete_start = 0
                            delete_end = 0

                            self.bucket_load_map[bucket.name][scope][collection]["start"] = create_start
                            self.bucket_load_map[bucket.name][scope][collection]["end"] = create_end

                        else:
                            start = self.bucket_load_map[bucket.name][scope][collection]["start"]
                            end = self.bucket_load_map[bucket.name][scope][collection]["end"]

                            create_start = end
                            create_end = create_start + int((create_perc / 100.0) * (end - start))

                            delete_start = start
                            delete_end = delete_start + int((delete_perc / 100.0) * (end - start))

                            read_start = delete_end
                            read_end = read_start + int((read_perc / 100.0) * (end - start))

                            update_start = read_end
                            update_end = update_start + int((update_perc / 100.0) * (end - start))

                            self.bucket_load_map[bucket.name][scope][collection]["start"] = delete_end
                            self.bucket_load_map[bucket.name][scope][collection]["end"] = create_end

                            key_type = self.bucket_load_map[bucket.name][scope][collection]["key_type"]
                            value_type = self.bucket_load_map[bucket.name][scope][collection]["val_type"]

                        print("\n\nBucket = {}, Scope = {}, Collection = {}, create_start = {}, create_end = {}, read_start = {}, read_end = {}, update_start = {}, update_end = {}, delete_start = {}, delete_end = {}".format(bucket.name, scope, collection, create_start, create_end, read_start, read_end, update_start, update_end, delete_start, delete_end))

                        work_load_settings = DocLoaderUtils.get_workload_settings(
                            key=self.key, key_size=self.key_size,
                            doc_size=self.doc_size,
                            create_perc=100, create_start=create_start,
                            create_end=create_end, read_perc=100,
                            read_start=read_start, read_end=read_end,
                            update_start=update_start, update_end=update_end,
                            update_perc=100, mutated=mutated,
                            delete_start=delete_start, delete_end=delete_end,
                            delete_perc=100, ops_rate=ops_rate,
                            key_type=key_type, value_type=value_type)

                        dg = DocumentGenerator(work_load_settings,
                                               key_type, value_type)
                        loader_map.update(
                            {"{}:{}:{}".format(bucket.name, scope, collection): dg})

                        buckets.append(bucket)

            _, tasks = DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, loader_map,
                self.cluster, buckets,
                async_load=async_load)

            if validate_data and not async_load:
                result = DocLoaderUtils.data_validation(
                    self.doc_loading_tm, loader_map, self.cluster,
                    buckets=buckets,
                    process_concurrency=self.process_concurrency,
                    ops_rate=ops_rate)
                self.assertTrue(result, "Data validation failed")

            return tasks

    def set_indexer_params(self):
        redistribute_indexes = self.input.param("redistribute_indexes", True)
        index_replica = self.input.param("index_replica", 0)
        enable_page_bloom_filter = self.input.param("enable_page_bloom_filter", False)
        indexer_threads = self.input.param("indexer_threads", 0)
        memory_snapshot_interval = self.input.param("memory_snapshot_interval", 200)
        stable_snapshot_interval = self.input.param("stable_snapshot_interval", 5000)
        max_rollback_points = self.input.param("max_rollback_points", 2)
        index_log_level = self.input.param("index_log_level", "info")
        index_storage_mode = self.input.param("index_storage_mode", "plasma")
        enable_shard_affinity = self.input.param("enable_shard_affinity", None)

        redistribute_indexes = str(redistribute_indexes).lower()
        enable_page_bloom_filter = str(enable_page_bloom_filter).lower()
        enable_shard_affinity = str(enable_shard_affinity).lower()

        # If version < 7.6, then enable_shard_affinity = None
        if float(self.cluster.version[:3]) < 7.6:
            enable_shard_affinity = None

        index_query_node = self.cluster.index_nodes[0]
        rest = RestConnection(index_query_node)

        self.log.info("Setting indexer params")
        rest.set_indexer_params(redistributeIndexes=redistribute_indexes,
                                numReplica=index_replica,
                                enablePageBloomFilter=enable_page_bloom_filter,
                                indexerThreads=indexer_threads,
                                memorySnapshotInterval=memory_snapshot_interval,
                                stableSnapshotInterval=stable_snapshot_interval,
                                maxRollbackPoints=max_rollback_points,
                                logLevel=index_log_level,
                                storageMode=index_storage_mode,
                                enableShardAffinity=enable_shard_affinity)

    def fetch_queries(self):
        queries_template = [
            "SELECT price FROM `{}`.`{}`.`{}` WHERE price > 0 LIMIT {} OFFSET {}",
            # "SELECT suffix FROM `{}`.`{}`.`{}` WHERE suffix is not NULL LIMIT {} OFFSET {}",
            "SELECT name FROM `{}`.`{}`.`{}` WHERE avg_rating > 3 AND free_breakfast = true LIMIT {} OFFSET {}",
            "SELECT country, avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice FROM `{}`.`{}`.`{}` WHERE free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 and `type`='Hotel' group by country LIMIT {} OFFSET {}",
            "SELECT name FROM `{}`.`{}`.`{}` WHERE ANY r IN reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country IS NOT NULL LIMIT {} OFFSET {}",
            'SELECT name FROM `{}`.`{}`.`{}` WHERE avg_rating > 3 AND country like "%F%" LIMIT {} OFFSET {}',
            'SELECT name FROM `{}`.`{}`.`{}` WHERE name like "%Dil%" LIMIT {} OFFSET {}',
            'SELECT address FROM `{}`.`{}`.`{}` WHERE ANY v IN reviews SATISFIES v.ratings.`Overall` > 3  END and price < 1000 LIMIT {} OFFSET {}',
            'SELECT name FROM `{}`.`{}`.`{}` WHERE ANY v IN reviews SATISFIES v.ratings.`Rooms` > 3  END and price > 1000 LIMIT {} OFFSET {}',
            'SELECT address FROM `{}`.`{}`.`{}` WHERE country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) LIMIT {} OFFSET {}'
        ]
        queries = []
        num_docs = self.input.param("num_docs", 1000000)
        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    for query in queries_template:
                        offset = random.choice(range(0, num_docs))
                        limit = random.choice(range(0, 200))
                        query = query.format(bucket.name, scope, collection, limit, offset)
                        queries.append(query)

        return queries

    def compare_stats(self):
        def print_stats_table(stats_dict, title_to_cmp, workload_to_cmp):
            table = TableView(self.log.info)

            table.set_headers(["Stat", "Field", initial_version_title, title_to_cmp, "Percentage Change", "Increase Threshold", "Decrease Threshold"])

            table_row_dict = {}

            for title in stats_dict:
                if title != initial_version_title and title != title_to_cmp:
                    continue
                for workload in stats_dict[title]:
                    if workload != workload_to_cmp:
                        continue

                    for stat in stats_dict[title][workload]:
                        for field in stats_dict[title][workload][stat]:
                            mean =  stats_dict[title][workload][stat][field]["mean"]
                            median = stats_dict[title][workload][stat][field]["median"]
                            variance = stats_dict[title][workload][stat][field]["variance"]
                            std_dev = stats_dict[title][workload][stat][field]["std_dev"]
                            skewness = stats_dict[title][workload][stat][field]["skewness"]
                            excess_kurtosis = stats_dict[title][workload][stat][field]["excess_kurtosis"]
                            sample_size = stats_dict[title][workload][stat][field]["sample_size"]

                            if stat not in table_row_dict:
                                table_row_dict[stat] = {}

                            if field not in table_row_dict[stat]:
                                table_row_dict[stat][field] = {}

                            if title not in table_row_dict[stat][field]:
                                table_row_dict[stat][field][title] = {}

                            table_row_dict[stat][field][title]["mean"] = mean
                            table_row_dict[stat][field][title]["median"] = median
                            table_row_dict[stat][field][title]["variance"] = variance
                            table_row_dict[stat][field][title]["std_dev"] = std_dev
                            table_row_dict[stat][field][title]["skewness"] = skewness
                            table_row_dict[stat][field][title]["excess_kurtosis"] = excess_kurtosis
                            table_row_dict[stat][field][title]["sample_size"] = sample_size

            for stat in table_row_dict:
                row = [""] * 7
                table.add_row(row)
                row = [""] * 7
                row[0] = stat
                table.add_row(row)

                for field in table_row_dict[stat]:
                    row = [""] * 7
                    table.add_row(row)

                    row = [""] * 7
                    row[1] = field
                    table.add_row(row)


                    for metric in table_row_dict[stat][field][initial_version_title]:
                        row = list()

                        row.append("")
                        row.append("")

                        initial_val = table_row_dict[stat][field][initial_version_title][metric]
                        title_val = table_row_dict[stat][field][title_to_cmp][metric]

                        row.append("{}: {}".format(metric, initial_val))
                        row.append("{}: {}".format(metric, title_val))

                        if initial_val != 0:
                            percentage_change = (1.0 * (title_val - initial_val))/ initial_val
                        else:
                            percentage_change = 0
                        if percentage_change >= 0:
                            percentage_change = "+{}".format(percentage_change)

                        row.append("{}".format(percentage_change))
                        row.append("{}".format(increase_threshold_dict[stat]))
                        row.append("{}".format(decrease_threshold_dict[stat]))

                        table.add_row(row)

            table.display("Comparison of stats for workload {} of {} with {}".format(workload_to_cmp, title_to_cmp, initial_version_title))

        def compare_metric(value1, value2, increase_threshold, decrease_threshold):
            diff = abs(value1 - value2)
            if diff == 0:
                self.log.debug("Value1 = {}, Value2 = {}, diff = {}".format(value1, value2, diff))
                return True

            perc_change = ((1.0 * diff) / value1) if value1 != 0 else None

            if value2 >= value1:
                res = perc_change < increase_threshold if perc_change is not None else False
                self.log.debug("Value1 = {}, Value2 = {}, diff = {}, percentage_increase = {}, increase_threshold = {}".format(value1, value2, diff, perc_change, increase_threshold))
            else:
                res = perc_change > decrease_threshold if perc_change is not None else False
                self.log.debug("Value1 = {}, Value2 = {}, diff = {}, percentage_decrease = {}, decrease_threshold = {}".format(value1, value2, diff, perc_change, decrease_threshold))

            return res

        def mean(series):
            return (1.0 * sum(series)) / len(series)

        def median(series):
            series_sorted = sorted(series)
            n = len(series_sorted)
            if n % 2 == 0:
                return (series_sorted[n // 2 - 1] + series_sorted[n // 2]) / 2.0
            else:
                return series_sorted[n // 2]

        def variance(series):
            avg = mean(series)
            return (1.0 * sum((x - avg) ** 2 for x in series)) / len(series)

        def std_dev(series):
            variance_series = variance(series)
            return math.sqrt(variance_series)

        def skewness(series):
            avg = mean(series)
            n = len(series)
            std_dev_series = std_dev(series)
            if std_dev_series == 0:
                return 0
            return (1.0 * sum((x - avg) ** 3 for x in series)) / (n * (std_dev_series ** 3))

        def excess_kurtosis(series):
            avg = mean(series)
            n = len(series)
            std_dev_series = std_dev(series)
            if std_dev_series == 0:
                return 0
            return (1.0 * sum((x - avg) ** 4 for x in series)) / (n * (std_dev_series ** 4)) - 3.0

        def convert_to_series_dict(data):
            result = {}
            for timestamp, ip_data in data:
                for ip, value in ip_data.items():
                    if ip not in result:
                        result[ip] = []
                    result[ip].append(value)

            return result

        def create_stat_csv_all(comparison_map):
            def generate_csv(data, csv_path):
                with open(csv_path, 'w') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(data.keys())
                    writer.writerows(zip(*data.values()))

            csv_data = {}
            for comparison_title in comparison_map:
                for workload in comparison_map[comparison_title]:
                    with open(comparison_map[comparison_title][workload], 'r') as file:
                        stats = json.load(file)

                    for stat in stats:
                        series_dict = convert_to_series_dict(stats[stat])
                        for field in series_dict:

                            if workload not in csv_data:
                                csv_data[workload] = {}

                            if stat not in csv_data[workload]:
                                csv_data[workload][stat] = {}

                            if field not in csv_data[workload][stat]:
                                csv_data[workload][stat][field] = {}

                            if comparison_title not in csv_data[workload][stat][field]:
                                csv_data[workload][stat][field][comparison_title] = []

                            csv_data[workload][stat][field][comparison_title] = series_dict[field]

            table = TableView(self.log.info)
            table.set_headers(["Workload", "Stat", "Field", "Path to csv"])

            for workload in csv_data:
                for stat in csv_data[workload]:
                    for field in csv_data[workload][stat]:
                        csv_path = os.path.join(self.log_path, "comparison_{}_{}_{}.csv".format(workload, stat, field))
                        generate_csv(csv_data[workload][stat][field], csv_path)
                        self.log.debug("CSV file for the workload {}: monitored stat {}: field {} generated at {}".format(workload, stat, field, csv_path))
                        table.add_row([workload, stat, field, csv_path])

            table.display("CSV files for the monitored stats")

        try:
            bucket_disk_used_increase_threshold = self.input.param("bucket_disk_used_increase_threshold", 0.6)
            bucket_item_count_increase_threshold = self.input.param("bucket_item_count_increase_threshold", 0.1)
            node_cpu_utilization_increase_threshold = self.input.param("node_cpu_utilization_increase_threshold", 0.1)
            index_total_disk_size_increase_threshold = self.input.param("index_total_disk_size_increase_threshold", 0.2)
            bucket_memory_used_increase_threshold = self.input.param("bucket_memory_used_increase_threshold", 0.1)
            node_memory_free_increase_threshold = self.input.param("node_memory_free_increase_threshold", 0.1)
            index_memory_used_increase_threshold = self.input.param("index_memory_used_increase_threshold", 0.1)
            bucket_disk_used_decrease_threshold = self.input.param("bucket_disk_used_decrease_threshold", 0.3)
            bucket_item_count_decrease_threshold = self.input.param("bucket_item_count_decrease_threshold", 0.01)
            node_cpu_utilization_decrease_threshold = self.input.param("node_cpu_utilization_decrease_threshold", 0.3)
            index_total_disk_size_decrease_threshold = self.input.param("index_total_disk_size_decrease_threshold", 0.6)
            bucket_memory_used_decrease_threshold = self.input.param("bucket_memory_used_decrease_threshold", 0.3)
            node_memory_free_decrease_threshold = self.input.param("node_memory_free_decrease_threshold", 0.3)
            index_memory_used_decrease_threshold = self.input.param("index_memory_used_decrease_threshold", 0.3)

            increase_threshold_dict = {
                "bucket_disk_used": bucket_disk_used_increase_threshold,
                "bucket_item_count": bucket_item_count_increase_threshold,
                "node_cpu_utilization": node_cpu_utilization_increase_threshold,
                "index_total_disk_size": index_total_disk_size_increase_threshold,
                "bucket_memory_used": bucket_memory_used_increase_threshold,
                "node_memory_free": node_memory_free_increase_threshold,
                "index_memory_used": index_memory_used_increase_threshold
            }

            decrease_threshold_dict = {
                "bucket_disk_used": bucket_disk_used_decrease_threshold,
                "bucket_item_count": bucket_item_count_decrease_threshold,
                "node_cpu_utilization": node_cpu_utilization_decrease_threshold,
                "index_total_disk_size": index_total_disk_size_decrease_threshold,
                "bucket_memory_used": bucket_memory_used_decrease_threshold,
                "node_memory_free": node_memory_free_decrease_threshold,
                "index_memory_used": index_memory_used_decrease_threshold
            }

            cmp_dict = {}
            initial_version_title = "Initial_version_workload"

            for comparison_title in self.comparison_map:
                if comparison_title not in cmp_dict:
                    cmp_dict[comparison_title] = {}
                for workload in self.comparison_map[comparison_title]:

                    with open(self.comparison_map[comparison_title][workload], 'r') as file:
                        stats = json.load(file)

                    if workload not in cmp_dict:
                        cmp_dict[comparison_title][workload] = {}

                    for stat in stats:
                        series_dict = convert_to_series_dict(stats[stat])
                        if stat not in cmp_dict[comparison_title][workload]:
                            cmp_dict[comparison_title][workload][stat] = {}

                        for field in series_dict:
                            mean_series = mean(series_dict[field])
                            median_series = median(series_dict[field])
                            variance_series = variance(series_dict[field])
                            std_dev_series = std_dev(series_dict[field])
                            skewness_series = skewness(series_dict[field])
                            excess_kurtosis_series = excess_kurtosis(series_dict[field])
                            sample_size = len(series_dict[field])

                            if field not in cmp_dict[comparison_title][workload][stat]:
                                cmp_dict[comparison_title][workload][stat][field] = {
                                    "mean": mean_series,
                                    "median": median_series,
                                    "variance": variance_series,
                                    "std_dev": std_dev_series,
                                    "skewness": skewness_series,
                                    "excess_kurtosis": excess_kurtosis_series,
                                    "sample_size": sample_size
                                }

            for comparison_title in cmp_dict:
                for workload in cmp_dict[comparison_title]:
                    print_stats_table(cmp_dict, comparison_title, workload)

            for comparison_title in cmp_dict:
                if comparison_title == initial_version_title:
                    continue
                for workload in cmp_dict[comparison_title]:
                    for stat in cmp_dict[comparison_title][workload]:
                        for field in cmp_dict[comparison_title][workload][stat]:
                            mean_series = cmp_dict[comparison_title][workload][stat][field]["mean"]
                            median_series = cmp_dict[comparison_title][workload][stat][field]["median"]
                            variance_series = cmp_dict[comparison_title][workload][stat][field]["variance"]
                            std_dev_series = cmp_dict[comparison_title][workload][stat][field]["std_dev"]
                            skewness_series = cmp_dict[comparison_title][workload][stat][field]["skewness"]
                            excess_kurtosis_series = cmp_dict[comparison_title][workload][stat][field]["excess_kurtosis"]

                            res_mean = compare_metric(mean_series, cmp_dict[initial_version_title][workload][stat][field]["mean"], increase_threshold_dict[stat], decrease_threshold_dict[stat])
                            res_median = compare_metric(median_series, cmp_dict[initial_version_title][workload][stat][field]["median"], increase_threshold_dict[stat], decrease_threshold_dict[stat])
                            res_variance = compare_metric(variance_series, cmp_dict[initial_version_title][workload][stat][field]["variance"], increase_threshold_dict[stat], decrease_threshold_dict[stat])
                            res_std_dev = compare_metric(std_dev_series, cmp_dict[initial_version_title][workload][stat][field]["std_dev"], increase_threshold_dict[stat], decrease_threshold_dict[stat])
                            res_skewness = compare_metric(skewness_series, cmp_dict[initial_version_title][workload][stat][field]["skewness"], increase_threshold_dict[stat], decrease_threshold_dict[stat])
                            res_excess_kurtosis = compare_metric(excess_kurtosis_series, cmp_dict[initial_version_title][workload][stat][field]["excess_kurtosis"], increase_threshold_dict[stat], decrease_threshold_dict[stat])

                            self.assertTrue(res_mean, "Mean value for {} in {} for {} workload during {} is not within threshold".format(field, stat, workload, comparison_title))
                            self.assertTrue(res_median, "Median value for {} in {} for {} workload during {} is not within threshold".format(field, stat, workload, comparison_title))
                            self.assertTrue(res_variance, "Variance value for {} in {} for {} workload during {} is not within threshold".format(field, stat, workload, comparison_title))
                            self.assertTrue(res_std_dev, "Standard Deviation value for {} in {} for {} workload during {} is not within threshold".format(field, stat, workload, comparison_title))
                            self.assertTrue(res_skewness, "Skewness value for {} in {} for {} workload during {} is not within threshold".format(field, stat, workload, comparison_title))
                            self.assertTrue(res_excess_kurtosis, "Excess Kurtosis value for {} in {} for {} workload during {} is not within threshold".format(field, stat, workload, comparison_title))
        finally:
            create_stat_csv_all(self.comparison_map)

    def test_upgrade(self):
        '''
            1. Sets the cluster up with initial version
            2. Sets indexer params
            3. Creates a bucket and loads hotel dataset
            4. Creates indexes on the hotel dataset
            5. Run comparison workload
            9. Upgrade the cluster using any of the upgrade methods (Run query and CRUD workloads during the upgrade)
            10. In mixed mode after each node upgrade perform a comparison workload
            11. Post upgrade, run a comparison workload
            12. Rebalance out n nodes and rebalance in n nodes from each service post which run the comparison workload
            13. Rebalance in n nodes and rebalance out n nodes from each service post which run the comparison workload
            14. Swap rebalance n nodes for each service post which run the comparison workload
            15. Failover 1 node from each service and add back - delta recovery, full recovery post which run the comparison workload
            16. Update replicas of a bucket and rebalance. Change it back and rebalance post which run the comparison workload
            17. Kill memcached and once its back up run the comparison workload
            20. Kill indexer and once its back up run the comparison workload
            18. All the stats during workloads are captured. Compare them all and make sure they do not deviate much and are within the tresholds defined


            Workloads
            1. Comparison workload - Same workload run whenever invoked. Stats are captured during this workload. This comprises of
                i. A CRUD workload with certain read,update,create and delete percentages.
                ii. A Query workload
                iii. A CRUD workload with certain read,update,create and delete percentages running alongside a Query workload
                iv. An idle workload for 10 minutes

            2. Rebalance workload - Workload run during rebalances.
                i. Random percentages of reads, updates, creates and deletes are picked.
                ii. A Query workload is run alongside this workload

        '''

        try:
            # Set indexer settings
            self.PrintStep("Setting Indexer Params")
            self.set_indexer_params()

            # Create a hotel dataset, create indexes
            self.create_hotel_dataset_bucket()
            self.create_new_indexes_for_hotel_dataset()

            self.set_comparison_queries()

            self.PrintStep("Starting comparison workload for initial version")
            # Run a comparison workload before upgrade
            self.comparison_workload("Initial_version_workload")

            self.PrintStep("Starting Upgrades")
            # Start upgrades here
            for upgrade_version in self.upgrade_chain[1:]:
                self.initial_version = self.upgrade_version
                self.upgrade_version = upgrade_version

                ### Fetching the first node to upgrade ###
                node_to_upgrade = self.fetch_node_to_upgrade()
                itr = 0

                ### Each node in the cluster is upgraded iteratively ###
                while node_to_upgrade is not None:
                    itr += 1
                    ### The upgrade procedure starts ###
                    self.log.info("Selected node for upgrade: {}".format(node_to_upgrade.ip))

                    # Start profiling if needed
                    if self.index_profile_during_rebalances:
                        self.start_index_profiling()

                    # Start rebalance workload
                    self.rebalance_workload([node_to_upgrade])

                    ### Based on the type of upgrade, the upgrade function is called ###
                    if self.upgrade_type in ["failover_delta_recovery",
                                             "failover_full_recovery"]:
                        if "kv" not in node_to_upgrade.services.lower():
                            upgrade_type = "failover_full_recovery"
                            self.upgrade_function[upgrade_type](node_to_upgrade, graceful=False)
                        else:
                            self.upgrade_function[self.upgrade_type](node_to_upgrade)
                    elif self.upgrade_type == "full_offline":
                        self.upgrade_function[self.upgrade_type](self.cluster.nodes_in_cluster, self.upgrade_version)
                    else:
                        self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                             self.upgrade_version)

                    # Upgrade of node
                    self.cluster_util.print_cluster_stats(self.cluster)

                    # Wait for rebalance workload to complete
                    self.wait_for_rebalance_workload_completion()

                    # Stop profiling if needed
                    if self.index_profile_during_rebalances:
                        self.stop_index_profiling()

                    self.sleep(900, "Sleeping for 15 minutes before running the next workload") # Ref - MB-65965

                    if self.test_failure is not None:
                        self.fail("Test failed during upgrade")

                    ### Fetching the next node to upgrade ##
                    node_to_upgrade = self.fetch_node_to_upgrade()

                    if node_to_upgrade is not None:
                        self.PrintStep("Starting Mixed mode comparison workload")
                        self.comparison_workload("Mixed_mode_{}->{}_{}".format(self.initial_version, self.upgrade_version, itr))

                self.cluster.version = upgrade_version
                self.cluster_features = \
                    self.upgrade_helper.get_supported_features(self.cluster.version)

                self.PrintStep("Starting Post upgrade comparison workload")
                self.comparison_workload("Post_upgrade_{}".format(self.cluster.version))

            ### Printing cluster stats after the upgrade of the whole cluster ###
            self.cluster_util.print_cluster_stats(self.cluster)
            self.PrintStep("Upgrade of the whole cluster to {0} complete".format(
                                                            self.upgrade_version))

            # Adding _system scope and collections under it to the local bucket object since
            # these are added once the cluster is upgraded to 7.6
            # if float(self.upgrade_version[:3]) >= 7.6 and float(self.initial_version[:3]) < 7.6:
            #     self.add_system_scope_to_all_buckets()

            # Set indexer settings
            # self.PrintStep("Setting Indexer Params")
            # self.set_indexer_params()

            self.upgrade_helper.install_version_on_nodes(
                    self.spare_nodes, self.cluster.version)

            rebalance_tasks = [
                "rebalance_in_out",
                "rebalance_out_in",
                "swap_rebalance",
                "failover_add_back_delta_recovery",
                "failover_add_back_full_recovery",
                "replica_update_rebalance"
                ]

            if self.rebalance_tasks_post_upgrade is None:
                self.rebalance_tasks_post_upgrade = rebalance_tasks
            else:
                self.rebalance_tasks_post_upgrade = self.rebalance_tasks_post_upgrade.split("-")

            for task in self.rebalance_tasks_post_upgrade:
                if task not in rebalance_tasks:
                    self.fail("Invalid rebalance task {}".format(task))

                self.PrintStep("Starting task {}".format(task))

                # Start profiling if needed
                if self.index_profile_during_rebalances:
                    self.start_index_profiling()

                self.rebalance_post_upgrade(task)

                # Stop profiling if needed
                if self.index_profile_during_rebalances:
                    self.stop_index_profiling()

                self.sleep(180, "Sleeping after rebalance succeeds before running comparison workload")
                self.PrintStep("Starting comparison workload post {}".format(task))
                self.comparison_workload("Post_upgrade_{}_Post_{}".format(self.cluster.version, task))

            # Kill memcached on a random kv node
            self.PrintStep("Killing memcached")
            shell = RemoteMachineShellConnection(random.choice(self.cluster.kv_nodes))
            shell.kill_memcached()
            shell.disconnect()

            for node in self.cluster.kv_nodes:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0], servers=[node],
                    wait_time=600))

            self.sleep(180, "Sleeping after memcached restart before running comparison workload")
            self.PrintStep("Starting comparison workload post memcached kill")
            self.comparison_workload("Post_upgrade_{}_Post_memcached_kill".format(self.cluster.version))

            # Kill indexer on a random index node
            self.PrintStep("Killing indexer")
            shell = RemoteMachineShellConnection(random.choice(self.cluster.index_nodes))
            shell.kill_indexer()
            shell.disconnect()

            self.sleep(300, "Sleeping after indexer restart before running comparison workload")
            self.PrintStep("Starting comparison workload post indexer kill")
            self.comparison_workload("Post_upgrade_{}_Post_indexer_kill".format(self.cluster.version))

        finally:
            ### All the stats during workloads are captured. Compare them all and make sure they do not deviate much and are within the tresholds defined
            self.compare_stats()
