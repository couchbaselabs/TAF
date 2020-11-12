from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
import random
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import doc_generator
from table_view import TableView
from sdk_exceptions import SDKException
import threading
import time
import re
import json

class volume(BaseTestCase):  # will add the __init__ functions after the test has been stabilised
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        BaseTestCase.setUp(self)
        self.rest = RestConnection(self.servers[0])
        self.rest.update_autofailover_settings(True, 5)
        self.op_type = self.input.param("op_type", "create")
        self.tasks = []  # To have all tasks running in parallel.
        self._iter_count = 0  # To keep a check of how many items are deleted
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.num_buckets = self.input.param("num_buckets", 4)
        self.mutate = 0
        self.doc_ops = self.input.param("doc_ops", "create")
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')
        self.iterations = self.input.param("iterations", 6)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.new_num_writer_threads = self.input.param("new_num_writer_threads", 6)
        self.new_num_reader_threads = self.input.param("new_num_reader_threads", 8)
        # bucket params
        self.bucket_names = self.input.param("bucket_names", "bucket1;bucket2;bucket3;bucket4")
        self.duration = self.input.param("bucket_expiry", 0)
        self.eviction_policy = self.input.param("eviction_policy", Bucket.EvictionPolicy.VALUE_ONLY)
        self.bucket_type = self.input.param("bucket_type", Bucket.Type.MEMBASE)  # Bucket.bucket_type.EPHEMERAL
        self.compression_mode = self.input.param("compression_mode",
                                                 Bucket.CompressionMode.PASSIVE)  # Bucket.bucket_compression_mode.ACTIVE
        # parameters to speed up the election of a new master. See MB-41562 for more details
        self.heartbeat_interval = self.input.param("heartbeat_interval", 500)
        self.timeout_interval_count = self.input.param("timeout_interval_count", 3)
        # parameters to speed up the auto-failover initiation. See MB-41562 for more details
        self.lease_time = self.input.param("lease_time", 5000)
        self.lease_grace_time = self.input.param("lease_grace_time", 2000)
        self.lease_renew_after = self.input.param("lease_renew_after", 500)
        self.skip_data_validation = self.input.param("skip_data_validation", True)
        self.services_init = self.input.param("services_init", "kv-kv-kv-kv-kv-index-n1ql")
        self.services = self.cluster_util.get_services(self.cluster.servers, self.services_init)
        self.query_thread = None

    def tearDown(self):
        for server in self.servers:
            remote = RemoteMachineShellConnection(server)
            self.log.info("Starting couchbase server on : {0}".format(server.ip))
            remote.start_couchbase()
            remote.disconnect()
        if self.query_thread:
            # stop query thread before tearDown if its still running
            self.query_thread_flag = False
            self.query_thread.join()
            self.query_thread = None
        # Do not call the base class's teardown, as we want to keep the cluster intact after the volume run
        self.log.info("Printing bucket stats before teardown")
        self.bucket_util.print_bucket_stats()
        server_with_crashes = self.check_coredump_exist(self.servers)
        self.assertEqual(len(server_with_crashes), 0,
                         msg="Test failed, Coredump found on servers {}"
                         .format(server_with_crashes))

    def create_required_buckets(self):
        self.log.info("Get the available memory quota")
        self.info = self.rest.get_nodes_self()
        threshold_memory = 100
        # threshold_memory_vagrant = 100
        total_memory_in_mb = self.info.mcdMemoryReserved
        total_available_memory_in_mb = total_memory_in_mb
        active_service = self.info.services

        # If the mentioned service is already present, we remove that much memory from available memory quota
        if "index" in active_service:
            total_available_memory_in_mb -= self.info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= self.info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= self.info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= self.info.eventingMemoryQuota

        available_memory = total_available_memory_in_mb - threshold_memory
        # available_memory =  total_available_memory_in_mb - threshold_memory_vagrant
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=available_memory)

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        ramQuota = self.input.param("ramQuota", available_memory)
        if self.bucket_names:
            bucket_names = self.bucket_names.split(';')
        if self.num_buckets == 1:
            bucket = Bucket({"name": "GleamBookUsers", "ramQuotaMB": ramQuota, "maxTTL": self.duration,
                             "replicaNumber": self.num_replicas,
                             "evictionPolicy": self.eviction_policy, "bucketType": self.bucket_type,
                             "compressionMode": self.compression_mode})
            self.bucket_util.create_bucket(bucket)
        elif self.num_buckets == len(bucket_names):
            for i in range(self.num_buckets):
                bucket = Bucket(
                    {"name": bucket_names[i], "ramQuotaMB": ramQuota / self.num_buckets, "maxTTL": self.duration,
                     "replicaNumber": self.num_replicas,
                     "evictionPolicy": self.eviction_policy, "bucketType": self.bucket_type,
                     "compressionMode": self.compression_mode})
                self.bucket_util.create_bucket(bucket)
        else:
            self.fail("Number of bucket/Names not sufficient")

        # rebalance the new buckets across all nodes.
        self.log.info("Rebalance Starts")
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[])
        self.rest.monitorRebalance()
        return bucket

    def set_num_writer_and_reader_threads(self, num_writer_threads="default", num_reader_threads="default"):
        for node in self.cluster_util.get_kv_nodes():
            bucket_helper = BucketHelper(node)
            bucket_helper.update_memcached_settings(num_writer_threads=num_writer_threads,
                                                    num_reader_threads=num_reader_threads)

    def initial_data_load(self, initial_load):
        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                                           initial_load, "create", exp=0,
                                                           batch_size=10,
                                                           process_concurrency=self.process_concurrency,
                                                           replicate_to=self.replicate_to,
                                                           persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                                           retries=self.sdk_retries, update_count=self.mutate,
                                                           transaction_timeout=self.transaction_timeout,
                                                           commit=self.transaction_commit,
                                                           durability=self.durability_level, sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
        else:
            tasks_info = self.bucket_util._async_load_all_buckets(self.cluster, initial_load,
                                                                  "create", exp=0,
                                                                  persist_to=self.persist_to,
                                                                  replicate_to=self.replicate_to,
                                                                  batch_size=10,
                                                                  pause_secs=5,
                                                                  timeout_secs=30,
                                                                  durability=self.durability_level,
                                                                  process_concurrency=self.process_concurrency,
                                                                  retries=self.sdk_retries)

            for task, task_info in tasks_info.items():
                self.task_manager.get_task_result(task)
        self.sleep(10)

    # Loading documents in 2 buckets in parallel through transactions
    def doc_load_using_txns(self):
        if "update" in self.doc_ops and self.gen_update_users is not None:
            self.tasks.append(self.doc_loader_txn("update", self.gen_update_users))
        if "create" in self.doc_ops and self.gen_create_users is not None:
            self.tasks.append(self.doc_loader_txn("create", self.gen_create_users))
        if "delete" in self.doc_ops and self.gen_delete_users is not None:
            self.tasks.append(self.doc_loader_txn("delete", self.gen_delete_users))
        self.sleep(20)
        for task in self.tasks:
            self.task.jython_task_manager.get_task_result(task)

    def doc_loader_txn(self, op_type, kv_gen):
        if op_type == "update":
            print("Value of Mutated is", self.mutate)
            self.sleep(5)
        process_concurrency = self.process_concurrency
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                                       kv_gen, op_type, exp=0,
                                                       batch_size=10,
                                                       process_concurrency=process_concurrency,
                                                       replicate_to=self.replicate_to,
                                                       persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                                       retries=self.sdk_retries, update_count=self.mutate,
                                                       transaction_timeout=self.transaction_timeout,
                                                       commit=self.transaction_commit, durability=self.durability_level,
                                                       sync=self.sync, defer=self.defer)
        return task

    # Loading documents through normal doc loader
    def normal_doc_loader(self):
        tasks_info = dict()
        if "update" in self.doc_ops and self.gen_update_users is not None:
            task_info = self.doc_loader("update", self.gen_update_users)
            tasks_info.update(task_info.items())
        if "create" in self.doc_ops and self.gen_create_users is not None:
            task_info = self.doc_loader("create", self.gen_create_users)
            tasks_info.update(task_info.items())
        if "delete" in self.doc_ops and self.gen_delete_users is not None:
            task_info = self.doc_loader("delete", self.gen_delete_users)
            tasks_info.update(task_info.items())
        return tasks_info

    def doc_loader(self, op_type, kv_gen):
        process_concurrency = self.process_concurrency
        if op_type == "update":
            if "create" not in self.doc_ops:
                self.create_perc = 0
            if "delete" not in self.doc_ops:
                self.delete_perc = 0
            process_concurrency = (self.update_perc * process_concurrency) / (
                    self.create_perc + self.delete_perc + self.update_perc)
        if op_type == "create":
            if "update" not in self.doc_ops:
                self.update_perc = 0
            if "delete" not in self.doc_ops:
                self.delete_perc = 0
            process_concurrency = (self.create_perc * process_concurrency) / (
                    self.create_perc + self.delete_perc + self.update_perc)
        if op_type == "delete":
            if "create" not in self.doc_ops:
                self.create_perc = 0
            if "update" not in self.doc_ops:
                self.update_perc = 0
            process_concurrency = (self.delete_perc * process_concurrency) / (
                    self.create_perc + self.delete_perc + self.update_perc)
        retry_exceptions = [
            SDKException.AmbiguousTimeoutException,
            SDKException.RequestCanceledException,
            SDKException.DurabilityAmbiguousException,
            SDKException.DurabilityImpossibleException,
        ]
        tasks_info = self.bucket_util._async_load_all_buckets(self.cluster, kv_gen,
                                                              op_type, 0, batch_size=20,
                                                              persist_to=self.persist_to,
                                                              replicate_to=self.replicate_to,
                                                              durability=self.durability_level, pause_secs=5,
                                                              timeout_secs=30, process_concurrency=process_concurrency,
                                                              retries=self.sdk_retries,
                                                              retry_exceptions=retry_exceptions)
        return tasks_info

    # killing baby sitter
    def kill_babysittter_process(self, target_node):
        node = self.get_server_info(target_node)
        remote = RemoteMachineShellConnection(node)
        self.log.info("Killing babysitter on : {0}".format(node.ip))
        remote.kill_babysitter()
        remote.disconnect()
        self.sleep(2)

    def start_couchbase_server(self, target_node):
        node = self.get_server_info(target_node)
        remote = RemoteMachineShellConnection(node)
        self.log.info("Starting couchbase server on : {0}".format(node.ip))
        remote.start_couchbase()
        remote.disconnect()
        self.sleep(600)

    def rebalance(self, nodes_in=0, nodes_out=0):
        servs_in = random.sample(self.available_servers, nodes_in)

        self.nodes_cluster = self.cluster_util.get_kv_nodes()
        self.nodes_cluster.remove(self.cluster.master)
        servs_out = random.sample(self.nodes_cluster, nodes_out)

        if nodes_in == nodes_out:
            self.vbucket_check = False

        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, servs_out, check_vbucket_shuffling=self.vbucket_check)

        self.available_servers = [servs for servs in self.available_servers if servs not in servs_in]
        self.available_servers += servs_out

        self.cluster.nodes_in_cluster.extend(servs_in)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        return rebalance_task

    def rebalance_validation(self, tasks_info, rebalance_task):
        if not rebalance_task.result:
            for task, _ in tasks_info.items():
                self.task.jython_task_manager.get_task_result(task)
            self.fail("Rebalance Failed")

    def data_validation(self, tasks_info):
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

            self.sleep(10)

            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.log.info("Validating Active/Replica Docs")
        if self.atomicity:
            self.check_replica = False
        else:
            self.check_replica = True

        for bucket in self.bucket_util.buckets:
            tasks = list()
            if self.gen_update_users is not None:
                tasks.append(self.task.async_validate_docs(self.cluster, bucket, self.gen_update_users, "update", 0,
                                                           batch_size=10, check_replica=self.check_replica))
            if self.gen_create_users is not None:
                tasks.append(self.task.async_validate_docs(self.cluster, bucket, self.gen_create_users, "create", 0,
                                                           batch_size=10, check_replica=self.check_replica))
            if self.gen_delete_users is not None:
                tasks.append(self.task.async_validate_docs(self.cluster, bucket, self.gen_delete_users, "delete", 0,
                                                           batch_size=10, check_replica=self.check_replica))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            self.sleep(20)

        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(
                self.end - self.initial_load_count * self.delete_perc / 100 * self._iter_count)

    def data_load(self):
        tasks_info = dict()
        if self.atomicity:
            self.doc_load_using_txns()
            self.sleep(10)
        else:
            tasks_info = self.normal_doc_loader()
            self.sleep(10)
        return tasks_info

    def generate_docs(self):
        self.create_perc = self.input.param("create_perc", 100)
        self.update_perc = self.input.param("update_perc", 10)
        self.delete_perc = self.input.param("delete_perc", 10)

        self.gen_delete_users = None
        self.gen_create_users = None
        self.gen_update_users = None

        if "update" in self.doc_ops:
            self.mutate += 1
            self.gen_update_users = doc_generator("Users", 0, self.initial_load_count * self.update_perc / 100,
                                                  doc_size=self.doc_size, mutate=self.mutate)
        if "delete" in self.doc_ops:
            self.gen_delete_users = doc_generator("Users", self.start,
                                                  self.start + (self.initial_load_count * self.delete_perc) / 100,
                                                  doc_size=self.doc_size)
            self._iter_count += 1

        if "create" in self.doc_ops:
            self.start = self.end
            self.end += self.initial_load_count * self.create_perc / 100
            self.gen_create_users = doc_generator("Users", self.start, self.end, doc_size=self.doc_size)

    def data_validation_mode(self, tasks_info):
        # if not self.atomicity:
        self.data_validation(tasks_info)
        '''
        else:
            for task in self.tasks:
                self.task.jython_task_manager.get_task_result(task)
            self.sleep(10)
        '''

    def get_bucket_dgm(self, bucket):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Active Resident Threshold of {0} is {1}".format(bucket.name, dgm))

    def print_crud_stats(self):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Initial Items", "Current Items", "Items Updated", "Items Created", "Items Deleted"])
        if self._iter_count != 0:
            self.table.add_row(
                [str(self.start - self.initial_load_count * self.delete_perc / 100 * (self._iter_count - 1)),
                 str(self.end - self.initial_load_count * self.delete_perc / 100 * self._iter_count),
                 str(self.update_perc - self.update_perc) + "---" +
                 str(self.initial_load_count * self.update_perc / 100),
                 str(self.start) + "---" + str(self.end),
                 str(self.start - self.initial_load_count * self.create_perc / 100) + "---" +
                 str(self.start + (
                         self.initial_load_count * self.delete_perc / 100) - self.initial_load_count * self.create_perc / 100)])
        self.table.display("Docs statistics")

    def set_non_default_orchestrator_heartbeats_and_timeouts(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        self.rest_client = RestConnection(self.cluster.master)
        if self.heartbeat_interval:
            self.rest_client.set_heartbeat_interval(self.heartbeat_interval)
            status, content = self.rest_client.get_heartbeat_interval()
            self.log.info("get_heartbeat_interval : {0}".format(content))
        if self.timeout_interval_count:
            self.rest_client.set_timeout_interval_count(self.timeout_interval_count)
            status, content = self.rest_client.get_timeout_interval_count()
            self.log.info("get_timeout_interval_count : {0}".format(content))
        if self.lease_time:
            self.rest_client.set_lease_time(self.lease_time)
            status, content = self.rest_client.get_lease_time()
            self.log.info("get_lease_time : {0}".format(content))
        if self.lease_grace_time:
            self.rest_client.set_lease_grace_time(self.lease_grace_time)
            status, content = self.rest_client.get_lease_grace_time()
            self.log.info("get_lease_grace_time : {0}".format(content))
        if self.lease_renew_after:
            self.rest_client.set_lease_renew_after(self.lease_renew_after)
            status, content = self.rest_client.get_lease_renew_after()
            self.log.info("get_lease_renew_after : {0}".format(content))

    def get_node(self, orchestrator=True):
        output = self.rest.get_terse_cluster_info()
        orchestrator_node = output["orchestrator"]
        self.log.info("Orchestrator : {0}".format(orchestrator_node))
        nodes = self.cluster_util.get_nodes(self.cluster.master)
        if orchestrator:
            for node in nodes:
                if orchestrator_node == node.id:
                    self.log.info("Returned Node : {0}".format(orchestrator_node))
                    return node
        else:
            for node in nodes:
                if orchestrator != node.id:
                    self.log.info("Returned Node : {0}".format(node))
                    return node

    def get_server_info(self, node):
        for server in self.servers:
            if server.ip == node.ip:
                return server

    def get_other_server_info(self, node):
        for server in self.cluster_util.get_kv_nodes():
            if server.ip != node.ip:
                return server

    def run_cbq_query(self, query):
        result = self.n1ql_rest.query_tool(query)
        return result

    def create_primary_indexes(self):
        bucket_names = self.bucket_names.split(';')
        for bucket_name in bucket_names:
            primary_index_query = "CREATE PRIMARY INDEX " \
                                  "on `%s` " \
                                  "USING GSI" \
                                  % (bucket_name)
            result = self.run_cbq_query(primary_index_query)
            self.log.info("create primary_index : {0}".format(result))
            self.assertTrue(result['status'] == "success", "Query %s failed." % primary_index_query)
            time.sleep(5)

    def run_select_query(self):
        bucket_names = self.bucket_names.split(';')
        while self.query_thread_flag:
            for bucket_name in bucket_names:
                select_query = "select * from `%s` where age > 1 limit 1" % (bucket_name)
                result = self.run_cbq_query(select_query)
                self.assertTrue(result['status'] == "success", "Query %s failed." % select_query)
                time.sleep(2)

    def get_failover_count(self, node):
        rest = RestConnection(node)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def set_rebalance_moves_per_node(self, rebalanceMovesPerNode=4):
        # build the body
        body = dict()
        body["rebalanceMovesPerNode"] = rebalanceMovesPerNode
        rest = RestConnection(self.cluster.master)
        rest.set_rebalance_settings(body)
        result = rest.get_rebalance_settings()
        self.log.info("Changed Rebalance settings: {0}".format(json.loads(result)))

    def rebalance_in(self, bucket):
        self.generate_docs()
        self.gen_delete_users = None
        self._iter_count = 0
        if not self.atomicity:
            self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                   num_reader_threads="disk_io_optimized")
        rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
        tasks_info = self.data_load()
        if not self.atomicity:
            self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                   num_reader_threads=self.new_num_reader_threads)
        self.task.jython_task_manager.get_task_result(rebalance_task)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        if not self.skip_data_validation:
            self.data_validation_mode(tasks_info)
        self.tasks = []
        self.bucket_util.print_bucket_stats()
        self.print_crud_stats()
        self.get_bucket_dgm(bucket)

    def kill_babysitter_orchestrator_hard_failover_recovery_and_rebalance(self, bucket):
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        std = self.std_vbucket_dist or 1.0
        if not self.skip_data_validation:
            prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.nodes_in_cluster,
                                                                      self.bucket_util.buckets)
            prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.nodes_in_cluster,
                                                                     self.bucket_util.buckets)
            self.sleep(10)
            disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(

                self.cluster.nodes_in_cluster, self.bucket_util.buckets, path=None)

        orchestrator = self.get_node(orchestrator=True)
        other_server = self.get_other_server_info(orchestrator)
        other_server_conn = RestConnection(other_server)
        self.log.info("Chosen orchestrator: {0}".format(orchestrator.id))

        self.generate_docs()
        tasks_info = self.data_load()
        # kill babysitter process on the orchestrator
        self.kill_babysittter_process(orchestrator)
        # keep trying hard failing over the orchestrator until it happens
        num_failed_over_nodes = 0
        for i in range(100):
            # Mark Node for failover
            try:
                other_server_conn.fail_over(orchestrator.id, graceful=False)
            except Exception:
                pass
            num_failed_over_nodes = self.get_failover_count(other_server)
            self.log.info("Number of failed over nodes : {0}".format(num_failed_over_nodes))
            if num_failed_over_nodes:
                break
        # restart couchbase-server
        self.start_couchbase_server(orchestrator)
        try:
            for bucket in self.bucket_util.buckets:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster_util.get_kv_nodes(), bucket))
        except Exception:
            self.start_couchbase_server(orchestrator)
        else:
            pass
        self.generate_docs()
        tasks_info1 = self.data_load()
        if num_failed_over_nodes:
            self.log.info("Doing a delta recovery")
            self.rest.set_recovery_type(otpNode=orchestrator.id, recoveryType="delta")
        else:
            self.fail("failover was not successful, hence could not recover")
        self.sleep(300)
        if not self.atomicity:
            self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                   num_reader_threads=self.new_num_reader_threads)

        if not self.atomicity:
            self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                   num_reader_threads="disk_io_optimized")
        try:
            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        except Exception as e:
            self.log.info("Inside exception, rebalance failed with : {0}".format(str(e)))
            self.sleep(300)
            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(10)
        if not self.skip_data_validation:
            self.data_validation_mode(tasks_info)
            self.data_validation_mode(tasks_info1)
            self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.nodes_in_cluster,
                                                    self.bucket_util.buckets)
            self.sleep(10)
            self.bucket_util.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.cluster.servers[:self.nodes_in + self.nodes_init],
                self.bucket_util.buckets, path=None)
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)

        if not self.skip_data_validation:
            self.bucket_util.vb_distribution_analysis(
                servers=nodes, buckets=self.bucket_util.buckets,
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster_util.vbuckets)
        self.bucket_util.print_bucket_stats()
        self.print_crud_stats()
        self.get_bucket_dgm(bucket)

    def rebalance_out(self, bucket):
        self.generate_docs()
        if not self.atomicity:
            self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                   num_reader_threads="disk_io_optimized")
        rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)
        tasks_info = self.data_load()
        if not self.atomicity:
            self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                   num_reader_threads=self.new_num_reader_threads)
        self.task.jython_task_manager.get_task_result(rebalance_task)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        if not self.skip_data_validation:
            self.data_validation_mode(tasks_info)
        self.tasks = []
        self.bucket_util.print_bucket_stats()
        self.print_crud_stats()
        self.get_bucket_dgm(bucket)

    def test_volume_MB_41562(self):
        ########################################################################################################################
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
        self.log.info("Display process IDs of all mb_master processes before ns_serv restart")
        status, content = self.rest.get_process_ids_of_all_mb_master_processes()
        self.log.info("Status : {0} Content : {1}".format(status, content))
        self.log.info("Step 1: Set Non default orchestrator heartbeats and timeouts")
        self.set_non_default_orchestrator_heartbeats_and_timeouts()
        try:
            self.rest.restart_ns_server()
        except Exception as e:
            self.log.info("Exception : {0}".format(str(e)))
            pass
        self.sleep(180)
        self.rest = RestConnection(self.servers[0])
        self.log.info("Display process IDs of all mb_master processes after ns_serv restart")
        status, content = self.rest.get_process_ids_of_all_mb_master_processes()
        self.log.info("Status : {0} Content : {1}".format(status, content))
        # default is 4, we are changing this to increase the rebalance run time which is crucial for this test
        self.set_rebalance_moves_per_node(rebalanceMovesPerNode=1)
        ########################################################################################################################
        self.log.info("Step 2: Create a n node cluster")
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master],
                            nodes_init,
                            [], services=self.services)
        self.log.info("Display process IDs of all mb_master processes after rebalancing all the nodes in the cluster")
        status, content = self.rest.get_process_ids_of_all_mb_master_processes()
        self.log.info("Status : {0} Content : {1}".format(status, content))
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.index_node = self.cluster_util.get_nodes_from_services_map(service_type="index",
                                                                        get_all_nodes=False,
                                                                        servers=self.cluster.servers[:self.nodes_init],
                                                                        master=self.cluster.master)
        self.n1ql_node = self.cluster_util.get_nodes_from_services_map(service_type="n1ql",
                                                                       get_all_nodes=False,
                                                                       servers=self.cluster.servers[:self.nodes_init],
                                                                       master=self.cluster.master)
        self.n1ql_rest = RestConnection(self.n1ql_node)
        ########################################################################################################################
        self.log.info("Step 3: Create required buckets and indexes.")
        bucket = self.create_required_buckets()
        self.create_primary_indexes()
        self.sleep(60)
        self.query_thread_flag = True
        self.query_thread = threading.Thread(target=self.run_select_query)
        self.query_thread.start()
        self.loop = 0
        #######################################################################################################################
        while self.loop < self.iterations:
            self.log.info("Step 4: Do initial Loading of docs")
            self.start = 0
            self.bucket_util.add_rbac_user()
            self.end = self.initial_load_count = self.input.param("initial_load", 100000)
            initial_load = doc_generator("Users", self.start, self.start + self.initial_load_count,
                                         doc_size=self.doc_size)
            self.initial_data_load(initial_load)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 5: Rebalance in with Loading of docs")
            self.rebalance_in(bucket)
            ########################################################################################################################
            self.log.info("Step 6: Failover a node and DeltaRecovery that node with loading in parallel")
            for i in range(5):
                self.kill_babysitter_orchestrator_hard_failover_recovery_and_rebalance(bucket)
            #########################################################################################################################
            self.log.info("Step 7: Rebalance Out with Loading of docs")
            self.rebalance_out(bucket)
            ########################################################################################################################
            self.log.info("Step 8: Flush the bucket and start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                self.bucket_util.flush_all_buckets(self.cluster.master)
                self.sleep(300)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                    self.nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(self.nodes_cluster,
                                              int(len(self.cluster.nodes_in_cluster) - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out)
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
                    reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                    self.get_bucket_dgm(bucket)
                self._iter_count = 0
            else:
                self.log.info("Volume Test Run Complete")
                self.get_bucket_dgm(bucket)
        ############################################################################################################################
        self.query_thread_flag = False
        self.query_thread.join()
        self.query_thread = None