from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from Cb_constants import CbServer
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
import random
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from error_simulation.cb_error import CouchbaseError
from couchbase_helper.documentgenerator import doc_generator
from table_view import TableView
from sdk_exceptions import SDKException


class volume(BaseTestCase):
    # will add the __init__ functions after the test has been stabilised
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        BaseTestCase.setUp(self)
        self.rest = RestConnection(self.servers[0])
        self.op_type = self.input.param("op_type", "create")
        self.tasks = []         # To have all tasks running in parallel.
        self._iter_count = 0    # To keep a check of how many items are deleted
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')
        self.iterations = self.input.param("iterations", 2)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.new_num_writer_threads = self.input.param(
            "new_num_writer_threads", 6)
        self.new_num_reader_threads = self.input.param(
            "new_num_reader_threads", 8)

    def create_required_buckets(self):
        self.log.info("Get the available memory quota")
        self.info = self.rest.get_nodes_self()
        threshold_memory = 100
        # threshold_memory_vagrant = 100
        total_memory_in_mb = self.info.mcdMemoryReserved
        total_available_memory_in_mb = total_memory_in_mb
        active_service = self.info.services

        # If the mentioned service is already present,
        # we remove that much memory from available memory quota
        if "index" in active_service:
            total_available_memory_in_mb -= self.info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= self.info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= self.info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= self.info.eventingMemoryQuota

        available_memory = total_available_memory_in_mb - threshold_memory
        self.rest.set_service_mem_quota(
            {CbServer.Settings.KV_MEM_QUOTA: available_memory})

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        duration = self.input.param("bucket_expiry", 0)
        eviction_policy = self.input.param("eviction_policy", Bucket.EvictionPolicy.VALUE_ONLY)
        self.bucket_type = self.input.param("bucket_type", Bucket.Type.MEMBASE) # Bucket.bucket_type.EPHEMERAL
        compression_mode = self.input.param("compression_mode", Bucket.CompressionMode.PASSIVE)  # Bucket.bucket_compression_mode.ACTIVE
        ramQuota = self.input.param("ramQuota", available_memory)
        bucket_names = self.input.param("bucket_names", "GleamBookUsers")
        if bucket_names:
            bucket_names = bucket_names.split(';')
        if self.bucket_type:
            self.bucket_type = self.bucket_type.split(';')
        if compression_mode:
            compression_mode = compression_mode.split(';')
        if eviction_policy:
            eviction_policy = eviction_policy.split(';')
        if self.num_buckets == 1:
            bucket = Bucket({"name": "GleamBookUsers", "ramQuotaMB": ramQuota, "maxTTL": duration, "replicaNumber":self.num_replicas,
                            "evictionPolicy": eviction_policy[0], "bucketType":self.bucket_type[0], "compressionMode":compression_mode[0]})
            self.bucket_util.create_bucket(bucket)
        elif 1 < self.num_buckets == len(bucket_names):
            for i in range(self.num_buckets):
                bucket = Bucket({"name": bucket_names[i], "ramQuotaMB": ramQuota/self.num_buckets, "maxTTL": duration, "replicaNumber":self.num_replicas,
                             "evictionPolicy": eviction_policy[i], "bucketType":self.bucket_type[i], "compressionMode":compression_mode[i]})
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

    def volume_doc_generator_users(self, key, start, end):
        template = '{{ "id":"{0}", "alias":"{1}", "name":"{2}", "user_since":"{3}", "employment":{4} }}'
        return GleamBookUsersDocumentGenerator(key, template,
                                               start=start, end=end)

    def volume_doc_generator_messages(self, key, start, end):
        template = '{{ "message_id": "{0}", "author_id": "{1}", "send_time": "{2}" }}'
        return GleamBookMessagesDocumentGenerator(key, template,
                                                  start=start, end=end)

    def initial_data_load(self, initial_load):
        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                                            initial_load, "create" , exp=0,
                                                            batch_size=10,
                                                            process_concurrency=self.process_concurrency,
                                                            replicate_to=self.replicate_to,
                                                            persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                                            retries=self.sdk_retries,update_count=self.mutate, transaction_timeout=self.transaction_timeout,
                                                            commit=self.transaction_commit,durability=self.durability_level,sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
        else:
            tasks_info = self.bucket_util._async_load_all_buckets(self.cluster, initial_load,
                                                            "create", exp=0,
                                                            persist_to = self.persist_to,
                                                            replicate_to=self.replicate_to,
                                                            batch_size= 10,
                                                            timeout_secs=30,
                                                            durability=self.durability_level,
                                                            process_concurrency = self.process_concurrency,
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
        if "delete" in self.doc_ops and self.gen_delete_users is not  None:
            self.tasks.append(self.doc_loader_txn("delete", self.gen_delete_users))
        self.sleep(20)
        for task in self.tasks:
            self.task.jython_task_manager.get_task_result(task)

    def doc_loader_txn(self, op_type, kv_gen):
        if op_type == "update":
            print("Value of Mutated is", self.mutate)
            self.sleep(5)
        process_concurrency = self.process_concurrency
        # if op_type == "update":
        #     if "create" not in self.doc_ops:
        #         self.create_perc = 0
        #     if "delete" not in self.doc_ops:
        #         self.delete_perc = 0
        #     process_concurrency = (self.update_perc*process_concurrency)/(self.create_perc + self.delete_perc + self.update_perc)
        # if op_type == "create":
        #     if "update" not in self.doc_ops:
        #         self.update_perc = 0
        #     if "delete" not in self.doc_ops:
        #         self.delete_perc = 0
        #     process_concurrency = (self.create_perc*process_concurrency)/(self.create_perc + self.delete_perc + self.update_perc)
        # if op_type == "delete":
        #     if "create" not in self.doc_ops:
        #         self.create_perc = 0
        #     if "update" not in self.doc_ops:
        #         self.update_perc = 0
        #     process_concurrency = (self.delete_perc*process_concurrency)/(self.create_perc + self.delete_perc + self.update_perc)
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
            process_concurrency = (self.update_perc*process_concurrency)/(self.create_perc + self.delete_perc + self.update_perc)
        if op_type == "create":
            if "update" not in self.doc_ops:
                self.update_perc = 0
            if "delete" not in self.doc_ops:
                self.delete_perc = 0
            process_concurrency = (self.create_perc*process_concurrency)/(self.create_perc + self.delete_perc + self.update_perc)
        if op_type == "delete":
            if "create" not in self.doc_ops:
                self.create_perc = 0
            if "update" not in self.doc_ops:
                self.update_perc = 0
            process_concurrency = (self.delete_perc*process_concurrency)/(self.create_perc + self.delete_perc + self.update_perc)
        retry_exceptions = [
            SDKException.AmbiguousTimeoutException,
            SDKException.RequestCanceledException,
            SDKException.DurabilityAmbiguousException,
            SDKException.DurabilityImpossibleException,
        ]
        tasks_info = self.bucket_util._async_load_all_buckets(self.cluster, kv_gen,
                                                              op_type, 0, batch_size=20,
                                                              persist_to=self.persist_to, replicate_to=self.replicate_to,
                                                              durability=self.durability_level,
                                                              timeout_secs=30, process_concurrency=process_concurrency,
                                                              retries=self.sdk_retries,
                                                              retry_exceptions=retry_exceptions)
        return tasks_info

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
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
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
                tasks.append(self.task.async_validate_docs(
                    self.cluster, bucket, self.gen_update_users, "update", 0,
                    batch_size=10, check_replica=self.check_replica))
            if self.gen_create_users is not None:
                tasks.append(self.task.async_validate_docs(
                    self.cluster, bucket, self.gen_create_users, "create", 0,
                    batch_size=10, check_replica=self.check_replica))
            if self.gen_delete_users is not  None:
                tasks.append(self.task.async_validate_docs(
                    self.cluster, bucket, self.gen_delete_users, "delete", 0,
                    batch_size=10, check_replica=self.check_replica))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            self.sleep(20)

        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.end - self.initial_load_count*self.delete_perc/100*self._iter_count)

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
        self.create_perc = self.input.param("create_perc",100)
        self.update_perc = self.input.param("update_perc", 10)
        self.delete_perc = self.input.param("delete_perc", 10)

        self.gen_delete_users = None
        self.gen_create_users = None
        self.gen_update_users = None

        if "update" in self.doc_ops:
            self.mutate += 1
            self.gen_update_users = doc_generator("Users", 0, self.initial_load_count*self.update_perc/100,
                                                doc_size = self.doc_size, mutate = self.mutate)
        if "delete" in self.doc_ops:
            self.gen_delete_users = doc_generator("Users", self.start,
                                              self.start + (self.initial_load_count*self.delete_perc)/100, doc_size = self.doc_size)
            self._iter_count += 1

        if "create" in self.doc_ops:
            self.start = self.end
            self.end += self.initial_load_count*self.create_perc/100
            self.gen_create_users = doc_generator("Users", self.start, self.end, doc_size = self.doc_size)

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
            self.table.add_row([str(self.start - self.initial_load_count*self.delete_perc/100*(self._iter_count-1)),
                                str(self.end- self.initial_load_count*self.delete_perc/100*self._iter_count),
                                str(self.update_perc - self.update_perc) + "---" +
                                str(self.initial_load_count*self.update_perc/100),
                                str(self.start) + "---" + str(self.end),
                                str(self.start - self.initial_load_count*self.create_perc/100) + "---" +
                                str(self.start + (self.initial_load_count*self.delete_perc/100) - self.initial_load_count*self.create_perc/100)])
        self.table.display("Docs statistics")

    def test_volume_taf(self):
        ########################################################################################################################
        self.log.info("Step1: Create a n node cluster")
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.query_node = self.cluster.master
        ########################################################################################################################
        self.log.info("Step 2 & 3: Create required buckets.")
        bucket = self.create_required_buckets()
        self.loop = 0
        #######################################################################################################################
        while self.loop<self.iterations:
            self.log.info("Step 4: Pre-Requisites for Loading of docs")
            self.start = 0
            self.bucket_util.add_rbac_user()
            self.end = self.initial_load_count = self.input.param("initial_load", 1000)
            initial_load = doc_generator("Users", self.start, self.start + self.initial_load_count, doc_size=self.doc_size)
            self.initial_data_load(initial_load)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 5: Rebalance in with Loading of docs")
            self.generate_docs()
            self.gen_delete_users=None
            self._iter_count = 0
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in = 1, nodes_out = 0)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            #########################################################################################################################
            self.log.info("Step 6: Rebalance Out with Loading of docs")
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in = 0, nodes_out = 1)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            #######################################################################################################################
            self.log.info("Step 7: Rebalance In_Out with Loading of docs")
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in = 2, nodes_out = 1)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 8: Swap with Loading of docs")
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 9: Updating the bucket replica to 2")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=2)
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in =1, nodes_out= 0)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            if "ephemeral" in self.bucket_type:
                self.log.info("No Memcached kill for epehemral bucket")
            else:
                self.log.info("Step 10: Stopping and restarting memcached process")
                self.generate_docs()
                if not self.atomicity:
                    self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                           num_reader_threads=self.new_num_reader_threads)
                rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [])
                tasks_info = self.data_load()
                if not self.atomicity:
                    self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                           num_reader_threads="disk_io_optimized")
                # self.sleep(600, "Wait for Rebalance to start")
                self.task.jython_task_manager.get_task_result(rebalance_task)
                reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                self.stop_process()
                self.data_validation_mode(tasks_info)
                self.tasks = []
                self.bucket_util.print_bucket_stats()
                self.print_crud_stats()
                self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 11: Failover a node and RebalanceOut that node with loading in parallel")
            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.nodes_in_cluster, self.bucket_util.buckets)
            prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.nodes_in_cluster, self.bucket_util.buckets)
            self.sleep(10)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
                self.cluster.nodes_in_cluster, self.bucket_util.buckets, path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

            # Mark Node for failover
            self.generate_docs()
            tasks_info = self.data_load()
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)

            self.sleep(300)
            self.nodes = self.rest.node_statuses()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[self.chosen[0].id])
            # self.sleep(600)
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance failed")

            servs_out = [node for node in self.cluster.servers if node.ip == self.chosen[0].ip]
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
            self.available_servers += servs_out
            self.sleep(10)

            self.data_validation_mode(tasks_info)

            self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.nodes_in_cluster, self.bucket_util.buckets)
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
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            # self.sleep(600)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 12: Failover a node and FullRecovery that node")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.nodes_in_cluster, self.bucket_util.buckets)
            prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.nodes_in_cluster, self.bucket_util.buckets)
            self.sleep(10)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
                self.cluster.nodes_in_cluster, self.bucket_util.buckets, path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

            self.generate_docs()
            tasks_info = self.data_load()
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)

            self.sleep(300)

            # Mark Node for full recovery
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id, recoveryType="full")

            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            # self.sleep(600)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.sleep(10)

            self.data_validation_mode(tasks_info)

            self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.nodes_in_cluster, self.bucket_util.buckets)
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
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 13: Failover a node and DeltaRecovery that node with loading in parallel")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.nodes_in_cluster, self.bucket_util.buckets)
            prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.nodes_in_cluster, self.bucket_util.buckets)
            self.sleep(10)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
                self.cluster.nodes_in_cluster, self.bucket_util.buckets, path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

            self.generate_docs()
            tasks_info = self.data_load()
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)

            self.sleep(300)
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id, recoveryType="delta")
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            # self.sleep(600)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.sleep(10)

            self.data_validation_mode(tasks_info)

            self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.nodes_in_cluster, self.bucket_util.buckets)
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
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
        ########################################################################################################################
            self.log.info("Step 14: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=1)
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [])
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
        ########################################################################################################################
            self.log.info("Step 15: Flush the bucket and start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                self.bucket_util.flush_all_buckets(self.cluster.master)
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                    self.nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(self.nodes_cluster, int(len(self.cluster.nodes_in_cluster) - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out)
                    # self.sleep(600)
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
