'''
Created on Mar 29, 2020

@author: riteshagarwal
'''

import random

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from TestInput import TestInputSingleton
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView
from Cb_constants.CBServer import CbServer
import os
from memcached.helper.data_helper import MemcachedClientHelper
from cb_tools.cbstats import Cbstats
import threading


class volume(BaseTestCase):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        BaseTestCase.setUp(self)
        self.rest = RestConnection(self.servers[0])
        self.op_type = self.input.param("op_type", "create")
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
        self.create_perc = 100
        self.update_perc = self.input.param("update_perc", 50)
        self.delete_perc = self.input.param("delete_perc", 50)
        self.expiry_perc = self.input.param("expiry_perc", 0)
        self.start = 0
        self.end = 0
        self.initial_items = self.start
        self.final_items = self.end
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0
        self.end_step = self.input.param("end_step", None)
        self.num_collections = self.input.param("num_collections", 1)
        self.key_prefix = "Users"
        self.skip_read_on_error = False
        self.suppress_error_table = False

        self.disable_magma_commit_points = self.input.param(
            "disable_magma_commit_points", False)
        self.fragmentation = int(self.input.param("fragmentation", 50))

    def create_required_buckets(self):
        self.log.info("Get the available memory quota")
        self.info = self.rest.get_nodes_self()
        threshold_memory = 100
        # threshold_memory_vagrant = 100
        total_memory_in_mb = self.info.mcdMemoryReserved
        total_available_memory_in_mb = total_memory_in_mb

        # If the mentioned service is already present,
        # we remove that much memory from available memory quota
        if "index" in self.info.services:
            total_available_memory_in_mb -= self.info.indexMemoryQuota
        if "fts" in self.info.services:
            total_available_memory_in_mb -= self.info.ftsMemoryQuota
        if "cbas" in self.info.services:
            total_available_memory_in_mb -= self.info.cbasMemoryQuota
        if "eventing" in self.info.services:
            total_available_memory_in_mb -= self.info.eventingMemoryQuota

        available_memory = total_available_memory_in_mb - threshold_memory

        self.rest.set_service_memoryQuota(service='memoryQuota',
                                          memoryQuota=available_memory)

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        self.bucket_expiry = self.input.param("bucket_expiry", 0)
        ramQuota = self.input.param("ramQuota", available_memory)
        buckets = self.input.param("bucket_names",
                                   "GleamBookUsers").split(';')
        self.bucket_type = self.bucket_type.split(';')
        self.compression_mode = self.compression_mode.split(';')
        self.bucket_eviction_policy = self.bucket_eviction_policy
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i],
                 Bucket.ramQuotaMB: ramQuota/self.num_buckets,
                 Bucket.maxTTL: self.bucket_expiry,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: self.bucket_type[i],
                 Bucket.compressionMode: self.compression_mode[i]})
            self.bucket_util.create_bucket(bucket)

        # rebalance the new buckets across all nodes.
        self.log.info("Rebalance Starts")
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[])
        self.rest.monitorRebalance()
        return bucket

    def set_num_writer_and_reader_threads(self, num_writer_threads="default",
                                          num_reader_threads="default"):
        for node in self.cluster_util.get_kv_nodes():
            bucket_helper = BucketHelper(node)
            bucket_helper.update_memcached_settings(
                num_writer_threads=num_writer_threads,
                num_reader_threads=num_reader_threads)

    def generate_docs(self, doc_ops=None,
                      create_end=None,
                      create_start=None,
                      update_end=None,
                      update_start=None,
                      delete_end=None,
                      delete_start=None,
                      expire_end=None,
                      expire_start=None):
        self.gen_delete = None
        self.gen_create = None
        self.gen_update = None
        self.gen_expiry = None
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0
        self.initial_items = self.final_items

        if doc_ops is None:
            doc_ops = self.doc_ops

        if "update" in doc_ops:
            if update_start is not None:
                self.update_start = update_start
            else:
                self.update_start = 0
            if update_end is not None:
                self.update_end = update_end
            else:
                self.update_end = self.num_items*self.update_perc/100
            self.mutate += 1
            self.gen_update = doc_generator(
                self.key_prefix, self.update_start,
                self.update_end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size, mutate=self.mutate)

        if "delete" in doc_ops:
            if delete_start is not None:
                self.delete_start = delete_start
            else:
                self.delete_start = self.start
            if delete_end is not None:
                self.delete_end = delete_end
            else:
                self.delete_end = self.start+(self.num_items *
                                              self.delete_perc)/100
            self.gen_delete = doc_generator(
                self.key_prefix,
                self.delete_start,
                self.delete_end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            self.final_items -= (self.delete_end - self.delete_start) * self.num_collections

        if "expiry" in doc_ops and self.maxttl:
            if expire_start is not None:
                self.expire_start = expire_start
            else:
                self.expire_start = self.start+(self.num_items *
                                                self.delete_perc)/100
            if expire_end is not None:
                self.expire_end = expire_end
            else:
                self.expire_end = self.start+self.num_items *\
                                  (self.delete_perc + self.expiry_perc)/100
            self.gen_expiry = doc_generator(
                self.key_prefix,
                self.expire_start,
                self.expire_end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            self.final_items -= (self.expire_end - self.expire_start) * self.num_collections

        if "create" in doc_ops:
            self.start = self.end
            self.end += self.num_items*self.create_perc/100
            if create_start is not None:
                self.create_start = create_start
            else:
                self.create_start = self.start
            if create_end is not None:
                self.create_end = create_end
                self.end = self.create_end
            else:
                self.create_end = self.end
            self.gen_create = doc_generator(
                self.key_prefix,
                self.create_start, self.create_end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            self.final_items += (abs(self.create_end - self.create_start)) * self.num_collections

    def doc_loader(self, op_type, kv_gen, exp=0, scope=None, collection=None):
        if scope is None:
            scope = CbServer.default_scope
        if collection is None:
            collection = CbServer.default_collection
        retry_exceptions = [
            SDKException.AmbiguousTimeoutException,
            SDKException.RequestCanceledException
        ]
        tasks_info = self.bucket_util._async_load_all_buckets(
            self.cluster, kv_gen, op_type, exp,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, pause_secs=5,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            retry_exceptions=retry_exceptions,
            skip_read_on_error=self.skip_read_on_error,
            suppress_error_table=self.suppress_error_table,
            scope=scope, collection=collection)
        return tasks_info

    def data_load(self,
                  scope=CbServer.default_scope,
                  collections=[CbServer.default_scope]):
        tasks_info = dict()
        for collection in collections:
            if self.gen_update is not None:
                task_info = self.doc_loader("update", self.gen_update,
                                            scope=scope,
                                            collection=collection)
                tasks_info.update(task_info.items())
            if self.gen_create is not None:
                task_info = self.doc_loader("create", self.gen_create,
                                            scope=scope,
                                            collection=collection)
                tasks_info.update(task_info.items())
            if self.gen_delete is not None:
                task_info = self.doc_loader("delete", self.gen_delete,
                                            scope=scope,
                                            collection=collection)
                tasks_info.update(task_info.items())
            if self.gen_expiry is not None and self.maxttl:
                task_info = self.doc_loader("update", self.gen_expiry,
                                            self.maxttl,
                                            scope=scope,
                                            collection=collection)
                tasks_info.update(task_info.items())
        return tasks_info

    def wait_for_doc_load_completion(self, tasks_info, wait_for_stats=True):
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                       self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        for task, task_info in tasks_info.items():
            self.assertFalse(
                task_info["ops_failed"],
                "Doc ops failed for task: {}".format(task.thread_name))

        if wait_for_stats:
            self.bucket_util._wait_for_stats_all_buckets()

    def data_validation(self, scope=CbServer.default_scope,
                        collections=[CbServer.default_scope]):
        self.log.info("Validating Active/Replica Docs")
        self.check_replica = False
        for bucket in self.bucket_util.buckets:
            tasks = list()
            for collection in collections:
                if self.gen_update is not None:
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        check_replica=self.check_replica,
                        scope=scope, collection=collection))
                if self.gen_create is not None:
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_create, "create", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        check_replica=self.check_replica,
                        scope=scope, collection=collection))
                if self.gen_delete is not None:
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_delete, "delete", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        check_replica=self.check_replica,
                        scope=scope, collection=collection))
                if self.gen_expiry is not None:
                    self.sleep(self.maxttl,
                               "Wait for docs expiry time.")
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_expiry, "delete", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        check_replica=self.check_replica,
                        scope=scope, collection=collection))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)

    def get_bucket_dgm(self, bucket):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Active Resident Threshold of {0} is {1}".format(
            bucket.name, dgm))

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
            self.cluster.servers[:self.nodes_init],
            servs_in, servs_out,
            check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=150)

        self.available_servers = [servs for servs in self.available_servers
                                  if servs not in servs_in]
        self.available_servers += servs_out

        self.cluster.nodes_in_cluster.extend(servs_in)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster)
                                             - set(servs_out))
        return rebalance_task

    def print_crud_stats(self):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Initial Items",
                                "Current Items",
                                "Items Updated",
                                "Items Created",
                                "Items Deleted",
                                "Items Expired"])
        self.table.add_row([
            str(self.initial_items),
            str(self.final_items),
            str(abs(self.update_start)) + "-" + str(abs(self.update_end)),
            str(abs(self.create_start)) + "-" + str(abs(self.create_end)),
            str(abs(self.delete_start)) + "-" + str(abs(self.delete_end)),
            str(abs(self.expire_start)) + "-" + str(abs(self.expire_end))
            ])
        self.table.display("Docs statistics")

    def perform_load(self, crash=False, num_kills=1, validate_data=True):
        tasks_info = self.data_load(
            scope=self.scope_name,
            collections=self.bucket.scopes[self.scope_name].collections.keys())
        self.wait_for_doc_load_completion(tasks_info)

        if crash:
            self.kill_memcached(num_kills=num_kills)

        if validate_data:
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())

        self.get_magma_disk_usage()
        self.bucket_util.print_bucket_stats()
        self.print_crud_stats()
        self.get_bucket_dgm(self.bucket)
        crashes = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if len(crashes) > 0:
            self.task.jython_task_manager.abort_all_tasks()

        self.assertTrue(len(crashes) == 0, "Found servers having crashes")

    def get_magma_disk_usage(self, bucket=None):
        if bucket is None:
            bucket = self.bucket
        servers = self.cluster.nodes_in_cluster
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            kvstore += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()
        self.log.debug("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        return kvstore, wal, keyTree, seqTree

    def crash_thread(self, nodes=None, graceful=False):
        self.stop_crash = False
        if not nodes:
            nodes = self.cluster.nodes_in_cluster

        while not self.stop_crash:
            sleep = random.randint(60, 120)
            self.sleep(sleep,
                       "Waiting for %s sec to kill memc on all nodes" %
                       sleep)
            self.kill_memcached(nodes, num_kills=1, graceful=graceful, wait=True)

    def kill_memcached(self, servers=None, num_kills=1,
                       graceful=False, wait=True):
        if not servers:
            servers = self.cluster.nodes_in_cluster

        for _ in xrange(num_kills):
            self.sleep(2, "Sleep for 2 seconds between continuous memc kill")
            for server in servers:
                shell = RemoteMachineShellConnection(server)

                if graceful:
                    shell.restart_couchbase()
                else:
                    shell.kill_memcached()

                shell.disconnect()

        crashes = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if len(crashes) > 0:
            self.task.jython_task_manager.abort_all_tasks()

        self.assertTrue(len(crashes) == 0, "Found servers having crashes")

        if wait:
            for server in servers:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                        [server],
                        self.bucket_util.buckets[0],
                        wait_time=self.wait_timeout * 20))

    def perform_rollback(self, mem_only_items=100000, doc_type="create",
                         kill_rollback=1):
        _iter = 0
        while _iter < 10:
            tasks_info = dict()
            start = self.num_items

            # Stopping persistence on NodeA
            mem_client = MemcachedClientHelper.direct_client(
                self.cluster.nodes_in_cluster[0], self.bucket_util.buckets[0])
            mem_client.stop_persistence()

            shell = RemoteMachineShellConnection(self.cluster.nodes_in_cluster[0])
            cbstats = Cbstats(shell)
            self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                       name)

            gen_docs = doc_generator(
                self.key, start, mem_only_items,
                doc_size=self.doc_size, doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value)

            for collection in self.bucket.scopes[self.scope_name].collections.keys():
                tasks_info.update(self.doc_loader(doc_type, gen_docs,
                                                  exp=5,
                                                  scope=self.scope_name,
                                                  collection=collection))
            self.wait_for_doc_load_completion(tasks_info, wait_for_stats=False)

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_only_items}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            self.kill_memcached(num_kills=kill_rollback)

            self.bucket_util.verify_stats_all_buckets(self.final_items,
                                                      timeout=300)

            self.get_magma_disk_usage()
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)
            _iter += 1

    def PrintStep(self, msg=None):
        print "\n"
        print "\t", "#"*60
        print "\t", "#"
        print "\t", "#  %s" % msg
        print "\t", "#"
        print "\t", "#"*60
        print "\n"

    def Volume(self):
        #######################################################################
        self.PrintStep("Step1: Create a n node cluster")
        if self.nodes_init > 1:
            nodes_init = self.cluster.servers[1:self.nodes_init]
            self.task.rebalance([self.cluster.master], nodes_init, [])
            self.cluster.nodes_in_cluster.extend(
                [self.cluster.master] + nodes_init)

        #######################################################################
        self.PrintStep("Step 2 & 3: Create required buckets.")
        self.bucket = self.create_required_buckets()
        self.loop = 0
        self.scope_name = "VolumeScope"
        collection_prefix = "VolumeCollection"
        self.bucket_util.create_scope(self.cluster.master,
                                      self.bucket,
                                      {"name": self.scope_name})
        for i in range(self.num_collections):
            collection_name = collection_prefix + str(i)
            self.log.info("Creating scope::collection '%s::%s'"
                          % (self.scope_name, collection_name))
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               self.scope_name,
                                               {"name": collection_name})
            self.sleep(2)
        #######################################################################
        while self.loop < self.iterations:
            self.PrintStep("Step 4: Pre-Requisites for Loading of docs")
            self.bucket_util.add_rbac_user()
            self.generate_docs(doc_ops="create")
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            for task in tasks_info:
                self.task.jython_task_manager.get_task_result(task)
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)
            self.create_perc = self.input.param("create_perc", 100)
            ###################################################################
            self.PrintStep("Step 5: Rebalance in with Loading of docs")
            self.generate_docs(doc_ops="create")
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 6: Rebalance Out with Loading of docs")
            self.generate_docs()
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 7: Rebalance In_Out with Loading of docs")
            self.generate_docs()
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1)
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 8: Swap with Loading of docs")
            self.generate_docs()
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 9: Updating the bucket replica to 2")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=2)
            self.generate_docs()
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.wait_for_doc_load_completion(tasks_info)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 10: Stopping and restarting memcached process")
            self.generate_docs()
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            rebalance_task = self.task.async_rebalance(self.cluster.servers,
                                                       [], [])
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.stop_process()
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 11: Failover a node and RebalanceOut that node \
            with loading in parallel")
            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.nodes_in_cluster, self.bucket_util.buckets)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                get_and_compare_active_replica_data_set_all(
                    self.cluster.nodes_in_cluster, self.bucket_util.buckets,
                    path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Mark Node for failover
            self.generate_docs()
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.rest.monitorRebalance()
            self.nodes = self.rest.node_statuses()
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                ejectedNodes=[self.chosen[0].id])
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True),
                            msg="Rebalance failed")

            servs_out = [node for node in self.cluster.servers
                         if node.ip == self.chosen[0].ip]
            self.cluster.nodes_in_cluster = list(
                set(self.cluster.nodes_in_cluster) - set(servs_out))
            self.available_servers += servs_out
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())

            self.bucket_util.compare_failovers_logs(
                prev_failover_stats,
                self.cluster.nodes_in_cluster,
                self.bucket_util.buckets)

            self.bucket_util.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.cluster.servers[:self.nodes_in + self.nodes_init],
                self.bucket_util.buckets, path=None)
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
            self.bucket_util.vb_distribution_analysis(
                servers=nodes, buckets=self.bucket_util.buckets,
                num_replicas=2,
                std=std, total_vbuckets=self.cluster_util.vbuckets)
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 12: Failover a node and FullRecovery\
             that node")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.nodes_in_cluster, self.bucket_util.buckets)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                get_and_compare_active_replica_data_set_all(
                    self.cluster.nodes_in_cluster,
                    self.bucket_util.buckets,
                    path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            self.generate_docs()
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.rest.monitorRebalance()
            # Mark Node for full recovery
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="full")

            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])

            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())

            self.bucket_util.compare_failovers_logs(
                prev_failover_stats,
                self.cluster.nodes_in_cluster,
                self.bucket_util.buckets)

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
            self.get_bucket_dgm(self.bucket)

            ###################################################################
            self.PrintStep("Step 13: Failover a node and DeltaRecovery that \
            node with loading in parallel")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.nodes_in_cluster, self.bucket_util.buckets)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                get_and_compare_active_replica_data_set_all(
                    self.cluster.nodes_in_cluster,
                    self.bucket_util.buckets,
                    path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            self.generate_docs()
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.rest.monitorRebalance()
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="delta")
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())

            self.bucket_util.compare_failovers_logs(
                prev_failover_stats,
                self.cluster.nodes_in_cluster,
                self.bucket_util.buckets)

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
            self.get_bucket_dgm(self.bucket)

        #######################################################################
            self.PrintStep("Step 14: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=1)
            self.generate_docs()
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            rebalance_task = self.task.async_rebalance(self.cluster.servers,
                                                       [], [])
            tasks_info = self.data_load(
                scope=self.scope_name,
                collections=self.bucket.scopes[self.scope_name].collections.keys())
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.wait_for_doc_load_completion(tasks_info)
            self.data_validation(scope=self.scope_name,
                                 collections=self.bucket.
                                 scopes[self.scope_name].collections.keys())
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(self.bucket)

        #######################################################################
            self.PrintStep("Step 15: Flush the bucket and \
            start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                self.bucket_util.flush_all_buckets(self.cluster.master)
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    nodes_cluster = self.cluster.nodes_in_cluster[:]
                    nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(
                        nodes_cluster,
                        int(len(self.cluster.nodes_in_cluster)
                            - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out)

                    self.task.jython_task_manager.get_task_result(
                        rebalance_task)
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(
                        set(self.cluster.nodes_in_cluster) - set(servs_out))
                    self.get_bucket_dgm(self.bucket)
            else:
                self.log.info("Volume Test Run Complete")
                self.get_bucket_dgm(self.bucket)

    def SteadyStateVolume(self):
        #######################################################################
        self.PrintStep("Step 1: Create a n node cluster")
        if self.nodes_init > 1:
            nodes_init = self.cluster.servers[1:self.nodes_init]
            self.task.rebalance([self.cluster.master], nodes_init, [])
            self.cluster.nodes_in_cluster.extend(
                [self.cluster.master] + nodes_init)

        #######################################################################
        self.PrintStep("Step 2: Create required buckets and 10 collections.")
        self.bucket = self.create_required_buckets()
        props = "magma"
        update_bucket_props = False

        if self.disable_magma_commit_points:
            props += ";magma_max_commit_points=0"
            update_bucket_props = True

        if self.fragmentation != 50:
            props += ";magma_delete_frag_ratio=%s" % str(self.fragmentation/100.0)
            update_bucket_props = True

        if update_bucket_props:
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.bucket_util.buckets)

        self.scope_name = "VolumeScope"
        collection_prefix = "VolumeCollection"
        self.bucket_util.create_scope(self.cluster.master,
                                      self.bucket,
                                      {"name": self.scope_name})

        for i in range(self.num_collections):
            collection_name = collection_prefix + str(i)
            self.log.info("Creating scope::collection '%s::%s'"
                          % (self.scope_name, collection_name))
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               self.scope_name,
                                               {"name": collection_name})
            self.sleep(2)
        #######################################################################
        self.PrintStep("Step 3: Create and Delete everything. checkout fragmentation")
        '''
        creates: 0 - 10M
        deletes: 0 - 10M
        Final Docs = 0
        '''
        self.create_perc = 100
        self.delete_perc = 100

        self.generate_docs(doc_ops="create")
        self.perform_load(validate_data=False)

        self.generate_docs(doc_ops="delete",
                           delete_start=self.start,
                           delete_end=self.end)
        self.perform_load(validate_data=True)
        '''
        fragmentation at this time: 0
        '''
        #######################################################################
        self.PrintStep("Step 4: Load 100M/100GB items, sequential keys")
        '''
        creates: 0 - 10M
        creates: 0 - 10M
        Final Docs = 20M (0-20M)
        '''
        self.create_perc = 100
        _iter = 0
        while _iter < 2:
            self.generate_docs(doc_ops="create")
            self.perform_load(validate_data=False)
            _iter += 1
        '''
        fragmentation at this time: 0, total data: 2X, stale: 0
        '''
        #######################################################################
        self.PrintStep("Step 5: Update the first set of 10% items 10 times")
        '''
        update: 0 - 1M * 10
        Final Docs = 20M (0-20M)
        '''
        self.update_perc = 10
        _iter = 0
        while _iter < 10:
            self.generate_docs(doc_ops="update")
            self.perform_load(crash=False, validate_data=True)
            _iter += 1
        if self.end_step == 5:
            exit(5)
        '''
        fragmentation at this time: 50, total data: 2X, stale: X
        '''
        #######################################################################
        self.PrintStep("Step 6: Reverse Update last set of 10% items 10 times")
        '''
        Reverse Update: 10M - 9M
        Final Docs = 20M (0-20M)
        '''
        _iter = 0
        self.update_perc = 10
        while _iter < 10:
            self.generate_docs(doc_ops="update",
                               update_start=-self.num_items+1,
                               update_end=self.num_items *
                               self.update_perc/100-self.num_items+1)
            self.perform_load(crash=False, validate_data=True)
            _iter += 1
        if self.end_step == 6:
            exit(6)
        '''
        fragmentation: 50, total data: 2X, stale: X
        '''
        #######################################################################
        self.PrintStep("Step 7: Create random keys")
        '''
        Create Random: 0 - 10M
        Final Docs = 30M (0-20M, 10M Random)
        '''
        temp = self.key_prefix
        self.key_prefix = "random_keys"
        self.create_perc = 100

        self.generate_docs(doc_ops="create")
        self.perform_load(crash=False, validate_data=True)

        self.key_prefix = temp
        '''
        fragmentation: 50, total data: 3X, stale: X
        '''
        #######################################################################
        self.PrintStep("Step 8: Update all random 10 times")
        '''
        Update Random: 0 - 10M
        Final Docs = 30M (0-20M, 10M Random)
        '''
        _iter = 0
        self.update_perc = 100
        self.key_prefix = "random_keys"

        while _iter < 10:
            self.generate_docs(doc_ops="update")
            self.perform_load(crash=False, validate_data=True)
            _iter += 1

        self.key_prefix = temp
        if self.end_step == 8:
            exit(8)
        '''
        fragmentation: 50, total data: 3X, stale: 1.5X
        '''
        #######################################################################
        self.PrintStep("Step 9: Delete/Re-Create all random items")
        '''
        Delete Random: 0 - 10M
        Create Random: 0 - 10M
        Final Docs = 30M (0-20M, 10M Random)
        '''
        self.key_prefix = "random_keys"
        self.delete_perc = 100

        self.generate_docs(doc_ops="delete")
        self.perform_load(crash=False, validate_data=True)

        '''
        fragmentation: 50, total data: 3X, stale: 1.5X
        '''
        self.generate_docs(doc_ops="create")
        self.perform_load(crash=False, validate_data=True)

        self.key_prefix = temp
        if self.end_step == 9:
            exit(9)
        #######################################################################
        self.PrintStep("Step 10: Update 10% items 10 times and \
        crash during recovery")
        '''
        Update: 0 - 1M
        Final Docs = 30M (0-20M, 10M Random)
        '''
        self.update_perc = 10
        _iter = 0
        while _iter < 10:
            self.generate_docs(doc_ops="update")
            self.perform_load(crash=False, validate_data=True)
            _iter += 1

        if self.end_step == 10:
            exit(10)
        #######################################################################
        self.PrintStep("Step 11: Normal Rollback with creates")
        '''
        Final Docs = 30M (0-20M, 10M Random)
        '''
        mem_only_items = self.input.param("rollback_items", 1000000)
        self.perform_rollback(mem_only_items, doc_type="create")

        if self.end_step == 11:
            exit(11)
        #######################################################################
        self.PrintStep("Step 12: Normal Rollback with updates")
        '''
        Final Docs = 30M (0-20M, 10M Random)
        '''
        mem_only_items = self.input.param("rollback_items", 1000000)
        self.perform_rollback(mem_only_items, doc_type="update")

        if self.end_step == 12:
            exit(12)
        #######################################################################
        self.PrintStep("Step 13: Normal Rollback with deletes")
        '''
        Final Docs = 30M (0-20M, 10M Random)
        '''
        mem_only_items = self.input.param("rollback_items", 1000000)
        self.perform_rollback(mem_only_items, doc_type="delete")

        if self.end_step == 13:
            exit(13)
        #######################################################################
        self.PrintStep("Step 14: Normal Rollback with expiry")
        '''
        Final Docs = 30M (0-20M, 10M Random)
        '''
        mem_only_items = self.input.param("rollback_items", 1000000)
        self.perform_rollback(mem_only_items, doc_type="expiry")

        if self.end_step == 14:
            exit(14)
        #######################################################################
        self.skip_read_on_error = True
        self.suppress_error_table = True
        self.PrintStep("Step 15: Random crashes during Creates")
        '''
        Creates: 20M-50M
        Final Docs = 60M (0-50M, 10M Random)
        '''
        self.create_perc = 300
        self.generate_docs(doc_ops="create")
        tasks_info = self.data_load(
            scope=self.scope_name,
            collections=self.bucket.scopes[self.scope_name].collections.keys())

        th = threading.Thread(target=self.crash_thread,
                              kwargs={"graceful": False})
        th.start()

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        #######################################################################
        self.PrintStep("Step 15: Random crashes during Updates")
        '''
        Updates: 0M-20M
        Final Docs = 60M (0-50M, 10M Random)
        '''
        self.update_perc = 200
        self.generate_docs(doc_ops="update")
        tasks_info = self.data_load(
            scope=self.scope_name,
            collections=self.bucket.scopes[self.scope_name].collections.keys())

        th = threading.Thread(target=self.crash_thread,
                              kwargs={"graceful": False})
        th.start()

        self.wait_for_doc_load_completion(tasks_info)
        #######################################################################
        self.PrintStep("Step 15: Random crashes during Deletes")
        '''
        Deletes: 0M-20M
        Final Docs = 40M (20-50M, 10M Random)
        '''
        self.delete_perc = 200
        self.generate_docs(doc_ops="delete",
                           delete_start=0,
                           delete_end=self.num_items*self.delete_perc/100)
        tasks_info = self.data_load(
            scope=self.scope_name,
            collections=self.bucket.scopes[self.scope_name].collections.keys())

        th = threading.Thread(target=self.crash_thread,
                              kwargs={"graceful": False})
        th.start()

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        #######################################################################
        self.PrintStep("Step 15: Random crashes during Expiry")
        '''
        Updates: 0M-20M , MAXTTL=5s
        Final Docs = 40M (20-50M, 10M Random)
        '''
        self.expiry_perc = 200
        self.generate_docs(doc_ops="expiry",
                           expire_start=0,
                           expire_end=self.num_items*self.expiry_perc/100)
        tasks_info = self.data_load(
            scope=self.scope_name,
            collections=self.bucket.scopes[self.scope_name].collections.keys())

        th = threading.Thread(target=self.crash_thread,
                              kwargs={"graceful": False})
        th.start()

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        #######################################################################
        self.PrintStep("Step 20: Drop a collection")
        for i in range(1, self.num_collections, 2):
            collection_name = collection_prefix + str(i)
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.bucket,
                                             self.scope_name,
                                             collection_name)
            self.bucket.scopes[self.scope_name].collections.pop(collection_name)
            self.kill_memcached()
            self.get_magma_disk_usage()
        if self.end_step == 20:
            exit(20)
