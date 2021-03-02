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
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView
from Cb_constants.CBServer import CbServer
import os
from memcached.helper.data_helper import MemcachedClientHelper
from cb_tools.cbstats import Cbstats
import threading
import time
from membase.api.exception import RebalanceFailedException
import math
import subprocess
from math import ceil
from Jython_tasks.task_manager import TaskManager


class volume(BaseTestCase):
    def init_doc_params(self):
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

    def setUp(self):
        BaseTestCase.setUp(self)
        self.init_doc_params()

        self.num_collections = self.input.param("num_collections", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(':')
        self.max_tasks_per_collection = 8
        process_concurrency = int(math.ceil(self.max_tasks_per_collection /
                                            float(len(self.doc_ops))))
        process_concurrency = self.input.param("pc", process_concurrency)
        doc_tasks = (self.num_buckets*self.num_scopes*self.num_collections) * len(self.doc_ops) * process_concurrency + 1
        self.thread_to_use = min(64, doc_tasks)
        self.input.test_params.update({"threads_to_use":
                                       self.thread_to_use})
        print "Total Doc-Tasks workers = {}".format(self.thread_to_use)
        print "Total Doc-Tasks = {}".format(doc_tasks)
        self.doc_loading_tm = TaskManager(number_of_threads=self.thread_to_use)
        self.process_concurrency = self.input.param("pc", process_concurrency)

        self.rest = RestConnection(self.servers[0])
        self.op_type = self.input.param("op_type", "create")
        self.dgm = self.input.param("dgm", None)
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.iterations = self.input.param("iterations", 2)
        self.step_iterations = self.input.param("step_iterations", 1)
        self.rollback = self.input.param("rollback", True)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.new_num_writer_threads = self.input.param(
            "new_num_writer_threads", 6)
        self.new_num_reader_threads = self.input.param(
            "new_num_reader_threads", 8)
        self.end_step = self.input.param("end_step", None)
        self.key_prefix = "Users"
        self.crashes = self.input.param("crashes", 20)
        self.check_dump_thread = True
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.track_failures = True
        self.loader_dict = None

        self.disable_magma_commit_points = self.input.param(
            "disable_magma_commit_points", False)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.cursor_dropping_checkpoint = self.input.param(
            "cursor_dropping_checkpoint", None)
        self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                       True)
        #######################################################################
        self.PrintStep("Step 1: Create a %s node cluster" % self.nodes_init)
        if self.nodes_init > 1:
            nodes_init = self.cluster.servers[1:self.nodes_init]
            self.task.rebalance([self.cluster.master], nodes_init, [])
            self.cluster.nodes_in_cluster.extend(
                [self.cluster.master] + nodes_init)
        else:
            self.cluster.nodes_in_cluster.extend([self.cluster.master])
        self.cluster_util.set_metadata_purge_interval()
        #######################################################################
        self.PrintStep("Step 2: Create required buckets and collections.")
        self.create_required_buckets()
        props = "magma"
        update_bucket_props = False

        if self.disable_magma_commit_points:
            props += ";magma_max_commit_points=0"
            update_bucket_props = True

        if self.cursor_dropping_checkpoint:
            props += ";cursor_dropping_checkpoint_mem_upper_mark=%s" %\
                str(self.cursor_dropping_checkpoint)
            update_bucket_props = True

        if update_bucket_props:
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.bucket_util.buckets)
            self.sleep(10, "Sleep for 10 seconds so that collections \
            can be created")
        else:
            for node in self.servers:
                shell = RemoteMachineShellConnection(node)
                shell.enable_diag_eval_on_non_local_hosts()
                shell.disconnect()

        self.scope_name = self.input.param("scope_name",
                                           CbServer.default_scope)
        if self.scope_name != CbServer.default_scope:
            self.bucket_util.create_scope(self.cluster.master,
                                          self.bucket,
                                          {"name": self.scope_name})

        if self.num_scopes > 1:
            self.scope_prefix = self.input.param("scope_prefix",
                                                 "VolumeScope")
            for bucket in self.bucket_util.buckets:
                for i in range(self.num_scopes):
                    scope_name = self.scope_prefix + str(i)
                    self.log.info("Creating scope: %s"
                                  % (scope_name))
                    self.bucket_util.create_scope(self.cluster.master,
                                                  bucket,
                                                  {"name": scope_name})
                    self.sleep(0.5)
            self.num_scopes += 1
        for bucket in self.bucket_util.buckets:
            for scope in bucket.scopes.keys():
                if self.num_collections > 1:
                    self.collection_prefix = self.input.param("collection_prefix",
                                                              "VolumeCollection")

                    for i in range(self.num_collections):
                        collection_name = self.collection_prefix + str(i)
                        self.bucket_util.create_collection(self.cluster.master,
                                                           bucket,
                                                           scope,
                                                           {"name": collection_name})
                        self.sleep(0.5)
#         self.num_collections += 1
        self.rest = RestConnection(self.cluster.master)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        if self.sdk_client_pool:
            max_clients = min(self.task_manager.number_of_threads,
                              20)
            clients_per_bucket = int(ceil(max_clients / self.num_buckets))
            for bucket in self.bucket_util.buckets:
                self.sdk_client_pool.create_clients(
                    bucket,
                    [self.cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)

    def tearDown(self):
        self.check_dump_thread = False
        self.stop_crash = True
        BaseTestCase.tearDown(self)

    def get_memory_footprint(self):
        out = subprocess.Popen(['ps', 'v', '-p', str(os.getpid())],stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        self.PrintStep("RAM FootPrint: %s" % str(mem))
        return mem

    def create_required_buckets(self):
        self.log.info("Get the available memory quota")
        self.info = self.rest.get_nodes_self()

        # threshold_memory_vagrant = 100
        kv_memory = self.info.memoryQuota - 100

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        self.bucket_expiry = self.input.param("bucket_expiry", 0)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = ["GleamBookUsers"]*self.num_buckets
        self.bucket_type = self.bucket_type.split(';')*self.num_buckets
        self.compression_mode = self.compression_mode.split(';')*self.num_buckets
        self.bucket_eviction_policy = self.bucket_eviction_policy
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i] + str(i),
                 Bucket.ramQuotaMB: ramQuota/self.num_buckets,
                 Bucket.maxTTL: self.bucket_expiry,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: self.bucket_type[i],
                 Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
                 Bucket.compressionMode: self.compression_mode[i],
                 Bucket.fragmentationPercentage: self.fragmentation})
            self.bucket_util.create_bucket(bucket)

        # rebalance the new buckets across all nodes.
        self.log.info("Rebalance Starts")
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[])
        self.rest.monitorRebalance()

    def set_num_writer_and_reader_threads(self, num_writer_threads="default",
                                          num_reader_threads="default"):
        for node in self.cluster_util.get_kv_nodes():
            bucket_helper = BucketHelper(node)
            bucket_helper.update_memcached_settings(
                num_writer_threads=num_writer_threads,
                num_reader_threads=num_reader_threads)

    def generate_docs(self, doc_ops=None,
                      create_end=None, create_start=None,
                      update_end=None, update_start=None,
                      delete_end=None, delete_start=None,
                      expire_end=None, expire_start=None):
        self.get_memory_footprint()
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
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            self.final_items -= (self.delete_end - self.delete_start) * self.num_collections * self.num_scopes

        if "expiry" in doc_ops:
            if self.maxttl == 0:
                self.maxttl = self.input.param("maxttl", 10)
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
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            self.final_items -= (self.expire_end - self.expire_start) * self.num_collections * self.num_scopes

        if "create" in doc_ops:
            if create_start is not None:
                self.create_start = create_start
            else:
                self.create_start = self.end
            self.start = self.create_start

            if create_end is not None:
                self.create_end = create_end
            else:
                self.create_end = self.start + self.num_items*self.create_perc/100
            self.end = self.create_end

            self.gen_create = doc_generator(
                self.key_prefix,
                self.create_start, self.create_end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)
            self.final_items += (abs(self.create_end - self.create_start)) * self.num_collections * self.num_scopes

    def doc_loader(self, loader_spec):
        task = self.task.async_load_gen_docs_from_spec(
            self.cluster, self.doc_loading_tm, loader_spec,
            self.sdk_client_pool,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            print_ops_rate=True,
            start_task=True,
            track_failures=self.track_failures)

        return task

    def data_load(self):
        loader_dict = dict()
        retry_exceptions = [
            SDKException.AmbiguousTimeoutException,
            SDKException.RequestCanceledException,
            SDKException.ServerOutOfMemoryException
        ]
        common_params = {"retry_exceptions": retry_exceptions,
                         "suppress_error_table": self.suppress_error_table,
                         "durability_level": self.durability_level,
                         "skip_read_success_results": False,
                         "target_items": 5000,
                         "skip_read_on_error": self.skip_read_on_error,
                         "ignore_exceptions": [],
                         "sdk_timeout_unit": "seconds",
                         "sdk_timeout": 60,
                         "doc_ttl": 0,
                         "doc_gen_type": "default"}
        tasks_info = dict()
        for bucket in self.bucket_util.buckets:
            loader_dict.update({bucket: dict()})
            loader_dict[bucket].update({"scopes": dict()})
            for scope in bucket.scopes.keys():
                loader_dict[bucket]["scopes"].update({scope: dict()})
                loader_dict[bucket]["scopes"][scope].update({"collections":dict()})
                for collection in bucket.scopes[scope].collections.keys():
                    if collection == "_default" and scope != "default":
                        continue
                    loader_dict[bucket]["scopes"][scope]["collections"].update({collection:dict()})
                    if self.gen_update is not None:
                        op_type = "update"
                        common_params.update({"doc_gen":self.gen_update})
                    if self.gen_create is not None:
                        op_type = "create"
                        common_params.update({"doc_gen":self.gen_create})
                    if self.gen_delete is not None:
                        op_type = "delete"
                        common_params.update({"doc_gen":self.gen_delete})
                    if self.gen_expiry is not None and self.maxttl:
                        op_type = "update"
                        common_params.update({"doc_gen":self.gen_expiry,
                                              "doc_ttl":self.maxttl})
                    loader_dict[bucket]["scopes"][scope]["collections"][collection].update({op_type:common_params})
        self.loader_dict = loader_dict
        return self.doc_loader(loader_dict)

    def wait_for_doc_load_completion(self, task, wait_for_stats=True):
        self.doc_loading_tm.get_task_result(task)
        self.bucket_util.validate_doc_loading_results(task)
        self.assertTrue(task.result,
                        "Doc ops failed for task: {}".format(task.thread_name))

        if wait_for_stats:
            try:
                self.bucket_util._wait_for_stats_all_buckets(timeout=1200)
            except Exception as e:
                self.get_gdb()
                raise e

    def get_gdb(self):
        for node in self.cluster.nodes_in_cluster:
            gdb_shell = RemoteMachineShellConnection(node)
            gdb_out = gdb_shell.execute_command('gdb -p `(pidof memcached)` -ex "thread apply all bt" -ex detach -ex quit')[0]
            print node.ip
            print gdb_out
            gdb_shell.disconnect()

    def data_validation(self):
        self.log.info("Validating Active/Replica Docs")
        task = self.task.async_validate_docs_using_spec(
            self.cluster, self.doc_loading_tm, self.loader_dict,
            check_replica=False,
            sdk_client_pool=self.sdk_client_pool,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency)

        self.doc_loading_tm.get_task_result(task)

    def get_bucket_dgm(self, bucket):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Active Resident Threshold of {0} is {1}".format(
            bucket.name, dgm))
        return dgm

    def _induce_error(self, error_condition, nodes=[]):
        nodes = nodes or [self.cluster.master]
        for node in nodes:
            if error_condition == "stop_server":
                self.cluster_util.stop_server(node)
            elif error_condition == "enable_firewall":
                self.cluster_util.start_firewall_on_node(node)
            elif error_condition == "kill_memcached":
                self.cluster_util.kill_server_memcached(node)
            elif error_condition == "reboot_server":
                shell = RemoteMachineShellConnection(node)
                shell.reboot_node()
            elif error_condition == "kill_erlang":
                shell = RemoteMachineShellConnection(node)
                shell.kill_erlang()
                self.sleep(self.sleep_time * 3)
                shell.disconnect()
            else:
                self.fail("Invalid error induce option")

    def _recover_from_error(self, error_condition):
        for node in self.cluster.nodes_in_cluster:
            if error_condition == "stop_server" or error_condition == "kill_erlang":
                self.cluster_util.start_server(node)
            elif error_condition == "enable_firewall":
                self.cluster_util.stop_firewall_on_node(node)

        for node in self.cluster.nodes_in_cluster:
            result = self.cluster_util.wait_for_ns_servers_or_assert([node],
                                                                     wait_time=1200)
            self.assertTrue(result, "Server warmup failed")
            self.check_warmup_complete(node)

    def rebalance(self, nodes_in=0, nodes_out=0,
                  retry_get_process_num=150):
        self.servs_in = random.sample(self.available_servers, nodes_in)

        self.nodes_cluster = self.cluster.nodes_in_cluster[:]
        self.nodes_cluster.remove(self.cluster.master)
        self.servs_out = random.sample(self.nodes_cluster, nodes_out)

        if nodes_in == nodes_out:
            self.vbucket_check = False

        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init],
            self.servs_in, self.servs_out,
            check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=retry_get_process_num)

        self.available_servers = [servs for servs in self.available_servers
                                  if servs not in self.servs_in]
        self.available_servers += self.servs_out

        self.cluster.nodes_in_cluster.extend(self.servs_in)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster)
                                             - set(self.servs_out))
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

    def perform_load(self, crash=False, num_kills=1, wait_for_load=True,
                     validate_data=True):
        task = self.data_load()

        if wait_for_load:
            self.wait_for_doc_load_completion(task)
        else:
            return task

        if crash:
            self.kill_memcached(num_kills=num_kills)

        if validate_data:
            self.data_validation()

        self.print_stats()
        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if result:
            self.PrintStep("CRASH | CRITICAL | WARN messages found in cb_logs")
            if self.assert_crashes_on_load:
                self.task.jython_task_manager.abort_all_tasks()
                self.assertFalse(result)

    def print_stats(self):
        self.bucket_util.print_bucket_stats()
        self.print_crud_stats()
        for bucket in self.bucket_util.buckets:
            self.get_bucket_dgm(bucket)
            if bucket.storageBackend == Bucket.StorageBackend.magma:
                self.get_magma_disk_usage(bucket)
                self.check_fragmentation_using_magma_stats(bucket)
                self.check_fragmentation_using_kv_stats(bucket)

    def check_fragmentation_using_kv_stats(self, bucket, servers=None):
        result = dict()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            frag_val = self.bucket_util.get_fragmentation_kv(
                bucket, server)
            self.log.debug("Current Fragmentation for node {} is {} \
            ".format(server.ip, frag_val))
            result.update({server.ip: frag_val})
        self.log.info("KV stats fragmentation values {}".format(result))

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        stats = list()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            fragmentation_values = list()
            shell = RemoteMachineShellConnection(server)
            output = shell.execute_command(
                    "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                    )[0][0].split('\n')[0]
            shell.disconnect()
            self.log.debug("machine: {} - core(s): {}".format(server.ip,
                                                              output))
            for i in range(min(int(output), 64)):
                grep_field = "rw_{}:magma".format(i)
                _res = self.get_magma_stats(bucket, [server],
                                            field_to_grep=grep_field)
                fragmentation_values.append(float(_res[server.ip][grep_field]
                                                  ["Fragmentation"]))
                stats.append(_res)
            result.update({server.ip: fragmentation_values})
        self.log.info("magma stats fragmentation result {} \
        ".format(result))
        for value in result.values():
            if max(value) > self.fragmentation:
                self.log.info(stats)
                return False
        return True

    def get_magma_stats(self, bucket, servers=None, field_to_grep=None):
        magma_stats_for_all_servers = dict()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            result = dict()
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
            result = cbstat_obj.magma_stats(bucket.name,
                                            field_to_grep=field_to_grep)
            shell.disconnect()
            magma_stats_for_all_servers[server.ip] = result
        return magma_stats_for_all_servers

    def get_magma_disk_usage(self, bucket=None):
        if bucket is None:
            bucket = self.bucket
        servers = self.cluster.nodes_in_cluster
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0
        data_files = 0

        for server in servers:
            shell = RemoteMachineShellConnection(server)
            bucket_path = os.path.join(RestConnection(server).get_data_path(),bucket.name)
            kvstore += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(bucket_path, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])

            cmd = 'find ' + bucket_path + '/magma*/ -maxdepth 1 -type d \
            -print0 | while read -d "" -r dir; do files=("$dir"/*/*/*); \
            printf "%d,%s\n" "${#files[@]}" "$dir"; done'
            data_files = shell.execute_command(cmd)[0]
            for files in data_files:
                if "kvstore" in files and int(files.split(",")[0]) >= 300:
                    self.log.warn("Number of files in {}--{} is {}".format(
                        server.ip, files.split(",")[1].rstrip(), files.split(",")[0]))
            shell.disconnect()
        self.log.debug("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        return kvstore, wal, keyTree, seqTree

    def crash_thread(self, nodes=None, num_kills=1, graceful=False):
        self.stop_crash = False
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.nodes_in_cluster

        while not self.stop_crash:
            self.get_memory_footprint()
            sleep = random.randint(60, 120)
            self.sleep(sleep,
                       "Iteration:{} waiting to kill memc on all nodes".
                       format(self.crash_count))
            self.kill_memcached(nodes, num_kills=num_kills,
                                graceful=graceful, wait=True)
            self.crash_count += 1
            if self.crash_count > self.crashes:
                self.stop_crash = True
        self.sleep(300)

    def kill_memcached(self, servers=None, num_kills=1,
                       graceful=False, wait=True):
        if not servers:
            servers = self.cluster.nodes_in_cluster

        for _ in xrange(num_kills):
            self.sleep(5, "Sleep for 5 seconds between continuous memc kill")
            for server in servers:
                shell = RemoteMachineShellConnection(server)

                if graceful:
                    shell.restart_couchbase()
                else:
                    shell.kill_memcached()

                shell.disconnect()

        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if result:
            self.stop_crash = True
            self.task.jython_task_manager.abort_all_tasks()
            self.assertFalse(
                result,
                "CRASH | CRITICAL | WARN messages found in cb_logs")

        if wait:
            for server in servers:
                self.check_warmup_complete(server)

    def check_dump(self):
        count = 1
        while self.check_dump_thread:
            self.log.debug("Checking crashes {}".format(count))
            result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
            if result:
                self.stop_crash = True
                self.check_dump_thread = False
                self.task.jython_task_manager.abort_all_tasks()
                self.assertFalse(
                    result,
                    "CRASH | CRITICAL | WARN messages found in cb_logs")
            self.sleep(60)
            count += 1

    def check_warmup_complete(self, server):
        for bucket in self.bucket_util.buckets:
            start_time = time.time()
            result = self.bucket_util._wait_warmup_completed(
                                [server],
                                self.bucket_util.buckets[0],
                                wait_time=self.wait_timeout * 20)
            if not result:
                self.stop_crash = True
                self.task.jython_task_manager.abort_all_tasks()
                self.assertTrue(result, "Warm-up failed in %s seconds"
                                % (self.wait_timeout * 20))
            else:
                self.log.info("Bucket:%s warm-up completed in %s." %
                              (bucket.name, str(time.time() - start_time)))

    def perform_rollback(self, start=None, mem_only_items=100000,
                         doc_type="create", kill_rollback=1):
        if not self.rollback:
            return
        rollbacks = self.input.param("rollbacks", 2)
#         mem_only_items = random.randint(mem_only_items, mem_only_items*2)
        _iter = 0
        self.gen_create, self.gen_update, self.gen_delete, self.gen_expiry = [None]*4
        while _iter < rollbacks:
            self.PrintStep("Rollback with %s: %s" % (doc_type,
                                                     str(_iter)))
            tasks_info = dict()
            node = self.cluster.nodes_in_cluster[0]
            # Stopping persistence on NodeA
            mem_client = MemcachedClientHelper.direct_client(
                node, self.bucket_util.buckets[0])
            mem_client.stop_persistence()

            shell = RemoteMachineShellConnection(node)
            cbstats = Cbstats(shell)
            target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                  name)
            shell.disconnect()

            gen_docs = doc_generator(
                self.key_prefix,
                start, mem_only_items,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size)

            if doc_type == "create":
                self.gen_create = gen_docs
            if doc_type == "update":
                self.gen_update = gen_docs
            if doc_type == "delete":
                self.gen_delete = gen_docs
            if doc_type == "expiry":
                self.gen_expiry = gen_docs
                if self.maxttl == 0:
                    self.maxttl = self.input.param("maxttl", 10)
                doc_type = "update"

            task = self.perform_load(wait_for_load=False)
            self.wait_for_doc_load_completion(task, wait_for_stats=False)
            ep_queue_size_map = {node: mem_only_items *
                                 self.num_scopes *
                                 self.num_collections}
            vb_replica_queue_size_map = {node: 0}

            for server in self.cluster.nodes_in_cluster:
                if server.ip != node.ip:
                    ep_queue_size_map.update({server: 0})
                    vb_replica_queue_size_map.update({server: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map,
                                                timeout=3600)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size",
                    timeout=3600)

            self.kill_memcached(num_kills=kill_rollback)

            self.bucket_util.verify_stats_all_buckets(self.final_items,
                                                      timeout=3600)

            self.print_stats()
            _iter += 1

    def pause_rebalance(self):
        rest = RestConnection(self.cluster.master)
        i = 1
        self.sleep(10, "Let the rebalance begin!")
        expected_progress = 20
        while expected_progress < 100:
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))
            if not RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Stop the rebalance")
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, msg="Unable to stop rebalance")
                rebalance_task = self.task.async_rebalance(self.cluster.nodes_in_cluster,
                                                           [], [],
                                                           retry_get_process_num=100)
                self.sleep(10, "Rebalance % ={}. Let the rebalance begin!".
                           format(expected_progress))
            i += 1
        return rebalance_task

    def abort_rebalance(self, rebalance, error_type="kill_memcached"):
        self.sleep(30, "Let the rebalance begin!")
        rest = RestConnection(self.cluster.master)
        i = 1
        expected_progress = 20
        rebalance_task = rebalance
        while expected_progress < 80:
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress,
                                                         wait_step=10)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))

            if not RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Abort rebalance")
                self._induce_error(error_type)
                result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
                if result:
                    self.task.jython_task_manager.abort_all_tasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                self.sleep(60, "Sleep after error introduction")
                self._recover_from_error(error_type)
                result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
                if result:
                    self.task.jython_task_manager.abort_all_tasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                try:
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                except RebalanceFailedException:
                    pass
                if rebalance.result:
                    self.log.error("Rebalance passed/finished which is not expected")
                    self.log.info("Rebalance % after rebalance finished = {}".
                                  format(expected_progress))
                    break
                else:
                    self.log.info("Restarting Rebalance after killing at {}".
                                  format(expected_progress))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.nodes_in_cluster, [], self.servs_out,
                        retry_get_process_num=500)
                    self.sleep(120, "Let the rebalance begin after abort")
                    self.log.info("Rebalance % = {}".
                                  format(self.rest._rebalance_progress()))
            i += 1
        return rebalance_task

    def PrintStep(self, msg=None):
        print "\n"
        print "\t", "#"*60
        print "\t", "#"
        print "\t", "#  %s" % msg
        print "\t", "#"
        print "\t", "#"*60
        print "\n"

    def ClusterOpsVolume(self):
        #######################################################################
        def end_step_checks(tasks):
            self.wait_for_doc_load_completion(tasks)
            self.data_validation()

            self.print_stats()
            result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
            if result:
                self.stop_crash = True
                self.task.jython_task_manager.abort_all_tasks()
                self.assertFalse(
                    result,
                    "CRASH | CRITICAL | WARN messages found in cb_logs")

        self.loop = 0
        while self.loop < self.iterations:
            '''
            Create sequential: 0 - 10M
            Final Docs = 10M (0-10M, 10M seq items)
            '''
            self.PrintStep("Step 3: Create %s items sequentially" % self.num_items)
            self.generate_docs(doc_ops="create")
            self.perform_load(validate_data=False)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M

            This Step:
            Create Random: 0 - 20M

            Final Docs = 30M (0-20M, 20M Random)
            Nodes In Cluster = 3
            '''
            temp = self.key_prefix
            self.key_prefix = "random_keys"
            self.create_perc = 100*2
            self.PrintStep("Step 4: Create %s random keys" %
                           str(self.num_items*self.update_perc/100))

            self.generate_docs(doc_ops="create")
            self.perform_load(validate_data=False)

            self.key_prefix = temp
            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 20M

            This Step:
            Update Sequential: 0 - 10M
            Update Random: 0 - 20M to create 50% fragmentation

            Final Docs = 30M (0-20M, 20M Random)
            Nodes In Cluster = 3
            '''

            temp = self.key_prefix
            self.key_prefix = "random_keys"
            self.update_perc = 100*2
            self.PrintStep("Step 5: Update %s random keys to create 50 percent\
             fragmentation" % str(self.num_items*self.update_perc/100))
            self.generate_docs(doc_ops="update")
            self.perform_load(validate_data=False)

            self.key_prefix = temp
            self.update_perc = 100
            self.generate_docs(doc_ops="update")
            self.perform_load(validate_data=False)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 20M

            This Step:
            Create Random: 20 - 30M
            Delete Random: 10 - 20M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 4

            Final Docs = 30M (Random: 0-10M, 20-30M, Sequential: 0-10M)
            Nodes In Cluster = 4
            '''

            self.PrintStep("Step 6: Rebalance in with Loading of docs")

            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)

            self.key_prefix = "random_keys"
            self.create_perc = 100
            self.update_perc = 100
            self.delete_perc = 50
            self.expiry_perc = 50
            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks)
            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 20 - 30M

            This Step:
            Create Random: 30 - 40M
            Delete Random: 20 - 30M
            Update Random: 0 - 10M
            Nodes In Cluster = 4 -> 3

            Final Docs = 30M (Random: 0-10M, 30-40M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 7: Rebalance Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)

            self.create_perc = 100
            self.update_perc = 100
            self.delete_perc = 100
            self.expiry_perc = 0
            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 30 - 40M

            This Step:
            Create Random: 40 - 50M
            Delete Random: 30 - 40M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 4

            Final Docs = 30M (Random: 0-10M, 40-50M, Sequential: 0-10M)
            Nodes In Cluster = 4
            '''
            self.PrintStep("Step 8: Rebalance In_Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1)

            self.create_perc = 100
            self.update_perc = 100
            self.delete_perc = 0
            self.expiry_perc = 100
            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 40 - 50M

            This Step:
            Create Random: 50 - 60M
            Delete Random: 40 - 50M
            Update Random: 0 - 10M
            Nodes In Cluster = 4 -> 4 (SWAP)

            Final Docs = 30M (Random: 0-10M, 50-60M, Sequential: 0-10M)
            Nodes In Cluster = 4
            '''
            self.PrintStep("Step 9: Swap with Loading of docs")

            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)

            self.create_perc = 100
            self.update_perc = 100
            self.delete_perc = 50
            self.expiry_perc = 50
            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 50 - 60M

            This Step:
            Create Random: 60 - 70M
            Delete Random: 50 - 60M
            Update Random: 0 - 10M
            Nodes In Cluster = 4 -> 3

            Final Docs = 30M (Random: 0-10M, 60-70M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 10: Failover a node and RebalanceOut that node \
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
            self.create_perc = 100
            self.update_perc = 100
            self.delete_perc = 0
            self.expiry_perc = 100
            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks_info = self.data_load()
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.assertTrue(self.rest.monitorRebalance(), msg="Failover -> Rebalance failed")
            self.nodes = self.rest.node_statuses()
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                ejectedNodes=[self.chosen[0].id])
            self.assertTrue(self.rest.monitorRebalance(), msg="Rebalance failed")

            servs_out = [node for node in self.cluster.servers
                         if node.ip == self.chosen[0].ip]
            self.cluster.nodes_in_cluster = list(
                set(self.cluster.nodes_in_cluster) - set(servs_out))
            self.available_servers += servs_out
            end_step_checks(tasks_info)

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
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster_util.vbuckets)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 60 - 70M

            This Step:
            Create Random: 70 - 80M
            Delete Random: 60 - 70M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 3

            Final Docs = 30M (Random: 0-10M, 70-80M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 11: Failover a node and FullRecovery\
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

            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks_info = self.data_load()
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.assertTrue(self.rest.monitorRebalance(), msg="Failover -> Rebalance failed")
            # Mark Node for full recovery
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="full")

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [],
                retry_get_process_num=100)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks_info)

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
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster_util.vbuckets)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 70 - 80M

            This Step:
            Create Random: 80 - 90M
            Delete Random: 70 - 80M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 3

            Final Docs = 30M (Random: 0-10M, 80-90M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 12: Failover a node and DeltaRecovery that \
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

            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            tasks_info = self.data_load()
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
                self.cluster.servers[:self.nodes_init], [], [],
                retry_get_process_num=100)
            self.set_num_writer_and_reader_threads(
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks_info)

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
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster_util.vbuckets)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 80 - 90M

            This Step:
            Create Random: 90 - 100M
            Delete Random: 80 - 90M
            Update Random: 0 - 10M
            Replica 1 - > 2

            Final Docs = 30M (Random: 0-10M, 90-100M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 13: Updating the bucket replica to 2")

            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=2)

            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            tasks_info = self.data_load()

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks_info)

            ####################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 90 - 100M

            This Step:
            Create Random: 100 - 110M
            Delete Random: 90 - 100M
            Update Random: 0 - 10M
            Replica 2 - > 1

            Final Docs = 30M (Random: 0-10M, 100-110M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 14: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=1)
            self.generate_docs(doc_ops=["create", "update", "delete", "expiry"])
            self.set_num_writer_and_reader_threads(
                num_writer_threads=self.new_num_writer_threads,
                num_reader_threads=self.new_num_reader_threads)
            rebalance_task = self.task.async_rebalance(self.cluster.servers,
                                                       [], [],
                                                       retry_get_process_num=100)
            tasks_info = self.data_load()

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks(tasks_info)

        #######################################################################
            self.PrintStep("Step 15: Flush the bucket and \
            start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                result = self.bucket_util.flush_all_buckets(self.cluster.master)
                self.assertTrue(result, "Flush bucket failed!")
                self.sleep(300)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    nodes_cluster = self.cluster.nodes_in_cluster[:]
                    nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(
                        nodes_cluster,
                        int(len(self.cluster.nodes_in_cluster)
                            - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out,
                        retry_get_process_num=100)

                    self.task.jython_task_manager.get_task_result(
                        rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")

                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(
                        set(self.cluster.nodes_in_cluster) - set(servs_out))
            else:
                self.log.info("Volume Test Run Complete")

    def SteadyStateVolume(self):
        check_dump_th = threading.Thread(target=self.check_dump)
        check_dump_th.start()
        self.loop = 1
        while self.loop <= self.iterations:
            #######################################################################
            '''
            creates: 0 - 10M
            deletes: 0 - 10M
            Final Docs = 0
            '''
            self.PrintStep("Step 3: Create %s items and Delete everything. \
            checkout fragmentation" % str(self.num_items*self.create_perc/100))
            self.create_perc = 100
            self.delete_perc = 100
            self.generate_docs(doc_ops="create")
            self.perform_load(validate_data=False)
            if self.end_step == 2:
                exit(2)
            self.generate_docs(doc_ops="delete",
                               delete_start=self.start,
                               delete_end=self.end)
            self.suppress_error_table = True
            self.perform_load(validate_data=True)
            self.suppress_error_table = False
            if self.end_step == 3:
                exit(3)
            '''
            fragmentation at this time: 0
            '''
            #######################################################################
            '''
            creates: 0 - 10M
            creates: 0 - 10M
            Final Docs = 20M (0-20M)
            '''
            self.create_perc = 200
            self.PrintStep("Step 4: Load %s items, sequential keys" %
                           str(self.num_items*self.create_perc/100))
            self.generate_docs(doc_ops="create",
                               create_start=self.start,
                               create_end=self.end*self.create_perc/100)
            self.perform_load(validate_data=False)
            if self.end_step == 4:
                exit(4)
            '''
            fragmentation at this time: 0, total data: 2X, stale: 0
            '''
            #######################################################################
            '''
            update: 0 - 1M * 10
            Final Docs = 20M (0-20M)
            '''
            self.update_perc = 100
            self.PrintStep("Step 5: Update the first set of %s percent (%s) items \
            %s times" % (str(self.update_perc),
                         str(self.num_items*self.update_perc/100),
                         str(self.step_iterations)))
            _iter = 0
            while _iter < self.step_iterations:
                self.PrintStep("Step 5.%s: Update the first set of %s percent (%s) \
                items %s times" % (str(_iter), str(self.update_perc),
                                   str(self.num_items*self.update_perc/100),
                                   str(self.step_iterations)))
                self.generate_docs(doc_ops="update")
                self.perform_load(crash=False, validate_data=True)
                _iter += 1
            if self.end_step == 5:
                exit(5)
            '''
            fragmentation at this time: 50, total data: 2X, stale: X
            '''
            #######################################################################
            '''
            Reverse Update: 10M - 9M
            Final Docs = 20M (0-20M)
            '''
            _iter = 0
            self.update_perc = 100
            self.PrintStep("Step 6: Reverse Update last set of %s percent (%s-%s) \
            items %s times" % (str(self.update_perc), str(self.num_items-1),
                               str(self.num_items+1 -
                                   self.num_items*self.update_perc/100),
                               str(self.step_iterations)))
            while _iter < self.step_iterations:
                self.PrintStep("Step 6.%s: Reverse Update last set of %s percent \
                (%s-%s) items %s times" % (str(_iter), str(self.update_perc),
                                           str(self.num_items+1),
                                           str(self.num_items+1 -
                                               self.num_items*self.update_perc/100),
                                           str(self.step_iterations)))
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
            '''
            Create Random: 0 - 10M
            Final Docs = 30M (0-20M, 10M Random)
            '''
            temp = self.key_prefix
            self.key_prefix = "random_keys"
            self.create_perc = 100
            self.PrintStep("Step 7: Create %s random keys" %
                           str(self.num_items*self.create_perc/100))
            self.generate_docs(doc_ops="create")
            self.perform_load(crash=False, validate_data=True)
            self.key_prefix = temp
            '''
            fragmentation: 50, total data: 3X, stale: X
            '''
            #######################################################################
            '''
            Update Random: 0 - 10M
            Final Docs = 30M (0-20M, 10M Random)
            '''
            _iter = 0
            self.update_perc = 100
            self.key_prefix = "random_keys"
            self.PrintStep("Step 8: Update all %s random items %s times" %
                           (str(self.num_items*self.update_perc/100),
                            str(self.step_iterations)))
            while _iter < self.step_iterations:
                self.PrintStep("Step 8.%s: Update all %s random items %s times" %
                               (str(_iter),
                                str(self.num_items*self.update_perc/100),
                                str(self.step_iterations)))
                self.generate_docs(doc_ops="update",
                                   update_start=self.start,
                                   update_end=self.end*self.update_perc/100)
                self.perform_load(crash=False, validate_data=True)
                _iter += 1
            self.key_prefix = temp
            if self.end_step == 8:
                exit(8)
            '''
            fragmentation: 50, total data: 3X, stale: 1.5X
            '''
            #######################################################################
            '''
            Delete Random: 0 - 10M
            Create Random: 0 - 10M
            Final Docs = 30M (0-20M, 10M Random)
            '''
            self.key_prefix = "random_keys"
            self.delete_perc = 100
            self.PrintStep("Step 9: Delete/Re-Create all %s random items" %
                           str(self.num_items*self.delete_perc/100))
            self.generate_docs(doc_ops="delete")
            self.suppress_error_table = True
            self.perform_load(crash=False, validate_data=True)
            self.suppress_error_table = False
            '''
            fragmentation: 50, total data: 3X, stale: 1.5X
            '''
            self.generate_docs(doc_ops="create",
                               create_start=self.start,
                               create_end=self.end)
            self.perform_load(crash=False, validate_data=True)
            self.key_prefix = temp
            if self.end_step == 9:
                exit(9)
            #######################################################################
            '''
            Update: 0 - 1M
            Final Docs = 30M (0-20M, 10M Random)
            '''
            self.update_perc = 10
            self.PrintStep("Step 10: Update %s percent(%s) items %s times and \
            crash during recovery" % (str(self.update_perc),
                                      str(self.num_items*self.update_perc/100),
                                      str(self.step_iterations)))
            _iter = 0
            while _iter < self.step_iterations and self.crashes:
                self.PrintStep("Step 10.%s: Update %s percent(%s) items %s times \
                and crash during recovery" % (str(_iter), str(self.update_perc),
                                              str(self.num_items *
                                                  self.update_perc/100),
                                              str(self.step_iterations)))
                self.generate_docs(doc_ops="update")
                self.perform_load(crash=True, validate_data=True)
                _iter += 1
            self.bucket_util._wait_for_stats_all_buckets(timeout=1200)
            if self.end_step == 10:
                exit(10)
            #######################################################################
            self.PrintStep("Step 11: Normal Rollback with creates")
            '''
            Final Docs = 30M (0-20M, 10M Random)
            '''
            mem_only_items = self.input.param("rollback_items", 100000)
            self.perform_rollback(self.final_items, mem_only_items,
                                  doc_type="create")
            if self.end_step == 11:
                exit(11)
            #######################################################################
            self.PrintStep("Step 12: Normal Rollback with updates")
            '''
            Final Docs = 30M (0-20M, 10M Random)
            '''
            self.perform_rollback(0, mem_only_items, doc_type="update")
            if self.end_step == 12:
                exit(12)
            #######################################################################
            self.PrintStep("Step 13: Drop a collection")
            total_collections = self.num_collections
            total_scopes = self.num_scopes
            drop = 0
            for bucket in self.bucket_util.buckets:
                for scope in bucket.scopes.keys():
                    drop = 0
                    for collection in bucket.scopes[scope].collections.keys()[1:total_collections:2]:
                        self.bucket_util.drop_collection(self.cluster.master,
                                                         bucket,
                                                         scope,
                                                         collection)
                        bucket.scopes[scope].collections.pop(collection)
                        drop += 1
            self.num_collections = self.num_collections - drop
            self.bucket_util._wait_for_stats_all_buckets()
            self.final_items = self.final_items * (self.num_collections)/total_collections
            self.log.info("Expected items after dropping collections: {}".
                          format(self.final_items))
            self.bucket_util.verify_stats_all_buckets(self.final_items,
                                                      timeout=3600)
            if self.end_step == 13:
                exit(13)
            #######################################################################
            self.PrintStep("Step 14: Normal Rollback with deletes")
            '''
            Final Docs = 30M (0-20M, 10M Random)
            '''
            self.perform_rollback(0, mem_only_items, doc_type="delete")
            if self.end_step == 14:
                exit(14)
            #######################################################################
            self.PrintStep("Step 15: Normal Rollback with expiry")
            '''
            Final Docs = 30M (0-20M, 10M Random)
            '''
            self.perform_rollback(0, mem_only_items, doc_type="expiry")
            if self.end_step == 15:
                exit(15)
            #######################################################################
            self.skip_read_on_error = True
            self.suppress_error_table = True
            self.track_failures = False
            if self.crashes:
                self.PrintStep("Step 16: Random crashes during CRUD-Expiry")
                '''
                Creates: 20M-50M
                Final Docs = 60M (0-50M, 10M Random)
                Updates: 0M-20M
                Final Docs = 60M (0-50M, 10M Random)
                Deletes: 0M-20M
                Final Docs = 40M (20-50M, 10M Random)
                Expiry: 0M-20M , MAXTTL=5s
                Final Docs = 40M (20-50M, 10M Random)
                '''
                self.create_perc = 300
                self.update_perc = 200
                self.delete_perc = 200
                self.expiry_perc = 200
                self.generate_docs(doc_ops="create;update;delete;expiry",
                                   delete_start=0,
                                   delete_end=self.num_items*self.delete_perc/100,
                                   expire_start=0,
                                   expire_end=self.num_items*self.expiry_perc/100)
                task = self.data_load()
                th = threading.Thread(target=self.crash_thread,
                                      kwargs={"graceful": False})
                th.start()
                self.task_manager.get_task_result(task)
                self.stop_crash = True
                th.join()
                if self.end_step == 16:
                    exit(16)
                #######################################################################
                self.skip_read_on_error = True
                self.suppress_error_table = True
                self.track_failures = False
                self.PrintStep("Step 17: Random crashes during CRUD-Expiry")
                '''
                Creates: 50M-80M
                Final Docs = 90M (0-80M, 10M Random)
                Updates: 0M-20M
                Final Docs = 90M (0-80M, 10M Random)
                Deletes: 0M-20M
                Final Docs = 70M (20-90M, 10M Random)
                Expiry: 0M-20M , MAXTTL=5s
                Final Docs = 40M (20-50M, 10M Random)
                '''
                self.create_perc = 300
                self.update_perc = 200
                self.delete_perc = 150
                self.expiry_perc = 150
                self.generate_docs(doc_ops="create;update;delete;expiry")
                task = self.data_load()
                th = threading.Thread(target=self.crash_thread,
                                      kwargs={"graceful": False,
                                              "num_kills": 20})
                th.start()
                self.task_manager.get_task_result(task)
                self.stop_crash = True
                th.join()
                if self.end_step == 17:
                    exit(17)
            #######################################################################
            for i in range(1, total_scopes-1, 2):
                scope = self.scope_prefix + str(i)
                for i in range(1, total_collections-1, 2):
                    collection = self.collection_prefix + str(i)
                    self.bucket_util.create_collection(self.cluster.master,
                                                       self.bucket,
                                                       self.scope,
                                                       {"name": collection})
                    self.sleep(0.5)
            self.bucket_util.flush_all_buckets(self.cluster.master)
            self.init_doc_params()
            self.sleep(300, "Iteration %s completed successfully !!!" % self.loop)
            self.loop += 1
            if self.end_step == 18:
                exit(18)

    def SystemTestMagma(self):
        #######################################################################
        self.loop = 1
        self.skip_read_on_error = True
        self.suppress_error_table = True
        self.track_failures = False
        self.crash_count = 0
        self.stop_rebalance = self.input.param("pause_rebalance", False)
        self.crashes = self.input.param("crashes", 20)

        self.PrintStep("Step 3: Create %s items sequentially" % self.num_items)
        self.expiry_perc = 100
        self.create_perc = 100
        self.update_perc = 100
        self.delete_perc = 100
        self.key_prefix = "random"

        self.doc_ops = self.input.param("doc_ops", "expiry")
        self.generate_docs(doc_ops=self.doc_ops,
                           expire_start=0,
                           expire_end=self.num_items,
                           create_start=self.num_items,
                           create_end=self.num_items*2,
                           update_start=self.num_items*2,
                           update_end=self.num_items*3
                           )
        self.perform_load(wait_for_load=False)
        self.sleep(300)
        while self.loop <= self.iterations:
            ###################################################################
            self.PrintStep("Step 4: Rebalance in with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 5: Rebalance Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 6: Rebalance In_Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1)

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 7: Swap with Loading of docs")

            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 8: Failover a node and RebalanceOut that node \
            with loading in parallel")

            # Chose node to failover
            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Failover Node
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.rest.monitorRebalance()

            # Rebalance out failed over node
            self.otpNodes = self.rest.node_statuses()
            self.rest.rebalance(otpNodes=[otpNode.id for otpNode in self.otpNodes],
                                ejectedNodes=[self.chosen[0].id])
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True),
                            msg="Rebalance failed")

            # Maintain nodes availability
            servs_out = [node for node in self.cluster.servers
                         if node.ip == self.chosen[0].ip]
            self.cluster.nodes_in_cluster = list(
                set(self.cluster.nodes_in_cluster) - set(servs_out))
            self.available_servers += servs_out
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 9: Failover a node and FullRecovery\
             that node")

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.rest.monitorRebalance()

            # Mark Node for full recovery
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="full")

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [],
                retry_get_process_num=100)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 10: Failover a node and DeltaRecovery that \
            node with loading in parallel")

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(10)
            self.rest.monitorRebalance()
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="delta")

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [],
                retry_get_process_num=100)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 12: Updating the bucket replica to 2")

            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=2)

            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 13: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=1)

            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, [], [],
                retry_get_process_num=100)
            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_thread,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 14: Start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    nodes_cluster = self.cluster.nodes_in_cluster[:]
                    nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(
                        nodes_cluster,
                        int(len(self.cluster.nodes_in_cluster)
                            - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out,
                        retry_get_process_num=100)

                    self.task.jython_task_manager.get_task_result(
                        rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(
                        set(self.cluster.nodes_in_cluster) - set(servs_out))

            self.print_stats()

        self.log.info("Volume Test Run Complete")
        self.task_manager.abort_all_tasks()

    def test_long_rebalance(self):
        #######################################################################
        self.loop = 1
        self.skip_read_on_error = True
        self.suppress_error_table = True
        self.track_failures = False
        self.crash_count = 0
        self.stop_rebalance = self.input.param("pause_rebalance", False)

        self.PrintStep("Step 1: Create %s items sequentially" % self.num_items)
        self.expiry_perc = 100
        self.create_perc = 100
        self.update_perc = 100
        self.delete_perc = 100
#         self.key_prefix = "random"

        self.doc_ops = self.input.param("doc_ops", "expiry")
        self.generate_docs(doc_ops=self.doc_ops,
                           expire_start=0,
                           expire_end=self.num_items,
                           create_start=self.num_items,
                           create_end=self.num_items*2,
                           update_start=self.num_items*2,
                           update_end=self.num_items*3
                           )
        self.perform_load(wait_for_load=False)
        for bucket in self.bucket_util.buckets:
            dgm = self.get_bucket_dgm(bucket)
            while self.dgm and dgm > self.dgm:
                self.sleep(300)
                dgm = self.get_bucket_dgm(bucket)
        while self.loop <= self.iterations:
            ###################################################################
            self.PrintStep("Step 4.{}: Rebalance IN with Loading of docs".
                           format(self.loop))
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0,
                                            retry_get_process_num=300)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()
            self.sleep(60, "Sleep for 60s after rebalance")

            self.PrintStep("Step 5.{}: Rebalance OUT with Loading of docs".
                           format(self.loop))
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1,
                                            retry_get_process_num=3000)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()
            self.sleep(60, "Sleep for 60s after rebalance")

            self.PrintStep("Step 6.{}: Rebalance SWAP with Loading of docs".
                           format(self.loop))
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1,
                                            retry_get_process_num=3000)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()
            self.sleep(60, "Sleep for 60s after rebalance")

            self.PrintStep("Step 7.{}: Rebalance IN/OUT with Loading of docs".
                           format(self.loop))
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1,
                                            retry_get_process_num=3000)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()
            self.sleep(60, "Sleep for 60s after rebalance")

            self.PrintStep("Step 8.{}: Rebalance OUT/IN with Loading of docs".
                           format(self.loop))
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=2,
                                            retry_get_process_num=3000)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()
            self.sleep(60, "Sleep for 60s after rebalance")

            self.loop += 1
