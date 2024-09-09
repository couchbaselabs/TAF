import math
import os
import random
import subprocess
import time
import copy

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from cb_constants import DocLoading
from cb_constants.CBServer import CbServer
from couchbase_helper.documentgenerator import doc_generator, SubdocDocumentGenerator
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
from sdk_constants.java_client import SDKConstants
from Jython_tasks.task import PrintBucketStats
from shell_util.remote_connection import RemoteMachineShellConnection


class StorageBase(BaseTestCase):
    def setUp(self):
        super(StorageBase, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.windows_platform = False
        self.data_path = self.fetch_data_path()
        self._data_validation = self.input.param("data_validation", True)

        # Bucket Params
        self.vbuckets = self.input.param("vbuckets", self.cluster.vbuckets)
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.magma)
        self.bucket_eviction_policy = self.input.param("bucket_eviction_policy",
                                                       Bucket.EvictionPolicy.FULL_EVICTION)
        self.bucket_util.add_rbac_user(self.cluster.master)
        self.bucket_name = self.input.param("bucket_name", None)
        self.magma_buckets = self.input.param("magma_buckets", 0)
        self.change_magma_quota = self.input.param("change_magma_quota", False)

        # SDK Exceptions
        self.check_temporary_failure_exception = False
        self.retry_exceptions = [SDKException.TimeoutException,
                                 SDKException.AmbiguousTimeoutException,
                                 SDKException.RequestCanceledException,
                                 SDKException.UnambiguousTimeoutException,
                                 SDKException.ServerOutOfMemoryException,
                                 SDKException.DurabilityAmbiguousException]
        self.ignore_exceptions = []

        # Sets autocompaction at bucket level
        self.autoCompactionDefined = str(self.input.param("autoCompactionDefined", "false")).lower()

        # Create Cluster
        self.rest.init_cluster(username=self.cluster.master.rest_username,
                               password=self.cluster.master.rest_password)

        nodes_init = self.cluster.servers[1:self.nodes_init]
        self.services = ["kv"] * self.nodes_init

        self.dcp_services = self.input.param("dcp_services", None)
        self.dcp_servers = []
        if self.dcp_services:
            server = self.rest.get_nodes_self()
            self.rest.set_service_mem_quota(
                {CbServer.Settings.INDEX_MEM_QUOTA: int(server.mcdMemoryReserved - 100)})
            self.dcp_services = [service.replace(":", ",") for service in self.dcp_services.split("-")]
            self.services.extend(self.dcp_services)
            self.dcp_servers = self.cluster.servers[self.nodes_init:
                                                    self.nodes_init+len(self.dcp_services)]
        nodes_in = nodes_init + self.dcp_servers
        self.init_rebalance_skip = self.input.param("init_rebalance_skip", False)
        if not self.init_rebalance_skip:
            result = self.task.rebalance(self.cluster, nodes_in, [],
                                         services=self.services[1:])
            self.assertTrue(result, "Initial rebalance failed")
        for idx, node in enumerate(self.cluster.nodes_in_cluster):
            node.services = self.services[idx]

        self.cluster.query_nodes = self.find_nodes_with_service("n1ql", nodes_in)
        self.cluster.index_nodes = self.find_nodes_with_service("index", nodes_in)
        self.cluster.kv_nodes = self.find_nodes_with_service("kv", nodes_in).insert(0,self.cluster.master)

        self.set_kv_quota = self.input.param("set_kv_quota", False)
        if self.set_kv_quota:
            self.kv_quota_mem = self.input.param("kv_quota_mem", None)
            self.log.info("Setting KV Memory quota to {0} MB".format(self.kv_quota_mem))
            self.rest.set_service_mem_quota(
                {CbServer.Settings.KV_MEM_QUOTA: self.kv_quota_mem})
        # Create Buckets
        if self.standard_buckets == 1:
            self.bucket_util.create_default_bucket(
                self.cluster,
                bucket_type=self.bucket_type,
                ram_quota=self.bucket_ram_quota,
                replica=self.num_replicas,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy,
                autoCompactionDefined=self.autoCompactionDefined,
                fragmentation_percentage=self.fragmentation,
                flush_enabled=self.flush_enabled,
                magma_key_tree_data_block_size=self.magma_key_tree_data_block_size,
                magma_seq_tree_data_block_size=self.magma_seq_tree_data_block_size,
                history_retention_collection_default=self.bucket_collection_history_retention_default,
                history_retention_seconds=self.bucket_dedup_retention_seconds,
                history_retention_bytes=self.bucket_dedup_retention_bytes,
                weight=self.bucket_weight, width=self.bucket_width)
        else:
            buckets_created = self.bucket_util.create_multiple_buckets(
                self.cluster,
                self.num_replicas,
                bucket_count=self.standard_buckets,
                bucket_type=self.bucket_type,
                storage={"couchstore": self.standard_buckets - self.magma_buckets,
                         "magma": self.magma_buckets},
                eviction_policy=self.bucket_eviction_policy,
                bucket_name=self.bucket_name,
                ram_quota=self.bucket_ram_quota,
                fragmentation_percentage=self.fragmentation,
                flush_enabled=self.flush_enabled,
                weight=self.bucket_weight, width=self.bucket_width,
                autoCompactionDefined=self.autoCompactionDefined,
                history_retention_collection_default=self.bucket_collection_history_retention_default,
                history_retention_seconds=self.bucket_dedup_retention_seconds,
                history_retention_bytes=self.bucket_dedup_retention_bytes)
            self.assertTrue(buckets_created, "Unable to create multiple buckets")
        if self.change_magma_quota:
            bucket_helper = BucketHelper(self.cluster.master)
            bucket_helper.set_magma_quota_percentage()
            self.sleep(30, "Sleep while magma storage quota setting is taking effect")

        self.buckets = self.cluster.buckets
        # Setting reader and writer threads
        self.num_writer_threads = self.input.param("num_writer_threads", "disk_io_optimized")
        self.num_reader_threads = self.input.param("num_reader_threads", "disk_io_optimized")
        self.num_storage_threads = self.input.param("num_storage_threads", "default")
        self.set_writer_reader_storage_threads()
        # sel.num_collections=1 signifies only default collection
        self.num_collections = self.input.param("num_collections", 1)
        self.num_scopes = self.input.param("num_scopes", 1)

        # SDK retry Strategy
        self.sdk_retry_strategy = self.input.param("sdk_retry_strategy",
                                                   SDKConstants.RetryStrategy.BEST_EFFORT)

        # Creation of scopes of num_scopes is > 1

        scope_prefix = "Scope"
        for bucket in self.cluster.buckets:
            for i in range(1, self.num_scopes):
                scope_name = scope_prefix + str(i)
                self.log.info("Creating bucket::scope {} {}\
                ".format(bucket.name, scope_name))
                self.bucket_util.create_scope(self.cluster.master,
                                              bucket,
                                              {"name": scope_name})
                self.sleep(2)
        self.scopes = self.buckets[0].scopes.keys()
        self.log.info("Scopes list is {}".format(self.scopes))

        collection_prefix = "FunctionCollection"
        # Creation of collection of num_collections is > 1
        for bucket in self.cluster.buckets:
            for scope_name in self.scopes:
                if scope_name == CbServer.system_scope:
                    continue
                for i in range(len(bucket.scopes[scope_name].collections), self.num_collections):
                    collection_name = collection_prefix + str(i)
                    self.log.info("Creating scope::collection {} {}\
                    ".format(scope_name, collection_name))
                    self.bucket_util.create_collection(
                        self.cluster.master, bucket,
                        scope_name, {"name": collection_name})
                    self.sleep(2)
                    if self.bucket_dedup_retention_seconds or self.bucket_dedup_retention_bytes:
                        if collection_name == "_default":
                            continue
                        self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                                                              bucket, scope_name,
                                                                              collection_name,
                                                                              "true")
        self.collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
        self.log.info("Collections list == {}".format(self.collections))

        if self.dcp_services and self.num_collections == 1:
            self.initial_idx = "initial_idx"
            self.initial_idx_q = "CREATE INDEX %s on default:`%s`.`%s`.`%s`(meta().id) with \
                {\"defer_build\": false};" % (self.initial_idx,
                                              self.buckets[0].name,
                                              CbServer.default_scope,
                                              self.collections[0])
            self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                     get_all_nodes=True)
            self.query_client = RestConnection(self.cluster.query_nodes[0])
            self.log.info("Query is: %s" % self.initial_idx_q)
            result = self.query_client.query_tool(self.initial_idx_q)
            self.assertTrue(result["status"] == "success", "Index query failed!")

        # Doc controlling params
        self.key = 'test_docs'
        self.key_size = self.input.param("key_size", 18)
        if self.random_key:
            self.key = "random_keys"
            '''
              With Small key size, when random.random() generate 0.0,
              Key size becomes bigger than the 250 bytes
              (L 259 in documentgenerator.py)
            '''
            self.key_size = self.input.param("key_size", 20)

        self.doc_ops = self.input.param("doc_ops", "create")
        self.sub_doc_ops = self.input.param("sub_doc_ops", "upsert")
        self.doc_size = self.input.param("doc_size", 2048)
        self.different_field = self.input.param("different_field", False)
        self.gen_create = None
        self.gen_delete = None
        self.gen_subdoc_upsert = None
        self.gen_read = None
        self.gen_update = None
        self.gen_expiry = None
        self.create_perc = 100
        self.read_perc = 0
        self.update_perc = 0
        self.upsert_perc = 0
        self.delete_perc = 0
        self.expiry_perc = 0
        self.start = 0
        self.end = 0
        self.create_start = 0
        self.create_end = 0
        self.update_start = 0
        self.update_end = 0
        self.delete_start = 0
        self.delete_end = 0
        self.read_start = 0
        self.read_end = 0
        self.expiry_start = 0
        self.expiry_end = 0
        self.mutate = 0
        self.validate = False
        self.key_type = self.input.param("key_type", "RandomKey")
        self.val_type = self.input.param("val_type", "SimpleValue")
        self.init_items_per_collection = self.num_items
        self.doc_loading_tm = None
        self.num_items_per_collection = copy.deepcopy(self.init_items_per_collection)
        '''
           --For DGM test
                  -self.init_items_per collection will overwrite in
                    load_buckets_in_dgm method

           --For Non-DGM tests in MultiCollection environment,
                  -self.num_items will be updated after doc loading

           -- self.init_num_items is needed to preserve initial
              doc count given in test
        '''
        self.init_num_items = self.num_items
        self.maxttl = self.input.param("maxttl", 10)

        # Common test params
        self.test_itr = self.input.param("test_itr", 4)
        self.update_itr = self.input.param("update_itr", 2)
        self.next_half = self.input.param("next_half", False)
        self.deep_copy = self.input.param("deep_copy", False)
        self.suppress_error_table = True
        self.skip_read_on_error = False
        self.track_failures = self.input.param("track_failures", True)

        if CbServer.cluster_profile == "serverless":
            # Workaround to hitting throttling on serverless config
            for bucket in self.cluster.buckets:
                self.bucket_util.set_throttle_n_storage_limit(bucket, throttle_limit=self.kv_throttling_limit)

    def find_nodes_with_service(self, service_type, nodes_list):
        filter_list = list()
        for node in nodes_list:
            if service_type in node.services:
                filter_list.append(node)

        return filter_list

    def _loader_dict(self, buckets=None, skip_default=False):
        loader_dict = dict()
        common_params = {"retry_exceptions": self.retry_exceptions,
                         "suppress_error_table": self.suppress_error_table,
                         "durability_level": self.durability_level,
                         "skip_read_success_results": False,
                         "target_items": 5000,
                         "skip_read_on_error": self.skip_read_on_error,
                         "track_failures": self.track_failures,
                         "ignore_exceptions": self.ignore_exceptions,
                         "sdk_timeout_unit": self.time_unit,
                         "sdk_timeout": self.sdk_timeout,
                         "doc_ttl": 0,
                         "doc_gen_type": "default",
                         "xattr_test":False}
        if buckets == None:
            buckets = self.cluster.buckets
        for bucket in buckets:
            loader_dict.update({bucket: dict()})
            loader_dict[bucket].update({"scopes": dict()})
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                if skip_default:
                    if scope == "_default":
                        continue
                loader_dict[bucket]["scopes"].update({scope: dict()})
                loader_dict[bucket]["scopes"][scope].update({"collections":dict()})
                for collection in bucket.scopes[scope].collections.keys():
                    loader_dict[bucket]["scopes"][scope]["collections"].update({collection:dict()})
                    if self.gen_update is not None:
                        op_type = DocLoading.Bucket.DocOps.UPDATE
                        common_params.update({"doc_gen": self.gen_update})
                        loader_dict[bucket]["scopes"][scope]["collections"][collection][op_type] = copy.deepcopy(common_params)
                    if self.gen_create is not None:
                        op_type = DocLoading.Bucket.DocOps.CREATE
                        common_params.update({"doc_gen": self.gen_create})
                        loader_dict[bucket]["scopes"][scope]["collections"][collection][op_type] = copy.deepcopy(common_params)
                    if self.gen_delete is not None:
                        op_type = DocLoading.Bucket.DocOps.DELETE
                        common_params.update({"doc_gen": self.gen_delete})
                        loader_dict[bucket]["scopes"][scope]["collections"][collection][op_type] = copy.deepcopy(common_params)
                    if self.gen_subdoc_upsert is not None:
                        op_type = DocLoading.Bucket.SubDocOps.INSERT
                        common_params.update({"doc_gen": self.gen_subdoc_upsert})
                        loader_dict[bucket]["scopes"][scope]["collections"][collection][op_type] = copy.deepcopy(common_params)
                    if self.gen_expiry is not None and self.maxttl:
                        op_type = DocLoading.Bucket.DocOps.UPDATE
                        common_params.update({"doc_gen": self.gen_expiry,
                                              "doc_ttl": self.maxttl})
                        loader_dict[bucket]["scopes"][scope]["collections"][collection][op_type] = copy.deepcopy(common_params)
                        #common_params.update({"doc_ttl": 0})
                    if self.gen_read is not None:
                        op_type = DocLoading.Bucket.DocOps.READ
                        common_params.update({"doc_gen": self.gen_read,
                                              "skip_read_success_results": True,
                                              "track_failures": False,
                                              "suppress_error_table": True})
                        loader_dict[bucket]["scopes"][scope]["collections"][collection][op_type] = common_params
        self.loader_dict = loader_dict

    def doc_loader(self, loader_spec):
        task = self.task.async_load_gen_docs_from_spec(
            self.cluster, self.task_manager, loader_spec,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            print_ops_rate=True,
            start_task=True,
            track_failures=self.track_failures)

        return task

    def data_load(self, buckets=None, skip_default=False):
        self._loader_dict(buckets, skip_default)
        return self.doc_loader(self.loader_dict)

    def wait_for_doc_load_completion(self, task, wait_for_stats=True):
        self.task_manager.get_task_result(task)
        self.bucket_util.validate_doc_loading_results(self.cluster, task)
        if not task.result:
            self.assertTrue(task.result,
                            "Doc ops failed for task: {}".format(task.thread_name))

        if wait_for_stats:
            try:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1800)
            except Exception as e:
                raise e

    def _loader_dict_new(self, cmd={}, scopes=None, collections=None):
        self.loader_map = dict()
        for bucket in self.cluster.buckets:
            scopes_keys = scopes or bucket.scopes.keys()
            for scope in scopes_keys:
                if scope == CbServer.system_scope:
                    continue
                collections_keys = collections or bucket.scopes[scope].collections.keys()
                for collection in collections_keys:
                    if collection == "_default" and scope == "_default":
                        continue
                    ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                          cmd.get("keySize", self.key_size),
                                          cmd.get("docSize", self.doc_size),
                                          cmd.get("cr", self.create_perc),
                                          cmd.get("rd", self.read_perc),
                                          cmd.get("up", self.update_perc),
                                          cmd.get("dl", self.delete_perc),
                                          cmd.get("ex", self.expiry_perc),
                                          cmd.get("workers", self.process_concurrency),
                                          cmd.get("ops", self.ops_rate),
                                          cmd.get("loadType", None),
                                          cmd.get("keyType", None),
                                          cmd.get("valueType", None),
                                          cmd.get("validate", False),
                                          cmd.get("gtm", False),
                                          cmd.get("deleted", False),
                                          cmd.get("mutated", self.mutate)
                                          )
                    hm = HashMap()
                    hm.putAll({DRConstants.create_s: self.create_start,
                               DRConstants.create_e: self.create_end,
                               DRConstants.update_s: self.update_start,
                               DRConstants.update_e: self.update_end,
                               DRConstants.expiry_s: self.expiry_start,
                               DRConstants.expiry_e: self.expiry_end,
                               DRConstants.delete_s: self.delete_start,
                               DRConstants.delete_e: self.delete_end,
                               DRConstants.read_s: self.read_start,
                               DRConstants.read_e: self.read_end})
                    dr = DocRange(hm)
                    ws.dr = dr
                    dg = DocumentGenerator(ws, self.key_type, self.val_type)
                    self.loader_map.update({bucket.name+scope+collection: dg})

    def retry_failures(self, tasks, wait_for_stats=True):
        for task in tasks:
            task.result = True
            for optype, failures in task.failedMutations.items():
                for failure in failures:
                    print("Test Retrying {}: {} -> {}".format(optype, failure.id(), failure.err().getClass().getSimpleName()))
                    try:
                        if optype == "create":
                            task.docops.insert(failure.id(), failure.document(), task.sdk.connection, task.setOptions)
                        if optype == "update":
                            task.docops.upsert(failure.id(), failure.document(), task.sdk.connection, task.upsertOptions)
                        if optype == "delete":
                            task.docops.delete(failure.id(), task.sdk.connection, task.removeOptions)
                    except (ServerOutOfMemoryException, TimeoutException) as e:
                        print("Retry {} failed for key: {} - {}".format(optype, failure.id(), e))
                        task.result = False
                    except (DocumentNotFoundException, DocumentExistsException) as e:
                        pass
            try:
                task.sdk.disconnectCluster()
            except Exception as e:
                print(e)
            self.assertTrue(task.result, "Task Failed: {}".format(task.taskName))
        if wait_for_stats:
            try:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                if self.track_failures:
                    self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items_per_collection*self.num_scopes*(self.num_collections-1), timeout=300)
            except Exception as e:
                raise e

    def new_loader(self, cmd=dict(), wait=False, scopes=None, collections=None):
        self._loader_dict_new(cmd)
        self.doc_loading_tm = TaskManager(self.process_concurrency)
        self.ops_rate = self.input.param("ops_rate", 2000)
        master = Server(self.cluster.master.ip, self.cluster.master.port,
                        self.cluster.master.rest_username, self.cluster.master.rest_password,
                        str(self.cluster.master.memcached_port))
        tasks = list()
        i = self.process_concurrency
        for bucket in self.cluster.buckets:
            self.printOps = PrintBucketStats(self.cluster, bucket,
                                             monitor_stats=["doc_ops"], sleep=1)
            self.task_manager.add_new_task(self.printOps)
        while i > 0:
            for bucket in self.cluster.buckets:
                scopes_keys = scopes or bucket.scopes.keys()
                for scope in scopes_keys:
                    if scope == CbServer.system_scope:
                        continue
                    collections_keys = collections or bucket.scopes[scope].collections.keys()
                    for collection in collections_keys:
                        if collection == "_default" and scope == "_default":
                            continue
                        client = NewSDKClient(master, bucket.name, scope, collection)
                        client.initialiseSDK()
                        self.sleep(1)
                        taskName = "Loader_%s_%s_%s_%s_%s" % (bucket.name, scope, collection, str(i), time.time())
                        task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection],
                                                client, self.durability_level,
                                                self.maxttl, self.time_unit,
                                                self.track_failures, 0,
                                                self.sdk_retry_strategy)
                        tasks.append(task)
                        self.doc_loading_tm.submit(task)
                        i -= 1

        if wait:
            self.doc_loading_tm.getAllTaskResult()
            self.printOps.end_task()
            self.retry_failures(tasks)
        else:
            return tasks

    def data_validation(self, scopes=None, collections=None):
        doc_ops = self.doc_ops.split(":")
        if self._data_validation:
            self.log.info("Validating Active/Replica Docs")
            cmd = dict()
            self.ops_rate = self.input.param("ops_rate", 2000)
            master = Server(self.cluster.master.ip, self.cluster.master.port,
                            self.cluster.master.rest_username, self.cluster.master.rest_password,
                            str(self.cluster.master.memcached_port))
            self.loader_map = dict()
            for bucket in self.cluster.buckets:
                scopes_keys = scopes or bucket.scopes.keys()
                for scope in scopes_keys:
                    if scope == CbServer.system_scope:
                        continue
                    collections_keys = collections or bucket.scopes[scope].collections.keys()
                    self.log.info("scope is {}".format(scope))
                    for collection in collections_keys:
                        self.log.info("collection is {}".format(collection))
                        if collection == "_default" and scope == "_default":
                            continue
                        for op_type in doc_ops:
                            cmd.update({"deleted": False})
                            hm = HashMap()
                            if op_type == "create":
                                hm.putAll({DRConstants.read_s: self.create_start,
                                           DRConstants.read_e: self.create_end})
                            elif op_type == "update":
                                hm.putAll({DRConstants.read_s: self.update_start,
                                           DRConstants.read_e: self.update_end})
                            elif op_type == "delete":
                                hm.putAll({DRConstants.read_s: self.delete_start,
                                           DRConstants.read_e: self.delete_end})
                                cmd.update({"deleted": True})
                            else:
                                continue
                            dr = DocRange(hm)
                            ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                                  cmd.get("keySize", self.key_size),
                                                  cmd.get("docSize", self.doc_size),
                                                  cmd.get("cr", 0),
                                                  cmd.get("rd", 100),
                                                  cmd.get("up", 0),
                                                  cmd.get("dl", 0),
                                                  cmd.get("ex", 0),
                                                  cmd.get("workers", self.process_concurrency),
                                                  cmd.get("ops", self.ops_rate),
                                                  cmd.get("loadType", None),
                                                  cmd.get("keyType", None),
                                                  cmd.get("valueType", None),
                                                  cmd.get("validate", True),
                                                  cmd.get("gtm", False),
                                                  cmd.get("deleted", False),
                                                  cmd.get("mutated", self.mutate))
                            ws.dr = dr
                            dg = DocumentGenerator(ws, self.key_type, self.val_type)
                            self.loader_map.update({bucket.name+scope+collection+op_type: dg})
            self.log.info("loader_map is {}".format(self.loader_map))

            tasks = list()
            i = self.process_concurrency
            while i > 0:
                for bucket in self.cluster.buckets:
                    for scope in bucket.scopes.keys():
                        for collection in bucket.scopes[scope].collections.keys():
                            if collection == "_default" and scope == "_default":
                                continue
                            for op_type in doc_ops:
                                if op_type not in ["create", "update", "delete"]:
                                    continue
                                client = NewSDKClient(master, bucket.name, scope, collection)
                                client.initialiseSDK()
                                self.sleep(1)
                                taskName = "Validate_%s_%s_%s_%s_%s_%s" % (bucket.name, scope, collection, op_type, str(i), time.time())
                                task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection+op_type],
                                                        client, "NONE",
                                                        self.maxttl, self.time_unit,
                                                        self.track_failures, 0)
                                tasks.append(task)
                                self.doc_loading_tm.submit(task)
                                i -= 1
        self.doc_loading_tm.getAllTaskResult()
        for task in tasks:
            try:
                task.sdk.disconnectCluster()
            except Exception as e:
                print(e)
        for task in tasks:
            self.assertTrue(task.result, "Validation Failed for: %s" % task.taskName)

    def initial_load(self):
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        if self.rev_write:
            self.create_start = -int(self.init_items_per_collection - 1)
            self.create_end = 1

        self.generate_docs(doc_ops="create")

        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        task = self.data_load()
        self.wait_for_doc_load_completion(task)

        self.num_items = self.init_items_per_collection * self.num_collections
        self.read_start = 0
        self.read_end = self.init_items_per_collection

    def load_buckets_in_dgm(self, kv_gen, op_type, exp, flag=0,
                            batch_size=1000,
                            timeout_secs=30, compression=True,
                            skip_read_on_error=False,
                            suppress_error_table=False,
                            track_failures=False):
        tasks_info = dict()
        self.collections.remove(CbServer.default_collection)
        docs_per_task = dict()
        docs_per_scope = dict.fromkeys(self.scopes, dict())
        for scope in self.scopes:
            task_per_collection = dict()
            if scope == CbServer.default_scope:
                self.collections.append(CbServer.default_collection)
            for collection in self.collections:
                task_info = self.bucket_util._async_load_all_buckets(
                    self.cluster, kv_gen, op_type, exp, flag,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=timeout_secs, time_unit=self.time_unit,
                    batch_size=batch_size,
                    sdk_compression=compression,
                    process_concurrency=self.process_concurrency,
                    retry_exceptions=self.retry_exceptions,
                    active_resident_threshold=self.active_resident_threshold,
                    skip_read_on_error=skip_read_on_error,
                    suppress_error_table=suppress_error_table,
                    dgm_batch=self.dgm_batch,
                    scope=scope,
                    collection=collection,
                    monitor_stats=self.monitor_stats,
                    track_failures=track_failures,
                    load_using=self.load_docs_using)
                tasks_info.update(task_info.items())
                task_per_collection[collection] = list(task_info.keys())[0]
            if scope == CbServer.default_scope:
                self.collections.remove(CbServer.default_collection)
            docs_per_scope[scope] = task_per_collection
        for task in tasks_info.keys():
            self.task_manager.get_task_result(task)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                docs_per_task[task] = task.doc_index
            self.log.info("docs_per_task : {}".format(docs_per_task))
            for scope in self.scopes:
                for collection in self.collections:
                    docs_per_scope[scope][collection] = docs_per_task[docs_per_scope[scope][collection]]
            docs_per_scope[CbServer.default_scope][CbServer.default_collection] = docs_per_task[docs_per_scope[CbServer.default_scope][CbServer.default_collection]]
        self.log.info("docs_per_scope :  {}".format(docs_per_scope))
        # For DGM TESTS, init_items_per_collection ==  max(list of items in each collection)
        self.init_items_per_collection = max([max(docs_per_scope[scope].values()) for scope in docs_per_scope])
        self.log.info("init_items_per_collection =={} ".format(self.init_items_per_collection))

    def tearDown(self):
        self.cluster_util.print_cluster_stats(self.cluster)
        dgm = None
        timeout = 60
        while dgm is None and timeout > 0:
            try:
                stats = BucketHelper(self.cluster.master).fetch_bucket_stats(
                    self.buckets[0].name)
                dgm = stats["op"]["samples"]["vb_active_resident_items_ratio"][
                    -1]
                self.log.info("## Active Resident Threshold of {0} is {1} ##".format(
                    self.buckets[0].name, dgm))
            except:
                self.log.debug("Fetching vb_active_resident_items_ratio(dgm) failed...retying")
                timeout -= 1
                time.sleep(1)

        super(StorageBase, self).tearDown()

    def genrate_docs_basic(self, start, end, target_vbucket=None, mutate=0):
        return doc_generator(self.key, start, end,
                             doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=target_vbucket,
                             vbuckets=self.cluster.vbuckets,
                             key_size=self.key_size,
                             randomize_doc_size=self.randomize_doc_size,
                             randomize_value=self.randomize_value,
                             mix_key_size=self.mix_key_size,
                             mutate=mutate,
                             deep_copy=self.deep_copy)

    def generate_sub_docs_basic(self, start, end):
        return self.custom_sub_doc_generator(self.key, start, end, key_size=self.key_size)

    def generate_docs(self, doc_ops=None,
                      target_vbucket=None,
                      create_end=None, create_start=None,
                      create_mutate=0,
                      update_end=None, update_start=None,
                      update_mutate=0,
                      read_end=None, read_start=None,
                      read_mutate=0,
                      delete_end=None, delete_start=None,
                      expiry_end=None, expiry_start=None,
                      expiry_mutate=0):

        doc_ops = doc_ops or self.doc_ops

        self.log.critical("Doc_ops :: %s, target_vbs: %s" % (doc_ops, target_vbucket))
        if "update" in doc_ops:
            if update_start is not None:
                self.update_start = update_start
            if update_end is not None:
                self.update_end = update_end

            if self.update_start is None:
                self.update_start = self.start
            if self.update_end is None:
                self.update_end = self.end*self.update_perc/100

            self.mutate += 1
            self.log.critical("Update:: %s:%s" % (self.update_start, self.update_end))
            self.gen_update = self.genrate_docs_basic(self.update_start,
                                                      self.update_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=self.mutate)
        if "delete" in doc_ops:
            if delete_start is not None:
                self.delete_start = delete_start
            if delete_end is not None:
                self.delete_end = delete_end

            if self.delete_start is None:
                self.delete_start = self.start
            if self.delete_end is None:
                self.delete_end = self.end*self.delete_perc/100

            self.log.critical("Delete :: %s:%s" % (self.delete_start, self.delete_end))
            self.gen_delete = self.genrate_docs_basic(self.delete_start,
                                                      self.delete_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=read_mutate)
        if "create" in doc_ops:
            if create_start is not None:
                self.create_start = create_start
            if self.create_start is None:
                self.create_start = self.end
            self.start = self.create_start

            if create_end is not None:
                self.create_end = create_end
            if self.create_end is None:
                self.create_end = self.start+self.num_items*self.create_perc/100
            self.end = self.create_end
            self.log.critical("Create :: %s:%s" % (self.create_start, self.create_end))

            self.gen_create = self.genrate_docs_basic(self.create_start,
                                                      self.create_end,
                                                      target_vbucket=target_vbucket,
                                                      mutate=create_mutate)
        if "read" in doc_ops:
            if read_start is not None:
                self.read_start = read_start
            if read_end is not None:
                self.read_end = read_end

            if self.read_start is None:
                self.read_start = self.create_start
            if self.read_end is None:
                self.read_end = self.create_end

            self.log.critical("Read:: %s:%s" % (self.read_start, self.read_end))
            self.gen_read = self.genrate_docs_basic(self.read_start,
                                                    self.read_end,
                                                    target_vbucket=target_vbucket,
                                                    mutate=read_mutate)
        if "expiry" in doc_ops:
            if expiry_start is not None:
                self.expiry_start = expiry_start
            elif self.expiry_start is None:
                self.expiry_start = self.start+(self.num_items *
                                                self.delete_perc)/100

            if expiry_end is not None:
                self.expiry_end = expiry_end
            elif self.expiry_end is None:
                self.expiry_end = self.start+self.num_items *\
                                  (self.delete_perc + self.expiry_perc)/100

            self.log.critical("Exp:: %s:%s" % (self.expiry_start, self.expiry_end))
            self.gen_expiry = self.genrate_docs_basic(self.expiry_start,
                                                      self.expiry_end,
                                                      target_vbucket=target_vbucket,

                                                      mutate=expiry_mutate)

    def generate_subDocs(self, sub_doc_ops=None,
                      upsert_end=None, upsert_start=None
                        ):
        sub_doc_ops = sub_doc_ops

        if DocLoading.Bucket.SubDocOps.UPSERT in sub_doc_ops:
            if upsert_start is not None:
                self.upsert_start = upsert_start
            if upsert_end is not None:
                self.upsert_end = upsert_end

            if self.upsert_start is None:
                self.upsert_start = self.start
            if self.upsert_end is None:
                self.upsert_end = self.end * self.upsert_perc / 100

            self.mutate += 1
            self.gen_subdoc_upsert = self.generate_sub_docs_basic(self.upsert_start,
                                                      self.upsert_end)

    def custom_sub_doc_generator(self, key, start, end,
                          target_vbucket=None, vbuckets=1024, key_size=8):
        mod_body_value_list = ['l3yd8gxieLqc', 'WNvwxIxr2n3Db', 'pde5T6150CWDDJ', 'AV5YFDlZdXUBUI', 'wgFLA8bHc8nTj', '8Mix2170glcrLi', 'C1A9qKXuhJypUCsT', '3JBybTwuih9oCYSxi', 'coV8DBuY4BLziZoc', 'BSlRsbFI8Mix21']
        template = '{{ "mod_body": "{0}"}}'
        return SubdocDocumentGenerator(key, template,
                                           mod_body_value_list,
                                           start=start, end=end,
                                           target_vbucket=target_vbucket,
                                           vbuckets=vbuckets,
                                           key_size=key_size)

    def loadgen_docs(self,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     skip_read_on_error=False,
                     suppress_error_table=False,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection,
                     _sync=True,
                     track_failures=True,
                     doc_ops=None,
                     sdk_retry_strategy=None,
                     iterations=1):
        doc_ops = doc_ops or self.doc_ops

        tasks_info = dict()
        read_tasks_info = dict()
        read_task = False

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)

        if "update" in doc_ops and self.gen_update is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                time_unit=self.time_unit,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures,
                sdk_retry_strategy=sdk_retry_strategy,
                iterations=iterations,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
        if "create" in doc_ops and self.gen_create is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                time_unit=self.time_unit,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures,
                sdk_retry_strategy=sdk_retry_strategy,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "expiry" in doc_ops and self.gen_expiry is not None and self.maxttl:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_expiry, "update",
                self.maxttl, self.random_exp,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                time_unit=self.time_unit,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures,
                sdk_retry_strategy=sdk_retry_strategy,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_expiry.end - self.gen_expiry.start)
        if "read" in doc_ops and self.gen_read is not None:
            read_tasks_info = self.bucket_util._async_validate_docs(
                self.cluster, self.gen_read, "read", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                timeout_secs=self.sdk_timeout,
                time_unit=self.time_unit,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                scope=scope,
                collection=collection,
                suppress_error_table=suppress_error_table,
                sdk_retry_strategy=sdk_retry_strategy)
            read_task = True
        if "delete" in doc_ops and self.gen_delete is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                time_unit=self.time_unit,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures,
                sdk_retry_strategy=sdk_retry_strategy,
                load_using=self.load_docs_using)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if _sync:
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster, load_using=self.load_docs_using)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        if read_task:
            # TODO: Need to converge read_tasks_info into tasks_info before
            #       itself to avoid confusions during _sync=False case
            tasks_info.update(read_tasks_info.items())
            if _sync:
                for task in read_tasks_info:
                    self.task_manager.get_task_result(task)

        return tasks_info

    def get_bucket_dgm(self, bucket):
        self.rest_client = BucketHelper(self.cluster.master)
        count = 0
        dgm = 100
        while count < 5:
            try:
                dgm = self.rest_client.fetch_bucket_stats(
                    bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
                self.log.info("Active Resident Threshold of {0} is {1}".format(
                    bucket.name, dgm))
                return dgm
            except Exception as e:
                self.sleep(5, e)
            count += 1
        return dgm

    def change_swap_space(self, servers=None, disable=True):
        servers = servers or self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            if disable:
                _ = shell.execute_command("swapoff -a")
                self.sleep(5)
                output = shell.execute_command(
                    "free | tail -1 | awk '{print $2}'")[0][0].split('\n')[0]
                self.assertEqual(
                    int(output), 0,
                    msg="Failed to disable swap space on server {} having value {} \
                     ".format(server, output))
            else:
                _ = shell.execute_command("swapon -a")
                self.sleep(5)
                output = shell.execute_command(
                    "free | tail -1 | awk '{print $2}'")[0][0].split('\n')[0]
                self.assertNotEqual(
                    int(output), 0,
                    msg="Failed to enable swap space on server {} having value {} \
                    ".format(server, output))
        return

    def check_fragmentation_using_bucket_stats(self, bucket, servers=None):
        # Disabling the check for time being
        #return True
        result = dict()
        frag_factor = 1.1
        if self.fragmentation <= 10:
            frag_factor = 1.7
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        time_end = time.time() + 60 * 5
        while time.time() < time_end:
            for server in servers:
                frag_val = self.bucket_util.get_fragmentation_kv(
                    self.cluster, bucket, server)
                self.log.debug("Current Fragmentation for node {} is {} \
                ".format(server.ip, frag_val))
                result.update({server.ip: frag_val})
            if (max(result.values())) <= frag_factor * (self.fragmentation):
                self.log.info("KV stats fragmentation values {}".format(result))
                return True
        self.log.info("KV stats fragmentation values {}".format(result))
        return False

    def get_fragmentation_upsert_docs_list(self):
        """
         This function gives the list of "number of docs" need
         to be updated to touch the given fragmentation value
        """
        update_doc_count = int(math.ceil(float(
                    self.fragmentation * self.num_items) / (
                        100 - self.fragmentation)))

        upsert_doc_list = list()
        while update_doc_count > self.num_items:
            upsert_doc_list.append(self.num_items)
            update_doc_count -= self.num_items
        if update_doc_count > 0:
            upsert_doc_list.append(update_doc_count)
        self.log.info("Upsert list {}".format(upsert_doc_list))
        return upsert_doc_list

    def validate_data(self,  op_type, kv_gen, _sync=True):
        self.log.info("Validating Docs")
        validate_tasks_info = dict()
        for collection in self.collections:
            temp_tasks_info = self.bucket_util._async_validate_docs(
                self.cluster, kv_gen, op_type, 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                timeout_secs=self.sdk_timeout,
                scope=CbServer.default_scope,
                collection=collection,
                retry_exceptions=self.retry_exceptions,
                ignore_exceptions=self.ignore_exceptions)
            validate_tasks_info.update(temp_tasks_info.items())
        if _sync:
            for task in validate_tasks_info:
                self.task_manager.get_task_result(task)
        else:
            return validate_tasks_info

    def sigkill_memcached(self, nodes=None, graceful=False):
        nodes = nodes or self.cluster.nodes_in_cluster
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            if graceful:
                shell.restart_couchbase()
            else:
                shell.kill_memcached()
            shell.disconnect()
        for node in self.cluster.nodes_in_cluster:
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[node],
                wait_time=self.wait_timeout * 20))

    def get_memory_footprint(self):
        out = subprocess.Popen(['ps', 'v', '-p', str(os.getpid())], stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        print("RAM FootPrint: %s" % str(mem))

    def crash(self, nodes=None, kill_itr=1, graceful=False,
              wait=True, force_collect=False):
        self.stop_crash = False
        self.crash_failure = False
        count = kill_itr
        self.loop_itr = 0
        msg = None

        nodes = nodes or self.cluster.nodes_in_cluster

        connections = dict()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            connections.update({node: shell})

        while not self.stop_crash:
            if self.loop_itr > 50:
                self.task.jython_task_manager.abort_all_tasks()
                self.stop_crash = True
            self.loop_itr += 1
            sleep = random.randint(30, 60)
            self.sleep(sleep,
                       "Iteration:{} waiting for {} sec to kill memcached on all nodes".
                       format(self.loop_itr, sleep))

            for node, shell in connections.items():
                if "kv" in node.services:
                    if graceful:
                        self.log.info("Restarting couchbase server on node {}".format(node.ip))
                        shell.restart_couchbase()
                    else:
                        self.log.info("Memcached sigKill on node {}".format(node.ip))
                        while count > 0:
                            shell.kill_memcached()
                            count -= 1
                            if count > 0:
                                self.sleep(3, "Sleep before killing memcached on node {} again".format(node.ip))
                        count = kill_itr

            result = self.check_coredump_exist(self.cluster.nodes_in_cluster,
                                               force_collect=force_collect)
            if result:
                self.stop_crash = True
                self.task.jython_task_manager.abort_all_tasks()
                self.crash_failure = result
                msg = "CRASH | CRITICAL | WARN messages found in cb_logs"
                self.log.critical(msg)

            if wait:
                for node in nodes:
                    if "kv" in node.services:
                        result = self.bucket_util._wait_warmup_completed(
                            self.cluster.buckets[0],
                            servers=[node],
                            wait_time=self.wait_timeout * 5)
                        if not result:
                            msg = "warm-up couldn't complete in %s seconds" %\
                                (self.wait_timeout * 5)
                            self.log.critical(msg)
                            self.task.jython_task_manager.abort_all_tasks()
                            self.stop_crash = True
                            self.crash_failure = True

        for _, shell in connections.items():
            shell.disconnect()

    def compact_bucket(self):
        compaction_tasks=[]
        self.stop_compaction = False
        iteration = 1
        while not self.stop_compaction:
            self.log.info("Compaction Iteration == {}".format(iteration))
            for bucket in self.cluster.buckets:
                compaction_tasks.append(self.task.async_compact_bucket(self.cluster.master,
                                               bucket))
            for task in compaction_tasks:
                self.task_manager.get_task_result(task)
            iteration += 1
            self.sleep(20, "sleep before next compaction iteration")

    def chmod(self, server, path, mod="000"):
        '''
            # (Base-10)    Binary    Sum (in binary)    Sum (in decimal)    rwx    Permission
            7    111    = 100 + 10 + 1    = 4(r) + 2(w) + 1(x)    rwx    read, write and execute
            6    110    = 100 + 10    = 4(r) + 2(w)    rw-    read and write
            5    101    = 100      + 1    = 4(r)        + 1(x)    r-x    read and execute
            4    100    = 100    = 4(r)    r--    read only
            3    011    =       10 + 1    =        2(w) + 1(x)    -wx    write and execute
            2    010    =       10    =        2(w)    -w-    write only
            1    001    =            1    =               1(x)    --x    execute only
            0    000    = 0    = 0    ---    none
        '''
        self.stop_chmod = False
        while self.stop_chmod is False:
            shell = RemoteMachineShellConnection(server)
            self.log.debug("{}: changing mod to {} for {}".format(server.ip, mod, path))
            shell.execute_command("chmod {} {}".format(mod, path))
            self.sleep(5)
            self.log.debug("{}: changing mod to {} for {}".format(server.ip, "777", path))
            shell.execute_command("chmod {} {}".format("777", path))
            self.sleep(5)
            shell.disconnect()

    def change_access_mode(self, nodes=None, mod="000", dest="seqIndex"):
        nodes = nodes or self.cluster.nodes_in_cluster
        if type(nodes) is not list:
            nodes = [nodes]
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            paths, _ = shell.execute_command("find {} -name {}".format(self.data_path,
                                                                       dest))
            self.log.info("Paths type is {}".format(type(paths)))
            for path in paths:
                shell.execute_command("chmod {} {}".format(mod, path))
            shell.disconnect()

    def set_metadata_purge_interval(self, value,
                                    buckets=[], node=None):
        self.log.info("Changing the bucket properties by changing {0} to {1}".
                      format("purge_interval", value))
        if not buckets:
            buckets = self.buckets
        if node is None:
            node = self.cluster.master
        rest = RestConnection(node)

        shell = RemoteMachineShellConnection(node)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        for bucket in buckets:
            cmd = '{ok, BC} = ns_bucket:get_bucket(' \
                  '"%s"), BC2 = lists:keyreplace(purge_interval, ' \
                  '1, BC, {purge_interval, %f})' \
                  ', ns_bucket:set_bucket_config("%s", BC2).' \
                  % (bucket.name, value, bucket.name)
            rest.diag_eval(cmd)

        # Restart Memcached in all cluster nodes to reflect the settings
        for server in self.cluster_util.get_kv_nodes(self.cluster):
            shell = RemoteMachineShellConnection(server)
            shell.restart_couchbase()
            shell.disconnect()

        # Check bucket-warm_up after Couchbase restart
        retry_count = 10
        buckets_warmed_up = self.bucket_util.is_warmup_complete(buckets,
                                                                retry_count)
        if not buckets_warmed_up:
            self.log.critical("Few bucket(s) not warmed up "
                              "within expected time")

    def fetch_data_path(self):
        data_path = self.rest.get_data_path()
        if "c:/Program Files" in data_path:
            data_path = data_path.replace("c:/Program Files",
                                           "/cygdrive/c/Program\ Files")
            self.windows_platform = True
        return data_path

    def set_writer_reader_storage_threads(self):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.update_memcached_settings(
            num_writer_threads=self.num_writer_threads,
            num_reader_threads=self.num_reader_threads,
            num_storage_threads=self.num_storage_threads)

    def load_data_cbc_pillowfight(self, server, bucket, items, doc_size,
                                  key_prefix="test_docs", threads=1,
                                  ops_rate=None):
        self.log.info(f"Loading {items} items of doc size: {doc_size} "
                      f"into the bucket with cbc-pillowfight")
        shell = RemoteMachineShellConnection(server)
        pillowfight_base_cmd = \
            "/opt/couchbase/bin/cbc-pillowfight -U {0}/{1} " \
            "-u Administrator -P password -I {2}" \
            " -t {3} -m {4} -M {4} --populate-only --random-body " \
            "--key-prefix={5} -Dtimeout=10"

        cmd = pillowfight_base_cmd.format(server.ip, bucket.name, items,
                                          threads, doc_size, key_prefix)
        if ops_rate is not None:
            cmd += " --rate-limit {}".format(ops_rate)

        self.log.info("Executing pillowfight command = {}".format(cmd))
        _, _ = shell.execute_command(cmd)
        self.sleep(30, "Wait after executing pillowfight command")
        shell.disconnect()

    def PrintStep(self, msg=None):
        print("\n")
        print("\t", "#"*60)
        print("\t", "#")
        print("\t", "#  %s" % msg)
        print("\t", "#")
        print("\t", "#"*60)
        print("\n")
