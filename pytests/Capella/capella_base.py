'''
Created on Apr 20, 2022

@author: ritesh.agarwal
'''


import time
import copy

from BucketLib.bucket import Bucket
from Cb_constants.CBServer import CbServer
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
from sdk_constants.java_client import SDKConstants
from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.docgen import WorkLoadSettings,\
    DocumentGenerator
from com.couchbase.test.sdk import Server
from com.couchbase.test.loadgen import WorkLoadGenerate
from Jython_tasks.task import PrintBucketStats
from java.util import HashMap
from com.couchbase.test.docgen import DocRange
from couchbase.test.docgen import DRConstants
from com.couchbase.client.core.error import ServerOutOfMemoryException,\
    DocumentExistsException, DocumentNotFoundException, TimeoutException
from capella.capella_utils import CapellaUtils as CapellaAPI


class CapellaBase(BaseTestCase):
    def tearDown(self):
        self.cluster_util.print_cluster_stats(self.cluster)
        for bucket in self.cluster.buckets:
            CapellaAPI.delete_bucket(self.cluster, bucket.name)

        super(CapellaBase, self).tearDown()

    def setUp(self):
        super(CapellaBase, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.windows_platform = False

        # Bucket Params
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.couchstore)
        self.bucket_eviction_policy = self.input.param("bucket_eviction_policy",
                                                       Bucket.EvictionPolicy.FULL_EVICTION)
        self.bucket_name = self.input.param("bucket_name", "default")
        self.magma_buckets = self.input.param("magma_buckets", 0)

        # SDK Exceptions
        self.check_temporary_failure_exception = False
        self.retry_exceptions = [SDKException.TimeoutException,
                                 SDKException.AmbiguousTimeoutException,
                                 SDKException.RequestCanceledException,
                                 SDKException.UnambiguousTimeoutException,
                                 SDKException.ServerOutOfMemoryException,
                                 SDKException.DurabilityAmbiguousException]
        self.ignore_exceptions = []

        # Create Buckets
        self.log.info("Get the available memory quota")
        rest = RestConnection(self.cluster.master)
        self.info = rest.get_nodes_self()

        # threshold_memory_vagrant = 100
        kv_memory = self.info.memoryQuota - 100

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        ram_quota = self.input.param("ramQuota", kv_memory)
        buckets = ["default"]*self.num_buckets
        bucket_type = self.bucket_type.split(';')*self.num_buckets
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i] + str(i),
                 Bucket.ramQuotaMB: ram_quota/self.num_buckets,
                 Bucket.maxTTL: self.bucket_ttl,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: bucket_type[i],
                 Bucket.durabilityMinLevel: self.bucket_durability_level,
                 Bucket.flushEnabled: True,
                 Bucket.fragmentationPercentage: self.fragmentation})
            self.bucket_params = {
                "name": bucket.name,
                "bucketConflictResolution": "seqno",
                "memoryAllocationInMb": bucket.ramQuotaMB,
                "flush": bucket.flushEnabled,
                "replicas": bucket.replicaNumber,
                "durabilityLevel": bucket.durability_level,
                "timeToLive": {"unit": "seconds", "value": bucket.maxTTL}
                }
            CapellaAPI.create_bucket(self.cluster, self.bucket_params)
            self.cluster.buckets.append(bucket)

        self.buckets = self.cluster.buckets
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
                for i in range(self.num_collections):
                    collection_name = collection_prefix + str(i)
                    self.log.info("Creating scope::collection {} {}\
                    ".format(scope_name, collection_name))
                    self.bucket_util.create_collection(
                        self.cluster.master, bucket,
                        scope_name, {"name": collection_name})
                    self.sleep(2)
        self.collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
        self.log.debug("Collections list == {}".format(self.collections))

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
        self.init_num_items = self.num_items
        self.maxttl = self.input.param("maxttl", 10)

        self.suppress_error_table = True
        self.skip_read_on_error = False
        self.track_failures = self.input.param("track_failures", True)

    def _loader_dict_new(self, cmd={}, scopes=None, collections=None):
        self.loader_map = dict()
        for bucket in self.cluster.buckets:
            scopes_keys = scopes or bucket.scopes.keys()
            for scope in scopes_keys:
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
                    self.cluster, self.cluster.buckets, timeout=120)
                if self.track_failures:
                    self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items_per_collection*self.num_scopes*(self.num_collections))
            except Exception as e:
                raise e

    def new_loader(self, cmd=dict(), wait=False, scopes=None, collections=None):
        self._loader_dict_new(cmd)
        self.doc_loading_tm = TaskManager(self.process_concurrency)
        self.ops_rate = self.input.param("ops_rate", 2000)
        master = Server(self.cluster.srv, self.cluster.master.port,
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
