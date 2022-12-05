import time
import copy
import re
import json

from cb_tools.mc_stat import McStat
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import doc_generator

from BucketLib.bucket import Bucket
from membase.api.rest_client import RestConnection
from StatsLib.StatsOperations import StatsHelper
from sdk_exceptions import SDKException
from Cb_constants.CBServer import CbServer
from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.sdk import Server, SDKClient
from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.docgen import WorkLoadSettings,\
    DocumentGenerator
from com.couchbase.test.loadgen import WorkLoadGenerate
from com.couchbase.test.docgen import DocRange
from java.util import HashMap
from couchbase.test.docgen import DRConstants
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest
from com.couchbase.client.core.error import ServerOutOfMemoryException,\
    DocumentExistsException, DocumentNotFoundException, TimeoutException
from cb_tools.cbstats import Cbstats
from com.couchbase.test.transactions import SimpleTransaction as Transaction


# LMT == LIMITING METERING THROTTLING

class LMT(ServerlessOnPremBaseTest):
    def init_doc_params(self):
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0
        self.read_start = 0
        self.read_end = 0

    def setUp(self):
        super(LMT, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        self.doc_loading_tm = TaskManager(self.process_concurrency)
        self.exp_pager_stime = self.input.param("exp_pager_stime", 10)
        self.init_items_per_collection = copy.deepcopy(self.num_items)
        self.num_items_per_collection = copy.deepcopy(self.init_items_per_collection)
        self.init_num_items = self.num_items
        self.doc_ops = self.input.param("doc_ops", "create")
        self._data_validation = self.input.param("data_validation", True)
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.track_failures = self.input.param("track_failures", True)
        self.loader_dict = None
        self.ops_rate = self.input.param("ops_rate", 5000)
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.magma)
        self.autoCompactionDefined = str(self.input.param("autoCompactionDefined", "false")).lower()

        self.log.info("Get the available memory quota")
        rest = RestConnection(self.cluster.master)
        self.info = rest.get_nodes_self()
        kv_memory = self.info.memoryQuota - 100
        ramQuota = self.input.param("ramQuota", kv_memory)
        self.PrintStep("Create required buckets and collections")
        self.bucket_throttling_limit = self.input.param("bucket_throttling_limit", 5000)
        self.check_temporary_failure_exception = False
        self.retry_exceptions = [SDKException.TimeoutException,
                                 SDKException.AmbiguousTimeoutException,
                                 SDKException.RequestCanceledException,
                                 SDKException.UnambiguousTimeoutException,
                                 SDKException.ServerOutOfMemoryException,
                                 SDKException.DurabilityAmbiguousException]
        self.ignore_exceptions = []
        # Monitor Stats Params
        self.ep_queue_stats = self.input.param("ep_queue_stats", True)
        self.monitor_stats = ["doc_ops", "ep_queue_size"]
        if not self.ep_queue_stats:
            self.monitor_stats = ["doc_ops"]

        self.test_itr = self.input.param("test_itr", 1)
        self.deep_copy = self.input.param("deep_copy", False)

        # Bucket Creation
        buckets = ["default"]*self.num_buckets
        for i in range(self.num_buckets):
            self.bucket_util.create_default_bucket(
                self.cluster,
                bucket_type=self.bucket_type,
                ram_quota=self.bucket_ram_quota,
                replica=self.num_replicas,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy,
                conflict_resolution=Bucket.ConflictResolution.SEQ_NO,
                wait_for_warmup=True,
                autoCompactionDefined=self.autoCompactionDefined,
                fragmentation_percentage=self.fragmentation,
                flush_enabled=self.flush_enabled,
                compression_mode=self.compression_mode,
                bucket_durability=self.bucket_durability_level,
                purge_interval=self.bucket_purge_interval,
                bucket_name="default"+str(i),
                vbuckets=None, weight=None, width=None)

        # Scope Creation
        scope_prefix = "Scope"
        self.buckets = self.cluster.buckets
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

        # Collection Creation
        collection_prefix = "FunctionCollection"
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
        self.collections = self.buckets[0].scopes[CbServer.default_scope].collections.keys()
        self.log.debug("Collections list == {}".format(self.collections))

        if self.bucket_throttling_limit != 5000:
            for bucket in self.cluster.buckets:
                # throttling limit will be set on each bucket node
                self.bucket_util.set_throttle_n_storage_limit(bucket=bucket,
                                                     throttle_limit=self.bucket_throttling_limit)

        # Initialise doc loader/generator params
        self.init_doc_params()
        self.create_perc = 100
        self.read_perc = 0
        self.update_perc = 0
        self.upsert_perc = 0
        self.delete_perc = 0
        self.expiry_perc = 0
        self.start = 0
        self.end = 0
        self.initial_items = self.start
        self.final_items = self.end
        self.mutation_perc = 100
        self.key_type = self.input.param("key_type", "RandomKey")
        self.val_type = self.input.param("val_type", "SimpleValue")

    def tearDown(self):
        super(LMT, self).tearDown()

    def PrintStep(self, msg=None):
        print "\n"
        print "\t", "#"*60
        print "\t", "#"
        print "\t", "#  %s" % msg
        print "\t", "#"
        print "\t", "#"*60
        print "\n"

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

            self.gen_expiry = self.genrate_docs_basic(self.expiry_start,
                                                      self.expiry_end,
                                                      target_vbucket=target_vbucket,

                                                      mutate=expiry_mutate)

        print "Read Start: %s" % self.read_start
        print "Read End: %s" % self.read_end
        print "Update Start: %s" % self.update_start
        print "Update End: %s" % self.update_end
        print "Expiry Start: %s" % self.expire_start
        print "Expiry End: %s" % self.expire_end
        print "Delete Start: %s" % self.delete_start
        print "Delete End: %s" % self.delete_end
        print "Create Start: %s" % self.create_start
        print "Create End: %s" % self.create_end

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
                     sdk_retry_strategy=None):
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
                sdk_client_pool=self.sdk_client_pool,
                sdk_retry_strategy=sdk_retry_strategy)
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
                sdk_client_pool=self.sdk_client_pool,
                sdk_retry_strategy=sdk_retry_strategy)
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
                sdk_client_pool=self.sdk_client_pool,
                sdk_retry_strategy=sdk_retry_strategy)
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
               sdk_client_pool=self.sdk_client_pool,
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
                sdk_client_pool=self.sdk_client_pool,
                sdk_retry_strategy=sdk_retry_strategy)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if _sync:
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster,
                                                           sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        if read_task:
            # TODO: Need to converge read_tasks_info into tasks_info before
            #       itself to avoid confusions during _sync=False case
            tasks_info.update(read_tasks_info.items())
            if _sync:
                for task in read_tasks_info:
                    self.task_manager.get_task_result(task)

        return tasks_info

    def _loader_dict(self, cmd={}):
        self.loader_map = dict()
        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
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
                                          cmd.get("mutated", 0)
                                          )
                    hm = HashMap()
                    hm.putAll({DRConstants.create_s: self.create_start,
                               DRConstants.create_e: self.create_end,
                               DRConstants.update_s: self.update_start,
                               DRConstants.update_e: self.update_end,
                               DRConstants.expiry_s: self.expire_start,
                               DRConstants.expiry_e: self.expire_end,
                               DRConstants.delete_s: self.delete_start,
                               DRConstants.delete_e: self.delete_end,
                               DRConstants.read_s: self.read_start,
                               DRConstants.read_e: self.read_end})
                    dr = DocRange(hm)
                    ws.dr = dr
                    dg = DocumentGenerator(ws, self.key_type, self.val_type)
                    self.loader_map.update({bucket.name+scope+collection: dg})

    def perform_load(self, wait_for_load=True,
                     validate_data=True):
        self._loader_dict()
        master = Server(self.cluster.master.ip, self.cluster.master.port,
                        self.cluster.master.rest_username, self.cluster.master.rest_password,
                        str(self.cluster.master.memcached_port))
        tasks = list()
        i = self.process_concurrency
        while i > 0:
            for bucket in self.cluster.buckets:
                for scope in bucket.scopes.keys():
                    for collection in bucket.scopes[scope].collections.keys():
                        if scope == CbServer.system_scope:
                            continue
                        if collection == "_default" and scope == "_default":
                            continue
                        client = NewSDKClient(master, bucket.name, scope, collection)
                        client.initialiseSDK()
                        self.sleep(1)
                        taskName = "Loader_%s_%s_%s_%s_%s" % (bucket.name, scope, collection, str(i), time.time())
                        task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection],
                                                client, self.durability_level,
                                                self.maxttl, self.time_unit,
                                                self.track_failures, 0)
                        tasks.append(task)
                        self.doc_loading_tm.submit(task)
                        i -= 1

        if wait_for_load:
            self.wait_for_doc_load_completion(tasks)
        else:
            return tasks

        if validate_data:
            self.data_validation()

    def wait_for_doc_load_completion(self, tasks, wait_for_stats=True):
        self.doc_loading_tm.getAllTaskResult()
        for task in tasks:
            task.result = True
            unique_str = "{}:{}:{}:".format(task.sdk.bucket, task.sdk.scope, task.sdk.collection)
            for optype, failures in task.failedMutations.items():
                for failure in failures:
                    if failure is not None:
                        print("Test Retrying {}: {}{} -> {}".format(optype, unique_str, failure.id(), failure.err().getClass().getSimpleName()))
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
                    self.cluster, self.cluster.buckets, timeout=14400)
                if self.track_failures:
                    self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items_per_collection*(self.num_scopes)*(self.num_collections-1),
                                                              timeout=14400)
            except Exception as e:
                if self.cluster.type == "default":
                    self.get_gdb()
                raise e

    def data_validation(self):
        doc_ops = self.mutations_to_validate
        pc = min(self.process_concurrency, 20)
        if self._data_validation:
            self.log.info("Validating Active/Replica Docs")
            cmd = dict()
            self.ops_rate = self.input.param("ops_rate", 2000)
            master = Server(self.cluster.master.ip, self.cluster.master.port,
                            self.cluster.master.rest_username, self.cluster.master.rest_password,
                            str(self.cluster.master.memcached_port))
            self.loader_map = dict()
            for bucket in self.cluster.buckets:
                for scope in bucket.scopes.keys():
                    for collection in bucket.scopes[scope].collections.keys():
                        if scope == CbServer.system_scope:
                            continue
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
                                                  cmd.get("workers", pc),
                                                  cmd.get("ops", self.ops_rate),
                                                  cmd.get("loadType", None),
                                                  cmd.get("keyType", None),
                                                  cmd.get("valueType", None),
                                                  cmd.get("validate", True),
                                                  cmd.get("gtm", False),
                                                  cmd.get("deleted", False),
                                                  cmd.get("mutated", 0))
                            ws.dr = dr
                            dg = DocumentGenerator(ws, self.key_type, self.val_type)
                            self.loader_map.update({bucket.name+scope+collection+op_type: dg})

            tasks = list()
            i = pc
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
                                taskName = "Validate_%s_%s_%s_%s_%s_%s" % (bucket.name,
                                                                           scope, collection,
                                                                           op_type,
                                                                           str(i), time.time())
                                task = WorkLoadGenerate(taskName,
                                                        self.loader_map[bucket.name+scope+collection+op_type],
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

    def get_stat(self, bucket):
        throttle_limit = list()
        ru = 0
        wu = 0
        num_throttled = 0
        shell = RemoteMachineShellConnection(self.cluster.master)
        mc_stat = McStat(shell)
        shell.disconnect()
        # check in all the nodes where the bucket is present
        for node in bucket.servers:
            stat = mc_stat.bucket_details(node, bucket.name)
            ru += stat["ru"]
            wu += stat["wu"]
            throttle_limit.append(stat["throttle_limit"])
            num_throttled += stat["num_throttled"]
        self.log.info("throttle_limit: %s, num_throttled: %s, RU: %s, WU: %s"
                      % (throttle_limit, num_throttled, ru, wu))
        # ru_from_prometheus, wu_from_prometheus = self.get_stat_from_prometheus(bucket)
        # ru_from_metering, wu_from_metering = self.get_stat_from_metering(bucket)
        # self.assertEqual(ru, ru_from_prometheus)
        # self.assertEqual(wu, wu_from_prometheus)
        # self.assertEqual(ru, ru_from_metering)
        # self.assertEqual(wu, wu_from_metering)
        return num_throttled, ru, wu

    def get_stat_from_prometheus(self, bucket):
        ru_from_prometheus = 0
        wu_from_prometheus = 0
        num_throttled_prometheus, total_storage_bytes = 0, 0
        for node in bucket.servers:
            content = StatsHelper(node).get_prometheus_metrics_high()
            wu_pattern = re.compile('meter_wu_total{bucket="%s"} (\d+)' % bucket.name)
            ru_pattern = re.compile('meter_ru_total{bucket="%s"} (\d+)' % bucket.name)
            num_throttled = re.compile('throttle_count_total{bucket="%s",for="kv"} (\d+)' % bucket.name)
            storage_bytes = re.compile('storage_bytes{bucket="%s"} (\d+)' % bucket.name)
            for line in content:
                if wu_pattern.match(line):
                    wu_from_prometheus += int(wu_pattern.findall(line)[0])
                elif ru_pattern.match(line):
                    ru_from_prometheus += int(ru_pattern.findall(line)[0])
                elif num_throttled.match(line):
                    num_throttled_prometheus += int(num_throttled.findall(line)[0])
                elif storage_bytes.match(line):
                    total_storage_bytes += int(storage_bytes.findall(line)[0])
        return total_storage_bytes, num_throttled_prometheus, ru_from_prometheus, wu_from_prometheus

    def get_storage_from_node(self, bucket):
        total_storage = 0
        for node in bucket.servers:
            remote_client = RemoteMachineShellConnection(node)
            cmd = "du -sbh /opt/couchbase/var/lib/couchbase/data/" + bucket.name
            output, e = remote_client.execute_command(cmd)
            total_storage += float(output[0].split("\t")[0].strip("M"))

    def get_stat_from_metering(self, bucket):
        wu_from_metering = 0
        ru_from_metering = 0
        num_throttle_metering, total_storage_bytes = 0, 0

        for node in bucket.servers:
            content = StatsHelper(node).metering()
            wu_pattern = re.compile('meter_wu_total{bucket="%s"} (\d+)' % bucket.name)
            ru_pattern = re.compile('meter_ru_total{bucket="%s"} (\d+)' % bucket.name)
            num_throttle = re.compile('throttle_count_total{bucket="%s",for="kv"} (\d+)' % bucket.name)
            storage_bytes = re.compile('storage_bytes{bucket="%s",for="kv"} (\d+)' % bucket.name)
            for line in content:
                if wu_pattern.match(line):
                    wu_from_metering += int(wu_pattern.findall(line)[0])
                elif ru_pattern.match(line):
                    ru_from_metering += int(ru_pattern.findall(line)[0])
                elif num_throttle.match(line):
                    num_throttle_metering += int(num_throttle.findall(line)[0])
                elif storage_bytes.match(line):
                    total_storage_bytes += int(storage_bytes.findall(line)[0])
        return total_storage_bytes, num_throttle_metering, ru_from_metering, wu_from_metering

    def get_item_count(self):
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for bucket in buckets:
            self.log.info("item_count is {0}".format(bucket.stats.itemCount))

    def compare_ru_wu_stat(self, ru, wu, expected_ru, expected_wu):
        self.assertEqual(wu, expected_wu)
        self.assertEqual(ru, expected_ru)

    def get_active_vbuckets(self, node, bucket):
        cbstat_obj = Cbstats(node, "Administrator", "password")
        active_vb_numbers = cbstat_obj.vbucket_list(bucket.name,
                                                    vbucket_type="active")
        return active_vb_numbers

    def throttling_limit_on_node(self, node, bucket, throttle_limit):
        shell = RemoteMachineShellConnection(self.cluster.master)
        mc_stat = McStat(shell)
        shell.disconnect()
        stat = mc_stat.bucket_details(node, bucket.name)
        if stat["throttle_limit"] < 1000:
            return throttle_limit
        throttle_limit = stat["throttle_limit"]
        return throttle_limit

    def generate_data_for_vbuckets(self, target_vbucket):
        self.key_value = dict()
        gen_docs = doc_generator("throttling", 0, self.num_items,
                                 doc_size=self.doc_size,
                                 mutation_type="create",
                                 randomize_value=False,
                                 target_vbucket=target_vbucket)
        while gen_docs.has_next():
            key, val = next(gen_docs)
            self.key_value[key] = val

    def calculate_expected_num_throttled(self, node, bucket, throttle_limit,
                                         write_units, expected_num_throttled):
        throttle_limit = self.throttling_limit_on_node(node, bucket,
                                                       throttle_limit)
        if throttle_limit < 10:
            expected_num_throttled += 1
        elif write_units > throttle_limit:
            to_add = (write_units/throttle_limit) - 1
            if (to_add - self.sdk_timeout) > 0:
                to_add += (to_add - self.sdk_timeout)
            expected_num_throttled += to_add
        return throttle_limit, expected_num_throttled

    def compute_docs_ranges(self, start=None, doc_ops=None):
        self.create_perc = 0
        self.update_perc = 0
        self.read_perc = 0
        self.delete_perc = 0
        self.expiry_perc = 0
        doc_ops = doc_ops or self.doc_ops
        ops_len = len(doc_ops.split(":"))
        self.perc = 100/ops_len
        self.log.info("self.perc is {}".format(self.perc))
        if "read" in doc_ops:
            self.read_start = 0
            self.read_end = self.init_items_per_collection
            if ops_len > 1:
                ops_len -= 1
            self.read_perc = self.perc

        if "create" in doc_ops:
            ops_len -= 1
            self.create_start = start or self.init_items_per_collection
            if start:
                self.create_end = start + start
            else:
                self.create_end = self.init_items_per_collection + self.init_items_per_collection
            self.num_items_per_collection += (self.create_end - self.create_start)
            self.create_perc = self.perc
        if ops_len == 1:
            self.update_start = 0
            self.update_end = self.init_num_items
            self.expiry_start = 0
            self.expiry_end = self.init_num_items
            self.delete_start = 0
            self.delete_end = self.init_num_items
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = self.init_num_items // 2
            self.delete_start = self.init_num_items // 2
            self.delete_end = self.init_num_items

            if "expiry" in doc_ops:
                self.delete_start = 0
                self.delete_end = self.init_num_items // 2
                self.expiry_start = self.init_num_items // 2
                self.expiry_end = self.init_num_items
        elif ops_len == 3:
            self.update_start = 0
            self.update_end = self.init_num_items // 3
            self.delete_start = self.init_num_items // 3
            self.delete_end = (2 * self.init_num_items) // 3
            self.expiry_start = (2 * self.init_num_items) // 3
            self.expiry_end = self.init_num_items
        if "update" in doc_ops:
            self.read_start = self.update_start
            self.read_end = self.update_end
        if "delete" in doc_ops:
            self.delete_perc = self.perc
        if "expiry" in doc_ops:
            self.expiry_perc = self.perc
        if "update" in doc_ops:
            self.update_perc = self.perc

        if "delete" in doc_ops:
            self.num_items_per_collection -= (self.delete_end - self.delete_start)
        if "expiry" in doc_ops:
            self.num_items_per_collection -= (self.expiry_end - self.expiry_start)

    def __durability_level(self):
        if self.durability_level == Bucket.DurabilityLevel.MAJORITY:
            self.durability = 1
        elif self.durability_level \
                == Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            self.durability = 2
        elif self.durability_level \
                == Bucket.DurabilityLevel.PERSIST_TO_MAJORITY:
            self.durability = 3
        else:
            self.durability = 5

    def create_Transaction(self, client):
        self.__durability_level()
        self.log.info("durability_level is %s and self.durability is %s"
                      %(self.durability_level, self.durability))
        transaction_config = Transaction().createTransactionConfig(
            self.transaction_timeout, self.durability)
        try:
            self.transaction = Transaction().createTansaction(
                client.cluster, transaction_config)
        except Exception as e:
            self.fail(e)
        return self.transaction

    def transaction_operations(self, client, create_docs=[],
                               update_docs=[], delete_docs=[],
                               commit=True):
        if self.defer:
            ret = Transaction().DeferTransaction(
                    client.cluster,
                    self.transaction, [client.collection], create_docs,
                    update_docs, delete_docs, self.update_count)
            encoded = ret.getT1()
            self.sleep(5) # wait before commit/rollback defer transactions
            exception = Transaction().DefferedTransaction(
                            client.cluster,
                            self.transaction, commit, encoded)
        else:
            exception = Transaction().RunTransaction(
                    client.cluster,
                    self.transaction, [client.collection], create_docs,
                    update_docs, delete_docs, commit,
                    self.sync, self.update_count)
        if exception:
            self.log.info("txn failed")
        self.sleep(1)
