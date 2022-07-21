import time
import copy
import re

from Cb_constants import DocLoading
from cb_tools.mc_stat import McStat,Mcthrottle
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator

from BucketLib.bucket import Bucket
from membase.api.rest_client import RestConnection
from StatsLib.StatsOperations import StatsHelper
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

    def setUp(self):
        super(LMT, self).setUp()
        self.num_buckets = self.input.param("num_buckets", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        self.doc_loading_tm = TaskManager(self.process_concurrency)
        self.doc_ops = self.input.param("doc_ops", "create")
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(':')
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

    def generate_docs(self, doc_ops=None,
                      create_end=None, create_start=None,
                      update_end=None, update_start=None,
                      delete_end=None, delete_start=None,
                      expire_end=None, expire_start=None,
                      read_end=None, read_start=None):
        self.init_doc_params()
        self.initial_items = self.final_items

        doc_ops = doc_ops or self.doc_ops
        self.mutations_to_validate = doc_ops

        if "read" in doc_ops:
            if read_start is not None:
                self.read_start = read_start
            else:
                self.read_start = 0
            if read_end is not None:
                self.read_end = read_end
            else:
                self.read_end = self.num_items * self.mutation_perc/100

        if "update" in doc_ops:
            if update_start is not None:
                self.update_start = update_start
            else:
                self.update_start = 0
            if update_end is not None:
                self.update_end = update_end
            else:
                self.update_end = self.num_items * self.mutation_perc/100
            # self.mutate += 1

        if "delete" in doc_ops:
            if delete_start is not None:
                self.delete_start = delete_start
            else:
                self.delete_start = self.start
            if delete_end is not None:
                self.delete_end = delete_end
            else:
                self.delete_end = self.start + self.num_items * self.mutation_perc/100
            self.final_items -= (self.delete_end - self.delete_start) * self.num_collections * self.num_scopes

        if "expiry" in doc_ops:
            if self.maxttl == 0:
                self.maxttl = self.input.param("maxttl", 10)
            if expire_start is not None:
                self.expire_start = expire_start
            else:
                self.expire_start = self.delete_end
            if expire_end is not None:
                self.expire_end = expire_end
            else:
                self.expire_end = self.expire_start + self.num_items * self.mutation_perc/100
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
                self.create_end = self.end + (self.expire_end - self.expire_start) + (self.delete_end - self.delete_start)
            self.end = self.create_end

            self.final_items += (abs(self.create_end - self.create_start)) * self.num_collections * self.num_scopes

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
        print "Final Start: %s" % self.start
        print "Final End: %s" % self.end

    def _loader_dict(self, cmd={}):
        self.loader_map = dict()
        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
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

        self.print_stats()

        if self.cluster.cloud_cluster:
            return

        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if result:
            self.PrintStep("CRASH | CRITICAL | WARN messages found in cb_logs")
            if self.assert_crashes_on_load:
                self.task_manager.abort_all_tasks()
                self.doc_loading_tm.abortAllTasks()
                self.assertFalse(result)

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
                    self.bucket_util.verify_stats_all_buckets(self.cluster, self.final_items,
                                                              timeout=14400)
            except Exception as e:
                if not self.cluster.cloud_cluster:
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

    def calculate_units(self, key, value, sub_doc_size=0, xattr=0, read=False):
        if read:
            limit = 4096
        else:
            limit = 1024
        total_size = key + value + sub_doc_size + xattr
        expected_cu, remainder = divmod(total_size, limit)
        if remainder:
            expected_cu += 1
        if self.durability_level != "NONE" and not read:
            expected_cu *= 2
        return expected_cu

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
        for node in bucket.servers:
            content = StatsHelper(node).get_prometheus_metrics_high()
            wu_pattern = re.compile('meter_wu_total{bucket="%s"} (\d+)' %bucket.name)
            ru_pattern = re.compile('meter_ru_total{bucket="%s"} (\d+)' %bucket.name)
            for line in content:
                if wu_pattern.match(line):
                    wu_from_prometheus += int(wu_pattern.findall(line)[0])
                elif ru_pattern.match(line):
                    ru_from_prometheus += int(ru_pattern.findall(line)[0])
        return ru_from_prometheus, wu_from_prometheus

    def get_stat_from_metering(self, bucket):
        ru_from_prometheus = 0
        wu_from_prometheus = 0
        for node in bucket.servers:
            content = StatsHelper(node).metering()
            wu_pattern = re.compile('counter_wu_total{bucket="%s"} (\d+)' %bucket.name)
            ru_pattern = re.compile('counter_ru_total{bucket="%s"} (\d+)' %bucket.name)
            for line in content:
                if wu_pattern.match(line):
                    wu_from_prometheus += int(wu_pattern.findall(line)[0])
                elif ru_pattern.match(line):
                    ru_from_prometheus += int(ru_pattern.findall(line)[0])
        return ru_from_prometheus, wu_from_prometheus

    def get_item_count(self):
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for bucket in buckets:
            self.log.info("item_count is {0}".format(bucket.stats.itemCount))

    def compare_ru_wu_stat(self, ru, wu, expected_ru, expected_wu):
        self.assertEqual(wu, expected_wu)
        self.assertEqual(ru, expected_ru)
