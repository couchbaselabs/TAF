'''
Created on 30-Aug-2021

@author: riteshagarwal
'''
import subprocess
import json
from BucketLib.bucket import Bucket
import os
from remote.remote_util import RemoteMachineShellConnection
import random
from BucketLib.BucketOperations import BucketHelper
from table_view import TableView
from membase.api.rest_client import RestConnection
from cb_tools.cbstats import Cbstats
from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.sdk import Server, SDKClient
from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.docgen import WorkLoadSettings,\
    DocumentGenerator
from com.couchbase.test.loadgen import WorkLoadGenerate
from com.couchbase.test.docgen import DocRange
from java.util import HashMap
from couchbase.test.docgen import DRConstants
from com.couchbase.client.core.error import DocumentExistsException,\
    TimeoutException, DocumentNotFoundException, ServerOutOfMemoryException
import time
from custom_exceptions.exception import RebalanceFailedException
from Cb_constants.CBServer import CbServer
from threading import Thread


class OPD:
    def __init__(self):
        pass

    def threads_calculation(self):
        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        self.doc_loading_tm = TaskManager(self.process_concurrency)

    def get_memory_footprint(self):
        out = subprocess.Popen(['ps', 'v', '-p', str(os.getpid())],stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        self.PrintStep("RAM FootPrint: %s" % str(mem))
        return mem

    def create_required_buckets(self, cluster):
        self.log.info("Get the available memory quota")
        rest = RestConnection(cluster.master)
        self.info = rest.get_nodes_self()

        # threshold_memory_vagrant = 100
        kv_memory = self.info.memoryQuota - 100

        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        self.bucket_expiry = self.input.param("bucket_expiry", 0)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = ["GleamBookUsers"]*self.num_buckets
        bucket_type = self.bucket_type.split(';')*self.num_buckets
        compression_mode = self.compression_mode.split(';')*self.num_buckets
        self.bucket_eviction_policy = self.bucket_eviction_policy
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i] + str(i),
                 Bucket.ramQuotaMB: ramQuota/self.num_buckets,
                 Bucket.maxTTL: self.bucket_expiry,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: bucket_type[i],
                 Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
                 Bucket.compressionMode: compression_mode[i],
                 Bucket.fragmentationPercentage: self.fragmentation,
                 Bucket.width: self.bucket_width,
                 Bucket.weight: self.bucket_weight,
                 Bucket.historyRetentionBytes: self.bucket_history_retention_bytes,
                 Bucket.historyRetentionSeconds: self.bucket_history_retention_seconds})

            self.bucket_util.create_bucket(cluster, bucket)

        # rebalance the new buckets across all nodes.
        self.log.info("Rebalance Starts")
        self.nodes = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in self.nodes],
                       ejectedNodes=[])
        rest.monitorRebalance()

    def create_required_collections(self, cluster, buckets=None):
        buckets = buckets or cluster.buckets
        self.scope_name = self.input.param("scope_name", "_default")

        def create_collections(bucket):
            node = cluster.master
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                if bucket.loadDefn.get("collections") > 0:
                    self.collection_prefix = self.input.param("collection_prefix",
                                                              "VolumeCollection")

                    for i in range(bucket.loadDefn.get("collections")):
                        collection_name = self.collection_prefix + str(i)
                        self.bucket_util.create_collection(node,
                                                           bucket,
                                                           scope,
                                                           {"name": collection_name})
                        self.sleep(0.1)

                collections = bucket.scopes[scope].collections.keys()
                self.log.debug("Collections list == {}".format(collections))

        for bucket in buckets:
            if bucket.loadDefn.get("scopes") > 1:
                self.scope_prefix = self.input.param("scope_prefix",
                                                     "VolumeScope")
                node = cluster.master or bucket.nebula_endpoint
                for i in range(bucket.loadDefn.get("scopes")):
                    scope_name = self.scope_prefix + str(i)
                    self.log.info("Creating scope: %s"
                                  % (scope_name))
                    self.bucket_util.create_scope(node,
                                                  bucket,
                                                  {"name": scope_name})
                    self.sleep(0.1)
        threads = []
        for bucket in buckets:
            th = Thread(
                    target=create_collections,
                    name="{}_create_collection".format(bucket.name),
                    args=(bucket,))
            threads.append(th)
            th.start()
        for thread in threads:
            thread.join()

    def stop_purger(self, tombstone_purge_age=60):
        """
        1. Disable ts purger
        2. Create fts indexes (to create metakv, ns_config entries)
        3. Delete fts indexes
        4. Grep ns_config for '_deleted' to get total deleted keys count
        5. enable ts purger and age = 1 mins
        6. Sleep for 2 minutes
        7. Grep for debug.log and check for latest tombstones purged count
        8. Validate step4 count matches step 7 count for all nodes
        """
        self.rest.update_tombstone_purge_age_for_removal(tombstone_purge_age)
        self.rest.disable_tombstone_purger()

    def get_bucket_dgm(self, bucket):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Active Resident Threshold of {0} is {1}".format(
            bucket.name, dgm))
        return dgm

    def generate_docs(self, doc_ops=None,
                      create_end=None, create_start=None,
                      update_end=None, update_start=None,
                      delete_end=None, delete_start=None,
                      expire_end=None, expire_start=None,
                      read_end=None, read_start=None,
                      bucket=None):
        bucket.create_end = 0
        bucket.create_start = 0
        bucket.read_end = 0
        bucket.read_start = 0
        bucket.update_end = 0
        bucket.update_start = 0
        bucket.delete_end = 0
        bucket.delete_start = 0
        bucket.expire_end = 0
        bucket.expire_start = 0
        try:
            bucket.final_items
        except:
            bucket.final_items = 0
        bucket.initial_items = bucket.final_items

        doc_ops = doc_ops or bucket.loadDefn.get("load_type")
        self.mutations_to_validate = doc_ops

        if "read" in doc_ops:
            if read_start is not None:
                bucket.read_start = read_start
            else:
                bucket.read_start = 0
            if read_end is not None:
                bucket.read_end = read_end
            else:
                bucket.read_end = bucket.loadDefn.get("num_items")/2 * self.mutation_perc/100

        if "update" in doc_ops:
            if update_start is not None:
                bucket.update_start = update_start
            else:
                bucket.update_start = 0
            if update_end is not None:
                bucket.update_end = update_end
            else:
                bucket.update_end = bucket.loadDefn.get("num_items")/2 * self.mutation_perc/100
            self.mutate += 1

        if "delete" in doc_ops:
            if delete_start is not None:
                bucket.delete_start = delete_start
            else:
                bucket.delete_start = bucket.start
            if delete_end is not None:
                bucket.delete_end = delete_end
            else:
                bucket.delete_end = bucket.start + bucket.loadDefn.get("num_items")/2 * self.mutation_perc/100
            bucket.final_items -= (bucket.delete_end - bucket.delete_start) * bucket.loadDefn.get("collections") * bucket.loadDefn.get("scopes")

        if "expiry" in doc_ops:
            if self.maxttl == 0:
                self.maxttl = self.input.param("maxttl", 10)
            if expire_start is not None:
                bucket.expire_start = expire_start
            else:
                bucket.expire_start = bucket.delete_end
            if expire_end is not None:
                bucket.expire_end = expire_end
            else:
                bucket.expire_end = bucket.expire_start + bucket.loadDefn.get("num_items")/2 * self.mutation_perc/100
            bucket.final_items -= (bucket.expire_end - bucket.expire_start) * bucket.loadDefn.get("collections") * bucket.loadDefn.get("scopes")

        if "create" in doc_ops:
            if create_start is not None:
                bucket.create_start = create_start
            else:
                bucket.create_start = bucket.end
            bucket.start = bucket.create_start

            if create_end is not None:
                bucket.create_end = create_end
            else:
                bucket.create_end = bucket.end + (bucket.expire_end - bucket.expire_start) + (bucket.delete_end - bucket.delete_start)
            bucket.end = bucket.create_end

            bucket.final_items += (abs(bucket.create_end - bucket.create_start)) * bucket.loadDefn.get("collections") * bucket.loadDefn.get("scopes")
        print "================{}=================".format(bucket.name)
        print "Read Start: %s" % bucket.read_start
        print "Read End: %s" % bucket.read_end
        print "Update Start: %s" % bucket.update_start
        print "Update End: %s" % bucket.update_end
        print "Expiry Start: %s" % bucket.expire_start
        print "Expiry End: %s" % bucket.expire_end
        print "Delete Start: %s" % bucket.delete_start
        print "Delete End: %s" % bucket.delete_end
        print "Create Start: %s" % bucket.create_start
        print "Create End: %s" % bucket.create_end
        print "Final Start: %s" % bucket.start
        print "Final End: %s" % bucket.end
        print "================{}=================".format(bucket.name)

    def _loader_dict(self, buckets, overRidePattern=None, cmd={}):
        self.loader_map = dict()
        self.default_pattern = [100, 0, 0, 0, 0]
        buckets = buckets or self.cluster.buckets
        for bucket in buckets:
            # process_concurrency = min(bucket.loadDefn.get("scopes") * bucket.loadDefn.get("collections"), 5)
            pattern = overRidePattern or bucket.loadDefn.get("pattern", self.default_pattern)
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    if collection == "_default" and scope == "_default":
                        continue
                    ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                          cmd.get("keySize", self.key_size),
                                          cmd.get("docSize", self.doc_size),
                                          cmd.get("cr", pattern[0]),
                                          cmd.get("rd", pattern[1]),
                                          cmd.get("up", pattern[2]),
                                          cmd.get("dl", pattern[3]),
                                          cmd.get("ex", pattern[4]),
                                          cmd.get("workers", self.process_concurrency),
                                          cmd.get("ops", bucket.loadDefn.get("ops")),
                                          cmd.get("loadType", None),
                                          cmd.get("keyType", self.key_type),
                                          cmd.get("valueType", bucket.loadDefn.get("valType")),
                                          cmd.get("validate", False),
                                          cmd.get("gtm", False),
                                          cmd.get("deleted", False),
                                          cmd.get("mutated", 0)
                                          )
                    hm = HashMap()
                    hm.putAll({DRConstants.create_s: bucket.create_start,
                               DRConstants.create_e: bucket.create_end,
                               DRConstants.update_s: bucket.update_start,
                               DRConstants.update_e: bucket.update_end,
                               DRConstants.expiry_s: bucket.expire_start,
                               DRConstants.expiry_e: bucket.expire_end,
                               DRConstants.delete_s: bucket.delete_start,
                               DRConstants.delete_e: bucket.delete_end,
                               DRConstants.read_s: bucket.read_start,
                               DRConstants.read_e: bucket.read_end})
                    dr = DocRange(hm)
                    ws.dr = dr
                    dg = DocumentGenerator(ws, self.key_type, bucket.loadDefn.get("valType"))
                    self.loader_map.update({bucket.name+scope+collection: dg})

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
                if self.track_failures and self.cluster.type == "default":
                    self.bucket_util.verify_stats_all_buckets(self.cluster, self.final_items,
                                                              timeout=14400)
            except Exception as e:
                if not self.cluster.type == "default":
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
                    if scope == CbServer.system_scope:
                            continue
                    for collection in bucket.scopes[scope].collections.keys():
                        if collection == "_default" and scope == "_default":
                            continue
                        for op_type in bucket.loadDefn.get("load_type"):
                            cmd.update({"deleted": False})
                            hm = HashMap()
                            if op_type == "create":
                                hm.putAll({DRConstants.read_s: bucket.create_start,
                                           DRConstants.read_e: bucket.create_end})
                            elif op_type == "update":
                                hm.putAll({DRConstants.read_s: bucket.update_start,
                                           DRConstants.read_e: bucket.update_end})
                            elif op_type == "delete":
                                hm.putAll({DRConstants.read_s: bucket.delete_start,
                                           DRConstants.read_e: bucket.delete_end})
                                cmd.update({"deleted": True})
                            else:
                                continue
                            dr = DocRange(hm)
                            ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                                  cmd.get("keySize", self.key_size),
                                                  cmd.get("docSize", bucket.doc_size),
                                                  cmd.get("cr", 0),
                                                  cmd.get("rd", 100),
                                                  cmd.get("up", 0),
                                                  cmd.get("dl", 0),
                                                  cmd.get("ex", 0),
                                                  cmd.get("workers", pc),
                                                  cmd.get("ops", bucket.loadDefn.get("ops")),
                                                  cmd.get("loadType", None),
                                                  cmd.get("keyType", None),
                                                  cmd.get("valueType", bucket.loadDefn.get("valType")),
                                                  cmd.get("validate", True),
                                                  cmd.get("gtm", False),
                                                  cmd.get("deleted", False),
                                                  cmd.get("mutated", 0))
                            ws.dr = dr
                            dg = DocumentGenerator(ws, self.key_type, bucket.loadDefn.get("valType"))
                            self.loader_map.update({bucket.name+scope+collection+op_type: dg})

            tasks = list()
            i = pc
            while i > 0:
                for bucket in self.cluster.buckets:
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
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

    def print_crud_stats(self, buckets):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Initial Items",
                                "Current Items",
                                "Items Updated",
                                "Items Created",
                                "Items Deleted",
                                "Items Expired"])
        for bucket in buckets:
            self.table.add_row([
                str(bucket.initial_items),
                str(bucket.final_items),
                str(abs(bucket.update_start)) + "-" + str(abs(bucket.update_end)),
                str(abs(bucket.create_start)) + "-" + str(abs(bucket.create_end)),
                str(abs(bucket.delete_start)) + "-" + str(abs(bucket.delete_end)),
                str(abs(bucket.expire_start)) + "-" + str(abs(bucket.expire_end))
                ])
        self.table.display("Docs statistics")

    def perform_load(self, wait_for_load=True,
                     validate_data=True, buckets=None, overRidePattern=None):
        self.get_memory_footprint()
        buckets = buckets or self.cluster.buckets
        self._loader_dict(buckets, overRidePattern)
        master = Server(self.cluster.master.ip, self.cluster.master.port,
                        self.cluster.master.rest_username, self.cluster.master.rest_password,
                        str(self.cluster.master.memcached_port))
        tasks = list()
        i = self.process_concurrency
        while i > 0:
            for bucket in buckets:
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
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
            self.get_memory_footprint()
        else:
            return tasks

        if validate_data:
            self.data_validation()

        self.print_stats()

    def print_stats(self):
        self.bucket_util.print_bucket_stats(self.cluster)
        self.cluster_util.print_cluster_stats(self.cluster)
        self.print_crud_stats(self.cluster.buckets)
        for bucket in self.cluster.buckets:
            self.get_bucket_dgm(bucket)

    def PrintStep(self, msg=None):
        print "\n"
        print "\t", "#"*60
        print "\t", "#"
        print "\t", "#  %s" % msg
        print "\t", "#"
        print "\t", "#"*60
        print "\n"
