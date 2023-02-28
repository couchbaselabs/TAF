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
    TimeoutException, DocumentNotFoundException, ServerOutOfMemoryException,\
    RequestCanceledException
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
                 Bucket.weight: self.bucket_weight})
            self.bucket_util.create_bucket(cluster, bucket)
            if bucket.serverless and self.nebula_details.get(cluster):
                self.bucket_util.update_bucket_nebula_servers(cluster, bucket)
                bucket.serverless.nebula_endpoint = self.nebula_details[cluster].endpoint

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
            node = cluster.master or bucket.serverless.nebula_endpoint
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

    def _induce_error(self, error_condition, nodes=[]):
        nodes = nodes or [self.cluster.master]
        for node in nodes:
            if error_condition == "stop_server":
                self.cluster_util.stop_server(node)
            elif error_condition == "enable_firewall":
                self.cluster_util.start_firewall_on_node(node)
            elif error_condition == "kill_memcached":
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
            elif error_condition == "reboot_server":
                shell = RemoteMachineShellConnection(node)
                shell.reboot_node()
            elif error_condition == "kill_erlang":
                shell = RemoteMachineShellConnection(node)
                shell.kill_erlang()
                shell.disconnect()
            else:
                self.fail("Invalid error induce option")

    def _recover_from_error(self, error_condition):
        for node in self.cluster.nodes_in_cluster:
            if error_condition == "stop_server" or error_condition == "kill_erlang":
                self.cluster_util.start_server(node)
            elif error_condition == "enable_firewall":
                self.cluster_util.stop_firewall_on_node(node)

        for node in self.cluster.kv_nodes + [self.cluster.master]:
            self.check_warmup_complete(node)
            result = self.cluster_util.wait_for_ns_servers_or_assert([node],
                                                                     wait_time=1200)
            self.assertTrue(result, "Server warmup failed")

    def rebalance(self, nodes_in=0, nodes_out=0, services=[],
                  retry_get_process_num=3000):
        self.servs_in = list()
        self.nodes_cluster = self.cluster.nodes_in_cluster[:]
        self.nodes_cluster.remove(self.cluster.master)
        self.servs_out = list()
        services = services or ["kv"]
        print "KV nodes in cluster: %s" % [server.ip for server in self.cluster.kv_nodes]
        print "CBAS nodes in cluster: %s" % [server.ip for server in self.cluster.cbas_nodes]
        print "INDEX nodes in cluster: %s" % [server.ip for server in self.cluster.index_nodes]
        print "FTS nodes in cluster: %s" % [server.ip for server in self.cluster.fts_nodes]
        print "QUERY nodes in cluster: %s" % [server.ip for server in self.cluster.query_nodes]
        print "EVENTING nodes in cluster: %s" % [server.ip for server in self.cluster.eventing_nodes]
        print "AVAILABLE nodes for cluster: %s" % [server.ip for server in self.available_servers]
        if nodes_out:
            if "cbas" in services:
                servers = random.sample(self.cluster.cbas_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "index" in services:
                servers = random.sample(self.cluster.index_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "fts" in services:
                servers = random.sample(self.cluster.fts_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "query" in services:
                servers = random.sample(self.cluster.query_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "eventing" in services:
                servers = random.sample(self.cluster.eventing_nodes, nodes_out)
                self.servs_out.extend(servers)
            if "kv" in services:
                nodes = [node for node in self.cluster.kv_nodes if node.ip != self.cluster.master.ip]
                servers = random.sample(nodes, nodes_out)
                self.servs_out.extend(servers)

        if nodes_in:
            if "cbas" in services:
                servers = random.sample(self.available_servers, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
            if "index" in services:
                servers = random.sample(self.available_servers, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
            if "fts" in services:
                servers = random.sample(self.available_servers, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
            if "query" in services:
                servers = random.sample(self.available_servers, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
            if "eventing" in services:
                servers = random.sample(self.available_servers, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]
            if "kv" in services:
                servers = random.sample(self.available_servers, nodes_in)
                self.servs_in.extend(servers)
                self.available_servers = [servs for servs in self.available_servers
                                          if servs not in servers]

        print "Servers coming in : %s with services: %s" % ([server.ip for server in self.servs_in], services)
        print "Servers going out : %s" % ([server.ip for server in self.servs_out])
        self.available_servers.extend(self.servs_out)
        print "NEW AVAILABLE nodes for cluster: %s" % ([server.ip for server in self.available_servers])
        if nodes_in == nodes_out:
            self.vbucket_check = False

        rebalance_task = self.task.async_rebalance(
            self.cluster, self.servs_in, self.servs_out,
            services=services,
            check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=retry_get_process_num)

        return rebalance_task

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

        doc_ops = doc_ops or self.doc_ops or bucket.loadDefn.get("load_type")
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
            process_concurrency = min(bucket.loadDefn.get("scopes") * bucket.loadDefn.get("collections"), 5)
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
                                          cmd.get("workers", process_concurrency),
                                          cmd.get("ops", bucket.loadDefn.get("ops")),
                                          cmd.get("loadType", None),
                                          cmd.get("keyType", self.key_type),
                                          cmd.get("valueType", self.val_type),
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
                    dg = DocumentGenerator(ws, self.key_type, self.val_type)
                    self.loader_map.update({bucket.name+scope+collection: dg})

    def wait_for_doc_load_completion(self, tasks, buckets, wait_for_stats=True):
        buckets = buckets or self.cluster.buckets
        self.doc_loading_tm.getAllTaskResult()
        for task in tasks:
            task.result = True
            unique_str = "{}:{}:{}:".format(task.sdk.bucket, task.sdk.scope, task.sdk.collection)
            for optype, failures in task.failedMutations.items():
                for failure in failures:
                    if failure is not None:
                        print("Test Retrying {}: {}{} -> {}".format(optype, unique_str, failure.id(), failure.err().getClass().getSimpleName()))
                        retry = 5
                        while retry > 0:
                            retry -= 1
                            try:
                                if optype == "create":
                                    task.docops.insert(failure.id(), failure.document(), task.sdk.connection, task.setOptions)
                                if optype == "update":
                                    task.docops.upsert(failure.id(), failure.document(), task.sdk.connection, task.upsertOptions)
                                if optype == "delete":
                                    task.docops.delete(failure.id(), task.sdk.connection, task.removeOptions)
                                break
                            except (ServerOutOfMemoryException, TimeoutException, RequestCanceledException) as e:
                                print("Retry {} failed for key: {} - {}".format(optype, failure.id(), e))
                                time.sleep(2)
                            except (DocumentNotFoundException, DocumentExistsException) as e:
                                break
                            if retry == 0:
                                task.result = False
            # try:
            #     task.sdk.disconnectCluster()
            # except Exception as e:
            #     print(e)
            self.assertTrue(task.result, "Task Failed: {}".format(task.taskName))
        if wait_for_stats:
            try:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, buckets, timeout=14400)
                # if self.track_failures:
                #     for bucket in buckets:
                #         self.bucket_util.verify_stats_all_buckets(self.cluster,
                #                                                   bucket.final_items,
                #                                                   timeout=14400,
                #                                                   buckets=[bucket])
            except Exception as e:
                if not self.cluster.type == "default":
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
        if self._data_validation:
            self.log.info("Validating Active/Replica Docs")
            cmd = dict()
            self.ops_rate = self.input.param("ops_rate", 2000)
            # if self.cluster.master:
            #     master = Server(self.cluster.master.ip, self.cluster.master.port,
            #                     self.cluster.master.rest_username, self.cluster.master.rest_password,
            #                     str(self.cluster.master.memcached_port))
            self.loader_map = dict()
            for bucket in self.cluster.buckets:
                pc = min(bucket.loadDefn.get("scopes") * bucket.loadDefn.get("collections"), 5)
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
                                                  cmd.get("docSize", self.doc_size),
                                                  cmd.get("cr", 0),
                                                  cmd.get("rd", 100),
                                                  cmd.get("up", 0),
                                                  cmd.get("dl", 0),
                                                  cmd.get("ex", 0),
                                                  cmd.get("workers", pc),
                                                  cmd.get("ops", bucket.loadDefn.get("ops")),
                                                  cmd.get("loadType", None),
                                                  cmd.get("keyType", self.key_type),
                                                  cmd.get("valueType", self.val_type),
                                                  cmd.get("validate", True),
                                                  cmd.get("gtm", False),
                                                  cmd.get("deleted", False),
                                                  cmd.get("mutated", 0))
                            ws.dr = dr
                            dg = DocumentGenerator(ws, self.key_type, self.val_type)
                            self.loader_map.update({bucket.name+scope+collection+op_type: dg})

            tasks = list()
            # i = pc
            # while i > 0:
            for bucket in self.cluster.buckets:
                # if bucket.serverless is not None and bucket.serverless.nebula_endpoint:
                #     nebula = bucket.serverless.nebula_endpoint
                #     print "Serverless Mode, Nebula will be used for SDK operations: %s" % nebula.srv
                #     master = Server(nebula.srv, nebula.port,
                #                     nebula.rest_username,
                #                     nebula.rest_password,
                #                     str(nebula.memcached_port))
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
                    for collection in bucket.scopes[scope].collections.keys():
                        if collection == "_default" and scope == "_default":
                            continue
                        for op_type in bucket.loadDefn.get("load_type"):
                            if op_type not in ["create", "update", "delete"]:
                                continue
                            # client = NewSDKClient(master, bucket.name, scope, collection)
                            # client.initialiseSDK()
                            self.sleep(1)
                            taskName = "Validate_%s_%s_%s_%s_%s" % (bucket.name, scope, collection, op_type, time.time())
                            task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection+op_type],
                                                    self.sdk_client_pool, "NONE",
                                                    self.maxttl, self.time_unit,
                                                    self.track_failures, 0)
                            task.set_collection_for_load(bucket.name, scope, collection)
                            tasks.append(task)
                            self.doc_loading_tm.submit(task)
                            # i -= 1
        self.doc_loading_tm.getAllTaskResult()
        # for task in tasks:
        #     try:
        #         task.sdk.disconnectCluster()
        #     except Exception as e:
        #         print(e)
        for task in tasks:
            self.assertTrue(task.result, "Validation Failed for: %s" % task.taskName)

    def print_crud_stats(self, bucket):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Initial Items",
                                "Current Items",
                                "Items Updated",
                                "Items Created",
                                "Items Deleted",
                                "Items Expired"])
        self.table.add_row([
            str(bucket.initial_items),
            str(bucket.final_items),
            str(abs(bucket.update_start)) + "-" + str(abs(bucket.update_end)),
            str(abs(bucket.create_start)) + "-" + str(abs(bucket.create_end)),
            str(abs(bucket.delete_start)) + "-" + str(abs(bucket.delete_end)),
            str(abs(bucket.expire_start)) + "-" + str(abs(bucket.expire_end))
            ])
        self.table.display("Docs statistics")

    def perform_load(self, crash=False, num_kills=1, wait_for_load=True,
                     validate_data=True, buckets=None, overRidePattern=None):
        buckets = buckets or self.cluster.buckets
        self.get_memory_footprint()
        self._loader_dict(buckets, overRidePattern)
        tasks = list()
        for bucket in buckets:
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    if collection == "_default" and scope == "_default":
                        continue
                    self.sleep(1)
                    taskName = "Loader_%s_%s_%s_%s" % (bucket.name, scope, collection, time.time())
                    task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection],
                                            self.sdk_client_pool, self.durability_level,
                                            self.maxttl, self.time_unit,
                                            self.track_failures, 0)
                    task.set_collection_for_load(bucket.name, scope, collection)
                    tasks.append(task)
                    self.doc_loading_tm.submit(task)

        if wait_for_load:
            self.wait_for_doc_load_completion(tasks, buckets)
            self.get_memory_footprint()
        else:
            return tasks

        if crash:
            self.kill_memcached(num_kills=num_kills)

        if validate_data:
            self.data_validation()

        self.print_stats(buckets)

        if self.cluster.type != "default":
            return

        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if result:
            self.PrintStep("CRASH | CRITICAL | WARN messages found in cb_logs")
            if self.assert_crashes_on_load:
                self.task_manager.abort_all_tasks()
                self.doc_loading_tm.abortAllTasks()
                self.assertFalse(result)

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

    def print_stats(self, buckets):
        if self.cluster.type != "serverless":
            self.bucket_util.print_bucket_stats(self.cluster)
            self.cluster_util.print_cluster_stats(self.cluster)
        for bucket in buckets:
            self.print_crud_stats(bucket)
            if self.cluster.type != "serverless":
                self.get_bucket_dgm(bucket)
            if bucket.storageBackend == Bucket.StorageBackend.magma and \
                self.cluster.type == "default":
                    self.get_magma_disk_usage(bucket)
                    # self.check_fragmentation_using_magma_stats(bucket)
                    self.check_fragmentation_using_kv_stats(bucket)

    def PrintStep(self, msg=None):
        print "\n"
        print "\t", "#"*60
        print "\t", "#"
        print "\t", "#  %s" % msg
        print "\t", "#"
        print "\t", "#"*60
        print "\n"

    def check_fragmentation_using_kv_stats(self, bucket, servers=None):
        result = dict()
        if servers is None:
            servers = self.cluster.kv_nodes + [self.cluster.master]
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            frag_val = self.bucket_util.get_fragmentation_kv(
                self.cluster, bucket, server)
            self.log.debug("Current Fragmentation for node {} is {} \
            ".format(server.ip, frag_val))
            result.update({server.ip: frag_val})
        self.log.info("KV stats fragmentation values {}".format(result))

    def dump_magma_stats(self, server, bucket, shard, kvstore):
        if bucket.storageBackend != Bucket.StorageBackend.magma \
            or self.cluster.type != "default":
            return
        shell = RemoteMachineShellConnection(server)
        data_path = RestConnection(server).get_data_path()
        while not self.stop_stats:
            for bucket in self.cluster.buckets:
                self.log.info(self.get_magma_stats(bucket, server)[server.ip]["rw_0:magma"])
                self.dump_seq_index(shell, data_path, bucket.name, shard, kvstore)
            self.sleep(600)
        shell.disconnect()

    def dump_seq_index(self, shell, data_path, bucket, shard, kvstore):
        magma_path = os.path.join(data_path, bucket, "magma.{}")
        magma = magma_path.format(shard)
        cmd = '/opt/couchbase/bin/magma_dump {}'.format(magma)
        cmd += ' --kvstore {} --tree seq'.format(kvstore)
        result = shell.execute_command(cmd)[0]
        self.log.info("Seq Tree for {}:{}:{}:{}: \n{}".format(shell.ip, bucket, shard, kvstore, result))

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        stats = list()
        if servers is None:
            servers = self.cluster.kv_nodes + [self.cluster.master]
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
                _res = self.get_magma_stats(bucket, server)
                fragmentation_values.append(json.loads(_res[server.ip][grep_field])["Fragmentation"])
                stats.append(_res)
            result.update({server.ip: fragmentation_values})
        res = list()
        for value in result.values():
            res.append(max(value))
        if max(res) < float(self.fragmentation)/100:
            self.log.info("magma stats fragmentation result {} \
            ".format(result))
            return True
        self.log.info("magma stats fragmentation result {} \
        ".format(result))
        return False

    def get_magma_stats(self, bucket, server=None):
        magma_stats_for_all_servers = dict()
        cbstat_obj = Cbstats(server)
        result = cbstat_obj.magma_stats(bucket.name)
        magma_stats_for_all_servers[server.ip] = result
        return magma_stats_for_all_servers

    def pause_rebalance(self):
        rest = RestConnection(self.cluster.master)
        i = 1
        self.sleep(10, "Let the rebalance begin!")
        expected_progress = 20
        while expected_progress < 100:
            expected_progress = 20 * i
            reached = self.cluster_util.rebalance_reached(
                rest, expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))
            if not self.cluster_util.is_cluster_rebalanced(rest):
                self.log.info("Stop the rebalance")
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, msg="Unable to stop rebalance")
                rebalance_task = self.task.async_rebalance(self.cluster,[], [],
                                                           retry_get_process_num=3000)
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
            reached = self.cluster_util.rebalance_reached(
                rest, expected_progress, wait_step=10, num_retry=3600)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))

            if not self.cluster_util.is_cluster_rebalanced(rest):
                self.log.info("Abort rebalance")
                self._induce_error(error_type, self.cluster.nodes_in_cluster)
                result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
                if result:
                    self.task_manager.abort_all_tasks()
                    self.doc_loading_tm.abortAllTasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                self.sleep(60, "Sleep after error introduction")
                self._recover_from_error(error_type)
                result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
                if result:
                    self.task_manager.abort_all_tasks()
                    self.doc_loading_tm.abortAllTasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                try:
                    self.task_manager.get_task_result(rebalance_task)
                except RebalanceFailedException:
                    pass
                if rebalance.result:
                    self.log.error("Rebalance passed/finished which is not expected")
                    self.log.info("Rebalance % after rebalance finished = {}".
                                  format(expected_progress))
                    return None
                else:
                    self.log.info("Restarting Rebalance after killing at {}".
                                  format(expected_progress))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster, [], self.servs_out,
                        retry_get_process_num=3000)
                    self.sleep(120, "Let the rebalance begin after abort")
                    self.log.info("Rebalance % = {}".
                                  format(self.rest._rebalance_progress()))
            i += 1
        return rebalance_task

    def crash_memcached(self, nodes=None, num_kills=1, graceful=False):
        self.stop_crash = False
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.kv_nodes + [self.cluster.master]

        while not self.stop_crash:
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
            servers = self.cluster.kv_nodes + [self.cluster.master]

        for server in servers:
            for _ in xrange(num_kills):
                if num_kills > 1:
                    self.sleep(2, "Sleep for 2 seconds b/w cont memc kill on same node.")
                shell = RemoteMachineShellConnection(server)
                if graceful:
                    shell.restart_couchbase()
                else:
                    shell.kill_memcached()
                shell.disconnect()
            self.sleep(5, "Sleep for 5 seconds before killing memc on next node.")

        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        if result:
            self.stop_crash = True
            self.task_manager.abort_all_tasks()
            self.doc_loading_tm.abortAllTasks()
            self.assertFalse(
                result,
                "CRASH | CRITICAL | WARN messages found in cb_logs")

        if wait:
            for server in servers:
                self.check_warmup_complete(server)

    def check_warmup_complete(self, server):
        for bucket in self.cluster.buckets:
            start_time = time.time()
            result = self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[server],
                wait_time=self.wait_timeout * 20)
            if not result:
                self.stop_crash = True
                self.task_manager.abort_all_tasks()
                self.doc_loading_tm.abortAllTasks()
                self.assertTrue(result, "Warm-up failed in %s seconds"
                                % (self.wait_timeout * 20))
            else:
                self.log.info("Bucket:%s warm-up completed in %s." %
                              (bucket.name, str(time.time() - start_time)))
