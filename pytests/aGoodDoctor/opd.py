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
from com.couchbase.test.sdk import Server, SDKClientPool
# from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.docgen import WorkLoadSettings
from com.couchbase.test.docgen import DocumentGenerator
from com.couchbase.test.loadgen import WorkLoadGenerate
from com.couchbase.test.docgen import DocRange
from java.util import HashMap
from com.couchbase.test.docgen import DRConstants
from com.couchbase.client.core.error import DocumentExistsException,\
    TimeoutException, DocumentNotFoundException, ServerOutOfMemoryException
import time
from custom_exceptions.exception import RebalanceFailedException,\
    ServerUnavailableException
from constants.cb_constants.CBServer import CbServer
from threading import Thread
import threading
from capella_utils.dedicated import CapellaUtils as DedicatedUtils
from TestInput import TestInputServer
from gsiLib.gsiHelper import GsiHelper
from _collections import defaultdict
from memcached.helper.data_helper import MemcachedClientHelper


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

    def create_required_buckets(self, cluster, sdk_init=True):
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

            bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
            if bucket.loadDefn.get("name"):
                bucket.name = bucket.loadDefn.get("name")
            self.bucket_util.create_bucket(cluster, bucket)

        if sdk_init:
            cluster.sdk_client_pool = SDKClientPool()
            num_clients = self.input.param("clients_per_db",
                                           min(5, bucket.loadDefn.get("collections")))
            for bucket in cluster.buckets:
                self.create_sdk_client_pool(cluster, [bucket],
                                            num_clients)
        self.create_required_collections(cluster)

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
                node = cluster.master
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

    def create_sdk_client_pool(self, cluster, buckets, req_clients_per_bucket):
        for bucket in buckets:
            self.log.info("Using SDK endpoint %s" % cluster.master.ip)
            server = Server(cluster.master.ip, cluster.master.port,
                            cluster.master.rest_username,
                            cluster.master.rest_password,
                            str(cluster.master.memcached_port))
            cluster.sdk_client_pool.create_clients(
                bucket.name, server, req_clients_per_bucket)
            bucket.clients = cluster.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
        self.sleep(1, "Wait for SDK client pool to warmup")

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

    def get_bucket_dgm(self, cluster, bucket):
        self.rest_client = BucketHelper(cluster.master)
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
            elif error_condition == "kill_indexer":
                shell = RemoteMachineShellConnection(node)
                shell.kill_indexer()
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
        if error_condition == "kill_memcached":
            for node in self.cluster.kv_nodes + [self.cluster.master]:
                self.check_warmup_complete(node)
                result = self.cluster_util.wait_for_ns_servers_or_assert([node],
                                                                         wait_time=1200)
                self.assertTrue(result, "Server warmup failed")
        elif error_condition == "kill_indexer":
            self.recover_indexer()

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
        if "index" in services:
            services=["index,n1ql"] * len(services)

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
            retry_get_process_num=retry_get_process_num,
            validate_bucket_ranking=False)

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
                bucket.expire_start = bucket.end
            bucket.start = bucket.expire_start
            if expire_end is not None:
                bucket.expire_end = expire_end
            else:
                bucket.expire_end = bucket.expire_start + bucket.loadDefn.get("num_items")/2 * self.mutation_perc/100
            bucket.end = bucket.expire_end
            # bucket.final_items -= (bucket.expire_end - bucket.expire_start) * bucket.loadDefn.get("collections") * bucket.loadDefn.get("scopes")

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

    def _loader_dict(self, cluster, buckets, overRidePattern=None, cmd={},
                     skip_default=True,
                     overWriteValType=None):
        self.loader_map = dict()
        self.default_pattern = [100, 0, 0, 0, 0]
        buckets = buckets or cluster.buckets
        for bucket in buckets:
            pattern = overRidePattern or bucket.loadDefn.get("pattern", self.default_pattern)
            for scope in bucket.scopes.keys():
                for i, collection in enumerate(bucket.scopes[scope].collections.keys()):
                    workloads = bucket.loadDefn.get("collections_defn", [bucket.loadDefn])
                    valType = overWriteValType or workloads[i % len(workloads)]["valType"]
                    dim = workloads[i % len(workloads)].get("dim", self.dim)
                    if scope == CbServer.system_scope:
                        continue
                    if collection == "_default" and scope == "_default" and skip_default:
                        continue
                    ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                          cmd.get("keySize", self.key_size),
                                          cmd.get("docSize", bucket.loadDefn.get("doc_size")),
                                          cmd.get("cr", pattern[0]),
                                          cmd.get("rd", pattern[1]),
                                          cmd.get("up", pattern[2]),
                                          cmd.get("dl", pattern[3]),
                                          cmd.get("ex", pattern[4]),
                                          cmd.get("workers", self.process_concurrency),
                                          cmd.get("ops", bucket.loadDefn.get("ops")),
                                          cmd.get("loadType", None),
                                          cmd.get("keyType", self.key_type),
                                          cmd.get("valueType", valType),
                                          cmd.get("validate", False),
                                          cmd.get("gtm", self.gtm),
                                          cmd.get("deleted", False),
                                          cmd.get("mutated", 0),
                                          cmd.get("model", self.model),
                                          cmd.get("mockVector", self.mockVector),
                                          cmd.get("dim", dim),
                                          cmd.get("base64", self.base64),
                                          "None",
                                          0,
                                          self.siftFileName
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
                    dg = DocumentGenerator(ws, self.key_type, valType)
                    self.loader_map.update({bucket.name+scope+collection: dg})

    def wait_for_doc_load_completion(self, cluster, tasks, wait_for_stats=True):
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
            # try:
            #     task.sdk.disconnectCluster()
            # except Exception as e:
            #     print(e)
            self.assertTrue(task.result, "Task Failed: {}".format(task.taskName))
        if wait_for_stats:
            try:
                self.bucket_util._wait_for_stats_all_buckets(
                    cluster, cluster.buckets, timeout=28800)
                if self.track_failures and cluster.type == "default":
                    for bucket in cluster.buckets:
                        self.bucket_util.verify_stats_all_buckets(
                            cluster, bucket.final_items, timeout=28800,
                            buckets=[bucket])
            except Exception as e:
                if self.cluster.type == "default":
                    self.get_gdb()
                raise e

    def load_sift_data(self, wait_for_load=True,
                     validate_data=True, cluster=None, buckets=None, overRidePattern=None, skip_default=True,
                     wait_for_stats=True,
                     override_num_items=None):
        cmd = {}
        tasks = list()
        steps = [0, 1000000, 2000000, 5000000, 10000000, 20000000, 50000000, 100000000, 200000000, 500000000, 1000000000]
        buckets = buckets or cluster.buckets
        coll_order = self.input.param("coll_order", 1)
        for bucket in buckets:
            pattern = overRidePattern or bucket.loadDefn.get("pattern")
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                collections = bucket.scopes[scope].collections.keys()
                if "_default" in collections:
                    collections.remove("_default")
                for i, collection in enumerate(sorted(collections)):
                    workloads = bucket.loadDefn.get("collections_defn", [bucket.loadDefn])
                    if coll_order == -1:
                        workloads = list(reversed(workloads))
                    workload = workloads[i % len(workloads)]
                    valType = workload["valType"]
                    dim = workload.get("dim", self.dim)
                    if collection == "_default" and scope == "_default" and skip_default:
                        continue
                    end_offset = 0
                    if cmd.get("cr", "0") > 0:
                        end_offset = override_num_items or workload.get("num_items")
                    elif cmd.get("up", "0") > 0:
                        end_offset = override_num_items or workload.get("num_items")

                    for k in range(len(steps)):
                        if end_offset > steps[k]:
                            for i in range(self.process_concurrency):
                                ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                                      cmd.get("keySize", self.key_size),
                                                      cmd.get("docSize", bucket.loadDefn.get("doc_size")),
                                                      cmd.get("cr", pattern[0]),
                                                      cmd.get("rd", pattern[1]),
                                                      cmd.get("up", pattern[2]),
                                                      cmd.get("dl", pattern[3]),
                                                      cmd.get("ex", pattern[4]),
                                                      cmd.get("workers", self.process_concurrency),
                                                      cmd.get("ops", bucket.loadDefn.get("ops")),
                                                      cmd.get("loadType", None),
                                                      cmd.get("keyType", self.key_type),
                                                      cmd.get("valueType", valType),
                                                      cmd.get("validate", False),
                                                      cmd.get("gtm", self.gtm),
                                                      cmd.get("deleted", False),
                                                      cmd.get("mutated", self.mutate),
                                                      cmd.get("model", self.model),
                                                      cmd.get("mockVector", self.mockVector),
                                                      cmd.get("dim", dim),
                                                      cmd.get("base64", self.base64),
                                                      "None",
                                                      0,
                                                      self.siftFileName
                                                      )
                                step = (steps[k+1] - steps[k])/self.process_concurrency;
                                start = steps[k] + step * i;
                                end = min(steps[k] + step * (i+1), end_offset);
                                hm = HashMap()
                                hm.putAll({DRConstants.create_s: start,
                                           DRConstants.create_e: end,
                                           DRConstants.update_s: start,
                                           DRConstants.update_e: end})
                                dr = DocRange(hm)
                                ws.dr = dr
                                dg = DocumentGenerator(ws, self.key_type, valType)
                                if pattern[0] > 0:
                                    taskName = "Loader_%s_%s_%s_%s_create_%s_%s" % (bucket.name, scope, collection, k, ws.dr.create_s, ws.dr.create_e)
                                else:
                                    taskName = "Loader_%s_%s_%s_%s_update_%s_%s" % (bucket.name, scope, collection, k, ws.dr.update_s, ws.dr.update_e)
                                task = WorkLoadGenerate(taskName, dg,
                                                        cluster.sdk_client_pool, self.esClient, "NONE",
                                                        self.maxttl, self.time_unit,
                                                        self.track_failures, 0)
                                task.set_collection_for_load(bucket.name, scope, collection)
                                tasks.append(task)
                                self.doc_loading_tm.submit(task)
                                i -= 1
                        else:
                            break
        if wait_for_load:
            self.wait_for_doc_load_completion(cluster, tasks, wait_for_stats)
            self.get_memory_footprint()
        else:
            return tasks

        if validate_data:
            self.data_validation(cluster, skip_default=skip_default)

        self.bucket_util.print_bucket_stats(cluster)
        self.cluster_util.print_cluster_stats(cluster)

    def get_gdb(self):
        for node in self.cluster.kv_nodes:
            gdb_shell = RemoteMachineShellConnection(node)
            gdb_out = gdb_shell.execute_command('gdb -p `(pidof memcached)` -ex "thread apply all bt" -ex detach -ex quit')[0]
            print node.ip
            print gdb_out
            gdb_shell.disconnect()

    def data_validation(self, cluster, skip_default=True):
        pc = min(self.process_concurrency, 20)
        if self._data_validation:
            self.log.info("Validating Active/Replica Docs")
            cmd = dict()
            self.ops_rate = self.input.param("ops_rate", 2000)
            # master = Server(self.cluster.master.ip, self.cluster.master.port,
            #                 self.cluster.master.rest_username, self.cluster.master.rest_password,
            #                 str(self.cluster.master.memcached_port))
            self.loader_map = dict()
            for bucket in cluster.buckets:
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                            continue
                    for i, collection in enumerate(bucket.scopes[scope].collections.keys()):
                        workloads = bucket.loadDefn.get("collections_defn", [bucket.loadDefn])
                        valType = workloads[i % len(workloads)]["valType"]
                        if collection == "_default" and scope == "_default" and skip_default:
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
                            elif op_type == "expiry":
                                hm.putAll({DRConstants.read_s: bucket.expire_start,
                                           DRConstants.read_e: bucket.expire_end})
                                cmd.update({"deleted": True})
                            else:
                                continue
                            dr = DocRange(hm)
                            ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                                                  cmd.get("keySize", self.key_size),
                                                  cmd.get("docSize", bucket.loadDefn.get("doc_size")),
                                                  cmd.get("cr", 0),
                                                  cmd.get("rd", 100),
                                                  cmd.get("up", 0),
                                                  cmd.get("dl", 0),
                                                  cmd.get("ex", 0),
                                                  cmd.get("workers", pc),
                                                  cmd.get("ops", bucket.original_ops),
                                                  cmd.get("loadType", None),
                                                  cmd.get("keyType", self.key_type),
                                                  cmd.get("valueType", valType),
                                                  cmd.get("validate", True),
                                                  cmd.get("gtm", False),
                                                  cmd.get("deleted", False),
                                                  cmd.get("mutated", 0))
                            ws.dr = dr
                            dg = DocumentGenerator(ws, self.key_type, valType)
                            self.loader_map.update({bucket.name+scope+collection+op_type: dg})

            tasks = list()
            i = pc
            while i > 0:
                for bucket in cluster.buckets:
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
                        for collection in bucket.scopes[scope].collections.keys():
                            if collection == "_default" and scope == "_default" and skip_default:
                                continue
                            for op_type in bucket.loadDefn.get("load_type"):
                                if op_type not in ["create", "update", "delete", "expiry"]:
                                    continue
                                # client = NewSDKClient(master, bucket.name, scope, collection)
                                # client.initialiseSDK()
                                # self.sleep(1)
                                taskName = "Validate_%s_%s_%s_%s_%s_%s" % (bucket.name, scope, collection, op_type, str(i), time.time())
                                task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection+op_type],
                                                        cluster.sdk_client_pool, "NONE",
                                                        self.maxttl, self.time_unit,
                                                        self.track_failures, 0)
                                task.set_collection_for_load(bucket.name, scope, collection)
                                tasks.append(task)
                                self.doc_loading_tm.submit(task)
                                i -= 1
            return tasks

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
                     validate_data=True, cluster=None, buckets=None, overRidePattern=None, skip_default=True,
                     wait_for_stats=True,
                     overWriteValType=None):
        self.get_memory_footprint()
        buckets = buckets or cluster.buckets
        self._loader_dict(cluster, buckets, overRidePattern, skip_default=skip_default,
                          overWriteValType=overWriteValType)
        # master = Server(self.cluster.master.ip, self.cluster.master.port,
        #                 self.cluster.master.rest_username, self.cluster.master.rest_password,
        #                 str(self.cluster.master.memcached_port))
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
                        if collection == "_default" and scope == "_default" and skip_default:
                            continue
                        # client = NewSDKClient(master, bucket.name, scope, collection)
                        # client.initialiseSDK()
                        # self.sleep(1)
                        taskName = "Loader_%s_%s_%s_%s" % (bucket.name, scope, collection, time.time())
                        task = WorkLoadGenerate(taskName, self.loader_map[bucket.name+scope+collection],
                                                cluster.sdk_client_pool, self.esClient,
                                                self.durability_level,
                                                self.maxttl, self.time_unit,
                                                self.track_failures, 0)
                        task.set_collection_for_load(bucket.name, scope, collection)
                        tasks.append(task)
                        self.doc_loading_tm.submit(task)
                        i -= 1

        if wait_for_load:
            self.wait_for_doc_load_completion(cluster, tasks, wait_for_stats)
            self.get_memory_footprint()
        else:
            return tasks

        if validate_data:
            self.data_validation(cluster, skip_default=skip_default)

        self.print_stats(cluster)

        if self.cluster.type != "default":
            return

        result = self.check_coredump_exist(cluster.nodes_in_cluster)
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

    def print_stats_loop(self, cluster, step=300):
        while not self.stop_run:
            try:
                self.print_stats(cluster)
            except:
                pass
            time.sleep(step)

    def print_stats(self, cluster):
        self.bucket_util.print_bucket_stats(cluster)
        self.cluster_util.print_cluster_stats(cluster)
        if self.val_type != "siftBigANN":
            self.print_crud_stats(cluster.buckets)
        for bucket in cluster.buckets:
            self.get_bucket_dgm(cluster, bucket)
            if bucket.storageBackend == Bucket.StorageBackend.magma and \
                cluster.type == "default":
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
        cbstat_obj.disconnect()
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
                self.cluster.master, expected_progress)
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

    def abort_rebalance(self, rebalance, error_type="kill_memcached", nodes=None):
        self.sleep(30, "Let the rebalance begin!")
        nodes = nodes or self.cluster.nodes_in_cluster
        rest = RestConnection(self.cluster.master)
        i = 1
        expected_progress = 20
        rebalance_task = rebalance
        while expected_progress < 80:
            expected_progress = 20 * i
            reached = self.cluster_util.rebalance_reached(
                self.cluster.master, expected_progress, wait_step=10, num_retry=3600)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))

            if not self.cluster_util.is_cluster_rebalanced(rest):
                self.log.info("Abort rebalance")
                self._induce_error(error_type, nodes)
                result = self.check_coredump_exist(nodes)
                if result:
                    self.task_manager.abort_all_tasks()
                    self.doc_loading_tm.abortAllTasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
                self.sleep(60, "Sleep after error introduction")
                self._recover_from_error(error_type)
                result = self.check_coredump_exist(nodes)
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
                if rebalance_task.result:
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
                                  format(rest._rebalance_progress()))
            i += 1
        return rebalance_task

    def crash_indexer(self, nodes=None, num_kills=1, graceful=False):
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.index_nodes

        while self.crash_count < self.crashes:
            sleep = random.randint(60, 120)
            self.sleep(sleep,
                       "Iteration:{} waiting to kill memc on all nodes".
                       format(self.crash_count))
            self.kill_memcached(num_kills=num_kills,
                                graceful=graceful, wait=True,
                                services=["kv"])
            self.check_index_pending_mutations()
            self.kill_memcached(nodes, num_kills=num_kills,
                                graceful=graceful, wait=True,
                                services=["indexer"])
            self.recover_indexer()
            self.check_index_pending_mutations()
            self.crash_count += 1
        self.sleep(300)

    def recover_indexer(self):
        for bucket in self.cluster.buckets:
            d = defaultdict(list)
            for key, val in bucket.indexes.items():
                _, _, _, _, c = val
                d[c].append(key)
            rest = GsiHelper(self.cluster.master, self.log)
            status = False
            for collection in sorted(d.keys()):
                for index_name in sorted(d.get(collection)):
                    status = rest.polling_create_index_status(
                        bucket, index_name, 1200)
                    print("index: {}, status: {}".format(index_name, status))
                    if status is True:
                        self.log.info("2i index is ready: {}".format(index_name))

    def crash_memcached(self, nodes=None, num_kills=1, graceful=False):
        self.stop_crash = False
        self.crash_count = 0
        if not nodes:
            nodes = self.cluster.kv_nodes

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
                       graceful=False, wait=True, services=["kv"]):
        if not servers:
            servers = self.cluster.kv_nodes

        for server in servers:
            for _ in xrange(num_kills):
                if num_kills > 1:
                    self.sleep(2, "Sleep for 2 seconds b/w cont memc kill on same node.")
                shell = RemoteMachineShellConnection(server)
                if graceful:
                    shell.restart_couchbase()
                if "kv" in services:
                    shell.kill_memcached()
                if "indexer" in services:
                    shell.kill_indexer()
                shell.disconnect()
            self.sleep(5, "Sleep for 5 seconds before killing memc on next node.")

        result = self.check_coredump_exist(servers)
        if result:
            self.stop_crash = True
            self.task_manager.abort_all_tasks()
            self.doc_loading_tm.abortAllTasks()
            self.assertFalse(
                result,
                "CRASH | CRITICAL | WARN messages found in cb_logs")

        if wait and "kv" in services:
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

    def restart_query_load(self, cluster, num=10):
        self.log.info("Changing query load by: {}".format(num))
        for ql in self.ql:
            ql.stop_query_load()
        for ql in self.ftsQL:
            ql.stop_query_load()
        for ql in self.cbasQL:
            ql.stop_query_load()
        self.sleep(10)
        for bucket in cluster.buckets:
            services = self.input.param("services", "data")
            if (cluster.index_nodes or "index" in services) and bucket.loadDefn.get("2iQPS", 0) > 0:
                bucket.loadDefn["2iQPS"] = bucket.loadDefn["2iQPS"] + num
                ql = [ql for ql in self.ql if ql.bucket == bucket][0]
                ql.start_query_load()
            if (cluster.fts_nodes or "search" in services) and bucket.loadDefn.get("ftsQPS", 0) > 0:
                bucket.loadDefn["ftsQPS"] = bucket.loadDefn["ftsQPS"] + num
                ql = [ql for ql in self.ftsQL if ql.bucket == bucket][0]
                ql.start_query_load()
            if (cluster.cbas_nodes or "analytics" in services) and bucket.loadDefn.get("cbasQPS", 0) > 0:
                bucket.loadDefn["cbasQPS"] = bucket.loadDefn["cbasQPS"] + num
                ql = [ql for ql in self.cbasQL if ql.bucket == bucket][0]
                ql.start_query_load()

    def monitor_query_status(self, print_duration=120):
        self.query_result = True

        def check_query_stats():
            st_time = time.time()
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    self.query_table = TableView(self.log.info)
                    self.table = TableView(self.log.info)
                    self.table.set_headers(["Bucket",
                                            "Total Queries",
                                            "Failed Queries",
                                            "Success Queries",
                                            "Rejected Queries",
                                            "Cancelled Queries",
                                            "Timeout Queries",
                                            "Errored Queries"])
                    for ql in self.ql:
                        self.query_table.set_headers(["Bucket",
                                                      "Query",
                                                      "Count",
                                                      "nProbe",
                                                      "Filtering(M)",
                                                      "Avg Execution Time(ms)",
                                                      "Latest 1K - Avg Execution Time(ms)",
                                                      "Avg Accuracy",
                                                      "Avg Recall",
                                                      "ES Avg Execution Time(ms)",
                                                      "ES Avg Recall"
                                                      ])
                        try:
                            for query, _ in sorted(ql.bucket.query_map.items(), key=lambda x: x[1]["identifier"]):
                                with ql.query_stats[query][5]:
                                    if ql.query_stats[query][1] > 0:
                                        self.query_table.add_row(
                                            [str(ql.bucket.name),
                                             ql.bucket.query_map[query]["identifier"],
                                             ql.query_stats[query][1],
                                             ql.bucket.query_map[query]["vector_defn"].get("nProbe"),
                                             ql.query_stats[query][4],
                                             round(ql.query_stats[query][0]/ql.query_stats[query][1], 2),
                                             round(sum(ql.query_stats[query][8][-1000:])/min(1000,len(ql.query_stats[query][8][-1000:])), 2),
                                             round(ql.query_stats[query][2]/ql.query_stats[query][1], 2),
                                             round(ql.query_stats[query][3]/ql.query_stats[query][1], 2),
                                             round(ql.query_stats[query][6]/ql.query_stats[query][1], 2),
                                             round(ql.query_stats[query][7]/ql.query_stats[query][1], 2)
                                                                        ])
                        except Exception as e:
                            print(e)
                        self.table.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        if ql.failures > 0:
                            self.query_result = False
                    self.table.display("N1QL Results Stats")
                    self.query_table.display("N1QL Performance Stats")

                    self.FTStable = TableView(self.log.info)
                    self.FTStable.set_headers(["Bucket",
                                               "Total Queries",
                                               "Failed Queries",
                                               "Success Queries",
                                               "Rejected Queries",
                                               "Cancelled Queries",
                                               "Timeout Queries",
                                               "Errored Queries"])
                    for ql in self.ftsQL:
                        self.FTStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        if ql.failures > 0:
                            self.query_result = False
                    self.FTStable.display("FTS Query Result Stats")

                    self.CBAStable = TableView(self.log.info)
                    self.CBAStable.set_headers(["Bucket",
                                                "Total Queries",
                                                "Failed Queries",
                                                "Success Queries",
                                                "Rejected Queries",
                                                "Cancelled Queries",
                                                "Timeout Queries",
                                                "Errored Queries"])
                    self.cbas_query_perf = TableView(self.log.info)
                    self.cbas_query_perf.set_headers(["Bucket",
                                                      "Query",
                                                      "Count",
                                                      "Avg Execution Time(ms)"])
                    for ql in self.cbasQL:
                        self.CBAStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        try:
                            for query in sorted(ql.query_stats.keys()):
                                if ql.query_stats[query][1] > 0:
                                    self.cbas_query_perf.add_row([str(ql.bucket.name),
                                                                  ql.bucket.query_map[query][0],
                                                                  ql.query_stats[query][1],
                                                                  ql.query_stats[query][0]/ql.query_stats[query][1]])
                        except Exception as e:
                            print(e)
                        if ql.failures > 0:
                            self.query_result = False
                    self.CBAStable.display("CBAS Query Result Stats")
                    self.cbas_query_perf.display("CBAS Query Performance")

                    st_time = time.time()
                    time.sleep(10)

        query_monitor = threading.Thread(target=check_query_stats)
        query_monitor.start()

    def refresh_cluster(self, tenant, cluster, type="dedicated"):
        while True:
            if cluster.nodes_in_cluster:
                self.log.info("Cluster Nodes: {}".format(cluster.nodes_in_cluster))
                try:
                    cluster.refresh_object(self.cluster_util.get_nodes(
                        random.choice(cluster.nodes_in_cluster)))
                    break
                except ServerUnavailableException:
                    pass
                except IndexError:
                    pass
            else:
                self.log.critical("Cluster object: Nodes in cluster are reset by rebalance task.")
                self.sleep(30)
                self.servers = DedicatedUtils.get_nodes(
                    self.pod, tenant, cluster.id)
                nodes = list()
                for server in self.servers:
                    temp_server = TestInputServer()
                    temp_server.ip = server.get("hostname")
                    temp_server.hostname = server.get("hostname")
                    temp_server.services = server.get("services")
                    temp_server.port = "18091"
                    temp_server.rest_username = cluster.username
                    temp_server.rest_password = cluster.password
                    temp_server.hosted_on_cloud = True
                    temp_server.memcached_port = "11207"
                    temp_server.type = type
                    if type == "columnar":
                        temp_server.cbas_port = 18095
                    nodes.append(temp_server)
                cluster.refresh_object(nodes)

    def trigger_rollback(self):
        mem_only_items = 100000
        rollbacks = 0
        while rollbacks < 20:
            self.PrintStep("Running Rollback: %s" % rollbacks)
            for i, node in enumerate(self.cluster.kv_nodes):
                self.key = "rollback_docs_%s_%s-" % (rollbacks, i)
                # Stopping persistence on NodeA
                for bucket in self.cluster.buckets:
                    mem_client = MemcachedClientHelper.direct_client(
                        node, bucket)
                    mem_client.stop_persistence()

                if self.val_type == "siftBigANN":
                    self.load_sift_data(cluster=self.cluster,
                                  buckets=self.cluster.buckets,
                                  overRidePattern=[100,0,0,0,0],
                                  validate_data=False,
                                  wait_for_stats=False,
                                  override_num_items=mem_only_items)
                else:
                    self.normal_load()

                self.check_index_pending_mutations()
                # Kill memcached on NodeA to trigger rollback on other Nodes
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()

                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0],
                    servers=[node],
                    wait_time=self.wait_timeout * 10))
                self.sleep(10, "Not Required, but waiting for 10s after warm up")
                self.check_index_pending_mutations()
                result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
                if result:
                    self.stop_crash = True
                    self.task_manager.abort_all_tasks()
                    self.doc_loading_tm.abortAllTasks()
                    self.assertFalse(
                        result,
                        "CRASH | CRITICAL | WARN messages found in cb_logs")
            rollbacks += 1

        self.key = self.input.param("key", "test_docs-")
