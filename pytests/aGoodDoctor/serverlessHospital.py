'''
Created on May 2, 2022

@author: ritesh.agarwal
'''

from basetestcase import BaseTestCase
from aGoodDoctor.serverlessn1ql import DoctorN1QL, QueryLoad
from aGoodDoctor.serverlessOpd import OPD
import os
from BucketLib.bucket import Bucket
from aGoodDoctor.serverlessfts import DoctorFTS, FTSQueryLoad
import time
import pprint
from membase.api.rest_client import RestConnection
from org.xbill.DNS import Lookup, Type
import json
import threading
from table_view import TableView
from BucketLib.BucketOperations import BucketHelper
from com.couchbase.test.sdk import Server
from com.couchbase.client.core.error import TimeoutException
from threading import Lock
from _collections import defaultdict
import subprocess
import shlex


class Murphy(BaseTestCase, OPD):

    def init_doc_params(self):
        self.create_perc = self.input.param("create_perc", 100)
        self.update_perc = self.input.param("update_perc", 20)
        self.delete_perc = self.input.param("delete_perc", 20)
        self.expiry_perc = self.input.param("expiry_perc", 20)
        self.read_perc = self.input.param("read_perc", 20)
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
        try:
            BaseTestCase.setUp(self)
            self.init_doc_params()
            self.ql = []
            self.ftsQL = []
            self.drIndexes = []
            self.num_collections = self.input.param("num_collections", 1)
            self.xdcr_collections = self.input.param("xdcr_collections", self.num_collections)
            self.num_collections_bkrs = self.input.param("num_collections_bkrs", self.num_collections)
            self.num_scopes = self.input.param("num_scopes", 1)
            self.xdcr_scopes = self.input.param("xdcr_scopes", self.num_scopes)
            self.kv_nodes = self.nodes_init
            self.cbas_nodes = self.input.param("cbas_nodes", 0)
            self.fts_nodes = self.input.param("fts_nodes", 0)
            self.index_nodes = self.input.param("index_nodes", 0)
            self.backup_nodes = self.input.param("backup_nodes", 0)
            self.xdcr_remote_nodes = self.input.param("xdcr_remote_nodes", 0)
            self.num_indexes = self.input.param("num_indexes", 0)
            self.mutation_perc = 100
            self.threads_calculation()
            self.op_type = self.input.param("op_type", "create")
            self.dgm = self.input.param("dgm", None)
            self.mutate = 0
            self.iterations = self.input.param("iterations", 10)
            self.step_iterations = self.input.param("step_iterations", 1)
            self.rollback = self.input.param("rollback", True)
            self.vbucket_check = self.input.param("vbucket_check", True)
            self.end_step = self.input.param("end_step", None)
            self.keyType = self.input.param("keyType", "SimpleKey")
            self.crashes = self.input.param("crashes", 20)
            self.check_dump_thread = True
            self.skip_read_on_error = False
            self.suppress_error_table = False
            self.track_failures = self.input.param("track_failures", True)
            self.loader_dict = None
            self.parallel_reads = self.input.param("parallel_reads", False)
            self._data_validation = self.input.param("data_validation", True)
            self.fragmentation = int(self.input.param("fragmentation", 50))
            self.key_type = self.input.param("key_type", "SimpleKey")
            self.ops_rate = self.input.param("ops_rate", 10000)
            self.cursor_dropping_checkpoint = self.input.param(
                "cursor_dropping_checkpoint", None)
            self.index_timeout = self.input.param("index_timeout", 3600)
            self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                           True)
            self.bucket_width = 1
            self.bucket_weight = 30
            self.bucket_count = 0
            self.load_defn = list()
            self.defaultLoadDefn = {
                "valType": "Hotel",
                "scopes": 1,
                "collections": 2,
                "num_items": 500000,
                "start": 0,
                "end": 500000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 2,
                "ftsQPS": 2,
                "collections_defn": [
                    {
                        "valType": "Hotel",
                        "2i": [2, 2],
                        "FTS": [2, 2],
                        }
                    ]
                }
            self.maxLoadDefn = {
                "valType": "Hotel",
                "scopes": 1,
                "collections": 100,
                "num_items": 10000,
                "start": 0,
                "end": 10000,
                "ops": 2000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 20,
                "ftsQPS": 10,
                "collections_defn": [
                    {
                        "valType": "Hotel",
                        "2i": (1, 1),
                        "FTS": [1, 1],
                        }
                    ]
                }
            self.loadDefn1 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 10,
                "num_items": 500000,
                "start": 0,
                "end": 500000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 50,
                "ftsQPS": 5,
                "collections_defn": [
                    {
                        "valType": "SimpleValue",
                        "2i": (1, 1),
                        "FTS": [1, 1],
                        }
                    ]
                }
            self.load_defn.append(self.loadDefn1)
            self.loadDefn2 = {
                "valType": "Hotel",
                "scopes": 1,
                "collections": 10,
                "num_items": 200000,
                "start": 0,
                "end": 200000,
                "ops": 4000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 10,
                "ftsQPS": 10,
                "collections_defn": [
                    {
                        "valType": "Hotel",
                        "2i": (1, 1),
                        "FTS": [1, 1],
                        }
                    ]
                }
            self.load_defn.append(self.loadDefn2)
            self.loadDefn3 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 5,
                "num_items": 200000,
                "start": 0,
                "end": 200000,
                "ops": 2000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 20,
                "ftsQPS": 5,
                "collections_defn": [
                    {
                        "valType": "SimpleValue",
                        "2i": (2, 2),
                        "FTS": [2, 2],
                        }
                    ]
                }
            self.load_defn.append(self.loadDefn3)
            self.loadDefn4 = {
                "valType": "Hotel",
                "scopes": 1,
                "collections": 5,
                "num_items": 100000,
                "start": 0,
                "end": 100000,
                "ops": 2000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 2,
                "ftsQPS": 0,
                "collections_defn": [
                    {
                        "valType": "Hotel",
                        "2i": (0, 1),
                        "FTS": [0, 0],
                        }
                    ]
                }
            self.load_defn.append(self.loadDefn4)
            self.kv_memmgmt = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 1,
                "num_items": 200000000,
                "start": 0,
                "end": 200000000,
                "ops": 10000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2iQPS": 0,
                "ftsQPS": 0,
                "collections_defn": [
                    {
                        "valType": "SimpleValue",
                        "2i": (2, 2),
                        "FTS": [2, 2],
                        }
                    ]
                }
            self.workload = self.input.param("workload", self.defaultLoadDefn)

            self.drIndex = DoctorN1QL(self.cluster, self.bucket_util)
            self.drFTS = DoctorFTS(self.cluster, self.bucket_util)
            self.sdk_client_pool = self.bucket_util.initialize_java_sdk_client_pool()
            self.stop_run = False
            self.lock = Lock()
        except Exception as e:
            self.log.critical(e)
            self.tearDown()
            raise Exception("SetUp Failed - {}".format(e))

    def tearDown(self):
        self.check_dump_thread = False
        self.stop_crash = True
        for ql in self.ql:
            ql.stop_run = True
        for ql in self.ftsQL:
            ql.stop_run = True

        self.drFTS.stop_run = True
        self.drIndex.stop_run = True
        BaseTestCase.tearDown(self)

    def create_gsi_indexes(self, buckets):
        return self.drIndex.create_indexes(buckets)

    def build_gsi_index(self, buckets, load=True):
        self.drIndex.build_indexes(buckets, self.dataplane_objs, wait=True)
        if load:
            for bucket in buckets:
                if bucket.loadDefn.get("2iQPS") > 0:
                    ql = QueryLoad(bucket)
                    ql.start_query_load()
                    self.ql.append(ql)

    def check_gsi_scaling(self, dataplane, prev_gsi_nodes):
        tenants = list()
        count = 5
        while count > 0:
            for node in dataplane.index_nodes:
                rest = RestConnection(node)
                resp = rest.urllib_request(rest.indexUrl + "stats")
                content = json.loads(resp.content)
                tenants.append(int(content["num_tenants"]))
            gsi_scaling = True
            for tenant in tenants:
                if tenant < 20:
                    gsi_scaling = False
                    break
            if gsi_scaling:
                if self.check_jobs_entry("index", "scalingService"):
                    self.PrintStep("Step: Test GSI Auto-Scaling due to num of GSI tenants per sub-cluster")
                    self.check_cluster_scaling(service="gsi")
                    curr_gsi_nodes = self.get_num_nodes_in_cluster(
                                        dataplane.id, service="index")
                    self.log.info("GSI nodes - Actual: {}, Expected: {}".
                                  format(curr_gsi_nodes, prev_gsi_nodes+2))
                    self.assertTrue(curr_gsi_nodes == prev_gsi_nodes+2,
                                    "GSI nodes - Actual: {}, Expected: {}".
                                    format(curr_gsi_nodes, prev_gsi_nodes+2))
                    break
            count -= 1

    def check_index_auto_scaling_rebl(self):
        self.drIndex.gsi_cooling = False
        self.gsi_cooling_start = time.time()

        def check():
            while not self.stop_run:
                if self.drIndex.gsi_cooling and self.gsi_cooling_start + 900 > time.time():
                    self.log.info("GSI is in cooling period for 15 mins after auto-scaling: {} pending".
                                  format(self.gsi_cooling_start + 900 - time.time()))
                    self.sleep(60)
                    continue
                self.drIndex.gsi_cooling = False
                self.log.info("Index - Check for LWM/HWM scale/defrag operation.")
                if self.drIndex.scale_down or self.drIndex.scale_up:
                    self.log.info("Index - Scale operation should trigger in a while.")
                    _time = time.time() + 30*60
                    while _time > time.time():
                        if self.check_jobs_entry("index", "scalingService"):
                            self.check_cluster_scaling(service="GSI", state="scaling")
                            self.drIndex.gsi_cooling = True
                            self.gsi_cooling_start = time.time()
                            self.drIndex.scale_down, self.drIndex.scale_up = False, False
                            break
                        self.log.critical("Index scalingService not found in /jobs")
                        time.sleep(10)
                elif self.drIndex.gsi_auto_rebl:
                    self.log.info("Index - Rebalance operation should trigger in a while.")
                    _time = time.time() + 30*60
                    while _time > time.time():
                        if self.check_jobs_entry("index", "rebalancingService"):
                            self.check_cluster_scaling(service="GSI", state="rebalancing")
                            self.drIndex.gsi_cooling = True
                            self.gsi_cooling_start = time.time()
                            self.drIndex.gsi_auto_rebl = False
                            break
                        self.log.critical("Index rebalancingService not found in /jobs")
                        time.sleep(10)
                time.sleep(60)
        gsi_scaling_monitor = threading.Thread(target=check)
        gsi_scaling_monitor.start()

    def create_fts_indexes(self, buckets, wait=True, load=True):
        self.drFTS.create_fts_indexes(buckets)
        if wait:
            status = self.drFTS.wait_for_fts_index_online(buckets, self.index_timeout, overRideCount=0)
            self.assertTrue(status, "FTS index build failed.")
        if load:
            for bucket in buckets:
                if bucket.loadDefn.get("ftsQPS") > 0:
                    ftsQL = FTSQueryLoad(bucket)
                    ftsQL.start_query_load()
                    self.ftsQL.append(ftsQL)

    def check_jobs_entry(self, service, operation):
        jobs = self.serverless_util.get_dataplane_jobs(self.dataplane_id)
        for job in jobs["clusterJobs"]:
            if job.get("payload"):
                if job["payload"].get("tags"):
                    print job["payload"].get("tags"), job["status"]
                    for details in job["payload"].get("tags"):
                        if details["key"] == operation and\
                            details["value"] == service and\
                                job["status"] == "processing":
                            return True
        return False

    def check_fts_scaling(self):
        self.drFTS.fts_cooling = False
        self.fts_cooling_start = time.time()

        def check():
            while not self.stop_run:
                if self.drFTS.fts_cooling and self.fts_cooling_start + 900 > time.time():
                    self.log.info("FTS is in cooling period for 15 mins after auto-scaling: {} pending".
                                  format(self.fts_cooling_start + 900 - time.time()))
                    self.sleep(60)
                    continue
                self.drFTS.fts_cooling = False
                self.log.info("FTS - Check for scale operation.")
                if self.drFTS.scale_down or self.drFTS.scale_up or self.drFTS.fts_auto_rebl:
                    self.log.info("FTS - Scale operation should trigger in a while.")
                    if self.drFTS.fts_auto_rebl:
                        _time = time.time() + 30*60
                        while _time > time.time():
                            if self.check_jobs_entry("fts", "rebalancingService"):
                                self.check_cluster_scaling(service="FTS", state="rebalancing")
                                break
                            self.log.critical("FTS rebalancingService not found in /jobs")
                            time.sleep(10)
                        self.drFTS.fts_auto_rebl = False
                    else:
                        _time = time.time() + 30*60
                        while _time > time.time():
                            if self.check_jobs_entry("fts", "scalingService"):
                                self.check_cluster_scaling(service="FTS", state="scaling")
                                self.drFTS.fts_cooling = True
                                self.fts_cooling_start = time.time()
                                break
                            self.log.critical("FTS scalingService not found in /jobs")
                            time.sleep(10)
                        self.drFTS.scale_down, self.drFTS.scale_up = False, False
                self.sleep(60)
        fts_scaling_monitor = threading.Thread(target=check)
        fts_scaling_monitor.start()

    def check_n1ql_scaling(self):
        self.drIndex.n1ql_cooling = False
        self.n1ql_cooling_start = time.time()

        def check():
            while not self.stop_run:
                if self.drIndex.n1ql_cooling and self.n1ql_cooling_start + 900 > time.time():
                    self.log.info("N1QL is in cooling period for 15 mins after auto-scaling: {} pending".
                                  format(self.n1ql_cooling_start + 900 - time.time()))
                    self.sleep(60)
                    continue
                self.log.info("N1QL - Check for scale operation.")
                if self.drIndex.scale_up_n1ql or self.drIndex.scale_down_n1ql:
                    self.log.info("N1QL - Scale operation should trigger in a while.")
                    _time = time.time() + 30*60
                    while _time > time.time():
                        if self.check_jobs_entry("n1ql", "scalingService"):
                            self.check_cluster_scaling(service="N1QL", state="scaling")
                            self.drIndex.n1ql_cooling = True
                            self.n1ql_cooling_start = time.time()
                            break
                        self.log.critical("N1QL scalingService not found in /jobs")
                        time.sleep(10)
                    self.drIndex.scale_up_n1ql, self.drIndex.scale_down_n1ql = False, False
                    self.n1ql_nodes_below30 = 0
                    self.n1ql_nodes_above60 = 0
                self.sleep(60)
        n1ql_scaling_monitor = threading.Thread(target=check)
        n1ql_scaling_monitor.start()

    def check_kv_scaling(self):
        self.log.info("KV - Scale operation should trigger in a while.")
        _time = time.time() + 5*60*60
        while _time > time.time():
            if self.check_jobs_entry("kv", "scalingService"):
                self.check_cluster_scaling(service="kv", state="scaling")
                break
            time.sleep(10)

    def create_sdk_client_pool(self, buckets, req_clients_per_bucket):
        for bucket in buckets:
            nebula = bucket.serverless.nebula_endpoint
            self.log.info("Using Nebula endpoint %s" % nebula.srv)
            server = Server(nebula.srv, nebula.port,
                            nebula.rest_username,
                            nebula.rest_password,
                            str(nebula.memcached_port))
            self.sdk_client_pool.create_clients(
                bucket.name, server, req_clients_per_bucket)
            bucket.clients = self.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
        self.sleep(1, "Wait for SDK client pool to warmup")

    def create_databases(self, count=1, dataplane_ids=[], load_defn=None):
        dataplane_ids = dataplane_ids or self.dataplanes
        temp = list()
        i = 0
        while True:
            if i == count:
                break
            for tenant in self.tenants:
                if i == count:
                    break
                for dataplane_id in dataplane_ids:
                    if i == count:
                        break
                    self.database_name = "VolumeTestBucket-{}".format(str(self.bucket_count).zfill(2))
                    self.bucket_count += 1
                    bucket = Bucket(
                            {Bucket.name: self.database_name,
                             Bucket.bucketType: Bucket.Type.MEMBASE,
                             Bucket.replicaNumber: 2,
                             Bucket.storageBackend: Bucket.StorageBackend.magma,
                             Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
                             Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
                             Bucket.numVBuckets: 64,
                             Bucket.width: self.bucket_width,
                             Bucket.weight: self.bucket_weight
                             })
                    start = time.time()
                    state = self.get_cluster_balanced_state(self.dataplane_objs[dataplane_id])
                    while start + 3600 > time.time() and not state:
                        self.log.info("Balanced state of the cluster: {}"
                                      .format(state))
                        self.check_healthy_state(self.dataplane_id, timeout=7200)
                        state = self.get_cluster_balanced_state(self.dataplane_objs[dataplane_id])
                    self.lock.acquire()
                    task = self.bucket_util.async_create_database(self.cluster, bucket,
                                                                  tenant,
                                                                  dataplane_id)
                    temp.append((task, bucket))
                    self.lock.release()
                    if load_defn:
                        bucket.loadDefn = load_defn
                    else:
                        bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                    self.sleep(2)
                    i += 1
        for task, bucket in temp:
            self.task_manager.get_task_result(task)
            self.assertTrue(task.result, "Database deployment failed: {}".
                            format(bucket.name))

        num_clients = self.input.param("clients_per_db",
                                       min(1, bucket.loadDefn.get("collections")))
        for _, bucket in temp:
            self.create_sdk_client_pool([bucket],
                                        num_clients)

    def SmallScaleVolume(self):
        #######################################################################
        self.PrintStep("Step: Create Serverless Databases")

        self.drFTS.index_stats(self.dataplane_objs)
        self.drIndex.index_stats(self.dataplane_objs)
        self.drIndex.query_stats(self.dataplane_objs)
        self.check_ebs_scaling()
        self.check_memory_management()
        self.check_cluster_state()
        self.check_fts_scaling()
        self.check_n1ql_scaling()
        self.check_index_auto_scaling_rebl()
        self.monitor_query_status()

        for i in range(1, 6):
            self.create_databases(20, load_defn=self.workload)
            self.refresh_dp_obj(self.dataplane_id)
            buckets = self.cluster.buckets[(i-1)*20:(i)*20]
            kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
            if kv_nodes < min((i+1)*3, 11):
                self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                self.check_kv_scaling()
            elif kv_nodes == min((i+1)*3, 11):
                dataplane_state = self.serverless_util.get_dataplane_info(
                    self.dataplane_id)["couchbase"]["state"]
                if dataplane_state != "healthy":
                    self.check_kv_scaling()
                else:
                    self.log.info("KV already scaled up during databases creation")
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.assertTrue(int(kv_nodes) >= min((i+1)*3, 11),
                            "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            self.start_initial_load(buckets)
            for dataplane in self.dataplane_objs.values():
                prev_gsi_nodes = self.get_num_nodes_in_cluster(dataplane.id,
                                                               service="index")
                status = self.create_gsi_indexes(buckets)
                print "GSI Status: {}".format(status)
                self.assertTrue(status, "GSI index creation failed")
                if prev_gsi_nodes < 10:
                    self.check_gsi_scaling(dataplane, prev_gsi_nodes)
            self.build_gsi_index(buckets)
            self.create_fts_indexes(buckets, wait=True)
            self.sleep(30)

        for bucket in self.cluster.buckets:
            try:
                self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
            except TimeoutException as e:
                print e
            except:
                pass

            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_healthy_state(self.dataplane_id, timeout=7200)
                state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            self.log.info("Deleting bucket: {}".format(bucket.name))
            if self.ql:
                ql = [load for load in self.ql if load.bucket == bucket][0]
                ql.stop_query_load()
                self.ql.remove(ql)
            if self.ftsQL:
                ql = [load for load in self.ftsQL if load.bucket == bucket][0]
                ql.stop_query_load()
                self.ftsQL.remove(ql)
            self.sleep(2, "Wait for query load to stop: {}".format(bucket.name))
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)

    def ElixirVolume(self):
        #######################################################################
        self.PrintStep("Step: Create Serverless Databases")

        self.drFTS.index_stats(self.dataplane_objs)
        self.drIndex.index_stats(self.dataplane_objs)
        self.drIndex.query_stats(self.dataplane_objs)
        self.check_ebs_scaling()
        self.check_memory_management()
        self.check_cluster_state()
        self.check_fts_scaling()
        self.check_n1ql_scaling()
        self.check_index_auto_scaling_rebl()
        self.monitor_query_status()

        for i in range(1, 6):
            self.create_databases(20, load_defn=self.workload)
            self.refresh_dp_obj(self.dataplane_id)
            buckets = self.cluster.buckets[(i-1)*20:(i)*20]
            kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
            if kv_nodes < min((i+1)*3, 11):
                self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                self.check_kv_scaling()
            elif kv_nodes == min((i+1)*3, 11):
                dataplane_state = self.serverless_util.get_dataplane_info(
                    self.dataplane_id)["couchbase"]["state"]
                if dataplane_state != "healthy":
                    self.check_kv_scaling()
                else:
                    self.log.info("KV already scaled up during databases creation")
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.assertTrue(int(kv_nodes) >= min((i+1)*3, 11),
                            "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            for dataplane in self.dataplane_objs.values():
                prev_gsi_nodes = self.get_num_nodes_in_cluster(dataplane.id,
                                                               service="index")
                status = self.create_gsi_indexes(buckets)
                print "GSI Status: {}".format(status)
                self.assertTrue(status, "GSI index creation failed")
                if prev_gsi_nodes < 10:
                    self.check_gsi_scaling(dataplane, prev_gsi_nodes)
            self.build_gsi_index(buckets)
            self.start_initial_load(buckets)
            self.create_fts_indexes(buckets, wait=True)
            self.sleep(30)

        self.sleep(1*60*60, "Let the workload run for 1 hour!!!")

        count = 0
        self.PrintStep("Step: Test KV Auto-Rebalance/Defragmentation")
        buckets = self.cluster.buckets
        buckets = sorted(buckets, key=lambda bucket: bucket.name)
        for bucket in buckets:
            if self.ql:
                ql = [load for load in self.ql if load.bucket == bucket][0]
                ql.stop_query_load()
                self.ql.remove(ql)
            if self.ftsQL:
                ql = [load for load in self.ftsQL if load.bucket == bucket][0]
                ql.stop_query_load()
                self.ftsQL.remove(ql)
            self.sleep(2, "Wait for query load to stop: {}".format(bucket.name))
            try:
                self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
            except TimeoutException as e:
                print e
            except:
                pass

            self.log.info("Acquire lock to check if the cluster is not scaling/rebalancing.")
            self.lock.acquire()
            self.log.info("Releasing lock as the cluster is not scaling/rebalancing.")
            self.lock.release()
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_healthy_state(self.dataplane_id, timeout=14400)
                state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            self.update_bucket_nebula_and_kv_nodes(self.cluster, bucket)
            self.assertEqual(len(self.cluster.bucketDNNodes[bucket]),
                             bucket.serverless.width*3,
                             "Bucket width and number of nodes mismatch")
            self.log.info("Deleting bucket: {}".format(bucket.name))
            self.cluster.buckets.remove(bucket)
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)
            count += 1
            if count % 25 == 0:
                self.check_healthy_state(self.dataplane_id, timeout=14400)

        self.cluster.buckets = list()
        self.ql = []
        self.ftsQL = []
        self.sleep(60*60, "Wait for cluster to scale down!")
        # Reset cluster specs to default
        # self.log.info("Reset cluster specs to default to proceed further")
        # self.generate_dataplane_config()
        # config = self.dataplane_config["overRide"]["couchbase"]["specs"]
        # self.log.info("Changing cluster specs to default.")
        # self.log.info(config)
        # self.serverless_util.change_dataplane_cluster_specs(self.dataplane_id, config)
        # self.check_cluster_scaling()

        self.create_databases(19, load_defn=self.maxLoadDefn)
        self.create_required_collections(self.cluster,
                                         self.cluster.buckets[0:20])
        self.create_gsi_indexes(self.cluster.buckets[0:20])
        self.build_gsi_index(self.cluster.buckets[0:20])
        self.start_initial_load(self.cluster.buckets[0:20])
        self.create_fts_indexes(self.cluster.buckets, wait=True)
        for bucket in self.cluster.buckets:
            self.generate_docs(bucket=bucket)
        load_tasks = self.perform_load(validate_data=True,
                                       buckets=self.cluster.buckets,
                                       wait_for_load=False)
        self.PrintStep("Step 3: Test Bucket-Rebalancing(change width)")
        self.log.info("Acquire lock to check if the cluster is not scaling/rebalancing.")
        self.lock.acquire()
        self.log.info("Releasing lock as the cluster is not scaling/rebalancing.")
        self.lock.release()
        # for width in [2, 3, 4]:
        #     self.PrintStep("Test change bucket width to {}".format(width))
        #     target_kv_nodes = 3 * width
        #     actual_kv_nodes = self.get_num_nodes_in_cluster()
        #     state = "rebalancing"
        #     if target_kv_nodes > actual_kv_nodes:
        #         state = "scaling"
        #     for bucket_obj in self.cluster.buckets[::2]:
        #         # Update the width of the buckets multiple times
        #         override = {"width": width, "weight": 60}
        #         bucket_obj.serverless.width = width
        #         bucket_obj.serverless.weight = 60
        #         self.serverless_util.update_database(bucket_obj.name, override)
        #         self.log.info("Updated width for bucket {} to {}".format(bucket_obj.name, width))
        #     if target_kv_nodes > actual_kv_nodes:
        #         self.check_cluster_scaling(state=state)
        #         self.log.info("Cluster is scaled up due the change in bucket width and ready to rebalance buckets")
        #     # start = time.time()
        #     # state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
        #     # while start + 3600 > time.time() and state:
        #     #     self.log.info("Balanced state of the cluster: {}"
        #     #                   .format(state))
        #     #     state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
        #     # self.log.info("Balanced state of the cluster: {}".format(state))
        #     # self.assertFalse(state, "Balanced state of the cluster: {}"
        #     #                  .format(state))
        #     # state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
        #     # self.assertTrue(state, "Balanced state of the cluster: {}".format(state))
        #     self.log.info("Buckets are rebalanced after change in their width")
        #
        #     for bucket in self.cluster.buckets:
        #         self.update_bucket_nebula_and_kv_nodes(self.cluster, bucket)
        #         self.log.info("DN nodes for {}: {}".format(bucket.name, self.cluster.bucketDNNodes[bucket]))
        #         while len(self.cluster.bucketDNNodes[bucket]) < bucket.serverless.width*3:
        #             self.check_cluster_scaling(state="rebalancing")
        #             self.update_bucket_nebula_and_kv_nodes(self.cluster, bucket)
        #         self.assertEqual(len(self.cluster.bucketDNNodes[bucket]),
        #                          bucket.serverless.width*3,
        #                          "Bucket width and number of nodes mismatch")

        self.doc_loading_tm.abortAllTasks()
        for task in load_tasks:
            try:
                task.sdk.disconnectCluster()
            except:
                pass
        self.PrintStep("Step: Test KV Auto-Rebalance/Defragmentation")
        for bucket in self.cluster.buckets:
            self.log.info("Deleting bucket: {}".format(bucket.name))
            if self.ql:
                ql = [load for load in self.ql if load.bucket == bucket][0]
                ql.stop_run = True
            if self.ftsQL:
                ql = [load for load in self.ftsQL if load.bucket == bucket][0]
                ql.stop_run = True
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and not state:
                self.check_healthy_state(self.dataplane_id)
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)
            self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
        self.cluster.buckets = list()
        self.ql = []
        self.ftsQL = []
        tasks = list()
        self.create_databases(18)
        self.create_databases(2, load_defn=self.kv_memmgmt)
        buckets = self.cluster.buckets[0:20]
        self.create_required_collections(self.cluster, buckets)
        self.create_fts_indexes(buckets, wait=False)
        self.create_gsi_indexes(buckets)
        self.check_cluster_scaling(service="GSI")
        self.build_gsi_index(buckets)
        self.start_initial_load(buckets)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.create_databases(18)
        self.create_databases(2, load_defn=self.kv_memmgmt)
        buckets = self.cluster.buckets[20:40]
        self.create_required_collections(self.cluster, buckets)
        self.create_fts_indexes(buckets, wait=False)
        self.start_initial_load(buckets)
        self.create_gsi_indexes(buckets)
        self.check_cluster_scaling(service="GSI")
        self.build_gsi_index(buckets)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.create_databases(18)
        self.create_databases(2, load_defn=self.kv_memmgmt)
        buckets = self.cluster.buckets[40:60]
        self.create_required_collections(self.cluster, buckets)
        self.create_gsi_indexes(buckets)
        self.check_cluster_scaling(service="GSI")
        self.build_gsi_index(buckets)
        self.start_initial_load(buckets)
        self.create_fts_indexes(buckets, wait=False)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.create_databases(18)
        self.create_databases(2, load_defn=self.kv_memmgmt)
        buckets = self.cluster.buckets[60:80]
        self.create_required_collections(self.cluster, buckets)
        self.create_gsi_indexes(buckets)
        self.check_cluster_scaling(service="GSI")
        self.build_gsi_index(buckets)
        self.create_fts_indexes(buckets, wait=False)
        self.start_initial_load(buckets)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.create_databases(18)
        self.create_databases(2, load_defn=self.kv_memmgmt)
        buckets = self.cluster.buckets[80:100]
        self.create_required_collections(self.cluster, buckets)
        self.start_initial_load(buckets)
        self.create_gsi_indexes(buckets)
        self.build_gsi_index(buckets)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.drIndex.stop_run = True
        self.drFTS.stop_run = True

    def PrivatePreview(self):
        #######################################################################
        self.track_failures = False
        self.PrintStep("Step: Create Serverless Databases")

        self.drFTS.index_stats(self.dataplane_objs)
        self.drIndex.index_stats(self.dataplane_objs)
        self.drIndex.query_stats(self.dataplane_objs)
        self.check_ebs_scaling()
        self.check_memory_management()
        self.check_cluster_state()
        self.check_fts_scaling()
        self.check_n1ql_scaling()
        self.check_index_auto_scaling_rebl()
        self.monitor_query_status()

        self.load_defn = []
        self.loadDefn1 = {
            "valType": "SimpleValue",
            "scopes": 1,
            "collections": 2,
            "num_items": 100000,
            "start": 0,
            "end": 100000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [0, 80, 20, 0, 0], # CRUDE
            "load_type": ["read", "upsert"],
            "2iQPS": 20,
            "ftsQPS": 10,
            "collections_defn": [
                    {
                        "valType": "SimpleValue",
                        "2i": (5, 5),
                        "FTS": [5, 5],
                        }
                    ]
            }
        self.load_defn.append(self.loadDefn1)
        self.loadDefn2 = {
            "valType": "SimpleValue",
            "scopes": 1,
            "collections": 2,
            "num_items": 500000,
            "start": 0,
            "end": 500000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [10, 80, 0, 10, 0], # CRUDE
            "load_type": ["create", "read", "delete"],
            "2iQPS": 20,
            "ftsQPS": 10,
            "collections_defn": [
                    {
                        "valType": "SimpleValue",
                        "2i": (5, 5),
                        "FTS": [5, 5],
                        }
                    ]
            }
        self.load_defn.append(self.loadDefn2)
        self.loadDefn3 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 10,
            "num_items": 5000000,
            "start": 0,
            "end": 5000000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "2iQPS": 20,
            "ftsQPS": 10,
            "collections_defn": [
                    {
                        "valType": "Hotel",
                        "2i": (1, 1),
                        "FTS": [1, 1],
                        }
                    ]
            }

        self.input.test_params.update({"clients_per_db": 1})
        for i in range(1, 10):
            self.create_databases(5)
            self.refresh_dp_obj(self.dataplane_id)
            buckets = self.cluster.buckets[(i-1)*5:(i)*5]
            if i % 4 == 0:
                kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
                if kv_nodes < min((i%4+1)*3, 11):
                    self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                    self.check_kv_scaling()
                elif kv_nodes == min((i%4+1)*3, 11):
                    dataplane_state = self.serverless_util.get_dataplane_info(
                        self.dataplane_id)["couchbase"]["state"]
                    if dataplane_state != "healthy":
                        self.check_kv_scaling()
                    else:
                        self.log.info("KV already scaled up during databases creation")
                kv_nodes = self.get_num_nodes_in_cluster(service="kv")
                self.assertTrue(int(kv_nodes) >= min((i%4+1)*3, 11),
                                "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            status = self.create_gsi_indexes(buckets)
            print "GSI Status: {}".format(status)
            self.build_gsi_index(buckets)
            self.create_fts_indexes(buckets, wait=True)
            self.start_initial_load(buckets)
            # try:
            #     for bucket in buckets:
            #         self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
            #         self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
            # except TimeoutException as e:
            #     print e
            # except:
            #     pass
            # self.sleep(30)

        self.input.test_params.update({"clients_per_db": 5})
        self.create_databases(5, load_defn=self.loadDefn3)
        buckets = self.cluster.buckets[-1:-6:-1]
        self.create_required_collections(self.cluster, buckets)
        status = self.create_gsi_indexes(buckets)
        print "GSI Status: {}".format(status)
        self.build_gsi_index(buckets)
        self.create_fts_indexes(buckets, wait=True)
        self.start_initial_load(buckets)
        for i in range(10):
            for bucket in buckets:
                self.generate_docs(bucket=bucket)
            self.log.info("Starting incremental load: %s" % i)
            self.perform_load(validate_data=False, buckets=buckets,
                              wait_for_load=True)

        for bucket in self.cluster.buckets:
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_healthy_state(self.dataplane_id, timeout=7200)
                state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            self.log.info("Deleting bucket: {}".format(bucket.name))
            if self.ql:
                ql = [load for load in self.ql if load.bucket == bucket][0]
                ql.stop_query_load()
                self.ql.remove(ql)
            if self.ftsQL:
                ql = [load for load in self.ftsQL if load.bucket == bucket][0]
                ql.stop_query_load()
                self.ftsQL.remove(ql)
            self.sleep(2, "Wait for query load to stop: {}".format(bucket.name))
            try:
                self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
            except TimeoutException as e:
                print e
            except:
                pass
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)

    def ElixirVolumeCOGS(self):
        #######################################################################
        self.PrintStep("Step: Create Serverless Databases")

        self.drFTS.index_stats(self.dataplane_objs)
        self.drIndex.index_stats(self.dataplane_objs)
        self.drIndex.query_stats(self.dataplane_objs)
        self.check_ebs_scaling()
        self.check_memory_management()
        self.check_cluster_state()
        self.check_fts_scaling()
        self.check_n1ql_scaling()
        self.check_index_auto_scaling_rebl()
        self.monitor_query_status()

        self.defaultLoadDefn = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 2,
            "num_items": 500000,
            "start": 0,
            "end": 500000,
            "ops": 5000,
            "doc_size": 1024,
            "pattern": [10, 80, 0, 10, 0], # CRUDE
            "load_type": ["create", "read", "delete"],
            "2iQPS": 0,
            "ftsQPS": 0,
            "collections_defn": [
                {
                    "valType": "Hotel",
                    "2i": [0, 0],
                    "FTS": [0, 0],
                    }
                ]
            }

        self.real_users = {
            "valType": "SimpleValue",
            "scopes": 1,
            "collections": 4,
            "num_items": 25000000,
            "start": 0,
            "end": 25000000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [5, 90, 0, 5, 0], # CRUDE
            "load_type": ["create", "read", "delete"],
            "2iQPS": 2,
            "ftsQPS": 2,
            "collections_defn": [
                {
                    "valType": "SimpleValue",
                    "2i": (1, 1),
                    "FTS": [1, 1],
                    },
                {
                    "valType": "SimpleValue",
                    "2i": (1, 1),
                    "FTS": [0, 0],
                    }
                ]
            }

        for i in range(1, 6):
            load = False
            if i == 5:
                load = True
                self.create_databases(20, load_defn=self.real_users)
            else:
                self.create_databases(20, load_defn=self.defaultLoadDefn)
            self.refresh_dp_obj(self.dataplane_id)
            buckets = self.cluster.buckets[-20:]
            kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
            if kv_nodes < min((i+1)*3, 11):
                self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                self.check_kv_scaling()
            elif kv_nodes == min((i+1)*3, 11):
                dataplane_state = self.serverless_util.get_dataplane_info(
                    self.dataplane_id)["couchbase"]["state"]
                if dataplane_state != "healthy":
                    self.check_kv_scaling()
                else:
                    self.log.info("KV already scaled up during databases creation")
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.assertTrue(int(kv_nodes) >= min((i+1)*3, 11),
                            "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            for dataplane in self.dataplane_objs.values():
                prev_gsi_nodes = self.get_num_nodes_in_cluster(dataplane.id,
                                                               service="index")
                status = self.create_gsi_indexes(buckets)
                print "GSI Status: {}".format(status)
                self.assertTrue(status, "GSI index creation failed")
                if prev_gsi_nodes < 10:
                    self.check_gsi_scaling(dataplane, prev_gsi_nodes)
            self.start_initial_load(buckets)
            self.build_gsi_index(buckets, load=load)
            self.create_fts_indexes(buckets, wait=True, load=load)
            if i != 5:
                try:
                    for bucket in buckets:
                        self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                        self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
                except TimeoutException as e:
                    print e
                except:
                    pass
            self.sleep(30)

        buckets = self.cluster.buckets[-20:]
        for bucket in self.cluster.buckets[-20:]:
            bucket.loadDefn["ops"] = 5000

        for i in range(10):
            for bucket in buckets:
                self.generate_docs(bucket=bucket)
            self.log.info("Starting incremental load: %s" % i)
            self.perform_load(validate_data=False, buckets=buckets,
                              wait_for_load=True)

        self.drIndex.stop_run = True
        self.drFTS.stop_run = True
