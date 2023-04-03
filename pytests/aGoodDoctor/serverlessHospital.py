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
                "collections": 1,
                "num_items": 2000000,
                "start": 0,
                "end": 2000000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (2, 2),
                "FTS": (2, 2)
                }
            self.gsiAutoScaleLoadDefn = {
                "valType": "Hotel",
                "scopes": 1,
                "collections": 2,
                "num_items": 1000000,
                "start": 0,
                "end": 1000000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (20, 50),
                "FTS": (0, 0)
                }
            self.loadDefn1 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 20,
                "num_items": 5000000,
                "start": 0,
                "end": 5000000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (20, 20),
                "FTS": (5, 5)
                }
            self.load_defn.append(self.loadDefn1)
            self.loadDefn2 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 10,
                "num_items": 2000000,
                "start": 0,
                "end": 2000000,
                "ops": 4000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (10, 10),
                "FTS": (10, 10)
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
                "2i": (10, 20),
                "FTS": (10, 5)
                }
            self.load_defn.append(self.loadDefn3)
            self.loadDefn4 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 5,
                "num_items": 1000000,
                "start": 0,
                "end": 200000,
                "ops": 2000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (0, 2),
                "FTS": (0, 0)
                }
            self.load_defn.append(self.loadDefn4)
            self.kv_memmgmt = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 1,
                "num_items": 250000000,
                "start": 0,
                "end": 250000000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (0, 0),
                "FTS": (0, 0)
                }
            self.load_defn.append(self.kv_memmgmt)
            self.workload = self.input.param("workload", self.defaultLoadDefn)

            if self.workload == "kv_memmgmt":
                self.workload = self.kv_memmgmt
            elif self.workload == "gsi_auto_scaling":
                self.workload = self.gsiAutoScaleLoadDefn

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

    def build_gsi_index(self, buckets):
        self.drIndex.build_indexes(buckets, self.dataplane_objs, wait=True)
        for bucket in buckets:
            if bucket.loadDefn.get("2i")[1] > 0:
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

    def create_fts_indexes(self, buckets, wait=True):
        self.drFTS.create_fts_indexes(buckets)
        if wait:
            status = self.drFTS.wait_for_fts_index_online(buckets, self.index_timeout, overRideCount=0)
            self.assertTrue(status, "FTS index build failed.")
        for bucket in buckets:
            if bucket.loadDefn.get("FTS")[1] > 0:
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
        self.sleep(2, "Wait for SDK client pool to warmup")

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
                        self.check_cluster_scaling(state="rebalancing")
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
                                       min(2, bucket.loadDefn.get("collections")))
        for _, bucket in temp:
            self.create_sdk_client_pool([bucket],
                                        num_clients)

    def check_cluster_scaling(self, dataplane_id=None, service="kv", state="scaling"):
        self.lock.acquire()
        dataplane_id = dataplane_id or self.dataplane_id
        try:
            self.log.info("Dataplane Jobs:")
            pprint.pprint(self.serverless_util.get_dataplane_jobs(dataplane_id))
            self.log.info("Scaling Records:")
            pprint.pprint(self.serverless_util.get_scaling_records(dataplane_id))
        except:
            self.log.info("Scaling records are empty")
        dataplane_state = "healthy"
        try:
            dataplane_state = self.serverless_util.get_dataplane_info(
                dataplane_id)["couchbase"]["state"]
        except:
            pass
        scaling_timeout = 5*60*60
        while dataplane_state == "healthy" and scaling_timeout >= 0:
            dataplane_state = "healthy"
            try:
                dataplane_state = self.serverless_util.get_dataplane_info(
                    dataplane_id)["couchbase"]["state"]
            except:
                pass
            self.log.info("Cluster state is: {}. Target: {} for {}"
                          .format(dataplane_state, state, service))
            time.sleep(2)
            scaling_timeout -= 2
        if not service.lower() in ["gsi", "fts"]:
            self.assertEqual(dataplane_state, state,
                             "Cluster scaling did not trigger in {} seconds.\
                             Actual: {} Expected: {}".format(
                                 scaling_timeout, dataplane_state, state))

        scaling_timeout = 10*60*60
        while dataplane_state != "healthy" and scaling_timeout >= 0:
            self.refresh_dp_obj(dataplane_id)
            dataplane_state = state
            try:
                dataplane_state = self.serverless_util.get_dataplane_info(
                    dataplane_id)["couchbase"]["state"]
            except:
                pass
            self.log.info("Cluster state is: {}. Target: {} for {}"
                          .format(dataplane_state, state, service))
            time.sleep(2)
            scaling_timeout -= 2
        self.log.info("Dataplane Jobs:")
        pprint.pprint(self.serverless_util.get_dataplane_jobs(dataplane_id))
        self.log.info("Scaling Records:")
        pprint.pprint(self.serverless_util.get_scaling_records(dataplane_id))
        if not service.lower() in ["gsi", "fts"]:
            self.assertEqual(dataplane_state, "healthy",
                             "Cluster scaling started but did not completed in {} seconds.\
                             Actual: {} Expected: {}".format(
                                 scaling_timeout, dataplane_state, "healthy"))
        self.sleep(10, "Wait before dataplane cluster nodes refresh")
        self.refresh_dp_obj(dataplane_id)
        self.lock.release()

    def refresh_dp_obj(self, dataplane_id):
        try:
            dp = self.dataplane_objs[dataplane_id]
            cmd = "dig @8.8.8.8  _couchbases._tcp.{} srv".format(dp.srv)
            proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
            out, _ = proc.communicate()
            records = list()
            for line in out.split("\n"):
                if "11207" in line:
                    records.append(line.split("11207")[-1].rstrip(".").lstrip(" "))
            ip = str(records[0])
            servers = RestConnection({"ip": ip,
                                      "username": dp.user,
                                      "password": dp.pwd,
                                      "port": 18091}).get_nodes()
            dp.refresh_object(servers)
        except:
            pass

    def get_num_nodes_in_cluster(self, dataplane_id=None, service="kv"):
        dataplane_id = dataplane_id or self.dataplane_id
        info = self.serverless_util.get_dataplane_info(dataplane_id)
        nodes = [spec["count"] for spec in info["couchbase"]["specs"] if spec["services"][0]["type"] == service][0]
        return nodes

    def update_bucket_nebula_and_kv_nodes(self, cluster, bucket):
        self.log.debug("Fetching SRV records for %s" % bucket.name)
        srv = self.serverless_util.get_database_nebula_endpoint(
            cluster.pod, cluster.tenant, bucket.name)
        self.assertEqual(bucket.serverless.nebula_endpoint.srv, srv, "SRV changed")
        bucket.serverless.nebula_obj.update_server_list()
        self.log.debug("Updating nebula servers for %s" % bucket.name)
        self.bucket_util.update_bucket_nebula_servers(
            cluster, bucket)

    def start_initial_load(self, buckets):
        self.PrintStep("Step 2: Create %s items: %s" % (self.num_items, self.key_type))
        for bucket in buckets:
            self.generate_docs(doc_ops=["create"],
                               create_start=0,
                               create_end=bucket.loadDefn.get("num_items")/2,
                               bucket=bucket)
        self.perform_load(validate_data=False, buckets=buckets, overRidePattern=[100,0,0,0,0])

        self.PrintStep("Step 3: Create %s items: %s" % (self.num_items, self.key_type))
        for bucket in self.cluster.buckets:
            self.generate_docs(doc_ops=["create"],
                               create_start=bucket.loadDefn.get("num_items")/2,
                               create_end=bucket.loadDefn.get("num_items"),
                               bucket=bucket)
        self.perform_load(validate_data=False, buckets=buckets, overRidePattern=[100,0,0,0,0])

    def get_cluster_balanced_state(self, dataplane):
        rest = RestConnection(dataplane.master)
        try:
            content = rest.get_pools_default()
            return content["balanced"]
        except:
            self.log.critical("{} /pools/default has failed!!".format(dataplane.master))
            pass

    def check_cluster_state(self):
        def start_check():
            for dataplane in self.dataplane_objs.values():
                state = self.get_cluster_balanced_state(dataplane)
                if not state:
                    self.log.critical("Dataplane State {}: {}".format(
                        dataplane.id, state))
                time.sleep(5)

        monitor_state = threading.Thread(target=start_check)
        monitor_state.start()

    def check_ebs_scaling(self):
        '''
        1. check current disk used
        2. If disk used > 50% check for EBS scale on all nodes for that service
        '''

        def check_disk():
            while not self.stop_run:
                for dataplane in self.dataplane_objs.values():
                    self.refresh_dp_obj(dataplane.id)
                    table = TableView(self.log.info)
                    table.set_headers(["Node",
                                       "Path",
                                       "TotalDisk",
                                       "UsedDisk",
                                       "% Disk Used"])
                    for node in dataplane.kv_nodes:
                        data = RestConnection(node).get_nodes_self()
                        for storage in data.availableStorage:
                            if "cb" in storage.path:
                                table.add_row([
                                    node.ip,
                                    storage.path,
                                    data.storageTotalDisk,
                                    data.storageUsedDisk,
                                    storage.usagePercent])
                                if storage.usagePercent > 90:
                                    self.log.critical("Disk did not scale while\
                                     it is approaching full!!!")
                                    self.doc_loading_tm.abortAllTasks()
                    table.display("EBS Statistics")
                time.sleep(120)
        disk_monitor = threading.Thread(target=check_disk)
        disk_monitor.start()

    def check_memory_management(self):
        '''
        1. Check the database disk used
        2. Cal DGM based on ram/disk used and if it is < 1% wait for tunable
        '''
        self.disk = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450]
        self.memory = [256, 256, 384, 512, 640, 768, 896, 1024, 1152, 1280]

        def check_ram():
            while not self.stop_run:
                for dataplane in self.dataplane_objs.values():
                    self.rest = BucketHelper(dataplane.master)
                    table = TableView(self.log.info)
                    table.set_headers(["Bucket",
                                       "Total Ram(MB)",
                                       "Total Data(GB)",
                                       "Logical Data",
                                       "Items"])
                    logical_data = defaultdict(int)
                    for node in dataplane.kv_nodes:
                        _, stats = RestConnection(node).query_prometheus("kv_logical_data_size_bytes")
                        if stats["status"] == "success":
                            stats = [stat for stat in stats["data"]["result"] if stat["metric"]["state"] == "active"]
                            for stat in stats:
                                logical_data[stat["metric"]["bucket"]] += int(stat["value"][1])
                    for bucket in self.cluster.buckets:
                        data = self.rest.get_bucket_json(bucket.name)
                        ramMB = data["quota"]["rawRAM"] / (1024 * 1024)
                        dataGB = data["basicStats"]["diskUsed"] / (1024 * 1024 * 1024)
                        items = data["basicStats"]["itemCount"]
                        logicalDdataGB = logical_data[bucket.name] / (1024 * 1024 * 1024)
                        table.add_row([bucket.name, ramMB, dataGB, logicalDdataGB, items])
                        for i, disk in enumerate(self.disk):
                            if disk > logicalDdataGB:
                                start = time.time()
                                while time.time() < start + 1200 and ramMB != self.memory[i-1]:
                                    self.log.info("Wait for bucket: {}, Expected: {}, Actual: {}".
                                                  format(bucket.name,
                                                         self.memory[i-1],
                                                         ramMB))
                                    time.sleep(5)
                                    data = self.rest.get_bucket_json(bucket.name)
                                    ramMB = data["quota"]["rawRAM"] / (1024 * 1024)
                                    continue
                                if ramMB != self.memory[i-1]:
                                    raise Exception("bucket: {}, Expected: {}, Actual: {}".
                                                    format(bucket.name,
                                                           self.memory[i-1],
                                                           ramMB))
                                break
                    table.display("Bucket Memory Statistics")
                time.sleep(120)
        mem_monitor = threading.Thread(target=check_ram)
        mem_monitor.start()

    def monitor_query_status(self, print_duration=600):

        def check_query_stats():
            st_time = time.time()
            while not self.stop_run:
                if st_time + print_duration < time.time():
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
                    self.table.display("N1QL Query Statistics")

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
                    self.FTStable.display("FTS Query Statistics")
                    st_time = time.time()
                    time.sleep(10)

        query_monitor = threading.Thread(target=check_query_stats)
        query_monitor.start()

    def SteadyStateVolume(self):
        #######################################################################
        self.PrintStep("Step 1: Create required buckets and collections.")
        self.log.info("Create CB buckets")
        # Create Buckets
        self.workload = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 1,
                "num_items": 2000000,
                "start": 0,
                "end": 2000000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (2, 2),
                "FTS": (2, 2)
                }
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

        self.create_databases(2, load_defn=self.workload)
        buckets = self.cluster.buckets
        self.refresh_dp_obj(self.dataplane_id)
        self.create_required_collections(self.cluster, self.cluster.buckets)
        self.start_initial_load(self.cluster.buckets)

        status = self.create_gsi_indexes(buckets)
        print "GSI Status: {}".format(status)
        self.assertTrue(status, "GSI index creation failed")
        self.build_gsi_index(buckets)
        self.create_fts_indexes(buckets, wait=True)
        self.sleep(30)

        self.loop = 1
        self.create_perc = 100

        while self.loop <= self.iterations:
            #######################################################################
            '''
            creates: 0 - 10M
            deletes: 0 - 10M
            Final Docs = 0
            '''
            for bucket in self.cluster.buckets:
                self.generate_docs(bucket=bucket)
            self.perform_load(validate_data=True, buckets=self.cluster.buckets)
            self.loop += 1

        for bucket in self.cluster.buckets:
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
                self.check_cluster_scaling()
            elif kv_nodes == min((i+1)*3, 11):
                dataplane_state = self.serverless_util.get_dataplane_info(
                    self.dataplane_id)["couchbase"]["state"]
                if dataplane_state != "healthy":
                    self.check_cluster_scaling()
                else:
                    self.log.info("KV already scaled up during databases creation")
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.assertTrue(int(kv_nodes) >= min((i+1)*3, 11),
                            "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            self.start_initial_load(buckets)
            for bucket in buckets:
                try:
                    self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                    self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
                except TimeoutException as e:
                    print e
                except:
                    pass
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
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_cluster_scaling(state="rebalancing")
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

        if self.workload != self.gsiAutoScaleLoadDefn:
            for i in range(1, 6):
                self.create_databases(20, load_defn=self.workload)
                self.refresh_dp_obj(self.dataplane_id)
                buckets = self.cluster.buckets[(i-1)*20:(i)*20]
                kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
                if kv_nodes < min((i+1)*3, 11):
                    self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                    self.check_cluster_scaling()
                elif kv_nodes == min((i+1)*3, 11):
                    dataplane_state = self.serverless_util.get_dataplane_info(
                        self.dataplane_id)["couchbase"]["state"]
                    if dataplane_state != "healthy":
                        self.check_cluster_scaling()
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
                for bucket in buckets:
                    try:
                        self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                        self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
                    except TimeoutException as e:
                        print e
                    except:
                        pass
                self.create_fts_indexes(buckets, wait=True)
                self.sleep(30)
            count = 0
            self.PrintStep("Step: Test KV Auto-Rebalance/Defragmentation")
            buckets = self.cluster.buckets
            buckets = sorted(buckets, key=lambda bucket: bucket.name)
            for bucket in buckets:
                self.log.info("Acquire lock to check if the cluster is not scaling/rebalancing.")
                self.lock.acquire()
                self.log.info("Releasing lock as the cluster is not scaling/rebalancing.")
                self.lock.release()
                start = time.time()
                state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
                while start + 3600 > time.time() and state is False:
                    self.log.info("Balanced state of the cluster: {}"
                                  .format(state))
                    self.check_cluster_scaling(state="rebalancing")
                    state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
                self.update_bucket_nebula_and_kv_nodes(self.cluster, bucket)
                self.assertEqual(len(self.cluster.bucketDNNodes[bucket]),
                                 bucket.serverless.width*3,
                                 "Bucket width and number of nodes mismatch")
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
                self.cluster.buckets.remove(bucket)
                self.serverless_util.delete_database(self.pod, self.tenant,
                                                     bucket.name)
                count += 1
                if count == 50:
                    self.check_cluster_scaling(state="rebalancing")
            self.cluster.buckets = list()
            self.ql = []
            self.ftsQL = []
            # Reset cluster specs to default
            self.log.info("Reset cluster specs to default to proceed further")
            self.generate_dataplane_config()
            config = self.dataplane_config["overRide"]["couchbase"]["specs"]
            self.log.info("Changing cluster specs to default.")
            self.log.info(config)
            self.serverless_util.change_dataplane_cluster_specs(self.dataplane_id, config)
            self.check_cluster_scaling()

        self.create_databases(19, load_defn=self.gsiAutoScaleLoadDefn)
        # self.create_databases(19)
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
                self.check_cluster_scaling(state="rebalancing")
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
        self.create_databases(20)
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

        self.create_databases(20)
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

        self.create_databases(20)
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

        self.create_databases(20)
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

        self.create_databases(20)
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
            "2i": (5, 5),
            "FTS": (5, 5)
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
            "2i": (5, 5),
            "FTS": (5, 5)
            }
        self.load_defn.append(self.loadDefn2)
        self.loadDefn3 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 10,
            "num_items": 10000000,
            "start": 0,
            "end": 10000000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "2i": (5, 20),
            "FTS": (5, 10)
            }

        self.input.test_params.update({"clients_per_db": 2})
        ten_together = self.input.param("ten_together", False)
        if ten_together:
            self.load_defn = []
            loadDefn1 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 2,
                "num_items": 10000000,
                "start": 0,
                "end": 10000000,
                "ops": 10000,
                "doc_size": 1024,
                "pattern": [0, 80, 20, 0, 0], # CRUDE
                "load_type": ["read", "upsert"],
                "2i": (5, 5),
                "FTS": (5, 5)
                }
            self.load_defn.append(loadDefn1)
            loadDefn2 = {
                "valType": "SimpleValue",
                "scopes": 1,
                "collections": 2,
                "num_items": 4000000,
                "start": 0,
                "end": 4000000,
                "ops": 10000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (5, 5),
                "FTS": (5, 5)
                }
            self.load_defn.append(loadDefn2)
            loadDefn3 = {
                "valType": "Hotel",
                "scopes": 1,
                "collections": 2,
                "num_items": 2000000,
                "start": 0,
                "end": 2000000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [0, 0, 100, 0, 0], # CRUDE
                "load_type": ["update"],
                "2i": (5, 5),
                "FTS": (5, 5)
                }
            self.load_defn.append(loadDefn3)
            for i in range(1, 6):
                self.create_databases(10)
                self.refresh_dp_obj(self.dataplane_id)
                buckets = self.cluster.buckets[(i-1)*10:(i)*10]
                if i % 2 == 0:
                    kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
                    if kv_nodes < min((i%2+1)*3, 11):
                        self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                        self.check_kv_scaling()
                    elif kv_nodes == min((i%2+1)*3, 11):
                        dataplane_state = self.serverless_util.get_dataplane_info(
                            self.dataplane_id)["couchbase"]["state"]
                        if dataplane_state != "healthy":
                            self.check_kv_scaling()
                        else:
                            self.log.info("KV already scaled up during databases creation")
                    kv_nodes = self.get_num_nodes_in_cluster(service="kv")
                    self.assertTrue(int(kv_nodes) >= min((i%2+1)*3, 11),
                                    "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
                self.create_required_collections(self.cluster, buckets)
                status = self.create_gsi_indexes(buckets)
                print "GSI Status: {}".format(status)
                self.build_gsi_index(buckets)
                self.create_fts_indexes(buckets, wait=True)
                self.start_initial_load(buckets)
                # for bucket in buckets:
                #     bucket.loadDefn["ops"] = 2000
                #     self.generate_docs(bucket=bucket)
                # self.perform_load(validate_data=False, buckets=buckets,
                #                   wait_for_load=False)
                try:
                    for bucket in buckets:
                        self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                        self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
                except TimeoutException as e:
                    print e
                except:
                    pass
                self.sleep(30)
        else:
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
                # for bucket in buckets:
                #     bucket.loadDefn["ops"] = 2000
                #     self.generate_docs(bucket=bucket)
                # self.perform_load(validate_data=False, buckets=buckets,
                #                   wait_for_load=False)
                try:
                    for bucket in buckets:
                        self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                        self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
                except TimeoutException as e:
                    print e
                except:
                    pass
                self.sleep(30)

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
        try:
            for bucket in buckets:
                self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
        except TimeoutException as e:
            print e
        except:
            pass
        for bucket in self.cluster.buckets:
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_cluster_scaling(state="rebalancing")
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

    def PrivatePreviewN1QL(self):
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
            "collections": 5,
            "num_items": 10000000,
            "start": 0,
            "end": 10000000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [0, 80, 20, 0, 0], # CRUDE
            "load_type": ["read", "upsert"],
            "2i": (5, 50),
            "FTS": (5, 5)
            }
        self.load_defn.append(self.loadDefn1)
        self.loadDefn2 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 5,
            "num_items": 4000000,
            "start": 0,
            "end": 4000000,
            "ops": 10000,
            "doc_size": 1024,
            "pattern": [10, 80, 0, 10, 0], # CRUDE
            "load_type": ["create", "read", "delete"],
            "2i": (5, 50),
            "FTS": (5, 5)
            }
        self.load_defn.append(self.loadDefn2)
        self.loadDefn3 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 5,
            "num_items": 2000000,
            "start": 0,
            "end": 2000000,
            "ops": 5000,
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "2i": (5, 50),
            "FTS": (5, 5)
            }
        self.load_defn.append(self.loadDefn3)

        self.input.test_params.update({"clients_per_db": 5})

        self.create_databases(4)
        self.refresh_dp_obj(self.dataplane_id)
        buckets = self.cluster.buckets
        self.create_required_collections(self.cluster, buckets)
        status = self.create_gsi_indexes(buckets)
        print "GSI Status: {}".format(status)
        self.build_gsi_index(buckets)
        self.create_fts_indexes(buckets, wait=True)
        self.start_initial_load(buckets)

        for _ in range(10):
            for bucket in buckets:
                self.generate_docs(bucket=bucket)
            self.perform_load(validate_data=False, buckets=buckets,
                              wait_for_load=False)
            self.doc_loading_tm.getAllTaskResult()
        try:
            for bucket in self.cluster.buckets:
                self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
        except TimeoutException as e:
            print e
        except:
            pass

        self.sleep(600)

        for bucket in self.cluster.buckets:
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_cluster_scaling(state="rebalancing")
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

    def COGS(self):
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
            "collections": 5,
            "num_items": 1000000,
            "start": 0,
            "end": 1000000,
            "ops": 5000,
            "doc_size": 1024,
            "pattern": [0, 80, 20, 0, 0], # CRUDE
            "load_type": ["read", "upsert"],
            "2i": (2, 2),
            "FTS": (1, 1)
            }
        self.load_defn.append(self.loadDefn1)
        self.loadDefn2 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 5,
            "num_items": 1000000,
            "start": 0,
            "end": 1000000,
            "ops": 2000,
            "doc_size": 1024,
            "pattern": [10, 80, 0, 10, 0], # CRUDE
            "load_type": ["create", "read", "delete"],
            "2i": (5, 5),
            "FTS": (1, 1)
            }
        self.load_defn.append(self.loadDefn2)
        self.loadDefn3 = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 5,
            "num_items": 200000,
            "start": 0,
            "end": 200000,
            "ops": 1000,
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "2i": (2, 2),
            "FTS": (2, 2)
            }
        self.load_defn.append(self.loadDefn3)

        for i in range(1, 6):
            self.create_databases(20, load_defn=self.workload)
            self.refresh_dp_obj(self.dataplane_id)
            buckets = self.cluster.buckets[(i-1)*20:(i)*20]
            kv_nodes = len(self.dataplane_objs[self.dataplane_id].kv_nodes)
            if kv_nodes < min((i+1)*3, 11):
                self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                self.check_cluster_scaling()
            elif kv_nodes == min((i+1)*3, 11):
                dataplane_state = self.serverless_util.get_dataplane_info(
                    self.dataplane_id)["couchbase"]["state"]
                if dataplane_state != "healthy":
                    self.check_cluster_scaling()
                else:
                    self.log.info("KV already scaled up during databases creation")
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.assertTrue(int(kv_nodes) >= min((i+1)*3, 11),
                            "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            self.start_initial_load(buckets)
            for bucket in buckets:
                try:
                    self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
                    self.sleep(2, "Closing SDK connection: {}".format(bucket.name))
                except TimeoutException as e:
                    print e
                except:
                    pass
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

        for i in range(10):
            for bucket in self.cluster.buckets:
                self.generate_docs(bucket=bucket)
            self.perform_load(validate_data=False, buckets=buckets)
            self.log.info("Iteration: {} - Data load pattern completed!!!".format(i))

        for bucket in self.cluster.buckets:
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and state is False:
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                self.check_cluster_scaling(state="rebalancing")
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
