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
            self.num_buckets = self.input.param("num_buckets", 1)
            self.kv_nodes = self.nodes_init
            self.cbas_nodes = self.input.param("cbas_nodes", 0)
            self.fts_nodes = self.input.param("fts_nodes", 0)
            self.index_nodes = self.input.param("index_nodes", 0)
            self.backup_nodes = self.input.param("backup_nodes", 0)
            self.xdcr_remote_nodes = self.input.param("xdcr_remote_nodes", 0)
            self.num_indexes = self.input.param("num_indexes", 0)
            self.mutation_perc = 100
            self.doc_ops = self.input.param("doc_ops", "create")
            if self.doc_ops:
                self.doc_ops = self.doc_ops.split(':')

            self.threads_calculation()
            self.op_type = self.input.param("op_type", "create")
            self.dgm = self.input.param("dgm", None)
            self.num_buckets = self.input.param("num_buckets", 1)
            self.mutate = 0
            self.iterations = self.input.param("iterations", 10)
            self.step_iterations = self.input.param("step_iterations", 1)
            self.rollback = self.input.param("rollback", True)
            self.vbucket_check = self.input.param("vbucket_check", True)
            self.end_step = self.input.param("end_step", None)
            self.key_prefix = "Users"
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
            self.val_type = self.input.param("val_type", "SimpleValue")
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
                "scopes": 1,
                "collections": 10,
                "num_items": 200000,
                "start": 0,
                "end": 200000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (2, 2),
                "FTS": (2, 2)
                }
            self.gsiAutoScaleLoadDefn = {
                "scopes": 1,
                "collections": 10,
                "num_items": 200000,
                "start": 0,
                "end": 200000,
                "ops": 5000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (100, 0),
                "FTS": (0, 0)
                }
            self.loadDefn100 = {
                "scopes": 1,
                "collections": 1,
                "num_items": 20000000,
                "start": 0,
                "end": 2000000,
                "ops": 4000,
                "doc_size": 1024,
                "pattern": [10, 80, 0, 10, 0], # CRUDE
                "load_type": ["create", "read", "delete"],
                "2i": (10, 10),
                "FTS": (10, 10)
                }
            self.loadDefn1 = {
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
            self.drIndex = DoctorN1QL(self.cluster, self.bucket_util)
            self.drFTS = DoctorFTS(self.cluster, self.bucket_util)
            self.sdk_client_pool = self.bucket_util.initialize_java_sdk_client_pool()
            self.stop_run = False
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

        BaseTestCase.tearDown(self)

    def create_gsi_indexes(self, buckets):
        self.drIndex.create_indexes(buckets)

    def build_gsi_index(self, buckets):
        self.drIndex.build_indexes(buckets, self.dataplane_objs, wait=True)
        for bucket in buckets:
            # self.drIndex.build_indexes([bucket])
            # self.drIndex.wait_for_indexes_online(self.log, self.dataplane_objs,
            #                                      [bucket])
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
                # self.assertTrue(int(gsi_nodes) > min((i+1)*2, 9),
                #                 "GSI nodes - Actual: {}, Expected: {}".
                #                 format(gsi_nodes, gsi_nodes+2))
                self.log.critical("GSI nodes - Actual: {}, Expected: {}".
                                  format(curr_gsi_nodes, prev_gsi_nodes+2))
                break
            count -= 1

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

    def check_fts_scaling(self):
        def check():
            while not self.stop_run:
                self.log.info("FTS - Check for scale operation.")
                if self.drFTS.scale_down or self.drFTS.scale_up:
                    self.log.info("FTS - Scale operation should trigger in a while.")
                    self.check_cluster_scaling(service="FTS", state="scaling")
                    self.drFTS.scale_down, self.drFTS.scale_up = False, False
                    self.drFTS.scale_down_count = 0
                    self.drFTS.scale_up_count = 0
                time.sleep(60)
        fts_scaling_monitor = threading.Thread(target=check)
        fts_scaling_monitor.start()

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
        self.sleep(5, "Wait for SDK client pool to warmup")

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
                    self.database_name = "VolumeTestBucket-{}".format(self.bucket_count)
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
                    task = self.bucket_util.async_create_database(self.cluster, bucket,
                                                                  tenant,
                                                                  dataplane_id)
                    temp.append((task, bucket))
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

        num_clients = self.input.param("clients_per_db", 5)
        for _, bucket in temp:
            self.create_sdk_client_pool([bucket],
                                        num_clients)

    def check_cluster_scaling(self, dataplane_id=None, service="kv", state="scaling"):
        dataplane_id = dataplane_id or self.dataplane_id
        pprint.pprint(self.serverless_util.get_scaling_records(dataplane_id))
        dataplane_state = self.serverless_util.get_dataplane_info(
            dataplane_id)["couchbase"]["state"]
        scaling_timeout = 3600
        while dataplane_state == "healthy" and scaling_timeout >= 0:
            dataplane_state = self.serverless_util.get_dataplane_info(
                dataplane_id)["couchbase"]["state"]
            self.log.info("Cluster state is: {}. Target: {} for {}"
                          .format(dataplane_state, state, service))
            time.sleep(2)
            scaling_timeout -= 2
        if not service.lower() in ["gsi", "fts"]:
            self.assertEqual(dataplane_state, state,
                             "Cluster scaling did not trigger in {} seconds.\
                             Actual: {} Expected: {}".format(
                                 scaling_timeout, dataplane_state, state))

        scaling_timeout = 7200
        while dataplane_state != "healthy" and scaling_timeout >= 0:
            dataplane_state = self.serverless_util.get_dataplane_info(
                dataplane_id)["couchbase"]["state"]
            self.log.info("Cluster state is: {}. Target: {} for {}"
                          .format(dataplane_state, state, service))
            time.sleep(2)
            scaling_timeout -= 2
        pprint.pprint(self.serverless_util.get_scaling_records(dataplane_id))
        if not service.lower() in ["gsi", "fts"]:
            self.assertEqual(dataplane_state, "healthy",
                             "Cluster scaling started but did not completed in {} seconds.\
                             Actual: {} Expected: {}".format(
                                 scaling_timeout, dataplane_state, "healthy"))
        if state == "scaling":
            dp = self.dataplane_objs[dataplane_id]
            records = Lookup("_couchbases._tcp.%s" % dp.srv, Type.SRV).run()
            ip = str(records[0].getTarget())
            servers = RestConnection({"ip": ip,
                                      "username": dp.user,
                                      "password": dp.pwd,
                                      "port": 18091}).get_nodes()
            dp.refresh_object(servers)

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
        content = rest.get_pools_default()
        return content["balanced"]

    def check_ebs_scaling(self):
        '''
        1. check current disk used
        2. If disk used > 50% check for EBS scale on all nodes for that service
        '''

        def check_disk():
            while not self.stop_run:
                for dataplane in self.dataplane_objs.values():
                    records = Lookup("_couchbases._tcp.%s" % dataplane.srv, Type.SRV).run()
                    ip = str(records[0].getTarget())
                    servers = RestConnection({"ip": ip,
                                              "username": dataplane.user,
                                              "password": dataplane.pwd,
                                              "port": 18091}).get_nodes()
                    dataplane.refresh_object(servers)
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
        self.disk = [0, 75, 150, 225, 300, 375, 450]
        self.memory = [256, 256, 384, 512, 640, 768, 896]

        def check_ram():
            while not self.stop_run:
                for dataplane in self.dataplane_objs.values():
                    self.rest = BucketHelper(dataplane.master)
                    table = TableView(self.log.info)
                    table.set_headers(["Bucket",
                                       "Total Ram(MB)",
                                       "Total Data(GB)"])
                    for bucket in self.cluster.buckets:
                        data = self.rest.get_bucket_json(bucket.name)
                        ramMB = data["quota"]["rawRAM"] / (1024 * 1024)
                        dataGB = data["basicStats"]["diskUsed"] / (1024 * 1024 * 1024)
                        table.add_row([bucket.name, ramMB, dataGB])
                        for i, disk in enumerate(self.disk):
                            if disk > dataGB:
                                self.log.info("{}: Disk used reported = {}".
                                              format(bucket.name,
                                                     dataGB))
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

    def SteadyStateVolume(self):
        #######################################################################
        self.PrintStep("Step 1: Create required buckets and collections.")
        self.log.info("Create CB buckets")
        # Create Buckets
        self.create_databases(self.num_buckets, load_defn=self.loadDefn100)
        self.create_required_collections(self.cluster, self.cluster.buckets)
        # self.create_gsi_indexes(self.cluster.buckets)
        # self.build_gsi_index(self.cluster.buckets)
        #
        # self.create_fts_indexes(self.cluster.buckets, wait=True)

        self.loop = 1
        self.create_perc = 100

        self.start_initial_load(self.cluster.buckets)
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

        # for load in self.ql:
        #     load.stop_run = True
        # for load in self.ftsQL:
        #     load.stop_run = True
        # for drIndex in self.drIndexes:
        #     for client in drIndex.sdkClients.values():
        #         client.close()

    def ElixirVolume(self):
        #######################################################################
        self.PrintStep("Step: Create Serverless Databases")

        # self.drFTS.index_stats(self.dataplane_objs)
        self.drIndex.index_stats(self.dataplane_objs)
        self.check_ebs_scaling()
        self.check_memory_management()
        # self.check_fts_scaling()
        for i in range(0, 5):
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.create_databases(20, load_defn=self.defaultLoadDefn)
            buckets = self.cluster.buckets[i*20:(i+1)*20]
            if kv_nodes <= min((i+1)*3, 11):
                self.PrintStep("Step: Test KV Auto-Scaling due to num of databases per sub-cluster")
                self.check_cluster_scaling()
            kv_nodes = self.get_num_nodes_in_cluster(service="kv")
            self.assertTrue(int(kv_nodes) > min((i+1)*3, 11),
                            "Incorrect number of kv nodes in the cluster - Actual: {}, Expected: {}".format(kv_nodes, kv_nodes+3))
            self.create_required_collections(self.cluster, buckets)
            self.start_initial_load(buckets)
            for dataplane in self.dataplane_objs.values():
                prev_gsi_nodes = self.get_num_nodes_in_cluster(dataplane.id,
                                                               service="index")
                self.create_gsi_indexes(buckets)
                self.check_gsi_scaling(dataplane, prev_gsi_nodes)
            self.build_gsi_index(buckets)
            self.create_fts_indexes(buckets, wait=True)
            self.sleep(30)

        count = 0
        self.PrintStep("Step: Test KV Auto-Rebalance/Defragmentation")
        for bucket in self.cluster.buckets:
            start = time.time()
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket.serverless.dataplane_id])
            while start + 3600 > time.time() and not state:
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
                ql.stop_run = True
            if self.ftsQL:
                ql = [load for load in self.ftsQL if load.bucket == bucket][0]
                ql.stop_run = True
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)
            self.sdk_client_pool.force_close_clients_for_bucket(bucket.name)
            count += 1
            if count == 50:
                self.check_cluster_scaling(state="rebalancing")
        self.cluster.buckets = list()
        self.ql = []
        self.ftsQL = []

        # Reset cluster specs to default
        self.log.info("Reset cluster specs to default to proceed further")
        config = self.dataplane_config["overRide"]["couchbase"]["specs"]
        self.serverless_util.change_dataplane_cluster_specs(self.dataplane_id, config)
        self.check_cluster_scaling()

        # self.create_databases(19, load_defn=self.gsiAutoScaleLoadDefn)
        self.create_databases(19)
        self.create_required_collections(self.cluster,
                                         self.cluster.buckets[0:20])
        self.start_initial_load(self.cluster.buckets[0:20])
        self.create_gsi_indexes(self.cluster.buckets[0:20])
        self.build_gsi_index(self.cluster.buckets[0:20])
        self.check_cluster_scaling(service="gsi")
        self.create_fts_indexes(self.cluster.buckets, wait=True)
        for bucket in self.cluster.buckets:
            self.generate_docs(bucket=bucket)
        load_tasks = self.perform_load(validate_data=True,
                                       buckets=self.cluster.buckets,
                                       wait_for_load=False)
        self.PrintStep("Step 3: Test Bucket-Rebalancing(change width)")
        for width in [2, 3, 4]:
            self.PrintStep("Test change bucket width to {}".format(width))
            target_kv_nodes = 3 * width
            actual_kv_nodes = self.get_num_nodes_in_cluster()
            for bucket_obj in self.cluster.buckets[::2]:
                # Update the width of the buckets multiple times
                override = {"width": width, "weight": 60}
                bucket_obj.serverless.width = width
                bucket_obj.serverless.weight = 60
                self.serverless_util.update_database(bucket_obj.name, override)
                self.log.info("Updated width for bucket {} to {}".format(bucket_obj.name, width))
            if target_kv_nodes > actual_kv_nodes:
                self.check_cluster_scaling()
                self.log.info("Cluster is scaled up due the change in bucket width and ready to rebalance buckets")
            # start = time.time()
            # state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
            # while start + 3600 > time.time() and state:
            #     self.log.info("Balanced state of the cluster: {}"
            #                   .format(state))
            #     state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
            # self.log.info("Balanced state of the cluster: {}".format(state))
            # self.assertFalse(state, "Balanced state of the cluster: {}"
            #                  .format(state))
            self.check_cluster_scaling(state="rebalancing")
            # state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
            # self.assertTrue(state, "Balanced state of the cluster: {}".format(state))
            self.log.info("Buckets are rebalanced after change in their width")

            for bucket in self.cluster.buckets:
                self.update_bucket_nebula_and_kv_nodes(self.cluster, bucket)
                while len(self.cluster.bucketDNNodes[bucket]) < bucket.serverless.width*3:
                    self.check_cluster_scaling(state="rebalancing")
                    self.update_bucket_nebula_and_kv_nodes(self.cluster, bucket)
                self.assertEqual(len(self.cluster.bucketDNNodes[bucket]),
                                 bucket.serverless.width*3,
                                 "Bucket width and number of nodes mismatch")

        self.doc_loading_tm.abort_all_tasks()
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
            state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
            while start + 3600 > time.time() and not state:
                self.check_cluster_scaling(state="rebalancing")
                self.log.info("Balanced state of the cluster: {}"
                              .format(state))
                state = self.get_cluster_balanced_state(self.dataplane_objs[bucket_obj.serverless.dataplane_id])
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
        self.start_initial_load(buckets)
        self.create_fts_indexes(buckets, wait=False)
        self.create_gsi_indexes(buckets)
        self.build_gsi_index(buckets)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.create_databases(20)
        buckets = self.cluster.buckets[20:40]
        self.create_required_collections(self.cluster, buckets)
        self.create_fts_indexes(buckets, wait=False)
        self.start_initial_load(buckets)
        self.create_gsi_indexes(buckets)
        self.build_gsi_index(buckets)
        for bucket in buckets:
            self.generate_docs(bucket=bucket)
        tasks.append(self.perform_load(wait_for_load=False, buckets=buckets))

        self.create_databases(20)
        buckets = self.cluster.buckets[40:60]
        self.create_required_collections(self.cluster, buckets)
        self.create_gsi_indexes(buckets)
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
