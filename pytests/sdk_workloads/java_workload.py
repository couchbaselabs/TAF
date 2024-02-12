from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
import threading
from BucketLib.bucket import Bucket
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from aGoodDoctor.hostedFTS import DoctorFTS, FTSQueryLoad
from aGoodDoctor.hostedN1QL import QueryLoad, DoctorN1QL
from aGoodDoctor.hostedCbas import DoctorCBAS, CBASQueryLoad
from aGoodDoctor.hostedBackupRestore import DoctorHostedBackupRestore
from aGoodDoctor.hostedEventing import DoctorEventing
from aGoodDoctor.hostedOPD import OPD
from table_view import TableView
import time
from cb_constants.CBServer import CbServer
from bucket_utils.bucket_ready_functions import CollectionUtils
from com.couchbase.test.sdk import Server


class JavaSDKWorkload(BaseTestCase, OPD):

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
        BaseTestCase.setUp(self)
        self.init_doc_params()

        self.num_collections = self.input.param("num_collections", 1)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.index_nodes = self.input.param("index_nodes", 0)
        self.backup_restore = self.input.param("bkrs", False)
        self.mutation_perc = 100
        self.threads_calculation()
        self.mutate = 0
        self.iterations = self.input.param("iterations", 10)
        self.end_step = self.input.param("end_step", None)
        self.key_prefix = "Users"
        self.crashes = self.input.param("crashes", 20)
        self.check_dump_thread = True
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.loader_dict = None
        self.track_failures = self.input.param("track_failures", True)
        self._data_validation = self.input.param("data_validation", True)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.key_type = self.input.param("key_type", "SimpleKey")
        self.val_type = self.input.param("val_type", "SimpleValue")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.index_timeout = self.input.param("index_timeout", 3600)
        self.load_defn = list()

        if self.cluster.eventing_nodes:
            self.drEventing = DoctorEventing(self.cluster, self.bucket_util)

        if self.cluster.index_nodes:
            self.drIndex = DoctorN1QL(self.cluster, self.bucket_util)

        if self.cluster.fts_nodes:
            self.drFTS = DoctorFTS(self.cluster, self.bucket_util)

        if self.cluster.cbas_nodes:
            self.drCBAS = DoctorCBAS(self.cluster, self.bucket_util)

        if self.backup_restore:
            self.drBackupRestore = DoctorHostedBackupRestore(cluster=self.cluster,
                                                             bucket_name=self.cluster.buckets[0].name,
                                                             pod=self.pod,
                                                             tenant=self.tenant)

        self.ql = list()
        self.ftsQL = list()
        self.cbasQL = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.cluster.sdk_client_pool = self.bucket_util.initialize_java_sdk_client_pool()
        self.query_result = True

    def tearDown(self):
        self.check_dump_thread = False
        self.stop_crash = True
        self.stop_run = True
        if self.cluster.query_nodes:
            for ql in self.ql:
                ql.stop_query_load()
        if self.cluster.fts_nodes:
            for ql in self.ftsQL:
                ql.stop_query_load()
        if self.cluster.cbas_nodes:
            for ql in self.cbasQL:
                ql.stop_query_load()
        self.sleep(10)
        BaseTestCase.tearDown(self)

    def create_sdk_client_pool(self, buckets, req_clients_per_bucket):
        for bucket in buckets:
            self.log.info("Using SDK endpoint %s" % self.cluster.srv)
            server = Server(self.cluster.srv, self.cluster.master.port,
                            self.cluster.master.rest_username,
                            self.cluster.master.rest_password,
                            str(self.cluster.master.memcached_port))
            self.cluster.sdk_client_pool.create_clients(
                bucket.name, server, req_clients_per_bucket)
            bucket.clients = self.cluster.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
        self.sleep(1, "Wait for SDK client pool to warmup")

    def create_buckets(self):
        self.PrintStep("Step 2: Create required buckets and collections.")
        self.log.info("Create CB buckets")
        # Create Buckets
        for cluster in [self.cluster, self.xdcr_cluster]:
            if cluster:
                self.log.info("Get the available memory quota")
                rest = RestConnection(cluster.master)
                self.info = rest.get_nodes_self()
                # threshold_memory_vagrant = 100
                kv_memory = int(self.info.memoryQuota*0.8)
                ramQuota = self.input.param("ramQuota", kv_memory)
                buckets = ["default"] * self.num_buckets
                bucket_type = self.bucket_type.split(';') * self.num_buckets
                for i in range(self.num_buckets):
                    bucket = Bucket(
                        {Bucket.name: buckets[i] + str(i),
                         Bucket.ramQuotaMB: ramQuota / self.num_buckets,
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
                        "storageBackend": bucket.storageBackend,
                        "durabilityLevel": bucket.durability_level,
                        "timeToLive": {"unit": "seconds", "value": bucket.maxTTL}
                    }
                    CapellaAPI.create_bucket(cluster, self.bucket_params)
                    self.bucket_util.get_updated_bucket_server_list(cluster, bucket)
                    bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                    cluster.buckets.append(bucket)
                if not cluster == self.xdcr_cluster:
                    self.buckets = cluster.buckets
                    num_clients = self.input.param("clients_per_db",
                                                   min(5, bucket.loadDefn.get("collections")))
                    for bucket in cluster.buckets:
                        self.create_sdk_client_pool([bucket],
                                                    num_clients)
                self.create_required_collections(cluster)


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
                                                      "Avg Execution Time(ms)"])
                        try:
                            for query in sorted(ql.query_stats.keys()):
                                if ql.query_stats[query][1] > 0:
                                    self.query_table.add_row([str(ql.bucket.name),
                                                              ql.bucket.query_map[query][0],
                                                              ql.query_stats[query][1],
                                                              ql.query_stats[query][0]/ql.query_stats[query][1]])
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
                    self.table.display("N1QL Query Statistics")
                    self.query_table.display("N1QL Query Execution Stats")

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
                    self.FTStable.display("FTS Query Statistics")

                    self.CBAStable = TableView(self.log.info)
                    self.CBAStable.set_headers(["Bucket",
                                                "Total Queries",
                                                "Failed Queries",
                                                "Success Queries",
                                                "Rejected Queries",
                                                "Cancelled Queries",
                                                "Timeout Queries",
                                                "Errored Queries"])
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
                        if ql.failures > 0:
                            self.query_result = False
                    self.CBAStable.display("CBAS Query Statistics")

                    st_time = time.time()
                    time.sleep(10)

        query_monitor = threading.Thread(target=check_query_stats)
        query_monitor.start()

    def run_workload(self):
        self.monitor_query_status()
        cpu_monitor = threading.Thread(target=self.print_cluster_cpu_ram,
                                       kwargs={"cluster": self.cluster})
        cpu_monitor.start()

        self.nimbus = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 2,
            "num_items": self.input.param("num_items", 1500000000),
            "start": 0,
            "end": self.input.param("num_items", 1500000000),
            "ops": 100000,
            "doc_size": 1024,
            "pattern": [0, 90, 10, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 300,
            "ftsQPS": 0,
            "cbasQPS": 0,
            "collections_defn": [
                {
                    "valType": "NimbusP",
                    "2i": [2, 2],
                    "FTS": [0, 0],
                    "cbas": [0, 0, 0]
                },
                {
                    "valType": "NimbusM",
                    "2i": [1, 2],
                    "FTS": [0, 0],
                    "cbas": [0, 0, 0]
                }
                ]
            }
        self.default = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": self.input.param("collections", 2),
            "num_items": self.input.param("num_items", 50000000),
            "start": 0,
            "end": self.input.param("num_items", 50000000),
            "ops": self.input.param("ops_rate", 50000),
            "doc_size": 1024,
            "pattern": [0, 80, 20, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 10,
            "ftsQPS": 10,
            "cbasQPS": 0,
            "collections_defn": [
                {
                    "valType": "Hotel",
                    "2i": [2, 2],
                    "FTS": [2, 2],
                    "cbas": [2, 2, 2]
                    },
                {
                    "valType": "Hotel",
                    "2i": [2, 2],
                    "FTS": [0, 0],
                    "cbas": [2, 2, 2]
                    }
                ]
            }

        temp = [{
            "valType": "Hotel",
            "2i": [0, 0],
            "FTS": [0, 0],
            "cbas": [0, 0, 0]
            }]*8
        self.default["collections_defn"].extend(temp)

        nimbus = self.input.param("nimbus", False)
        expiry = self.input.param("expiry", False)
        self.load_defn.append(self.default)
        if nimbus:
            self.load_defn = list()
            self.load_defn.append(self.nimbus)

        if expiry:
            for load in self.load_defn:
                load["pattern"] = [0, 90, 0, 0, 10]
                load["load_type"] = ["read", "expiry"]
        #######################################################################
        if not self.skip_init:
            self.create_buckets()
        else:
            for i, bucket in enumerate(self.cluster.buckets):
                bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                num_clients = self.input.param("clients_per_db",
                                               min(5, bucket.loadDefn.get("collections")))
                self.create_sdk_client_pool([bucket],
                                            num_clients)
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
                    if bucket.loadDefn.get("collections") > 0:
                        self.collection_prefix = self.input.param("collection_prefix",
                                                                  "VolumeCollection")

                        for i in range(bucket.loadDefn.get("collections")):
                            collection_name = self.collection_prefix + str(i)
                            collection_spec = {"name": collection_name}
                            CollectionUtils.create_collection_object(bucket, scope, collection_spec)

        self.skip_read_on_error = True
        self.suppress_error_table = True
        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''

        self.PrintStep("Step 2: Create %s items: %s" % (self.num_items, self.key_type))
        for bucket in self.cluster.buckets:
            self.generate_docs(doc_ops=["create","update", "read", "delete"],
                               create_start=0,
                               create_end=bucket.loadDefn.get("num_items")/2,
                               read_start=0,
                               read_end=bucket.loadDefn.get("num_items") / 2,
                               update_start=0,
                               update_end=bucket.loadDefn.get("num_items") / 2,
                               delete_start=0,
                               delete_end=bucket.loadDefn.get("num_items") / 2,
                               bucket=bucket)
        if not self.skip_init:
            self.perform_load(validate_data=False, buckets=self.cluster.buckets, overRidePattern=[100,100,50,0,0])

        self.PrintStep("Step 3: Create %s items: %s" % (self.num_items, self.key_type))
        for bucket in self.cluster.buckets:
            self.generate_docs(doc_ops=["create","update", "read", "delete"],
                               create_start=bucket.loadDefn.get("num_items")/2,
                               create_end=bucket.loadDefn.get("num_items"),
                               read_start=bucket.loadDefn.get("num_items") / 2,
                               read_end=bucket.loadDefn.get("num_items"),
                               update_start=bucket.loadDefn.get("num_items") / 2,
                               update_end=bucket.loadDefn.get("num_items"),
                               delete_start=bucket.loadDefn.get("num_items") / 2,
                               delete_end=bucket.loadDefn.get("num_items"),
                               bucket=bucket)
        if not self.skip_init:
            self.perform_load(validate_data=False, buckets=self.cluster.buckets, overRidePattern=[100,100,50,0,0])

        if self.cluster.cbas_nodes:
            self.drCBAS.create_datasets(self.cluster.buckets)
            self.drCBAS.create_indexes(self.cluster.buckets)
            result = self.drCBAS.wait_for_ingestion(
                self.cluster.buckets, self.index_timeout)
            self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("cbasQPS", 0) > 0:
                    ql = CBASQueryLoad(bucket)
                    ql.start_query_load()
                    self.cbasQL.append(ql)

        if self.cluster.index_nodes:
            self.drIndex.create_indexes(self.cluster.buckets)
            self.drIndex.build_indexes(self.cluster.buckets, wait=True)
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("2iQPS", 0) > 0:
                    ql = QueryLoad(bucket)
                    ql.start_query_load()
                    self.ql.append(ql)

        if self.cluster.eventing_nodes:
            self.drEventing.create_eventing_functions()
            self.drEventing.lifecycle_operation_for_all_functions("deploy", "deployed")

        if self.cluster.fts_nodes:
            self.drFTS.create_fts_indexes(self.cluster.buckets)
            status = self.drFTS.wait_for_fts_index_online(self.cluster.buckets,
                                                          self.index_timeout)
            self.assertTrue(status, "FTS index build failed.")
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("ftsQPS", 0) > 0:
                    ql = FTSQueryLoad(bucket)
                    ql.start_query_load()
                    self.ftsQL.append(ql)

        self.mutation_perc = self.input.param("mutation_perc", 100)
        for bucket in self.cluster.buckets:
            bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 5000)
            self.generate_docs(bucket=bucket)
        self.tasks = self.perform_load(wait_for_load=False)
        self.sleep(300)
