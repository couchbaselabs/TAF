'''
Created on May 2, 2022

@author: ritesh.agarwal
'''
import random

from membase.api.rest_client import RestConnection
import threading
from BucketLib.bucket import Bucket
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from pytests.basetestcase import BaseTestCase
from constants.cb_constants.CBServer import CbServer
try:
    from fts import DoctorFTS, FTSQueryLoad
except:
    pass
from n1ql import QueryLoad, DoctorN1QL
from cbas import DoctorCBAS, CBASQueryLoad
from hostedXDCR import DoctorXDCR
from hostedBackupRestore import DoctorHostedBackupRestore
from hostedOnOff import DoctorHostedOnOff
from hostedEventing import DoctorEventing
from constants.cloud_constants.capella_constants import AWS, GCP
from table_view import TableView
import time
from bucket_utils.bucket_ready_functions import CollectionUtils
from com.couchbase.test.sdk import Server
from TestInput import TestInputServer
from capella_utils.dedicated import CapellaUtils as DedicatedUtils
import pprint
from custom_exceptions.exception import ServerUnavailableException
from exceptions import IndexError
from elasticsearch import EsClient
import string
from hostedOPD import hostedOPD


class Murphy(BaseTestCase, hostedOPD):

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
        self.xdcr_collections = self.input.param("xdcr_collections", self.num_collections)
        self.num_collections_bkrs = self.input.param("num_collections_bkrs", self.num_collections)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.rebalance_type = self.input.param("rebalance_type", "all")
        self.backup_restore = self.input.param("bkrs", False)
        self.mutation_perc = 100
        self.threads_calculation()
        self.dgm = self.input.param("dgm", None)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.iterations = self.input.param("iterations", 10)
        self.key_prefix = "Users"
        self.crashes = self.input.param("crashes", 20)
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.track_failures = self.input.param("track_failures", True)
        self.loader_dict = None
        self._data_validation = self.input.param("data_validation", True)
        self.turn_cluster_off = self.input.param("cluster_off", False)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.key_type = self.input.param("key_type", "SimpleKey")
        self.val_type = self.input.param("val_type", "SimpleValue")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.gtm = self.input.param("gtm", False)
        self.index_timeout = self.input.param("index_timeout", 3600)
        self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                       True)
        self.num_of_datasets = self.input.param("num_datasets", 10)
        self.load_defn = list()

        self.drEventing = DoctorEventing(self.bucket_util)
        self.drIndex = DoctorN1QL(self.bucket_util)
        self.drFTS = DoctorFTS(self.bucket_util)
        self.drCBAS = DoctorCBAS(self.bucket_util)

        self.drBackupRestore = DoctorHostedBackupRestore(pod=self.pod)
        if self.xdcr_remote_clusters > 0:
            self.drXDCR = DoctorXDCR(pod=self.pod)

        self.ql = list()
        self.ftsQL = list()
        self.cbasQL = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.query_result = True

        self.vector = self.input.param("vector", False)
        self.esHost = self.input.param("esHost", None)
        self.esAPIKey = self.input.param("esAPIKey", None)
        if self.esHost:
            self.esHost = "http://" + self.esHost + ":9200"
        if self.esAPIKey:
            self.esAPIKey = "".join(self.esAPIKey.split(","))

    def tearDown(self):
        self.stop_crash = True
        self.stop_run = True
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.index_nodes:
                    self.drIndex.discharge_N1QL()
                if cluster.query_nodes:
                    for ql in self.ql:
                        ql.stop_query_load()
                if cluster.fts_nodes:
                    for ql in self.ftsQL:
                        ql.stop_query_load()
                if cluster.cbas_nodes:
                    for ql in self.cbasQL:
                        ql.stop_query_load()
        self.sleep(10)
        BaseTestCase.tearDown(self)

    def create_sdk_client_pool(self, cluster, buckets, req_clients_per_bucket):
        for bucket in buckets:
            self.log.info("Using SDK endpoint %s" % cluster.srv)
            server = Server(cluster.srv, cluster.master.port,
                            cluster.master.rest_username,
                            cluster.master.rest_password,
                            str(cluster.master.memcached_port))
            cluster.sdk_client_pool.create_clients(bucket.name, server, req_clients_per_bucket)
            bucket.clients = cluster.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
        self.sleep(1, "Wait for SDK client pool to warmup")

    def rebalance_config(self, rebl_service_group=None, num=0):
        provider = self.input.param("provider", "aws").lower()

        _type = AWS.StorageType.GP3 if provider == "aws" else "pd-ssd"
        storage_type = self.input.param("type", _type).lower()

        specs = []
        for service_group in self.services:
            _services = sorted(service_group.split(":"))
            service = _services[0]
            if service_group in self.rebl_services and service_group == rebl_service_group:
                self.num_nodes[service] = self.num_nodes[service] + num
            spec = {
                "count": self.num_nodes[service],
                "compute": {
                    "type": self.compute[service],
                },
                "services": [{"type": self.services_map[_service.lower()]} for _service in _services],
                "disk": {
                    "type": storage_type,
                    "sizeInGb": self.disk[service]
                },
                "diskAutoScaling": {"enabled": self.diskAutoScaling}
            }
            if provider == "aws":
                aws_storage_range = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
                aws_min_iops = [3000, 4370, 5740, 7110, 8480, 9850, 11220, 12590, 13960, 15330, 16000]
                for i, storage in enumerate(aws_storage_range):
                    if self.disk[service] >= storage:
                        self.iops[service] = aws_min_iops[i+1]
                    if self.disk[service] < 100:
                        self.iops[service] = aws_min_iops[0]
                spec["disk"]["iops"] = self.iops[service]
            specs.append(spec)

        self.PrintStep("Rebalance Config:")
        pprint.pprint(specs)
        return specs

    def create_buckets(self, pod, tenant, cluster, sdk_init=True):
        self.PrintStep("Step 2: Create required buckets and collections.")
        self.log.info("Create CB buckets")
        # Create Buckets
        self.log.info("Get the available memory quota")
        rest = RestConnection(cluster.master)
        self.info = rest.get_nodes_self()
        # threshold_memory_vagrant = 100
        kv_memory = int(self.info.memoryQuota*0.8)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = ["default"] * self.num_buckets
        bucket_type = self.bucket_type.split(';') * self.num_buckets
        for i in range(self.num_buckets):
            suffix = ''.join([random.choice(string.ascii_uppercase + string.digits) for _ in range(5)])
            bucket = Bucket(
                {Bucket.name: buckets[i] + str(i) + "_%s" % suffix,
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
                "durabilityLevel": bucket.durabilityMinLevel,
                "timeToLive": {"unit": "seconds", "value": bucket.maxTTL}
            }
            CapellaAPI.create_bucket(pod, tenant, cluster, self.bucket_params)
            self.bucket_util.get_updated_bucket_server_list(cluster, bucket)
            bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
            cluster.buckets.append(bucket)
        if sdk_init:
            num_clients = self.input.param("clients_per_db",
                                           min(5, bucket.loadDefn.get("collections")))
            for bucket in cluster.buckets:
                self.create_sdk_client_pool(cluster, [bucket],
                                            num_clients)
        self.create_required_collections(cluster)

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

    def refresh_cluster(self, tenant, cluster):
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
                    temp_server.type = "dedicated"
                    nodes.append(temp_server)
                cluster.refresh_object(nodes)

    def initial_setup(self):
        self.monitor_query_status()
        for tenant in self.tenants:
            for cluster in tenant.clusters:  
                cpu_monitor = threading.Thread(target=self.print_cluster_cpu_ram,
                                               kwargs={"cluster": cluster})
                cpu_monitor.start()

        self.nimbus = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": 2,
            "num_items": self.input.param("num_items", 1500000000),
            "start": 0,
            "end": self.input.param("num_items", 1500000000),
            "ops": self.input.param("ops_rate", 100000),
            "doc_size": 1024,
            "pattern": [0, 50, 50, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 300,
            "ftsQPS": 100,
            "cbasQPS": 100,
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
            "pattern": [0, 50, 50, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 10,
            "ftsQPS": 10,
            "cbasQPS": 10,
            "collections_defn": [
                {
                    "valType": "Hotel",
                    "2i": [self.input.param("gsi_indexes", 2),
                           self.input.param("gsi_queries", 2)],
                    "FTS": [self.input.param("fts_indexes", 2),
                            self.input.param("fts_queries", 2)],
                    "cbas": [self.input.param("cbas_indexes", 2),
                             self.input.param("cbas_datasets", 2),
                             self.input.param("cbas_queries", 2)]
                    }
                # {
                #     "valType": "Hotel",
                #     "2i": [self.input.param("gsi_indexes", 2),
                #            self.input.param("gsi_queries", 2)],
                #     "FTS": [0, 0],
                #     "cbas": [self.input.param("cbas_datasets", 2),
                #              self.input.param("cbas_indexes", 2),
                #              self.input.param("cbas_queries", 2)]
                #     }
                ]
            }
        
        self.vector_load = {
            "valType": "Vector",
            "scopes": 1,
            "collections": self.input.param("collections", 2),
            "num_items": self.input.param("num_items", 5000000),
            "start": 0,
            "end": self.input.param("num_items", 5000000),
            "ops": self.input.param("ops_rate", 5000),
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "2iQPS": 10,
            "ftsQPS": 10,
            "cbasQPS": 0,
            "collections_defn": [
                {
                    "valType": "Vector",
                    "2i": [2, 2],
                    "FTS": [2, 2],
                    "cbas": [2, 2, 2]
                    }
                ]
            }

        # temp = [{
        #     "valType": "Hotel",
        #     "2i": [0, 0],
        #     "FTS": [0, 0],
        #     "cbas": [0, 0, 0]
        #     }]*8
        # self.default["collections_defn"].extend(temp)

        nimbus = self.input.param("nimbus", False)
        expiry = self.input.param("expiry", False)
        self.load_defn.append(self.default)
        if nimbus:
            self.load_defn = list()
            self.load_defn.append(self.nimbus)

        if expiry:
            for load in self.load_defn:
                load["pattern"] = [0, 80, 0, 0, 20]
                load["load_type"] = ["read", "expiry"]

        if self.vector:
            self.load_defn = list()
            self.load_defn.append(self.vector_load)

        #######################################################################
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if not self.skip_init:
                    self.create_buckets(self.pod, tenant, cluster)
                else:
                    for i, bucket in enumerate(cluster.buckets):
                        bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                        num_clients = self.input.param("clients_per_db",
                                                       min(5, bucket.loadDefn.get("collections")))
                        self.create_sdk_client_pool(cluster, [bucket],
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

        for tenant in self.tenants:
            for cluster in tenant.xdcr_clusters:
                if not self.skip_init:
                    self.create_buckets(self.pod, tenant, cluster, sdk_init=False)

        self.skip_read_on_error = True
        self.suppress_error_table = True

        self.esClient = None
        self.model = self.input.param("model", "sentence-transformers/all-MiniLM-L6-v2")
        self.mockVector = self.input.param("mockVector", True)
        self.dim = self.input.param("dim", 384)
        if self.esHost and self.esAPIKey:
            self.esClient = EsClient(self.esHost, self.esAPIKey)
            self.esClient.initializeSDK()
            if not self.self.skip_init:
                for bucket in self.cluster.buckets:
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
                        for collection in bucket.scopes[scope].collections.keys():
                            if scope == CbServer.system_scope:
                                continue
                            if collection == "_default" and scope == "_default":
                                continue 
                            self.esClient.deleteESIndex(collection.lower())
                            self.esClient.createESIndex(collection.lower(), None)
        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''
        tasks = list()
        self.PrintStep("Step 2: Create %s items: %s" % (self.num_items, self.key_type))
        for tenant in self.tenants:
            i = 0
            for cluster in tenant.clusters:
                for bucket in cluster.buckets:
                    self.generate_docs(doc_ops=["create"],
                                       create_start=0,
                                       create_end=bucket.loadDefn.get("num_items")/2,
                                       bucket=bucket)
                if not self.skip_init:
                    tasks.append(self.perform_load(wait_for_load=False, cluster=cluster, buckets=cluster.buckets, overRidePattern=[100,0,0,0,0]))
                    if self.xdcr_remote_clusters > 0:
                        self.drXDCR.set_up_replication(tenant, source_cluster=cluster, destination_cluster=tenant.xdcr_clusters[i],
                                     source_bucket=cluster.buckets[0].name,
                                     destination_bucket=tenant.xdcr_clusters[i].buckets[0].name,)
                i += 1
        for tenant in self.tenants:
            i = 0
            for cluster in tenant.clusters:
                if tasks:
                    self.wait_for_doc_load_completion(cluster, tasks[i])
            i += 1

        tasks = list()
        self.PrintStep("Step 3: Create %s items: %s" % (self.num_items, self.key_type))
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for bucket in cluster.buckets:
                    self.generate_docs(doc_ops=["create"],
                                       create_start=bucket.loadDefn.get("num_items")/2,
                                       create_end=bucket.loadDefn.get("num_items"),
                                       bucket=bucket)
                if not self.skip_init:
                    tasks.append(self.perform_load(wait_for_load=False, cluster=cluster, buckets=cluster.buckets, overRidePattern=[100,0,0,0,0]))
        for tenant in self.tenants:
            i = 0
            for cluster in tenant.clusters:
                if tasks:
                    self.wait_for_doc_load_completion(cluster, tasks[i])
            i += 1

        tasks = list()
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.cbas_nodes:
                    self.drCBAS.create_datasets(cluster.buckets)
                    self.drCBAS.create_indexes(cluster.buckets)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.cbas_nodes:
                    result = self.drCBAS.wait_for_ingestion(
                        cluster.buckets, self.index_timeout)
                    self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)
                    for bucket in cluster.buckets:
                        if bucket.loadDefn.get("cbasQPS", 0) > 0:
                            ql = CBASQueryLoad(bucket)
                            ql.start_query_load()
                            self.cbasQL.append(ql)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.index_nodes:
                    self.drIndex.create_indexes(cluster.buckets)
                    self.drIndex.build_indexes(cluster, cluster.buckets, wait=True)
                    for bucket in cluster.buckets:
                        if bucket.loadDefn.get("2iQPS", 0) > 0:
                            ql = QueryLoad(bucket)
                            ql.start_query_load()
                            self.ql.append(ql)
                    self.drIndex.start_index_stats(cluster)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.eventing_nodes:
                    self.drEventing.create_eventing_functions(cluster)
                    self.drEventing.lifecycle_operation_for_all_functions(cluster, "deploy", "deployed")

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.fts_nodes:
                    self.drFTS.create_fts_indexes(cluster, dims=self.dim)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.fts_nodes:
                    status = self.drFTS.wait_for_fts_index_online(cluster,
                                                                  self.index_timeout)
                    self.assertTrue(status, "FTS index build failed.")
                    for bucket in cluster.buckets:
                        if bucket.loadDefn.get("ftsQPS", 0) > 0:
                            ql = FTSQueryLoad(cluster, bucket)
                            ql.start_query_load()
                            self.ftsQL.append(ql)

        self.mutation_perc = self.input.param("mutation_perc", 100)
        self.tasks = list()
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for bucket in cluster.buckets:
                    bucket.original_ops = bucket.loadDefn["ops"]
                    bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 5000)
                    self.generate_docs(bucket=bucket)
                self.tasks.append(self.perform_load(wait_for_load=False, cluster=cluster, buckets=cluster.buckets))
        self.sleep(10)

        upgrade = self.input.capella.get("upgrade_image")
        if upgrade:
            config = {
                "token": self.input.capella.get("override_key"),
                "image": self.input.capella.get("upgrade_image"),
                "server": self.input.capella.get("upgrade_server_version"),
                "releaseID": self.input.capella.get("upgrade_release_id")
                }
            rebalance_tasks = list()
            for tenant in self.tenants:
                for cluster in tenant.clusters:            
                    rebalance_task = self.task.async_upgrade_capella_prov(
                        self.pod, tenant, cluster, config, timeout=24*60*60)
                    rebalance_tasks.append(rebalance_task)
            self.wait_for_rebalances(rebalance_tasks)
        self.sleep(self.input.param("steady_state_workload_sleep", 120))

    def wait_for_rebalances(self, rebalance_tasks):
        rebl_ths = list()
        for task in rebalance_tasks:
            th = threading.Thread(target=self.monitor_rebalance,
                                  kwargs=dict({"tenant":task.tenant,
                                               "cluster":task.cluster,
                                               "rebalance_task":task}))
            th.start()
            rebl_ths.append(th)
        for th in rebl_ths:
            th.join()
        for task in rebalance_tasks:
            self.task_manager.get_task_result(task)
            self.assertTrue(task.result, "Cluster Upgrade Failed...")

    def find_master(self, tenant, cluster):
        i = 30
        task_details = None
        while True:
            try:
                self.refresh_cluster(tenant, cluster)
                rest = RestConnection(random.choice(cluster.nodes_in_cluster))
                break
            except ServerUnavailableException:
                self.refresh_cluster(tenant, cluster)
        while i > 0 and task_details is None:
            task_details = rest.ns_server_tasks("rebalance", "rebalance")
            self.sleep(10)
            i -= 1
        if task_details and task_details["status"] == "running":
            cluster.master.ip = task_details["masterNode"].split("@")[1]
            cluster.master.hostname = cluster.master.ip
            self.log.info("NEW MASTER: {}".format(cluster.master.ip))

    def monitor_rebalance(self, tenant, cluster, rebalance_task):
        self.find_master(tenant, cluster)
        self.rest = RestConnection(cluster.master)
        state = DedicatedUtils.get_cluster_state(
                    self.pod, tenant, cluster.id)
        while rebalance_task.state not in ["healthy",
                                                "upgradeFailed",
                                                "deploymentFailed",
                                                "redeploymentFailed",
                                                "rebalanceFailed",
                                                "scaleFailed"] and \
            state != "healthy":
            try:
                result = self.rest.newMonitorRebalance(sleep_step=60,
                                                       progress_count=1000)
                if result is False:
                    progress = self.rest.new_rebalance_status_and_progress()[1]
                    if progress == -100:
                        raise ServerUnavailableException
                self.assertTrue(result,
                                msg="Cluster rebalance failed")
                self.sleep(60, "To give CP a chance to update the cluster status to healthy.")
                state = DedicatedUtils.get_cluster_state(
                    self.pod, tenant, cluster.id)
                if state != "healthy":
                    self.refresh_cluster(tenant, cluster)
                    self.log.info("Rebalance task status: {}".format(rebalance_task.state))
                    self.sleep(60, "Wait for CP to trigger sub-rebalance")
            except ServerUnavailableException:
                self.log.critical("Node to get rebalance progress is not part of cluster")
                self.find_master(tenant, cluster)
                self.rest = RestConnection(cluster.master)
        state = DedicatedUtils.get_cluster_state(
                    self.pod, tenant, cluster.id)
        self.assertTrue(state == "healthy",
                        msg="Cluster rebalance failed")
        self.cluster_util.print_cluster_stats(cluster)

    def test_cluster_on_off(self):
        if self.turn_cluster_off:
            for tenant in self.tenants:
                for cluster in tenant.clusters:  
                    drClusterOnOff = DoctorHostedOnOff(self.pod, tenant, cluster)
                    cluster_off_result = drClusterOnOff.turn_off_cluster()
                    self.assertTrue(cluster_off_result, "Failed to turn off cluster")
                    self.sleep(200, "Wait before turning cluster on")
                    cluster_on_result = drClusterOnOff.turn_on_cluster()
                    self.assertTrue(cluster_on_result, "Failed to turn on cluster")
                    self.sleep(60, "Wait after cluster is turned on")

    def restore_from_backup(self):
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for bucket in cluster.buckets:
                    list_backups = self.drBackupRestore.list_all_backups(tenant, cluster, bucket).json()
                    backups_on_bucket = list_backups['backups']['data']
                    if not backups_on_bucket:
                        self.fail("No backups have been taken on bucket {}".format(bucket.name))
                    else:
                        for count, item in enumerate(backups_on_bucket):
                            self.log.debug("========= Backup number {} ==========".format(count))
                            self.log.debug("Backup debug info:{}".format(item['data']))
                    CapellaAPI.flush_bucket(self.pod, cluster, bucket.name)
                    time.sleep(120)
                    self.drBackupRestore.restore_from_backup(tenant, cluster, bucket, timeout=self.index_timeout)
                    time.sleep(60)
                    rest = RestConnection(cluster.master)
                    bucket_info = rest.get_bucket_details(bucket_name=bucket.name)
                    item_count = cluster.buckets[0].loadDefn.get("num_items") * bucket.loadDefn.get(
                        "collections")
                    if bucket_info['basicStats']['itemCount'] == item_count:
                        self.log.info("Post restore item count on the bucket is {}".format(item_count))

    def test_upgrades(self):
        self.initial_setup()
        self.stop_run = True
        for ql in self.ql:
            ql.stop_query_load()
        for ql in self.ftsQL:
            ql.stop_query_load()
        for ql in self.cbasQL:
            ql.stop_query_load()
        self.sleep(10, "Wait for 10s until all the query workload stops.")

        for cluster_tasks in self.tasks:
            for task in cluster_tasks:
                task.stop_work_load()
        for tenant in self.tenants:
            i = 0
            for cluster in tenant.clusters:
                self.wait_for_doc_load_completion(cluster, self.tasks[i])

        tasks = list()
        if self.track_failures:
            self.key_type = "RandomKey"
            for tenant in self.tenants:
                i = 0
                for cluster in tenant.clusters:
                    tasks.extend(self.data_validation(cluster))
        self.doc_loading_tm.getAllTaskResult()
        for task in tasks:
            self.assertTrue(task.result, "Validation Failed for: %s" % task.taskName)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.eventing_nodes:
                    self.drEventing.print_eventing_stats(cluster)
        self.assertTrue(self.query_result, "Please check the logs for query failures")

    def test_rebalance(self):
        self.initial_setup()
        h_scaling = self.input.param("h_scaling", True)
        v_scaling = self.input.param("v_scaling", False)
        vh_scaling = self.input.param("vh_scaling", False)
        computeList = GCP.compute
        provider = self.input.param("provider", "aws").lower()
        if provider == "aws":
            computeList = AWS.compute

        self.services = self.input.param("services", "data").split("-")
        self.rebl_services = self.input.param("rebl_services",
                                              self.input.param("services", "data")
                                              ).split("-")
        if h_scaling or vh_scaling:
            self.loop = 0
            self.rebl_nodes = self.input.param("horizontal_scale", 3)
            self.max_rebl_nodes = self.input.param("max_rebl_nodes", 27)
            while self.loop < self.iterations:
                ###################################################################
                self.PrintStep("Step 4.{}: Scale UP with Loading of docs".
                               format(self.loop))
                for service in self.rebl_services:
                    rebalance_tasks = list()
                    config = self.rebalance_config(service, self.rebl_nodes)
                    for tenant in self.tenants:
                        for cluster in tenant.clusters:  
                            rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                               config,
                                                                               timeout=self.index_timeout)
                            rebalance_tasks.append(rebalance_task)
                    self.wait_for_rebalances(rebalance_tasks)
                    self.sleep(60, "Sleep for 60s after rebalance")

                # turn cluster off and back on
                self.test_cluster_on_off()
                for tenant in self.tenants:
                    for cluster in tenant.clusters:  
                        self.restart_query_load(cluster, num=10)
                self.loop += 1

            self.loop = 0
            self.rebl_nodes = -self.rebl_nodes
            while self.loop < self.iterations:
                for tenant in self.tenants:
                    for cluster in tenant.clusters:  
                        self.restart_query_load(cluster, num=-10)
                self.PrintStep("Step 5.{}: Scale DOWN with Loading of docs".
                               format(self.loop))
                for service in self.rebl_services:
                    rebalance_tasks = list()
                    config = self.rebalance_config(service, self.rebl_nodes)
                    for tenant in self.tenants:
                        for cluster in tenant.clusters:  
                            rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                               config,
                                                                               timeout=self.index_timeout)
                            rebalance_tasks.append(rebalance_task)
                    self.wait_for_rebalances(rebalance_tasks)
                    self.sleep(60, "Sleep for 60s after rebalance")
                # turn cluster off and back on
                self.test_cluster_on_off()
                self.loop += 1

        if v_scaling or vh_scaling:
            self.loop = 0
            disk_increment = self.input.param("increment", 10)
            compute_change = 1
            while self.loop < self.iterations:
                self.PrintStep("Step 6.{}: Scale Disk with Loading of docs".
                               format(self.loop))
                self.loop += 1
                time.sleep(5*60)
                if self.rebalance_type == "all" or self.rebalance_type == "disk":
                    # Rebalance 1 - Disk Upgrade
                    for service_group in self.rebl_services:
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if not(len(service_group) == 1 and service in ["query"]):
                            self.disk[service] = self.disk[service] + disk_increment
                        config = self.rebalance_config(service)
                        rebalance_tasks = list()
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:
                                rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                                   config,
                                                                                   timeout=self.index_timeout)
                                rebalance_tasks.append(rebalance_task)
                        self.wait_for_rebalances(rebalance_tasks)
                        self.sleep(60, "Sleep for 60s after rebalance")
                    if self.backup_restore:
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:
                                for bucket in cluster.buckets:
                                    self.drBackupRestore.backup_now(tenant, cluster, bucket, wait_for_backup=False)
                    disk_increment = disk_increment * -1
                    #turn cluster off and back on
                    self.test_cluster_on_off()

            self.loop = 0
            while self.loop < self.iterations:
                self.PrintStep("Step 7.{}: Scale Compute with Loading of docs".
                               format(self.loop))
                self.loop += 1
                if self.rebalance_type == "all" or self.rebalance_type == "compute":
                    # Rebalance 2 - Compute Upgrade
                    for tenant in self.tenants:
                        for cluster in tenant.clusters:  
                            self.restart_query_load(cluster, num=10*compute_change)
                    for service_group in self.rebl_services:
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        comp = computeList.index(self.compute[service])
                        comp = comp + compute_change if len(computeList) > comp + compute_change else comp
                        self.compute[service] = computeList[comp]
                        config = self.rebalance_config()
                        rebalance_tasks = list()
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:  
                                rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                                   config,
                                                                                   timeout=self.index_timeout)
                                rebalance_tasks.append(rebalance_task)
                        self.wait_for_rebalances(rebalance_tasks)
                        self.sleep(60, "Sleep for 60s after rebalance")
                    if self.backup_restore:
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:
                                for bucket in cluster.buckets:
                                    self.drBackupRestore.backup_now(tenant, cluster, bucket, wait_for_backup=False)
                    compute_change = compute_change * -1
                    #turn cluster off and back on
                    self.test_cluster_on_off()

            self.loop = 0
            while self.loop < self.iterations:
                self.PrintStep("Step 8.{}: Scale Disk + Compute with Loading of docs".
                               format(self.loop))
                self.loop += 1
                if self.rebalance_type == "all" or self.rebalance_type == "disk_compute":
                    # Rebalance 3 - Both Disk/Compute Upgrade
                    for tenant in self.tenants:
                        for cluster in tenant.clusters:  
                            self.restart_query_load(cluster, num=10*compute_change)
                    for service_group in self.rebl_services:
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if not(len(service_group) == 1 and service in ["query"]):
                            self.disk[service] = self.disk[service] + disk_increment
                        comp = computeList.index(self.compute[service])
                        comp = comp + compute_change if len(computeList) > comp + compute_change else comp
                        self.compute[service] = computeList[comp]
                        config = self.rebalance_config()
                        config = self.rebalance_config(service)
                        rebalance_tasks = list()
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:  
                                rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                                   config,
                                                                                   timeout=self.index_timeout)
                                rebalance_tasks.append(rebalance_task)
                        self.wait_for_rebalances(rebalance_tasks)
                    if self.backup_restore:
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:
                                for bucket in cluster.buckets:
                                    self.drBackupRestore.backup_now(tenant, cluster, bucket, wait_for_backup=False)
                    self.sleep(60, "Sleep for 60s after rebalance")
                    disk_increment = disk_increment * -1
                    compute_change = compute_change * -1
                    self.cluster_util.print_cluster_stats(cluster)
                    #turn cluster off and back on
                    self.test_cluster_on_off()

            self.PrintStep("Step 4: XDCR replication being set up")
            if self.xdcr_remote_clusters > 0:
                for tenant in self.tenants:
                    i = 0
                    for cluster in tenant.clusters:  
                        num_items = cluster.buckets[0].loadDefn.get("num_items") * cluster.buckets[0].loadDefn.get(
                            "collections")
                        replication_done = self.drXDCR.is_replication_complete(
                            cluster=tenant.xdcr_clusters[i],
                            bucket_name=tenant.xdcr_clusters[i].buckets[0].name,
                            item_count=num_items)
                        if not replication_done:
                            self.log.error("Replication did not complete. Check logs!")
            if self.backup_restore:
                self.restore_from_backup()

        self.stop_crash = True
        self.stop_run = True
        for ql in self.ql:
            ql.stop_query_load()
        for ql in self.ftsQL:
            ql.stop_query_load()
        for ql in self.cbasQL:
            ql.stop_query_load()
        self.sleep(10, "Wait for 10s until all the query workload stops.")

        for cluster_tasks in self.tasks:
            for task in cluster_tasks:
                task.stop_work_load()
        for tenant in self.tenants:
            i = 0
            for cluster in tenant.clusters:
                self.wait_for_doc_load_completion(cluster, self.tasks[i])

        tasks = list()
        if self.track_failures:
            self.key_type = "RandomKey"
            for tenant in self.tenants:
                i = 0
                for cluster in tenant.clusters:
                    tasks.extend(self.data_validation(cluster))
        self.doc_loading_tm.getAllTaskResult()
        for task in tasks:
            self.assertTrue(task.result, "Validation Failed for: %s" % task.taskName)
            
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.eventing_nodes:
                    self.drEventing.print_eventing_stats(cluster)
        self.assertTrue(self.query_result, "Please check the logs for query failures")
