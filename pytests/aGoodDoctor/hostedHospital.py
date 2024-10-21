'''
Created on May 2, 2022

@author: ritesh.agarwal
'''

from membase.api.rest_client import RestConnection
import threading
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from pytests.basetestcase import BaseTestCase
from constants.cb_constants.CBServer import CbServer
from workloads import default, nimbus, vector_load
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
from constants.cloud_constants.capella_constants import AWS, GCP, AZURE
import time
from bucket_utils.bucket_ready_functions import CollectionUtils
from com.couchbase.test.sdk import Server, SDKClientPool
import pprint
from elasticsearch import EsClient
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
        hostedOPD.__init__(self)
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
        cluster.srv = self.input.param("private_link", cluster.srv)
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
                    "type": self.storage_type,
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

    def initial_setup(self):
        self.monitor_query_status()
        for tenant in self.tenants:
            for cluster in tenant.clusters:  
                cpu_monitor = threading.Thread(target=self.print_cluster_cpu_ram,
                                               kwargs={"cluster": cluster})
                cpu_monitor.start()

        _nimbus = self.input.param("nimbus", False)
        expiry = self.input.param("expiry", False)
        self.load_defn.append(default)
        if _nimbus:
            self.load_defn = list()
            self.load_defn.append(nimbus)

        if expiry:
            for load in self.load_defn:
                load["pattern"] = [0, 80, 0, 0, 20]
                load["load_type"] = ["read", "expiry"]

        if self.vector:
            self.load_defn = list()
            self.load_defn.append(vector_load)

        #######################################################################
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if not self.skip_init:
                    self.create_buckets(self.pod, tenant, cluster)
                else:
                    for i, bucket in enumerate(cluster.buckets):
                        bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                        cluster.sdk_client_pool = SDKClientPool()
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
                    self.drFTS.create_fts_indexes(cluster, dims=self.dim,
                                                  _type=self.fts_index_type)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.fts_nodes:
                    status = self.drFTS.wait_for_fts_index_online(cluster,
                                                                  self.index_timeout)
                    self.assertTrue(status, "FTS index build failed.")
                    self.sleep(300, "Wait for memory to be released after FTS index build.")
                    for bucket in cluster.buckets:
                        if bucket.loadDefn.get("ftsQPS", 0) > 0:
                            ql = FTSQueryLoad(cluster, bucket, mockVector=self.mockVector,
                                              dim=self.dim, base64=self.base64)
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

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster.fts_nodes:
                    self.sleep(3600, "Check fts vector query status during %s KV load" % self.input.param("rebl_ops_rate", 5000))

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
        elif provider == "azure":
            computeList = AZURE.compute

        self.services = self.input.param("services", "data").split("-")
        self.rebl_services = self.input.param("rebl_services",
                                              self.input.param("services", "data")
                                              ).split("-")
        query_upscale = 10
        if self.vector:
            query_upscale = 1
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
                        self.restart_query_load(cluster, num=query_upscale)
                self.loop += 1

            self.loop = 0
            self.rebl_nodes = -self.rebl_nodes
            while self.loop < self.iterations:
                for tenant in self.tenants:
                    for cluster in tenant.clusters:  
                        self.restart_query_load(cluster, num=-query_upscale)
                self.PrintStep("Step 5.{}: Scale DOWN with Loading of docs".
                               format(self.loop))
                self.sleep(600)
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
            disk_change = 1
            while self.loop < self.iterations:
                self.PrintStep("Step 6.{}: Scale Disk with Loading of docs".
                               format(self.loop))
                self.loop += 1
                if self.rebalance_type == "all" or self.rebalance_type == "disk":
                    # Rebalance 1 - Disk Upgrade
                    for service_group in self.rebl_services:
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if not(len(service_group) == 1 and service in ["query"]):
                            if provider == "azure":
                                index = AZURE.StorageType.order.index(self.storage_type)
                                self.storage_type = AZURE.StorageType.order[index+disk_change]
                                self.disk[service] = AZURE.StorageType.type[self.storage_type]["min"]
                                self.iops[service] = AZURE.StorageType.type[self.storage_type]["iops"]["min"]
                            else:
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
                    disk_change = disk_change * -1
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
                            self.restart_query_load(cluster, num=query_upscale*compute_change)
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
                            self.restart_query_load(cluster, num=query_upscale*compute_change)
                    for service_group in self.rebl_services:
                        service_group = sorted(service_group.split(":"))
                        service = service_group[0]
                        if not(len(service_group) == 1 and service in ["query"]):
                            if provider == "azure":
                                index = AZURE.StorageType.order.index(self.storage_type)
                                self.storage_type = AZURE.StorageType.order[index+disk_change]
                                self.disk[service] = AZURE.StorageType.type[self.storage_type]["min"]
                                self.iops[service] = AZURE.StorageType.type[self.storage_type]["iops"]["min"]
                            else:
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
                    disk_change = disk_change * -1
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
