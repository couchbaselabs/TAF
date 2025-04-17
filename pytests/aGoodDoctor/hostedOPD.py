'''
Created on 30-Aug-2021

@author: riteshagarwal
'''
import time

from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.sdk import Server, SDKClientPool
from table_view import TableView
from opd import OPD
from membase.api.rest_client import RestConnection
import random
import string
from BucketLib.bucket import Bucket
from capella_utils.dedicated import CapellaUtils
import threading
from workloads import default, nimbus, vector_load
from bucket_utils.bucket_ready_functions import CollectionUtils
from constants.cb_constants.CBServer import CbServer
from custom_exceptions.exception import ServerUnavailableException


class hostedOPD(OPD):
    def __init__(self):
        self.siftFileName = None
        pass

    def threads_calculation(self):
        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        num_clusters = self.num_clusters or 1 
        self.doc_loading_tm = TaskManager(self.process_concurrency*self.num_tenants*num_clusters*self.num_buckets)

    def print_cluster_cpu_ram(self, cluster, step=300):
        while not self.stop_run:
            try:
                self.cluster_util.print_cluster_stats(cluster)
            except:
                pass
            time.sleep(step)

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
            CapellaUtils.create_bucket(pod, tenant, cluster, self.bucket_params)
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

    def setup_buckets(self):
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
                load["pattern"] = [0, 0, 0, 0, 100]
                load["load_type"] = ["expiry"]

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

    def initial_load(self):
        self.skip_read_on_error = True
        self.suppress_error_table = True

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

    def live_kv_workload(self):
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
        state = CapellaUtils.get_cluster_state(
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
                state = CapellaUtils.get_cluster_state(
                    self.pod, tenant, cluster.id)
                if state != "healthy":
                    self.refresh_cluster(tenant, cluster)
                    self.log.info("Rebalance task status: {}".format(rebalance_task.state))
                    self.sleep(60, "Wait for CP to trigger sub-rebalance")
            except ServerUnavailableException:
                self.log.critical("Node to get rebalance progress is not part of cluster")
                self.find_master(tenant, cluster)
                self.rest = RestConnection(cluster.master)
        state = CapellaUtils.get_cluster_state(
                    self.pod, tenant, cluster.id)
        self.assertTrue(state == "healthy",
                        msg="Cluster rebalance failed")
        self.cluster_util.print_cluster_stats(cluster)

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

    def cbcollect_logs(self, tenant, cluster_id, log_id=""):
        CapellaUtils.trigger_log_collection(self.pod, tenant, cluster_id,
                                            log_id=log_id)
        table = TableView(self.log.info)
        table.add_row(["URL"])
        task = CapellaUtils.check_logs_collect_status(self.pod, tenant, cluster_id)
        for _, logInfo in sorted(task["perNode"].items()):
            table.add_row([logInfo["url"]])
        table.display("Cluster: {}".format(cluster_id))
