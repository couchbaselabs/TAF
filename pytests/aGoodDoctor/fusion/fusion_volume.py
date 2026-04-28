'''
Created on May 2, 2022

@author: ritesh.agarwal
'''

import os
import socket
import threading
import time
from datetime import datetime

from membase.api.rest_client import RestConnection
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from .fusion_aws_util import FusionAWSUtil
from .fusion_monitor_util import FusionMonitorUtil
from .fusion_cp_resource_monitor import FusionCPResourceMonitor
from .awslib.cloudtrail_delete_setup import CloudTrailSetup
from pytests.basetestcase import BaseTestCase
from py_constants.cb_constants.CBServer import CbServer
from aGoodDoctor.hostedXDCR import DoctorXDCR
from aGoodDoctor.hostedBackupRestore import DoctorHostedBackupRestore
from aGoodDoctor.hostedOnOff import DoctorHostedOnOff
from constants.cloud_constants.capella_constants import AWS, GCP, AZURE
from bucket_utils.bucket_ready_functions import CollectionUtils, JavaDocLoaderUtils
from aGoodDoctor.hostedOPD import hostedOPD
from aGoodDoctor.workloads import default, Hotel, siftBigANN, nimbus
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader

class VolumeTest(BaseTestCase, hostedOPD):

    DEFAULT_TIMEOUT = FusionMonitorUtil.DEFAULT_TIMEOUT

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

        self.drBackupRestore = DoctorHostedBackupRestore(pod=self.pod)
        if self.xdcr_remote_clusters > 0:
            self.drXDCR = DoctorXDCR(pod=self.pod)

        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.loader_tasks = list()

        JavaDocLoaderUtils(self.bucket_util, self.cluster_util)
        self.aws_access_key = self.input.param("aws_access_key", None)
        self.aws_secret_key = self.input.param("aws_secret_key", None)
        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("AWS credentials (aws_access_key, aws_secret_key) are required parameters")
        self.aws_region = self.input.param("region", "us-east-1")
        self.fusion_aws_util = FusionAWSUtil(self.aws_access_key, self.aws_secret_key, region=self.aws_region)
        self.fusion_monitor = FusionMonitorUtil(self.log, self.fusion_aws_util)
        self.cp_monitor = FusionCPResourceMonitor(self.log, self.fusion_aws_util)
        self.steady_state_workload_sleep = self.input.param("steady_state_workload_sleep", 1800)
        self.cloudtrail = None
        self.cloudtrail_targets = []
        self.fusion_rebalances = list()
        self.stop_run_event = threading.Event()

    def tearDown(self):
        if hasattr(self, "stop_run_event"):
            self.stop_run_event.set()
        if hasattr(self, "_log_dcp_items_stop_event"):
            self._log_dcp_items_stop_event.set()
        if hasattr(self, "_log_store_stop_event"):
            self._log_store_stop_event.set()
        self.stop_run = True
        
        # Get CloudTrail instance if available
        cloudtrail = getattr(self, "cloudtrail", None)
        
        # Wait for CPU monitor threads
        cpu_monitor_threads = getattr(self, "cpu_monitor_threads", [])
        for thread in cpu_monitor_threads:
            try:
                thread.join()
            except Exception:
                pass  # Thread may already be stopped
        
        # Stop loader tasks
        loader_tasks = getattr(self, "loader_tasks", [])
        for task in loader_tasks:
            try:
                self.task_manager.stop_task(task)
            except Exception:
                pass  # Task may already be stopped

        # Stop all loader tasks and clean up cloudtrail
        if cloudtrail:
            for tenant in self.tenants:
                for cluster in tenant.clusters:
                    for bucket in cluster.buckets:                    
                        # Teardown cloudtrail if configured
                        try:
                            cloudtrail.teardown_cloudtrail_delete_logging(
                                trail_name=f"fusion_log_store_deletion_logs_{cluster.id}_{bucket.name}",
                                target_bucket="fusion-accesslogs-ritesh-agarwal",
                                log_prefix=f"s3-object-deletes/{cluster.id}/{bucket.name}"
                            )
                        except Exception as e:
                            self.log.warning(
                                f"Skipping CloudTrail teardown for cluster {cluster.id}, bucket {bucket.name}: {e}"
                            )

        BaseTestCase.tearDown(self)

    def get_hostname_public_ip_mapping(self, cluster, suppress_log=False):
        """Get hostname to public IP mapping for cluster nodes using fusion monitor."""
        self.fusion_monitor.get_hostname_public_ip_mapping(cluster, suppress_log)

    def check_asg_cleanup_after_rebalance(self):
        """Check if ASG cleanup is running for all clusters."""
        clusters = [cluster for tenant in self.tenants for cluster in tenant.clusters]
        self.cp_monitor.check_asg_cleanup_after_rebalance(clusters)

    def monitor_cluster_status(self, tenant, cluster, rebalance_task):
        """Monitor fusion cluster status during rebalance."""
        import threading
        self.log.info(f"Monitoring cluster status for cluster {cluster.id}")
        rebalance_start_time = datetime.now()
        self.log.info(f"Rebalance start time (scaling): {rebalance_start_time.strftime('%Y-%m-%d %H:%M:%S')} for cluster {cluster.id}")
        threads = []
        result = {}
        
        # Wait for rebalance task completion
        thread = threading.Thread(
            target=lambda res, task: res.update({"monitor_rebalance_complete": self.wait_for_rebalances([task])}),
            args=(result, rebalance_task))
        thread.start()
        threads.append(thread)

        # Monitor accelerator instances
        accelerator_thread = threading.Thread(
            target=lambda res, clus: res.update({"monitor_cluster_accelerator_intances_complete": self.cp_monitor.monitor_cluster_accelerator_instances(clus, rebalance_task, self.fusion_rebalances)}),
            args=(result, cluster))
        accelerator_thread.start()
        accelerator_thread.join()
        self.assertTrue(result.get("monitor_cluster_accelerator_intances_complete", False), 
                       f"monitor_cluster_accelerator_intances failed for cluster {cluster.id}")
        
        self.wait_for_hydration_complete = self.input.param("wait_for_hydration_complete", True)
        self.hydration_time = self.input.param("hydration_time", 6400)

        # Monitor fusion guest volumes
        thread = threading.Thread(
            target=lambda res, clus: res.update(
                {"monitor_fusion_guest_volumes_complete": self.cp_monitor.monitor_fusion_guest_volumes(
                    tenant, clus, rebalance_task, self.fusion_monitor, self.fusion_rebalances,
                    self.wait_for_hydration_complete, self.hydration_time, self.find_master)}),
            args=(result, cluster))
        thread.start()
        threads.append(thread)

        for thread in threads:
            thread.join()
        self.assertTrue(result.get("monitor_fusion_guest_volumes_complete", False),
                       f"monitor_fusion_guest_volumes failed for cluster {cluster.id}")

        rebalance_end_time = datetime.now()
        elapsed_s = (rebalance_end_time - rebalance_start_time).total_seconds()
        elapsed_str = f"{int(elapsed_s // 60)}m {elapsed_s % 60:.1f}s" if elapsed_s >= 60 else f"{elapsed_s:.1f}s"
        self.log.info(
            f"Fusion rebalance completed for cluster {cluster.id}: "
            f"start={rebalance_start_time.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"end={rebalance_end_time.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"total={elapsed_str}"
        )

    def check_ebs_cleanup_for_cluster(self, cluster):
        """Check EBS cleanup for a specific cluster."""
        return self.cp_monitor.monitor_ebs_cleanup(cluster, self.stop_run_event)

    def scan_errors_for_clusters(self, clusters):
        """Scan memcached logs for errors on all clusters."""
        errors_found = self.cp_monitor.scan_memcached_logs_for_errors(clusters, self.steady_state_workload_sleep)
        self.assertFalse(errors_found, "Errors found in memcached logs on cluster instances")

    def parse_accelerator_logs_for_clusters(self, clusters):
        """Parse accelerator logs for all clusters."""
        self.cp_monitor.parse_accelerator_logs(clusters, self.fusion_rebalances, 
                                              self.aws_access_key, self.aws_secret_key, 
                                              self.aws_region)

    def initial_setup(self):
        # Enable fusion using Capella API
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                CapellaAPI.update_feature_flag_globally(self.pod, tenant, "fusion-rebalances", True)
                CapellaAPI.update_feature_flag_globally(self.pod, tenant, "fusion-fallback-replace", True)
                fusion_state = CapellaAPI.get_fusion_status(self.pod, tenant, cluster.id)
                self.log.info(f"Fusion state for cluster {cluster.id}: {fusion_state}")
                if fusion_state.get('state') == "enabled":
                    self.log.info(f"Fusion is already enabled for cluster {cluster.id}")
                    continue
                resp = CapellaAPI.enable_fusion(self.pod, tenant, cluster.id)
                self.log.info(f"Enable Fusion response for cluster {cluster.id}: {resp.status_code}")
                self.assertTrue(resp.status_code == 200, f"Failed to enable Fusion on cluster {cluster.id}: {resp.status_code}") 
                self.fusion_monitor.wait_for_fusion_status(cluster, state="enabled")
                self.get_hostname_public_ip_mapping(cluster)

        self.cpu_monitor_threads = list()
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                cpu_monitor = threading.Thread(target=self.print_cluster_cpu_ram,
                                               kwargs={"cluster": cluster})
                cpu_monitor.start()

        if self.val_type == "Hotel":
            self.load_defn.append(Hotel)
        elif self.val_type == "siftBigANN":
            self.load_defn.append(siftBigANN)
        elif self.val_type == "Nimbus":
            self.load_defn.append(nimbus)
        else:
            self.load_defn.append(default)

        #######################################################################
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if not self.skip_init:
                    self.create_buckets(self.pod, tenant, cluster)
                    self.sleep(60, "wait for s3 uri to be created")
                else:
                    for i, bucket in enumerate(cluster.buckets):
                        bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                        num_clients = self.input.param("clients_per_db",
                                                       min(5, bucket.loadDefn.get("collections")))
                        SiriusCouchbaseLoader.create_clients_in_pool(
                            cluster.master, cluster.master.rest_username,
                            cluster.master.rest_password,
                            bucket.name, req_clients=num_clients)
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

        try:
            self.cloudtrail = CloudTrailSetup(self.aws_access_key, self.aws_secret_key,
                                              region=self.aws_region)
        except Exception as e:
            self.cloudtrail = None
            self.log.warning(f"CloudTrail client initialization failed; continuing without CloudTrail checks: {e}")

        if self.cloudtrail:
            for tenant in self.tenants:
                for cluster in tenant.clusters:
                    for bucket in cluster.buckets:
                        try:
                            uri = self.fusion_monitor.get_fusion_s3_uri(cluster, bucket.name)
                            if uri:
                                uri = uri.split("?")[0] if uri else None
                            if not uri or len(uri.split("/")) < 3:
                                self.log.warning(
                                    f"Skipping CloudTrail setup for cluster {cluster.id}, bucket {bucket.name}: invalid fusion S3 URI"
                                )
                                continue

                            trail_name = f"fusion_log_store_deletion_logs_{cluster.id}_{bucket.name}"
                            log_prefix = f"s3-object-deletes/{cluster.id}/{bucket.name}"
                            self.cloudtrail.setup_cloudtrail_delete_obj_s3_logging(
                                source_bucket=uri.split("/")[2],
                                target_bucket="fusion-accesslogs-ritesh-agarwal",
                                trail_name=trail_name,
                                log_prefix=log_prefix
                            )
                            self.cloudtrail_targets.append((trail_name, log_prefix))
                        except Exception as e:
                            self.log.warning(
                                f"CloudTrail setup failed for cluster {cluster.id}, bucket {bucket.name}; continuing test: {e}"
                            )

        # Initialize fusion uploader maps on clusters
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                cluster.fusion_uploader_dict = dict()
                cluster.fusion_vb_uploader_map = dict()
                for bucket in cluster.buckets:
                    cluster.fusion_uploader_dict[bucket.name] = dict()
                    cluster.fusion_vb_uploader_map[bucket.name] = dict()
        
        # Get fusion uploader maps for all clusters
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.fusion_monitor.get_fusion_uploader_map(tenant, cluster, self.find_master)
        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''
        tasks = list()
        self.PrintStep("Step 2: Create %s items: %s" % (self.num_items, self.key_type))
        i = 0
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if not self.skip_init:
                    tasks = JavaDocLoaderUtils.load_data(cluster=cluster,
                                        buckets=cluster.buckets,
                                        overRidePattern={"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0},
                                        validate_data=False,
                                        wait_for_stats=False)
                    if not tasks:
                        self.log.critical(f"Failed to create doc load task for cluster {cluster.id}")
                        raise Exception(f"Failed to create doc load task for cluster {cluster.id}")
                    if self.xdcr_remote_clusters > 0:
                        self.drXDCR.set_up_replication(tenant, source_cluster=cluster, destination_cluster=tenant.xdcr_clusters[i],
                                        source_bucket=cluster.buckets[0].name,
                                        destination_bucket=tenant.xdcr_clusters[i].buckets[0].name,)

        i = 0
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                if cluster in tenant.xdcr_clusters:
                    self.drXDCR.set_up_replication(tenant, source_cluster=cluster, destination_cluster=tenant.xdcr_clusters[i],
                                                   source_bucket=cluster.buckets[0].name,
                                                   destination_bucket=tenant.xdcr_clusters[i].buckets[0].name,)
                    i += 1

        self.mutation_perc = self.input.param("mutation_perc", 100)
        self.tasks = list()
        # for tenant in self.tenants:
        #     for cluster in tenant.clusters:
        #         self.data_validation(cluster)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.mutations = self.input.param("mutations", True)
                self.mutation_th = threading.Thread(target=self.normal_mutations,
                                                    kwargs={"cluster": cluster})
                self.mutation_th.start()

        self.sleep(self.steady_state_workload_sleep, "Sleep for {self.steady_state_workload_sleep} seconds to allow the mutations to complete")
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
            self.wait_for_rebalances(rebalance_tasks, rebl_poll_interval=10)

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
                    CapellaAPI.flush_bucket(self.pod, tenant, cluster, bucket.name)
                    time.sleep(120)
                    self.drBackupRestore.restore_from_backup(tenant, cluster, bucket, timeout=self.index_timeout)
                    time.sleep(60)
                    rest = RestConnection(cluster.master)
                    bucket_info = rest.get_bucket_details(bucket_name=bucket.name)
                    first_bucket = cluster.buckets[0] if cluster.buckets else None
                    item_count = (first_bucket.loadDefn.get("num_items") if first_bucket else 0) * bucket.loadDefn.get("collections")
                    if bucket_info and bucket_info['basicStats']['itemCount'] == item_count:
                        self.log.info("Post restore item count on the bucket is {}".format(item_count))

    def log_rebalance_report(self):
        """Fetch and log NS Server rebalance report for all clusters."""
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.fusion_monitor.log_rebalance_report(cluster)

    def parse_accelerator_logs(self):
        """Parse accelerator logs for all clusters using control plane monitor."""
        clusters = [cluster for tenant in self.tenants for cluster in tenant.clusters]
        self.parse_accelerator_logs_for_clusters(clusters)

    def scan_memcahced_logs_for_errors(self):
        """Scan memcached logs for errors on all clusters using control plane monitor."""
        clusters = [cluster for tenant in self.tenants for cluster in tenant.clusters]
        self.scan_errors_for_clusters(clusters)

    def log_fusion_log_store_data_size(self):
        """Log fusion log store data size for all clusters and buckets using fusion monitor."""
        clusters = [cluster for tenant in self.tenants for cluster in tenant.clusters]
        self.fusion_monitor.log_fusion_log_store_data_size(clusters)

    def log_fusion_pending_bytes(self):
        """Log fusion pending bytes for all clusters using fusion monitor."""
        for tenant in self.tenants:
            self.fusion_monitor.log_fusion_pending_bytes(tenant, tenant.clusters, self.find_master)

    def log_fusion_dcp_items_remaining(self):
        """Log DCP items remaining for all clusters using fusion monitor."""
        self._log_dcp_items_stop_event = threading.Event()
        deadline = time.time() + 7200  # 2-hour absolute cap; guards against missed stop signal
        while not self._log_dcp_items_stop_event.is_set():
            if time.time() > deadline:
                self.log.warning("log_fusion_dcp_items_remaining: absolute timeout reached, stopping loop")
                break
            clusters = [cluster for tenant in self.tenants for cluster in tenant.clusters]
            self.fusion_monitor.log_fusion_dcp_items_remaining(clusters)
            self._log_dcp_items_stop_event.wait(300)  # Wait for 5 minutes before next

    def test_volume_scaling(self):
        # Start a background cbstats monitor until initial_setup completes
        self._log_store_stop_event = threading.Event()

        def _log_store_monitor():
            while not self._log_store_stop_event.is_set():
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                # Avoid tight loop
                self._log_store_stop_event.wait(300)  # Wait for 5 minutes before next cbstats run

        self._log_store_thread = threading.Thread(
            target=_log_store_monitor, name="log-store-monitor", daemon=True
        )
        self._log_store_thread.start()

        self.dcp_check_thread = threading.Thread(
            target=self.log_fusion_dcp_items_remaining,
            name="dcp-check-thread",
            daemon=True
        )
        self.dcp_check_thread.start()

        self.initial_setup()
        self._log_store_stop_event.set()

        self.compute["data"] = self.input.param("fusion_compute", "m5.4xlarge")
        self.fusion_rebalances = list()

        h_scaling = self.input.param("h_scaling", True)
        v_scaling = self.input.param("v_scaling", False)
        vh_scaling = self.input.param("vh_scaling", False)
        computeList = GCP.compute
        provider = self.input.param("provider", "aws").lower()
        if provider == "aws":
            computeList = AWS.compute
        elif provider == "azure":
            computeList = AZURE.compute
        
        ebs_cleanup_threads = list()
        ebs_available_threads = list()
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                ebs_cleanup_thread = threading.Thread(
                    target=self.cp_monitor.check_ebs_guest_vol_deletion,
                    kwargs={"tenant": tenant, "cluster": cluster, "fusion_monitor_util": self.fusion_monitor, "stop_run_event": self.stop_run_event, "find_master_func": self.find_master})
                ebs_cleanup_thread.start()
                ebs_cleanup_threads.append(ebs_cleanup_thread)

                ebs_available_thread = threading.Thread(
                    target=self.cp_monitor.monitor_available_volumes_by_fusion_rebalance,
                    kwargs={"cluster": cluster, "fusion_rebalances": self.fusion_rebalances, "stop_run_event": self.stop_run_event})
                ebs_available_thread.start()
                ebs_available_threads.append(ebs_available_thread)

        self.services = self.input.param("services", "data")
        self.rebl_services = self.input.param("rebl_services", self.services).split("-")
        if h_scaling or vh_scaling:
            self.loop = 0
            self.rebl_steps = [int(num) for num in self.input.param("rebl_steps", "3-5-7-8").split("-")]
            self.num_rebl_steps = self.input.param("num_rebl_steps", 4)
            self.max_rebl_nodes = self.input.param("max_rebl_nodes", 27)
            self.cycles = self.input.param("cycles", 1)
            while self.loop < self.cycles:
                self.loop += 1
                for rebl_step in range(self.iterations):
                    ###################################################################
                    self.log_fusion_log_store_data_size()
                    self.log_fusion_pending_bytes()
                    self.PrintStep("Step 4.{}: Scale UP with Loading of docs".
                                format(rebl_step))
                    for service in self.rebl_services:
                        rebalance_tasks = list()
                        config = self.rebalance_config(service, self.rebl_steps[rebl_step])
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:
                                rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                                config,
                                                                                timeout=self.index_timeout)
                                rebalance_tasks.append(rebalance_task)
                        # Start polling thread for each rebalance pass
                        for rebalance_task in rebalance_tasks:
                            self.monitor_cluster_status(rebalance_task.tenant, rebalance_task.cluster, rebalance_task)
                            self.fusion_monitor.get_fusion_uploader_map(rebalance_task.tenant, rebalance_task.cluster, self.find_master)
                        self.sleep(60, "Sleep for 60s after rebalance")
                        # Check for fusion accelerator node to be 0
                        for rebalance_task in rebalance_tasks:
                            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(rebalance_task.cluster)
                            self.assertTrue(result, "Fusion Accelerator nodes not killed after rebalance")
                        self.log_rebalance_report()
                        self.scan_memcahced_logs_for_errors()
                        self.parse_accelerator_logs()
                        self.check_asg_cleanup_after_rebalance()
                    # turn cluster off and back on
                    self.test_cluster_on_off()

                for rebl_step in range(self.iterations):
                    self.log_fusion_log_store_data_size()
                    self.log_fusion_pending_bytes()
                    self.PrintStep("Step 5.{}: Scale DOWN with Loading of docs".
                                format(rebl_step))
                    for service in self.rebl_services:
                        rebalance_tasks = list()
                        config = self.rebalance_config(service, -self.rebl_steps[rebl_step])
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:  
                                rebalance_task = self.task.async_rebalance_capella(self.pod, tenant, cluster,
                                                                                config,
                                                                                timeout=self.index_timeout)
                                rebalance_tasks.append(rebalance_task)
                        for rebalance_task in rebalance_tasks:
                            self.monitor_cluster_status(rebalance_task.tenant, rebalance_task.cluster, rebalance_task)
                            self.fusion_monitor.get_fusion_uploader_map(rebalance_task.tenant, rebalance_task.cluster, self.find_master)
                        self.sleep(60, "Sleep for 60s after rebalance")
                        # Check for fusion accelerator node to be 0
                        for rebalance_task in rebalance_tasks:
                            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(rebalance_task.cluster)
                            self.assertTrue(result, "Fusion Accelerator nodes not killed after rebalance")
                        self.log_rebalance_report()
                        self.scan_memcahced_logs_for_errors()
                        self.parse_accelerator_logs()
                        self.check_asg_cleanup_after_rebalance()
                # turn cluster off and back on
                self.test_cluster_on_off()

        if v_scaling or vh_scaling:
            self.loop = 0
            disk_increment = self.input.param("increment", 10)
            compute_change = 1
            disk_change = 1
            while self.loop < self.iterations:
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
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
                        for rebalance_task in rebalance_tasks:
                            self.monitor_cluster_status(rebalance_task.tenant, rebalance_task.cluster, rebalance_task)
                            self.fusion_monitor.get_fusion_uploader_map(rebalance_task.tenant, rebalance_task.cluster, self.find_master)
                        self.sleep(60, "Sleep for 60s after rebalance")
                        for rebalance_task in rebalance_tasks:
                            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(rebalance_task.cluster)
                            self.assertTrue(result, "Fusion Accelerator nodes not killed after rebalance")
                        self.log_rebalance_report()
                        self.scan_memcahced_logs_for_errors()
                        self.parse_accelerator_logs()
                        self.check_asg_cleanup_after_rebalance()
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
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self.PrintStep("Step 7.{}: Scale Compute with Loading of docs".
                               format(self.loop))
                self.loop += 1
                if self.rebalance_type == "all" or self.rebalance_type == "compute":
                    # Rebalance 2 - Compute Upgrade
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
                        for rebalance_task in rebalance_tasks:
                            self.monitor_cluster_status(rebalance_task.tenant, rebalance_task.cluster, rebalance_task)
                            self.fusion_monitor.get_fusion_uploader_map(rebalance_task.tenant, rebalance_task.cluster, self.find_master)
                        self.sleep(60, "Sleep for 60s after rebalance")
                        for rebalance_task in rebalance_tasks:
                            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(rebalance_task.cluster)
                            self.assertTrue(result, "Fusion Accelerator nodes not killed after rebalance")
                        self.log_rebalance_report()
                        self.scan_memcahced_logs_for_errors()
                        self.parse_accelerator_logs()
                        self.check_asg_cleanup_after_rebalance()
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
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self.PrintStep("Step 8.{}: Scale Disk + Compute with Loading of docs".
                               format(self.loop))
                self.loop += 1
                if self.rebalance_type == "all" or self.rebalance_type == "disk_compute":
                    # Rebalance 3 - Both Disk/Compute Upgrade
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
                        for rebalance_task in rebalance_tasks:
                            self.monitor_cluster_status(rebalance_task.tenant, rebalance_task.cluster, rebalance_task)
                            self.fusion_monitor.get_fusion_uploader_map(rebalance_task.tenant, rebalance_task.cluster, self.find_master)
                        self.sleep(60, "Sleep for 60s after rebalance")
                        for rebalance_task in rebalance_tasks:
                            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(rebalance_task.cluster)
                            self.assertTrue(result, "Fusion Accelerator nodes not killed after rebalance")
                        self.log_rebalance_report()
                        self.scan_memcahced_logs_for_errors()
                        self.parse_accelerator_logs()
                        self.check_asg_cleanup_after_rebalance()
                    if self.backup_restore:
                        for tenant in self.tenants:
                            for cluster in tenant.clusters:
                                for bucket in cluster.buckets:
                                    self.drBackupRestore.backup_now(tenant, cluster, bucket, wait_for_backup=False)
                    self.sleep(60, "Sleep for 60s after rebalance")
                    disk_increment = disk_increment * -1
                    compute_change = compute_change * -1
                    disk_change = disk_change * -1
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

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                result = self.check_ebs_cleanup_for_cluster(cluster)
                self.assertTrue(result, f"check_ebs_cleanup_for_cluster failed for cluster {cluster.id}")
                self.log.info(f"EBS cleanup completed successfully for cluster {cluster.id}")

    def normal_mutations(self, cluster):
        while self.mutations:
            self.mutate += 1
            for bucket in cluster.buckets:
                JavaDocLoaderUtils.generate_docs(bucket=bucket)
                bucket.original_ops = bucket.loadDefn["ops"]
                bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 5000)
            self.loader_tasks = JavaDocLoaderUtils.perform_load(cluster=cluster,
                                                                buckets=cluster.buckets,
                                                                overRidePattern={"update": 20,
                                                                                 "read": 80},
                                                                wait_for_load=False,
                                                                mutate=self.mutate,
                                                                suppress_error_table=self.suppress_error_table,
                                                                track_failures=self.track_failures)
            if not self.loader_tasks:
                self.log.critical(f"Failed to create doc load task for cluster {cluster.id}")
                raise Exception(f"Failed to create doc load task for cluster {cluster.id}")
            for task in self.loader_tasks:
                self.task_manager.get_task_result(task)
                self.loader_tasks.remove(task)

    def data_validation(self, cluster, skip_default=True):
        for bucket in cluster.buckets:
            JavaDocLoaderUtils.generate_docs(bucket=bucket, doc_ops=["read"])
            self.loader_tasks = JavaDocLoaderUtils.perform_load(cluster=cluster,
                                                                buckets=cluster.buckets,
                                                                wait_for_load=False,
                                                                mutate=self.mutate,
                                                                validate_data=True,
                                                                overRidePattern={"read": 100},
                                                                skip_default=skip_default,
                                                                suppress_error_table=False,
                                                                track_failures=True)
            if not self.loader_tasks:
                self.log.critical(f"Failed to create doc load task for cluster {cluster.id}")
                raise Exception(f"Failed to create doc load task for cluster {cluster.id}")
            for task in list(self.loader_tasks):
                self.task_manager.get_task_result(task)
                self.assertTrue(task.result, "Validation Failed for: %s" % task.taskName)
                self.loader_tasks.remove(task)