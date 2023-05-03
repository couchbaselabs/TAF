from BucketLib.bucket import Bucket
from Cb_constants.CBServer import CbServer
from Jython_tasks.task import CloudHibernationTask
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from collections_helper.collections_spec_constants import MetaConstants
from pytests.bucket_collections.collections_base import CollectionBase
from serverless.tenant_mgmt_on_cloud import TenantMgmtOnCloud
from capella_utils.serverless import CapellaUtils as ServerlessUtils
from threading import Thread

import time
from common_lib import sleep
import random

from com.couchbase.test.sdk import Server
from com.couchbase.test.docgen import DocumentGenerator

class HibernationOnCloud(TenantMgmtOnCloud):
    def setUp(self):
        super(HibernationOnCloud, self).setUp()
        self.db_name = "TAF-HibernationOnCloud"
        self.num_databases = self.input.param("num_databases_per_tenant", 1)

    def wait_for_dataplane_to_rebalance(self):
        start = time.time()
        timeout_in_seconds = 1800
        result = False
        while (time.time() - start) <= timeout_in_seconds:
            dp_info = self.serverless_util.get_dataplane_info(self.dataplanes[0])
            curr_state = dp_info["couchbase"]["state"]
            if curr_state == "rebalancing":
                return True

        return result

    def load_data(self):
        loader_map = dict()
        self.init_sdk_pool_object()
        self.create_sdk_client_pool(buckets=self.cluster.buckets,
                                    req_clients_per_bucket=1)

        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    work_load_settings = DocLoaderUtils.get_workload_settings(
                        key=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        create_perc=100, create_start=0,
                        create_end=self.num_items,
                        ops_rate=self.ops_rate)
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_map.update({"%s:%s:%s" % (bucket.name, scope, collection): dg})

        DocLoaderUtils.perform_doc_loading(
            self.doc_loading_tm, loader_map,
            self.cluster, self.cluster.buckets,
            async_load=False, validate_results=False,
            sdk_client_pool=self.sdk_client_pool)
        result = DocLoaderUtils.data_validation(
            self.doc_loading_tm, loader_map, self.cluster,
            buckets=self.cluster.buckets,
            process_concurrency=self.process_concurrency,
            ops_rate=self.ops_rate, sdk_client_pool=self.sdk_client_pool)
        self.assertTrue(result, "Data validation failed")

    def test_basic_pause_resume(self):

        buckets_spec = self.get_bucket_spec(bucket_name_format="taf-hibernation-%s",
                                            num_buckets=self.num_buckets)
        self.create_required_buckets(buckets_spec=buckets_spec)

        self.load_data()

        database_to_pause = self.cluster.buckets[0].name
        pause_task = self.bucket_util.pause_database(self.cluster, database_to_pause)
        self.task_manager.get_task_result(pause_task)
        self.assertTrue(pause_task.result, "Pause failed for database")

        resume_task = self.bucket_util.resume_database(self.cluster, database_to_pause)
        self.task_manager.get_task_result(resume_task)
        self.assertTrue(resume_task.result, "Resume failed for database")

    def test_pause_resume_with_operations(self):
        buckets_spec = self.get_bucket_spec(bucket_name_format="taf-hibernation-%s",
                                            num_buckets=self.num_buckets)
        self.create_required_buckets(buckets_spec=buckets_spec)

        self.load_data()

        databases = self.serverless_util.get_all_serverless_databases()
        databases_batch1 = databases[:int(len(databases) / 2)]
        databases_batch2 = databases[int(len(databases) / 2):]

        #pause and delete db's while pause is in progress
        for pause_db in databases_batch1:
            self.log.info("Pausing db: {0}".format(pause_db["id"]))
            pause_task = self.bucket_util.pause_database(self.cluster, pause_db["id"])
            self.sleep(20, "Wait for pause task to start")
            resp = self.serverless_util.delete_database(self.pod, tenant=self.tenant,
                                                     database_id=pause_db["id"])

        for pause_db in databases_batch1:
            self.log.info("Waiting for db {0} to get deleted".format(pause_db["id"]))
            result = self.serverless_util.wait_for_database_deleted(self.tenant,
                                                                    pause_db["id"])
            if result == False:
                self.fail("Failed to delete db {0}".format(pause_db["id"]))

        #For batch2 wait for database to pause then delete
        for pause_db in databases_batch2:
            self.log.info("Pausing db: {0}".format(pause_db["id"]))
            pause_task = self.bucket_util.pause_database(self.cluster, pause_db["id"],
                                                         timeout_to_start=20)
            self.task_manger.get_task_result(pause_task)
            self.assertTrue(pause_task.result, "Pause failed for database")

        #Delete for databases will be called in teardown so not required to
        #repeat the same code here

    def test_pause_resume_cluster_full(self):
        original_dataplane = self.dataplanes[0]
        self.log.info("Original dataplane: {0}".format(original_dataplane))
        buckets_spec = self.get_bucket_spec(bucket_name_format="pausedb-%s",
                                            num_buckets=1)
        self.create_required_buckets(buckets_spec=buckets_spec)

        pause_db = self.cluster.buckets[0].name
        self.log.info("Pausing db: {0}".format(pause_db))
        pause_task = self.bucket_util.pause_database(self.cluster, pause_db)
        self.task_manager.get_task_result(pause_task)
        self.assertTrue(pause_task.result, "Pause failed for database")

        buckets_spec = self.get_bucket_spec(bucket_name_format="taf1-hibernation-%s",
                                            num_buckets=100)
        self.create_required_buckets(buckets_spec=buckets_spec,timeout=1800)

        self.sleep(20, "Wait for before resuming bucket")

        resume_task = self.bucket_util.resume_database(self.cluster, pause_db)
        self.task_manager.get_task_result(resume_task)
        self.assertTrue(resume_task.result, "Resume failed for database")

        resumed_dataplane = self.serverless_util.get_database_dataplane_id(self.pod,
                                                                           pause_db)
        self.log.info("Resumed dataplane: {0}".format(resumed_dataplane))
        self.log.info("Original dataplane: {0}".format(original_dataplane))

        self.assertFalse(resumed_dataplane == original_dataplane, "Did not create" \
                         " new dataplane")

    def test_pause_resume_cluster_full_by_scaling(self):
        original_dataplane = self.dataplanes[0]
        buckets_spec = self.get_bucket_spec(bucket_name_format="pausedb-%s",
                                            num_buckets=26)
        self.create_required_buckets(buckets_spec=buckets_spec,
                                     timeout=1800)

        pause_db = self.cluster.buckets[0].name
        self.log.info("Pausing db: {0}".format(pause_db))
        pause_task = self.bucket_util.pause_database(self.cluster, pause_db)
        self.task_manager.get_task_result(pause_task)
        self.assertTrue(pause_task.result, "Pause failed for database")

        scenario = dict()
        for bucket in self.cluster.buckets[1:]:
            scenario[bucket.name] = {Bucket.width: 4}

        to_track = self._TenantMgmtOnCloud__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=3600, ignore_undesired_updates=True)
        self.task_manager.get_task_result(monitor_task)

        self.sleep(20, "Wait for before resuming bucket")

        resume_task = self.bucket_util.resume_database(self.cluster, pause_db)
        self.task_manager.get_task_result(resume_task)
        self.assertTrue(resume_task.result, "Resume failed for database")

        resumed_dataplane = self.serverless_util.get_database_dataplane_id(self.pod,
                                                                           pause_db)
        self.log.info("Resumed dataplane: {0}".format(resumed_dataplane))
        self.log.info("Original dataplane: {0}".format(original_dataplane))

        self.assertFalse(resumed_dataplane == original_dataplane, "Did not create" \
                         " new dataplane")

    def test_pause_multi_cluster(self):
        self.log.info("Dataplanes: {0}".format(self.dataplanes))
        dataplane_database_map = dict()
        for dataplane in self.dataplanes:
            dataplane_database_map[dataplane] = list()

        buckets_spec = self.get_bucket_spec(bucket_name_format="taf-dp1-%s",
                                            num_buckets=self.num_buckets)
        self.create_required_buckets(buckets_spec=buckets_spec,
                                     dataplane_id=self.dataplanes[0])
        buckets_spec = self.get_bucket_spec(bucket_name_format="taf-dp2-%s",
                                            num_buckets=self.num_buckets)
        self.create_required_buckets(buckets_spec=buckets_spec,
                                     dataplane_id=self.dataplanes[1])

        databases = self.serverless_util.get_all_serverless_databases()
        for db in databases:
            dataplane = self.serverless_util.get_database_dataplane_id(self.pod,
                                                                       db["id"])
            dataplane_database_map[dataplane].append(db["id"])

        self.log.info("Dataplane database map: {0}".format(dataplane_database_map))

        self.load_data()

        #Pause db's from different dataplanes parallely
        for index in range(self.num_buckets):
            pause_tasks = []
            for dataplane in dataplane_database_map:
                pause_db = dataplane_database_map[dataplane][index]
                pause_task = self.bucket_util.pause_database(self.cluster,
                                                             pause_db)
                pause_tasks.append(pause_task)

            for pause_task in pause_tasks:
                self.task_manager.get_task_result(pause_task)
                self.assertTrue(pause_task.result, "Pause failed for database!")


        #Resume some db's from different dataplanes parallely
        for index in range(int(self.num_buckets / 2)):
            resume_task_threads = []
            resume_tasks = []
            for dataplane in dataplane_database_map:
                resume_db = dataplane_database_map[dataplane][index]
                resume_task = self.bucket_util.resume_database(self.cluster,
                                                               resume_db)
                resume_tasks.append(resume_task)

            for resume_task in resume_tasks:
                self.task_manager.get_task_result(resume_task)
                self.assertTrue(resume_task.result, "Resume failed for database!")

        #Resume db's from single dataplane one after other
        for dataplane in dataplane_database_map:
            pause_db_list = dataplane_database_map[dataplane][int(self.num_buckets / 2):]
            for resume_db in pause_db_list:
                resume_task = self.bucket_util.resume_database(self.cluster,
                                                               resume_db)
                self.task_manger.get_task_result(resume_task)
                self.assertTrue(resume_task.result, "Resume failed for database!")

    def test_pause_resume_with_bucket_scaling(self):
        buckets_spec = self.get_bucket_spec(bucket_name_format="taf1-hibernation-%s",
                                            num_buckets=self.num_buckets)
        self.create_required_buckets(buckets_spec=buckets_spec,timeout=1800)
        pause_db = self.cluster.buckets[0].name
        buckets = self.cluster.buckets[1:]

        self.load_data()

        scenario = dict()
        for bucket in buckets:
            scenario[bucket.name] = {Bucket.width: 2}

        to_track = self._TenantMgmtOnCloud__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=3600, ignore_undesired_updates=True)
        self.wait_for_dataplane_to_rebalance()
        self.sleep(5, "Wait before starting pause")
        pause_task = self.bucket_util.pause_database(self.cluster, pause_db)
        self.task_manager.get_task_result(pause_task)
        self.assertTrue(pause_task.result, "Pause failed for database!")
        self.task_manager.get_task_result(monitor_task)

        scenario = dict()
        for bucket in buckets:
            scenario[bucket.name] = {Bucket.width: 3}

        to_track = self._TenantMgmtOnCloud__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=3600, ignore_undesired_updates=True)
        self.wait_for_dataplane_to_rebalance()
        self.sleep(10, "Wait before starting resume")
        resume_task = self.bucket_util.resume_database(self.cluster, pause_db)
        self.task_manager.get_task_result(resume_task)
        self.assertTrue(resume_task.result, "Resume failed for database!")
        self.task_manager.get_task_result(monitor_task)

    def test_pause_resume_with_max_width(self):
        buckets_spec = self.get_bucket_spec(bucket_name_format="taf-hibernation-%s",
                                            num_buckets=1)
        self.create_required_buckets(buckets_spec=buckets_spec)

        self.load_data()

        scenario = dict()
        for bucket in self.cluster.buckets:
            scenario[bucket.name] = {Bucket.width: 100}

        to_track = self._TenantMgmtOnCloud__trigger_bucket_param_updates(scenario)
        monitor_task = self.bucket_util.async_monitor_database_scaling(
            to_track, timeout=3600, ignore_undesired_updates=True)
        self.task_manager.get_task_result(monitor_task)

        self.sleep(5, "Wait before pausing bucket")

        pause_db = self.cluster.buckets[0].name
        pause_task = self.bucket_util.pause_database(self.cluster, pause_db)
        self.task_manager.get_task_result(pause_task)
        self.assertTrue(pause_task.result, "Pause failed for database!")

        resume_task = self.bucket_util.resume_database(self.cluster, pause_db)
        self.task_manager.get_task_result(resume_task)
        self.assertTrue(resume_task.result, "Resume failed for database!")

    def mini_volume(self):
        buckets_spec = self.get_bucket_spec(bucket_name_format="taf-hibernation-%s",
                                            num_buckets=10)
        self.create_required_buckets(buckets_spec=buckets_spec)

        loader_map = dict()
        self.init_sdk_pool_object()
        self.create_sdk_client_pool(buckets=self.cluster.buckets,
                                    req_clients_per_bucket=1)

        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    work_load_settings = DocLoaderUtils.get_workload_settings(
                        key=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        create_perc=100, create_start=0,
                        create_end=self.num_items,
                        ops_rate=self.ops_rate)
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_map.update({"%s:%s:%s" % (bucket.name, scope, collection): dg})

        DocLoaderUtils.perform_doc_loading(
            self.doc_loading_tm, loader_map,
            self.cluster, self.cluster.buckets,
            process_concurrency=10,
            async_load=False, validate_results=False,
            sdk_client_pool=self.sdk_client_pool)
        result = DocLoaderUtils.data_validation(
            self.doc_loading_tm, loader_map, self.cluster,
            buckets=self.cluster.buckets,
            process_concurrency=10,
            ops_rate=self.ops_rate, sdk_client_pool=self.sdk_client_pool)
        self.assertTrue(result, "Data validation failed")

        for i in range(100):
            self.log.info("Iteration number: {0}".format(i))
            for bucket in self.cluster.buckets:
                pause_db = bucket.name
                pause_api_result = self.serverless_util.pause_database(pause_db)
                if pause_api_result.status_code != 202:
                    self.fail("Pause failed for bucket {0}".format(pause_db))
                time_for_pause = 0
                while time_for_pause < 9:
                    self.sleep(10,"Wait for pause to start")
                    pause_wait_result = self.serverless_util.wait_for_hibernation_completion(self.tenant,
                                                                                            pause_db,
                                                                                            "pause")
                    if pause_wait_result == False:
                        time_for_pause += 1
                    else:
                        break
                if time_for_pause == 9:
                    self.fail("Pause did not complete for bucket {0}".format(pause_db))

                self.sleep(5, "Wait before resuming")
                resume_api_result = self.serverless_util.resume_database(pause_db)
                if resume_api_result.status_code != 202:
                    self.fail("Resume failed for bucket {0}".format(pause_db))
                time_for_resume = 0
                while time_for_resume < 9:
                    self.sleep(10, "Wait for resume to start")
                    resume_wait_result = self.serverless_util.wait_for_hibernation_completion(self.tenant,
                                                                                            pause_db,
                                                                                            "resume")
                    if resume_wait_result == False:
                        time_for_resume += 1
                    else:
                        break

                if time_for_resume == 9:
                    self.fail("Resume did not complete for bucket {0}".format(pause_db))


    def create_database(self, bucket=None, tenant_obj=None, timeout=600):
        if not bucket:
            bucket = self.get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
        self.log.info("bucket : {0}".format(bucket))
        task = self.bucket_util.async_create_database(
            self.cluster, bucket, dataplane_id=self.dataplane_id, tenant=tenant_obj,
            timeout=timeout)
        self.task_manager.get_task_result(task)
        self.assertTrue(task.result, "Database creation failed")
        return task.bucket_obj.name

    def test_pause_resume_multi_tenant(self):
        self.log.info("Tenants: {0}".format(self.tenants))

        tenant_database_map = dict()
        tenant_pause_database_map = dict()
        tenant_resume_database_map = dict()
        tenant_pause_task_map = dict()
        tenant_resume_task_map = dict()

        for tenant_idx, tenant in enumerate(self.tenants):
            tenant_database_map[tenant.id] = list()
            for index in range(self.num_databases):
                self.db_name = "TAF-{0}-{1}-{2}".format(tenant_idx, tenant.id, index)
                db_id = self.create_database(tenant_obj=tenant, timeout=1800)
                tenant_database_map[tenant.id].append(db_id)

        self.log.info("Tenant-db mapping: {0}".format(tenant_database_map))

        self.load_data()

        for tenant in self.tenants:
            random_number = random.randrange(self.num_databases)
            tenant_database_list = tenant_database_map[tenant.id]
            pause_databases = tenant_database_list[:random_number+1]
            tenant_pause_task_map[tenant.id] = list()
            tenant_pause_database_map[tenant.id] = list()
            for pause_db in pause_databases:
                self.log.info("Pausing databse: {0}".format(pause_db))
                pause_task = self.bucket_util.pause_database(self.cluster,
                                                             pause_db,
                                                             tenant)
                tenant_pause_task_map[tenant.id].append(pause_task)
                tenant_pause_database_map[tenant.id].append(pause_db)

        for tenant in self.tenants:
            pause_task_list = tenant_pause_task_map[tenant.id]
            for pause_task in pause_task_list:
                self.task_manager.get_task_result(pause_task)
                self.assertTrue(pause_task.result, "Pause failed failed for database!")


        for tenant in self.tenants:
            pause_databases_list = tenant_pause_database_map[tenant.id]
            tenant_resume_task_map[tenant.id] = list()
            for resume_db in pause_databases_list:
                resume_task = self.bucket_util.resume_database(self.clsuter,
                                                               resume_db,
                                                               tenant)
                tenant_resume_task_map[tenant.id].append(resume_task)

        for tenant in self.tenants:
            resume_task_list = tenant_resume_task_map[tenant.id]
            for resume_task in resume_task_list:
                self.task_manager.get_task_result(resume_task)
                self.assertTrue(resume_task, "Resume of database failed!")
