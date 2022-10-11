from random import choice

from BucketLib.bucket import Bucket
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cb_tools.cbstats import Cbstats
from cluster_utils.cluster_ready_functions import Nebula
from collections_helper.collections_spec_constants import MetaConstants
from serverlessbasetestcase import OnCloudBaseTest
from Cb_constants import CbServer
from capellaAPI.capella.serverless.CapellaAPI import CapellaAPI

from com.couchbase.test.docgen import DocumentGenerator
from com.couchbase.test.sdk import Server
from com.couchbase.test.taskmanager import TaskManager

from java.util.concurrent import ExecutionException


class TenantMgmtOnCloud(OnCloudBaseTest):
    def setUp(self):
        super(TenantMgmtOnCloud, self).setUp()
        self.key_type = "SimpleKey"
        self.val_type = "SimpleValue"
        self.doc_loading_tm = TaskManager(2)
        self.db_name = "TAF-TenantMgmtOnCloud"
        self.token = self.input.capella.get("token")
        self.with_data_load = self.input.param("with_data_load", False)

    def tearDown(self):
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()
        super(TenantMgmtOnCloud, self).tearDown()

    def get_bucket_spec(self, num_buckets=1, scopes_per_bucket=1,
                        collections_per_scope=1, items_per_collection=0):
        self.log.debug("Getting spec for %s buckets" % num_buckets)
        buckets = dict()
        for i in range(num_buckets):
            buckets[self.bucket_name_format % i] = dict()
        return {
            MetaConstants.NUM_BUCKETS: num_buckets,
            MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
            MetaConstants.NUM_SCOPES_PER_BUCKET: scopes_per_bucket,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: collections_per_scope,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: items_per_collection,
            MetaConstants.USE_SIMPLE_NAMES: True,

            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: 256,
            Bucket.width: 1,
            Bucket.weight: 30,
            Bucket.maxTTL: 0,
            "buckets": buckets
        }

    def __get_single_bucket_scenarios(self, target_scenario):
        bucket_name = choice(self.cluster.buckets).name
        weight_incr = {1: 30, 2: 15, 3: 10, 4: 7}
        weight_start = {1: 30, 2: 210, 3: 270, 4: 300}
        if target_scenario == "single_bucket_width_change":
            return [{bucket_name: {Bucket.width: width}}
                    for width in range(2, 4)]
        if target_scenario == "single_bucket_weight_increment":
            return [{bucket_name: {Bucket.weight: index*30}}
                    for index in range(2, 14)]
        if target_scenario == "single_bucket_width_weight_incremental":
            scenarios = list()
            width = 1
            weight = 30
            while width <= 4:
                scenarios.append(
                    {bucket_name: {Bucket.width: width,
                                   Bucket.weight: weight}})
                weight += weight_incr[width]
                if weight > 390:
                    width += 1
                    weight = weight_start[width]
            scenarios = scenarios + scenarios[::-1][1:]
            return scenarios
        if target_scenario == "single_bucket_width_weight_random":
            max_scenarios = 20
            scenarios = list()
            # Creates 20 random scenarios of random width/weight update
            for scenario_index in range(max_scenarios):
                width = choice(range(1, 5))
                weight = weight_start[width] \
                    + (weight_incr[width] * choice(range(1, 13)))
                scenarios.append({bucket_name: {Bucket.width: width,
                                                Bucket.weight: weight}})
            return scenarios

    def __get_multi_bucket_scenarios(self, num_buckets_to_target=2):
        scenarios = list()
        return scenarios

    def get_serverless_bucket_obj(self, db_name, width, weight, num_vbs=None):
        self.log.debug("Creating server bucket_obj %s:%s:%s"
                       % (db_name, width, weight))
        bucket_obj = Bucket({
            Bucket.name: db_name,
            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: 256,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
            Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
            Bucket.width: width,
            Bucket.weight: weight})
        if num_vbs:
            bucket_obj.numVBuckets = num_vbs

        return bucket_obj

    def create_database(self, bucket=None):
        if not bucket:
            bucket = self.get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
        task = self.bucket_util.async_create_database(
            self.cluster, bucket, dataplane_id=self.dataplane_id)
        self.task_manager.get_task_result(task)

    def __create_required_buckets(self, buckets_spec=None):
        if buckets_spec or self.spec_name:
            if buckets_spec is None:
                self.log.info("Creating buckets from spec: %s"
                              % self.spec_name)
                buckets_spec = \
                    self.bucket_util.get_bucket_template_from_package(
                        self.spec_name)
                buckets_spec[MetaConstants.USE_SIMPLE_NAMES] = True

            # Process params to over_ride values if required
            CollectionBase.over_ride_bucket_template_params(
                self, self.bucket_storage, buckets_spec)
            self.bucket_util.create_buckets_using_json_data(
                self.cluster, buckets_spec)
            self.sleep(5, "Wait for collections creation to complete")
        else:
            self.log.info("Creating '%s' buckets" % self.num_buckets)
            for _ in range(self.num_buckets):
                self.create_database()

    def create_sdk_client_pool(self, buckets, req_clients_per_bucket):
        if self.sdk_client_pool:
            self.sdk_client_pool = \
                self.bucket_util.initialize_java_sdk_client_pool()

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

    def test_create_delete_database(self):
        """
        1. Loading initial buckets
        2. Start data loading to all buckets
        3. Create more buckets when data loading is running
        4. Delete the created database while initial load is still running
        :return:
        """
        self.db_name = "%s-testCreateDeleteDatabase" % self.db_name
        dynamic_buckets = self.input.param("other_buckets", 1)
        # Create Buckets
        self.__create_required_buckets()

        loader_map = dict()

        # Create sdk_client_pool
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
                        ops_rate=100)
                    dg = DocumentGenerator(work_load_settings,
                                           self.key_type, self.val_type)
                    loader_map.update(
                        {bucket.name + scope + collection: dg})

        DocLoaderUtils.perform_doc_loading(
            self.doc_loading_tm, loader_map,
            self.cluster, self.cluster.buckets,
            async_load=False, validate_results=False,
            sdk_client_pool=self.sdk_client_pool)
        result = DocLoaderUtils.data_validation(
            self.doc_loading_tm, loader_map, self.cluster,
            buckets=self.cluster.buckets, doc_ops=["create"],
            process_concurrency=self.process_concurrency,
            ops_rate=self.ops_rate, sdk_client_pool=self.sdk_client_pool)
        self.assertTrue(result, "Data validation failed")

        new_buckets = list()
        for _ in range(dynamic_buckets):
            bucket = self.get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
            task = self.bucket_util.async_create_database(self.cluster, bucket)
            self.task_manager.get_task_result(task)
            nebula = Nebula(task.srv, task.server)
            self.log.info("Populate Nebula object done!!")
            bucket.serverless.nebula_endpoint = nebula.endpoint
            bucket.serverless.dapi = \
                self.serverless_util.get_database_DAPI(self.pod, self.tenant,
                                                       bucket.name)
            self.bucket_util.update_bucket_nebula_servers(self.cluster, nebula,
                                                          bucket)
            new_buckets.append(bucket)

        # TODO: Load docs to new buckets

        self.log.info("Removing '%s' buckets" % new_buckets)
        for bucket in new_buckets:
            self.serverless_util.delete_database(self.pod, self.tenant,
                                                 bucket.name)

    def test_recreate_database(self):
        """
        1. Create a database
        2. Remove the database immediately after create
        3. Recreate the database with the same name
        4. Check the recreate was successful
        :return:
        """
        db_name = "tntmgmtrecreatedb"
        max_itr = 5
        bucket = self.get_serverless_bucket_obj(
            db_name, self.bucket_width, self.bucket_weight)

        for itr in range(1, max_itr):
            self.log.info("Iteration :: %s" % itr)
            bucket_name = self.serverless_util.create_serverless_database(
                self.cluster.pod, self.cluster.tenant, bucket.name,
                "aws", "us-east-1",
                bucket.serverless.width, bucket.serverless.weight)
            self.log.info("Bucket %s created" % bucket_name)
            self.serverless_util.delete_database(
                self.pod, self.tenant, bucket_name)
            self.serverless_util.wait_for_database_deleted(
                self.tenant, bucket_name)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        self.task_manager.get_task_result(task)

    def test_bucket_collection_limit(self):
        pass

    def test_create_database_negative(self):
        """
        1. Create serverless database using invalid name and validate
        2. Create serverless db with name of one character
        :return:
        """
        invalid_char_err = \
            "{\"errorType\":\"ErrDatabaseNameInvalidChars\"," \
            "\"message\":\"Not able to create serverless database. " \
            "The name of the database contains one or more invalid " \
            "characters ','. Database names can only contain " \
            "alphanumeric characters, space, and hyphen.\"}"
        name_too_short_err = \
            "{\"errorType\":\"ErrDatabaseNameTooShort\"," \
            "\"message\":\"Not able to create serverless database. " \
            "The name provided must be at least two characters in length. " \
            "Please try again.\"}"
        bucket = self.get_serverless_bucket_obj("123,", 1, 30)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        try:
            self.task_manager.get_task_result(task)
        except ExecutionException as exception:
            if invalid_char_err not in str(exception):
                self.fail("Exception mismatch. Got::%s" % exception)

        bucket = self.get_serverless_bucket_obj("1", 1, 30)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        try:
            self.task_manager.get_task_result(task)
        except ExecutionException as exception:
            if name_too_short_err not in str(exception):
                self.fail("Exception mismatch. Got::%s" % exception)

    def test_bucket_scaling(self):
        def get_bucket_with_name(b_name):
            for b_obj in self.cluster.buckets:
                if b_obj.name == b_name:
                    return b_obj

        doc_loading_tasks = None
        self.bucket_name_format = "tntMgmtScaleTest-%s"
        target_scenario = self.input.param("target_scenario")

        spec = self.get_bucket_spec(self.num_buckets)
        self.__create_required_buckets(buckets_spec=spec)
        if target_scenario.startswith("single_bucket_"):
            scenarios = self.__get_single_bucket_scenarios(target_scenario)
        else:
            scenarios = self.__get_multi_bucket_scenarios(target_scenario)

        if self.with_data_load:
            self.create_sdk_client_pool(self.cluster.buckets, 1)
            loader_map = dict()
            for bucket in self.cluster.buckets:
                work_load_settings = DocLoaderUtils.get_workload_settings(
                    key=self.key, key_size=self.key_size,
                    doc_size=self.doc_size,
                    create_perc=100, create_start=0,
                    create_end=self.num_items,
                    ops_rate=100)
                dg = DocumentGenerator(work_load_settings,
                                       self.key_type, self.val_type)
                loader_map.update(
                    {bucket.name + CbServer.default_scope
                     + CbServer.default_collection: dg})

            doc_loading_tasks = DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, loader_map,
                self.cluster, self.cluster.buckets,
                async_load=True, validate_results=False,
                sdk_client_pool=self.sdk_client_pool)

        capella_api = CapellaAPI(self.pod.url_public, None, None, self.token)
        for scenario in scenarios:
            monitor_tasks = list()
            for bucket_name, update_dict in scenario.items():
                bucket_obj = get_bucket_with_name(bucket_name)

                args_to_monitor_db_scale_task = {
                    "cluster": self.cluster,
                    "bucket": bucket_obj,
                    "desired_ram_quota": None,
                    "desired_width": None,
                    "desired_weight": None,
                    "timeout": 300
                }

                over_ride = dict()
                if Bucket.width in update_dict:
                    over_ride[Bucket.width] = update_dict[Bucket.width]
                    args_to_monitor_db_scale_task["desired_width"] = \
                        update_dict[Bucket.width]
                    bucket_obj.serverless.width = update_dict[Bucket.width]
                if Bucket.weight in update_dict:
                    over_ride[Bucket.weight] = update_dict[Bucket.weight]
                    args_to_monitor_db_scale_task["desired_weight"] = \
                        update_dict[Bucket.weight]
                    bucket_obj.serverless.weight = update_dict[Bucket.weight]
                resp = capella_api.update_database(
                    bucket_obj.name, {"overRide": over_ride})
                self.assertTrue(resp.status_code == 200,
                                "Update Api failed")

                monitor_task = self.bucket_util.async_monitor_database_scaling(
                    **args_to_monitor_db_scale_task)
                monitor_tasks.append(monitor_task)

            for monitor_task in monitor_tasks:
                self.task_manager.get_task_result(monitor_task)

        if self.with_data_load:
            DocLoaderUtils.wait_for_doc_load_completion(self.doc_loading_tm,
                                                        doc_loading_tasks)

    def test_bucket_auto_ram_scaling(self):
        """
        :return:
        """
        start_index = 0
        batch_size = 50000
        self.__create_required_buckets()
        bucket = self.cluster.buckets[0]

        work_load_settings = DocLoaderUtils.get_workload_settings(
            key=self.key, key_size=self.key_size, doc_size=self.doc_size,
            create_perc=100, create_start=start_index, create_end=start_index,
            ops_rate=self.ops_rate)
        dg = DocumentGenerator(work_load_settings,
                               self.key_type, self.val_type)

        loader_key = "%s%s%s" % (bucket.name, CbServer.default_scope,
                                 CbServer.default_collection)
        self.create_sdk_client_pool([bucket], 1)
        dgm_index = 0
        storage_band = 1
        target_dgms = [3, 2.3, 2.0, 1.9, 1.8, 1.5]
        len_target_dgms = len(target_dgms)

        self.log.critical("Loading bucket till %s%% DGM"
                          % target_dgms[dgm_index])
        while dgm_index < len_target_dgms:
            work_load_settings.dr.create_s = work_load_settings.dr.create_e
            work_load_settings.dr.create_e += batch_size
            DocLoaderUtils.perform_doc_loading(
                self.doc_loading_tm, {loader_key: dg}, self.cluster,
                buckets=[bucket], async_load=False, validate_results=False,
                sdk_client_pool=self.sdk_client_pool)

            for server in bucket.servers:
                stat = Cbstats(server)
                resident_mem = stat.get_stats_memc(bucket.name)[
                    "vb_active_perc_mem_resident"]
                if int(resident_mem) <= target_dgms[dgm_index]:
                    dgm_index += 1
                    storage_band += 1
                    if dgm_index < len_target_dgms:
                        self.log.critical("Loading bucket till %s%% DGM"
                                          % target_dgms[dgm_index])

                    monitor_task = \
                        self.bucket_util.async_monitor_database_scaling(
                            self.cluster, bucket)
                    self.task_manager.get_task_result(monitor_task)
