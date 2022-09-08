from BucketLib.bucket import Bucket
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cluster_utils.cluster_ready_functions import Nebula
from serverlessbasetestcase import OnCloudBaseTest
from Cb_constants import CbServer
from capellaAPI.capella.serverless.CapellaAPI import CapellaAPI

from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.docgen import DocumentGenerator

from java.util.concurrent import ExecutionException


class TenantMgmtOnCloud(OnCloudBaseTest):
    def setUp(self):
        super(TenantMgmtOnCloud, self).setUp()
        self.key_type = "SimpleKey"
        self.val_type = "SimpleValue"
        self.doc_loading_tm = TaskManager(2)
        self.db_name = "TAF-TenantMgmtOnCloud"

    def tearDown(self):
        super(TenantMgmtOnCloud, self).tearDown()

    def __get_width_scenarios(self, target_scenario):
        self.log.debug("Fetching spec for scenario: %s" % target_scenario)
        scenario = dict()
        scenario["single_bucket_incr"] = {
            "spec": {
                "bucket-1": {
                    "width": 1,
                    "update_spec": {
                        "overRide": {
                            "width": 2,
                        }
                    }
                }
            },
        }
        scenario["single_bucket_decr"] = {
            "spec": {
                "bucket-1": {
                    "width": 2,
                    "update_spec": {
                        "overRide": {
                            "width": 1,
                        }
                    }
                }
            },
        }
        scenario["multi_bucket_incr"] = {
            "spec": {
                "bucket-1": {
                    "width": 1,
                    "update_spec": {
                        "overRide": {
                            "width": 2,
                        }
                    }
                },
                "bucket-2": {
                    "width": 1,
                    "update_spec": {
                        "overRide": {
                            "width": 2,
                        }
                    }
                }
            },
        }
        scenario["multi_bucket_decr"] = {
            "spec": {
                "bucket-1": {
                    "width": 2,
                    "update_spec": {
                        "overRide": {
                            "width": 1,
                        }
                    }
                },
                "bucket-2": {
                    "width": 2,
                    "update_spec": {
                        "overRide": {
                            "width": 1,
                        }
                    }
                }
            },
        }
        return scenario[target_scenario]

    def __get_serverless_bucket_obj(self, db_name, width, weight,
                                    num_vbs=None):
        self.log.debug("Creating server bucket_obj %s:%s:%s"
                       % (db_name, width, weight))
        bucket_obj = Bucket({
            Bucket.name: db_name,
            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
            Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
            Bucket.width: width,
            Bucket.weight: weight})
        if num_vbs:
            bucket_obj.numVBuckets = num_vbs

        return bucket_obj

    def __create_database(self, bucket=None):
        if not bucket:
            bucket = self.__get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
        task = self.bucket_util.async_create_database(
            self.cluster, bucket, dataplane_id=self.dataplane_id)
        self.task_manager.get_task_result(task)
        nebula = Nebula(task.srv, task.server)
        bucket.serverless.nebula_endpoint = nebula.endpoint
        bucket.serverless.dapi = self.serverless_util.get_database_DAPI(
            self.pod, self.tenant, bucket.name)
        self.bucket_util.update_bucket_nebula_servers(
            self.cluster, nebula, bucket)
        self.cluster.buckets.append(bucket)

    def test_create_delete_database(self):
        """
        1. Loading initial buckets
        2. Start data loading to all buckets
        3. Create more buckets when data loading is running
        4. Delete the newly created database while intial load is still running
        :return:
        """
        self.db_name = "%s-testCreateDeleteDatabase" % self.db_name
        dynamic_buckets = self.input.param("other_buckets", 1)
        # Create Buckets
        self.log.info("Creating '%s' buckets for initial load"
                      % self.num_buckets)
        for _ in range(self.num_buckets):
            self.__create_database()

        loader_map = dict()
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
            async_load=False, validate_results=False)
        result = DocLoaderUtils.data_validation(
            self.doc_loading_tm, loader_map, self.cluster,
            buckets=self.cluster.buckets, doc_ops=["create"],
            process_concurrency=self.process_concurrency,
            ops_rate=self.ops_rate)
        self.assertTrue(result, "Data validation failed")

        new_buckets = list()
        for _ in range(dynamic_buckets):
            bucket = self.__get_serverless_bucket_obj(
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
            self.serverless_util.delete_database(self.pod, self.tenant, bucket.name)

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
        bucket = self.__get_serverless_bucket_obj("123,", 1, 30)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        try:
            self.task_manager.get_task_result(task)
        except ExecutionException as exception:
            if invalid_char_err not in str(exception):
                self.fail("Exception mismatch. Got::%s" % exception)

        bucket = self.__get_serverless_bucket_obj("1", 1, 30)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        try:
            self.task_manager.get_task_result(task)
        except ExecutionException as exception:
            if name_too_short_err not in str(exception):
                self.fail("Exception mismatch. Got::%s" % exception)

    def test_update_bucket_width(self):
        def verify_bucket_scaling():
            self.sleep(120, "Waiting for bucket to complete scaling")
            for t_key in scenario_dict["spec"].keys():
                t_bucket = name_key_map[t_key]
                srv = self.serverless_util.get_database_nebula_endpoint(
                    self.cluster.pod, self.cluster.tenant, t_bucket.name)
                self.bucket_util.update_bucket_nebula_servers(
                    self.cluster, Nebula(srv, t_bucket.servers[0]),
                    t_bucket)
                if "update_spec" in scenario_dict["spec"][t_key]:
                    self.assertTrue(
                        len(self.cluster.bucketDNNodes[t_bucket]) == (
                                scenario_dict["spec"][t_key]["update_spec"]
                                ["overRide"]["width"]
                                * CbServer.Serverless.KV_SubCluster_Size),
                        "Bucket scaling failed")
                else:
                    self.assertTrue(
                        len(self.cluster.bucketDNNodes[t_bucket]) == (
                            scenario_dict["spec"][t_key]["width"]
                            * CbServer.Serverless.KV_SubCluster_Size),
                        "Bucket scaling failed")

        index = 0
        name_key_map = dict()
        token = self.input.capella.get("token")
        target_scenario = self.input.param("target_scenario")

        capella_api = CapellaAPI(self.pod.url_public, None, None, token)
        scenario_dict = self.__get_width_scenarios(target_scenario)
        for key in scenario_dict["spec"].keys():
            bucket = self.__get_serverless_bucket_obj(
                key, scenario_dict["spec"][key]["width"],
                self.bucket_weight)
            self.__create_database(bucket)
            name_key_map[key] = self.cluster.buckets[index]
            index += 1

        for key in scenario_dict["spec"].keys():
            if "update_spec" in scenario_dict["spec"][key]:
                resp = capella_api.update_database(
                    name_key_map[key].name,
                    scenario_dict["spec"][key]["update_spec"])
                self.assertTrue(resp.status_code == 200,
                                "Update Api failed")

        verify_bucket_scaling()
