import time
from datetime import datetime, timezone
import pytz
from _datetime import timedelta

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from Columnar.mini_volume_code_template import MiniVolume


class OnOff(ColumnarBaseTest):
    def setUp(self):
        super(OnOff, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')

        self.no_of_docs = self.input.param("no_of_docs", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if hasattr(self, "mini_volume"):
            self.mini_volume.stop_crud_on_data_sources()
            self.mini_volume.stop_process()
        if hasattr(self, "remote_cluster") and hasattr(self.remote_cluster, "buckets"):
            self.delete_all_buckets_from_capella_cluster(self.tenant, self.remote_cluster)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def remote_cluster_setup(self):
        for key in self.cb_clusters:
            self.remote_cluster = self.cb_clusters[key]
            break
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant.id,
                                                                               self.tenant.project_id,
                                                                               self.remote_cluster.id, "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")
        remote_cluster_certificate_request = (
            self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id, self.tenant.project_id,
                                                                     self.remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            self.remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        # creating bucket scope and collections for remote collection
        no_of_remote_buckets = self.input.param("no_of_remote_bucket", 1)
        self.create_bucket_scopes_collections_in_capella_cluster(self.tenant, self.remote_cluster, no_of_remote_buckets,
                                                                 bucket_ram_quota=200)

    def base_infra_setup(self):
        self.columnar_spec["database"]["no_of_databases"] = self.input.param("no_of_database", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param("no_of_scopes", 1)
        self.columnar_spec["synonym"]["no_of_synonyms"] = self.input.param(
            "synonym", 0)
        self.columnar_spec["index"]["no_of_indexes"] = self.input.param(
            "index", 0)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)
        if self.columnar_spec["remote_link"]["no_of_remote_links"] != 0:
            self.remote_cluster_setup()
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
                 "username": self.remote_cluster.username,
                 "password": self.remote_cluster.password,
                 "encryption": "full",
                 "certificate": self.remote_cluster_certificate}
            )
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("no_of_remote_coll", 1)

        self.columnar_spec["external_link"]["no_of_external_links"] = self.input.param(
            "no_of_external_links", 0)

        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]
        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param("no_of_external_coll", 0)
        if self.columnar_spec["external_dataset"]["num_of_external_datasets"]:
            external_dataset_properties = [{
                "external_container_name": self.s3_source_bucket,
                "path_on_external_container": None,
                "file_format": self.input.param("file_format", "json"),
                "include": ["*.{0}".format(self.input.param("file_format", "json"))],
                "exclude": None,
                "region": self.aws_region,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }]
            self.columnar_spec["external_dataset"][
                "external_dataset_properties"] = external_dataset_properties

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "no_of_standalone_coll", 0)
        self.columnar_spec["standalone_dataset"]["primary_key"] = []

        if not hasattr(self, "remote_cluster"):
            remote_cluster = None
        else:
            remote_cluster = [self.remote_cluster]
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=remote_cluster)
        if not result:
            self.fail(msg)

    def load_data_to_source(self, remote_start, remote_end):
        if hasattr(self, "remote_cluster") and hasattr(self.remote_cluster, "buckets"):
            for bucket in self.remote_cluster.buckets:
                if bucket.name != "_default":
                    for scope in bucket.scopes:
                        if scope != "_system" and scope != "_mobile":
                            for collection in bucket.scopes[scope].collections:
                                self.cbas_util.doc_operations_remote_collection_sirius(self.task_manager, collection,
                                                                                       bucket.name, scope,
                                                                                       "couchbases://" + self.remote_cluster.srv,
                                                                                       remote_start, remote_end,
                                                                                       doc_size=self.doc_size,
                                                                                       username=self.remote_cluster.username,
                                                                                       password=self.remote_cluster.password)
        standalone_collections = self.cbas_util.get_all_dataset_objs("standalone")
        for collection in standalone_collections:
            self.cbas_util.load_doc_to_standalone_collection(self.cluster, collection.name, collection.dataverse_name,
                                                             collection.database_name, self.no_of_docs, self.doc_size)

    def wait_for_off(self, timeout=3600):
        status = None
        start_time = time.time()
        while status != 'turning_off':
            try:
                resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                             self.cluster.instance_id)
                status = resp["data"]["state"]
            except Exception as e:
                self.log.error(str(e))
        while status == 'turning_off' and time.time() < start_time + timeout:
            try:
                resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                             self.cluster.instance_id)
                status = resp["data"]["state"]
                self.log.info("Instance is still turning off")
                time.sleep(20)
            except Exception as e:
                self.log.error(str(e))
        if status == "turned_off":
            self.log.info("Time taken for instance off {} seconds".format(time.time() - start_time))
            self.log.info("Instance off successful")
            return True
        else:
            self.log.error("Failed to turn off the instance")
            return False

    def wait_for_on(self, timeout=3600):
        status = None
        start_time = time.time()
        while status != 'turning_on':
            resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        while status == 'turning_on' and time.time() < start_time + timeout:
            try:
                resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                             self.cluster.instance_id)
                status = resp["data"]["state"]
                self.log.info("Instance is still turning on")
                time.sleep(20)
            except Exception as e:
                self.log.error(str(e))
        if status == "healthy":
            self.log.info("Instance on successful")
            self.log.info("Time taken to on the instance is {} seconds".format(time.time() - start_time))
            return True
        else:
            self.log.error("Failed to turn on the instance")
            return False

    def dataset_count(self, timeout=3600):
        items_in_datasets = {}
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            items_in_datasets[dataset.full_name] = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster,
                                                                                                dataset.full_name,
                                                                                                timeout=timeout,
                                                                                                analytics_timeout=timeout)
        return items_in_datasets

    def validate_entities_after_restore(self):
        links = set([link.name for link in self.cbas_util.get_all_link_objs()])
        datasets = set([dataset.full_name for dataset in self.cbas_util.get_all_dataset_objs()])
        indexes = set([index.full_name for index in self.cbas_util.get_all_index_objs()])
        synonyms = set([synonyms.full_name for synonyms in self.cbas_util.get_all_synonym_objs()])
        metadata_links = set(self.cbas_util.get_all_links_from_metadata(self.cluster))
        metadata_datasets = set(self.cbas_util.get_all_datasets_from_metadata(self.cluster))
        metadata_indexes = set(self.cbas_util.get_all_indexes_from_metadata(self.cluster))
        metadata_synonyms = set(self.cbas_util.get_all_synonyms_from_metadata(self.cluster))
        # match entities from TAF object and from columnar metadata
        self.log.info("Link entity matched") if links == metadata_links else self.fail("Link entity mismatch")
        self.log.info("Dataset entity matched") if datasets == metadata_datasets else self.fail("Dataset entity "
                                                                                                "mismatch")
        self.log.info("Index entity matched") if indexes == metadata_indexes else self.fail("Index entity mismatch")
        self.log.info("Synonyms entity matched") if synonyms == metadata_synonyms else self.fail("Synonyms entity "
                                                                                                 "mismatch")

    def scale_columnar_cluster(self, nodes, timeout=3600):
        status = None
        start_time = time.time()
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id,
                                                         self.tenant.project_id,
                                                         self.cluster.instance_id,
                                                         self.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
        time.sleep(20)
        # check for nodes in the cluster
        while status != "healthy" and time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                                   self.tenant.project_id,
                                                                   self.cluster.instance_id)
            resp = resp.json()
            status = resp["data"]["state"]
            self.log.info("Instance is still scaling")
            time.sleep(20)

        nodes_in_cluster = 0
        while nodes_in_cluster != nodes and time.time() < start_time + timeout:
            servers = self.get_nodes(self.cluster)
            nodes_in_cluster = len(servers)
            if nodes_in_cluster == nodes:
                return True
            self.log.info("Waiting for server map to get updated")
            time.sleep(20)
        self.log.error("Failed to update server map")
        return False

    def test_on_demand_on_off(self):

        self.base_infra_setup()
        self.load_data_to_source(0, self.no_of_docs)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count = self.dataset_count()
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
        else:
            self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
        if not self.wait_for_off():
            self.fail("Failed to turn off instance")

        # resume the instance
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        else:
            self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

        self.update_columnar_instance_obj(self.pod, self.tenant, self.cluster)
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")
        self.validate_entities_after_restore()

    def test_on_demand_off_after_scaling_and_scale_after_resume(self):
        self.base_infra_setup()
        self.load_data_to_source(0, self.no_of_docs)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count = self.dataset_count()
        self.scale_columnar_cluster(8)
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
        else:
            self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
        if not self.wait_for_off():
            self.fail("Failed to turn off instance")

        # resume instance
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        else:
            self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")
        self.scale_columnar_cluster(2)

        self.update_columnar_instance_obj(self.pod, self.tenant, self.cluster)
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")
        self.validate_entities_after_restore()

    def test_off_during_scale(self):
        # off during scale
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id, self.tenant.project_id,
                                                         self.cluster.instance_id,
                                                         self.cluster.name, '', 32)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
        else:
            resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            if resp.status_code != 422:
                if resp["errorType"] != "EntityStateInvalid":
                    self.fail("Status code and errorType mismatch")

        start_time = time.time()
        state = None
        while state != "healthy" and start_time + 900 > time.time():
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id, self.tenant.project_id,
                                                                   self.cluster.instance_id).json()
            state = resp["data"]["state"]
        if state != "healthy":
            self.fail("Fail to scale columnar instance")

    def test_scale_during_resume(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn off instance")
        if not self.wait_for_off():
            self.fail("Failed to turn the instance off")

        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")

        resp = self.columnarAPI.update_columnar_instance(self.tenant.id, self.tenant.project_id,
                                                         self.cluster.instance_id,
                                                         self.cluster.name, '', 4)
        if resp.status_code != 202:
            self.fail("Scaling api failed")

        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def test_off_during_off(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn off instance")

        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.log.info("Failed to turn of instance")

        if not self.wait_for_off():
            self.fail("Failed to turn off cluster")

        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def test_on_during_on(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn off instance")
        self.wait_for_off()
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.log.info("Turn Off api failed")
        if not self.wait_for_on():
            self.fail("Failed to turn the instance on")

    def test_on_during_off(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn off instance")
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 500:
            self.wait_for_off()
            resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            if resp.status_code != 202:
                self.fail("Failed to turn on the instance")
            if not self.wait_for_on():
                self.fail("Failed to turn on the instance")
            self.fail("Failed due to bug")
        self.log.info("Bug fixed")
        if not self.wait_for_off():
            self.fail("Failed to turn off the instance")
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn on the instance")
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def test_off_during_on(self):
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn off instance")
        if not self.wait_for_off():
            self.fail("Failed to turn off the instance")
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code != 202:
            self.fail("Failed to turn on the instance")

        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 500:
            if not self.wait_for_on():
                self.fail("Failed to turn on the instance")
            self.fail("Failed due to bug")

        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def convert_pacific_to_utc(self, pacific_time):
        utc_dt = pacific_time.astimezone(pytz.utc)
        utc_year = utc_dt.year
        utc_month = utc_dt.month
        utc_day = utc_dt.day
        utc_hour = utc_dt.hour
        utc_minute = utc_dt.minute
        utc_second = utc_dt.second
        input_datetime_utc = datetime(utc_year, utc_month, utc_day, utc_hour, utc_minute, utc_second)

        # Format the datetime to ISO 8601 with 'Z' for UTC
        formatted_datetime = input_datetime_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        return formatted_datetime

    def test_schedule_on_off(self):
        self.base_infra_setup()
        self.load_data_to_source(0, self.no_of_docs)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count = self.dataset_count()
        pacific = pytz.timezone('US/Pacific')
        now = datetime.now(pacific)

        # Calculate days until next Thursday (using modulo 7 to wrap around)
        days_until_friday = (4 - now.weekday()) % 7
        if days_until_friday == 0:
            days_until_friday += 7

        # Calculate the next Friday date
        next_friday = now + timedelta(days=days_until_friday)

        # Set the time to 16:30
        next_friday_1630 = next_friday.replace(hour=16, minute=30, second=0, microsecond=0)
        next_friday_1030 = next_friday.replace(hour=10, minute=30, second=0, microsecond=0)
        next_friday_1030_in_utc = self.convert_pacific_to_utc(next_friday_1030)
        next_friday_1630_in_utc = self.convert_pacific_to_utc(next_friday_1630)
        used_timezone = "US/Pacific"
        data = [
            {
                "day": "monday",
                "state": "on"
            },
            {
                "day": "tuesday",
                "state": "on"
            },
            {
                "day": "wednesday",
                "state": "on"
            },
            {
                "day": "thursday",
                "state": "on"
            },
            {
                "day": "friday",
                "state": "custom",
                "from": {
                    "hour": 10,
                    "minute": 30
                },
                "to": {
                    "hour": 16,
                    "minute": 30
                }
            },
            {
                "day": "saturday",
                "state": "off"
            },
            {
                "day": "sunday",
                "state": "off"
            }
        ]
        resp = self.columnarAPI.schedule_on_off(self.tenant.id, self.tenant.project_id,
                                                self.cluster.instance_id, data, used_timezone)
        if resp.status_code != 200:
            self.fail("Failed to add schedule to the instance {}".format(self.cluster.instance_id))

        internal_support_token = self.input.param("internal_support_token")
        columnar_internal = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                        self.tenant.pwd, internal_support_token)
        resp = columnar_internal.set_trigger_time_for_onoff(next_friday_1630_in_utc,
                                                            [self.cluster.cluster_id])
        if resp.status_code == 200:
            self.log.info("Applied sudo time to trigger off")
        else:
            self.fail("Failed to apply sudo time")

        self.wait_for_off()

        resp = columnar_internal.set_trigger_time_for_onoff(next_friday_1030_in_utc,
                                                            [self.cluster.instance_id])
        if resp.status_code == 200:
            self.log.info("Applied sudo time to trigger on")
        else:
            self.fail("Failed to apply sudo time")

        self.wait_for_on()
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        self.validate_entities_after_restore()
        doc_count_after_restore = self.dataset_count()
        if dataset_count != doc_count_after_restore:
            self.fail("Dataset data count mismatch")

    def initiate_on_off(self):
        # start turning off the columnar instance
        resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
        else:
            self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
        if not self.wait_for_off(timeout=1800):
            self.fail("Failed to turn off instance")

        # start turning on the columnar instance
        resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
        else:
            self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
        if not self.wait_for_on():
            self.fail("Failed to turn on the instance")

    def test_mini_volume_on_off(self):
        start_time = time.time()
        self.base_infra_setup()
        self.sirius_url = self.input.param("sirius_url", "http://127.0.0.1:4000")
        self.mini_volume = MiniVolume(self, self.sirius_url)
        self.mini_volume.calculate_volume_per_source()
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False, True)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)
            self.mini_volume.start_crud_on_data_sources(self.remote_start, self.remote_end)
            self.mini_volume.stop_process()
            self.mini_volume.stop_crud_on_data_sources()
            self.cbas_util.wait_for_ingestion_complete()
            docs_in_collections_before = self.dataset_count()
            self.initiate_on_off()
            self.update_columnar_instance_obj(self.pod, self.tenant, self.cluster)
            docs_in_collection_after = self.dataset_count()
            if docs_in_collection_after != docs_in_collections_before:
                self.fail("Doc count mismatch after on/off")
        self.log.info("Time taken to run mini-volume: {} minutes".format((time.time() - start_time)/60))
