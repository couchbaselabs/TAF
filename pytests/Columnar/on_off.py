import time
from datetime import datetime
import pytz
from datetime import timedelta

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from Columnar.mini_volume_code_template import MiniVolume

# External Database loader related imports
from Jython_tasks.sirius_task import CouchbaseUtil
from sirius_client_framework.sirius_constants import SiriusCodes


class OnOff(ColumnarBaseTest):
    def setUp(self):
        super(OnOff, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]

        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')

        self.no_of_docs = self.input.param("no_of_docs", 1000)
        
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

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
        if hasattr(self, "remote_cluster"):
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)
        
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(
            self.__class__.__name__, "Finished", stage="Teardown")

    def load_data_to_source(self, remote_start, remote_end):
        if hasattr(self, "remote_cluster"):
            for remote_bucket in self.remote_cluster.buckets:
                for scope_name, scope in remote_bucket.scopes.items():
                    if scope_name != "_system" and scope != "_mobile":
                        for collection_name, collection in (
                                scope.collections.items()):
                            self.log.info(
                                f"Loading docs in {remote_bucket.name}."
                                f"{scope_name}.{collection_name}")
                            cb_doc_loading_task = self.couchbase_doc_loader.load_docs_in_couchbase_collection(
                                bucket=remote_bucket.name, scope=scope_name,
                                collection=collection_name, start=remote_start,
                                end=remote_end,
                                doc_template=SiriusCodes.Templates.PRODUCT,
                                doc_size=self.doc_size, sdk_batch_size=1000
                            )
                            if not cb_doc_loading_task.result:
                                self.fail(
                                    f"Failed to load docs in couchbase collection "
                                    f"{remote_bucket.name}.{scope_name}.{collection_name}")
                            else:
                                collection.num_items = cb_doc_loading_task.success_count

        standalone_collections = self.cbas_util.get_all_dataset_objs(
            "standalone")
        for collection in standalone_collections:
            if not self.cbas_util.load_doc_to_standalone_collection(
                    self.columnar_cluster, collection.name,
                    collection.dataverse_name, collection.database_name,
                    self.no_of_docs, self.doc_size):
                self.fail(f"Failed to insert docs into standalone collection "
                          f"{collection.full_name}")

    def dataset_count(self, timeout=3600):
        items_in_datasets = {}
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            items_in_datasets[dataset.full_name] = self.cbas_util.get_num_items_in_cbas_dataset(
                self.columnar_cluster, dataset.full_name, timeout=timeout,
                analytics_timeout=timeout)
        return items_in_datasets

    def test_on_demand_on_off(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])
        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        dataset_count = self.dataset_count()

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        # resume the instance
        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)

    def test_on_demand_off_after_scaling_and_scale_after_resume(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])
        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        dataset_count = self.dataset_count()

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 8):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 8))

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster):
            self.fail("Failed to scale OUT instance even after 3600 seconds")

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        # resume instance
        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 2):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 8))

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster):
            self.fail("Failed to scale OUT instance even after 3600 seconds")

        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)

    def test_off_during_scale(self):
        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 32):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 32))
        # off during scale
        else:
            resp = self.columnarAPI.turn_off_instance(
                self.tenant.id, self.tenant.project_id,
                self.columnar_cluster.instance_id)
            if resp.status_code != 422:
                if resp["errorType"] != "EntityStateInvalid":
                    self.fail("Status code and errorType mismatch")

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster):
            self.fail("Failed to scale instance even after 3600 seconds")

    def test_scale_during_resume(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id,
                self.columnar_cluster, 32):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(len(self.columnar_cluster.nodes_in_cluster), 4))

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

    def test_off_during_off(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=False):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=False):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.wait_for_instance_to_turn_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

    def test_on_during_on(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=False):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=False):
            self.fail("Failed to Turn-On the cluster")

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

    def test_on_during_off(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=False):
            self.fail("Failed to Turn-Off the cluster")

        resp = self.columnarAPI.turn_on_instance(
            self.tenant.id, self.tenant.project_id,
            self.columnar_cluster.instance_id)
        if resp.status_code == 500:
            if not self.columnar_utils.wait_for_instance_to_turn_off(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, timeout=3600):
                self.fail("Failed to turn on the instance")
            if not self.columnar_utils.turn_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_on=True):
                self.fail("Failed to Turn-On the cluster")
            self.fail("Failed due to bug")

        self.log.info("Bug fixed")

        if not self.columnar_utils.wait_for_instance_to_turn_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
            self.fail("Failed to turn on the instance")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

    def test_off_during_on(self):
        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=False):
            self.fail("Failed to Turn-On the cluster")

        resp = self.columnarAPI.turn_off_instance(
            self.tenant.id, self.tenant.project_id,
            self.columnar_cluster.instance_id)
        if resp.status_code == 500:
            if not self.columnar_utils.wait_for_instance_to_turn_on(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, timeout=3600):
                self.fail("Failed to turn on the instance")
            self.fail("Failed due to bug")

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=3600):
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
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])
        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        dataset_count = self.dataset_count()

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

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

        if not self.columnar_utils.create_schedule_on_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, days=data,
                timezone=used_timezone):
            self.fail("Failed to Turn-Off the cluster")

        internal_support_token = self.capella.get("override_token")
        columnar_internal = ColumnarAPI(
            self.pod.url_public, '', '', self.tenant.user,
            self.tenant.pwd, internal_support_token)
        resp = columnar_internal.set_trigger_time_for_onoff(
            next_friday_1630_in_utc, [self.columnar_cluster.instance_id])
        if resp.status_code == 200:
            self.log.info("Applied sudo time to trigger off")
        else:
            self.fail("Failed to apply sudo time")

        if not self.columnar_utils.wait_for_instance_to_turn_off(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=600):
            self.fail("Failed to turn off the instance")

        resp = columnar_internal.set_trigger_time_for_onoff(
            next_friday_1030_in_utc, [self.columnar_cluster.instance_id])
        if resp.status_code == 200:
            self.log.info("Applied sudo time to trigger on")
        else:
            self.fail("Failed to apply sudo time")

        if not self.columnar_utils.wait_for_instance_to_turn_on(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=600):
            self.fail("Failed to turn off the instance")

        for collection in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items):
                self.fail(
                    f"FAILED: Initial ingestion into {collection.full_name}.")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)

        doc_count_after_restore = self.dataset_count()
        if dataset_count != doc_count_after_restore:
            self.fail("Dataset data count mismatch")

    def test_mini_volume_on_off(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])
        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        start_time = time.time()
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
            self.cbas_util.disconnect_links(self.columnar_cluster, self.columnar_spec)
            self.cbas_util.connect_links(self.columnar_cluster, self.columnar_spec)
            # start turning off the columnar instance
            if not self.columnar_utils.turn_off_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_off=True):
                self.fail("Failed to Turn-Off the cluster")

            # start turning on the columnar instance
            if not self.columnar_utils.turn_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_on=False):
                self.fail("Failed to Turn-On the cluster")
            self.columnar_utils.update_columnar_instance_obj(
                self.pod, self.tenant, self.columnar_cluster)
            docs_in_collection_after = self.dataset_count()
            if docs_in_collection_after != docs_in_collections_before:
                self.fail("Doc count mismatch after on/off")
        self.log.info("Time taken to run mini-volume: {} minutes".format((time.time() - start_time)/60))
