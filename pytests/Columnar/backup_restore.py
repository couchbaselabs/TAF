import time

from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from Columnar.columnar_base import ColumnarBaseTest
from Columnar.mini_volume_code_template import MiniVolume
from datetime import datetime, timezone, timedelta

# External Database loader related imports
from Jython_tasks.sirius_task import CouchbaseUtil
from sirius_client_framework.sirius_constants import SiriusCodes


class BackupRestore(ColumnarBaseTest):
    def setUp(self):
        super(BackupRestore, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]

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
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        # add code to delete buckets in provisioned instance
        if hasattr(self, "mini_volume"):
            self.mini_volume.stop_process()
            self.mini_volume.stop_crud_on_data_sources()
        if hasattr(self, "remote_cluster"):
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)

        # delete all created backups
        backups = self.columnar_utils.list_backups(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster)
        for backup in backups:
            self.columnar_utils.delete_backup(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, backup_id=backup["data"]["id"])

        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def dataset_count(self):
        items_in_datasets = {}
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            items_in_datasets[
                dataset.full_name] = self.cbas_util.get_num_items_in_cbas_dataset(
                self.columnar_cluster, dataset.full_name)
        return items_in_datasets

    def create_backup_wait_for_complete(self, retention=0, timeout=7200):
        self.log.info("Starting backup")
        resp = self.columnar_utils.create_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id,
            instance=self.columnar_cluster, retention_time=retention)
        if resp is None:
            self.fail("Unable to schedule backup")
        else:
            backup_id = resp["id"]

        self.log.info("Backup Id: {}".format(backup_id))
        if not self.columnar_utils.wait_for_backup_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                backup_id=backup_id, timeout=timeout):
            self.fail("Backup failed.")
        return backup_id

    def restore_wait_for_complete(self, backup_id, timeout=7200):
        self.log.info("Restoring backup")
        resp = self.columnar_utils.restore_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster,
            backup_id=backup_id)

        if resp is None:
            self.fail("Unable to start restore")
        else:
            restore_id = resp["id"]

        if not self.columnar_utils.wait_for_restore_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                restore_id=restore_id, timeout=timeout):
            self.fail("Unable to restore backup taken before the upgrade.")

    def scale_columnar_cluster(self, nodes, timeout=3600):
        self.log.info("Scaling columnar cluster")
        if not self.columnar_utils.scale_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                nodes=nodes):
            self.fail("Unable to initiate cluster scale operation")

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, timeout=timeout,
                verify_with_backend_cluster=True,
                expected_num_of_nodes=nodes):
            self.fail("Scaling operation failed")

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

    def test_backup_restore(self):
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

        backup_id = self.create_backup_wait_for_complete()

        dataset_count_after_backup = self.dataset_count()
        if dataset_count != dataset_count_after_backup:
            self.fail("Data missing after backup")

        # restore from backup
        self.restore_wait_for_complete(backup_id)

        if not self.columnar_utils.wait_for_instance_to_be_healthy(
                pod=self.pod, tenant=self.tenant,
                instance=self.columnar_cluster):
            self.fail("Cluster is not is healthy state")

        if not self.columnar_utils.allow_ip_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster):
            self.fail("Unable to set Allowed IP post restoring backup")

        if not self.cbas_util.wait_for_cbas_to_recover(
                self.columnar_cluster, timeout=600):
            self.fail("Columnar cluster unable to recover after restoring "
                      "backup.")

        # validate data after restore
        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

    def test_backup_restore_with_scaling(self):
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

        scale_stage = self.input.param("scale_stage")
        scale_nodes = self.input.param("scale_nodes")

        if scale_stage == "before_backup":
            self.scale_columnar_cluster(scale_nodes)
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            if not self.columnar_utils.wait_for_instance_to_be_healthy(
                    pod=self.pod, tenant=self.tenant,
                    instance=self.columnar_cluster):
                self.fail("Cluster is not is healthy state")

        if scale_stage == "during_backup":
            resp = self.columnar_utils.create_backup(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster)
            if resp is None:
                self.fail("Unable to schedule backup")
            else:
                backup_id = resp["id"]

            self.log.info("Backup Id: {}".format(backup_id))
            backup_state = None
            while not backup_state == "pending":
                backup_info = self.columnar_utils.get_backup_info(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, backup_id=backup_id
                )
                if not backup_info:
                    self.fail("Backup with backup id: {0}, Not found".format(
                        backup_id))

                backup_state = backup_info["progress"]["status"]
            self.scale_columnar_cluster(scale_nodes)
            while backup_state != "complete":
                backup_info = self.columnar_utils.get_backup_info(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, backup_id=backup_id
                )
                if not backup_info:
                    self.fail("Backup with backup id: {0}, Not found".format(
                        backup_id))

                backup_state = backup_info["progress"]["status"]
                self.log.info("Waiting for backup to be completed, current "
                              "state: {}".format(backup_state))
                time.sleep(20)
            self.restore_wait_for_complete(backup_id)
            if not self.columnar_utils.wait_for_instance_to_be_healthy(
                    pod=self.pod, tenant=self.tenant,
                    instance=self.columnar_cluster):
                self.fail("Cluster is not is healthy state")

        if scale_stage == "after_backup" or scale_stage == "before_resume":
            backup_id = self.create_backup_wait_for_complete()
            self.scale_columnar_cluster(scale_nodes)
            self.restore_wait_for_complete(backup_id)
            if not self.columnar_utils.wait_for_instance_to_be_healthy(
                    pod=self.pod, tenant=self.tenant,
                    instance=self.columnar_cluster):
                self.fail("Cluster is not is healthy state")

        if scale_stage == "after_resume":
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            if not self.columnar_utils.wait_for_instance_to_be_healthy(
                    pod=self.pod, tenant=self.tenant,
                    instance=self.columnar_cluster):
                self.fail("Cluster is not is healthy state")
            if not self.columnar_utils.allow_ip_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster):
                self.fail("Unable to set Allowed IP post restoring backup")
            self.scale_columnar_cluster(scale_nodes)
            time.sleep(120)

        if not self.columnar_utils.allow_ip_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster):
            self.fail("Fail to allow IP on instance")

        if not self.cbas_util.wait_for_cbas_to_recover(
                self.columnar_cluster, timeout=600):
            self.fail("Columnar cluster unable to recover after restoring "
                      "backup.")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)

        doc_count_after_restore = self.dataset_count()
        if dataset_count != doc_count_after_restore:
            self.fail("Dataset data count mismatch")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

    def test_backup_retention_time(self):
        backup_time = self.input.param("backup_retention_time", 168)
        if backup_time != 168:
            backup_id = self.create_backup_wait_for_complete(backup_time)
        else:
            backup_id = self.create_backup_wait_for_complete()

        backup_info = self.columnar_utils.get_backup_info(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id,
            instance=self.columnar_cluster, backup_id=backup_id
        )
        if not backup_info:
            self.fail("Backup with backup id: {0}, Not found".format(
                backup_id))

        if backup_info["retention"] != backup_time:
            self.fail("Backup retention time mismatch, expected {}, actual {}".
                      format(backup_time, backup_info["retention"]))

    def test_schedule_backup(self):
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

        backup_interval = self.input.param("backup_interval", 24)
        retention = self.input.param("backup_retention", 24)

        self.load_data_to_source(0, self.no_of_docs)

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        if not self.cbas_util.wait_for_data_ingestion_in_the_collections(
                self.columnar_cluster):
            self.fail("Ingestion into analytics collections failed")

        result = self.cbas_util.disconnect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while disconnecting link")

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

        current_utc_time = datetime.now(timezone.utc)
        current_utc_time = current_utc_time.replace(minute=30, second=0, microsecond=0)
        new_utc_time = current_utc_time + timedelta(hours=backup_interval)
        # Format the UTC time as a string in the desired format
        formatted_utc_time = current_utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        formatted_new_utc_time = new_utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        if not self.columnar_utils.create_schedule_backup(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, interval=backup_interval,
                retention=retention, start_time=formatted_utc_time):
            self.fail("Creating scheduled backup failed.")

        time.sleep(60)
        override_token = self.capella["override_token"]
        dataset_count = self.dataset_count()

        columnar_internal = ColumnarAPI(
            self.pod.url_public, '', '', self.tenant.user,
            self.tenant.pwd, override_token)
        resp = columnar_internal.set_trigger_time_for_scheduled_backup(
            formatted_new_utc_time, [self.columnar_cluster.instance_id])
        if resp.status_code == 202:
            self.log.info("Applied sudo time to trigger backup")
        else:
            self.fail("Failed to apply time to trigger backup")
        time.sleep(60)

        backup = self.columnar_utils.list_backups(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster)[0]

        backup_id = backup["data"]["id"]
        self.log.info("Backup Id: {}".format(backup_id))
        if not self.columnar_utils.wait_for_backup_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                backup_id=backup_id, timeout=3600):
            self.fail("Backup failed.")

        self.restore_wait_for_complete(backup_id)

        if not self.columnar_utils.wait_for_instance_to_be_healthy(
                pod=self.pod, tenant=self.tenant,
                instance=self.columnar_cluster):
            self.fail("Cluster is not is healthy state")

        if not self.columnar_utils.allow_ip_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster):
            self.fail("Unable to set Allowed IP post restoring backup")

        if not self.cbas_util.wait_for_cbas_to_recover(
                self.columnar_cluster, timeout=600):
            self.fail("Columnar cluster unable to recover after restoring "
                      "backup.")

        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")

        status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
            self.columnar_cluster)
        if not status:
            self.fail(error)

        result = self.cbas_util.connect_links(
            self.columnar_cluster, self.columnar_spec)
        if not all(result):
            self.fail("Error while connecting link")

    def test_mini_volume_backup_restore(self):
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
            {"id": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        backup_timeout = self.input.param("backup_timeout", 7200)
        self.mini_volume = MiniVolume(self, "http://127.0.0.1:4000")
        self.mini_volume.calculate_volume_per_source()
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)
            self.mini_volume.start_crud_on_data_sources(self.remote_start, self.remote_end)
            self.mini_volume.stop_process()
            self.mini_volume.stop_crud_on_data_sources()

            if not self.cbas_util.wait_for_data_ingestion_in_the_collections(
                    self.columnar_cluster):
                self.fail("Ingestion into analytics collections failed")

            count_before_backup = self.dataset_count()

            result = self.cbas_util.disconnect_links(
                self.columnar_cluster, self.columnar_spec)
            if not all(result):
                self.fail("Error while disconnecting link")

            result = self.cbas_util.connect_links(
                self.columnar_cluster, self.columnar_spec)
            if not all(result):
                self.fail("Error while connecting link")

            backup_id = self.create_backup_wait_for_complete(timeout=backup_timeout)

            self.restore_wait_for_complete(backup_id, timeout=backup_timeout)

            if not self.columnar_utils.wait_for_instance_to_be_healthy(
                    pod=self.pod, tenant=self.tenant,
                    instance=self.columnar_cluster):
                self.fail("Cluster is not is healthy state")

            if not self.columnar_utils.allow_ip_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster):
                self.fail("Unable to set Allowed IP post restoring backup")

            if not self.cbas_util.wait_for_cbas_to_recover(
                    self.columnar_cluster, timeout=600):
                self.fail("Columnar cluster unable to recover after restoring "
                          "backup.")

            dataset_count_after_restore = self.dataset_count()
            if count_before_backup != dataset_count_after_restore:
                self.fail("Data mismatch after restore")

            status, error = self.cbas_util.perform_metadata_validation_for_all_entities(
                self.columnar_cluster)
            if not status:
                self.fail(error)

            result = self.cbas_util.connect_links(
                self.columnar_cluster, self.columnar_spec)
            if not all(result):
                self.fail("Error while connecting link")

        # A successful run
        self.log.info("Mini-Volume for backup-restore finished")
