from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
import time
from Columnar.mini_volume_code_template import MiniVolume


class BackupRestore(ColumnarBaseTest):
    def setUp(self):
        super(BackupRestore, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]

        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.remote_cluster = None

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        # add code to delete buckets in provisioned instance
        if hasattr(self, "mini_volume"):
            self.mini_volume.stop_process()
            self.mini_volume.stop_crud_on_data_sources()
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
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"id": "string"}]

        if not hasattr(self, "remote_cluster"):
            remote_cluster = None
        else:
            remote_cluster = [self.remote_cluster]
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=remote_cluster)
        if not result:
            self.fail(msg)

    def get_backup_from_backup_lists(self, backup_id=None):
        """
        Returns backup info for a backup id
        Returns all backups for instance if backup_id is None
        Parameters:
            backup_id: Optional, fetch the backup info for backup_id
        """
        resp = self.columnarAPI.list_backups(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        backup_list = resp.json()["data"]
        if not backup_id:
            return backup_list
        else:
            for backup in backup_list:
                if backup["data"]["id"] == backup_id:
                    return backup["data"]
        return -1

    def get_restore_from_restore_list(self, backup_id, restore_id=None):
        """
        Returns restore info for a restore id
        Returns all restore for instance if restore is None
        Parameters:
            backup_id: fetch the backup info for backup_id
            restore_id: Optional, only provide when to fetch specific restore
        """
        resp = self.columnarAPI.list_restores(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        restore_list = resp.json()["data"]
        if not restore_id:
            return restore_list
        else:
            for restore in restore_list:
                if restore["data"]["id"] == restore_id:
                    return restore["data"]
        return -1

    def dataset_count(self):
        items_in_datasets = {}
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            items_in_datasets[dataset.full_name] = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster,
                                                                                                dataset.full_name)
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

    def create_backup_wait_for_complete(self, retention=None, timeout=3600):
        if retention:
            resp = self.columnarAPI.create_backup(self.tenant.id, self.tenant.project_id, self.cluster.instance_id,
                                                  retention)
        else:
            resp = self.columnarAPI.create_backup(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        backup_id = resp.json()["id"]
        self.log.info("Backup Id: {}".format(backup_id))

        # wait for backup to complete
        start_time = time.time()
        backup_state = None
        while backup_state != "complete" and time.time() < start_time + timeout:
            backup_state = self.get_backup_from_backup_lists(backup_id)
            if backup_state == -1:
                self.fail("Backup with backup id: {0}, Not found".format(backup_id))
            backup_state = self.get_backup_from_backup_lists(backup_id)["progress"]["status"]
            self.log.info("Waiting for backup to be completed, current state: {}".format(backup_state))
            time.sleep(60)
        if backup_state != "complete":
            self.fail("Failed to create backup with timeout of {}".format(timeout))
        else:
            self.log.info("Successfully created backup in {} seconds".format(time.time() - start_time))
        return backup_id

    def restore_wait_for_complete(self, backup_id, timeout=3600):
        resp = self.columnarAPI.create_restore(self.tenant.id, self.tenant.project_id, self.cluster.instance_id, backup_id)
        restore_id = resp.json()["id"]
        start_time = time.time()
        self.log.info("Restore Id: {}".format(restore_id))
        restore_state = None
        while restore_state != "complete" and time.time() < start_time + timeout:
            restore_state = self.get_restore_from_restore_list(backup_id, restore_id)["status"]
            if restore_state == -1:
                self.fail("Restore id: {0} not found for backup id: {1}".format(restore_id, backup_id))
            time.sleep(60)
        if restore_state != "complete":
            self.fail("Fail to restore backup with timeout of {}".format(timeout))
        else:
            self.log.info("Successfully restored backup in {} seconds".format(time.time() - start_time))

    def scale_columnar_cluster(self, nodes, timeout=3600):
        start_time = time.time()
        status = None
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id,
                                                         self.tenant.project_id,
                                                         self.cluster.instance_id,
                                                         self.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
            # check for nodes in the cluster
        while status != "healthy" and start_time + timeout > time.time():
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                                   self.tenant.project_id,
                                                                   self.cluster.instance_id)
            resp = resp.json()
            status = resp["data"]["state"]
        if time.time() > start_time + timeout:
            self.log.error("Cluster state is {} after 15 minutes".format(status))

        while start_time + timeout > time.time():
            servers = self.get_nodes(self.cluster)
            nodes_in_cluster = len(servers)
            if nodes_in_cluster == nodes:
                return True
            self.log.info("Waiting for server map to get updated")
            time.sleep(20)
        return False

    def load_data_to_source(self, remote_start, remote_end, standalone_start, standalone_end):
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

    def wait_for_instance_to_be_healthy(self, timeout=600):
        status = None
        start_time = 600
        while (not status or status != "healthy") and time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                                               self.tenant.project_id,
                                                                               self.cluster.instance_id)
            resp = resp.json()
            status = resp["data"]["state"]
            self.log.info("Instance state: {}".format(status))
            time.sleep(30)
        if status != "healthy":
            self.fail("Instance failed to be healthy")
        else:
            self.log.info("Instance is in healthy state")

    def test_backup_restore(self):
        self.base_infra_setup()
        self.load_data_to_source(0, self.no_of_docs, 1, self.no_of_docs)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count = self.dataset_count()
        backup_id = self.create_backup_wait_for_complete()
        dataset_count_after_backup = self.dataset_count()
        if dataset_count != dataset_count_after_backup:
            self.fail("Data missing after backup")

        # restore from backup
        self.restore_wait_for_complete(backup_id)
        self.wait_for_instance_to_be_healthy()
        # validate data after restore
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count_after_restore = self.dataset_count()
        if dataset_count != dataset_count_after_restore:
            self.fail("Data mismatch after restore")
        self.validate_entities_after_restore()

    def test_backup_restore_with_scaling(self):
        scale_stage = self.input.param("scale_stage")
        scale_nodes = self.input.param("scale_nodes")
        self.base_infra_setup()
        self.load_data_to_source(0, self.no_of_docs, 1, self.no_of_docs)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        dataset_count = self.dataset_count()
        if scale_stage == "before_backup":
            self.scale_columnar_cluster(scale_nodes)
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            self.wait_for_instance_to_be_healthy()

        if scale_stage == "during_backup":
            resp = self.columnarAPI.create_backup(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            backup_id = resp.json()["id"]
            self.log.info("Backup Id: {}".format(backup_id))
            backup_state = None
            while not backup_state == "pending":
                backup_state = self.get_backup_from_backup_lists(backup_id)
                if backup_state == -1:
                    self.fail("Backup with backup id: {0}, Not found".format(backup_id))
            self.scale_columnar_cluster(scale_nodes)
            while backup_state != "complete":
                backup_state = self.get_backup_from_backup_lists(backup_id)
                if backup_state == -1:
                    self.fail("Backup with backup id: {0}, Not found".format(backup_id))
                backup_state = self.get_backup_from_backup_lists(backup_id)["progress"]["status"]
                self.log.info("Waiting for backup to be completed, current state: {}".format(backup_state))
            self.restore_wait_for_complete(backup_id)
            self.wait_for_instance_to_be_healthy()

        if scale_stage == "after_backup" or scale_stage == "before_resume":
            backup_id = self.create_backup_wait_for_complete()
            self.scale_columnar_cluster(scale_nodes)
            self.restore_wait_for_complete(backup_id)
            self.wait_for_instance_to_be_healthy()

        if scale_stage == "after_resume":
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            self.wait_for_instance_to_be_healthy()
            self.scale_columnar_cluster(scale_nodes)

        for collection in remote_datasets:
            self.cbas_util.wait_for_ingestion_complete(self.cluster, collection.full_name, self.no_of_docs)
        self.validate_entities_after_restore()
        doc_count_after_restore = self.dataset_count()
        if dataset_count != doc_count_after_restore:
            self.fail("Dataset data count mismatch")

    def test_backup_retention_time(self):
        backup_time = self.input.param("backup_retention_time", 168)
        if backup_time != 168:
            backup_id = self.create_backup_wait_for_complete(backup_time)
        else:
            backup_id = self.create_backup_wait_for_complete()
        backup_info = self.get_backup_from_backup_lists(backup_id)
        if backup_info["retention"] != backup_time:
            self.fail("Backup retention time mismatch, expected {}, actual {}".
                      format(backup_time, backup_info["retention"]))

    def test_mini_volume_backup_restore(self):
        self.base_infra_setup()
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
            self.cbas_util.wait_for_data_ingestion_in_the_collections()
            count_before_backup = self.dataset_count()
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            self.wait_for_instance_to_be_healthy()
            dataset_count_after_restore = self.dataset_count()
            if count_before_backup != dataset_count_after_restore:
                self.fail("Data mismatch after restore")
            self.validate_entities_after_restore()

        # A successful run
        self.log.info("Mini-Volume for backup-restore finished")
