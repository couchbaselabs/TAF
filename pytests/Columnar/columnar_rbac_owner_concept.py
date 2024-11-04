import random
import string
from datetime import datetime, timedelta

from Columnar.columnar_base import ColumnarBaseTest
from Jython_tasks.sirius_task import CouchbaseUtil
from capella_utils.columnar import ColumnarUtils, ColumnarRBACUtil
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from sirius_client_framework.sirius_constants import SiriusCodes


def generate_random_password(length=12):
    """Generate a random password."""
    password_characters = string.ascii_letters + string.digits
    password = ''.join(random.choice(password_characters) for _ in range(length))
    password += "!123Aa"
    return password


def generate_random_entity_name(length=5, object_type="database"):
    """Generate random database name."""
    base_name = "TAF-" + object_type
    entity_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
    entity_name = base_name + "-" + entity_id
    return entity_name


def generate_create_view_cmd(view_full_name, view_defn, if_not_exists=False):
    cmd = "create analytics view {0}".format(view_full_name)

    if if_not_exists:
        cmd += " If Not Exists"

    cmd += " as {0};".format(view_defn)

    return cmd


class ColumnarRBACOwnerConcept(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None

    def setUp(self):
        super(ColumnarRBACOwnerConcept, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = None
        self.cluster_backup_restore = self.input.param("backup_restore", False)
        self.cluster_on_off = self.input.param("on_off", False)
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.database_privileges = ["link_describe", "link_create_collection", "link_copy_from", "link_copy_to",
                                    "link_disconnect", "link_connect", "link_alter", "link_drop", "link_create",
                                    "database_drop", "database_create", "scope_drop", "scope_create",
                                    "collection_analyze", "collection_delete", "collection_upsert", "collection_insert",
                                    "collection_select", "collection_drop", "collection_create", "synonym_drop",
                                    "synonym_create", "view_select", "view_drop", "view_create", "function_execute",
                                    "function_drop", "function_create", "index_drop", "index_create"]
        self.sink_s3_bucket_name = None
        self.capellaAPIv2 = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key,
                                         self.tenant.api_access_key, self.tenant.user,
                                         self.tenant.pwd)
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')
        self.columnar_utils = ColumnarUtils(self.log)
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        if self.perform_columnar_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                self.cbas_util.cleanup_cbas(instance)
        if hasattr(self, "remote_cluster"):
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)
        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(
            self.__class__.__name__, "Finished", stage="Teardown")

    def update_user(self, user, privileges=[], resources=[], resource_type="instance"):
        if len(resources) == 0:
            resources.append("")

        for idx, res in enumerate(resources):
            resources[idx] = self.cbas_util.unformat_name(res)

        resource_privilege_map = []
        for res in resources:
            res_privilege_obj = {
                "name": res,
                "type": resource_type,
                "privileges": privileges
            }
            resource_privilege_map.append(res_privilege_obj)
        privileges_payload = self.columnar_rbac_util.create_privileges_payload(resource_privilege_map)
        return self.columnar_rbac_util.update_api_keys(self.pod, self.tenant,
                                                       self.tenant.project_id, self.columnar_cluster,
                                                       user_id=user.id,
                                                       username=user.username,
                                                       password=user.password,
                                                       privileges_payload=privileges_payload)

    def create_user(self, privileges=[], resources=[],
                    resource_type="instance", username=None):
        if len(resources) == 0:
            resources.append("")

        for idx, res in enumerate(resources):
            resources[idx] = self.cbas_util.unformat_name(res)
        if not username:
            username = generate_random_entity_name(object_type="user")
        password = generate_random_password()
        resource_privilege_map = []
        for res in resources:
            res_privilege_obj = {
                "name": res,
                "type": resource_type,
                "privileges": privileges
            }
            resource_privilege_map.append(res_privilege_obj)

        privileges_payload = self.columnar_rbac_util.create_privileges_payload(resource_privilege_map)
        user = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                       self.tenant.project_id, self.columnar_cluster,
                                                       username=username,
                                                       password=password,
                                                       privileges_payload=privileges_payload)
        return user

    def delete_user(self, user):
        self.columnar_rbac_util.delete_api_keys(self.pod, self.tenant, self.tenant.project_id,
                                                self.columnar_cluster, user.id)

    def load_data_to_source(self, remote_start, remote_end, username=None, password=None):
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
                    self.no_of_docs, self.doc_size, username=username, password=password):
                return False
        return True

    def create_backup_wait_for_complete(self, retention=0, timeout=3600):
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

    def restore_wait_for_complete(self, backup_id, timeout=3600):
        self.log.info("Restoring backup")
        resp = self.columnar_utils.restore_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster,
            backup_id=backup_id)

        if resp is None:
            self.fail("Unable to start restore")

    def validate_owner(self, username, num_datasets=0, num_links=0, num_synonyms=0, num_index=0, num_databases=0,
                       num_dataverse=0, num_views=0):
        # check for owners in metadata entries
        # fetch all datasets and compare
        if num_datasets:
            dataset_query = "select * from Metadata.`Dataset`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, dataset_query, mode="immediate", timeout=300,
                analytics_timeout=300)

            results = [x for x in results if x['Dataset']['Creator']['Name'] == username]
            if not len(results) == num_datasets:
                self.fail("not all datasets present with the correct owner")

        if num_links:
            link_query = "select * from Metadata.`Link`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, link_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            results = [x for x in results if x['Link']['Creator']['Name'] == username]
            if not len(results) == num_links:
                self.fail("not all links present with the correct owner")

        if num_synonyms:
            synonyms_query = "select * from Metadata.`Synonym`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, synonyms_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            results = [x for x in results if x['Synonym']['Creator']['Name'] == username]
            if not len(results) == num_synonyms:
                self.fail("not all synonyms present with the correct owner")

        if num_index:
            indexes_query = "select * from Metadata.`Index`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, indexes_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            results = [x for x in results if x['Index']['Creator']['Name'] == username]
            if not len(results) == num_index:
                self.fail("not all indexes present with the correct owner")

        if num_databases:
            databases_query = "select * from Metadata.`Database`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, databases_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            results = [x for x in results if x['Database']['Creator']['Name'] == username]
            if not len(results) == num_databases:
                self.fail("not all databases present with the correct owner")

        if num_dataverse:
            dataverse_query = "select * from Metadata.`Dataverse`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, dataverse_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            results = [x for x in results if x['Dataverse']['Creator']['Name'] == username]
            if not len(results) == num_dataverse:
                self.fail("not all dataverses present with the correct owner")

        if num_views:
            views_query = "select * from Metadata.`View`"
            status, _, _, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, views_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            results = [x for x in results if x['View']['Creator']['Name'] == username]
            if not len(results) == num_views:
                self.fail("not all views present with the correct owner")

    def test_create_object_super_user(self):
        self.log.info("RBAC test for user creation, delete and re-create started")

        # create columnar entities using a user with privileges
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        user = self.create_user(self.database_privileges, [], "instance")

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
            remote_clusters=[self.remote_cluster], username=user.username, password=user.password)

        if not result:
            self.fail(msg)

        # load data in collections using super-user
        if not self.load_data_to_source(0, self.no_of_docs):
            self.fail("Owner not able to execute query on datasets")

        # validate owner in Metadata
        total_datasets = (self.input.param("num_standalone_collections", 0) +
                          self.input.param("num_external_collections", 0) +
                          self.input.param("num_remote_collections", 0))
        total_links = (self.input.param("num_external_links", 0) +
                       self.input.param("num_remote_links", 0) +
                       self.input.param("num_kafka_links", 0))

        # validate owner of created objects and entities
        self.validate_owner(user.username, total_datasets, total_links, self.input.param("num_synonyms", 0),
                            self.input.param("num_indexes", 0) + total_datasets - self.input.param(
                                "num_external_collections", 0),
                            self.input.param("num_db", 0) - 1, self.input.param("num_dv", 0))

        # create objects using super-user in dataverse and databases and links created by user
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])
        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]
        self.columnar_spec["database"]["no_of_databases"] = 1
        self.columnar_spec["dataverse"]["no_of_dataverses"] = 1

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster], username=user.username, password=user.password)

        if not self.load_data_to_source(self.no_of_docs + 1, self.no_of_docs * 2):
                self.fail("Super-user not able to execute query on datasets")

        if not result:
            self.fail(msg)

        # delete object and disconnect links using super-user
        if self.perform_columnar_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                if not self.cbas_util.cleanup_cbas(instance, username=user.username, password=user.password):
                    self.fail("Super Owner not able to drop columnar entities")

    def test_create_user_recreate_user(self):
        self.log.info("RBAC test for user creation, delete and re-create started")

        # create columnar entities using a user with privileges
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        user = self.create_user(self.database_privileges, [], "instance")

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
            remote_clusters=[self.remote_cluster], username=user.username, password=user.password)

        if not result:
            self.fail(msg)

        # load data in collections
        if not self.load_data_to_source(0, self.no_of_docs, user.username, user.password):
                self.fail("Owner not able to execute query on datasets")

        # validate owner in Metadata
        total_datasets = (self.input.param("num_standalone_collections", 0) +
                          self.input.param("num_external_collections", 0) +
                          self.input.param("num_remote_collections", 0))
        total_links = (self.input.param("num_external_links", 0) +
                       self.input.param("num_remote_links", 0) +
                       self.input.param("num_kafka_links", 0))

        if self.cluster_on_off:
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

        if self.cluster_backup_restore:
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

        # validate owner of created objects and entities
        self.validate_owner(user.username, total_datasets, total_links, self.input.param("num_synonyms", 0),
                            self.input.param("num_indexes", 0) + total_datasets - self.input.param("num_external_collections", 0),
                            self.input.param("num_db", 0) - 1, self.input.param("num_dv", 0))

        # remove privileges and roles from user. The user should only honour owner concept
        if not self.update_user(user, [], [], "instance"):
            self.fail("Failed to update api user privileges")
        # load data in collection using the user to honour owner concept
        if not self.load_data_to_source(self.no_of_docs + 1, self.no_of_docs * 2, user.username, user.password):
            self.fail("Owner not able to execute query on datasets")

        if self.perform_columnar_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                if not self.cbas_util.cleanup_cbas(instance, username=user.username, password=user.password):
                    self.fail("Failed to drop columnar entities")

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster], username=user.username, password=user.password)

        if not result:
            self.fail(msg)

        # delete user and re-create with same username
        self.delete_user(user)
        user = self.create_user([], [], "instance", username=user.username)
        dataset_objects = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in dataset_objects:
            document = self.cbas_util.generate_docs(document_size=self.doc_size)
            if not self.cbas_util.insert_into_standalone_collection(self.columnar_cluster, dataset.name, document,
                                                                dataset.dataverse_name, dataset.database_name,
                                                                validate_error_msg=True,
                                                                expected_error="Insufficient permissions or the "
                                                                               "requested object does not exist",
                                                                expected_error_code=20001, username=user.username,
                                                                password=user.password):
                self.fail("Non-Owner with same username able to execute query on datasets without privileges")
        if self.perform_columnar_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                if self.cbas_util.cleanup_cbas(instance, username=user.username, password=user.password):
                    self.fail("Non-Owner with same username able to drop columnar entities")

    def test_backward_compatibility(self):

        # create some entities on 1.0.4 clusters
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        user = self.create_user(self.database_privileges, [], "instance")

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
            remote_clusters=[self.remote_cluster], username=user.username, password=user.password)

        if not result:
            self.fail(msg)

        # start upgrade to Ionic and above
        upgrade_start_time = datetime.utcnow() + timedelta(minutes=1)
        queue_time = upgrade_start_time + timedelta(minutes=2)
        upgrade_end_time = upgrade_start_time + timedelta(hours=2)
        resp = self.capellaAPI.schedule_cluster_upgrade(
            current_images=[self.input.param("columnar_image")],
            new_image=self.input.param("upgrade_version"),
            start_datetime=upgrade_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_datetime=upgrade_end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            queue_datetime=queue_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            provider="hostedAWS",
            cluster_ids=[self.columnar_cluster.cluster_id])
        if resp.status_code == 202:
            upgrade_info = resp.json()
        else:
            self.fail(f"Failed to schedule columnar upgrade. Error - "
                      f"{resp.content}")

        self.log.info("Waiting for columnar cluster upgrades to finish")
        if not self.columnar_utils.wait_for_maintenance_job_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                maintenance_job_id=upgrade_info["id"], timeout=7200
        ):
            self.fail("Upgrade failed.")

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
            remote_clusters=[self.remote_cluster], username=user.username, password=user.password)

        if not result:
            self.fail(msg)

        dataset_objects = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in dataset_objects:
            document = self.cbas_util.generate_docs(document_size=self.doc_size)
            if not self.cbas_util.insert_into_standalone_collection(self.columnar_cluster, dataset.name, document,
                                                                    dataset.dataverse_name, dataset.database_name,
                                                                    validate_error_msg=True,
                                                                    expected_error="Insufficient permissions or the "
                                                                                   "requested object does not exist",
                                                                    expected_error_code=20001, username=user.username,
                                                                    password=user.password):
                self.fail("Non-Owner with same username able to execute query on datasets without privileges")

        if self.perform_columnar_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                if not self.cbas_util.cleanup_cbas(instance, username=user.username, password=user.password):
                    self.fail("Owner un-able to drop columnar entities")

    def test_links_owner_concept(self):
        pass

    def test_database_owner_create_object(self):
        user = self.create_user(["database_drop", "database_create"], [], "instance")

        # create database
        if not self.cbas_util.create_database(self.columnar_cluster, database_name="owner_database", username=user.username,
                                       password=user.password):
            self.fail("Failed to create database with privilege [\"database_drop\", \"database_create\"")

        if not self.update_user(user, [], [], "instance"):
            self.fail("Failed to update api user privileges")

        if (not self.cbas_util.create_scope(self.columnar_cluster, cbas_scope_name="owner_scope", database_name="owner_database",
                                    username=user.username, password=user.password) and
                self.cbas_util.create_standalone_collection(self.columnar_cluster, "owner_collection",
                                                            dataverse_name="owner_scope", database_name="owner_database",
                                                            username=user.username, password=user.password)):
            self.fail("Owner of database failed to create scopes and collections")

        if not self.load_data_to_source(0, 0, user.username, password=user.password):
            self.fail("Database owner failed to run query on collections")

        if not self.validate_owner(user.username, 1, 0, 0, 0, 1, 1, 0):
            self.fail("Owner of all the objects are not the database owner")

        for instance in self.tenant.columnar_instances:
            if not self.cbas_util.cleanup_cbas(instance, username=user.username, password=user.password):
                self.fail("Database Owner not able to drop columnar entities")

    def test_non_database_owner_create_object(self):
        # not able to create scopes and collections
        user = self.create_user(["database_drop", "database_create"], [], "instance")

        # create database
        if not self.cbas_util.create_database(self.columnar_cluster, database_name="owner_database",
                                              username=user.username,
                                              password=user.password):
            self.fail("Failed to create database with privilege [\"database_drop\", \"database_create\"")

        user = self.create_user(["database_drop", "database_create"], [], "instance")

        if ( self.cbas_util.create_scope(self.columnar_cluster, cbas_scope_name="owner_scope",
                                            database_name="owner_database",
                                            username=user.username, password=user.password) or
                self.cbas_util.create_standalone_collection(self.columnar_cluster, "owner_collection",
                                                            dataverse_name="owner_scope",
                                                            database_name="owner_database",
                                                            username=user.username, password=user.password)):
            self.fail("Non-Owner of database able to create scopes and collections")
