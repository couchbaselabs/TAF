"""
Created on 2025
@author: Anisha Sinha
"""
import time
from deepdiff import DeepDiff

from CbasLib.CBASOperations import CBASHelper
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.sirius_task import CouchbaseUtil
from pytests.Columnar.columnar_base import ColumnarBaseTest


class SingleMessageTransformationTest(ColumnarBaseTest):
    """
    Test class for single message transformation testing
    """
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    def setUp(self):
        super(SingleMessageTransformationTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = None
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )
        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.remote_collection_list = []
        self.remote_dataset_name = None

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)
    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)

        super(SingleMessageTransformationTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_setup(self):
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        self.log.info("Create test infra from spec.")
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.log.info("Load data to remote collection.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        self.load_doc_to_remote_collections(self.remote_cluster, valType="Product",
                                            create_start_index=0, create_end_index=self.initial_doc_count,
                                            wait_for_completion=True)
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.remote_dataset_name = remote_datasets[0].full_name

    def create_udf_and_remotedataset(self, udf_query, if_not_exists=False, udf_name=None, or_replace=False):
        if not udf_name:
            udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_query, transform_function=True, if_not_exists=if_not_exists, or_replace=or_replace):
            self.fail(f"Error while creating UDF with name {udf_name}")

        self.log.info("Create collection using the transform function.")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
            self.columnar_cluster,
            self.remote_cluster.buckets[0].name,
            "_default",
            "_default",
            remote_link,
            use_only_existing_db=True,
            use_only_existing_dv=True,
            database="Default",
            dataverse="Default",
            capella_as_source=True)[0]
        result = self.cbas_util.create_remote_dataset(
            self.columnar_cluster,
            remote_coll_obj.name,
            remote_coll_obj.full_kv_entity_name,
            remote_coll_obj.link_name,
            remote_coll_obj.dataverse_name,
            remote_coll_obj.database_name,
            transform_function=udf_name)
        if not result:
            self.fail("Failed to create remote collection {} from transform function {}".format(
                remote_coll_obj.name, udf_name))

        return remote_coll_obj

    def validate_collection_created_from_udf(self, udf_query, remote_query):
        # Validate result by running query on remote dataset and comparing with the transform collection result.
        status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, udf_query)
        udf_results = results
        status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, remote_query)
        diff = DeepDiff(results, udf_results, ignore_order=True)
        if diff:
            self.log.info(diff)
            self.fail(
                f"Data mismatch between expected and actual result of transform function."
                f"{diff}")

    def drop_udf(self):
        udf_list = self.cbas_util.get_all_udfs_from_metadata(self.columnar_cluster)
        for udf in udf_list:
            return self.cbas_util.drop_udf(self.columnar_cluster, CBASHelper.format_name(udf[0]), None, None, udf[1])

    def test_smt_string_manipulation(self):
        """
        Test create transform function with string manipulation.
        """
        self.log.info("Running transform function with string manipulation test.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT product_name || seller_name as name_seller
                        FROM [item] as item
                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = f"select name_seller from {udf_coll_obj.name}"
        remote_query = f"select product_name || seller_name as name_seller from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_filter_with_where_clause(self):
        """
        Test create transform function with where clause.
        """
        self.log.info("Running transform function with where clause test.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT product_name
                        FROM [item] as item
                        WHERE num_sold>10000
                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = f"select product_name from {udf_coll_obj.name}"
        remote_query = f"select product_name from {self.remote_dataset_name} where num_sold>10000"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_select_multiple_fields(self):
        """
        Test create transform function with multiple fields.
        """
        self.log.info("Running transform function with select multiple fields.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_multiple_fields_excluded(self):
        """
        Test create transform function with multiple fields excluded.
        """
        self.log.info("Running transform function with multiple fields excluded.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc
                            FROM (SELECT i.*
                              EXCLUDE product_category,
                                      product_reviews,
                                      product_link
                              FROM [item] AS i
                              ) AS doc
                            LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        for field in ["product_category", "product_reviews", "product_link"]:
            udf_select_query = f"select {field} from {udf_coll_obj.name}"
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, udf_select_query)
            if results[0]:
                self.fail("Query result is not empty for selecting excluded field.")
        self.log.info("Test completed successfully.")

    def test_smt_create_udf_if_not_exists(self):
        """
        Test create udf if not exists.
        """
        self.log.info("Running transform function if not exists.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_name = self.cbas_util.generate_name()
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        self.create_udf_and_remotedataset(udf_create_query, udf_name=udf_name)

        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True, if_not_exists=True):
            self.fail(f"Error while creating UDF with name {udf_name} which already exists using if not exists.")
        self.log.info("Test completed successfully.")

    def test_smt_replace_udf(self):
        """
        Test replace transform function.
        """
        self.log.info("Running transform function with Replace UDF.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error while creating UDF.")
        self.log.info("Replace UDF with a new query.")
        udf_create_query = f"""SELECT product_name || seller_name as name_seller
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query, udf_name=udf_name, or_replace=True)
        udf_select_query = f"select name_seller from {udf_coll_obj.name}"
        remote_query = f"select product_name || seller_name as name_seller from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_nested_udf(self):
        """
        Test create nested transform function.
        """
        self.log.info("Running nested transform functions.")

        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc
                                    FROM (SELECT i.*
                                      EXCLUDE product_category,
                                              product_reviews,
                                              product_link
                                      FROM [item] AS i
                                      ) AS doc
                                    LIMIT 1"""
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error while creating UDF.")
        nested_udf_query = f"""SELECT VALUE doc
                                    FROM (SELECT i.*
                                      EXCLUDE product_specs,
                                              product_features,
                                              product_image_links
                                      FROM {udf_name}(item) AS i
                                      ) AS doc
                                    LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(nested_udf_query)
        for field in ["product_category", "product_reviews", "product_link",
                      "product_specs", "product_features", "product_image_links"]:
            udf_select_query = f"select {field} from {udf_coll_obj.name}"
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, udf_select_query)
            if results[0]:
                self.fail("Query result is not empty for selecting excluded field.")
        self.log.info("Test completed successfully.")

    def test_smt_delete_kv_items(self):
        """
        Test create transform function delete kv items.
        """
        self.log.info("Running transform function and delete items in kv.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Delete half the docs created from cluster.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        delete_end_index = self.initial_doc_count // 2
        self.log.info(f"Deleting {delete_end_index} docs from remote cluster")
        self.load_doc_to_remote_collections(self.remote_cluster, "Product",
                                            delete_start_index=0, delete_end_index=delete_end_index,
                                            create_percent=0, delete_percent=100)

        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_update_kv_items(self):
        """
        Test create transform function update kv items.
        """
        self.log.info("Running transform function and update items in kv.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                        FROM [item] as item
                                        LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Update half the docs created from cluster.")
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        update_end_index = self.initial_doc_count // 2
        self.log.info(f"Updating {update_end_index} docs from remote cluster")
        self.load_doc_to_remote_collections(self.remote_cluster, "Product",
                                            update_start_index=0, update_end_index=update_end_index)

        udf_select_query = f"select quantity, product_name from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_rename_field(self):
        """
        Test create transform function with renaming fields.
        """
        self.log.info("Running transform function with renaming fields.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT VALUE doc FROM
                                (SELECT item.*, quantity as product_quantity, weight as product_weight
                                EXCLUDE quantity, weight
                                FROM [item] as item) AS doc
                                LIMIT 1"""
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        udf_select_query = f"select product_quantity, product_weight from {udf_coll_obj.name}"
        remote_query = f"select quantity as product_quantity, weight as product_weight from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_add_static_field(self):
        """
        Test create transform function with add static field.
        """
        self.log.info("Running transform function with add static field.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name,1 as version
                                FROM [item] as item
                                LIMIT 1 """
        udf_coll_obj = self.create_udf_and_remotedataset(udf_create_query)
        self.log.info("Sleep for a second before validation.")
        time.sleep(1)
        udf_select_query = f"select quantity, product_name, version from {udf_coll_obj.name}"
        remote_query = f"select quantity, product_name,1 as version from {self.remote_dataset_name}"
        self.log.info("Validate collection created with the transform function.")
        self.validate_collection_created_from_udf(udf_select_query, remote_query)
        self.log.info("Test completed successfully.")

    def test_smt_drop_unused_udf(self):
        """
        Test smt drop unused udf.
        """
        self.log.info("Running transform function for drop unused udf.")
        self.log.info("Start test setup.")
        self.test_setup()
        self.log.info("Create UDF with transform function.")
        udf_create_query = f"""SELECT quantity, product_name
                                                FROM [item] as item
                                                LIMIT 1 """
        udf_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_udf(
                cluster=self.columnar_cluster, name=udf_name, parameters=["item"],
                body=udf_create_query, transform_function=True):
            self.fail("Error while creating UDF.")

        if not self.drop_udf():
            self.fail("Unable to drop UDF.")
        self.log.info("Test completed successfully.")
