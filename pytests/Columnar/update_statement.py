"""
Created on 1-Mar-2026

@author: himanshu.jain@couchbase.com
"""

from unittest import result
from datetime import datetime

from TestInput import TestInputSingleton
runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "columnar":
    from Columnar.columnar_base import ColumnarBaseTest
else:
    from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest


class UpdateStatementTest(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    def setUp(self):
        super(UpdateStatementTest, self).setUp()
        self.initial_doc_count = self.input.param("initial_doc_count", 1000)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if hasattr(self, "columnar_spec"):
            if not self.cbas_util.delete_cbas_infra_created_from_spec(
                    self.columnar_cluster):
                self.fail("Error while deleting cbas entities")

        super(UpdateStatementTest, self).tearDown()

        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def create_load_documents_standalone_collection(self, primary_key={}, doc_template="hotel", doc_template_params=None):
        # create standalone collection
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster,
                                                                       database_name=database_name,
                                                                       dataverse_name=dataverse_name)

            if not self.cbas_util.create_standalone_collection(
                cluster=self.columnar_cluster,
                collection_name=dataset_obj[0].name,
                primary_key=primary_key,
                dataverse_name=dataset_obj[0].dataverse_name,
                database_name=dataset_obj[0].database_name,
                validate_error_msg=validate_error,
                expected_error=error_message
            ):
                self.log.error(
                    f"Failed to create standalone collection {dataset_obj[0].name}")

            if not self.cbas_util.load_doc_to_standalone_collection(
                cluster=self.columnar_cluster,
                collection_name=dataset_obj[0].name,
                dataverse_name=dataset_obj[0].dataverse_name,
                database_name=dataset_obj[0].database_name,
                no_of_docs=self.initial_doc_count,
                document_size=self.doc_size,
                doc_template=doc_template,
                doc_template_params=doc_template_params
            ):
                self.log.error(
                    f"Failed to load data into standalone collection {dataset_obj[0].name}")

    def test_update_existing_field(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        print(datasets)

        for dataset in datasets:
            # Get random doc ID
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            # Execute update statement
            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=['h.city = "Bangalore"'],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            # valdate the update statement
            cmd = f'SELECT value h.city FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster,
                cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(
                results[0], "Bangalore", "Update statement did not update the field city")

        self.log.info("Validation completed for test_update_existing_field")
        return

    def test_update_non_existing_field(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=["h.new_feature_flag = TRUE"],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE h.new_feature_flag FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(
                results[0], True, "new_feature_flag should be TRUE after adding non-existing field")
        self.log.info(
            "Validation completed for test_update_non_existing_field")
        return

    def test_update_multiple_fields(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=['h.city = "Bangalore"',
                             'h.price = 9999', 'h.free_parking = TRUE'],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT h.city, h.price, h.free_parking FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(results[0]["city"],
                             "Bangalore", "city was not updated")
            self.assertEqual(results[0]["price"], 9999,
                             "price was not updated")
            self.assertEqual(results[0]["free_parking"],
                             True, "free_parking was not updated")
        self.log.info("Validation completed for test_update_multiple_fields")
        return

    def test_remove_field_using_missing(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=["h.free_parking = MISSING"],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE IS_MISSING(h.free_parking) FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(
                results[0], "free_parking field should have been removed by setting to MISSING")
        self.log.info(
            "Validation completed for test_remove_field_using_missing")
        return

    def test_update_field_multiple_times(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            cmd = f'SELECT VALUE h.price FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(f"Failed to query initial price: {errors}")
            initial_price = results[0]
            expected_price = (initial_price + 100) * 2 - 50

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                change_sequence_sets=[
                    ["h.price = h.price + 100"],
                    ["h.price = h.price * 2"],
                    ["h.price = h.price - 50"],
                ],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE h.price FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(results[0], expected_price,
                             f"Expected price {expected_price} after change-sequence, got {results[0]}")
        self.log.info(
            "Validation completed for test_update_field_multiple_times")
        return

    def test_update_delete_field(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                change_sequence_sets=[
                    ['h.city = "IntermediateCity"'],
                    ['h.city = MISSING'],
                ],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE IS_MISSING(h.city) FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(results[0],
                            "city field should have been removed by change-sequence SET to MISSING")
        self.log.info("Validation completed for test_update_delete_field")
        return

    def test_update_multi_datatypes(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=[
                    'h.name = {"first_name": "John", "last_name": "Wick"}',
                    'h.price = "9999"',
                    'h.free_breakfast = TRUE',
                    'h.padding = NULL',
                    'h.public_likes = ["Alice", "Bob"]',
                ],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = (f'SELECT h.name, h.price, h.free_breakfast, h.padding, h.public_likes '
                   f'FROM {dataset} h WHERE h.id = uuid("{randomId}")')
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            doc = results[0]
            self.assertEqual(type(doc["name"]), dict,
                             "string field name was not updated")
            self.assertEqual(doc["price"], "9999",
                             "string field price was not updated")
            self.assertEqual(doc["free_breakfast"], True,
                             "bool field free_breakfast was not updated")
            self.assertIsNone(
                doc["padding"], "null field padding was not set to null")
            self.assertEqual(doc["public_likes"], [
                             "Alice", "Bob"], "array field public_likes was not updated")
        self.log.info("Validation completed for test_update_multi_datatypes")
        return

    def test_add_new_field_from_computed_value(self):
        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            cmd = f'SELECT VALUE h.price FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(f"Failed to query initial price: {errors}")
            initial_price = results[0]
            expected_is_premium = initial_price > 5000

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=["h.is_premium = (h.price > 5000)"],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE h.is_premium FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(results[0], expected_is_premium,
                             f"is_premium should be {expected_is_premium} for price {initial_price}")
        self.log.info(
            "Validation completed for test_add_new_field_from_computed_value")
        return

    def test_add_timestamp_field(self):
        def is_valid_iso(ts):
            try:
                # fromisoformat handles the 'T' and milliseconds automatically
                datetime.fromisoformat(ts)
                return True
            except ValueError:
                return False

        self.create_load_documents_standalone_collection()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            # Get random doc ID
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            # Execute update statement
            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="h",
                set_clauses=['h.timestamp_field = current_datetime()'],
                where_clause=f'h.id = uuid("{randomId}")',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            # valdate the update statement
            cmd = f'SELECT value h.timestamp_field FROM {dataset} h WHERE h.id = uuid("{randomId}")'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster,
                cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(
                is_valid_iso(results[0]), f"Timestamp field value is not valid ISO format: {results[0]}")

        self.log.info("Validation completed for test_add_timestamp_field")
        return

    def test_update_nested_existing_field(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=['p.address.primaryAddress.city = "NestedCity"'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE p.address.primaryAddress.city FROM {dataset} p WHERE p.id = "{randomId}"'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(
                results[0] == "NestedCity", "Nested field address.primaryAddress.city was not updated")
        self.log.info(
            "Validation completed for test_update_nested_existing_field")
        return

    def test_add_nested_non_existing_field(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=['p.address.metadata.source = "imported"'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f'SELECT VALUE p.address.metadata.source FROM {dataset} p WHERE p.id = "{randomId}"'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(
                results[0] == "imported", "Nested non-existing field address.metadata.source was not added")
        self.log.info(
            "Validation completed for test_add_nested_non_existing_field")
        return

    def test_update_null_to_non_null(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=[
                    'p.address.coordinates.`secondary`.latitude = NULL'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Failed to set nested field to NULL: {errors}")

            cmd = f'SELECT VALUE p.address.coordinates.`secondary`.latitude FROM {dataset} p WHERE p.id = "{randomId}"'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(f"Failed to query null field: {errors}")
            self.assertIsNone(
                results[0], "Expected address.coordinates.`secondary`.latitude to be NULL before update")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=[
                    'p.address.coordinates.`secondary`.latitude = 12.345678'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(
                    f"Failed to update null field to non-null value: {errors}")

            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(
                results[0], 12.345678, "Nested null field was not updated to the expected non-null value")
        self.log.info("Validation completed for test_update_null_to_non_null")
        return

    def test_delete_non_existing_field_noop(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            cmd = f'SELECT VALUE p.address.primaryAddress.city FROM {dataset} p WHERE p.id = "{randomId}"'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query initial nested field value: {errors}")
            initial_city = results[0]

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=[
                    'p.ghostField = MISSING',
                    'p.address.ghostSubField.x = MISSING'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(
                    f"Delete non-existing field statement failed with error: {errors}")

            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(
                results[0], initial_city, "Deleting non-existing fields should not modify existing fields")

            cmd = (f'SELECT VALUE {{"ghostFieldMissing": IS_MISSING(p.ghostField), '
                   f'"ghostSubFieldMissing": IS_MISSING(p.address.ghostSubField)}} '
                   f'FROM {dataset} p WHERE p.id = "{randomId}"')
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to validate missing fields after update: {errors}")
            self.assertTrue(results[0]["ghostFieldMissing"],
                            "ghostField should remain missing after delete no-op")
            self.assertTrue(results[0]["ghostSubFieldMissing"],
                            "ghostSubField should remain missing after delete no-op")
        self.log.info(
            "Validation completed for test_delete_non_existing_field_noop")
        return

    def test_update_primary_key_negative(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=["p.id = '999'"],
                where_clause=f'p.id = "{randomId}"',
            )
            print(f"status: {status}, errors: {errors}")
            if status != "success":
                self.fail(
                    f"Update statement failed with error: {errors}")
            self.assertTrue(
                errors, "Expected an error when updating the primary key field")

            cmd = f'SELECT VALUE COUNT(*) FROM {dataset} p WHERE p.id = "{randomId}"'
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to verify original document after negative primary key update: {errors}")
            self.assertEqual(
                results[0], 1, "Original document should remain unchanged after failed primary key update")
        self.log.info(
            "Validation completed for test_update_primary_key_negative")
        return

    def test_returning_clause(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(INSERT INTO p.hobbies (["Coding"]))',
                ],
                where_clause=f'p.id = "{randomId}"',
                returning_clause="p.hobbies"
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            self.assertTrue(type(results[0]) == list and len(
                results[0]) > 0, f"Returning clause failed, got: {results[0]}")
        self.log.info("Validation completed for test_returning_clause")
        return

    def test_update_array_without_pos(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(INSERT INTO p.hobbies (["Coding"]))',
                ],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f"select value ds.hobbies from {dataset} ds where ds.id = '{randomId}';"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue("Coding" in results[0],
                            "Updated array should does not contain the inserted value 'Coding'")
        self.log.info("Validation completed for test_update_array_without_pos")
        return

    def test_update_array_with_pos(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(INSERT INTO p.hobbies AT 0 (["Coding"]))',
                ],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f"select value ds.hobbies from {dataset} ds where ds.id = '{randomId}';"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(results[0][0] == "Coding",
                            "Updated array should have 'Coding' inserted at position 0")
        self.log.info("Validation completed for test_update_array_with_pos")
        return

    def test_update_multiple_array_elements(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=['p.hobbies = [1,2,3,4,5,6,7,8,9,10]'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(UPDATE p.hobbies AS hobby AT ord SET hobby = 0 WHERE ord IN [1,2,3,10])',
                ],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f"select value ds.hobbies from {dataset} ds where ds.id = '{randomId}';"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(results[0][0] == 0 and results[0][1] == 0 and results[0][2] == 0 and results[0][9] == 0,
                            f"Hobbies arr is incorrect after updating: got {results[0]}")
        self.log.info(
            "Validation completed for test_update_multiple_array_elements")
        return

    def test_update_multiple_array_elements_with_condition(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=['p.hobbies = [1,2,3,4,5,6,7,8,9,10]'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(UPDATE p.hobbies AS hobby AT ord SET hobby = 0 WHERE hobby%2=0)',
                ],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f"select value ds.hobbies from {dataset} ds where ds.id = '{randomId}';"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(result[0], [1, 0, 3, 0, 5, 0, 7, 0, 9, 0],
                             f"Hobbies arr is incorrect after updating: got {results[0]}")
        self.log.info(
            "Validation completed for test_update_multiple_array_elements_with_condition")
        return

    def test_delete_multiple_array_elements(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=['p.hobbies = [1,2,3,4,5,6,7,8,9,10]'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(DELETE FROM p.hobbies AT ord WHERE ord IN [1,2,3,10])',
                ],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            cmd = f"select value ds.hobbies from {dataset} ds where ds.id = '{randomId}';"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertTrue(results[0][0] == 0 and results[0][1] == 0 and results[0][2] == 0 and results[0][9] == 0,
                            f"Hobbies arr is incorrect after updating: got {results[0]}")
        self.log.info(
            "Validation completed for test_delete_multiple_array_elements")
        return

    def test_delete_array_elements_with_condition(self):
        self.create_load_documents_standalone_collection(
            primary_key={"id": "string"}, doc_template="heterogeneous", doc_template_params={"heterogeneity": 1})
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            randomId = self.cbas_util.get_random_doc(
                self.columnar_cluster, dataset.name, dataset.dataverse_name, dataset.database_name)
            self.log.info(f"Random ID: {randomId}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                set_clauses=['p.hobbies = [1,2,3,4,5,6,7,8,9,10]'],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(f"Update statement failed with error: {errors}")

            status, metrics, errors, results, _, warnings = self.cbas_util.update_dataset(
                self.columnar_cluster,
                bucket=dataset,
                alias="p",
                array_mutations=[
                    '(DELETE FROM p.hobbies AS hobby WHERE hobby%2=0)',
                ],
                where_clause=f'p.id = "{randomId}"',
            )
            if status != "success":
                self.fail(
                    f"Delete array elements with condition failed with error: {errors}")

            cmd = f"select value ds.hobbies from {dataset} ds where ds.id = '{randomId}';"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if status != "success":
                self.fail(
                    f"Failed to query standalone collection after update: {errors}")
            self.assertEqual(results[0], [1, 3, 5, 7, 9],
                             f"Unexpected array value after deleting elements with condition: {results[0]}")
        self.log.info(
            "Validation completed for test_delete_array_elements_with_condition")
        return
