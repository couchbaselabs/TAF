"""
Created on 28-July-2026

@author: himanshu.jain@couchbase.com
"""

import json
import math

from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase


class LSMSampling(ColumnarOnPremBase):

    # Shared across every test in this module for the lifetime of the
    # run - one column-storage and one row-storage standalone collection
    # are created (and loaded) once, keyed by storage format, and reused
    # by every test instead of being recreated/reloaded per test.
    _collection_names = {}
    _full_names = {}

    def setUp(self):
        super(LSMSampling, self).setUp()

        self.initial_doc_count = self.input.param("initial_doc_count", 1000000)
        self.storage_format = self.input.param("storage", "column")
        self.sample_size = self.input.param("sample", None)
        self.sample_method = self.input.param("sample_method", None)
        self.sample_seed = self.input.param("sample_seed", None)
        self.drop_statistics = self.input.param("drop_statistics", False)
        self.expected_error = self.input.param("expected_error", None)
        self.mutation_percentages = [
            int(pct) for pct in
            self.input.param("mutation_percentages", "5:10:30:50").split(":")]
        self.num_partitions = self.get_num_storage_partitions()

        self.setupCollection()
        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__
        )

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        # Keep the collections/data around for reuse (loading
        # initial_doc_count docs is expensive), only drop the per-test
        # ANALYZE sample index.
        if self.cbas_util.verify_sample_present_in_Metadata(
                self.columnar_cluster, self.collection_name, "Default"):
            self.cbas_util.drop_sample_for_analytics_collections(
                self.columnar_cluster, self.full_name, analytics=False)

        super(LSMSampling, self).tearDown()
        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def get_num_storage_partitions(self):
        """Fetches numStoragePartitions via GET /settings/analytics
        (reusing the existing AnalyticsRestAPI) instead of hardcoding it,
        since it's cluster-configurable."""
        status, content = AnalyticsRestAPI(
            self.columnar_cluster.master).get_analytics_settings()
        if not status:
            self.fail("Failed to fetch analytics settings: {0}".format(
                content))
        return content["numStoragePartitions"]

    def setupCollection(self):
        if not LSMSampling._full_names:
            self.input.test_params["num_external_links"] = "1"
            columnar_spec = self.populate_columnar_infra_spec(
                columnar_spec=self.cbas_util.get_columnar_spec(
                    "full_template"),
                external_collection_file_formats=["json"])

            columnar_spec["standalone_dataset"]["num_of_standalone_coll"] = 1
            columnar_spec["standalone_dataset"]["primary_key"] = [
                {"id": "string"}]

            # First iteration also creates the (shared) external link and
            # external collection; the second iteration re-uses them.
            for i, storage_format in enumerate(("column", "row")):
                columnar_spec["external_link"]["no_of_external_links"] = int(
                    i == 0)
                columnar_spec["external_dataset"]["num_of_external_datasets"] = int(
                    i == 0)
                columnar_spec["standalone_dataset"]["storage_format"] = storage_format

                result, msg = self.cbas_util.create_cbas_infra_from_spec(
                    cluster=self.columnar_cluster, cbas_spec=columnar_spec,
                    bucket_util=self.bucket_util, wait_for_ingestion=False)
                if not result:
                    self.fail(msg)

            ext_collection = self.cbas_util.get_all_dataset_objs("external")[0]

            for standalone_collection in self.cbas_util.get_all_dataset_objs(
                    "standalone"):
                fmt = standalone_collection.storage_format
                LSMSampling._collection_names[fmt] = standalone_collection.name
                LSMSampling._full_names[fmt] = standalone_collection.full_name

                cmd = "INSERT INTO {0} SELECT VALUE d FROM {1} d LIMIT {2};".format(
                    standalone_collection.full_name, ext_collection.full_name,
                    self.initial_doc_count)
                status, metrics, errors, results, _, warnings = (
                    self.cbas_util.execute_statement_on_cbas_util(
                        self.columnar_cluster, cmd, timeout=3600,
                        analytics_timeout=3600))
                if status != "success":
                    self.fail("Failed to load data into {0}: {1}".format(
                        standalone_collection.full_name, errors))

        self.collection_name = LSMSampling._collection_names[self.storage_format]
        self.full_name = LSMSampling._full_names[self.storage_format]

    def test_analyze_dataset(self):
        result = self.cbas_util.create_sample_for_analytics_collections(
            self.columnar_cluster, self.full_name,
            sample_size=self.sample_size, sample_seed=self.sample_seed,
            sample_method=self.sample_method,
            validate_error_msg=bool(self.expected_error),
            expected_error=self.expected_error, analytics=False)

        if self.expected_error:
            if not result:
                self.fail("ANALYZE DATASET did not fail with expected "
                          "error '{0}' for {1}".format(
                              self.expected_error, self.full_name))
            return

        if not result:
            self.fail("ANALYZE DATASET failed for {0} with sample={1}, "
                      "sample_method={2}, sample_seed={3}".format(
                          self.full_name, self.sample_size,
                          self.sample_method, self.sample_seed))

        if not self.cbas_util.verify_sample_present_in_Metadata(
                self.columnar_cluster, self.collection_name, "Default",
                sample_method=self.sample_method, sample_size=self.sample_size,
                sample_seed=self.sample_seed):
            self.fail("Sample statistics not found/mismatched in "
                      "Metadata.Index after ANALYZE DATASET for {0} "
                      "(sample={1}, sample_method={2}, sample_seed={3})"
                      .format(self.full_name, self.sample_size,
                              self.sample_method, self.sample_seed))

        if self.drop_statistics:
            if not self.cbas_util.drop_sample_for_analytics_collections(
                    self.columnar_cluster, self.full_name, analytics=False):
                self.fail("ANALYZE DATASET DROP STATISTICS failed for {0}"
                          .format(self.full_name))

            if self.cbas_util.verify_sample_present_in_Metadata(
                    self.columnar_cluster, self.collection_name, "Default"):
                self.fail("Sample statistics still present in Metadata.Index "
                          "after DROP STATISTICS for {0}".format(
                              self.full_name))

    def test_mutation_effect_on_dataset_sample(self):
        """
        Verifies that ANALYZE DATASET (SAMPLE statistics) picks up mutations
        (deletes/upserts) correctly
        """
        # This test defaults sample/sample_method/sample_seed differently
        # from test_analyze_dataset (which defaults them to None), since it
        # needs an actual sample to check mutation effects against.
        sample_size = self.sample_size or "high"
        sample_method = self.sample_method or "random"
        sample_seed = 1000 if self.sample_seed is None else self.sample_seed

        current_doc_count = self.cbas_util.get_num_items_in_cbas_dataset(
            self.columnar_cluster, self.full_name)
        self.log.info("{0}: starting doc count = {1}".format(
            self.full_name, current_doc_count))

        for pct in self.mutation_percentages:
            num_docs = max(1, int(current_doc_count * pct / 100))
            known_deleted_ids = self.get_doc_ids(num_docs)

            current_doc_count = self._delete_pct(
                pct, num_docs, known_deleted_ids, current_doc_count)
            self._refresh_and_verify(
                sample_size, sample_method, sample_seed, current_doc_count,
                known_deleted_ids)

            current_doc_count = self._upsert_back(
                pct, num_docs, current_doc_count)
            self._refresh_and_verify(
                sample_size, sample_method, sample_seed, current_doc_count,
                known_deleted_ids)

    # ---- helpers reused by test_mutation_effect_on_dataset_sample ----

    def _delete_pct(self, pct, num_to_delete, known_deleted_ids,
                    current_doc_count):
        """Deletes num_to_delete docs (the known_deleted_ids first, then a
        LIMIT-based subquery for the rest) and asserts the resulting count."""
        self.log.info("{0}: deleting {1}% ({2} docs)".format(
            self.full_name, pct, num_to_delete))

        if not self.cbas_util.delete_from_standalone_collection(
                self.columnar_cluster, self.collection_name, "Default",
                "Default", where_clause="alias.id in {0}".format(
                    json.dumps(known_deleted_ids)),
                use_alias=True):
            self.fail("Failed to delete known docs from {0}".format(
                self.full_name))

        remaining_to_delete = num_to_delete - len(known_deleted_ids)
        if remaining_to_delete > 0:
            where_clause = (
                "alias.id in (SELECT VALUE x.id FROM {0} AS x "
                "LIMIT {1})".format(self.full_name, remaining_to_delete))
            if not self.cbas_util.delete_from_standalone_collection(
                    self.columnar_cluster, self.collection_name, "Default",
                    "Default", where_clause=where_clause, use_alias=True):
                self.fail("Failed to delete {0}% of docs from {1}".format(
                    pct, self.full_name))

        current_doc_count -= num_to_delete
        self.assert_doc_count(
            current_doc_count, "after deleting {0}%".format(pct))
        self.log.info("{0}: doc count after delete = {1}".format(
            self.full_name, current_doc_count))
        return current_doc_count

    def _upsert_back(self, pct, num_to_delete, current_doc_count):
        """Upserts num_to_delete brand-new docs, bringing the doc count back
        up to what it was before the matching _delete_pct call (e.g. 100
        docs -> delete 5% (5 docs) -> upsert 5 docs -> back to 100 docs)."""
        self.log.info("{0}: upserting {1} new docs back for {2}%".format(
            self.full_name, num_to_delete, pct))

        upsert_docs = [
            {"id": "{0}_{1}_{2}".format(
                pct, i, self.cbas_util.generate_name(name_cardinality=1, seed=1)),
             "mutation_pct": pct}
            for i in range(num_to_delete)]
        if not self.cbas_util.upsert_into_standalone_collection(
                self.columnar_cluster, self.collection_name, upsert_docs,
                "Default", "Default"):
            self.fail("Failed to upsert {0} docs into {1}".format(
                num_to_delete, self.full_name))

        current_doc_count += num_to_delete
        self.assert_doc_count(
            current_doc_count,
            "after upserting {0} docs back for {1}%".format(
                num_to_delete, pct))
        self.log.info("{0}: doc count after upsert = {1}".format(
            self.full_name, current_doc_count))
        return current_doc_count

    def _refresh_and_verify(self, sample_size, sample_method, sample_seed,
                            expected_count, deleted_ids):
        self.refresh_sample(sample_size, sample_method, sample_seed)
        self.verify_sample_metadata_and_dump(
            sample_seed, sample_method, expected_count,
            deleted_ids=deleted_ids)

    def get_doc_ids(self, limit):
        """Fetch up to `limit` doc ids currently present in self.full_name."""
        cmd = "SELECT VALUE x.id FROM {0} x LIMIT {1};".format(
            self.full_name, limit)
        status, _, errors, results, _, _ = (
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd))
        if status != "success":
            self.fail("Failed to fetch doc ids from {0}: {1}".format(
                self.full_name, errors))
        return results

    def assert_doc_count(self, expected_count, context):
        actual_count = self.cbas_util.get_num_items_in_cbas_dataset(
            self.columnar_cluster, self.full_name)
        if actual_count != expected_count:
            self.fail("Doc count mismatch for {0} {1}: expected {2}, got {3}"
                      .format(self.full_name, context, expected_count,
                              actual_count))

    def refresh_sample(self, sample_size, sample_method, sample_seed):
        """Runs ANALYZE DATASET to (re)create SAMPLE statistics. If the
        dataset already has SAMPLE statistics and re-analyzing directly on
        top of them fails, falls back to an explicit DROP STATISTICS
        followed by a fresh ANALYZE."""
        if self.cbas_util.create_sample_for_analytics_collections(
                self.columnar_cluster, self.full_name, sample_size=sample_size,
                sample_seed=sample_seed, sample_method=sample_method, analytics=False):
            return

        self.cbas_util.drop_sample_for_analytics_collections(
            self.columnar_cluster, self.full_name, analytics=False)
        if not self.cbas_util.create_sample_for_analytics_collections(
                self.columnar_cluster, self.full_name, sample_size=sample_size,
                sample_seed=sample_seed, sample_method=sample_method, analytics=False):
            self.fail("ANALYZE DATASET failed for {0}".format(self.full_name))

    def verify_sample_metadata_and_dump(self, sample_seed, sample_method,
                                        expected_source_cardinality,
                                        deleted_ids=None):
        index_row = self.cbas_util.get_sample_index_metadata(
            self.columnar_cluster, self.collection_name, "Default")
        if not index_row:
            self.fail("Sample statistics not found in Metadata.Index for {0}"
                      .format(self.collection_name))
        print(f"Metadata.Index row for {self.collection_name}:\n {index_row}")

        if str(index_row.get("SampleSeed")) != str(sample_seed):
            self.fail("SampleSeed mismatch for {0}: expected {1}, got {2}"
                      .format(self.collection_name, sample_seed,
                              index_row.get("SampleSeed")))

        if index_row.get("SampleMethod") != sample_method:
            self.fail("SampleMethod mismatch for {0}: expected {1}, got {2}"
                      .format(self.collection_name, sample_method,
                              index_row.get("SampleMethod")))

        # if index_row.get("SourceCardinality") != expected_source_cardinality:
        #     self.fail("SourceCardinality mismatch for {0}: expected {1}, "
        #               "got {2}".format(self.collection_name,
        #                                expected_source_cardinality,
        #                                index_row.get("SourceCardinality")))

        target = index_row.get("SampleCardinalityTarget")
        per_partition = max(1, math.ceil(target / self.num_partitions))
        expected_dump_count = per_partition * self.num_partitions

        actual_dump_count = self.cbas_util.get_dump_index_count(
            self.columnar_cluster, self.collection_name, index_row["IndexName"])
        print(
            f"DUMP_INDEX row count for {self.collection_name}: {actual_dump_count}; expected {expected_dump_count} (SampleCardinalityTarget={target}, num_partitions={self.num_partitions})")
        if actual_dump_count != expected_dump_count:
            self.fail(
                "DUMP_INDEX row count mismatch for {0}: "
                "SampleCardinalityTarget={1}, num_partitions={2} => "
                "expected {3}, got {4}".format(
                    self.collection_name, target, self.num_partitions,
                    expected_dump_count, actual_dump_count))

        if deleted_ids:
            overlap_count = self.cbas_util.get_dump_index_count(
                self.columnar_cluster, self.collection_name,
                index_row["IndexName"],
                where_clause="p.id in {0}".format(json.dumps(deleted_ids)))
            if overlap_count != 0:
                self.fail(
                    "{0} previously deleted doc(s) still present in SAMPLE "
                    "dump index for {1}".format(overlap_count,
                                                self.collection_name))
