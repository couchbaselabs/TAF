"""
Created on 05-Aug-2026

@author: himanshu.jain@couchbase.com
"""

import json
import os
import random
import struct
import threading
import time

from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from bucket_collections.collections_base import CollectionBase
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI


class VectorSearch(ColumnarOnPremBase):

    _kv_loaded = False

    DATASET_TIER_DOC_COUNTS = {
        "1M": 1000000,
        "2M": 2000000,
        "5M": 5000000,
        "10M": 10000000,
        "20M": 20000000,
        "50M": 50000000,
        "100M": 100000000,
        "200M": 200000000,
        "500M": 500000000,
        "1000M": 1000000000,
    }

    L2_SIMILARITY_FAMILY = {"euclidean", "euclidean_squared",
                            "l2", "l2_squared"}
    FILTERABLE_FIELDS = ("color", "brand", "country", "category", "type")

    def setUp(self):
        super(VectorSearch, self).setUp()

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        # Docloader (SIFTLoader) params, mirrors the sample java
        # magmadocloader.jar SIFTLoader command.
        self.docloader_value_type = self.input.param(
            "docloader_value_type", "siftBigANN")
        self.docloader_create_start = self.input.param(
            "docloader_create_start", 0)
        self.docloader_create_end = self.input.param(
            "docloader_create_end", 100000)
        self.process_concurrency = self.input.param(
            "process_concurrency", 4)
        self.base_vectors_file_path = self.input.param(
            "base_vectors_file_path", "/Volumes/nfsdata/bigann")
        self.ingestion_timeout = self.input.param("ingestion_timeout", 600)

        # Vector index params
        self.vector_field = self.input.param("vector_field", "embedding")
        self.dimension = self.input.param("dimension", 128)
        self.similarity = self.input.param("similarity", "euclidean_squared")
        self.include_fields = self.input.param("include_fields", None)
        if self.include_fields:
            self.include_fields = self.include_fields.split(":")
        self.index_type = self.input.param("index_type", "VTREE")

        self.train_list_fraction = self.input.param(
            "train_list_fraction", None)
        self.quantization = self.input.param("quantization", None)
        self.epsilon = self.input.param("epsilon", None)
        self.num_clusters = self.input.param("num_clusters", None)
        self.cross_pollination_m = self.input.param(
            "cross_pollination_m", None)
        self.rng_factor = self.input.param("rng_factor", None)

        # Negative-test-only knob: a single arbitrary "name:value" field
        # to merge into the WITH clause (e.g. to exercise the "unknown
        # field" validation error).
        self.extra_with_params = None
        extra_with_param = self.input.param("extra_with_param", None)
        if extra_with_param:
            field_name, field_value = extra_with_param.split(":", 1)
            self.extra_with_params = {field_name: field_value}

        self.expected_error = self.input.param("expected_error", None)

        self._created_indexes = []

        # KNN/ANN search params
        self.k = self.input.param("k", 10)
        self.function_name = self.input.param(
            "function_name", "vector_distance")
        self.distance_function = self.input.param(
            "distance_function", self.similarity)
        self.min_probe_fraction = self.input.param(
            "min_probe_fraction", None)
        self.k_multiplier = self.input.param("k_multiplier", None)
        self.create_vector_index = bool(
            self.input.param("create_vector_index", True))

        self.where_clause = None
        self.filter_fields = [
            field for field in self.FILTERABLE_FIELDS
            if self.input.param(field, None) is not None]
        if self.filter_fields:
            self.where_clause = " AND ".join(
                "i.{0}='{1}'".format(field, self.input.param(field))
                for field in self.filter_fields)

        # test_recall-only params
        self.num_queries = self.input.param("num_queries", 100)
        self.knn_groundtruth = self.input.param("knn_groundtruth", False)
        self.min_recall = self.input.param("min_recall", None)
        if self.min_recall is not None:
            self.min_recall = float(self.min_recall)

        # test_mutation-only params
        self.mutation_type = self.input.param("mutation_type", "insert")
        self.mutation_docloader_start = self.input.param(
            "mutation_docloader_start", self.docloader_create_end)
        self.mutation_docloader_end = self.input.param(
            "mutation_docloader_end", 1000000)
        self.mutation_ingestion_sleep = self.input.param(
            "mutation_ingestion_sleep", 300)

        self.setup_cluster()

        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__
        )

    def setup_cluster(self):
        self.load_kv_data()
        self.remote_dataset = self.create_remote_dataset()

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        for dataset_full_name, index_name in getattr(
                self, "_created_indexes", []):
            self.cbas_util.drop_cbas_index(
                self.columnar_cluster, index_name,
                dataset_full_name, if_exists=True)

        super(VectorSearch, self).tearDown()
        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    #########################################################
    # Helper functions

    def load_kv_data(self):
        self.log.info("Creating bucket/scope/collection on remote KV cluster")

        # Load KV data only once per test run
        if VectorSearch._kv_loaded:
            self.bucket_util.get_all_buckets(self.remote_cluster)
            if self.remote_cluster.buckets:
                CollectionBase.create_clients_for_sdk_pool(
                    self, self.remote_cluster)
                for bucket in self.remote_cluster.buckets:
                    scope = self.bucket_util.get_scope_obj(
                        bucket, "_default")
                    collection = self.bucket_util.get_collection_obj(
                        scope, "_default")
                    collection.num_items = bucket.stats.itemCount
                return
            self.log.warning(
                "_kv_loaded was set but the remote cluster has no "
                "buckets - a prior test's teardown likely dropped "
                "them. Reloading from scratch instead of reusing "
                "stale state.")
            VectorSearch._kv_loaded = False

        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)

        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master,
                self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        self.log.info(
            "Loading {0} vector docs into remote cluster using "
            "Docloader".format(self.docloader_create_end -
                               self.docloader_create_start))
        self.load_remote_collections(
            self.remote_cluster,
            create_start_index=self.docloader_create_start,
            create_end_index=self.docloader_create_end,
            template=self.docloader_value_type,
            process_concurrency=self.process_concurrency,
            base_vectors_file_path=self.base_vectors_file_path)

        VectorSearch._kv_loaded = True

    def create_remote_dataset(self):
        self.log.info("Creating remote link and remote collection on EA")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        if not self.cbas_util.connect_link(
                self.columnar_cluster, remote_link.full_name):
            self.fail("Failed to connect remote link")

        self.log.info("Verifying remote collection doc count")
        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)
        remote_dataset = self.cbas_util.get_all_dataset_objs("remote")[0]
        if not self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, remote_dataset.full_name,
                remote_dataset.num_of_items, timeout=self.ingestion_timeout):
            self.fail(
                "Doc count mismatch between remote collection {0} "
                "and KV bucket".format(remote_dataset.full_name))

        self.log.info(
            "Analyzing dataset {0}".format(remote_dataset.full_name))
        if not self.cbas_util.create_sample_for_analytics_collections(
                self.columnar_cluster, remote_dataset.full_name,
                sample_size="high", sample_seed=1000, analytics=False):
            self.fail("ANALYZE DATASET failed for {0}".format(
                remote_dataset.full_name))

        return remote_dataset

    def create_standalone_vector_dataset(self, doc_template_params,
                                         no_of_docs=10000):
        """
        Creates a standalone (Columnar-native) collection and loads
        no_of_docs multi-vector docs generated by MultiVecProduct
        (pytests/Columnar/templates/crudTemplate/
        multi_vec_docgen_template.py) with doc_template_params into
        it. Used by the P2 tests below that need a doc shape
        (multiple embedding fields, heterogeneous/missing/null
        embeddings, variable dimension, tags array) the SIFT-loaded
        self.remote_dataset doesn't have - same standalone-collection
        pattern as HeterogeneousIndexTest.
        create_load_documents_standalone_collection() in
        heterogeneous_index.py.
        """
        primary_key = {"id": "string"}
        dataset_obj = self.cbas_util.create_standalone_dataset_obj(
            self.columnar_cluster, primary_key=primary_key)[0]
        if not self.cbas_util.create_standalone_collection(
                self.columnar_cluster, dataset_obj.name,
                dataverse_name=dataset_obj.dataverse_name,
                database_name=dataset_obj.database_name,
                primary_key=primary_key):
            self.fail("Failed to create standalone collection {0}".format(
                dataset_obj.name))
        if not self.cbas_util.load_doc_to_standalone_collection(
                self.columnar_cluster, dataset_obj.name,
                dataset_obj.dataverse_name, dataset_obj.database_name,
                no_of_docs=no_of_docs, doc_template="multi_vec",
                doc_template_params=doc_template_params):
            self.fail("Failed to load {0} docs into {1}".format(
                no_of_docs, dataset_obj.name))

        self.log.info(
            "Analyzing dataset {0}".format(dataset_obj.full_name))
        if not self.cbas_util.create_sample_for_analytics_collections(
                self.columnar_cluster, dataset_obj.full_name,
                sample_size="high", sample_seed=1000, analytics=False):
            self.fail("ANALYZE DATASET failed for {0}".format(
                dataset_obj.full_name))
        return dataset_obj

    def _create_and_track_vector_index(self, dataset, vector_field,
                                       dimension, similarity,
                                       index_name=None,
                                       require_success=True, **kwargs):
        """
        Thin wrapper around cbas_util.create_vector_index() shared by
        every test in this class that builds a vector index. Tracks
        (dataset, index_name) in self._created_indexes so tearDown()
        drops it. create_vector_index() logs the real error via
        self.log.error() on any failure regardless, so it's always
        safe to call whether or not expected_error is set.

        require_success (default True) controls what self.expected_error
        applies to:
          - True (test_create_vector_index/test_recall/test_mutation/
            test_different_dimension_embeddings_qvec/most P2 tests -
            anything where THIS create is always expected to
            succeed, even when self.expected_error is set for a
            later step in the same test, e.g. the ANN query run
            against the resulting index): validate_error_msg is
            forced off, so create_vector_index() is judged purely on
            status=="success"; fail the test if it didn't succeed.
          - False (the P2 negative/edge-case tests where
            self.expected_error describes how THIS create itself is
            expected to fail): validate_error_msg=bool(
            self.expected_error) is used, so create_vector_index()
            enforces that expected_error when set, and otherwise
            (expected_error not set yet) don't assert either way -
            create_vector_index() already logs the actual result via
            self.log.error()/self.log.info() internally, so nothing
            extra is logged here.
        """
        index_name = index_name or self.cbas_util.format_name(
            "idx_{0}".format(vector_field))
        error_holder = kwargs.pop("error_holder", {})
        validate_error_msg = (not require_success) and bool(
            self.expected_error)
        result = self.cbas_util.create_vector_index(
            self.columnar_cluster, index_name, dataset.full_name,
            vector_field, dimension, similarity,
            validate_error_msg=validate_error_msg,
            expected_error=self.expected_error, error_holder=error_holder,
            **kwargs)
        self.log.info(
            f"Create vector index status: {error_holder.get('status')}, result: {error_holder.get('errors')}")
        self._created_indexes.append((dataset.full_name, index_name))

        if require_success:
            if not result:
                self.fail(
                    "Failed to create vector index {0} (field={1}, "
                    "dimension={2}) on {3}".format(
                        index_name, vector_field, dimension,
                        dataset.full_name))
        elif self.expected_error:
            if not result:
                self.fail(
                    "Vector index creation (field={0}, dimension={1}) "
                    "on {2} did not fail with the expected error: "
                    "{3}. Actual response: status={4}, errors={5}"
                    .format(
                        vector_field, dimension, dataset.full_name,
                        self.expected_error, error_holder.get("status"),
                        error_holder.get("errors")))
        return index_name, result

    @staticmethod
    def generate_random_vector(dimension, min_value=0, max_value=255):
        """
        Generates a random query vector (qvec) of the given dimension,
        with each component a random integer in [min_value, max_value].
        Mirrors the SIFT embedding domain (128 floats, each a whole
        number in 0.0-255.0).
        """
        return [random.randint(min_value, max_value)
                for _ in range(dimension)]

    @staticmethod
    def read_bvecs(path, count, offset=0):
        """
        Reads `count` records starting at record index `offset` from a
        .bvecs-format file (BIGANN base/query vectors). Each record is
        a little-endian int32 dimension prefix followed by `dim`
        unsigned bytes, one per vector component (see bigANN.md / the
        TEXMEX .bvecs format in the vector-search reference repo).
        Returns a list of `count` (or fewer, at EOF) float lists.
        """
        vectors = []
        with open(path, "rb") as f:
            if offset:
                dim = struct.unpack("<i", f.read(4))[0]
                record_size = 4 + dim
                f.seek(offset * record_size)
            for _ in range(count):
                dim_bytes = f.read(4)
                if len(dim_bytes) < 4:
                    break
                dim = struct.unpack("<i", dim_bytes)[0]
                raw = f.read(dim)
                components = struct.unpack("<" + ("B" * dim), raw)
                vectors.append([float(c) for c in components])
        return vectors

    @staticmethod
    def read_ivecs(path, count, offset=0):
        """
        Same idea as read_bvecs, but for .ivecs-format files (BIGANN
        ground truth / integer vectors): each record is a
        little-endian int32 dimension prefix followed by `dim`
        little-endian int32 components.
        """
        vectors = []
        with open(path, "rb") as f:
            if offset:
                dim = struct.unpack("<i", f.read(4))[0]
                record_size = 4 + dim * 4
                f.seek(offset * record_size)
            for _ in range(count):
                dim_bytes = f.read(4)
                if len(dim_bytes) < 4:
                    break
                dim = struct.unpack("<i", dim_bytes)[0]
                raw = f.read(dim * 4)
                components = struct.unpack("<" + ("i" * dim), raw)
                vectors.append(list(components))
        return vectors

    @staticmethod
    def _normalize_id(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return value

    @classmethod
    def recall_at_k(cls, returned_ids, true_ids, k):
        """
        recall@k = |returned_ids[:k] intersect true_ids[:k]| / k - the
        standard ANN recall metric (membership only, order-blind). See
        test_plan_1.md / vector_search_pipeline.py in the vector-search
        reference repo, and pytests/aGoodDoctor/n1ql.py:compare_result
        for the equivalent already used elsewhere in this repo.
        """
        if k <= 0:
            return 1.0
        returned_top_k = {cls._normalize_id(i) for i in returned_ids[:k]}
        true_top_k = {cls._normalize_id(i) for i in true_ids[:k]}
        return len(returned_top_k & true_top_k) / float(k)

    def validate_doc_vector_alignment(self):
        """
        Sanity-checks the assumption the shipped-BIGANN-ground-truth
        path of test_recall relies on: that loaded doc id `i`
        corresponds to line `i` of bigann_base.bvecs (per the SIFT doc
        schema described in bigANN.md - facet/id values are assigned
        by which slice of the corpus a doc's index falls into). Doc id
        assignment happens server-side in the Sirius Java loader, not
        in this repo, so this can't be verified by reading TAF's
        Python code alone - it's verified here instead, once per run,
        before trusting any recall number computed against the shipped
        idx_<tier>.ivecs ground truth. Not called when knn_groundtruth
        is used, since those ids come from querying this same dataset
        and never need to be reconciled against an externally supplied
        corpus-line numbering.

        Reads the vector stored at bigann_base.bvecs line
        `docloader_create_start` directly off disk, queries the
        dataset for its exact nearest neighbour (an exact self-query
        must return that same id, since the vector *is* that document's
        own embedding), and fails loudly on any mismatch instead of
        letting a silent id<->vector mismatch produce meaningless
        recall numbers.
        """
        check_id = self.docloader_create_start
        base_vectors_path = os.path.join(
            self.base_vectors_file_path, "bigann_base.bvecs")

        expected_vectors = self.read_bvecs(
            base_vectors_path, 1, offset=check_id)
        if not expected_vectors:
            self.fail(
                "Could not read record {0} from {1} to validate the "
                "doc id <-> corpus line assumption test_recall relies "
                "on".format(check_id, base_vectors_path))

        status, _, errors, results, _, _ = self.cbas_util.knn_distance(
            self.columnar_cluster, self.remote_dataset.full_name,
            self.vector_field, expected_vectors[0], 1,
            function_name=self.function_name,
            distance_function=self.distance_function)
        if status != "success":
            self.fail(
                "Doc id <-> vector alignment check query failed: "
                "{0}".format(errors))

        actual_id = results[0] if results else None
        print(f"actual_id: {actual_id}, check_id: {check_id}")
        if self._normalize_id(actual_id) != self._normalize_id(check_id):
            self.fail(
                "Doc id <-> corpus line alignment check failed: "
                "querying with the exact vector stored at {0} line "
                "{1} returned closest id {2}, expected {1}. Recall "
                "numbers computed against the shipped BIGANN ground "
                "truth (or against bigann_base.bvecs offsets in "
                "general) would be meaningless without this holding - "
                "check the SIFTLoader's id assignment convention "
                "before re-running.".format(
                    base_vectors_path, check_id, actual_id))

    def generate_knn_groundtruth(self, query_vectors):
        """
        Brute-force ground truth via knn_distance() (exact, index-free
        vector_distance()/cosine_similarity()/etc. scan), one query at
        a time. Used in place of the shipped BIGANN idx_*.ivecs ground
        truth when:
          - distance_function isn't in L2_SIMILARITY_FAMILY (the
            shipped ground truth is only valid for the metric it was
            computed under - see bigANN.md / test_plan_1.md), or
          - knn_groundtruth=True was explicitly requested, or
          - the loaded doc range doesn't exactly match a known BIGANN
            tier size, so no matching idx_<tier>.ivecs file exists.
        """
        self.log.info(
            "Generating brute-force ground truth for {0} queries via "
            "knn_distance() (function_name={1}, distance_function={2}, "
            "k={3})".format(
                len(query_vectors), self.function_name,
                self.distance_function, self.k))

        ground_truths = []
        for i, qvec in enumerate(query_vectors):
            status, _, errors, results, _, _ = self.cbas_util.knn_distance(
                self.columnar_cluster, self.remote_dataset.full_name,
                self.vector_field, qvec, self.k,
                function_name=self.function_name,
                distance_function=self.distance_function)
            if status != "success":
                self.fail(
                    "Brute-force ground truth KNN query #{0} on {1} "
                    "failed: {2}".format(
                        i, self.remote_dataset.full_name, errors))
            ground_truths.append(results)
        return ground_truths

    def check_vector_index_used(self, query_vectors):
        """
        Runs EXPLAIN on an ANN query
        """

        distance_args = [
            'i.{0}'.format(self.vector_field), 'qvec',
            '"{0}"'.format(self.distance_function)]
        if self.min_probe_fraction is not None:
            distance_args.append(str(self.min_probe_fraction))
            if self.k_multiplier is not None:
                distance_args.append(str(self.k_multiplier))

        explain_statement = (
            'LET qvec = {0}\n'
            'SELECT VALUE i.{1}\n'
            'FROM {2} i\n'
            '{3}'
            'ORDER BY ann_distance({4})\n'
            'LIMIT {5};'
        ).format(
            json.dumps(query_vectors[0]), "id",
            self.remote_dataset.full_name,
            'WHERE {0}\n'.format(self.where_clause) if self.where_clause
            else '', ', '.join(distance_args), self.k)

        index_used = self.cbas_util.verify_index_used(
            self.columnar_cluster, explain_statement, index_used=True,
            index_name=self.index_name)
        if not index_used:
            self.log.error(
                "EXPLAIN didn't confirm vector index {0} is used for "
                "the ANN query on {1}: {2}".format(
                    self.index_name, self.remote_dataset.full_name,
                    explain_statement))
        return index_used

    #########################################################

    def test_create_vector_index(self):
        """
        Validate vector index creation
        """
        self.index_name, _ = self._create_and_track_vector_index(
            self.remote_dataset, self.vector_field, self.dimension,
            self.similarity, index_type=self.index_type,
            include_fields=self.include_fields,
            train_list_fraction=self.train_list_fraction,
            quantization=self.quantization, epsilon=self.epsilon,
            num_clusters=self.num_clusters,
            cross_pollination_m=self.cross_pollination_m,
            rng_factor=self.rng_factor,
            extra_with_params=self.extra_with_params)

        if not self.expected_error and not self.cbas_util.verify_vector_index_present_in_Metadata(
                self.columnar_cluster, self.remote_dataset.name,
                self.index_name, dimension=self.dimension,
                similarity=self.similarity,
                include_fields=self.include_fields,
                train_list_fraction=self.train_list_fraction,
                quantization=self.quantization, epsilon=self.epsilon,
                num_clusters=self.num_clusters,
                cross_pollination_m=self.cross_pollination_m,
                rng_factor=self.rng_factor):
            self.fail(
                "Vector index {0} not found/mismatched in Metadata.Index "
                "for {1}".format(
                    self.index_name, self.remote_dataset.full_name))

    def test_knn_search(self):
        """
        Validate exact KNN vector search via knn_distance() (which
        builds/executes a query using either vector_distance(field,
        qvec, "<distance_function>") or a distance-specific function
        like cosine_similarity(field, qvec)). No vector index is
        required, this is a brute-force scan.
        """
        qvec = self.generate_random_vector(self.dimension)

        status, _, errors, results, _, _ = self.cbas_util.knn_distance(
            self.columnar_cluster, self.remote_dataset.full_name,
            self.vector_field, qvec, self.k,
            function_name=self.function_name,
            distance_function=self.distance_function)

        if self.expected_error:
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, self.expected_error):
                self.fail(
                    "KNN search on {0} did not fail with the expected "
                    "error: {1}, got {2}".format(
                        self.remote_dataset.full_name,
                        self.expected_error, results))
            return

        if status != "success":
            self.fail("KNN search on {0} failed: {1}".format(
                self.remote_dataset.full_name, errors))

        if len(results) != self.k:
            self.fail(
                "Expected {0} results from KNN search on {1}, got "
                "{2}".format(
                    self.k, self.remote_dataset.full_name, len(results)))

    def test_ann_search(self):
        """
        Validate the ann_distance() function and its optional
        min_probe_fraction/k_multiplier parameters. No vector index is
        required/created - this validates the function/parameters, not
        ANN search recall/quality.
        """
        qvec = self.generate_random_vector(self.dimension)

        status, _, errors, results, _, _ = self.cbas_util.ann_distance(
            self.columnar_cluster, self.remote_dataset.full_name,
            self.vector_field, qvec, self.k,
            distance_function=self.distance_function,
            min_probe_fraction=self.min_probe_fraction,
            k_multiplier=self.k_multiplier)

        if self.expected_error:
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, self.expected_error):
                self.fail(
                    "ANN search on {0} did not fail with the expected "
                    "error: {1}, got {2}".format(
                        self.remote_dataset.full_name,
                        self.expected_error, results))
            return

        if status != "success":
            self.fail("ANN search on {0} failed: {1}".format(
                self.remote_dataset.full_name, errors))

        if len(results) != self.k:
            self.fail(
                "Expected {0} results from ANN search on {1}, got "
                "{2}".format(
                    self.k, self.remote_dataset.full_name, len(results)))

    def test_recall(self):
        """
        Computes ANN search recall@k (and top-1 accuracy) for the
        vector index built on the already-loaded BIGANN corpus subset
        (docloader_create_start/end, loaded during setUp), against
        ground truth resolved as follows:
          - the BIGANN-shipped idx_<tier>.ivecs file, when
            distance_function is in the euclidean/L2 family AND the
            loaded doc count (docloader_create_end -
            docloader_create_start) exactly matches a known tier in
            DATASET_TIER_DOC_COUNTS (both required for the shipped
            ground truth to be a valid answer key - see bigANN.md /
            test_plan_1.md in the vector-search reference repo);
          - otherwise, a brute-force per-query ground truth generated
            on the fly via generate_knn_groundtruth()/knn_distance().

        recall@k = |returned_ids intersect true_top_k_ids| / k,
        averaged across num_queries query vectors (see recall_at_k()).
        Mean recall/accuracy are always logged; they're only asserted
        pass/fail when min_recall is set on the conf line - by default
        this test is report-only, since a meaningful recall bar is
        config/hardware dependent.
        """
        query_vectors_path = os.path.join(
            self.base_vectors_file_path, "bigann_query.bvecs")
        self.log.info(
            "Reading {0} query vectors from {1}".format(
                self.num_queries, query_vectors_path))
        query_vectors = self.read_bvecs(
            query_vectors_path, self.num_queries)
        if len(query_vectors) < self.num_queries:
            self.fail(
                "Expected {0} query vectors from {1}, only found "
                "{2}".format(
                    self.num_queries, query_vectors_path,
                    len(query_vectors)))
        print(f"Query vectors: {query_vectors}")

        # ---- build the vector index the ANN queries below will use ----
        self.index_name = self.cbas_util.format_name(
            f"idx_{self.vector_field}")
        if self.create_vector_index:
            self._create_and_track_vector_index(
                self.remote_dataset, self.vector_field, self.dimension,
                self.similarity, index_name=self.index_name,
                index_type=self.index_type,
                include_fields=self.include_fields,
                train_list_fraction=self.train_list_fraction,
                quantization=self.quantization, epsilon=self.epsilon,
                num_clusters=self.num_clusters,
                cross_pollination_m=self.cross_pollination_m,
                rng_factor=self.rng_factor, timeout=3600,
                analytics_timeout=3600)

        # ---- verify EXPLAIN shows the vector index will be used ----
        if not self.check_vector_index_used(query_vectors):
            self.log.critical(
                "EXPLAIN didn't confirm vector index {0} is used for the "
                "ANN query on {1}".format(
                    self.index_name, self.remote_dataset.full_name))

        # ---- resolve ground truth ----
        loaded_doc_count = (
            self.docloader_create_end - self.docloader_create_start)
        tier_label = next(
            (label for label, count in self.DATASET_TIER_DOC_COUNTS.items()
             if count == loaded_doc_count), None)

        tier_mismatch = tier_label is None
        metric_unsupported = self.distance_function.lower() not in \
            self.L2_SIMILARITY_FAMILY
        use_knn_groundtruth = (
            self.knn_groundtruth or tier_mismatch or metric_unsupported)

        if not self.knn_groundtruth:
            if tier_mismatch:
                self.log.warning(
                    "Loaded doc range [{0}, {1}) ({2} docs) doesn't "
                    "exactly match a known BIGANN tier ({3}) - falling "
                    "back to brute-force ground truth instead of a "
                    "shipped idx_*.ivecs file".format(
                        self.docloader_create_start,
                        self.docloader_create_end, loaded_doc_count,
                        sorted(self.DATASET_TIER_DOC_COUNTS.values())))
            elif metric_unsupported:
                self.log.info(
                    "distance_function={0} is not in the euclidean/L2 "
                    "family the shipped BIGANN ground truth was computed "
                    "under - generating our own brute-force ground truth "
                    "instead (pass knn_groundtruth=True explicitly to "
                    "silence this)".format(self.distance_function))

        if use_knn_groundtruth:
            ground_truths = self.generate_knn_groundtruth(query_vectors)
        else:
            self.validate_doc_vector_alignment()

            gt_path = os.path.join(
                self.base_vectors_file_path,
                "idx_{0}.ivecs".format(tier_label))
            self.log.info("Reading ground truth from {0}".format(gt_path))
            ground_truths = self.read_ivecs(gt_path, self.num_queries)
            if len(ground_truths) < self.num_queries:
                self.fail(
                    "Expected {0} ground truth rows from {1}, only "
                    "found {2}".format(
                        self.num_queries, gt_path, len(ground_truths)))

        print(f"Ground truths: {ground_truths}")

        # ---- run ANN queries and score recall@k / top-1 accuracy ----
        recalls = []
        accuracies = []
        for i, qvec in enumerate(query_vectors):
            print(
                f"\nRunning ANN query #{i} with qvec={qvec}, "
                f"where_clause={self.where_clause}")
            status, _, errors, results, _, _ = self.cbas_util.ann_distance(
                self.columnar_cluster, self.remote_dataset.full_name,
                self.vector_field, qvec, self.k,
                distance_function=self.distance_function,
                min_probe_fraction=self.min_probe_fraction,
                k_multiplier=self.k_multiplier,
                where_clause=self.where_clause)

            if status != "success":
                self.fail(
                    "ANN query #{0} on {1} failed: {2}".format(
                        i, self.remote_dataset.full_name, errors))

            true_ids = ground_truths[i]

            recall_k = self.recall_at_k(results, true_ids, self.k)
            recalls.append(recall_k)
            accuracies.append(
                100 if results and true_ids and
                self._normalize_id(results[0]) ==
                self._normalize_id(true_ids[0]) else 0)
            print(
                f"\nresults={results}, \ntrue_ids={true_ids}, \nrecall@{self.k}={recall_k}, \ntop-1 accuracy={accuracies[-1]}%")

        mean_recall = round(sum(recalls) / float(len(recalls)), 4)
        mean_accuracy = round(
            sum(accuracies) / (float(len(accuracies))*100), 4)

        self.log.info(
            "recall@{0} over {1} queries: mean={2:.4f} min={3:.4f} "
            "max={4:.4f}, mean top-1 accuracy={5:.1f} "
            "[loaded_docs={6} similarity={7} distance_function={8} "
            "quantization={9} train_list_fraction={10} epsilon={11} "
            "num_clusters={12} cross_pollination_m={13} rng_factor={14} "
            "min_probe_fraction={15} k_multiplier={16} "
            "knn_groundtruth={17}]".format(
                self.k, len(recalls), mean_recall, min(recalls),
                max(recalls), mean_accuracy, loaded_doc_count,
                self.similarity, self.distance_function, self.quantization,
                self.train_list_fraction, self.epsilon, self.num_clusters,
                self.cross_pollination_m, self.rng_factor,
                self.min_probe_fraction, self.k_multiplier,
                use_knn_groundtruth))

        if self.min_recall is not None and mean_recall < self.min_recall:
            self.fail(
                "Mean recall@{0} {1:.4f} is below the required "
                "min_recall {2:.4f} [loaded_docs={3} similarity={4} "
                "distance_function={5} quantization={6} "
                "train_list_fraction={7} min_probe_fraction={8} "
                "k_multiplier={9}]".format(
                    self.k, mean_recall, self.min_recall, loaded_doc_count,
                    self.similarity, self.distance_function,
                    self.quantization, self.train_list_fraction,
                    self.min_probe_fraction, self.k_multiplier))

    def test_recall_knn(self):
        """
        Compared KNN and ground truth recall@k for the BIGANN dataset.
        """
        query_vectors_path = os.path.join(
            self.base_vectors_file_path, "bigann_query.bvecs")
        self.log.info(
            "Reading {0} query vectors from {1}".format(
                self.num_queries, query_vectors_path))
        query_vectors = self.read_bvecs(
            query_vectors_path, self.num_queries)
        if len(query_vectors) < self.num_queries:
            self.fail(
                "Expected {0} query vectors from {1}, only found "
                "{2}".format(
                    self.num_queries, query_vectors_path,
                    len(query_vectors)))
        print(f"Query vectors: {query_vectors}")

        # ---- resolve ground truth: idx_<tier>.ivecs only, no ----
        # ---- brute-force fallback - fail if it doesn't apply  ----
        loaded_doc_count = (
            self.docloader_create_end - self.docloader_create_start)
        tier_label = next(
            (label for label, count in self.DATASET_TIER_DOC_COUNTS.items()
             if count == loaded_doc_count), None)

        if self.knn_groundtruth:
            self.fail(
                "test_recall_knn only scores against the shipped "
                "idx_<tier>.ivecs ground truth - knn_groundtruth=True "
                "is not supported, remove it from the conf line")

        if self.distance_function.lower() not in self.L2_SIMILARITY_FAMILY:
            self.fail(
                "distance_function={0} is not in the euclidean/L2 "
                "family the shipped BIGANN ground truth ({1}) was "
                "computed under - test_recall_knn requires one of "
                "{1} and cannot fall back to a brute-force "
                "oracle".format(
                    self.distance_function,
                    sorted(self.L2_SIMILARITY_FAMILY)))

        if tier_label is None:
            self.fail(
                "Loaded doc range [{0}, {1}) ({2} docs) doesn't exactly "
                "match a known BIGANN tier ({3}) - no idx_<tier>.ivecs "
                "ground truth file exists for it and test_recall_knn "
                "cannot fall back to a brute-force oracle".format(
                    self.docloader_create_start, self.docloader_create_end,
                    loaded_doc_count,
                    sorted(self.DATASET_TIER_DOC_COUNTS.values())))

        self.validate_doc_vector_alignment()

        gt_path = os.path.join(
            self.base_vectors_file_path,
            "idx_{0}.ivecs".format(tier_label))
        self.log.info("Reading ground truth from {0}".format(gt_path))
        ground_truths = self.read_ivecs(gt_path, self.num_queries)
        if len(ground_truths) < self.num_queries:
            self.fail(
                "Expected {0} ground truth rows from {1}, only "
                "found {2}".format(
                    self.num_queries, gt_path, len(ground_truths)))

        print(f"Ground truths: {ground_truths}")

        # ---- run KNN queries and score recall@k / top-1 accuracy ----
        recalls = []
        accuracies = []
        for i, qvec in enumerate(query_vectors):
            print(f"\nRunning KNN query #{i} with qvec={qvec}")
            status, _, errors, results, _, _ = self.cbas_util.knn_distance(
                self.columnar_cluster, self.remote_dataset.full_name,
                self.vector_field, qvec, self.k,
                function_name=self.function_name,
                distance_function=self.distance_function)

            if status != "success":
                self.fail(
                    "KNN query #{0} on {1} failed: {2}".format(
                        i, self.remote_dataset.full_name, errors))

            true_ids = ground_truths[i]

            recall_k = self.recall_at_k(results, true_ids, self.k)
            recalls.append(recall_k)
            accuracies.append(
                100 if results and true_ids and
                self._normalize_id(results[0]) ==
                self._normalize_id(true_ids[0]) else 0)
            print(
                f"\nresults={results}, \ntrue_ids={true_ids}, \nrecall@{self.k}={recall_k}, \ntop-1 accuracy={accuracies[-1]}%")

        mean_recall = sum(recalls) / float(len(recalls))
        mean_accuracy = sum(accuracies) / float(len(accuracies))

        self.log.info(
            "KNN recall@{0} over {1} queries: mean={2:.4f} min={3:.4f} "
            "max={4:.4f}, mean top-1 accuracy={5:.1f}% "
            "[loaded_docs={6} tier={7} similarity={8} "
            "distance_function={9} function_name={10} "
            "groundtruth=idx_{7}.ivecs]".format(
                self.k, len(recalls), mean_recall, min(recalls),
                max(recalls), mean_accuracy, loaded_doc_count, tier_label,
                self.similarity, self.distance_function,
                self.function_name))

        if self.min_recall is not None and mean_recall < self.min_recall:
            self.fail(
                "Mean KNN recall@{0} {1:.4f} is below the required "
                "min_recall {2:.4f} [loaded_docs={3} similarity={4} "
                "distance_function={5} function_name={6}]".format(
                    self.k, mean_recall, self.min_recall, loaded_doc_count,
                    self.similarity, self.distance_function,
                    self.function_name))

    def test_mutation(self):
        """
        ANN vector-index recall/accuracy under a mutation applied
        mid-test: loads an initial docloader_create_start/
        docloader_create_end batch (during setUp -> load_kv_data()),
        builds a vector index, confirms via EXPLAIN that the ANN plan
        actually uses it (check_vector_index_used()), and captures ANN
        recall@k / top-1 accuracy - "before". Then applies one
        mutation - insert/upsert/delete, chosen by the mutation_type
        conf param - to [mutation_docloader_start,
        mutation_docloader_end), waits for the KV/CBAS item counts to
        agree again, and re-captures ANN recall@k / top-1 accuracy -
        "after". Prints/logs the before/after comparison and fails
        the test if any ANN/KNN recall@k or top-1 accuracy value
        drifts by more than +/-5% (relative to the before value)
        between the before and after stages.

        mutation_type (conf param, default "insert"):
          - insert: [mutation_docloader_start, mutation_docloader_end)
            are new docs, growing the collection (e.g. 100k -> 1M:
            docloader_create_end=100000, mutation_docloader_start=
            100001, mutation_docloader_end=1000000).
          - upsert: re-writes the embedding of the already-loaded docs
            in that range in place - doc count is unchanged, only
            their vectors change. Since the shipped BIGANN ground
            truth is computed against the *original* corpus, "after"
            recall reflects a ground truth that's now stale for the
            upserted docs - this captures the resulting drop, it
            doesn't compensate for it.
          - delete: removes the already-loaded docs in that range,
            shrinking the collection (e.g. 1M -> 500k:
            docloader_create_end=1000000,
            mutation_docloader_start=500000,
            mutation_docloader_end=1000000).
          For upsert/delete, pick a range that excludes doc id
          docloader_create_start itself (e.g. the upper half of the
          loaded range, as in the examples above) -
          validate_doc_vector_alignment() (used below when scoring
          against the shipped idx_<tier>.ivecs ground truth) depends
          on that doc still holding its original embedding.

        Ground-truth resolution mirrors test_recall() (same helpers -
        generate_knn_groundtruth()/read_ivecs()/
        validate_doc_vector_alignment()); capture_recall() below is a
        local closure over query_vectors just so it can be called once
        per stage instead of duplicating that block. Both ANN
        (ann_distance(), vector-index-backed) and KNN (knn_distance(),
        brute-force/index-free) recall@k / top-1 accuracy
        (recall_at_k()) are captured - before and after the mutation -
        against the same resolved ground truth. The one exception:
        when no shipped idx_<tier>.ivecs applies to the current doc
        count (tier mismatch, unsupported distance_function, or
        knn_groundtruth=True), ground truth itself comes from
        generate_knn_groundtruth() - i.e. from knn_distance() - so KNN
        scoring is skipped for that stage, since scoring knn_distance()
        against a knn_distance()-generated ground truth would just be
        comparing it to itself.
        """
        query_vectors_path = os.path.join(
            self.base_vectors_file_path, "bigann_query.bvecs")
        self.log.info(
            "Reading {0} query vectors from {1}".format(
                self.num_queries, query_vectors_path))
        query_vectors = self.read_bvecs(
            query_vectors_path, self.num_queries)
        if len(query_vectors) < self.num_queries:
            self.fail(
                "Expected {0} query vectors from {1}, only found "
                "{2}".format(
                    self.num_queries, query_vectors_path,
                    len(query_vectors)))

        # ---- build the vector index once, before the mutation ----
        self.index_name = self.cbas_util.format_name(
            f"idx_{self.vector_field}")
        if self.create_vector_index:
            self._create_and_track_vector_index(
                self.remote_dataset, self.vector_field, self.dimension,
                self.similarity, index_name=self.index_name,
                index_type=self.index_type,
                include_fields=self.include_fields,
                train_list_fraction=self.train_list_fraction,
                quantization=self.quantization, epsilon=self.epsilon,
                num_clusters=self.num_clusters,
                cross_pollination_m=self.cross_pollination_m,
                rng_factor=self.rng_factor, timeout=3600,
                analytics_timeout=3600)

        # ---- verify EXPLAIN shows the vector index will be used ----
        if not self.check_vector_index_used(query_vectors):
            self.log.critical(
                "EXPLAIN didn't confirm vector index {0} is used for "
                "the ANN query on {1}".format(
                    self.index_name, self.remote_dataset.full_name))

        def capture_recall(loaded_doc_count, stage):
            # ---- resolve ground truth (test_recall's rule) ----
            tier_label = next(
                (label for label, count in
                 self.DATASET_TIER_DOC_COUNTS.items()
                 if count == loaded_doc_count), None)
            tier_mismatch = tier_label is None
            metric_unsupported = self.distance_function.lower() not in \
                self.L2_SIMILARITY_FAMILY
            use_knn_groundtruth = (
                self.knn_groundtruth or tier_mismatch or
                metric_unsupported)

            if use_knn_groundtruth:
                ground_truths = self.generate_knn_groundtruth(
                    query_vectors)
            else:
                self.validate_doc_vector_alignment()
                gt_path = os.path.join(
                    self.base_vectors_file_path,
                    "idx_{0}.ivecs".format(tier_label))
                self.log.info(
                    "Reading ground truth from {0}".format(gt_path))
                ground_truths = self.read_ivecs(gt_path, self.num_queries)
                if len(ground_truths) < self.num_queries:
                    self.fail(
                        "Expected {0} ground truth rows from {1}, "
                        "only found {2}".format(
                            self.num_queries, gt_path,
                            len(ground_truths)))

            # ---- run ANN queries (vector-index-backed) and, unless ----
            # ---- ground truth itself came from knn_distance() (via  ----
            # ---- generate_knn_groundtruth() - see docstring), KNN   ----
            # ---- queries (brute-force, index-free) too - score both ----
            # ---- recall@k / top-1 accuracy against ground truth     ----
            score_knn = not use_knn_groundtruth
            ann_recalls, ann_accuracies = [], []
            knn_recalls, knn_accuracies = [], []
            for i, qvec in enumerate(query_vectors):
                status, _, errors, results, _, _ = \
                    self.cbas_util.ann_distance(
                        self.columnar_cluster,
                        self.remote_dataset.full_name,
                        self.vector_field, qvec, self.k,
                        distance_function=self.distance_function,
                        min_probe_fraction=self.min_probe_fraction,
                        k_multiplier=self.k_multiplier,
                        where_clause=self.where_clause)

                if status != "success":
                    self.fail(
                        "{0} ANN query #{1} on {2} failed: {3}".format(
                            stage, i, self.remote_dataset.full_name,
                            errors))

                true_ids = ground_truths[i]
                recall_k = self.recall_at_k(results, true_ids, self.k)
                ann_recalls.append(recall_k)
                ann_accuracies.append(
                    100 if results and true_ids and
                    self._normalize_id(results[0]) ==
                    self._normalize_id(true_ids[0]) else 0)

                if score_knn:
                    status, _, errors, results, _, _ = \
                        self.cbas_util.knn_distance(
                            self.columnar_cluster,
                            self.remote_dataset.full_name,
                            self.vector_field, qvec, self.k,
                            function_name=self.function_name,
                            distance_function=self.distance_function)

                    if status != "success":
                        self.fail(
                            "{0} KNN query #{1} on {2} failed: "
                            "{3}".format(
                                stage, i, self.remote_dataset.full_name,
                                errors))

                    recall_k = self.recall_at_k(results, true_ids, self.k)
                    knn_recalls.append(recall_k)
                    knn_accuracies.append(
                        100 if results and true_ids and
                        self._normalize_id(results[0]) ==
                        self._normalize_id(true_ids[0]) else 0)

            mean_ann_recall = round(
                sum(ann_recalls) / float(len(ann_recalls)), 4)
            mean_ann_accuracy = round(
                sum(ann_accuracies) / float(len(ann_accuracies)), 4)
            self.log.info(
                "{0} ANN recall@{1} over {2} queries: mean={3:.4f} "
                "min={4:.4f} max={5:.4f}, mean top-1 accuracy="
                "{6:.1f}% [loaded_docs={7}]".format(
                    stage, self.k, len(ann_recalls), mean_ann_recall,
                    min(ann_recalls), max(ann_recalls),
                    mean_ann_accuracy, loaded_doc_count))

            if score_knn:
                mean_knn_recall = round(
                    sum(knn_recalls) / float(len(knn_recalls)), 4)
                mean_knn_accuracy = round(
                    sum(knn_accuracies) / float(len(knn_accuracies)), 4)
                self.log.info(
                    "{0} KNN recall@{1} over {2} queries: mean={3:.4f} "
                    "min={4:.4f} max={5:.4f}, mean top-1 accuracy="
                    "{6:.1f}% [loaded_docs={7}]".format(
                        stage, self.k, len(knn_recalls), mean_knn_recall,
                        min(knn_recalls), max(knn_recalls),
                        mean_knn_accuracy, loaded_doc_count))
            else:
                mean_knn_recall = None
                mean_knn_accuracy = None
                self.log.info(
                    "{0}: skipping KNN scoring for loaded_docs={1} - "
                    "ground truth came from knn_distance() itself "
                    "(knn_groundtruth={2}, tier_mismatch={3}, "
                    "metric_unsupported={4}), so KNN can't be scored "
                    "against it without comparing knn_distance() to "
                    "itself".format(
                        stage, loaded_doc_count, self.knn_groundtruth,
                        tier_mismatch, metric_unsupported))

            return {
                "loaded_docs": loaded_doc_count,
                "ann": {
                    "recall": mean_ann_recall,
                    "accuracy": mean_ann_accuracy},
                "knn": {
                    "recall": mean_knn_recall,
                    "accuracy": mean_knn_accuracy} if score_knn else None}

        loaded_doc_count = (
            self.docloader_create_end - self.docloader_create_start)
        before = capture_recall(loaded_doc_count, "before {0}".format(
            self.mutation_type))

        # ---- mutate: insert/upsert/delete [mutation_docloader_start, ----
        # ---- mutation_docloader_end) per mutation_type             ----
        mutation_count = (
            self.mutation_docloader_end - self.mutation_docloader_start)
        if self.mutation_type == "insert":
            load_kwargs = dict(
                create_start_index=self.mutation_docloader_start,
                create_end_index=self.mutation_docloader_end)
            after_doc_count = loaded_doc_count + mutation_count
        elif self.mutation_type == "upsert":
            load_kwargs = dict(
                create_percent=0, update_percent=100,
                update_start_index=self.mutation_docloader_start,
                update_end_index=self.mutation_docloader_end)
            after_doc_count = loaded_doc_count
        elif self.mutation_type == "delete":
            load_kwargs = dict(
                create_percent=0, delete_percent=100,
                delete_start_index=self.mutation_docloader_start,
                delete_end_index=self.mutation_docloader_end)
            after_doc_count = loaded_doc_count - mutation_count
        else:
            self.fail(
                "Unknown mutation_type={0} - expected one of "
                "insert/upsert/delete".format(self.mutation_type))

        self.log.info(
            "Applying mutation_type={0} to docs [{1}, {2}) in the "
            "remote collection backing {3}".format(
                self.mutation_type, self.mutation_docloader_start,
                self.mutation_docloader_end,
                self.remote_dataset.full_name))
        self.load_remote_collections(
            self.remote_cluster,
            template=self.docloader_value_type,
            process_concurrency=self.process_concurrency,
            base_vectors_file_path=self.base_vectors_file_path,
            **load_kwargs)

        if self.mutation_type == "insert":
            self.cbas_util.refresh_remote_dataset_item_count(
                self.bucket_util)
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, self.remote_dataset.full_name,
                    self.remote_dataset.num_of_items,
                    timeout=self.ingestion_timeout):
                self.fail(
                    "Doc count mismatch between remote collection {0} "
                    "and KV bucket after the {1} batch".format(
                        self.remote_dataset.full_name, self.mutation_type))
        else:
            self.log.info(
                "mutation_type={0} doesn't give a reliable expected doc "
                "count to wait on - sleeping {1}s to let CBAS catch up "
                "on ingesting the mutation instead".format(
                    self.mutation_type, self.mutation_ingestion_sleep))
            time.sleep(self.mutation_ingestion_sleep)
            self.cbas_util.refresh_remote_dataset_item_count(
                self.bucket_util)
            if (self.mutation_type in ("delete", "upsert")
                    and self.remote_dataset.num_of_items != after_doc_count):
                self.fail(
                    "mutation_type={0}: remote collection {1} doc count "
                    "{2} does not match expected count {3} after the "
                    "mutation batch".format(
                        self.mutation_type,
                        self.remote_dataset.full_name,
                        self.remote_dataset.num_of_items,
                        after_doc_count))

        after = capture_recall(
            after_doc_count, "after {0}".format(self.mutation_type))

        self.log.info(
            "test_mutation ({0}) before vs after [similarity={1} "
            "distance_function={2} k={3}]: before={4} "
            "after={5}".format(
                self.mutation_type, self.similarity,
                self.distance_function, self.k, before, after))

        def format_stage_summary(label, result):
            summary = (
                "{0} ({1} docs): ANN recall@{2}={3:.4f} "
                "accuracy={4:.1f}%".format(
                    label, result["loaded_docs"], self.k,
                    result["ann"]["recall"], result["ann"]["accuracy"]))
            if result["knn"] is not None:
                summary += (
                    ", KNN recall@{0}={1:.4f} accuracy={2:.1f}%".format(
                        self.k, result["knn"]["recall"],
                        result["knn"]["accuracy"]))
            else:
                summary += ", KNN recall=skipped (ground truth was KNN)"
            return summary

        print(
            "\n=== test_mutation ({0}) before/after ANN + KNN recall "
            "+ top-1 accuracy ===\n{1}\n{2}\n".format(
                self.mutation_type,
                format_stage_summary("before", before),
                format_stage_summary("after", after)))

        # ---- before vs after: tolerance-based comparison ----
        # Replaces the old min_recall absolute-threshold assertion:
        # the mutation should not meaningfully change ANN/KNN
        # recall@k or top-1 accuracy, so a before/after value that
        # drifts by more than RECALL_ACCURACY_TOLERANCE (relative to
        # the before value) fails the test.
        RECALL_ACCURACY_TOLERANCE = 0.05  # +/- 5%
        mismatches = []

        def compare_metric(metric_label, before_val, after_val):
            if before_val == 0:
                # No relative baseline to compare against - only an
                # exact match of "still zero" counts as within
                # tolerance.
                if after_val != 0:
                    mismatches.append(
                        "{0}: before={1} after={2}".format(
                            metric_label, before_val, after_val))
                return
            relative_diff = abs(after_val - before_val) / abs(before_val)
            if relative_diff > RECALL_ACCURACY_TOLERANCE:
                mismatches.append(
                    "{0}: before={1} after={2} (drifted {3:.1%}, "
                    "tolerance is {4:.0%})".format(
                        metric_label, before_val, after_val,
                        relative_diff, RECALL_ACCURACY_TOLERANCE))

        compare_metric(
            "ANN recall@{0}".format(self.k),
            before["ann"]["recall"], after["ann"]["recall"])
        compare_metric(
            "ANN top-1 accuracy",
            before["ann"]["accuracy"], after["ann"]["accuracy"])
        if before["knn"] is not None and after["knn"] is not None:
            compare_metric(
                "KNN recall@{0}".format(self.k),
                before["knn"]["recall"], after["knn"]["recall"])
            compare_metric(
                "KNN top-1 accuracy",
                before["knn"]["accuracy"], after["knn"]["accuracy"])

        if mismatches:
            self.fail(
                "test_mutation ({0}): before/after recall/accuracy "
                "drifted beyond the {1:.0%} tolerance [similarity={2} "
                "distance_function={3} k={4}]: {5}".format(
                    self.mutation_type, RECALL_ACCURACY_TOLERANCE,
                    self.similarity, self.distance_function, self.k,
                    "; ".join(mismatches)))

    def test_incorrect_dimensions(self):
        """
        CREATE INDEX <name>
        ON <dataset>(embedding VECTOR)
        TYPE VTREE
        WITH {"dimension": 1, "similarity": "euclidean_squared"}
        EXCLUDE UNKNOWN KEY;
        """
        mismatched_dimension = 1
        self._create_and_track_vector_index(
            self.remote_dataset, self.vector_field, mismatched_dimension,
            self.similarity, require_success=False)

    def test_multiple_embeddings_data(self):
        """
        Data has two embeddings fields with same dimension: embedding1 and embedding2. The vector index is built on embedding1.
        """
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": "embedding1", "dimension": self.dimension,
            "second_embedding_field": "embedding2",
            "second_embedding_dimension": self.dimension})

        self._create_and_track_vector_index(
            dataset, "embedding1", self.dimension, self.similarity,
            require_success=True)

    def test_multiple_dimension_data(self):
        """
        Data has single vector field with different dimensions (128, 256). Create vector index on the field.
        """
        variable_dimensions = (self.dimension, self.dimension * 2)
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "embedding_mode": "variable_dimension",
            "variable_dimensions": variable_dimensions})

        self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            require_success=True)

    def test_invalid_embeddings_ann(self):
        """
        Pass null for vector_field in ann_distance(<vector_field>, <query_vector>,  <distance_function>). Catch the error/warning
        """
        qvec = self.generate_random_vector(self.dimension)
        statement = (
            'LET qvec = {0}\n'
            'SELECT VALUE i.id\n'
            'FROM {1} i\n'
            'ORDER BY ann_distance(null, qvec, "{2}")\n'
            'LIMIT {3};'
        ).format(
            json.dumps(qvec), self.remote_dataset.full_name,
            self.distance_function, self.k)

        status, _, errors, results, _, warnings = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement)
        self.log.info(
            "test_invalid_embeddings_ann: status={0} errors={1} "
            "warnings={2} results={3}".format(
                status, errors, warnings, results))

        if not warnings:
            self.fail(
                "ann_distance(null, qvec, ...) on {0} did not "
                "throw warning".format(
                    self.remote_dataset.full_name,
                    warnings))

    def test_null_query_vector(self):
        """
        Pass qvec = [0,0,0,0,0,0]
        Query:
        LET qvec = [0,0,0,0,0,0]
        SELECT value i.color
        FROM XLhuX84F5w8QCN3wu i
        where i.color='Green'
        ORDER BY ann_distance(embedding, qvec, "L2")
        LIMIT 1;
        """
        qvec = [0, 0, 0, 0, 0, 0]
        status, _, errors, results, _, warnings = self.cbas_util.ann_distance(
            self.columnar_cluster, self.remote_dataset.full_name,
            self.vector_field, qvec, 1,
            distance_function=self.distance_function,
            where_clause="i.color='Green'", field="color")
        self.log.info(
            "test_null_query_vector: qvec={0} status={1} errors={2} "
            "warnings={3} results={4}".format(
                qvec, status, errors, warnings, results))

        if not warnings:
            self.fail(
                "ANN search with an all-zero {0}-dim qvec against "
                "the {1}-dim field {2} on {3} did not warn with "
                .format(
                    len(qvec), self.dimension, self.vector_field,
                    self.remote_dataset.full_name,
                    warnings))

    def test_different_dimension_embeddings_qvec(self):
        """
        embedding: 128 dim
        qvec: 64 dim
        Example Query:
        let qvec=[134, 222, 140, 117, 93, 156, 238, 118, 215, 124, 117, 146, 101, 91, 160, 81, 27, 229, 130, 75, 190, 235, 250, 9, 55, 39, 94, 50, 45, 75, 203, 213, 164, 185, 201, 221, 197, 33, 46, 188, 154, 247, 155, 182, 66, 101, 39, 149, 92, 133, 44, 37, 255, 155, 243, 67, 170, 204, 126, 1, 125, 120, 15, 188]
        SELECT i.id AS id
        FROM yMrt0ddfqD0xcIboW0g1VRXvktXa i
        ORDER BY ann_distance(embedding, qvec, "euclidean_squared")
        LIMIT 10;
        """
        variable_dimensions = (self.dimension, self.dimension * 2)
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "embedding_mode": "variable_dimension",
            "variable_dimensions": variable_dimensions})

        self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            require_success=True)

        qvec_dimension = self.input.param(
            "qvec_dimension", self.dimension // 2)
        qvec = self.generate_random_vector(qvec_dimension)

        status, _, errors, results, _, warnings = self.cbas_util.ann_distance(
            self.columnar_cluster, dataset.full_name,
            self.vector_field, qvec, self.k,
            distance_function=self.distance_function)
        self.log.info(
            "test_different_dimension_embeddings_qvec: "
            "qvec_dimension={0} (field dimension={1}) status={2} "
            "errors={3} warnings={4} results={5}".format(
                qvec_dimension, self.dimension, status, errors, warnings,
                results))

        if self.expected_error:
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, self.expected_error):
                self.fail(
                    "ANN search with a {0}-dim qvec against the "
                    "{1}-dim field {2} on {3} did not fail with the "
                    "expected error: {4}, got {5}".format(
                        qvec_dimension, self.dimension, self.vector_field,
                        dataset.full_name,
                        self.expected_error, results))

    def test_swap_embeddings_qvec_ann(self):
        """
        ann_distance(qvec, embedding, "L2")
        """
        qvec = self.generate_random_vector(self.dimension)
        statement = (
            'LET qvec = {0}\n'
            'SELECT VALUE i.id\n'
            'FROM {1} i\n'
            'ORDER BY ann_distance(qvec, i.{2}, "{3}")\n'
            'LIMIT {4};'
        ).format(
            json.dumps(qvec), self.remote_dataset.full_name,
            self.vector_field, self.distance_function, self.k)

        status, _, errors, results, _, warnings = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement)
        self.log.info(
            "test_swap_embeddings_qvec_ann: status={0} errors={1} "
            "warnings={2} results={3}".format(
                status, errors, warnings, results))

        if self.expected_error:
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, self.expected_error):
                self.fail(
                    "ann_distance(qvec, embedding, ...) (swapped "
                    "argument order) on {0} did not fail with the "
                    "expected error: {1}, got {2}".format(
                        self.remote_dataset.full_name,
                        self.expected_error, results))

    def test_advise_ann_index(self):
        """
        Create vector index with include on color
        explain
        LET qvec = [27, 184, 156, 15, 202, 81, 231, 94, 160, 132, 199, 57, 153, 85, 218, 154, 203, 76, 93, 136, 199, 166, 227, 126, 193, 41, 154, 116, 37, 47, 78, 102, 54, 222, 113, 62, 163, 59, 38, 87, 45, 148, 151, 21, 84, 98, 193, 233, 74, 200, 57, 88, 108, 88, 101, 207, 61, 101, 252, 49, 114, 44, 167, 177, 213, 0, 49, 207, 223, 153, 73, 209, 36, 208, 119, 101, 183, 219, 180, 110, 98, 254, 151, 99, 179, 75, 239, 141, 71, 65, 78, 76, 13, 0, 227, 134, 115, 238, 146, 98, 249, 120, 127, 189, 37, 136, 9, 126, 219, 237, 43, 14, 220, 234, 187, 68, 226, 197, 184, 57, 78, 62, 23, 25, 142, 69, 207, 97]
        SELECT value i.color
        FROM XLhuX84F5w8QCN3wu i
        where i.color='Green'
        ORDER BY ann_distance(embedding, qvec, "dot")
        LIMIT 1;
        You can see the use of index

        Run advise:
        advise
        LET qvec = [27, 184, 156, 15, 202, 81, 231, 94, 160, 132, 199, 57, 153, 85, 218, 154, 203, 76, 93, 136, 199, 166, 227, 126, 193, 41, 154, 116, 37, 47, 78, 102, 54, 222, 113, 62, 163, 59, 38, 87, 45, 148, 151, 21, 84, 98, 193, 233, 74, 200, 57, 88, 108, 88, 101, 207, 61, 101, 252, 49, 114, 44, 167, 177, 213, 0, 49, 207, 223, 153, 73, 209, 36, 208, 119, 101, 183, 219, 180, 110, 98, 254, 151, 99, 179, 75, 239, 141, 71, 65, 78, 76, 13, 0, 227, 134, 115, 238, 146, 98, 249, 120, 127, 189, 37, 136, 9, 126, 219, 237, 43, 14, 220, 234, 187, 68, 226, 197, 184, 57, 78, 62, 23, 25, 142, 69, 207, 97]
        SELECT value i.color
        FROM XLhuX84F5w8QCN3wu i
        where i.color='Green'
        ORDER BY ann_distance(embedding, qvec, "dot")
        LIMIT 1;
        You can see the index in current_indexes
        """
        self.index_name, _ = self._create_and_track_vector_index(
            self.remote_dataset, self.vector_field, self.dimension,
            self.similarity,
            index_name=self.cbas_util.format_name(
                "idx_advise_{0}".format(self.vector_field)),
            index_type=self.index_type, include_fields=["color"])

        qvec = self.generate_random_vector(self.dimension)
        query = (
            'LET qvec = {0}\n'
            'SELECT VALUE i.color\n'
            'FROM {1} i\n'
            "WHERE i.color='Green'\n"
            'ORDER BY ann_distance(i.{2}, qvec, "{3}")\n'
            'LIMIT 1;'
        ).format(
            json.dumps(qvec), self.remote_dataset.full_name,
            self.vector_field, self.distance_function)

        # ---- EXPLAIN - confirm the vector index is used ----
        if not self.cbas_util.verify_index_used(
                self.columnar_cluster, query, index_used=True,
                index_name=self.index_name):
            self.fail(
                "EXPLAIN didn't confirm vector index {0} is used for "
                "the ANN query on {1}".format(
                    self.index_name, self.remote_dataset.full_name))

        # ---- ADVISE - confirm the index shows up in current_indexes ----
        status, _, errors, results, _, warnings = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, "advise " + query)
        self.log.info(
            "test_advise_ann_index: advise status={0} errors={1} "
            "warnings={2} results={3}".format(
                status, errors, warnings, results))
        if status != "success" or not results:
            self.fail("ADVISE query on {0} failed: {1}".format(
                self.remote_dataset.full_name, errors))

        advice = results[0][0].get("advice", {})
        if advice.get("#operator") != "IndexAdvice":
            self.fail(
                "ADVISE response for {0} did not contain an "
                "IndexAdvice operator: {1}".format(
                    self.remote_dataset.full_name, advice))

        current_indexes = advice.get("adviseinfo", {}).get(
            "current_indexes", [])
        self.log.info("current_indexes: {0}".format(current_indexes))
        if not any(self.cbas_util.unformat_name(self.index_name) in
                   str(idx) for idx in current_indexes):
            self.fail(
                "ADVISE current_indexes for {0} did not report index "
                "{1} as already in use: {2}".format(
                    self.remote_dataset.full_name, self.index_name,
                    current_indexes))

    def test_heterogeneous_embeddings(self):
        """
        Data has heterogeneous embeddings: int, bool, 128-dim int32, null
        Create index on the heterogeneous embeddings field. Catch the error.
        """
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "dimension": self.dimension, "embedding_mode": "heterogeneous"})

        self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            require_success=True)

    def test_missing_embeddings_field(self):
        """
        Data has embedding field where its missing in 30% of docs
        Create index on the embedding field. Catch the error.
        """
        missing_probability = self.input.param("missing_probability", 0.3)
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "dimension": self.dimension, "embedding_mode": "missing",
            "missing_probability": missing_probability})

        self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            require_success=True)

    def test_embeddings_field_null(self):
        """
        Data has embedding field where its all null
        Create index on the embedding field. Catch the error.
        """
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "dimension": self.dimension, "embedding_mode": "null"})

        self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            require_success=False)

    def test_include_array(self):
        """
        Data has array field says tags: len: 3-5
        Create index example:
        CREATE INDEX idx_include
        ON testTag(embedding VECTOR)
        INCLUDE (`tags`)
        TYPE VTREE
        WITH {"dimension": 128, "similarity": "euclidean_squared"}
        EXCLUDE UNKNOWN KEY;

        Run query:
        explain text
        LET qvec = [27, 184, 156, 15, 202, 81, 231, 94, 160, 132, 199, 57, 153, 85, 218, 154, 203, 76, 93, 136, 199, 166, 227, 126, 193, 41, 154, 116, 37, 47, 78, 102, 54, 222, 113, 62, 163, 59, 38, 87, 45, 148, 151, 21, 84, 98, 193, 233, 74, 200, 57, 88, 108, 88, 101, 207, 61, 101, 252, 49, 114, 44, 167, 177, 213, 0, 49, 207, 223, 153, 73, 209, 36, 208, 119, 101, 183, 219, 180, 110, 98, 254, 151, 99, 179, 75, 239, 141, 71, 65, 78, 76, 13, 0, 227, 134, 115, 238, 146, 98, 249, 120, 127, 189, 37, 136, 9, 126, 219, 237, 43, 14, 220, 234, 187, 68, 226, 197, 184, 57, 78, 62, 23, 25, 142, 69, 207, 97]
        SELECT ds.tags
        FROM testTag ds
        WHERE "bestseller" IN ds.tags
        ORDER BY ann_distance(ds.embedding, qvec, "l2_squared")
        LIMIT 10;

        Verify idx_include used in the plan
        """
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "dimension": self.dimension, "include_tags": True})

        self.index_name, _ = self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            index_name=self.cbas_util.format_name(
                "idx_include_{0}".format(self.vector_field)),
            index_type=self.index_type, include_fields=["tags"])

        qvec = self.generate_random_vector(self.dimension)
        query = (
            'LET qvec = {0}\n'
            'SELECT ds.tags\n'
            'FROM {1} ds\n'
            'WHERE "bestseller" IN ds.tags\n'
            'ORDER BY ann_distance(ds.{2}, qvec, "{3}")\n'
            'LIMIT 10;'
        ).format(
            json.dumps(qvec), dataset.full_name, self.vector_field,
            self.distance_function)

        if not self.cbas_util.verify_index_used(
                self.columnar_cluster, query, index_used=True,
                index_name=self.index_name):
            self.fail(
                "EXPLAIN didn't confirm vector index {0} (INCLUDE "
                "tags) is used for the ANN query on {1}".format(
                    self.index_name, dataset.full_name))

        status, _, errors, results, _, warnings = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, query)
        self.log.info(
            "test_include_array: status={0} errors={1} warnings={2} "
            "results={3}".format(status, errors, warnings, results))
        if status != "success":
            self.fail(
                "ANN query with an INCLUDE'd tags filter on {0} "
                "failed: {1}".format(dataset.full_name, errors))

    def test_dimension_mismatch(self):
        """
        Data has two embeddings fields with different dimensions: embedding1 (128) and embedding2 (256). 
        The vector index is built on embedding2. Catch the error
        """
        second_dimension = self.dimension * 2
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": "embedding1", "dimension": self.dimension,
            "second_embedding_field": "embedding2",
            "second_embedding_dimension": second_dimension})

        self._create_and_track_vector_index(
            dataset, "embedding2", second_dimension, self.similarity)

    def test_ann_multiple_dimensions(self):
        """
        Example query:
        LET qvec1 = [27, 184, 156, 15, 202, 81, 231, 94, 160, 132, 199, 57, 153, 85, 218, 154, 203, 76, 93, 136, 199, 166, 227, 126, 193, 41, 154, 116, 37, 47, 78, 102, 54, 222, 113, 62, 163, 59, 38, 87, 45, 148, 151, 21, 84, 98, 193, 233, 74, 200, 57, 88, 108, 88, 101, 207, 61, 101, 252, 49, 114, 44, 167, 177, 213, 0, 49, 207, 223, 153, 73, 209, 36, 208, 119, 101, 183, 219, 180, 110, 98, 254, 151, 99, 179, 75, 239, 141, 71, 65, 78, 76, 13, 0, 227, 134, 115, 238, 146, 98, 249, 120, 127, 189, 37, 136, 9, 126, 219, 237, 43, 14, 220, 234, 187, 68, 226, 197, 184, 57, 78, 62, 23, 25, 142, 69, 207, 97], 
        qvec2 = [228, 249, 112, 36, 16, 180, 216, 11, 168, 43, 86, 52, 145, 187, 224, 57, 231, 6, 131, 44, 88, 63, 55, 169, 17, 155, 83, 117, 30, 245, 2, 105, 127, 249, 194, 159, 64, 111, 246, 208, 243, 118, 129, 93, 123, 251, 109, 150, 24, 171, 77, 98, 92, 48, 148, 160, 80, 112, 94, 25, 10, 136, 197, 172, 148, 94, 209, 17, 51, 98, 230, 136, 156, 71, 129, 20, 124, 117, 196, 214, 219, 199, 67, 199, 221, 41, 164, 150, 206, 178, 234, 23, 244, 214, 139, 87, 112, 46, 89, 49, 154, 46, 62, 198, 156, 0, 73, 181, 91, 167, 205, 167, 209, 7, 150, 169, 200, 144, 199, 50, 174, 209, 186, 236, 186, 129, 30, 185, 171, 114, 139, 237, 164, 43, 172, 101, 81, 93, 55, 247, 99, 249, 41, 55, 193, 250, 250, 254, 117, 175, 234, 211, 139, 113, 238, 174, 0, 140, 187, 79, 37, 18, 8, 216, 106, 83, 113, 144, 3, 139, 56, 186, 73, 111, 199, 47, 151, 193, 89, 149, 204, 69, 134, 159, 68, 132, 186, 129, 244, 73, 160, 237, 109, 103, 11, 4, 193, 73, 138, 46, 115, 18, 4, 120, 6, 113, 188, 55, 34, 238, 109, 126, 201, 251, 217, 205, 139, 154, 153, 90, 126, 57, 80, 162, 27, 159, 156, 29, 219, 73, 59, 22, 34, 148, 190, 237, 175, 143, 136, 144, 234, 73, 249, 251, 32, 83, 231, 197, 147, 19, 143, 149, 30, 111, 255, 14]
        SELECT id, ann_distance(`embedding1`,qvec1,"l2_squared") ann1,  ann_distance(`embedding2`,qvec2,"l2_squared") ann2
        FROM multiVec
        ORDER BY  ann_distance(`embedding2`,qvec2,"l2_squared")
        LIMIT 10;
        """
        second_dimension = self.dimension * 2
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": "embedding1", "dimension": self.dimension,
            "second_embedding_field": "embedding2",
            "second_embedding_dimension": second_dimension})

        qvec1 = self.generate_random_vector(self.dimension)
        qvec2 = self.generate_random_vector(second_dimension)
        statement = (
            'LET qvec1 = {0}, qvec2 = {1}\n'
            'SELECT id, ann_distance(`embedding1`,qvec1,"{2}") ann1, '
            'ann_distance(`embedding2`,qvec2,"{2}") ann2\n'
            'FROM {3}\n'
            'ORDER BY ann_distance(`embedding2`,qvec2,"{2}")\n'
            'LIMIT {4};'
        ).format(
            json.dumps(qvec1), json.dumps(qvec2), self.distance_function,
            dataset.full_name, self.k)

        status, _, errors, results, _, warnings = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement)
        self.log.info(
            "test_ann_multiple_dimensions: status={0} errors={1} "
            "warnings={2} results={3}".format(
                status, errors, warnings, results))

        if self.expected_error:
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, self.expected_error):
                self.fail(
                    "Multi-field ann_distance query (embedding1 "
                    "dim={0}, embedding2 dim={1}) on {2} did not "
                    "fail with the expected error: {3}".format(
                        self.dimension, second_dimension,
                        dataset.full_name, self.expected_error))
        else:
            if status != "success":
                self.fail(
                    "Multi-field ann_distance query on {0} failed: "
                    "{1}".format(dataset.full_name, errors))
            elif len(results) != self.k:
                self.fail(
                    "Expected {0} results from multi-field "
                    "ann_distance query on {1}, got {2}".format(
                        self.k, dataset.full_name, len(results)))

    def test_string_query_vector(self):
        """
        qvec = string of floats
        Run ann_distance query
        """
        variable_dimensions = (self.dimension, self.dimension * 2)
        dataset = self.create_standalone_vector_dataset({
            "embedding_field": self.vector_field,
            "embedding_mode": "variable_dimension",
            "variable_dimensions": variable_dimensions})

        self._create_and_track_vector_index(
            dataset, self.vector_field, self.dimension, self.similarity,
            require_success=True)

        qvec = [str(random.uniform(0, 255))
                for _ in range(self.dimension)]

        status, _, errors, results, _, warnings = self.cbas_util.ann_distance(
            self.columnar_cluster, dataset.full_name,
            self.vector_field, qvec, self.k,
            distance_function=self.distance_function)
        self.log.info(
            "test_string_query_vector: qvec(type=str)={0} status={1} "
            "errors={2} warnings={3} results={4}".format(
                qvec, status, errors, warnings, results))

        if self.expected_error:
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, self.expected_error):
                self.fail(
                    "ANN search with a string-typed qvec on {0} did "
                    "not fail with the expected error: {1}".format(
                        self.remote_dataset.full_name,
                        self.expected_error))

    def test_restart_node_during_vector_index_creation(self):
        """
        Restarts a non-CC EA (analytics) node while a vector index
        create is still in flight - the create is expected to fail,
        and that failure is required to match expected_error
        (placeholder here until a live run shows the actual text).

        _create_and_track_vector_index() (tracking + its own
        expected_error check against the normal CBAS status/error
        response, including the "index actually got created despite
        the restart" case) runs on a background thread so the main
        thread is free to restart the node concurrently. Its
        self.fail() raised on that thread wouldn't otherwise fail the
        test, so any exception it raises is captured and re-raised
        here once the thread joins.

        A restarted CBAS node can also make the create fail as a raw
        connection/transport exception instead of a graceful CBAS
        error response - _create_and_track_vector_index() never sees
        that case, so it's checked separately below: only treated as
        the expected failure (silently) if it actually matches
        expected_error, otherwise re-raised so the test fails loudly.
        """
        error = {}
        error_holder = {}

        def create_index():
            try:
                self._create_and_track_vector_index(
                    self.remote_dataset, self.vector_field,
                    self.dimension, self.similarity,
                    index_type=self.index_type,
                    include_fields=self.include_fields,
                    train_list_fraction=self.train_list_fraction,
                    quantization=self.quantization, epsilon=self.epsilon,
                    num_clusters=self.num_clusters,
                    cross_pollination_m=self.cross_pollination_m,
                    rng_factor=self.rng_factor, timeout=600,
                    analytics_timeout=600, error_holder=error_holder)
            except Exception as exc:
                error["exc"] = exc

        create_thread = threading.Thread(target=create_index)
        create_thread.start()

        # Let CREATE INDEX reach the server and start building before
        # pulling a node out from under it.
        time.sleep(self.input.param("restart_delay", 1))

        restart_node = random.choice(
            list(set(self.columnar_cluster.cbas_nodes) -
                 {self.columnar_cluster.cbas_cc_node}) or
            self.columnar_cluster.cbas_nodes)
        self.log.info(
            "Restarting EA node {0} while vector index create on {1} "
            "is in flight".format(
                restart_node.ip, self.remote_dataset.full_name))
        status, result, _ = AnalyticsRestAPI(
            restart_node).restart_analytics_service()
        if not status:
            self.fail(
                "Failed to restart analytics service on EA node "
                "{0}: {1}".format(restart_node.ip, result))

        create_thread.join(
            timeout=self.input.param("create_index_wait_timeout", 600))
        if create_thread.is_alive():
            self.fail(
                "Vector index create on {0} did not finish within the "
                "wait timeout after restarting EA node {1}".format(
                    self.remote_dataset.full_name, restart_node.ip))

        # Actual response captured off create_vector_index() via
        # error_holder, regardless of outcome - use this to fill in
        # expected_error on the conf line once it's known.
        print(
            "Vector index create response after restarting EA node "
            "{0}: status={1}, errors={2}".format(
                restart_node.ip, error_holder.get("status"),
                error_holder.get("errors")))

        if "exc" in error:
            exc = error["exc"]
            if isinstance(exc, self.failureException):
                # _create_and_track_vector_index() already validated
                # the result against expected_error and decided this
                # should fail - that verdict stands as-is.
                raise exc
            # A raw exception (e.g. a connection/transport error from
            # the CBAS node restart) rather than a graceful CBAS error
            # response - only swallow it if it actually matches
            # expected_error.
            if not (self.expected_error and
                    self.expected_error in str(exc)):
                raise exc
        # No exception at all means _create_and_track_vector_index()
        # already confirmed the create failed and matched
        # expected_error (or, if expected_error wasn't set, that it
        # succeeded as required) - nothing further to check here.

    def test_multiple_knn_iterations(self):
        """
        Runs the same KNN (knn_distance(), brute-force/index-free)
        query set repeatedly - num_iterations times (default 100) -
        and scores each iteration's recall@k / top-1 accuracy against
        a ground truth captured once, up front, via
        generate_knn_groundtruth() (i.e. against itself, since
        knn_distance() is an exact/index-free scan). Every iteration
        is expected to reproduce the exact same top-k neighbours -
        recall@k and top-1 accuracy of 1.0, every time; anything less
        would point at non-determinism/instability in the KNN path
        itself. num_queries/k/docloader_create_end are kept small via
        the conf line so num_iterations repeats finish quickly.
        """
        num_iterations = self.input.param("num_iterations", 100)

        query_vectors = [
            self.generate_random_vector(self.dimension)
            for _ in range(self.num_queries)]
        ground_truths = self.generate_knn_groundtruth(query_vectors)

        for iteration in range(num_iterations):
            recalls = []
            accuracies = []
            for i, qvec in enumerate(query_vectors):
                status, _, errors, results, _, _ = \
                    self.cbas_util.knn_distance(
                        self.columnar_cluster, self.remote_dataset.full_name,
                        self.vector_field, qvec, self.k,
                        function_name=self.function_name,
                        distance_function=self.distance_function)
                if status != "success":
                    self.fail(
                        "KNN query #{0} on iteration {1}/{2} against "
                        "{3} failed: {4}".format(
                            i, iteration + 1, num_iterations,
                            self.remote_dataset.full_name, errors))

                true_ids = ground_truths[i]
                recalls.append(self.recall_at_k(results, true_ids, self.k))
                accuracies.append(
                    1 if results and true_ids and
                    self._normalize_id(results[0]) ==
                    self._normalize_id(true_ids[0]) else 0)

            mean_recall = sum(recalls) / float(len(recalls))
            mean_accuracy = sum(accuracies) / float(len(accuracies))
            self.log.info(
                "KNN iteration {0}/{1}: recall@{2}={3:.4f} top-1 "
                "accuracy={4:.4f}".format(
                    iteration + 1, num_iterations, self.k, mean_recall,
                    mean_accuracy))

            if mean_recall != 1.0 or mean_accuracy != 1.0:
                self.fail(
                    "KNN iteration {0}/{1} did not reproduce its "
                    "ground truth exactly: recall@{2}={3:.4f}, top-1 "
                    "accuracy={4:.4f} (both expected 1.0 - "
                    "knn_distance() is an exact/brute-force scan and "
                    "should be deterministic)".format(
                        iteration + 1, num_iterations, self.k, mean_recall,
                        mean_accuracy))
