import json
import Queue
import threading
import time

import requests

from cbas.cbas_base import CBASBaseTest
from cb_constants import CbServer
from CbasLib.CBASOperations import CBASHelper
from collections_helper.collections_spec_constants import MetaConstants


class CBASKVCollectionScale(CBASBaseTest):

    def setUp(self):
        super(CBASKVCollectionScale, self).setUp()
        self.cluster = self.cb_clusters.values()[0]
        self.num_buckets = int(self.input.param("num_buckets", 1))
        self.num_scopes = int(self.input.param("num_scopes", 1))
        self.num_collections = int(self.input.param("num_collections", 10000))
        self.num_analytics_collections = int(
            self.input.param("num_analytics_collections", 1000))

    def tearDown(self):
        super(CBASKVCollectionScale, self).tearDown()

    def run_cbas_statement(self, session, cbas_helper, statement, timeout=120):
        """
        Run a statement on /analytics/service over a reused keep-alive session.
        Avoids per-statement GET /nodes/self and TCP+TLS handshake that
        execute_statement_on_cbas_util does (~720ms/statement when the
        testrunner is far from the cluster).
        """
        headers = cbas_helper._create_capi_headers(connection="keep-alive")
        params = json.dumps({"statement": statement,
                             "timeout": "{0}s".format(timeout)})
        self.log.debug("Running query on cbas: %s" % statement)
        start = time.time()
        response = session.post(
            cbas_helper.cbas_base_url + "/analytics/service",
            data=params, headers=headers, timeout=timeout + 30, verify=False)
        content = response.json()
        self.log.debug("Query status: %s, took %.0f ms: %s, results: %s"
                       % (content.get("status"),
                          (time.time() - start) * 1000, statement,
                          content.get("results")))
        return content.get("status"), content.get("errors"), \
            content.get("results")

    def test_create_10k_collections_on_kv_cluster(self):
        def get_name_sort_key(name): return (
            0, int(name.rsplit("-", 1)[1])
        ) if name.rsplit("-", 1)[-1].isdigit() else (1, name)

        self.sleep(900, "Pausing for manual Analytics memory configuration")

        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            self.bucket_spec)
        buckets_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION] = True
        # Create all scopes/collections with a single manifest import call
        # instead of one REST call per collection
        buckets_spec[MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT] = True
        buckets_spec["buckets"] = {}

        for bucket_idx in range(self.num_buckets):
            bucket_name = "bucket-{0}".format(bucket_idx)
            scopes = {
                CbServer.default_scope: {
                    MetaConstants.REMOVE_DEFAULT_COLLECTION: True,
                    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 0,
                    "collections": {}
                }
            }
            for scope_idx in range(self.num_scopes):
                scope_name = "scope-{0}".format(scope_idx)
                collections = {}
                for collection_idx in range(self.num_collections):
                    collection_name = "collection-{0}".format(collection_idx)
                    collections[collection_name] = {
                        MetaConstants.NUM_ITEMS_PER_COLLECTION: self.num_items
                    }
                scopes[scope_name] = {
                    MetaConstants.REMOVE_DEFAULT_COLLECTION: True,
                    "collections": collections
                }
            buckets_spec["buckets"][bucket_name] = {"scopes": scopes}

        self.log.info(
            "Creating and loading {0} docs in KV collections".format(
                self.num_items))
        self.collectionSetUp(
            self.cluster, load_data=True, buckets_spec=buckets_spec)

        expected = self.num_buckets * self.num_scopes * self.num_collections
        kv_entries = []
        target_buckets = set(
            ["bucket-{0}".format(i) for i in range(self.num_buckets)])

        for bucket in self.cluster.buckets:
            if bucket.name not in target_buckets:
                continue
            for scope in self.bucket_util.get_active_scopes(bucket):
                collections = self.bucket_util.get_active_collections(
                    bucket, scope.name, only_names=True)
                if scope.name == CbServer.system_scope:
                    continue
                if scope.name == CbServer.default_scope:
                    self.assertEqual(
                        len(collections), 0,
                        "Expected no KV collections under _default scope")
                    continue
                for collection in collections:
                    kv_entries.append((bucket.name, scope.name, collection))

        kv_entries = sorted(
            kv_entries,
            key=lambda entry: (
                get_name_sort_key(entry[0]),
                get_name_sort_key(entry[1]),
                get_name_sort_key(entry[2])))
        kv_entities = [
            CBASHelper.format_name(bucket, scope, collection)
            for bucket, scope, collection in kv_entries
        ]

        self.assertEqual(
            len(kv_entities), expected,
            "Collection creation mismatch. Expected: {0}, Actual: {1}".format(
                expected, len(kv_entities)))

        if self.num_analytics_collections:
            self.assertTrue(
                self.num_analytics_collections <= len(kv_entities),
                "Requested analytics collections: {0}, available KV collections: {1}".format(
                    self.num_analytics_collections, len(kv_entities)))

            self.log.info("Disconnecting link Local")
            if not self.cbas_util.disconnect_link(self.cluster, "Local"):
                self.fail("Failed to disconnect link Local")

            try:
                requests.packages.urllib3.disable_warnings()
            except Exception:
                pass
            cbas_helper = CBASHelper(self.cluster.cbas_cc_node)
            session = requests.Session()
            analytics_collection_names = []
            for i, kv_entity in enumerate(
                    kv_entities[:self.num_analytics_collections], 0):
                analytics_collection_name = "analytics_{0}".format(i)
                analytics_collection_names.append(analytics_collection_name)

                self.log.info(
                    "Creating analytics collection: {0}".format(
                        analytics_collection_name))
                status, errors, _ = self.run_cbas_statement(
                    session, cbas_helper,
                    "create analytics collection {0} on {1};".format(
                        analytics_collection_name, kv_entity))
                if status != "success":
                    self.fail(
                        "Failed to create analytics collection {0} on {1}: "
                        "{2}".format(analytics_collection_name, kv_entity,
                                     errors))

            self.log.info("Connecting link Local")
            if not self.cbas_util.connect_link(self.cluster, "Local", timeout=600, analytics_timeout=600):
                self.fail("Failed to connect link Local")

            self.log.info(
                "Waiting for ingestion to complete across all analytics "
                "datasets")
            if not self.cbas_util.wait_for_ingestion_via_status_api(
                    self.cluster, timeout=1800):
                self.fail(
                    "Ingestion did not complete for all analytics datasets "
                    "within timeout")
                self.sleep(900, "Pausing for manual cbcollect")

            # Validate item count of all analytics collections using
            # parallel count(*) queries (1 query per collection instead of 2)
            num_threads = int(self.input.param("count_validation_threads", 16))
            name_queue = Queue.Queue()
            for analytics_collection_name in analytics_collection_names:
                name_queue.put(analytics_collection_name)
            failed_collections = list()
            lock = threading.Lock()

            def validate_count_worker():
                worker_session = requests.Session()
                while True:
                    try:
                        name = name_queue.get_nowait()
                    except Queue.Empty:
                        return
                    count = None
                    try:
                        status, _, results = self.run_cbas_statement(
                            worker_session, cbas_helper,
                            "select count(*) as cnt from {0};".format(name),
                            timeout=300)
                        if status == "success":
                            count = results[0]["cnt"]
                    except Exception as e:
                        self.log.warning(
                            "Count query failed for {0}: {1}".format(name, e))
                    # Retry via existing util (with retries) only on mismatch
                    if count != self.num_items and \
                            not self.cbas_util.validate_cbas_dataset_items_count(
                                self.cluster, name, self.num_items):
                        with lock:
                            failed_collections.append(name)

            self.log.info(
                "Validating item count of {0} analytics collections using "
                "{1} threads".format(
                    len(analytics_collection_names), num_threads))
            workers = [threading.Thread(target=validate_count_worker)
                       for _ in range(num_threads)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()

            if failed_collections:
                self.fail(
                    "Item count mismatch for {0} analytics collections. "
                    "Expected: {1}. Collections: {2}".format(
                        len(failed_collections), self.num_items,
                        failed_collections))
        self.log.info(
            "Successfully created {0} KV collections and {1} analytics "
            "collections".format(len(kv_entities),
                                 len(analytics_collection_names)))
