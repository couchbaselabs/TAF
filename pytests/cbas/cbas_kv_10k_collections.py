import json
import Queue
import threading
import time

import requests
from java.lang import Runtime, System

from cbas.cbas_base import CBASBaseTest
from cb_constants import CbServer
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from CbasLib.CBASOperations import CBASHelper
from collections_helper.collections_spec_constants import MetaConstants, \
    MetaCrudParams


class CBASKVCollectionScale(CBASBaseTest):

    def setUp(self):
        # Improvement 1: 4 Analytics data paths per Analytics node => 4
        # partitions per node (partitions otherwise follow vCPU count).
        # Must be set before the nodes join the cluster, i.e. before base
        # setUp.
        # self.input is only available after the base setUp
        test_input = TestInputSingleton.input
        self.num_cbas_paths = int(test_input.param("num_cbas_paths", 4))
        cbas_paths = ["/data/cbas/{0}".format(i)
                      for i in range(self.num_cbas_paths)]
        # Servers follow services_init order (e.g. kv-kv-cbas-cbas)
        services = [service for cluster_services in
                    test_input.param("services_init",
                                     "kv:n1ql:index").split("|")
                    for service in cluster_services.split("-")]
        for server, server_services in zip(test_input.servers, services):
            if "cbas" in server_services.split(":") and \
                    not server.cbas_path:
                server.cbas_path = str(cbas_paths)
                shell = RemoteMachineShellConnection(server)
                try:
                    shell.execute_command(
                        "chattr -i /data ; mkdir -p {0} ; "
                        "chown -R couchbase:couchbase /data/cbas".format(
                            " ".join(cbas_paths)))
                finally:
                    shell.disconnect()
        super(CBASKVCollectionScale, self).setUp()
        self.cluster = self.cb_clusters.values()[0]
        self.num_buckets = int(self.input.param("num_buckets", 1))
        self.num_scopes = int(self.input.param("num_scopes", 1))
        self.num_collections = int(self.input.param("num_collections", 10000))
        self.num_analytics_collections = int(
            self.input.param("num_analytics_collections", 1000))

    def tearDown(self):
        super(CBASKVCollectionScale, self).tearDown()

    def log_heap(self, tag, force_gc=False):
        """Log JVM heap usage of the testrunner; returns used/max ratio."""
        if force_gc:
            System.gc()
        rt = Runtime.getRuntime()
        used = rt.totalMemory() - rt.freeMemory()
        mx = rt.maxMemory()
        self.log.info("HEAP[%s] used=%dMB max=%dMB (%.0f%%)"
                      % (tag, used // 1048576, mx // 1048576,
                         100.0 * used / mx))
        return float(used) / mx

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

    def configure_analytics_10k_test(self):
        """
        Analytics configuration for the 10k collection test (MB-74181):
        set and verify the cluster wide cbasMemoryQuota (storage settings are
        left at defaults, no restart needed), then verify that every
        Analytics node reports one partition per data path.
        """
        cbas_quota = int(self.input.param("cbas_memory_quota", 90112))
        rest = RestConnection(self.cluster.master)
        if not rest.set_service_mem_quota(
                {CbServer.Settings.CBAS_MEM_QUOTA: cbas_quota}):
            self.fail("Failed to set cbasMemoryQuota to {0}".format(
                cbas_quota))
        applied_quota = rest.get_pools_default().get(
            CbServer.Settings.CBAS_MEM_QUOTA)
        self.assertEqual(
            applied_quota, cbas_quota,
            "cbasMemoryQuota mismatch. Expected: {0}, Actual: {1}".format(
                cbas_quota, applied_quota))
        self.log.info("Updated cbasMemoryQuota: {0} MB".format(applied_quota))

        # GET /analytics/cluster: "partitions" lists data partitions with
        # their node and path (metadata partition -1 is only in
        # "partitionsTopology", so it is not counted here)
        response = self.cbas_util.fetch_analytics_cluster_response(
            self.cluster)
        node_names = {node["nodeId"]: node["nodeName"]
                      for node in response["nodes"]}
        node_paths = dict()
        for partition in response["partitions"]:
            node_paths.setdefault(
                node_names[partition["nodeId"]], []).append(partition["path"])
        self.assertEqual(
            len(node_paths), len(self.cluster.cbas_nodes),
            "Analytics node count mismatch: {0}".format(node_paths))
        for node, paths in node_paths.items():
            self.log.info("Analytics node {0}: {1} partitions, paths: "
                          "{2}".format(node, len(paths), sorted(paths)))
            self.assertEqual(
                len(paths), self.num_cbas_paths,
                "Partition count mismatch on {0}. Expected: {1}".format(
                    node, self.num_cbas_paths))

    def test_create_10k_collections_on_kv_cluster(self):
        def get_name_sort_key(name): return (
            0, int(name.rsplit("-", 1)[1])
        ) if name.rsplit("-", 1)[-1].isdigit() else (1, name)

        self.configure_analytics_10k_test()

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
                scopes[scope_name] = {
                    MetaConstants.REMOVE_DEFAULT_COLLECTION: True,
                    "collections": {
                        "collection-{0}".format(i): {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION:
                                self.num_items}
                        for i in range(self.num_collections)}
                }
            buckets_spec["buckets"][bucket_name] = {"scopes": scopes}

        self.log.info(
            "Creating and loading {0} docs in KV collections".format(
                self.num_items))
        # Improvement 3: distinct key per collection so docs spread across
        # vbuckets instead of all landing in one vbucket
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            self.doc_spec_name)
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.UNIQUE_DOC_KEY_PER_COLLECTION] = True
        self.collectionSetUp(
            self.cluster, load_data=True, buckets_spec=buckets_spec,
            doc_loading_spec=doc_loading_spec)
        self.log_heap("after collectionSetUp", force_gc=True)

        expected = self.num_buckets * self.num_scopes * self.num_collections
        kv_entries = []
        target_buckets = {"bucket-{0}".format(i)
                          for i in range(self.num_buckets)}

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

        # Names are built; drop the 10k-entry spec/tuple list so the
        # testrunner heap is not held by them
        buckets_spec = None
        kv_entries = None
        collections = None
        # The create loop below only uses REST; close the SDK clients so their
        # background threads/buffers don't keep allocating during the loop.
        # (shutdown() resets the pool, so a second call in tearDown is safe)
        try:
            if self.cluster.sdk_client_pool:
                self.cluster.sdk_client_pool.shutdown()
        except Exception as e:
            self.log.warning("SDK client pool shutdown failed: {0}".format(e))
        self.log_heap("after releasing spec/entries + SDK pool", force_gc=True)

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
            # Recycle the HTTP session periodically: a single long-lived
            # Jython SSL session appears to retain memory per request
            recycle_every = max(1, int(
                self.input.param("session_recycle_every", 2500)))
            analytics_collection_names = []
            self.log_heap("before analytics collection creation",
                          force_gc=True)
            for i, kv_entity in enumerate(
                    kv_entities[:self.num_analytics_collections], 0):
                if i % recycle_every == 0:
                    self.log_heap("creating #{0} pre-gc".format(i))
                    if self.log_heap("creating #{0} post-gc".format(i),
                                     force_gc=True) > 0.90:
                        self.fail(
                            "Testrunner JVM heap nearly exhausted while "
                            "creating analytics collection #{0}; aborting "
                            "instead of hanging in GC".format(i))
                if i and i % recycle_every == 0:
                    session.close()
                    session = requests.Session()
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
