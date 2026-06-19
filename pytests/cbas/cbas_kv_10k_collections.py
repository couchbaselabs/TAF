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

    def test_create_10k_collections_on_kv_cluster(self):
        def get_name_sort_key(name): return (
            0, int(name.rsplit("-", 1)[1])
        ) if name.rsplit("-", 1)[-1].isdigit() else (1, name)

        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            self.bucket_spec)
        buckets_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION] = True
        # buckets_spec[MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT] = True
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

            analytics_collection_names = []
            for i, kv_entity in enumerate(
                    kv_entities[:self.num_analytics_collections], 0):
                analytics_collection_name = "analytics_{0}".format(i)
                analytics_collection_names.append(analytics_collection_name)

                self.log.info(
                    "Creating analytics collection: {0}".format(
                        analytics_collection_name))
                if not self.cbas_util.create_dataset(
                        self.cluster, analytics_collection_name, kv_entity,
                        analytics_collection=True):
                    self.fail(
                        "Failed to create analytics collection {0} on {1}".format(
                            analytics_collection_name, kv_entity))

            self.log.info("Connecting link Local")
            if not self.cbas_util.connect_link(self.cluster, "Local"):
                self.fail("Failed to connect link Local")

            for analytics_collection_name in analytics_collection_names:
                self.log.info(
                    "Validating analytics collection count: {0}".format(
                        analytics_collection_name))
                if not self.cbas_util.validate_cbas_dataset_items_count(
                        self.cluster, analytics_collection_name,
                        self.num_items):
                    self.fail(
                        "Item count mismatch for analytics collection {0}. "
                        "Expected: {1}".format(
                            analytics_collection_name, self.num_items))
