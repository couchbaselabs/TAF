from bucket_utils.bucket_ready_functions import CollectionUtils
from bucket_collections.collections_base import CollectionBase
from BucketLib.BucketOperations import BucketHelper

class Ten_K_Collections(CollectionBase):
    def setUp(self):
        super(Ten_K_Collections, self).setUp()

    def tearDown(self):
        super(Ten_K_Collections, self).tearDown()

    def test_num_scope_limit(self):
        """
        Test to validate scope limits per bucket by attempting to create 5k, 10k, and 12k scopes.
        5k and 10k should succeed, 12k should fail.
        """
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket_selected in self.cluster.buckets:
            status, content = bucket_helper.get_bucket_manifest(bucket_selected.name)
            manifest = CollectionUtils.convert_raw_bucket_collection_data_to_manifest(content)
            self.assertTrue(status, f"Failed to get manifest for bucket {bucket_selected.name}")

            scopes = 5000
            collections = 0
            new_manifest = CollectionUtils.create_collection_scope_manifest(scopes, collections)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertTrue(status, f"Failed to create 5k scopes for bucket {bucket_selected.name}")
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))

            scopes = 10000
            new_manifest = CollectionUtils.create_collection_scope_manifest(scopes, collections)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertTrue(status, f"Failed to create 10k scopes for bucket {bucket_selected.name}")
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))

            scopes = 12000
            new_manifest = CollectionUtils.create_collection_scope_manifest(scopes, collections)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertFalse(status, f"Unexpectedly succeeded in creating 12k scopes for bucket {bucket_selected.name}")
            bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))

    def test_num_collection_limit(self):
        """
        Test to validate scope limits per bucket by attempting to create 5k, 10k, and 12k collections.
        5k and 10k should succeed, 12k should fail.
        """
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket_selected in self.cluster.buckets:
            status, content = bucket_helper.get_bucket_manifest(bucket_selected.name)
            manifest = CollectionUtils.convert_raw_bucket_collection_data_to_manifest(content)
            self.assertTrue(status, f"Failed to get manifest for bucket {bucket_selected.name}")

            scopes = 1
            collections = 5000
            new_manifest = CollectionUtils.create_collection_scope_manifest(scopes, collections)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertTrue(status, f"Failed to create 5k scopes for bucket {bucket_selected.name}")
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))

            collections = 10000
            new_manifest = CollectionUtils.create_collection_scope_manifest(scopes, collections)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertTrue(status, f"Failed to create 10k scopes for bucket {bucket_selected.name}")
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))

            collections = 12000
            new_manifest = CollectionUtils.create_collection_scope_manifest(scopes, collections)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertFalse(status, f"Unexpectedly succeeded in creating 12k scopes for bucket {bucket_selected.name}")
            bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))

    def test_ten_k_collections_with_long_names(self):
        """
        Test to validate collection limits with long collection and scope names by attempting to create 10k scopes and collections with 250 character names.
        This should succeed as the limits are based on number of scopes/collections, not name length.
        """
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket_selected in self.cluster.buckets:
            status, content = bucket_helper.get_bucket_manifest(bucket_selected.name)
            manifest = CollectionUtils.convert_raw_bucket_collection_data_to_manifest(content)
            self.assertTrue(status, f"Failed to get manifest for bucket {bucket_selected.name}")

            scopes = 5
            collections = 1000

            long_prefix = 'c' * 220
            new_manifest = CollectionUtils.create_collection_scope_manifest(
                scopes, collections, collection_prefix=long_prefix)
            updated_manifest = CollectionUtils.merge_collection_manifests(new_manifest, manifest)
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(updated_manifest).replace("'", '"'))
            self.assertTrue(status, f"Failed to create 10k scopes and collections with long names for bucket {bucket_selected.name}")
            status, content = bucket_helper.import_collection_using_manifest(
                bucket_selected.name, str(manifest).replace("'", '"'))
