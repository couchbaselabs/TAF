from basetestcase import BaseTestCase
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection


class CollectionBase(BaseTestCase):
    def setUp(self):
        super(CollectionBase, self).setUp()

        self.key = 'test_collection'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        self.spec_name = self.input.param("bucket_spec",
                                          "single_bucket.default")

        self.action_phase = self.input.param("action_phase",
                                             "before_default_load")
        self.crud_batch_size = 100
        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            self.durability_level)

        # Initialize cluster using given nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Create bucket(s) and add rbac user
        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            self.spec_name)
        self.bucket_util.create_buckets_using_json_data(buckets_spec)
        self.bucket_util.load_initial_items_per_collection_spec(
            self.task, self.cluster, self.bucket_util.buckets)
        self.bucket_util.add_rbac_user()

        self.cluster_util.print_cluster_stats()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        self.bucket_util.print_bucket_stats()
        self.log.info("=== CollectionBase setup complete ===")

    def create_scopes(self, bucket, num_scopes, scope_name=None):
        """
        Generic function to create required num_of_scopes.

        :param bucket: Bucket object under which the scopes need
                       to be created
        :param num_scopes: Number of scopes to be created under the 'bucket'
        :param scope_name: Generic name to be used for naming a scope.
                           'None' results in creating random scope names
        """
        created_scopes = 0
        while created_scopes < num_scopes:
            if scope_name is None:
                name = self.bucket_util.get_random_name()
            else:
                name = scope_name + "_%s" % created_scopes

            scope_already_exists = \
                name in bucket.scopes.keys() \
                and \
                bucket.scopes[name].is_dropped is False
            scope_created = False
            while not scope_created:
                self.log.info("Creating scope '%s'" % name)
                try:
                    scope_spec = {"name": name}
                    self.bucket_util.create_scope(self.cluster.master,
                                                  bucket,
                                                  scope_spec)
                    if scope_already_exists:
                        raise Exception("Scope with duplicate name is "
                                        "created under bucket %s" % bucket)
                    created_scopes += 1
                    scope_created = True
                except Exception as e:
                    if scope_already_exists and scope_name is None:
                        self.log.info("Scope creation with duplicate name "
                                      "'%s' failed as expected. Will retry "
                                      "with different name")
                    else:
                        self.log.error("Scope creation failed!")
                        raise Exception(e)

    def create_collections(self, bucket, num_collections, scope_name,
                           collection_name=None,
                           create_collection_with_scope_name=False):
        """
        Generic function to create required num_of_collections.

        :param bucket:
            Bucket object under which the scopes need
            to be created
        :param num_collections:
            Number of collections to be created under
            the given 'scope_name'
        :param scope_name:
            Scope under which the collections needs to be created
        :param collection_name:
            Generic name to be used for naming a scope.
            'None' results in creating collection with random names
        :param create_collection_with_scope_name:
            Boolean to decide whether to create the first collection
            with the same 'scope_name'
        """
        created_collections = 0
        target_scope = bucket.scopes[scope_name]
        while created_collections < num_collections:
            if created_collections == 0 \
                    and create_collection_with_scope_name:
                col_name = scope_name
            else:
                if collection_name is None:
                    col_name = BucketUtils.get_random_name()
                else:
                    col_name = collection_name + "_%s" % created_collections

            collection_already_exists = \
                col_name in target_scope.collections.keys() \
                and target_scope.collections[col_name].is_dropped is False

            collection_created = False
            while not collection_created:
                self.log.info("Creating collection '%s::%s'"
                              % (scope_name, col_name))
                try:
                    BucketUtils.create_collection(self.cluster.master,
                                                  bucket,
                                                  scope_name,
                                                  {"name": col_name})
                    if collection_already_exists:
                        raise Exception("Collection with duplicate name "
                                        "got created under bucket::scope "
                                        "%s::%s" % (bucket, scope_name))
                    created_collections += 1
                    collection_created = True
                except Exception as e:
                    if collection_already_exists and collection_name is None:
                        self.log.info("Collection creation with duplicate "
                                      "name '%s' failed as expected. Will "
                                      "retry with different name")
                    else:
                        self.log.error("Collection creation failed!")
                        raise Exception(e)
