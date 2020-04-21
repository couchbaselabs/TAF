from random import sample

from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.durability_helper import DurabilityHelper
from remote.remote_util import RemoteMachineShellConnection


class FlushTests(CollectionBase):
    def setUp(self):
        super(FlushTests, self).setUp()
        self.bucket = self.bucket_util.buckets[0]

    def tearDown(self):
        super(FlushTests, self).tearDown()

    @staticmethod
    def __get_random_doc_ttl_and_durability_level():
        # Max doc_ttl value=2147483648. Reference:
        # docs.couchbase.com/server/6.5/learn/buckets-memory-and-storage/expiration.html
        doc_ttl = sample([0, 30000, 2147483648], 1)[0]
        durability_level = sample(
            BucketUtils.get_supported_durability_levels() + [""], 1)[0]
        return doc_ttl, durability_level

    @staticmethod
    def __get_mutate_spec(doc_ttl, durability):
        return {
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 5,
            MetaCrudParams.COLLECTIONS_TO_DROP: 15,

            MetaCrudParams.SCOPES_TO_DROP: 3,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 2,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 2,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 10,

            MetaCrudParams.SCOPES_TO_RECREATE: 1,
            MetaCrudParams.COLLECTIONS_TO_RECREATE: 5,

            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {
                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 5000,
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 100,
            },

            # Doc_loading task options
            MetaCrudParams.DOC_TTL: doc_ttl,
            MetaCrudParams.DURABILITY_LEVEL: durability,

            MetaCrudParams.RETRY_EXCEPTIONS: [],
            MetaCrudParams.IGNORE_EXCEPTIONS: [],

            # Applies only for DocCrud / SubDocCrud operation
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: 5,
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
        }

    def run_collection_mutatation(self, doc_ttl, durability):
        """
        - Perform Scope-Collection create/drop/re-create
        - Perform doc_ops randomly on few collections

        :param doc_ttl: Doc_ttl value to be considered during doc_loading
        :param durability: Durability_level to be considered during CRUDs
        :return:
        """
        self.log.info("Running collection mutation from template with "
                      "doc_ttl: %s, durability: %s"
                      % (doc_ttl, durability))
        mutate_spec = self.__get_mutate_spec(doc_ttl, durability)
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                mutate_spec,
                mutation_num=0)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")

    def bucket_flush_and_validate(self, bucket_obj):
        """
        - Flush the entire bucket
        - Validate scope/collections are intact post flush

        :param bucket_obj: Target bucket object to flush
        :return: None
        """
        self.log.info("Flushing bucket: %s" % bucket_obj.name)
        self.bucket_util.flush_bucket(self.cluster.master, self.bucket)

        self.log.info("Validating scope/collections mapping and doc_count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Print bucket stats
        self.bucket_util.print_bucket_stats()

    def load_initial_data(self):
        """
        Reload same data from initial_load spec template to validate
        post bucket flush collection stability
        :return: None
        """
        self.log.info("Loading same docs back into collections")
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0)

        # Print bucket stats
        self.bucket_util.print_bucket_stats()

        if doc_loading_task.result is False:
            self.fail("Post flush doc_creates failed")

        self.log.info("Validating scope/collections mapping and doc_count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_flush_bucket_without_mutations(self):
        """
        Performs Bucket flush when no collection/doc CRUDs are running
        Supported params:
        recreate_same_docs - If True, recreates same docs post flush
        mutate_after_recreate - Integer value to run flush-CRUDs in a loop
        """
        recreate_same_docs = self.input.param("recreate_same_docs", False)
        mutate_after_recreate = self.input.param("mutate_after_recreate", 0)

        self.bucket_flush_and_validate(self.bucket)

        if recreate_same_docs:
            self.load_initial_data()

        for index in range(mutate_after_recreate):
            self.log.info("Flush bucket: %s, iteration: %s"
                          % (self.bucket.name, index))
            self.bucket_flush_and_validate(self.bucket)

            doc_ttl, durability_level = \
                self.__get_random_doc_ttl_and_durability_level()
            self.run_collection_mutatation(doc_ttl, durability_level)

    def test_flush_bucket_during_mutations(self):
        """
        Performs flush tests while data_loading is running in background
        Supported params:
        collection_mutations - If True, performs scope/collection create/drop
                               ops during bucket flush
        doc_mutations - If True, performs document CRUDs during bucket flush
        Note: Both the params cannot be False for a valid test
        :return:
        """
        collection_mutations = self.input.param("collection_mutations", True)
        doc_mutation = self.input.param("doc_mutation", True)

        node_dict = dict()
        kv_nodes = self.cluster_util.get_kv_nodes()
        for node in kv_nodes:
            node_dict[node] = dict()
            node_dict[node]["shell"] = RemoteMachineShellConnection(node)
            node_dict[node]["cbstat"] = Cbstats(node_dict[node]["shell"])
            node_dict[node]["scope_stats"] = dict()
            node_dict[node]["collection_stats"] = dict()

            # Fetch scope/collection stats before flush for validation
            node_dict[node]["scope_stats"]["pre_flush"] = \
                node_dict[node]["cbstat"].get_scopes(self.bucket)
            node_dict[node]["collection_stats"]["pre_flush"] = \
                node_dict[node]["cbstat"].get_collections(self.bucket)

        doc_ttl, durability_level = \
            self.__get_random_doc_ttl_and_durability_level()
        mutate_spec = self.__get_mutate_spec(doc_ttl, durability_level)

        doc_mutation_spec = {
            "doc_crud": {
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 100,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 50,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 10,
            },

            # Doc_loading task options
            MetaCrudParams.DOC_TTL: doc_ttl,
            MetaCrudParams.DURABILITY_LEVEL: durability_level,

            MetaCrudParams.RETRY_EXCEPTIONS: [],
            MetaCrudParams.IGNORE_EXCEPTIONS: [],

            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
        }
        if not doc_mutation:
            del mutate_spec["doc_crud"]
            mutate_spec[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 0
        elif not collection_mutations:
            mutate_spec = doc_mutation_spec

        # To avoid printing error table during doc_loading since failures
        # are expected due to documents getting flushed
        mutate_spec[MetaCrudParams.SKIP_READ_ON_ERROR] = True

        self.log.info("Running mutations with doc_ttl: %s, durability: %s"
                      % (doc_ttl, durability_level))
        mutate_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                mutate_spec,
                mutation_num=0,
                async_load=True,
                validate_task=False)

        self.sleep(5, "Wait for mutation task to start")

        self.log.info("Flushing bucket: %s" % self.bucket.name)
        self.bucket_util.flush_bucket(self.cluster.master, self.bucket)

        # Wait for mutation task to complete
        self.task_manager.get_task_result(mutate_task)

        # Validate only the scope/collection hierarchy
        for node in kv_nodes:
            # Fetch scope/collection stats after flush for validation
            node_dict[node]["scope_stats"]["post_flush"] = \
                node_dict[node]["cbstat"].get_scopes(self.bucket)
            node_dict[node]["collection_stats"]["post_flush"] = \
                node_dict[node]["cbstat"].get_collections(self.bucket)

            # Validate pre and post flush stats
            if node_dict[node]["scope_stats"]["pre_flush"] \
                    != node_dict[node]["scope_stats"]["post_flush"]:
                self.log_failure("%s - Scope stats mismatch after flush")
            if node_dict[node]["collection_stats"]["pre_flush"] \
                    != node_dict[node]["collection_stats"]["post_flush"]:
                self.log_failure("%s - Collection stats mismatch after flush")

            # Close node's shell connections
            node_dict[node]["shell"].disconnect()

        # Fails test case in case of any detected failure
        self.validate_test_failure()

    def test_data_post_flush(self):
        """
        Perform multiple doc_mutations post flush operation
        :return:
        """
        mutate_count = self.input.param("mutate_count", 10)
        self.bucket_flush_and_validate(self.bucket)
        self.load_initial_data()
        for mutate_num in range(1, mutate_count+1):
            self.log.info("Performing mutations, iteration: %s" % mutate_num)
            doc_ttl, durability_level = \
                self.__get_random_doc_ttl_and_durability_level()
            mutate_spec = self.__get_mutate_spec(doc_ttl, durability_level)
            mutate_spec[
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 50
            mutate_spec[
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 20
            mutate_spec[
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 10
            mutate_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                mutate_spec,
                mutation_num=mutate_num)
            if mutate_task.result is False:
                self.fail("Collection mutation failed")
