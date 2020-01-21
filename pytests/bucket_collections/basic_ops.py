from bucket_collections.collections_base import CollectionBase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient


class BasicOps(CollectionBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.def_bucket = self.bucket_util.buckets[0]

    def create_delete_collections(self):
        """
        1. Create Scope-Collection
        2. Validate '_default' collection values are intact
        3. Load documents into created collection
        4. Validate documents are loaded in new collection
        5. Delete the collection and validate the '_default' collection
           is unaffected
        """
        scope_name = "my_scope_1"
        collection_name = "my_collection_1"
        gen_add = doc_generator(self.key, 0, self.num_items)
        gen_set = doc_generator(self.key, 0, self.num_items, mutate=1,
                                mutation_type='SET')

        self.log.info("Creating scope::collection '%s::%s'"
                      % (scope_name, collection_name))
        self.bucket_util.create_scope(self.cluster.master,
                                      self.def_bucket,
                                      scope_name)
        self.bucket_util.create_collection(self.cluster.master,
                                           self.def_bucket,
                                           scope_name,
                                           collection_name)

        self.bucket_util.create_collection(self.cluster.master,
                                           self.def_bucket,
                                           scope_name,
                                           "my_collection_2")

        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(shell_conn)
        cbstats.get_collections(self.bucket_util.buckets[0])

        self.log.info("Validating the documents in default collection")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.log.info("Load documents into the created collection")
        sdk_client = SDKClient([self.cluster.master],
                               self.def_bucket,
                               scope_name,
                               collection_name)
        while gen_add.has_next():
            key, value = gen_add.next()
            result = sdk_client.crud("create", key, value,
                                     replicate_to=self.replicate_to,
                                     persist_to=self.persist_to,
                                     durability=self.durability_level,
                                     timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Doc create failed for collection: %s"
                                 % result)
                break
        sdk_client.close()
        self.validate_test_failure()

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)

        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_set, "update", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=scope_name,
            collection=collection_name)
        self.task_manager.get_task_result(task)

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)
        self.validate_test_failure()
