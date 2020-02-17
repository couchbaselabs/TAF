from Cb_constants import CbServer
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException


class BasicOps(CollectionBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.scope_name = "my_scope_%d"
        self.collection_name = "my_collection_%d"
        self.def_bucket = self.bucket_util.buckets[0]
        # To override default num_items to '0'
        self.num_items = self.input.param("num_items", 0)

    def test_delete_default_collection(self):
        """
        Test to delete '_default' collection under '_default' scope.

        Params:
        client_type: Supports collection deletion using REST/SDK client.
        data_load: Load data into default collection based load_during_phase.
                   supports 'disabled / before_drop / during_drop'
        """
        self.def_bucket \
            .scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items = self.num_items
        task = None
        client_type = self.input.param("client_type", "sdk").lower()
        data_load = self.input.param("load_data", "disabled")
        load_gen = doc_generator('test_drop_default',
                                 0, self.num_items,
                                 mutate=0,
                                 target_vbucket=self.target_vbucket)

        if data_load in ["before_drop", "during_drop"]:
            self.log.info("Loading %s docs into '%s::%s' collection"
                          % (self.num_items,
                             CbServer.default_scope,
                             CbServer.default_collection))
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, load_gen, "create", self.maxttl,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                scope=CbServer.default_scope,
                collection=CbServer.default_collection,
                suppress_error_table=True)

        # To make sure data_loading done before collection drop
        if data_load == "before_drop":
            self.task_manager.get_task_result(task)
            if task.fail:
                self.log_failure("Doc loading failed for keys: %s"
                                 % task.fail.keys())

        # Data validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.def_bucket)

        # Drop collection phase
        self.log.info("Deleting collection '%s::%s'"
                      % (CbServer.default_scope,
                         CbServer.default_collection))
        if client_type == "sdk":
            client = SDKClient([self.cluster.master], self.def_bucket)
            client.drop_collection(CbServer.default_scope,
                                   CbServer.default_collection)
            client.close()
            BucketUtils.mark_collection_as_dropped(self.def_bucket,
                                                   CbServer.default_scope,
                                                   CbServer.default_collection)
        elif client_type == "rest":
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.def_bucket,
                                             CbServer.default_scope,
                                             CbServer.default_collection)
        else:
            self.log_failure("Invalid client_type '%s'" % client_type)

        # Wait for doc_loading task to complete
        if data_load == "during_drop":
            self.task_manager.get_task_result(task)
            if task.fail:
                self.log.info("Doc loading failed for keys: %s" % task.fail)

        self.bucket_util._wait_for_stats_all_buckets()
        # Validate drop collection using cbstats
        for node in self.cluster_util.get_kv_nodes():
            shell_conn = RemoteMachineShellConnection(node)
            cbstats = Cbstats(shell_conn)
            c_data = cbstats.get_collections(self.def_bucket)
            if c_data["count"] != 0:
                self.log_failure("%s - Expected collection count is '0'."
                                 "Actual: %s" % (node.ip, c_data["count"]))
            if c_data["default_exists"]:
                self.log_failure("%s: _default collection exists in cbstats"
                                 % node.ip)

        # SDK connection to default(dropped) collection to validate failure
        try:
            client = SDKClient([self.cluster.master], self.def_bucket,
                               scope=CbServer.default_scope,
                               collection=CbServer.default_collection)
            result = client.crud("create", "test_key-1", "TestValue")
            if result["status"] is True:
                self.log_failure("CRUD succeeded on deleted collection")
            elif SDKException.RetryReason.KV_COLLECTION_OUTDATED \
                    not in result["error"]:
                self.log_failure("Invalid error '%s'" % result["error"])
            client.close()
        except Exception as e:
            self.log.info(e)

        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.def_bucket)
        self.validate_test_failure()

    def test_create_scopes(self):
        """
        1. Load data into '_default' collection (if required by test)
        2. Create scope(s) under the bucket
        3. Validate the scopes are created properly
        4. Validate '_default' collection is intact
        """
        num_scopes = self.input.param("num_scopes", 1)
        self.def_bucket \
            .scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items = self.num_items

        if self.action_phase == "before_default_load":
            self.create_scopes(self.def_bucket, num_scopes)

        create_gen = doc_generator("scope_create_key",
                                   0, self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   target_vbucket=self.target_vbucket,
                                   mutation_type="ADD",
                                   mutate=1,
                                   key_size=self.key_size)
        update_gen = doc_generator("scope_create_key",
                                   0, self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   target_vbucket=self.target_vbucket,
                                   mutation_type="SET",
                                   mutate=2,
                                   key_size=self.key_size)
        self.log.info("Loading %d docs into '_default' collection"
                      % self.num_items)
        client = SDKClient([self.cluster.master], self.def_bucket)
        while create_gen.has_next():
            key, val = create_gen.next()
            result = client.crud("create", key, val, exp=self.maxttl,
                                 durability=self.durability_level,
                                 timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Doc create failed for '_default' collection")
                break
        client.close()

        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.def_bucket)

        # Perform update mutation
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, update_gen, "update", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)

        # Create scope(s) while CRUDs are running in background
        if self.action_phase == "during_default_load":
            self.create_scopes(self.def_bucket, num_scopes)

        # Validate drop collection using cbstats
        for node in self.cluster_util.get_kv_nodes():
            shell_conn = RemoteMachineShellConnection(node)
            cbstats = Cbstats(shell_conn)
            scope_data = cbstats.get_scopes(self.def_bucket)
            c_data = cbstats.get_collections(self.def_bucket)
            if c_data["count"] != 1:
                self.log_failure("%s - Expected scope count is '1'."
                                 "Actual: %s" % (node.ip, c_data["count"]))
            if not c_data["default_exists"]:
                self.log_failure("%s: _default collection missing in cbstats"
                                 % node.ip)

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(task)
        self.validate_test_failure()

    def test_create_collections(self):
        """
        1. Load data into '_default' collection (if required by test)
        2. Create collection(s) under the specific 'scope'
        3. Validate the collections are created properly
        4. Validate '_default' collection is intact
        """

        num_collections = self.input.param("num_collections", 1)
        use_default_scope = self.input.param("use_default_scope", False)
        use_scope_name_for_collection = \
            self.input.param("use_scope_name_for_collection", False)
        scope_name = CbServer.default_scope
        self.def_bucket \
            .scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items = self.num_items

        collection_with_scope_name = use_scope_name_for_collection
        if use_default_scope:
            collection_with_scope_name = False

        # Create custom scope if not using 'default' scope
        if not use_default_scope:
            scope_name = self.bucket_util.get_random_name()
            self.log.info("Creating scope '%s'" % scope_name)
            BucketUtils.create_scope(self.cluster.master,
                                     self.def_bucket,
                                     {"name": scope_name})

        if self.action_phase == "before_default_load":
            self.create_collections(
                self.def_bucket,
                num_collections,
                scope_name,
                create_collection_with_scope_name=collection_with_scope_name)

        load_gen = doc_generator(self.key, 0, self.num_items)
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)

        # Create collections(s) while CRUDs are running in background
        if self.action_phase == "during_default_load":
            self.create_collections(
                self.def_bucket,
                num_collections,
                scope_name,
                create_collection_with_scope_name=collection_with_scope_name)

        self.task_manager.get_task_result(task)

        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.def_bucket)
        self.validate_test_failure()

    def create_delete_collections(self):
        """
        1. Create Scope-Collection
        2. Validate '_default' collection values are intact
        3. Load documents into created collection
        4. Validate documents are loaded in new collection
        5. Delete the collection and validate the '_default' collection
           is unaffected
        """
        use_scope_name_for_collection = \
            self.input.param("use_scope_name_for_collection", False)
        scope_name = BucketUtils.get_random_name()
        collection_name = scope_name
        if not use_scope_name_for_collection:
            collection_name = BucketUtils.get_random_name()

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
