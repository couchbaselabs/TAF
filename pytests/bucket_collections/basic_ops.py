# -*- coding: utf-8 -*-

from Cb_constants import CbServer
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats


class BasicOps(CollectionBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.bucket = self.bucket_util.buckets[0]
        # To override default num_items to '0'
        self.num_items = self.input.param("num_items", 10000)

    def tearDown(self):
        super(BasicOps, self).tearDown()

    def __dockey_data_ops(self, dockey="dockey"):
        target_vb = None
        if self.target_vbucket is not None:
            target_vb = [self.target_vbucket]

        gen_load = doc_generator(dockey, 0, self.num_items,
                                 key_size=self.key_size,
                                 doc_size=self.doc_size,
                                 doc_type=self.doc_type,
                                 vbuckets=self.cluster_util.vbuckets,
                                 target_vbucket=target_vb)

        bucket = self.bucket_util.get_all_buckets()[0]
        for op_type in ["create", "update", "delete"]:
            for _, scope in bucket.scopes.items():
                for _, collection in scope.collections.items():
                    task = self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_load, op_type, self.maxttl,
                        batch_size=20,
                        persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        durability=self.durability_level,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries,
                        scope=scope.name,
                        collection=collection.name)
                    self.task.jython_task_manager.get_task_result(task)
                    if op_type == "create":
                        bucket.scopes[scope.name] \
                            .collections[collection.name].num_items \
                            += self.num_items
                    elif op_type == "delete":
                        bucket.scopes[scope.name] \
                            .collections[collection.name].num_items \
                            -= self.num_items
                    # Doc count validation
                    self.bucket_util._wait_for_stats_all_buckets()
                    # Prints bucket stats after doc_ops
                    self.bucket_util.print_bucket_stats()
                    self.bucket_util \
                        .validate_docs_per_collections_all_buckets()

    def __validate_cas_for_key(self, key, crud_result, known_cas):
        vb = self.bucket_util.get_vbucket_num_for_key(key)
        if crud_result["cas"] in known_cas:
            if vb in known_cas[crud_result["cas"]]:
                self.log_failure(
                    "Duplicate CAS within vb-%s" % vb)
            else:
                known_cas[crud_result["cas"]].append(vb)
        else:
            known_cas[crud_result["cas"]] = [vb]

    def test_delete_default_collection(self):
        """
        Test to delete '_default' collection under '_default' scope.

        Params:
        client_type: Supports collection deletion using REST/SDK client.
        data_load: Load data into default collection based load_during_phase.
                   supports 'disabled / before_drop / during_drop'
        """
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
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                scope=CbServer.default_scope,
                collection=CbServer.default_collection,
                suppress_error_table=True)
            self.bucket.scopes[CbServer.default_scope] \
                .collections[CbServer.default_collection] \
                .num_items += self.num_items

        # To make sure data_loading done before collection drop
        if data_load == "before_drop":
            self.task_manager.get_task_result(task)
            if task.fail:
                self.log_failure("Doc loading failed for keys: %s"
                                 % task.fail.keys())

        # Data validation
        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)

        # Drop collection phase
        self.log.info("Deleting collection '%s::%s'"
                      % (CbServer.default_scope,
                         CbServer.default_collection))
        if client_type == "sdk":
            client = SDKClient([self.cluster.master], self.bucket,
                               compression_settings=self.sdk_compression)
            client.drop_collection(CbServer.default_scope,
                                   CbServer.default_collection)
            client.close()
            BucketUtils.mark_collection_as_dropped(self.bucket,
                                                   CbServer.default_scope,
                                                   CbServer.default_collection)
        elif client_type == "rest":
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.bucket,
                                             CbServer.default_scope,
                                             CbServer.default_collection)
        else:
            self.log_failure("Invalid client_type '%s'" % client_type)

        self.sleep(60)

        # Wait for doc_loading task to complete
        if data_load == "during_drop":
            self.task_manager.get_task_result(task)
            if task.fail:
                self.log.info("Doc loading failed for keys: %s" % task.fail)

        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        # Validate drop collection using cbstats
        for node in self.cluster_util.get_kv_nodes():
            shell_conn = RemoteMachineShellConnection(node)
            cbstats = Cbstats(shell_conn)
            c_data = cbstats.get_collections(self.bucket)
            expected_collection_count = \
                len(self.bucket_util.get_active_collections(
                    self.bucket,
                    CbServer.default_scope,
                    only_names=True))
            if c_data["count"] != expected_collection_count:
                self.log_failure("%s - Expected collection count is '%s'. "
                                 "Actual: %s"
                                 % (node.ip,
                                    expected_collection_count,
                                    c_data["count"]))
            if "_default" in c_data:
                self.log_failure("%s: _default collection exists in cbstats"
                                 % node.ip)

        # SDK connection to default(dropped) collection to validate failure
        try:
            client = SDKClient([self.cluster.master], self.bucket,
                               scope=CbServer.default_scope,
                               collection=CbServer.default_collection,
                               compression_settings=self.sdk_compression)
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
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_create_scopes(self):
        """
        1. Load data into '_default' collection (if required by test)
        2. Create scope(s) under the bucket
        3. Validate the scopes are created properly
        4. Validate '_default' collection is intact
        """
        num_scopes = self.input.param("num_scopes", 1)
        if self.action_phase == "before_default_load":
            BucketUtils.create_scopes(self.cluster, self.bucket, num_scopes)

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
        client = SDKClient([self.cluster.master], self.bucket,
                           compression_settings=self.sdk_compression)
        while create_gen.has_next():
            key, val = create_gen.next()
            result = client.crud("create", key, val, exp=self.maxttl,
                                 durability=self.durability_level,
                                 timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Doc create failed for '_default' collection")
                break
        client.close()
        # Update num_items for default collection
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items

        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)

        # Perform update mutation
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, update_gen, "update", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)

        # Create scope(s) while CRUDs are running in background
        if self.action_phase == "during_default_load":
            BucketUtils.create_scopes(self.cluster, self.bucket, num_scopes)

        # Validate drop collection using cbstats
        for node in self.cluster_util.get_kv_nodes():
            shell_conn = RemoteMachineShellConnection(node)
            cbstats = Cbstats(shell_conn)
            c_data = cbstats.get_collections(self.bucket)
            if c_data["count"] != 1:
                self.log_failure("%s - Expected scope count is '1'."
                                 "Actual: %s" % (node.ip, c_data["count"]))
            if "_default" not in c_data:
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
        collection_with_scope_name = use_scope_name_for_collection
        if use_default_scope:
            collection_with_scope_name = False

        # Create custom scope if not using 'default' scope
        if not use_default_scope:
            scope_name = self.bucket_util.get_random_name()
            self.log.info("Creating scope '%s'" % scope_name)
            BucketUtils.create_scope(self.cluster.master,
                                     self.bucket,
                                     {"name": scope_name})

        if self.action_phase == "before_default_load":
            BucketUtils.create_collections(
                self.cluster,
                self.bucket,
                num_collections,
                scope_name,
                create_collection_with_scope_name=collection_with_scope_name)

        load_gen = doc_generator(self.key, 0, self.num_items)
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)

        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items

        # Create collections(s) while CRUDs are running in background
        if self.action_phase == "during_default_load":
            BucketUtils.create_collections(
                self.cluster,
                self.bucket,
                num_collections,
                scope_name,
                create_collection_with_scope_name=collection_with_scope_name)

        self.task_manager.get_task_result(task)

        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
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
                                      self.bucket,
                                      scope_name)
        self.bucket_util.create_collection(self.cluster.master,
                                           self.bucket,
                                           scope_name,
                                           collection_name)

        self.bucket_util.create_collection(self.cluster.master,
                                           self.bucket,
                                           scope_name,
                                           "my_collection_2")

        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(shell_conn)
        cbstats.get_collections(self.bucket_util.buckets[0])

        self.log.info("Validating the documents in default collection")
        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.log.info("Load documents into the created collection")
        sdk_client = SDKClient([self.cluster.master],
                               self.bucket,
                               scope=scope_name,
                               collection=collection_name,
                               compression_settings=self.sdk_compression)
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
        self.bucket.scopes[scope_name] \
            .collections[collection_name] \
            .num_items += self.num_items

        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)

        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_set, "update", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope_name,
            collection=collection_name)
        self.task_manager.get_task_result(task)

        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)
        self.validate_test_failure()

    def test_similar_keys_in_all_collections(self):
        """
        Use single client to select collections on the fly and insert
        docs under each available collection and validate the results.
        :return:
        """
        doc_gen = doc_generator("test_same_key", 0, self.num_items,
                                key_size=self.key_size,
                                doc_size=self.doc_size,
                                mix_key_size=False,
                                randomize_doc_size=False)
        # Set to keep track of all inserted CAS values
        # Format know_cas[CAS] = list(vb_lists)
        known_cas = dict()

        # Client to insert docs under different collections
        client = SDKClient([self.cluster.master], self.bucket,
                           compression_settings=self.sdk_compression)

        while doc_gen.has_next():
            key, value = doc_gen.next()
            for _, scope in self.bucket.scopes.items():
                for _, collection in scope.collections.items():
                    client.select_collection(scope.name, collection.name)
                    result = client.crud("create", key, value, self.maxttl,
                                         durability=self.durability_level,
                                         timeout=self.sdk_timeout,
                                         time_unit="seconds")
                    if result["status"] is False:
                        self.log_failure("Doc create failed for key '%s' "
                                         "collection::scope %s::%s - %s"
                                         % (key, scope.name, collection.name,
                                            result))
                    else:
                        self.__validate_cas_for_key(key, result, known_cas)
                        collection.num_items += 1

        # Close SDK connection
        client.close()

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_dockey_whitespace_data_ops(self):
        generic_key = "d o c k e y"
        if self.key_size:
            self.key_size = self.key_size - len(generic_key)
            generic_key = generic_key + "_" * self.key_size
        self.__dockey_data_ops(generic_key)

    def test_dockey_binary_data_ops(self):
        generic_key = "d\ro\nckey"
        if self.key_size:
            self.key_size = self.key_size - len(generic_key)
            generic_key = generic_key + "\n" * self.key_size
        self.__dockey_data_ops(generic_key)

    def test_dockey_unicode_data_ops(self):
        generic_key = "\u00CA"
        if self.key_size:
            self.key_size = self.key_size - len(generic_key)
            generic_key = generic_key + "é" * self.key_size
        self.__dockey_data_ops(generic_key)

    def test_doc_key_size(self):
        """
        Insert document key with min and max key size on each available
        collection and validate
        :return:
        """
        min_doc_gen = doc_generator("test_min_key_size", 0, self.num_items,
                                    key_size=1,
                                    doc_size=self.doc_size,
                                    mix_key_size=False,
                                    randomize_doc_size=False)
        max_doc_gen = doc_generator("test_max_key_size", 0, self.num_items,
                                    key_size=245,
                                    doc_size=self.doc_size,
                                    mix_key_size=False,
                                    randomize_doc_size=False)
        # Set to keep track of all inserted CAS values
        # Format know_cas[CAS] = list(vb_lists)
        known_cas = dict()

        # Client to insert docs under different collections
        client = SDKClient([self.cluster.master], self.bucket,
                           compression_settings=self.sdk_compression)

        for doc_gen in [min_doc_gen, max_doc_gen]:
            while doc_gen.has_next():
                key, value = doc_gen.next()
                for _, scope in self.bucket.scopes.items():
                    for _, collection in scope.collections.items():
                        client.select_collection(scope.name, collection.name)
                        result = client.crud("create", key, value, self.maxttl,
                                             durability=self.durability_level,
                                             timeout=self.sdk_timeout,
                                             time_unit="seconds")
                        if result["status"] is False:
                            self.log_failure("Doc create failed for key '%s' "
                                             "collection::scope %s::%s - %s"
                                             % (key,
                                                scope.name,
                                                collection.name,
                                                result))
                        else:
                            self.__validate_cas_for_key(key, result, known_cas)
                            collection.num_items += 1
        # Close SDK connection
        client.close()

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_doc_size(self):
        """
        Insert document with empty content and max size on each available
        collection and validate
        :return:
        """
        # Empty docs
        min_doc_size_gen = doc_generator("test_min_doc_size",
                                         0, self.num_items,
                                         key_size=self.key_size,
                                         doc_size=0,
                                         mix_key_size=False,
                                         randomize_doc_size=False)
        # 20 MB docs
        max_doc_size_gen = doc_generator("test_max_doc_size",
                                         0, self.num_items,
                                         key_size=self.key_size,
                                         doc_size=1024 * 1024 * 20,
                                         mix_key_size=False,
                                         randomize_doc_size=False)
        # Set to keep track of all inserted CAS values
        # Format know_cas[CAS] = list(vb_lists)
        known_cas = dict()

        # Client to insert docs under different collections
        client = SDKClient([self.cluster.master], self.bucket,
                           compression_settings=self.sdk_compression)

        for doc_gen in [min_doc_size_gen, max_doc_size_gen]:
            while doc_gen.has_next():
                key, value = doc_gen.next()
                for _, scope in self.bucket.scopes.items():
                    for _, collection in scope.collections.items():
                        client.select_collection(scope.name, collection.name)
                        result = client.crud("create", key, value, self.maxttl,
                                             durability=self.durability_level,
                                             timeout=self.sdk_timeout,
                                             time_unit="seconds")
                        if result["status"] is False:
                            self.log_failure("Doc create failed for key '%s' "
                                             "collection::scope %s::%s - %s"
                                             % (key,
                                                scope.name,
                                                collection.name,
                                                result))
                        else:
                            self.__validate_cas_for_key(key, result, known_cas)
                            collection.num_items += 1

        # Close SDK connection
        client.close()

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_sub_doc_size(self):
        """
        Insert sub-documents into multiple collections and validate.
        Tests with variable sub_doc size and content
        :return:
        """
        # Empty docs
        min_doc_size_gen = doc_generator("test_min_doc_size",
                                         0, self.num_items,
                                         key_size=self.key_size,
                                         doc_size=0,
                                         mix_key_size=False,
                                         randomize_doc_size=False)
        # 20 MB docs
        max_doc_size_gen = doc_generator("test_max_doc_size",
                                         0, self.num_items,
                                         key_size=self.key_size,
                                         doc_size=1024 * 1024 * 1024 * 20,
                                         mix_key_size=False,
                                         randomize_doc_size=False)
        for _, scope in self.bucket.scopes.items():
            for _, collection in scope.collections.items():
                tasks = list()
                for doc_gen in [min_doc_size_gen, max_doc_size_gen]:
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, self.bucket, doc_gen,
                        "create", self.maxttl,
                        batch_size=200, process_concurrency=1,
                        durability=self.durability_level,
                        compression=self.sdk_compression,
                        timeout_secs=self.sdk_timeout,
                        scope=scope.name,
                        collection=collection.name,
                        suppress_error_table=True))

                for task in tasks:
                    self.task_manager.get_task_result(task)

        self.log.info("Loading sub_docs into the collections")

    def test_create_delete_recreate_collection(self):
        collections = BucketUtils.get_random_collections(
            self.bucket_util.buckets, 10, 10, 1)
        # delete collection
        for bucket_name, scope_dict in collections.iteritems():
            bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                                bucket_name)
            scope_dict = scope_dict["scopes"]
            for scope_name, collection_dict in scope_dict.items():
                collection_dict = collection_dict["collections"]
                for c_name, _ in collection_dict.items():
                    BucketUtils.drop_collection(self.cluster.master,
                                                bucket,
                                                scope_name, c_name)
        # recreate collection
        for bucket_name, scope_dict in collections.iteritems():
            bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                                bucket_name)
            scope_dict = scope_dict["scopes"]
            for scope_name, collection_dict in scope_dict.items():
                collection_dict = collection_dict["collections"]
                for c_name, _ in collection_dict.items():
                    # Cannot create a _default collection
                    if c_name == CbServer.default_collection:
                        continue
                    col_obj = \
                        bucket.scopes[scope_name].collections[c_name]
                    BucketUtils.create_collection(self.cluster.master,
                                                  bucket,
                                                  scope_name,
                                                  col_obj.get_dict_object())
        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_create_delete_recreate_scope(self):
        scope_drop_fails = False
        bucket_dict = BucketUtils.get_random_scopes(
            self.bucket_util.buckets, "all", 1)
        # Delete scopes
        for bucket_name, scope_dict in bucket_dict.items():
            bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                                bucket_name)
            for scope_name, _ in scope_dict["scopes"].items():
                if scope_name == CbServer.default_scope:
                    scope_drop_fails = True
                try:
                    BucketUtils.drop_scope(self.cluster.master,
                                           bucket, scope_name)
                    if scope_drop_fails:
                        raise Exception("default scope deleted")
                except Exception as drop_exception:
                    if scope_drop_fails \
                            and "delete_scope failed" in str(drop_exception):
                        pass
                    else:
                        raise drop_exception

        # Recreate scopes
        for bucket_name, scope_dict in bucket_dict.items():
            bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                                bucket_name)
            for scope_name, _ in scope_dict["scopes"].items():
                # Cannot create a _default scope
                if scope_name == CbServer.default_collection:
                    continue
                BucketUtils.create_scope(self.cluster.master, bucket,
                                         {"name": scope_name})
        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_drop_collection_compaction(self):
        collections = BucketUtils.get_random_collections(
            self.bucket_util.buckets, 10, 10, 1)
        # Delete collection
        for self.bucket_name, scope_dict in collections.iteritems():
            bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                                self.bucket_name)
            scope_dict = scope_dict["scopes"]
            for scope_name, collection_dict in scope_dict.items():
                collection_dict = collection_dict["collections"]
                for c_name, c_data in collection_dict.items():
                    BucketUtils.drop_collection(self.cluster.master, bucket,
                                                scope_name, c_name)
        # Trigger compaction
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        _ = remote_client.wait_till_compaction_end(
            RestConnection(self.cluster.master),
            self.bucket_name,
            timeout_in_seconds=(self.wait_timeout * 10))
        remote_client.disconnect()
        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_create_delete_collection_same_order(self):
        # Create collection in increasing order
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        cb_stat = Cbstats(shell_conn)
        collection_count = 1
        while collection_count < 1000:
            doc_loading_spec = \
                self.bucket_util.get_crud_template_from_package(
                    "def_add_collection")
            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.bucket_util.buckets,
                                                    doc_loading_spec,
                                                    mutation_num=0,
                                                    batch_size=self.batch_size)
            collection_count = cb_stat.get_collections(self.bucket)["count"]
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Delete collections
        while collection_count > 1:
            doc_loading_spec = \
                self.bucket_util.get_crud_template_from_package(
                    "def_drop_collection")
            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.bucket_util.buckets,
                                                    doc_loading_spec,
                                                    mutation_num=0,
                                                    batch_size=self.batch_size)
            collection_count = cb_stat.get_collections(self.bucket)["count"]

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_load_default_collection(self):
        self.delete_default_collection = \
            self.input.param("delete_default_collection", False)
        self.perform_ops = self.input.param("perform_ops", False)
        load_gen = doc_generator('test_drop_default',
                                 0, self.num_items,
                                 mutate=0,
                                 target_vbucket=self.target_vbucket)

        # Add num_items which are about to be loaded into
        # collection's num_items for validation
        self.bucket.scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items += self.num_items

        self.log.info("Loading %s docs into '%s::%s' collection"
                      % (self.num_items,
                         CbServer.default_scope,
                         CbServer.default_collection))
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=2,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection,
            suppress_error_table=True)

        # perform some collection operation
        if self.perform_ops:
            doc_loading_spec = \
                self.bucket_util.get_crud_template_from_package("initial_load")
            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.bucket_util.buckets,
                                                    doc_loading_spec,
                                                    mutation_num=0,
                                                    batch_size=self.batch_size)
        self.task_manager.get_task_result(task)
        # Data validation
        self.bucket_util._wait_for_stats_all_buckets()
        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()
        task = self.task.async_validate_docs(
            self.cluster, self.bucket, load_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=2)
        self.task_manager.get_task_result(task)

        if self.delete_default_collection:
            for bucket in self.bucket_util.buckets:
                BucketUtils.drop_collection(self.cluster.master, bucket)

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_invalid_name_collection(self):
        for _ in range(1000):
            scope_name = BucketUtils.get_random_name(invalid_name=True)
            try:
                status, content = BucketHelper(
                    self.cluster.master).create_scope(
                    self.bucket, scope_name)
                if status is True:
                    self.log_failure("Scope '%s::%s' creation not failed: %s"
                                     % (self.bucket, scope_name, content))
            except Exception as e:
                self.log.debug(e)

        for _ in range(1000):
            collection_name = BucketUtils.get_random_name(invalid_name=True)
            try:
                status, content = BucketHelper(self.cluster.master) \
                    .create_collection(self.bucket, CbServer.default_scope,
                                       collection_name)
                if status is True:
                    self.log_failure(
                        "Collection '%s::%s::%s' creation not failed: %s"
                        % (self.bucket, CbServer.default_scope,
                           collection_name, content))
            except Exception as e:
                self.log.debug(e)

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_load_collection(self):
        """ Initial load collections,
        update and delete docs randomly in collection"""
        run_compaction = self.input.param("compaction", False)
        load_spec = self.input.param("load_spec", "initial_load")
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(load_spec)

        self.bucket_util.run_scenario_from_spec(self.task,
                                                self.cluster,
                                                self.bucket_util.buckets,
                                                doc_loading_spec,
                                                mutation_num=0,
                                                batch_size=self.batch_size)

        if run_compaction:
            compaction_task = self.task.async_compact_bucket(
                self.cluster.master, self.bucket)
            self.task.jython_task_manager.get_task_result(compaction_task)
            # ToDo : Uncomment after fixing this compaction task
            # if compaction_task.result is False:
            #     self.fail("compaction failed")

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_delete_collection_during_load(self):
        """ Get a random collection/scope ,
            delete collection/scope while loading"""
        delete_scope = self.input.param("delete_scope", False)
        exclude_dict = dict()
        if delete_scope:
            exclude_dict = {
                self.bucket.name:
                {"scopes": {
                    CbServer.default_scope: {
                        "collections": {}}}}}
        bucket_dict = BucketUtils.get_random_collections(
            [self.bucket], 1, "all", "all", exclude_from=exclude_dict)
        scope_name = collection_name = None
        scope_dict = bucket_dict[self.bucket.name]["scopes"]
        for t_scope, scope_data in scope_dict.items():
            print(scope_data["collections"])
            if scope_data["collections"]:
                scope_name = t_scope
                collection_name = scope_data["collections"].keys()[0]
                break

        self.num_items = \
            self.bucket \
                .scopes[scope_name] \
                .collections[collection_name] \
                .num_items
        load_gen = doc_generator(self.key, self.num_items, self.num_items * 20)

        self.log.info("Delete collection while load %s: %s"
                      % (scope_name, collection_name))
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create",
            exp=self.maxttl,
            batch_size=200, process_concurrency=1,
            scope=scope_name,
            collection=collection_name,
            sdk_client_pool=self.sdk_client_pool,
            suppress_error_table=True,
            compression=self.sdk_compression,
            print_ops_rate=True, retries=0)

        self.sleep(5)
        self.bucket_util.print_bucket_stats()

        if delete_scope:
            # Attempt deleting scope only if it is NOT default scope
            if scope_name != CbServer.default_scope:
                self.bucket_util.drop_scope(self.cluster.master,
                                            self.bucket,
                                            scope_name)

        else:
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.bucket,
                                             scope_name,
                                             collection_name)

        # validate task failure
        self.task_manager.get_task_result(task)

        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()
