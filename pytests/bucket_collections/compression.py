from random import sample

from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.documentgenerator import doc_generator
from sdk_client3 import SDKClient

class SDKCompression(CollectionBase):
    def setUp(self):
        super(SDKCompression, self).setUp()

        self.key =  self.input.param("key","test-compression")
        self.bucket = self.bucket_util.buckets[0]

        self.diff_client_for_validation = \
            self.input.param("diff_client_for_valiation", False)
        non_snappy_client = self.input.param("non_snappy_client", False)
        compression_settings = self.sdk_compression
        if non_snappy_client:
            compression_settings = None

        # Create required clients
        self.snappy_client = SDKClient(
            [self.cluster.master],
            self.bucket,
            compression_settings=self.sdk_compression)
        self.second_client = SDKClient(
            [self.cluster.master],
            self.bucket,
            compression_settings=compression_settings)

        # Create required doc_generators
        self.create_gen = doc_generator(self.key, 0, self.num_items,
                                        key_size=self.key_size,
                                        doc_size=self.doc_size,
                                        randomize_doc_size=True,
                                        target_vbucket=self.target_vbucket,
                                        mutation_type="ADD")
        self.update_gen = doc_generator(self.key, 0, self.num_items,
                                        key_size=self.key_size,
                                        doc_size=self.doc_size,
                                        randomize_doc_size=True,
                                        target_vbucket=self.target_vbucket,
                                        mutation_type="SET",
                                        mutate=1)

    def tearDown(self):
        # Close all SDK client connections before base-test teardown
        self.snappy_client.close()
        self.second_client.close()

        super(SDKCompression, self).tearDown()

    def test_compression_insert_validate(self):
        """
        1. Insert docs into multiple collections using snappy client.
           All inserted docs will have random doc_size/content
        2. Read back the docs from the same/different client and validate
           This validating client can be both snappy/non-snappy client.
        """
        random_clients = self.input.param("random_clients", False)
        s_name = None
        c_name = None
        bucket_dict = BucketUtils.get_random_collections(
            self.bucket_util.buckets,
            req_num=1,
            consider_scopes="all", consider_buckets="all")
        for bucket_name, scope_dict in bucket_dict.items():
            for scope_name, col_dict in scope_dict["scopes"].items():
                for collection_name, _ in col_dict["collections"].items():
                    s_name = scope_name
                    c_name = collection_name

        # Select clients for doc_ops based on user input params
        create_client = self.snappy_client
        update_client = self.snappy_client
        read_client = self.snappy_client
        if random_clients:
            sdk_clients = [self.snappy_client, self.second_client]
            create_client = sample(sdk_clients, 1)[0]
            update_client = sample(sdk_clients, 1)[0]
            read_client = sample(sdk_clients, 1)[0]
        elif self.diff_client_for_validation:
            read_client = self.second_client

        # Log client's compression info for debug purpose
        self.log.info("Create client's compression: %s"
                      % create_client.compression)
        self.log.info("Update client's compression: %s"
                      % update_client.compression)
        self.log.info("Read client's compression: %s"
                      % read_client.compression)

        self.log.info("Performing doc loading in bucket %s" % self.bucket)
        for _, scope in self.bucket.scopes.items():
            self.log.info("Mutating docs under scope: %s" % scope.name)
            for _, collection in scope.collections.items():
                self.snappy_client.select_collection(scope.name,
                                                     collection.name)
                while self.create_gen.has_next():
                    # Add new doc using snappy client
                    key, value = self.create_gen.next()
                    result = create_client.crud(
                        "create", key, value,
                        exp=self.maxttl,
                        durability=self.durability_level)
                    if result["status"] is False:
                        self.log_failure("Key '%s' insert failed for %s: %s"
                                         % (key, collection.name, result))

                    # Mutate same doc using the same client
                    key, value = self.update_gen.next()
                    result = update_client.crud(
                        "update", key, value,
                        exp=self.maxttl,
                        durability=self.durability_level)
                    if result["status"] is False:
                        self.log_failure("Key '%s' update failed for %s: %s"
                                         % (key, collection.name, result))

                # Reset doc_gens to be utilized by subsequent loaders
                self.create_gen.reset()
                self.update_gen.reset()
                # Validate and report fast failures per collection
                self.validate_test_failure()

        self.log.info("Validating docs in bucket %s" % self.bucket)
        for _, scope in self.bucket.scopes.items():
            self.log.info("Reading docs under scope: %s" % scope.name)
            for _, collection in scope.collections.items():
                read_client.select_collection(scope.name,
                                              collection.name)
                while self.update_gen.has_next():
                    key, value = self.update_gen.next()
                    result = read_client.crud("read", key)
                    if str(result["value"]) != str(value):
                        self.log_failure(
                            "Value mismatch for %s in collection %s: %s"
                            % (key, collection.name, result))

        self.validate_test_failure()
        self.bucket.scopes[s_name].collections[c_name].num_items += self.num_items
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)

    def test_compression_with_parallel_mutations_on_same_collection(self):
        """
        1. Insert docs into single collections using both snappy/non-snappy
           clients in parallel (Includes overlapping CRUDs on same docs)
        2. Validate the results and stability of mutated docs
        """

        tasks = list()
        # Used for doc_loading tasks's SDK client creation
        scope = None
        collection = None
        self.batch_size = 30

        bucket_dict = BucketUtils.get_random_collections(
            self.bucket_util.buckets,
            req_num=1,
            consider_scopes="all", consider_buckets="all")
        for bucket_name, scope_dict in bucket_dict.items():
            for scope_name, col_dict in scope_dict["scopes"].items():
                for collection_name, _ in col_dict["collections"].items():
                    scope = scope_name
                    collection = collection_name

        self.log.info("Creating doc generators")
        create_gen_1 = self.create_gen
        create_gen_2 = doc_generator(self.key,
                                     self.num_items, self.num_items*2,
                                     key_size=self.key_size,
                                     doc_size=self.doc_size,
                                     randomize_doc_size=True,
                                     target_vbucket=self.target_vbucket,
                                     mutation_type="ADD")
        update_gen_1 = self.update_gen
        update_gen_2 = doc_generator(self.key, 0, self.num_items,
                                     key_size=self.key_size,
                                     doc_size=self.doc_size,
                                     randomize_doc_size=True,
                                     target_vbucket=self.target_vbucket,
                                     mutation_type="SET",
                                     mutate=2)

        self.log.info("Loading initial docs into collection %s::%s"
                      % (scope, collection))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, create_gen_1, "create", self.maxttl,
            batch_size=self.batch_size, process_concurrency=3,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope, collection=collection))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, create_gen_2, "create", self.maxttl,
            batch_size=self.batch_size, process_concurrency=3,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=None,
            timeout_secs=self.sdk_timeout,
            scope=scope, collection=collection))

        for task in tasks:
            self.task_manager.get_task_result(task)
            if task.fail.keys():
                self.log_failure("Failures during initial doc loading "
                                 "for keys: %s" % task.fail.keys())

        self.bucket.scopes[scope].collections[collection].num_items \
            += (self.num_items * 2)
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)

        self.log.info("Performing overlapping mutations")
        tasks = list()
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, update_gen_1, "update", self.maxttl,
            batch_size=self.batch_size, process_concurrency=3,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            task_identifier="update_1",
            scope=scope, collection=collection))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, update_gen_2, "update", self.maxttl,
            batch_size=self.batch_size, process_concurrency=3,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=None,
            timeout_secs=self.sdk_timeout,
            task_identifier="update_2",
            scope=scope, collection=collection))
        for task in tasks:
            self.task_manager.get_task_result(task)
            if task.fail.keys():
                self.log_failure("Failures during %s updates for keys: %s"
                                 % (task.thread_name, task.fail.keys()))

        # Validate docs using snappy/non-snappy client in random
        task = self.task.async_validate_docs(
            self.cluster, self.bucket,
            create_gen_2, "create",
            compression=sample([self.sdk_compression, None], 1)[0],
            batch_size=self.batch_size,
            process_concurrency=3,
            scope=scope, collection=collection)
        self.task.jython_task_manager.get_task_result(task)

        # Intermediate collection-doc validation
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)

        self.log.info("Performing parallel deletes")
        tasks = list()
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, create_gen_1, "delete", self.maxttl,
            batch_size=self.batch_size, process_concurrency=3,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope, collection=collection))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, create_gen_2, "delete", self.maxttl,
            batch_size=self.batch_size, process_concurrency=3,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope, collection=collection))

        for task in tasks:
            self.task_manager.get_task_result(task)
            if task.fail.keys():
                self.log_failure("Failures during initial doc loading "
                                 "for keys: %s" % task.fail.keys())

        # Doc validation
        self.bucket.scopes[scope].collections[collection].num_items \
            -= (self.num_items * 2)
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
