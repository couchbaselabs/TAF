import json
from threading import Thread

from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator
from sdk_client3 import SDKClient, TransactionConfig

from sdk_exceptions import SDKException


class IsolationDocTest(ClusterSetup):
    def setUp(self):
        super(IsolationDocTest, self).setUp()

        # Create default bucket
        self.bucket_size = self.input.param("bucket_size", 256)
        self.create_bucket(self.cluster)

        self.doc_op = self.input.param("doc_op", "create")
        self.operation = self.input.param("operation", "afterAtrPending")
        self.transaction_fail_count = self.input.param("fail_count", 99999)
        self.transaction_fail = self.input.param("fail", True)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0

        # Create SDK client for each bucket
        self.sdk_clients = dict()
        trans_config = TransactionConfig(self.durability_level,
                                         self.transaction_timeout)
        for bucket in self.cluster.buckets:
            self.sdk_clients[bucket.name] = SDKClient(
                self.cluster, bucket, transaction_config=trans_config)

        self.read_failed = dict()
        self.stop_thread = False
        self.docs = list()
        self.keys = list()
        self.__create_transaction_docs()

    def tearDown(self):
        # Close sdk_clients created in init()
        for bucket in self.cluster.buckets:
            self.sdk_clients[bucket.name].close()

        super(IsolationDocTest, self).tearDown()

    def __perform_read_on_doc_keys(self, bucket, keys,
                                   expected_exception=None):
        self.read_failed[bucket] = False
        client = self.sdk_clients[bucket.name]

        expected_val = dict()
        if expected_exception is None:
            for key in keys:
                result = client.crud("read", key)
                expected_val[key] = \
                    client.translate_to_json_object(result["value"])
            self.log.info("Current values read complete")

        while not self.stop_thread:
            for key in keys:
                result = client.crud("read", key)
                result["value"] = \
                    client.translate_to_json_object(result["value"])
                if expected_exception is not None:
                    if expected_exception not in str(result["error"]):
                        self.read_failed[bucket] = True
                        self.log_failure("Key %s, exception %s not seen: %s"
                                         % (key, expected_exception, result))
                elif result["value"] != expected_val[key]:
                    self.read_failed[bucket] = True
                    self.log_failure("Key %s, Expected: %s, Actual: %s"
                                     % (key, expected_val[key],
                                        result["value"]))
            if self.read_failed[bucket]:
                break

    def __perform_query_on_doc_keys(self, bucket, keys, expected_val):
        self.read_failed[bucket] = False
        client = self.sdk_clients[bucket.name]
        while not self.stop_thread:
            for key in keys:
                query = "Select * from `%s` where meta().id='%s'" \
                        % (bucket.name, key)
                result = client.cluster.query(query)
                if result.metaData().status().toString() != "SUCCESS":
                    self.read_failed[bucket] = True
                    self.log_failure("Query %s failed: %s" % (query, result))
                elif key not in expected_val:
                    if result.rowsAsObject().size() != 0:
                        self.read_failed[bucket] = True
                        self.log_failure("Index found for key %s: %s"
                                         % (key, result))
                elif key in expected_val:
                    # Return type of rowsAsObject - java.util.ArrayList
                    rows = result.rowsAsObject()
                    if rows.size() != 1:
                        self.read_failed[bucket] = True
                        self.log_failure("Index not found for key %s: %s"
                                         % (key, result))
                    else:
                        value = json.loads(str(rows.get(0)))[bucket.name]
                        if value != expected_val[key]:
                            self.read_failed[bucket] = True
                            self.log_failure("Mismatch in value for key %s."
                                             "Expected: %s, Got: %s"
                                             % (key, expected_val[key], value))
            if self.read_failed[bucket]:
                break

    def __create_transaction_docs(self):
        self.value = {'value': 'value1'}
        self.content = \
            self.sdk_clients[self.cluster.buckets[0].name] \
            .translate_to_json_object(self.value)
        for i in range(self.num_items):
            key = "test_docs-" + str(i)
            doc = Tuples.of(key, self.content)
            self.keys.append(key)
            self.docs.append(doc)

    def __run_mock_test(self, client, doc_op):
        self.log.info("Starting Mock_Transaction")
        if "Atr" in self.operation:
            exception = Transaction().MockRunTransaction(
                client.cluster, self.transaction_config,
                client.collection, self.docs, doc_op,
                self.transaction_commit,
                self.operation, self.transaction_fail_count)
        else:
            if "Replace" in self.operation:
                exception = Transaction().MockRunTransaction(
                    client.cluster, self.transaction_config,
                    client.collection, self.docs, self.keys, [],
                    self.transaction_commit, self.operation, self.keys[-1],
                    self.transaction_fail)
                self.value = {'mutated': 1, 'value': 'value1'}
                self.content = client.translate_to_json_object(self.value)
            else:
                exception = Transaction().MockRunTransaction(
                    client.cluster, self.transaction_config,
                    client.collection, self.docs, [], [],
                    self.transaction_commit, self.operation, self.keys[-1],
                    self.transaction_fail)

            if "Remove" in self.operation:
                exception = Transaction().MockRunTransaction(
                    client.cluster, self.transaction_config,
                    client.collection, [], [], self.keys,
                    self.transaction_commit, self.operation, self.keys[-1],
                    self.transaction_fail)
        return exception

    def test_staged_doc_read(self):
        self.verify = self.input.param("verify", True)

        bucket = self.cluster.buckets[0]
        expected_exception = SDKException.DocumentNotFoundException

        # Create SDK client for transactions
        client = SDKClient(self.cluster, bucket)

        if self.doc_op in ["update", "delete"]:
            for doc in self.docs:
                result = client.crud("create", doc.getT1(), doc.getT2(),
                                     durability=self.durability_level,
                                     timeout=60)
                if result["status"] is False:
                    self.log_failure("Key %s create failed: %s"
                                     % (doc.getT1(), result))
                    break
            expected_exception = None

        read_thread = Thread(target=self.__perform_read_on_doc_keys,
                             args=(bucket, self.keys),
                             kwargs=dict(expected_exception=expected_exception)
                             )
        read_thread.start()

        # Transaction load
        exception = self.__run_mock_test(client, self.doc_op)
        if SDKException.TransactionExpired not in str(exception):
            self.log_failure("Expected exception not found")

        self.log.info("Terminating reader thread")
        self.stop_thread = True
        read_thread.join()

        self.transaction_fail_count = 2
        exception = self.__run_mock_test(client, self.doc_op)
        if exception:
            self.log_failure(exception)

        # verify the values
        for key in self.keys:
            result = client.read(key)
            if "Remove" in self.operation \
                    or self.transaction_commit is False \
                    or self.verify is False:
                if result['status']:
                    actual_val = client.translate_to_json_object(
                        result['value'])
                    self.log.info("Actual value for key %s is %s"
                                  % (key, actual_val))
                    self.log_failure(
                        "Key '%s' should be deleted but present in the bucket"
                        % key)
            else:
                actual_val = client.translate_to_json_object(
                    result['value'])
                if self.doc_op == "update":
                    self.content.put("mutated", 1)
                elif self.doc_op == "delete":
                    self.content.removeKey("value")

                if self.content != actual_val:
                    self.log.info("Key %s Actual: %s, Expected: %s"
                                  % (key, actual_val, self.content))
                    self.log_failure("Mismatch in doc content")

        # Close SDK client
        client.close()

        if self.read_failed[self.cluster.buckets[0]] is True:
            self.log_failure("Failure in read thread for bucket: %s"
                             % self.cluster.buckets[0].name)

        self.validate_test_failure()

    def test_staged_doc_query_from_index(self):
        self.verify = self.input.param("verify", True)

        expected_val = dict()
        bucket = self.cluster.buckets[0]

        # Create SDK client for transactions
        client = SDKClient(self.cluster, bucket)

        if self.doc_op in ["update", "delete"]:
            for doc in self.docs:
                result = client.crud("create", doc.getT1(), doc.getT2(),
                                     durability=self.durability_level,
                                     timeout=60)
                if result["status"] is False:
                    self.log_failure("Key %s create failed: %s"
                                     % (doc.getT1(), result))
                    break
                expected_val[doc.getT1()] = json.loads(str(doc.getT2()))

        # Create primary Index on all buckets
        for t_bucket in self.cluster.buckets:
            q_result = client.cluster.query("CREATE PRIMARY INDEX ON `%s`"
                                            % t_bucket.name)
            if q_result.metaData().status().toString() != "SUCCESS":
                client.close()
                self.fail("Create primary index failed for bucket %s"
                          % t_bucket.name)
        self.sleep(10, "Wait for primary indexes to get warmed up")

        query_thread = Thread(target=self.__perform_query_on_doc_keys,
                              args=(bucket, self.keys, expected_val))
        query_thread.start()

        # Transaction load
        exception = self.__run_mock_test(client, self.doc_op)
        if SDKException.TransactionExpired not in str(exception):
            self.log_failure("Expected exception not found")

        self.log.info("Terminating query thread")
        self.stop_thread = True
        query_thread.join()

        self.transaction_fail_count = 2
        exception = self.__run_mock_test(client, self.doc_op)
        if exception:
            self.log_failure(exception)

        # verify the values
        for key in self.keys:
            result = client.read(key)
            if "Remove" in self.operation \
                    or self.transaction_commit is False \
                    or self.verify is False:
                if result['status']:
                    actual_val = client.translate_to_json_object(
                        result['value'])
                    self.log.info("Actual value for key %s is %s"
                                  % (key, actual_val))
                    self.log_failure(
                        "Key '%s' should be deleted but present in the bucket"
                        % key)
            else:
                actual_val = client.translate_to_json_object(
                    result['value'])

                if self.doc_op == "update":
                    self.content.put("mutated", 1)
                elif self.doc_op == "delete":
                    self.content.removeKey("value")

                if self.content != actual_val:
                    self.log.info("Key %s Actual: %s, Expected: %s"
                                  % (key, actual_val, self.content))
                    self.log_failure("Mismatch in doc content")

        # Close SDK client
        client.close()

        if self.read_failed[self.cluster.buckets[0]] is True:
            self.log_failure("Failure in read thread for bucket: %s"
                             % self.cluster.buckets[0].name)
        self.validate_test_failure()

    def test_run_purger_during_transaction(self):
        def perform_create_deletes():
            index = 0
            client = SDKClient(self.cluster, self.cluster.buckets[0])
            self.log.info("Starting ops to create tomb_stones")
            while not self.stop_thread:
                key = "temp_key--%s" % index
                result = client.crud("create", key, "")
                if result["status"] is False:
                    self.log_failure("Key %s create failed: %s"
                                     % (key, result))
                    break
                result = client.crud("delete", key)
                if result["status"] is False:
                    self.log_failure("Key %s delete failed: %s"
                                     % (key, result))
                    break
                index += 1
            client.close()
            self.log.info("Total keys deleted: %s" % index)

        pager_val = self.transaction_timeout+1

        self.log.info("Setting expiry pager value to %d" % pager_val)
        self.bucket_util._expiry_pager(self.cluster, pager_val)

        tombstone_creater = Thread(target=perform_create_deletes)
        tombstone_creater.start()

        gen_create = doc_generator(self.key, 0, self.num_items)
        trans_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            gen_create, "create", exp=self.maxttl,
            batch_size=50,
            process_concurrency=4,
            timeout_secs=self.sdk_timeout,
            update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=self.transaction_commit,
            durability=self.durability_level,
            sync=self.sync)

        self.bucket_util._run_compaction(self.cluster, number_of_times=20)

        # Wait for transaction task to complete
        self.task_manager.get_task_result(trans_task)

        # Halt tomb-stone create thread
        self.stop_thread = True
        tombstone_creater.join()

    def test_transaction_docs_keys_already_in_tombstone(self):
        load_gen = doc_generator(self.key, 0, self.num_items)

        # Create docs which are going to be created by Tranx Task
        create_task = self.task.async_load_gen_docs(
            self.cluster, self.cluster.buckets[0], load_gen, "create",
            exp=self.maxttl, compression=self.sdk_compression,
            timeout_secs=60, process_concurrency=8, batch_size=200,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(create_task)

        # Perform delete of docs / wait for docs to expire
        if self.maxttl == 0:
            delete_task = self.task.async_load_gen_docs(
                self.cluster, self.cluster.buckets[0], load_gen, "delete",
                exp=self.maxttl, compression=self.sdk_compression,
                timeout_secs=60, process_concurrency=8, batch_size=200,
                load_using=self.load_docs_using)
            self.task_manager.get_task_result(delete_task)
        else:
            self.sleep(self.maxttl+1, "Wait for created docs to expire")

        # Start Transaction load
        trans_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            load_gen, "create", exp=self.maxttl,
            batch_size=50,
            process_concurrency=3,
            timeout_secs=self.sdk_timeout,
            update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=True,
            durability=self.durability_level,
            sync=self.sync,
            retries=0)
        self.task_manager.get_task_result(trans_task)

    def test_rollback_transaction(self):
        load_gen = doc_generator(self.key, 0, self.num_items)

        expected_exception = None
        if self.doc_op == "create":
            expected_exception = SDKException.DocumentNotFoundException

        self.keys = list()
        while load_gen.has_next():
            key, _ = load_gen.next()
            self.keys.append(key)
        load_gen.reset()

        if self.doc_op != "create":
            trans_task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                load_gen, "create", exp=self.maxttl,
                batch_size=50,
                process_concurrency=8,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=True,
                durability=self.durability_level,
                sync=self.sync,
                retries=0)
            self.task_manager.get_task_result(trans_task)

        # Start reader thread for validation
        read_thread = Thread(target=self.__perform_read_on_doc_keys,
                             args=(self.cluster.buckets[0], self.keys),
                             kwargs=dict(expected_exception=expected_exception)
                             )
        read_thread.start()
        if self.doc_op != "create":
            self.sleep(30, "Wait for reader thread to fetch the values")

        # Transaction task with commit=False so that rollback will be triggered
        for index in range(1, 11):
            self.log.info("Running rollback transaction: %s" % index)
            trans_task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                load_gen, self.doc_op, exp=self.maxttl,
                batch_size=50,
                process_concurrency=3,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=False,
                durability=self.durability_level,
                sync=self.sync,
                retries=0)
            self.task_manager.get_task_result(trans_task)

        # Stop reader thread
        self.stop_thread = True
        read_thread.join()

        if self.read_failed[self.cluster.buckets[0]]:
            self.log_failure("Reader thread failed")

        self.validate_test_failure()

    def test_transaction_with_rebalance(self):
        rebalance_type = self.input.param("rebalance_type", "in")
        nodes_to_add = list()
        nodes_to_remove = list()

        load_gen_1 = doc_generator(self.key, 0, self.num_items)
        load_gen_2 = doc_generator(self.key, self.num_items, self.num_items*2)

        if rebalance_type == "in":
            nodes_to_add = [self.cluster.servers[self.nodes_init+i]
                            for i in range(self.nodes_in)]
        elif rebalance_type == "out":
            nodes_to_remove = \
                [self.cluster.servers[len(self.cluster.nodes_in_cluster)-i-1]
                 for i in range(self.nodes_out)]
        elif rebalance_type == "swap":
            nodes_to_remove = \
                [self.cluster.servers[len(self.cluster.nodes_in_cluster)-i-1]
                 for i in range(self.nodes_out)]
            nodes_to_add = [self.cluster.servers[self.nodes_init+i]
                            for i in range(self.nodes_in)]
        else:
            self.fail("Invalid value rebalance_type: %s" % rebalance_type)

        # Create docs for update/delete operation
        if self.doc_op != "create":
            trans_task_1 = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                load_gen_1, "create", exp=self.maxttl,
                batch_size=50,
                process_concurrency=4,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=True,
                durability=self.durability_level,
                sync=self.sync,
                retries=0)
            trans_task_2 = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                load_gen_2, "create", exp=self.maxttl,
                batch_size=50,
                process_concurrency=4,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=True,
                durability=self.durability_level,
                sync=self.sync,
                retries=0)
            self.task_manager.get_task_result(trans_task_1)
            self.task_manager.get_task_result(trans_task_2)
            load_gen_1 = doc_generator(self.key, 0, self.num_items, mutate=1)
            load_gen_2 = doc_generator(self.key, self.num_items,
                                       self.num_items*2)

        # Start transaction tasks with success & rollback for shadow docs test
        # Successful transaction
        trans_task_1 = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            load_gen_1, self.doc_op, exp=self.maxttl,
            batch_size=50,
            process_concurrency=3,
            timeout_secs=self.sdk_timeout,
            update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=True,
            durability=self.durability_level,
            sync=self.sync,
            retries=1)
        # Rollback transaction
        trans_task_2 = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            load_gen_2, self.doc_op, exp=self.maxttl,
            batch_size=50,
            process_concurrency=3,
            timeout_secs=self.sdk_timeout,
            update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=False,
            durability=self.durability_level,
            sync=self.sync,
            retries=0)

        self.sleep(3, "Wait for transactions to start")
        # Start rebalance task
        rebalance_task = self.task.async_rebalance(
            self.cluster, nodes_to_add, nodes_to_remove)

        # Wait for transactions and rebalance task to complete
        try:
            self.task_manager.get_task_result(trans_task_1)
        except BaseException as e:
            self.task_manager.get_task_result(trans_task_2)
            raise e
        self.task_manager.get_task_result(rebalance_task)

        if rebalance_task.result is False:
            self.log_failure("Rebalance failure")

        self.validate_test_failure()
