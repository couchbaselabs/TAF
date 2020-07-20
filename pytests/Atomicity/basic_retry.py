"""
Basic test cases with commit,rollback scenarios
"""

import threading

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from sdk_client3 import SDKClient

from com.couchbase.client.java.json import JsonObject
from com.couchbase.test.transactions import SimpleTransaction as Transaction
from reactor.util.function import Tuples


class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.add_rbac_user()
        if self.default_bucket:
            self.bucket_util.create_default_bucket(
                replica=self.num_replicas,
                compression_mode=self.compression_mode, ram_quota=100)

        self.sleep(10, "Wait for bucket to become ready for ops")

        self.def_bucket = self.bucket_util.get_all_buckets()
        self.client = SDKClient([self.cluster.master], self.def_bucket[0])
        self.__durability_level()
        self.create_Transaction()
        self._stop = threading.Event()
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        self.client.close()
        super(basic_ops, self).tearDown()

    def __durability_level(self):
        if self.durability_level == Bucket.DurabilityLevel.MAJORITY:
            self.durability = 1
        elif self.durability_level \
                == Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            self.durability = 2
        elif self.durability_level \
                == Bucket.DurabilityLevel.PERSIST_TO_MAJORITY:
            self.durability = 3
        elif self.durability_level == "ONLY_NONE":
            self.durability = 4
        else:
            self.durability = 0

    def get_doc_generator(self, start, end):
        age = range(5)
        name = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = JsonObject.create()
        template.put("age", age)
        template.put("first_name", name)
        template.put("body", body)
        generator = DocumentGenerator(self.key, template,
                                      start=start,
                                      end=end,
                                      key_size=self.key_size,
                                      doc_size=self.doc_size,
                                      doc_type=self.doc_type,
                                      randomize=True,
                                      age=age, first_name=name)
        return generator

    def set_exception(self, exception):
        self.exception = exception
        raise BaseException("Got an exception %s" % self.exception)

    def __chunks(self, l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def create_Transaction(self, client=None):
        if not client:
            client = self.client
        transaction_config = Transaction().createTransactionConfig(
            self.transaction_timeout, self.durability)
        try:
            self.transaction = Transaction().createTansaction(
                client.cluster, transaction_config)
        except Exception as e:
            self.set_exception(e)

    def __thread_to_transaction(self, transaction, op_type, doc, txn_commit,
                                update_count=1, sync=True,
                                set_exception=True, client=None):
        exception = None
        if client is None:
            client = self.client
        if op_type == "create":
            exception = Transaction().RunTransaction(
                transaction, [client.collection], doc, [], [],
                txn_commit, sync, update_count)
        elif op_type == "update":
            self.log.info("updating all the keys through threads")
            exception = Transaction().RunTransaction(
                transaction, [client.collection], [], doc, [],
                txn_commit, sync, update_count)
        elif op_type == "delete":
            exception = Transaction().RunTransaction(
                transaction, [client.collection], [], [], doc,
                txn_commit, sync, update_count)
        if set_exception and exception:
            self.set_exception("Failed")

    def doc_gen(self, num_items, start=0, value={'value': 'value1'},
                op_type="create"):
        self.docs = []
        self.keys = []
        self.content = self.client.translate_to_json_object(value)
        for i in range(start, self.num_items):
            key = "test_docs-" + str(i)
            if op_type == "create":
                doc = Tuples.of(key, self.content)
                self.keys.append(key)
                self.docs.append(doc)
            else:
                self.docs.append(key)

    def verify_doc(self, num_items, client):
        for i in range(num_items):
            key = "test_docs-" + str(i)
            result = client.read(key)
            actual_val = self.client.translate_to_json_object(result['value'])
            self.assertEquals(self.content, actual_val)

    def test_MultiThreadTxnLoad(self):
        """
        Load data through txn, update half the items through different threads
        and delete half the items through different threads. if update_retry
        then update and delete the same key in two different transaction
        and make sure update fails
        """

        self.num_txn = self.input.param("num_txn", 9)
        self.update_retry = self.input.param("update_retry", False)

        self.doc_gen(self.num_items)
        threads = []

        # create the docs
        exception = Transaction().RunTransaction(
            self.transaction, [self.client.collection], self.docs, [], [],
            self.transaction_commit, True, self.update_count)
        if exception:
            self.set_exception("Failed")

        if self.update_retry:
            threads.append(threading.Thread(
                target=self.__thread_to_transaction,
                args=(self.transaction, "delete", self.keys,
                      self.transaction_commit, self.update_count)))
            threads.append(threading.Thread(
                target=self.__thread_to_transaction,
                args=(self.transaction, "update", self.keys, 10,
                      self.update_count)))

        else:
            update_docs = self.__chunks(self.keys[:self.num_items/2],
                                        self.num_txn)
            delete_docs = self.__chunks(self.keys[self.num_items/2:],
                                        self.num_txn)

            for keys in update_docs:
                threads.append(threading.Thread(
                    target=self.__thread_to_transaction,
                    args=(self.transaction, "update", keys,
                          self.transaction_commit, self.update_count)))

            for keys in delete_docs:
                threads.append(threading.Thread(
                    target=self.__thread_to_transaction,
                    args=(self.transaction, "delete", keys,
                          self.transaction_commit, self.update_count)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.sleep(60, "Wait for transactions to complete")
        if self.update_retry:
            for key in self.keys:
                result = self.client.read(key)
                self.assertEquals(result['status'], False)

        else:
            self.value = {'mutated': 1, 'value': 'value1'}
            self.content = self.client.translate_to_json_object(self.value)

            self.verify_doc(self.num_items/2, self.client)

            for key in self.keys[self.num_items/2:]:
                result = self.client.read(key)
                self.assertEquals(result['status'], False)

    def test_basic_retry(self):
        """
        Load set of data to the cluster, update through 2 different threads,
        make sure transaction maintains the order of update
        :return:
        """
        self.write_conflict = self.input.param("write_conflict", 2)

        self.log.info("going to create and execute the task")
        self.gen_create = self.get_doc_generator(0, self.num_items)
        task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.def_bucket,
            self.gen_create, "create", exp=0,
            batch_size=10,
            process_concurrency=8,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries, update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=True, durability=self.durability_level, sync=self.sync)
        self.task.jython_task_manager.get_task_result(task)
        self.log.info("Get all the keys in the cluster")
        self.doc_gen(self.num_items)

        threads = []
        for update_count in [2, 4, 6]:
            threads.append(threading.Thread(
                target=self.__thread_to_transaction,
                args=(self.transaction, "update", self.keys,
                      self.transaction_commit, update_count)))
        # Add verification task
        if self.transaction_commit:
            self.update_count = 6
        else:
            self.update_count = 0

        for thread in threads:
            thread.start()
            self.sleep(2, "Wait for transaction thread to start")

        for thread in threads:
            thread.join()

    def test_basic_retry_async(self):
        self.log.info("going to create and execute the task")
        self.gen_create = self.get_doc_generator(0, self.num_items)
        task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.def_bucket,
            self.gen_create, "create", exp=0,
            batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries, update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=True, durability=self.durability_level,
            sync=True, num_threads=1)
        self.task.jython_task_manager.get_task_result(task)
        self.log.info("get all the keys in the cluster")
        keys = ["test_docs-0"]*2

        exception = Transaction().RunTransaction(
            self.transaction, [self.client.collection], [], keys, [],
            self.transaction_commit, False, 0)
        if exception:
            self.set_exception(Exception(exception))

    def basic_concurrency(self):
        self.crash = self.input.param("crash", False)

        self.doc_gen(self.num_items)

        # run transaction
        thread = threading.Thread(target=self.__thread_to_transaction,
                                  args=(self.transaction, "create", self.docs,
                                        self.transaction_commit,
                                        self.update_count, True, False))
        thread.start()
        self.sleep(1, "Wait for transaction thread to start")

        if self.crash:
            self.client.cluster.disconnect()
            self.transaction.close()
            self.client1 = SDKClient([self.cluster.master], self.def_bucket[0])
            self.create_Transaction(self.client1)
            self.sleep(self.transaction_timeout+60,
                       "Wait for transaction cleanup to complete")
            exception = Transaction().RunTransaction(
                self.transaction, [self.client1.collection], self.docs, [], [],
                self.transaction_commit, self.sync, self.update_count)
            if exception:
                self.sleep(60, "Wait for transaction cleanup to happen")

            self.verify_doc(self.num_items, self.client1)
            self.client1.close()

        else:
            key = "test_docs-0"
            # insert will succeed due to doc_isoloation feature
            result = self.client.insert(key, "value")
            self.assertEqual(result["status"], True)

            # Update should pass
            result = self.client.upsert(key, "value")
            self.assertEqual(result["status"], True)

            # delete should pass
            result = self.client.delete(key)
            self.assertEqual(result["status"], True)

        thread.join()

    def test_stop_loading(self):
        """
        Load through transactions and close the transaction abruptly,
        create a new transaction sleep for 60 seconds and
        perform create on the same set of docs
        """
        self.num_txn = self.input.param("num_txn", 9)
        self.doc_gen(self.num_items)
        threads = []

        docs = list(self.__chunks(self.docs, len(self.docs)/self.num_txn))

        for doc in docs:
            threads.append(threading.Thread(
                target=self.__thread_to_transaction,
                args=(self.transaction, "create", doc,
                      self.transaction_commit, self.update_count,
                      True, False)))

        for thread in threads:
            thread.start()

        self.client.cluster.disconnect()
        self.transaction.close()

        self.client1 = SDKClient([self.cluster.master], self.def_bucket[0])
        self.create_Transaction(self.client1)
        self.sleep(self.transaction_timeout+60,
                   "Wait for transaction cleanup to happen")

        self.log.info("going to start the load")
        for doc in docs:
            exception = Transaction().RunTransaction(
                self.transaction, [self.client1.collection], doc, [], [],
                self.transaction_commit, self.sync, self.update_count)
            if exception:
                self.sleep(60, "Wait for transaction cleanup to happen")

        self.verify_doc(self.num_items, self.client1)
        self.client1.close()

    def __insert_sub_doc_and_validate(self, doc_id, op_type, key, value):
        _, failed_items = self.client.crud(
            op_type,
            doc_id,
            [key, value],
            durability=self.durability_level,
            timeout=self.sdk_timeout,
            time_unit="seconds",
            create_path=True,
            xattr=True)
        self.assertFalse(failed_items, "Subdoc Xattr insert failed")

    def __read_doc_and_validate(self, doc_id, expected_val, subdoc_key=None):
        if subdoc_key:
            success, failed_items = self.client.crud("subdoc_read",
                                                     doc_id,
                                                     subdoc_key,
                                                     xattr=True)
            self.assertFalse(failed_items, "Xattr read failed")
            self.assertEqual(expected_val,
                             str(success[doc_id]["value"][0]),
                             "Sub_doc value mismatch: %s != %s"
                             % (success[doc_id]["value"][0],
                                expected_val))

    def test_TxnWithXattr(self):
        self.system_xattr = self.input.param("system_xattr", False)
        if self.system_xattr:
            xattr_key = "my._attr"
        else:
            xattr_key = "my.attr"
        val = "v" * self.doc_size

        self.doc_gen(self.num_items)
        thread = threading.Thread(target=self.__thread_to_transaction,
                                  args=(self.transaction, "create", self.docs,
                                        self.transaction_commit,
                                        self.update_count))
        thread.start()
        thread.join()

        self.doc_gen(self.num_items, op_type="update",
                     value={"mutated": 1, "value": "value1"})
        thread = threading.Thread(
            target=self.__thread_to_transaction,
            args=(self.transaction, "update", self.docs,
                  self.transaction_commit, self.update_count))
        thread.start()
        self.sleep(1)
        self.__insert_sub_doc_and_validate("test_docs-0", "subdoc_insert",
                                           xattr_key, val)

        thread.join()

        if self.transaction_commit:
            self.__read_doc_and_validate("test_docs-0", val, xattr_key)
        self.sleep(60, "Wait for transaction to complete")
        self.verify_doc(self.num_items, self.client)

    def test_TxnWithMultipleXattr(self):
        xattrs_to_insert = [["my.attr", "value"],
                            ["new_my.attr", "new_value"]]

        self.doc_gen(self.num_items)
        thread = threading.Thread(target=self.__thread_to_transaction,
                                  args=(self.transaction, "create", self.docs,
                                        self.transaction_commit,
                                        self.update_count))
        thread.start()
        thread.join()

        self.doc_gen(self.num_items, op_type="update",
                     value={"mutated": 1, "value": "value1"})
        thread = threading.Thread(
            target=self.__thread_to_transaction,
            args=(self.transaction, "update", self.docs,
                  self.transaction_commit, self.update_count))

        thread.start()
        self.sleep(1, "Wait for transx-thread to start")
        for key, val in xattrs_to_insert:
            self.__insert_sub_doc_and_validate("test_docs-0", "subdoc_insert",
                                               key, val)
        thread.join()

        if self.transaction_commit:
            for key, val in xattrs_to_insert:
                self.__read_doc_and_validate("test_docs-0", val, key)
        self.sleep(60, "Wait for transaction to complete")
        self.verify_doc(self.num_items, self.client)
