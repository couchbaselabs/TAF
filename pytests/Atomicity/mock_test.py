import time

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient

from reactor.util.function import Tuples
import com.couchbase.test.transactions.SimpleTransaction as Transaction

"""
Basic test cases with commit,rollback scenarios
"""


class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()
        self.fail = self.input.param("fail", False)
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.add_rbac_user()

        if self.default_bucket:
            self.bucket_util.create_default_bucket(
                replica=self.num_replicas,
                compression_mode=self.compression_mode,
                ram_quota=100,
                bucket_type=self.bucket_type)

        time.sleep(10)
        self.def_bucket = self.bucket_util.get_all_buckets()
        self.client = SDKClient(RestConnection(self.cluster.master),
                                self.def_bucket[0])
        self.__durability_level()

        self.operation = self.input.param("operation", "afterAtrPending")
        # create load
        self.value = {'value': 'value1'}
        self.content = self.client.translate_to_json_object(self.value)

        self.docs = []
        self.keys = []
        for i in range(self.num_items):
            key = "test_docs-" + str(i)
            doc = Tuples.of(key, self.content)
            self.keys.append(key)
            self.docs.append(doc)

        self.transaction_config = Transaction().createTransactionConfig(
            self.transaction_timeout, self.durability)
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

    def set_exception(self, exception):
        raise BaseException(exception)

    def test_txnwithhooks(self):
        self.verify = self.input.param("verify", True)
        # transaction load
        if "Atr" in self.operation:
            exception = Transaction().MockRunTransaction(
                self.client.cluster, self.transaction_config,
                self.client.collection, self.docs,
                self.transaction_commit, self.operation, self.fail)

        else:
            if "Replace" in self.operation:
                exception = Transaction().MockRunTransaction(
                    self.client.cluster, self.transaction_config,
                    self.client.collection, self.docs, self.keys, [],
                    self.transaction_commit, self.operation,
                    self.keys[-1], self.fail)
                self.value = {'mutated': 1, 'value': 'value1'}
                self.content = self.client.translate_to_json_object(self.value)
            else:
                exception = Transaction().MockRunTransaction(
                    self.client.cluster, self.transaction_config,
                    self.client.collection, self.docs, [], [],
                    self.transaction_commit, self.operation,
                    self.keys[-1], self.fail)

            if "Remove" in self.operation:
                exception = Transaction().MockRunTransaction(
                    self.client.cluster, self.transaction_config,
                    self.client.collection, [], [], self.keys,
                    self.transaction_commit, self.operation,
                    self.keys[-1], self.fail)

        # verify the values
        for key in self.keys:
            result = self.client.read(key)
            if "Remove" in self.operation \
                    or self.transaction_commit is False \
                    or self.verify is False:
                if result['status']:
                    actual_val = \
                        self.client.translate_to_json_object(result['value'])
                    self.log.info("actual value for key %s is %s"
                                  % (key, actual_val))
                    msg = "Key '%s' should be deleted but still exists" % key
                    self.set_exception(msg)
            else:
                actual_val = \
                    self.client.translate_to_json_object(result['value'])
                if self.content != actual_val:
                    self.log.info("Key '%s' - Actual value: %s,"
                                  "Expected value: %s"
                                  % (key, actual_val, self.content))
                    self.set_exception("Mismatch in expected values")

        if exception and self.fail is not True:
            self.set_exception(exception)
