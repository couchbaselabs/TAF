import time
from basetestcase import BaseTestCase
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import DocumentGenerator
from epengine.durability_base import DurabilityTestsBase
from __builtin__ import True
from BucketLib.bucket import Bucket
import com.couchbase.test.transactions.SimpleTransaction as Transaction
import logging
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient as VBucketAwareMemcached
import threading
from random import randint
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient as VBucketAwareMemcached
import random
from couchbase_helper.documentgenerator import doc_generator
from reactor.util.function import Tuples
import com.couchbase.test.transactions.SimpleTransaction as Transaction
import com.couchbase.client.java.json.JsonObject as JsonObject
import json as pyJson

"""
Basic test cases with commit,rollback scenarios
"""

class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()
        self.test_log = logging.getLogger("test")
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.add_rbac_user()

        if self.default_bucket:
            self.bucket_util.create_default_bucket(replica=self.num_replicas,
                                               compression_mode=self.compression_mode, ram_quota=100, bucket_type=self.bucket_type)

        time.sleep(10)
        self.def_bucket= self.bucket_util.get_all_buckets()
        self.client = VBucketAwareMemcached(RestConnection(self.cluster.master), self.def_bucket[0])
        self.__durability_level()

        self.operation = self.input.param("operation", "afterAtrPending")
        # create load
        self.value = {'value':'value1'}
        self.content = self.client.translate_to_json_object(self.value)

        self.docs = []
        self.keys = []
        for i in range(self.num_items):
            key = "test_docs-" + str(i)
            doc = Tuples.of(key, self.content)
            self.keys.append(key)
            self.docs.append(doc)

        self.transaction_config = Transaction().createTransactionConfig(self.transaction_timeout, self.durability)
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        self.client.close()
        super(basic_ops, self).tearDown()

    def __durability_level(self):
        if self.durability_level == "MAJORITY":
            self.durability = 1
        elif self.durability_level == "MAJORITY_AND_PERSIST_ON_MASTER":
            self.durability = 2
        elif self.durability_level == "PERSIST_TO_MAJORITY":
            self.durability = 3
        elif self.durability_level == "ONLY_NONE":
            self.durability = 4
        else:
            self.durability = 0

    def set_exception(self, exception):
        self.exception = exception
        self.fail("Got an exception {}".format(self.exception))

    def test_txnwithhooks(self):

        # transaction load
        if "Atr" in self.operation:
            exception = Transaction().MockRunTransaction(self.client.cluster, self.transaction_config, 
                                self.client.collection, self.docs, self.transaction_commit, self.operation)

        else:
            if "Replace" in self.operation:
                exception = Transaction().MockRunTransaction(self.client.cluster, self.transaction_config, 
                                self.client.collection, self.docs, self.keys, [], self.transaction_commit, self.operation, self.keys[-1]) 
                self.value = {'mutated':1, 'value':'value1'}
                self.content = self.client.translate_to_json_object(self.value)
            else:
                exception = Transaction().MockRunTransaction(self.client.cluster, self.transaction_config, 
                                self.client.collection, self.docs, [], [], self.transaction_commit, self.operation, self.keys[-1]) 

            if "Remove" in self.operation:
                exception = Transaction().MockRunTransaction(self.client.cluster, self.transaction_config,
                                 self.client.collection, [], [], self.keys, self.transaction_commit, self.operation, self.keys[-1])

        # verify the values
        for key in self.keys:
            result = self.client.read(key)
            if "Remove" in self.operation or self.transaction_commit == False :
                if result['status']:
                    msg = "Key should be deleted but present in the cluster {}".format(key)
                    self.set_exception(msg)
            else:
                actual_val = self.client.translate_to_json_object(result['value'])
                if self.content != actual_val:
                    self.test_log.info("actual value for key {} is {}".format(key,actual_val))
                    self.test_log.info("expected value for key {} is {}".format(key,self.content))
                    self.set_exception("actual and expected value does not match")

        if exception:
            self.set_exception(exception)


