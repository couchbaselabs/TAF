import time

from Cb_constants import CbServer
from sdk_client3 import SDKClient
from storage.magma.magma_base import MagmaBaseTest

from java.util import HashMap
from reactor.util.function import Tuples

from com.couchbase.test.docgen import WorkLoadSettings, \
    DocumentGenerator, \
    DocRange
from com.couchbase.test.loadgen import TransactionWorkLoadGenerate, \
    WorkLoadGenerate
from com.couchbase.test.sdk import Server
from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.transactions import Transaction

from couchbase.test.docgen import DRConstants


class TransactionTests(MagmaBaseTest):
    def setUp(self):
        super(TransactionTests, self).setUp()
        self.with_ops = self.input.param("doc_ops", False)
        self.rollback = self.input.param("rollback", None)
        self.transaction_pattern = list()
        transaction_pattern = self.input.param("trans_pattern", "C_R:C:C_U")
        transaction_pattern = transaction_pattern.split(":")
        for pattern in transaction_pattern:
            self.transaction_pattern.append(pattern.split("_"))

    def tearDown(self):
        super(TransactionTests, self).tearDown()

    def load_docs(self, num_workers, cmd=dict(), mutated=0):
        master = Server(self.cluster.master.ip, self.cluster.master.port,
                        self.cluster.master.rest_username,
                        self.cluster.master.rest_password,
                        str(self.cluster.master.memcached_port))
        hm = HashMap()
        hm.putAll({DRConstants.create_s: self.init_items_per_collection,
                   DRConstants.create_e: self.init_items_per_collection,
                   DRConstants.delete_s: 0,
                   DRConstants.delete_e: self.init_items_per_collection/2,
                   DRConstants.read_s: self.init_items_per_collection/2,
                   DRConstants.read_e: self.init_items_per_collection})
        self.dr = DocRange(hm)

        ws = WorkLoadSettings(cmd.get("keyPrefix", self.key),
                              cmd.get("keySize", self.key_size),
                              cmd.get("docSize", self.doc_size),
                              cmd.get("cr", self.create_perc),
                              cmd.get("rd", self.read_perc),
                              cmd.get("up", self.update_perc),
                              cmd.get("dl", self.delete_perc),
                              cmd.get("workers", self.process_concurrency),
                              cmd.get("ops", self.ops_rate),
                              cmd.get("loadType", None),
                              cmd.get("keyType", None),
                              cmd.get("valueType", None),
                              cmd.get("validate", False),
                              cmd.get("gtm", False),
                              cmd.get("deleted", False),
                              cmd.get("mutated", mutated))
        ws.dr = self.dr
        dg = DocumentGenerator(ws, "", None)
        tasks = list()
        while num_workers > 0:
            for bucket in self.buckets:
                for scope in bucket.scopes.keys():
                    for collection in self.collections:
                        client = NewSDKClient(master, bucket.name,
                                              scope, collection)
                        client.initialiseSDK()
                        th_name = "Loader_%s_%s_%s_%s_%s" \
                                  % (bucket.name, scope, collection,
                                     str(num_workers), time.time())
                        task = WorkLoadGenerate(th_name, dg, client,
                                                self.durability_level)
                        tasks.append(task)
                        self.tm.submit(task)
                        num_workers -= 1

    def test_transactions(self):
        tasks = list()
        self.check_fragmentation_using_magma_stats(self.cluster.buckets[0])
        bucket = self.cluster.buckets[0]
        workers = self.process_concurrency
        self.tm = TaskManager(workers)
        transaction_app = Transaction()
        trans_config = transaction_app.createTransactionConfig(
            self.transaction_timeout,
            self.sdk_timeout,
            self.transaction_durability_level)

        workload = dict()
        workload["keyPrefix"] = "transactions"
        workload["keySize"] = self.key_size
        workload["docSize"] = self.doc_size
        workload["mutated"] = 0
        workload["keyRange"] = Tuples.of(0, self.num_items)
        workload["batchSize"] = 1
        workload["workers"] = 3
        workload["transaction_pattern"] = [
            [CbServer.default_scope, CbServer.default_collection,
             self.transaction_pattern]
        ]

        work_load = WorkLoadSettings(
            workload["keyPrefix"],
            workload["keySize"],
            workload["docSize"],
            workload["mutated"],
            workload["keyRange"],
            workload["batchSize"],
            workload["transaction_pattern"],
            workload["workers"])
        work_load.setTransactionRollback(self.rollback)
        client = SDKClient([self.cluster.master], bucket)
        transaction_obj = transaction_app.createTransaction(client.cluster,
                                                            trans_config)
        for index, load_pattern in enumerate(work_load.transaction_pattern):
            th_name = "Transaction_%s" % index
            batch_size = load_pattern[0]
            num_transactions = load_pattern[1]
            trans_pattern = load_pattern[2]

            task = TransactionWorkLoadGenerate(
                th_name, client.cluster, client.bucketObj, transaction_obj,
                work_load.doc_gen, batch_size, num_transactions, trans_pattern,
                work_load.commit_transaction, work_load.rollback_transaction,
                transaction_app)
            tasks.append(task)
            self.tm.submit(task)
        self.tm.getAllTaskResult()
        client.close()
        result = \
            self.check_fragmentation_using_magma_stats(self.cluster.buckets[0])
        self.assertTrue(result, "Magma framentation error")
