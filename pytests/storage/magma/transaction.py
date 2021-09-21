import time

from Cb_constants import CbServer
from sdk_client3 import SDKClient
from storage.magma.magma_base import MagmaBaseTest
from Jython_tasks.task import TimerTask

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

        # Requires doc_size to be 4096 and the data to be random
        self.final_disk_check = self.input.param("final_disk_check", False)

        # A list of tasks that will be stopped at the end of the test
        self.tasks = []

        # Start periodic magma disk check task that runs every 5 seconds
        self.tasks.append(TimerTask(self.periodic_disk_check, args=[], kwds={}, interval=5))
        self.task_manager.add_new_task(self.tasks[-1])

    def tearDown(self):
        for task in self.tasks:
            self.task_manager.stop_task(task)
        super(TransactionTests, self).tearDown()

    def periodic_disk_check(self):
        """ Check data size to disk size does not exceed fragmentation. """
        self.log.info("Checking disk usage")
        self.assertTrue(self.magma_utils.check_disk_usage(self.cluster.servers, self.cluster.buckets, self.fragmentation))

    def final_fragmentation_check(self, items):
        """ Checks the disk usage is at most 2.8x the esimated data size once
        the transactional documents have been loaded.

        Requires additional data is loaded requires no additional items are
        loaded.

        Args:
            items (int): The number of documents produced by the transaction.
        """
        for bucket in self.cluster.buckets:
            # Fetch the size of the kvstore files
            kvstore, _, _, _ = \
            self.magma_utils.get_disk_usage(self.cluster, bucket,
                    "/opt/couchbase/var/lib/couchbase/data/", servers=self.servers)

            # The size of the data the user expected to store in mb
            data_size = (items * self.doc_size)

            # Add transaction ATR records (assume 1024 ATR records of 100 bytes each)
            data_size += 1024 * 100

            self.log.info("Estimated data size {}".format(data_size))

            # The kvstore size should be less than or equal to the data_size * 2.8 at 50% fragmentation
            self.assertLessEqual(kvstore , data_size * 2.8)

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

        transaction_items = self.input.param("transaction_items", 1000)

        workload = dict()
        workload["keyPrefix"] = "transactions"
        workload["keySize"] = self.key_size
        workload["docSize"] = self.doc_size
        workload["mutated"] = 0
        workload["keyRange"] = Tuples.of(0, transaction_items)
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

        # The final disk check requires randomized documents of size of 4096
        # and no additional documents should be loaded.
        if self.final_disk_check:
            self.final_fragmentation_check(transaction_items)
