from Cb_constants import DocLoading, CbServer
from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import DocumentGenerator, doc_generator
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient

from com.couchbase.client.java.json import JsonObject
from com.couchbase.test.docgen import TransactionalWorkLoadSettings, TransactionDocGenerator
from com.couchbase.test.loadgen import TransactionWorkLoadGenerate
from com.couchbase.test.sdk import Server
from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.transactions import Transaction

"""
Basic test cases with commit,rollback scenarios
"""


class basic_ops(ClusterSetup):
    def setUp(self):
        super(basic_ops, self).setUp()

        if self.num_buckets:
            self.bucket_util.create_multiple_buckets(
                self.cluster,
                self.num_replicas,
                bucket_count=self.num_buckets,
                bucket_type=self.bucket_type,
                ram_quota=self.bucket_size,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy)
        else:
            self.create_bucket(self.cluster)

        # self.sleep(10, "Wait for bucket to become ready for ops")

        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def get_doc_generator(self, start, end):
        age = range(5)
        first = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = JsonObject.create()
        template.put("age", None)
        template.put("first_name", None)
        template.put("body", None)
        generator = DocumentGenerator(self.key, template, randomize=True,
                                      age=age,
                                      first_name=first, body=body,
                                      start=start, end=end,
                                      key_size=self.key_size,
                                      doc_size=self.doc_size,
                                      doc_type=self.doc_type)
        return generator

    @staticmethod
    def generate_docs_bigdata(docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day,
                                                    start=start,
                                                    value_size=document_size)

    def test_basic_commit(self):
        """
        Test transaction commit, rollback, time ahead,
        time behind scenarios with replica, persist_to and
        replicate_to settings
        """
        # Atomicity.basic_ops.basic_ops.test_basic_commit
        self.drift_ahead = self.input.param("drift_ahead", False)
        self.drift_behind = self.input.param("drift_behind", False)
        gen_create = self.get_doc_generator(0, self.num_items)
        self.op_type = self.input.param("op_type", 'create')

        if self.drift_ahead:
            shell = RemoteMachineShellConnection(self.servers[0])
            self.assertTrue(shell.change_system_time(3600),
                            'Failed to advance the clock')

            output, _ = shell.execute_command('date')
            self.log.info('Date after is set forward {0}'.format(output))

        if self.drift_behind:
            shell = RemoteMachineShellConnection(self.servers[0])
            self.assertTrue(shell.change_system_time(-3600),
                            'Failed to advance the clock')

            output, _ = shell.execute_command('date')
            self.log.info('Date after is set behind {0}'.format(output))

        self.log.info("Loading docs using AtomicityTask")
        task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            gen_create, self.op_type, exp=0,
            batch_size=10,
            process_concurrency=self.process_concurrency,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries, update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=self.transaction_commit, durability=self.durability_level,
            sync=self.sync, defer=self.defer)
        self.log.info("going to execute the task")
        self.task.jython_task_manager.get_task_result(task)

        if self.op_type == "time_out":
            self.sleep(90, "Wait for staged docs to get cleared")
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                gen_create, "create", exp=0,
                batch_size=10,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                retries=self.sdk_retries, update_count=self.update_count,
                transaction_timeout=200,
                commit=self.transaction_commit,
                durability=self.durability_level,
                sync=self.sync, defer=self.defer)
            self.task_manager.get_task_result(task)

    def test_large_doc_size_commit(self):
        gen_create = self.generate_docs_bigdata(docs_per_day=self.num_items,
                                                document_size=self.doc_size)
        self.log.info("going to create a task")
        task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            gen_create, "create", exp=0,
            batch_size=10,
            process_concurrency=self.process_concurrency,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries,
            transaction_timeout=self.transaction_timeout,
            commit=self.transaction_commit, durability=self.durability_level,
            sync=self.sync, defer=self.defer)

        self.log.info("going to execute the task")
        self.task.jython_task_manager.get_task_result(task)

    def test_MB_41944(self):
        num_index = self.input.param("num_index", 1)
        # Create doc_gen for loading
        doc_gen = doc_generator(self.key, 0, 1)

        # Get key for delete op and reset the gen
        key, v = doc_gen.next()
        doc_gen.reset()

        # Open SDK client connection
        client = SDKClient([self.cluster.master], self.cluster.buckets[0])

        query = list()
        query.append("CREATE PRIMARY INDEX index_0 on %s USING GSI"
                     % self.cluster.buckets[0].name)
        if num_index == 2:
            query.append("CREATE INDEX index_1 on %s(name,age) "
                         "WHERE mutated=0 USING GSI"
                         % self.cluster.buckets[0].name)
        # Create primary index on the bucket
        for q in query:
            client.cluster.query(q)
        # Wait for index to become online`
        for index, _ in enumerate(query):
            query = "SELECT state FROM system:indexes WHERE name='index_%s'" \
                    % index
            index = 0
            state = None
            while index < 30:
                state = client.cluster.query(query) \
                    .rowsAsObject()[0].get("state")
                if state == "online":
                    break
                self.sleep(1)

            if state != "online":
                self.log_failure("Index 'index_%s' not yet online" % index)

        # Start transaction to create the doc
        trans_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            doc_gen, DocLoading.Bucket.DocOps.CREATE)
        self.task_manager.get_task_result(trans_task)

        # Perform sub_doc operation on same key
        _, fail = client.crud(DocLoading.Bucket.SubDocOps.INSERT,
                              key=key, value=["_sysxattr", "sysxattr-payload"],
                              xattr=True)
        if fail:
            self.log_failure("Subdoc insert failed: %s" % fail)
        else:
            self.log.info("Subdoc insert success")

        # Delete the created doc
        result = client.crud(DocLoading.Bucket.DocOps.DELETE, key)
        if result["status"] is False:
            self.log_failure("Doc delete failed: %s" % result["error"])
        else:
            self.log.info("Document deleted")
            # Re-insert same doc through transaction
            trans_task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets,
                doc_gen, DocLoading.Bucket.DocOps.CREATE)
            self.task_manager.get_task_result(trans_task)

        # Close SDK Client connection
        client.close()
        self.validate_test_failure()

    def test_multi_transactions(self):
        tasks = list()
        bucket = self.cluster.buckets[0]
        transaction_app = Transaction()
        trans_config = transaction_app.createTransactionConfig(
            self.transaction_timeout,
            self.transaction_durability_level)
        # master = Server(self.cluster.master.ip, self.cluster.master.port,
        #                 self.cluster.master.rest_username,
        #                 self.cluster.master.rest_password,
        #                 str(self.cluster.master.memcached_port))
        self.process_concurrency = 8
        self.tm = TaskManager(self.process_concurrency)

        workload = dict()
        workload["keyPrefix"] = "test_transaction-"
        workload["start"] = 0
        workload["end"] = self.num_items
        workload["batchSize"] = 1
        workload["min_key_size"] = 20
        workload["max_key_size"] = 20
        workload["randomize_keys"] = False
        workload["workers"] = self.process_concurrency
        workload["items"] = 0
        workload["transaction_on_same_keys"] = True
        workload["transaction_pattern"] = [
            [CbServer.default_scope, CbServer.default_collection,
             [["C", "R"], ["C"], ["C", "U"]]]
        ]
        workload["commit"] = None
        workload["rollback"] = None

        work_load = TransactionalWorkLoadSettings(
            workload["keyPrefix"],
            workload["start"],
            workload["end"],
            workload["batchSize"],
            workload["min_key_size"],
            workload["max_key_size"],
            workload["randomize_keys"],
            workload["workers"],
            workload["items"],
            workload["transaction_on_same_keys"],
            workload["transaction_pattern"],
            workload["commit"],
            workload["rollback"])

        client = SDKClient([self.cluster.master], bucket)
        transaction_obj = transaction_app.createTransaction(client.cluster,
                                                            trans_config)

        for index, load_pattern in enumerate(work_load.load_pattern):
            th_name = "Transaction_%s" % index
            batch_size = load_pattern[0]
            num_transactions = load_pattern[1]
            trans_pattern = load_pattern[2]
            task = TransactionWorkLoadGenerate(
                th_name, client.cluster, client.bucketObj, transaction_obj,
                work_load.doc_gen, batch_size, num_transactions, trans_pattern,
                work_load.commit, work_load.rollback, transaction_app)
            tasks.append(task)
            self.tm.submit(task)

        self.tm.getAllTaskResult()

        client.close()
