from random import choice
from threading import Thread, Lock

from basetestcase import ClusterSetup
from cb_constants import DocLoading
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from com.couchbase.test.transactions import SimpleTransaction as Transaction
from reactor.util.function import Tuples

from sdk_client3 import TransactionConfig, SDKClient


class OutOfOrderReturns(ClusterSetup):
    def setUp(self):
        super(OutOfOrderReturns, self).setUp()

        # Create default bucket
        self.create_bucket(self.cluster)

        self.ooo_order = 0
        self.test_lock = Lock()
        self.doc_ops = self.input.param("doc_ops", "update;update").split(";")

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        self.bucket = self.cluster.buckets[0]

        # Create sdk_clients for pool
        if self.cluster.sdk_client_pool:
            self.log.info("Creating SDK client pool")
            self.cluster.sdk_client_pool.create_clients(
                self.cluster, self.bucket,
                req_clients=self.sdk_pool_capacity,
                compression_settings=self.sdk_compression)

        # Create shell connection to each kv_node for cbstat object
        self.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        self.node_data = dict()
        for node in self.kv_nodes:
            shell = RemoteMachineShellConnection(node)
            cb_stat = Cbstats(node)
            self.node_data[node] = dict()
            self.node_data[node]["shell"] = shell
            self.node_data[node]["cb_stat"] = cb_stat
            self.node_data[node]["active_vbs"] = \
                cb_stat.vbucket_list(self.bucket.name, vbucket_type="active")
            self.node_data[node]["replica_vbs"] = \
                cb_stat.vbucket_list(self.bucket.name, vbucket_type="replica")

        # Print cluster & bucket stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

    def tearDown(self):
        # Close all opened remote_shell connection
        for node in self.kv_nodes:
            self.node_data[node]["shell"].disconnect()
            self.node_data[node]["cb_stat"].disconnect()

        super(OutOfOrderReturns, self).tearDown()

    def __validate_crud_result(self, op_type, client_result):
        if op_type == "read_with_replicas":
            pass
        elif client_result["status"] is False:
            self.log_failure("Doc_op %s failed: %s"
                             % (op_type, client_result["error"]))

    def crud(self, client, doc_op, key, value=None, durability="",
             expected_thread_val=1, warn_only=False):
        failure_log = self.log_failure
        if warn_only:
            failure_log = self.log.warning

        self.log.info("Starting %s for key %s with durability='%s'"
                      % (doc_op, key, durability))
        if doc_op == "read_with_replicas":
            result = client.get_from_all_replicas(key)
        else:
            result = client.crud(doc_op, key, value,
                                 durability=durability,
                                 timeout=self.sdk_timeout)

        # Acquire lock to make sure this thread completed honouring OoO
        self.test_lock.acquire()
        if self.ooo_order != expected_thread_val:
            failure_log("%s - expected thread_num: %s, got %s"
                        % (doc_op, expected_thread_val, self.ooo_order))
        self.ooo_order += 1
        self.test_lock.release()
        self.__validate_crud_result(doc_op, result)

    def test_dgm_reads(self):
        evicted_doc_keys = list()
        non_evicted_doc_keys = list()
        req_docs_to_test = 10
        initial_load = 50000
        dgm_gen_init = doc_generator(self.key, 0, initial_load,
                                     doc_size=self.doc_size)
        dgm_task_init = self.task.async_load_gen_docs(
            self.cluster, self.bucket, dgm_gen_init, "create", exp=0,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=1)
        self.task_manager.get_task_result(dgm_task_init)

        dgm_gen = doc_generator(self.key, initial_load , initial_load + 1,
                                doc_size=self.doc_size)
        dgm_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, dgm_gen, "create", exp=0,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=4,
            active_resident_threshold=self.active_resident_threshold)
        self.task_manager.get_task_result(dgm_task)
        self.num_items = dgm_task.doc_index

        client = self.cluster.sdk_client_pool.get_client_for_bucket(
            self.bucket, self.scope_name, self.collection_name)

        # Fetch evicted doc keys
        dgm_gen = doc_generator(self.key, 0, self.num_items, doc_size=1)
        while len(evicted_doc_keys) != req_docs_to_test:
            if not dgm_gen.has_next():
                end_i = self.num_items + 10000
                dgm_gen = doc_generator(self.key, self.num_items, end_i,
                                        doc_size=1)
                self.num_items = end_i
            doc_key, _ = dgm_gen.next()
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(
                doc_key, self.bucket.num_vbuckets)
            for node in self.kv_nodes:
                if vb_for_key in self.node_data[node]["active_vbs"]:
                    stat = self.node_data[node]["cb_stat"].vkey_stat(
                        self.bucket.name, doc_key, vbucket_num=vb_for_key)
                    if stat["is_resident"] == "false":
                        evicted_doc_keys.append(doc_key)

        dgm_gen = doc_generator(self.key, -(self.num_items-1), 0, doc_size=1)
        while len(non_evicted_doc_keys) != req_docs_to_test:
            doc_key, _ = dgm_gen.next()
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(
                doc_key, self.bucket.num_vbuckets)
            for node in self.kv_nodes:
                if vb_for_key in self.node_data[node]["active_vbs"]:
                    stat = self.node_data[node]["cb_stat"].vkey_stat(
                        self.bucket.name, doc_key, vbucket_num=vb_for_key)
                    if stat["is_resident"] == "true":
                        non_evicted_doc_keys.append(doc_key)

        self.log.info("Evicted docs: %s" % evicted_doc_keys)
        self.log.info("Non-Evicted docs: %s" % non_evicted_doc_keys)

        load_gen = doc_generator(self.key, self.num_items,
                                 self.num_items+req_docs_to_test)

        # Test evicted key read
        for index in range(req_docs_to_test):
            read_key = evicted_doc_keys[index]
            self.ooo_order = 0
            if self.doc_ops[0] == DocLoading.Bucket.DocOps.CREATE:
                op_key, value = load_gen.next()
            else:
                op_key, value = non_evicted_doc_keys[index], "{'f1': 'val'}"

            read_thread = Thread(target=self.crud,
                                 args=[client, "read_with_replicas",
                                       read_key],
                                 kwargs={"expected_thread_val": 1,
                                         "warn_only": True})
            ooo_op_thread = Thread(target=self.crud,
                                   args=[client, self.doc_ops[0], op_key],
                                   kwargs={"value": value,
                                           "expected_thread_val": 0,
                                           "warn_only": True})

            read_thread.start()
            ooo_op_thread.start()

            read_thread.join()
            ooo_op_thread.join()

            self.validate_test_failure()

    def test_with_sync_write(self):
        cluster_node = choice(self.kv_nodes)
        target_vb_type, simulate_error = \
            DurabilityHelper.get_vb_and_error_type(self.durability_level)
        doc_gen = doc_generator(
            self.key, 0, 2,
            target_vbucket=self.node_data[cluster_node][
                "%s_vbs" % target_vb_type])
        client = self.cluster.sdk_client_pool.get_client_for_bucket(
            self.bucket, self.scope_name, self.collection_name)

        key_1, value_1 = doc_gen.next()
        key_2, value_2 = doc_gen.next()

        if self.doc_ops[0] != DocLoading.Bucket.DocOps.CREATE:
            client.crud(DocLoading.Bucket.DocOps.CREATE, key_1, value_1)
        if self.doc_ops[1] != DocLoading.Bucket.DocOps.CREATE:
            client.crud(DocLoading.Bucket.DocOps.CREATE, key_2, value_2)

        sync_op = Thread(target=self.crud,
                         args=[client, self.doc_ops[0], key_1],
                         kwargs={"value": value_1,
                                 "durability": self.durability_level,
                                 "expected_thread_val": 1})
        async_op = Thread(target=self.crud,
                          args=[client, self.doc_ops[1], key_2],
                          kwargs={"value": value_2,
                                  "expected_thread_val": 0})

        cb_err = CouchbaseError(self.log,
                                self.node_data[cluster_node]["shell"],
                                node=cluster_node)
        cb_err.create(simulate_error, self.bucket.name)

        # Start doc_ops
        sync_op.start()
        self.sleep(1, "Wait before async operation")
        async_op.start()

        # Wait for ops to complete
        async_op.join()
        cb_err.revert(simulate_error, self.bucket.name)
        sync_op.join()

        self.validate_test_failure()

    def trans_doc_gen(self, start, end, op_type):
        docs = list()
        value = {'mutated': 0}
        content = self.client.translate_to_json_object(value)
        for i in range(start, end):
            key = "%s-%s" % (self.key, i)
            if op_type == DocLoading.Bucket.DocOps.CREATE:
                doc = Tuples.of(key, content)
                docs.append(doc)
            else:
                docs.append(key)
        return docs

    def __transaction_runner(self, trans_obj, docs, op_type, tnx_options):
        exception = None
        if op_type == DocLoading.Bucket.DocOps.CREATE:
            exception = trans_obj.RunTransaction(
                self.client.cluster, [self.client.collection],
                docs, [], [], True, True, 1, tnx_options)
        elif op_type == DocLoading.Bucket.DocOps.UPDATE:
            exception = trans_obj.RunTransaction(
                self.client.cluster, [self.client.collection],
                [], docs, [], True, True, 1, tnx_options)
        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            exception = trans_obj.RunTransaction(
                self.client.cluster, [self.client.collection],
                [], [], docs, True, True, 1, tnx_options)
        if exception:
            self.log_failure("'%s' transx failed: %s" % (op_type, exception))

    def test_transaction_with_crud(self):
        doc_op = self.doc_ops[0]
        transx_op = self.doc_ops[1]
        trans_obj = Transaction()
        supported_d_levels = self.bucket_util.get_supported_durability_levels()

        self.client = self.cluster.sdk_client_pool.get_client_for_bucket(
            self.bucket, self.scope_name, self.collection_name)

        trans_options = SDKClient.get_transaction_options(
            TransactionConfig(durability=self.durability_level,
                              timeout=self.transaction_timeout))

        half_of_num_items = self.num_items / 2
        doc_gen = doc_generator(self.key, 0, half_of_num_items)

        # Create docs for update/delete ops
        if doc_op != DocLoading.Bucket.DocOps.CREATE:
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_gen,
                DocLoading.Bucket.DocOps.CREATE,
                timeout_secs=self.sdk_timeout,
                process_concurrency=8,
                batch_size=100)
            self.task_manager.get_task_result(task)
        if transx_op != DocLoading.Bucket.DocOps.CREATE:
            t_doc_gen = self.trans_doc_gen(half_of_num_items, self.num_items,
                                           DocLoading.Bucket.DocOps.CREATE)
            self.__transaction_runner(trans_obj, t_doc_gen,
                                      DocLoading.Bucket.DocOps.CREATE,
                                      trans_options)

        replicate_to = choice(range(0, self.num_replicas))
        persist_to = choice(range(0, self.num_replicas + 1))
        durability = choice(supported_d_levels)
        self.log.info("%s replicate_to=%s, persist_to=%s, durability=%s"
                      % (doc_op, replicate_to, persist_to, durability))

        t_doc_gen = self.trans_doc_gen(half_of_num_items, self.num_items,
                                       transx_op)
        trans_thread = Thread(
            target=self.__transaction_runner,
            args=[trans_obj, t_doc_gen, transx_op, trans_options])
        crud_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen, doc_op,
            replicate_to=replicate_to, persist_to=persist_to,
            durability=durability,
            timeout_secs=self.sdk_timeout,
            process_concurrency=1,
            batch_size=1)
        trans_thread.start()
        trans_thread.join()
        self.task_manager.get_task_result(crud_task)
        if crud_task.fail:
            self.log_failure("Failures seen during doc_crud: %s"
                             % crud_task.fail)
        self.validate_test_failure()

    def test_parallel_transactions(self):
        trans_obj = Transaction()
        self.client = self.cluster.sdk_client_pool.get_client_for_bucket(
            self.bucket, self.scope_name, self.collection_name)
        # Create transaction options
        trans_options = SDKClient.get_transaction_options(
            TransactionConfig(durability=self.durability_level,
                              timeout=self.transaction_timeout))

        # Create docs for update/delete ops
        if self.doc_ops[0] != DocLoading.Bucket.DocOps.CREATE:
            docs = self.trans_doc_gen(0, self.num_items/2,
                                      DocLoading.Bucket.DocOps.CREATE)
            self.__transaction_runner(trans_obj, docs,
                                      DocLoading.Bucket.DocOps.CREATE,
                                      trans_options)
        if self.doc_ops[1] != DocLoading.Bucket.DocOps.CREATE:
            docs = self.trans_doc_gen(self.num_items/2, self.num_items,
                                      DocLoading.Bucket.DocOps.CREATE)
            self.__transaction_runner(trans_obj, docs,
                                      DocLoading.Bucket.DocOps.CREATE,
                                      trans_options)

        # Create doc_gens for test
        doc_set = list()
        doc_set.append(self.trans_doc_gen(0, self.num_items/2,
                                          self.doc_ops[0]))
        doc_set.append(self.trans_doc_gen(self.num_items/2, self.num_items,
                                          self.doc_ops[1]))

        t1 = Thread(target=self.__transaction_runner,
                    args=[trans_obj, doc_set[0], self.doc_ops[0],
                          trans_options])
        t2 = Thread(target=self.__transaction_runner,
                    args=[trans_obj, doc_set[1], self.doc_ops[1],
                          trans_options])

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.validate_test_failure()
