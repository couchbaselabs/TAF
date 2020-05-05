import random
import time

from java.util.concurrent import ExecutionException

from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from xdcrbasetest import XDCRNewBaseTest
from xdcr_utils.xdcr_ready_functions import XDCRUtils


class XDCRTransactions(XDCRNewBaseTest):
    def setUp(self):
        super(XDCRTransactions, self).setUp()
        self.xdcr_util = XDCRUtils(self.clusters, self.task, self.taskmgr)
        self.src_cluster = self.xdcr_util.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.xdcr_util.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.master
        self.dest_master = self.dest_cluster.master
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        self.rdirection = self.input.param("rdirection", "unidirection")
        self.initial_xdcr = self.input.param("initial",
                                             random.choice([True, False]))
        self.atomicity = self.input.param("atomicity", True)
        self.doc_ops = ["create"]
        if self.initial_xdcr:
            self.mutate(self.doc_ops)
            self.xdcr_util.setup_xdcr_and_load()
        else:
            self.xdcr_util.load_and_setup_xdcr()
            self.mutate(self.doc_ops)

    def tearDown(self):
        super(XDCRTransactions, self).tearDown()

    def wait_for_op_to_complete(self, timeout=60):
        self.sleep(timeout, "Wait for ops to complete")

    def verify_results(self):
        for cb_cluster in self.clusters:
            for remote_cluster_ref in cb_cluster.xdcr_remote_clusters:
                for repl in remote_cluster_ref.get_replications():
                    dest_transaction_keys = self._transaction_records_exist()
                    if dest_transaction_keys > 0:
                        self.fail("{0} unexpected transaction records exist on dest bucket for {1}".format(
                            dest_transaction_keys, repl))
                    else:
                        self.log.info("{0} transaction records exist on dest bucket for {1}".format(
                            dest_transaction_keys, repl))

    def _transaction_records_exist(self):
        txn_filters = ["REGEXP_CONTAINS(meta().id, '^_txn:atr-') AND REGEXP_CONTAINS(meta().id, '-#[a-f0-9]+$')",
                       "REGEXP_CONTAINS(meta().id, '_txn:client-record')"]
        C1_to_C2_replications = self.src_rest.get_replications()
        return self.xdcr_util.verify_filtered_items(self.src_master, self.dest_master, C1_to_C2_replications, txn_filters)

    def get_cluster_objects_for_input(self, input):
        """returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated
           for eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.xdcr_util.get_cb_cluster_by_name(cluster_name))
        return clusters

    def mutate(self, doc_ops):
        try:
            for op in doc_ops:
                commit = random.choice([True, False])
                if op == "create":
                    start = self.num_items
                    end = self.num_items * 2
                if op == "update":
                    start = self.num_items / 4
                    end = self.num_items
                if op == "delete":
                    start = self.num_items / 4
                    end = (self.num_items / 2) - 1
            kv_gen = doc_generator("xdcr", start, end)
            self.load_all_buckets(kv_gen, op, commit)
        except ExecutionException as e:
            self.log.warn("Exceptions seen during doc ops but proceeding anyway {0}".format(e))

    def load_all_buckets(self, gen, op, commit):
        tasks = []
        if self.atomicity:
            tasks.append(self.task.async_load_gen_docs_atomicity(
                self.src_cluster,
                self.src_cluster.bucket_util.buckets,
                gen, op, exp=0, commit=commit,
                batch_size=10,
                process_concurrency=8))
        else:
            for bucket in self.src_cluster.bucket_util.buckets:
                tasks.append(self.task.async_load_gen_docs(
                    self.src_cluster, bucket, gen, op, 0, batch_size=20,
                    process_concurrency=1))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def test_replication_with_ops(self):
        tasks = []
        rebalance_in = self.input.param("rebalance_in", None)
        rebalance_out = self.input.param("rebalance_out", None)
        swap_rebalance = self.input.param("swap_rebalance", None)
        failover = self.input.param("failover", None)
        graceful = self.input.param("graceful", None)
        pause = self.input.param("pause", None)
        reboot = self.input.param("reboot", None)
        if pause:
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.xdcr_remote_clusters:
                    tasks.append(remote_cluster_refs.pause_all_replications())

        if rebalance_in:
            for cluster in self.get_cluster_objects_for_input(rebalance_in):
                tasks.append(cluster.async_rebalance_in())

        if failover:
            for cluster in self.get_cluster_objects_for_input(failover):
                tasks.append(cluster.failover_and_rebalance_nodes(
                    graceful=graceful,
                    rebalance=True))

        if rebalance_out:
            for cluster in self.get_cluster_objects_for_input(rebalance_out):
                tasks.append(cluster.async_rebalance_out())

        if swap_rebalance:
            for cluster in self.get_cluster_objects_for_input(swap_rebalance):
                tasks.append(cluster.async_swap_rebalance())

        if pause:
            self.wait_for_op_to_complete(10)
            for cluster in self.get_cluster_objects_for_input(pause):
                for remote_cluster_refs in cluster.xdcr_remote_clusters:
                    tasks.append(remote_cluster_refs.resume_all_replications())

        if reboot:
            for cluster in self.get_cluster_objects_for_input(reboot):
                cluster.warmup_node()
            self.wait_for_op_to_complete(60)
        self.mutate(["delete", "update", "create"])
        for task in tasks:
            try:
                task.jython_task_manager.get_task_result(task)
            except ExecutionException:
                pass
        # Wait for replication to catch up
        self.wait_for_op_to_complete(120)
        self.verify_results()
