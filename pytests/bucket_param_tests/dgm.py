from threading import Thread

from BucketLib.BucketOperations import BucketHelper
from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection

from table_view import TableView


class Bucket_DGM_Tests(BaseTestCase):
    def setUp(self):
        super(Bucket_DGM_Tests, self).setUp()
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(
            ram_quota=self.bucket_size, replica=self.num_replicas,
            maxTTL=self.maxttl, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()

        self.cluster_util.print_cluster_stats()
        doc_create = doc_generator(
            self.key, 0, self.num_items,
            key_size=self.key_size, doc_size=self.doc_size,
            doc_type=self.doc_type, vbuckets=self.cluster_util.vbuckets)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_create, "create", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10,
                process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)
            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("========= Finished Bucket_DGM_Tests setup =======")

    def tearDown(self):
        super(Bucket_DGM_Tests, self).tearDown()

    def test_dgm_to_non_dgm(self):
        # Prepare DGM scenario
        bucket = self.bucket_util.get_all_buckets()[0]
        dgm_gen = doc_generator(
            self.key, self.num_items, self.num_items+1)
        dgm_task = self.task.async_load_gen_docs(
            self.cluster, bucket, dgm_gen, "create", 0,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=4,
            active_resident_threshold=self.active_resident_threshold)
        self.task_manager.get_task_result(dgm_task)
        num_items = dgm_task.doc_index

        gen_create = doc_generator(self.key, num_items,
                                   num_items+self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster_util.vbuckets)
        gen_update = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster_util.vbuckets)
        gen_delete = doc_generator(self.key, self.num_items, num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster_util.vbuckets)

        # Perform continuous updates while bucket moves from DGM->non-DGM state
        if not self.atomicity:
            tasks = list()
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_update, "update", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=2))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_delete, "delete", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                batch_size=10, process_concurrency=2,
                timeout_secs=self.sdk_timeout,
                skip_read_on_error=True))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=2))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            task = self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, gen_create,
                          "create", 0, batch_size=20,
                          process_concurrency=8,
                          replicate_to=self.replicate_to,
                          persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout,
                          retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          commit=self.transaction_commit,
                          durability=self.durability_level,
                          sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
            task = self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, gen_create,
                          "update", 0, batch_size=20,
                          process_concurrency=8,
                          replicate_to=self.replicate_to,
                          persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout,
                          retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          update_count=self.update_count,
                          commit=self.transaction_commit,
                          durability=self.durability_level,
                          sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
            task = self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, gen_delete,
                          "rebalance_delete", 0, batch_size=20,
                          process_concurrency=8,
                          replicate_to=self.replicate_to,
                          persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout,
                          retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          commit=self.transaction_commit,
                          durability=self.durability_level,
                          sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)

    def test_MB_40531(self):
        """
        Test to validate,
        1. Active resident ratio on the nodes never goes
           down below the replica_rr value
        2. 'evictable' (vb_replica_itm_mem - vb_replica_meta_data_mem) value
           never goes below wm_threshold of total bucket memory (ep_max_size)
        :return:
        """
        def check_replica_eviction():
            tbl = TableView(self.log.info)
            tbl.set_headers(["Node", "Memory", "WM_Threshold",
                             "Itm_mem", "Meta_mem", "Evictable_mem",
                             "A_rr", "R_rr"])
            while self.test_failure is None and run_eviction_check:
                tbl.rows = []
                for kv_node in node_data.keys():
                    all_stats = \
                        node_data[kv_node]["cbstat"].all_stats(bucket.name)
                    bucket_mem = int(all_stats["ep_max_size"])
                    wm_threshold = \
                        (float(all_stats["ep_mem_high_wat_percent"])
                         - float(all_stats["ep_mem_low_wat_percent"]))*100
                    evictable_mem = \
                        int(all_stats["vb_replica_itm_memory"]) \
                        - int(all_stats["vb_replica_meta_data_memory"])
                    active_rr = int(all_stats["vb_active_perc_mem_resident"])
                    replica_rr = int(all_stats["vb_replica_perc_mem_resident"])

                    tbl.add_row([kv_node.ip, str(bucket_mem),
                                 str(wm_threshold),
                                 all_stats["vb_replica_itm_memory"],
                                 all_stats["vb_replica_meta_data_memory"],
                                 str(evictable_mem),
                                 str(active_rr), str(replica_rr)])

                    if active_rr != 100 \
                            and evictable_mem > (bucket_mem/wm_threshold):
                        tbl.display("Node memory stats")
                        self.log_failure("%s - Active keys evicted before "
                                         "meeting the threshold: %s"
                                         % (kv_node.ip, all_stats))

                    if replica_rr > active_rr:
                        tbl.display("Node memory stats")
                        self.log_failure("%s: (active_rr) %s < %s (replica_rr)"
                                         % (kv_node.ip, active_rr, replica_rr))

        bucket = self.bucket_util.buckets[0]
        node_data = dict()
        kv_nodes = self.cluster_util.get_kv_nodes()
        for node in kv_nodes:
            cbstat = Cbstats(RemoteMachineShellConnection(node))
            node_data[node] = dict()
            node_data[node]["cbstat"] = cbstat
            node_data[node]["active"] = cbstat.vbucket_list(bucket.name,
                                                            "active")
            node_data[node]["replica"] = cbstat.vbucket_list(bucket.name,
                                                             "replica")

        target_dgm = 30
        run_eviction_check = True
        bucket_helper = BucketHelper(self.cluster.master)

        eviction_check_thread = Thread(target=check_replica_eviction)
        eviction_check_thread.start()

        op_index = 0
        op_batch_size = 8000
        create_batch_size = 10000

        # Perform ADD/SET/READ until targeted DGM value is reached
        curr_dgm = bucket_helper.fetch_bucket_stats(bucket.name)[
            "op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Wait for DGM to reach %s%%. Current DGM: %s%%"
                      % (target_dgm, curr_dgm))
        while int(curr_dgm) > target_dgm and self.test_failure is None:
            create_gen = doc_generator(
                self.key, self.num_items, self.num_items+create_batch_size,
                key_size=self.key_size, doc_size=self.doc_size,
                mutation_type="ADD")
            update_gen = doc_generator(
                self.key, op_index, op_index+op_batch_size,
                key_size=self.key_size, doc_size=self.doc_size,
                mutation_type="ADD")
            read_gen = doc_generator(
                self.key, op_index, op_index+op_batch_size,
                key_size=self.key_size, doc_size=0)

            create_task = self.task.async_load_gen_docs(
                self.cluster, bucket, create_gen, "create", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                batch_size=200,
                process_concurrency=1)
            update_task = self.task.async_load_gen_docs(
                self.cluster, bucket, update_gen, "update", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                batch_size=200,
                process_concurrency=1)
            read_task = self.task.async_load_gen_docs(
                self.cluster, bucket, read_gen, "read",
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                batch_size=200,
                process_concurrency=1)

            self.task_manager.get_task_result(create_task)
            self.task_manager.get_task_result(update_task)
            self.task_manager.get_task_result(read_task)

            # Update indexes for next iteration
            op_index += op_batch_size
            self.num_items += create_batch_size

            curr_dgm = bucket_helper.fetch_bucket_stats(bucket.name)[
                "op"]["samples"]["vb_active_resident_items_ratio"][-1]
            self.log.info("Current DGM: %s%%" % curr_dgm)

        # Stop eviction check thread
        run_eviction_check = False
        eviction_check_thread.join()

        # Close shell connections
        for node in kv_nodes:
            node_data[node]["cbstat"].shellConn.disconnect()

        self.validate_test_failure()
