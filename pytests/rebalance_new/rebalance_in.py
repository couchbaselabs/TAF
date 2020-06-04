import time

import Jython_tasks.task as jython_tasks
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from rebalance_base import RebalanceBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from rebalance_new import rebalance_base

retry_exceptions = rebalance_base.retry_exceptions +\
                    [SDKException.RequestCanceledException]


class RebalanceInTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceInTests, self).setUp()

    def tearDown(self):
        super(RebalanceInTests, self).tearDown()

    def test_rebalance_in_with_ops_durable(self):
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items+ self.items)
        self.gen_delete = self.get_doc_generator(self.items / 2,
                                                 self.items)
        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        self.sleep(10)

        tasks_info = self.bucket_util._async_load_all_buckets(
            self.cluster, self.gen_create, "create", 0,
            batch_size=self.batch_size, process_concurrency=self.process_concurrency,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            durability=self.durability_level,
            ryow=self.ryow, check_persistence=self.check_persistence)

        self.task_manager.get_task_result(rebalance_task)
        for task in tasks_info.keys():
            self.task_manager.get_task_result(task)
            if task.__class__ == jython_tasks.Durability:
                self.log.error(task.sdk_acked_curd_failed.keys())
                self.log.error(task.sdk_exception_crud_succeed.keys())
                self.assertTrue(
                    len(task.sdk_acked_curd_failed) == 0,
                    "sdk_acked_curd_failed for docs: %s" % task.sdk_acked_curd_failed.keys())
                self.assertTrue(
                    len(task.sdk_exception_crud_succeed) == 0,
                    "sdk_exception_crud_succeed for docs: %s" % task.sdk_exception_crud_succeed.keys())
                self.assertTrue(
                    len(task.sdk_exception_crud_succeed) == 0,
                    "create failed for docs: %s" % task.create_failed.keys())
                self.assertTrue(
                    len(task.sdk_exception_crud_succeed) == 0,
                    "update failed for docs: %s" % task.update_failed.keys())
                self.assertTrue(
                    len(task.sdk_exception_crud_succeed) == 0,
                    "delete failed for docs: %s" % task.delete_failed.keys())
        self.assertTrue(rebalance_task.result, "Rebalance Failed")

    def test_rebalance_in_with_ops(self):
        items = self.num_items
        delete_from = items/2
        create_from = items

        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + items)
        self.gen_delete = self.get_doc_generator(delete_from,
                                                 delete_from + items/2)

        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])

        self.sleep(10, "wait for rebalance to start")

        # CRUDs while rebalance is running in parallel
        tasks_info = self.loadgen_docs(retry_exceptions=retry_exceptions)

        delete_from += items
        create_from += items
        # Waif for rebalance and doc mutation tasks to complete
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.task_manager.abort_all_tasks()
            for task in tasks_info:
                try:
                    for client in task.clients:
                        client.close()
                except:
                    pass
            self.fail("Rebalance Failed")

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.cluster.nodes_in_cluster.extend(servs_in)

        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        if self.doc_ops == "delete":
            return
        self.sleep(20, "Wait for cluster to be ready after rebalance")

        # CRUDs after rebalance operations
        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + items)
        self.gen_delete = self.get_doc_generator(delete_from,
                                                 delete_from + items/2)
        tasks_info = self.loadgen_docs(retry_exceptions=retry_exceptions,
                                       task_verification=True)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info['ops_failed'],
                    "Doc ops failed for task: {}".format(task.thread_name))

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_rebalance_in_with_ops_sync_async(self):
        items = self.num_items
        delete_from = items/2
        create_from = items

        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + items)
        self.gen_delete = self.get_doc_generator(delete_from,
                                                 delete_from + items/2)

        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])

        self.sleep(10, "wait for rebalance to start")

        # CRUDs while rebalance is running in parallel
        tasks_info = self.loadgen_docs(retry_exceptions=retry_exceptions)

        delete_from += items
        create_from += items
        # CRUDs after rebalance operations
        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + items)
        self.gen_delete = self.get_doc_generator(delete_from,
                                                 delete_from + items/2)
        temp = self.durability_level
        self.durability_level = "NONE"
        tasks_info.update(self.loadgen_docs())
        self.durability_level = temp

        delete_from += items
        create_from += items
        # Waif for rebalance and doc mutation tasks to complete
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.task_manager.abort_all_tasks()
            for task in tasks_info:
                try:
                    for client in task.clients:
                        client.close()
                except:
                    pass
            self.fail("Rebalance Failed")

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.cluster.nodes_in_cluster.extend(servs_in)

        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.sleep(20, "Wait for cluster to be ready after rebalance")

        # CRUDs after rebalance operations
        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + items)
        self.gen_delete = self.get_doc_generator(delete_from,
                                                 delete_from + items/2)
        tasks_info = self.loadgen_docs(retry_exceptions=retry_exceptions,
                                       task_verification=True)
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info['ops_failed'],
                    "Doc ops failed for task: {}".format(task.thread_name))

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.validate_docs_per_collections_all_buckets()

    def rebalance_in_after_ops(self):
        """
        Rebalances nodes into cluster while doing docs ops:create/delete/update

        This test begins by loading a given number of items into the cluster.
        Then adds nodes_in nodes at a time and rebalances that nodes
        into the cluster.
        During the rebalance we perform docs ops(add/remove/update/readd)
        in the cluster( operate with a half of items that were loaded before).
        Once the cluster is rebalanced we wait for the disk queues to drain,
        then verify that there has been no data loss and sum(curr_items) match
        the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """
        self.gen_update = self.get_doc_generator(0, self.num_items)
        std = self.std_vbucket_dist or 1.0
        tasks = []
        for bucket in self.bucket_util.buckets:
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, self.gen_update, "update", 0,
                batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                pause_secs=5, timeout_secs=self.sdk_timeout,
                retries=self.sdk_retries))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        if self.test_abort_snapshot:
            self.log.info("Creating abort scenarios for all vbs")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name, "replica")
                load_gen = doc_generator(self.key, 0, 5000,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        self.sleep(20)
        for bucket in self.bucket_util.buckets:
            current_items = self.bucket_util.get_bucket_current_item_count(self.cluster, bucket)
            self.num_items = current_items
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.sleep(20)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        rebalance = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.sleep(60)
        self.cluster.nodes_in_cluster.extend(servs_in)
        for bucket in self.bucket_util.buckets:
            current_items = self.bucket_util.get_bucket_current_item_count(self.cluster, bucket)
            self.num_items = current_items
        self.bucket_util.validate_docs_per_collections_all_buckets(timeout=self.wait_timeout)
        self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
        new_failover_stats = self.bucket_util.compare_failovers_logs(
            prev_failover_stats, self.cluster.servers[:self.nodes_in + self.nodes_init],
            self.bucket_util.buckets)
        new_vbucket_stats = self.bucket_util.compare_vbucket_seqnos(
            prev_vbucket_stats, self.cluster.servers[:self.nodes_in + self.nodes_init],
            self.bucket_util.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        self.sleep(30)
        self.bucket_util.data_analysis_active_replica_all(
            disk_active_dataset, disk_replica_dataset,
            self.cluster.servers[:self.nodes_in + self.nodes_init],
            self.bucket_util.buckets, path=None)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(
            servers=nodes, buckets=self.bucket_util.buckets,
            num_replicas=self.num_replicas,
            std=std, total_vbuckets=self.cluster_util.vbuckets)

    def rebalance_in_with_failover_full_addback_recovery(self):
        """
        Rebalances nodes in with failover and full recovery add back of a node

        This test begins by loading a given number of items into the cluster.
        Then adds nodes_in nodes at a time and rebalances that nodes
        into the cluster.
        During the rebalance we perform docs ops(add/remove/update/readd)
        in the cluster( operate with a half of items that were loaded before).
        Once the cluster is rebalanced we wait for the disk queues to drain,
        then verify that there has been no data loss and sum(curr_items)
        match the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """

        self.gen_update = self.get_doc_generator(0, self.items/2)
        self.gen_create = self.get_doc_generator(self.num_items, self.num_items+self.items)
        self.gen_delete = self.get_doc_generator(self.items/2, self.items)
        std = self.std_vbucket_dist or 1.0
        tasks_info = self.loadgen_docs()

        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
        self.rest = RestConnection(self.cluster.master)
        self.nodes = self.cluster_util.get_nodes(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="full")
        rebalance = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.sleep(10)
        self.cluster.nodes_in_cluster.extend(servs_in)
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                       self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util.validate_docs_per_collections_all_buckets(timeout=self.wait_timeout)
        self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
        self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.servers[:self.nodes_in + self.nodes_init], self.bucket_util.buckets)
        self.bucket_util.data_analysis_active_replica_all(
            disk_active_dataset, disk_replica_dataset,
            self.cluster.servers[:self.nodes_in + self.nodes_init],
            self.bucket_util.buckets, path=None)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(
            servers=nodes, buckets=self.bucket_util.buckets,
            num_replicas=self.num_replicas,
            std=std, total_vbuckets=self.cluster_util.vbuckets)

    def rebalance_in_with_failover(self):
        """
        Rebalances  after we do add node and graceful failover

        This test begins by loading a given number of items into the cluster.
        It then adds nodes_in nodes at a time and rebalances that nodes
        into the cluster.
        During the rebalance we perform docs ops(add/remove/update/readd)
        in the cluster( operate with a half of items that were loaded before).
        We then  add a node and do graceful failover followed by rebalance
        Once the cluster is rebalanced we wait for the disk queues to drain,
        then verify that there has been no data loss and sum(curr_items)
        match the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """

        fail_over = self.input.param("fail_over", False)
        self.gen_update = self.get_doc_generator(0, self.num_items)
        std = self.std_vbucket_dist or 1.0
        tasks = []
        for bucket in self.bucket_util.buckets:
            tasks.append(self.task.async_load_gen_docs(
                                    self.cluster, bucket, self.gen_update, "update", 0,
                                    batch_size=20, persist_to=self.persist_to,
                                    replicate_to=self.replicate_to, pause_secs=5,
                                    durability=self.durability_level,
                                    timeout_secs=self.sdk_timeout,
                                    retries=self.sdk_retries))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        self.sleep(20)
        for bucket in self.bucket_util.buckets:
            current_items = self.bucket_util.get_bucket_current_item_count(self.cluster, bucket)
            self.num_items = current_items
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets(timeout=self.wait_timeout)
        self.sleep(20)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
        self.rest = RestConnection(self.cluster.master)
        self.nodes = self.cluster_util.get_nodes(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        self.rest = RestConnection(self.cluster.master)
        self.rest.add_node(self.cluster.master.rest_username,
                           self.cluster.master.rest_password,
                           self.cluster.servers[self.nodes_init].ip,
                           self.cluster.servers[self.nodes_init].port)
        # Mark Node for failover
        self.rest.fail_over(chosen[0].id, graceful=fail_over)
        if fail_over:
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True),
                            msg="Graceful Failover Failed")
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[chosen[0].id])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True),
                        msg="Rebalance Failed")
        self.sleep(60)
        # Verification
        new_server_list = self.cluster_util.add_remove_servers(
            self.cluster.servers, self.cluster.servers[:self.nodes_init],
            [chosen[0]], [self.cluster.servers[self.nodes_init]])
        self.cluster.nodes_in_cluster = new_server_list
        self.bucket_util.validate_docs_per_collections_all_buckets(timeout=self.wait_timeout)
        self.bucket_util.verify_cluster_stats(self.num_items,
                                              check_ep_items_remaining=True)
        self.bucket_util.compare_failovers_logs(
            prev_failover_stats, new_server_list, self.bucket_util.buckets)
        self.sleep(30)
        self.bucket_util.data_analysis_active_replica_all(
            disk_active_dataset, disk_replica_dataset, new_server_list,
            self.bucket_util.buckets, path=None)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(
            servers=nodes, buckets=self.bucket_util.buckets,
            num_replicas=self.num_replicas,
            std=std, total_vbuckets=self.cluster_util.vbuckets)

    def rebalance_in_with_compaction_and_ops(self):
        """
        Rebalances nodes into a cluster while doing
        docs ops:create, delete, update

        This test begins by loading a given number of items into the cluster.
        We later run compaction on all buckets and do ops as well
        """

        compaction_tasks = list()
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items)
        self.gen_delete = self.get_doc_generator(self.num_items / 2,
                                                 (self.num_items+self.items)/2)
        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])

        self.sleep(10, "wait for rebalance to start")

        for bucket in self.bucket_util.buckets:
            compaction_tasks.append(self.task.async_compact_bucket(
                self.cluster.master, bucket))

        # CRUDs while rebalance is running in parallel
        tasks_info = self.loadgen_docs(retry_exceptions=retry_exceptions)

        self.task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.task_manager.abort_all_tasks()
            for task in tasks_info:
                try:
                    for client in task.clients:
                        client.close()
                except:
                    pass
            self.fail("Rebalance Failed")
        print "Rebalance fisihed successfully in testcase: %s" % rebalance_task.result
        for task in compaction_tasks:
            self.task_manager.get_task_result(task)
            if not task.result:
                self.task_manager.abort_all_tasks()
                for task in tasks_info:
                    try:
                        for client in task.clients:
                            client.close()
                    except:
                        pass
            self.assertTrue(task.result, "Compaction failed for bukcet: %s" %
                            task.bucket.name)

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info['ops_failed'],
                    "Doc ops failed for task: {}".format(task.thread_name))

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.validate_docs_per_collections_all_buckets()

        self.cluster.nodes_in_cluster.extend(servs_in)
        self.sleep(60)
        for bucket in self.bucket_util.buckets:
            current_items = self.bucket_util.get_bucket_current_item_count(self.cluster, bucket)
            self.num_items = current_items
        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        self.assertTrue(rebalance_task.result, "Rebalance Failed")

    def rebalance_in_with_ops_batch(self):
        self.gen_delete = self.get_doc_generator((self.num_items / 2 - 1), (self.num_items / 2 - 1)+self.items)
        self.gen_create = self.get_doc_generator(self.num_items+1, self.num_items+self.items/2)
        servs_in = [self.cluster.servers[i + 1] for i in range(self.nodes_in)]
        rebalance = self.task.async_rebalance(self.cluster.servers[:1], servs_in, [])
        if self.doc_ops is not None:
            # define which doc's ops will be performed during rebalancing
            # allows multiple of them but one by one
            if "update" in self.doc_ops:
                self._load_all_buckets(
                    self.gen_update, "update", 0, 4294967295, True,
                    batch_size=20, pause_secs=5, timeout_secs=180)
            if "create" in self.doc_ops:
                self._load_all_buckets(
                    self.gen_create, "create", 0, 4294967295, True,
                    batch_size=20, pause_secs=5, timeout_secs=180)
            if "delete" in self.doc_ops:
                self._load_all_buckets(
                    self.gen_delete, "delete", 0, 4294967295, True,
                    batch_size=20, pause_secs=5, timeout_secs=180)
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.cluster.nodes_in_cluster.extend(servs_in)
        self.sleep(60)
        for bucket in self.bucket_util.buckets:
            current_items = self.bucket_util.get_bucket_current_item_count(self.cluster, bucket)
            self.num_items = current_items
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def rebalance_in_get_random_key(self):
        """
        Rebalances nodes into a cluster during getting random keys.

        This test begins by loading a given number of items into the node.
        Then it creates cluster with self.nodes_init nodes. Then we
        send requests to all nodes in the cluster to get random key values.
        Next step is add nodes_in nodes into cluster and rebalance it.
        During rebalancing we get random keys from all nodes and
        verify that are different every time.
        Once the cluster has been rebalanced we again get random keys from all
        new nodes in the cluster, then we wait for the disk queues to drain,
        and then verify that there has been no data loss, sum(curr_items)
        match the curr_items_total
        """

        servs_in = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        rebalance = self.task.async_rebalance(self.cluster.servers[:1], servs_in, [])
        self.sleep(5)
        rest_cons = [RestConnection(self.cluster.servers[i]) for i in xrange(self.nodes_init)]
        result = []
        num_iter = 0
        # get random keys for each node during rebalancing
        while rest_cons[0]._rebalance_progress_status() == 'running' and num_iter < 100:
            temp_result = []
            self.log.info("getting random keys for all nodes in cluster....")
            for rest in rest_cons:
                result.append(rest.get_random_key('default'))
                self.sleep(1)
                temp_result.append(rest.get_random_key('default'))

            if tuple(temp_result) == tuple(result):
                self.log.exception("random keys are not changed")
            else:
                result = temp_result
            num_iter += 1

        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.cluster.nodes_in_cluster.extend(servs_in)
        self.sleep(60)
        for bucket in self.bucket_util.buckets:
            current_items = self.bucket_util.get_bucket_current_item_count(self.cluster, bucket)
            self.num_items = current_items
        # get random keys for new added nodes
        rest_cons = [RestConnection(self.cluster.servers[i]) for i in xrange(self.nodes_init + self.nodes_in)]
        for rest in rest_cons:
            result = rest.get_random_key('default')
        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def incremental_rebalance_in_with_ops(self):
        """
        Rebalances nodes into a cluster while doing mutations.

        This test begins by loading a given number of items into the cluster.
        Then adds two nodes at a time & rebalances that node into the cluster.
        During the rebalance we update(all of the items in the cluster)/
        delete(num_items/(num_servers-1) in each iteration)/
        create(a half of initial items in each iteration).
        Once the cluster has been rebalanced we wait for the disk queues to
        drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """

        num_of_items = self.num_items
        task = None
        op_type = None

        for i in range(self.nodes_init, self.num_servers, 2):
            tasks_info = dict()
            # Start rebalance task
            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:i], self.cluster.servers[i:i + 2], [])

            self.sleep(10, "wait for rebalance to start")
            # define which doc_op to perform during rebalance
            # only one type of ops can be passed
            for bucket in self.bucket_util.buckets:
                if "update" in self.doc_ops:
                    op_type = "update"
                    # 1/2th of data will be updated in each iteration
                    if self.atomicity:
                        task = self.task.async_load_gen_docs_atomicity(
                            self.cluster, self.bucket_util.buckets,
                            self.gen_update, "rebalance_only_update",
                            0, batch_size=20,
                            timeout_secs=self.sdk_timeout,
                            process_concurrency=8, retries=self.sdk_retries,
                            transaction_timeout=self.transaction_timeout,
                            commit=self.transaction_commit,
                            durability=self.durability_level, sync=self.sync,
                            defer=self.defer)
                    else:
                        task = self.task.async_load_gen_docs(
                            self.cluster, bucket, self.gen_update, "update", 0,
                            batch_size=20, persist_to=self.persist_to,
                            replicate_to=self.replicate_to, pause_secs=5,
                            durability=self.durability_level,
                            timeout_secs=self.sdk_timeout,
                            retries=self.sdk_retries)
                elif "create" in self.doc_ops:
                    op_type = "create"
                    # 1/2th of initial data will be added in each iteration
                    tem_num_items = int(self.num_items * (1 + i / 2.0))
                    self.gen_create = self.get_doc_generator(num_of_items,
                                                             tem_num_items)
                    num_of_items = tem_num_items
                    if self.atomicity:
                        task = self.task.async_load_gen_docs_atomicity(
                            self.cluster, bucket, self.gen_create,
                            "create", 0,
                            batch_size=10, timeout_secs=self.sdk_timeout,
                            process_concurrency=8,
                            retries=self.sdk_retries,
                            transaction_timeout=self.transaction_timeout,
                            commit=self.transaction_commit,
                            durability=self.durability_level,
                            sync=self.sync, defer=self.defer)
                    else:
                        task = self.task.async_load_gen_docs(
                            self.cluster, bucket, self.gen_create, "create", 0,
                            batch_size=20, persist_to=self.persist_to,
                            replicate_to=self.replicate_to, pause_secs=5,
                            durability=self.durability_level,
                            timeout_secs=self.sdk_timeout,
                            retries=self.sdk_retries)
                elif "delete" in self.doc_ops:
                    op_type = "delete"
                    # 1/(num_servers) of initial data will be removed after
                    # each iteration at the end we should get an
                    # empty base or couple items
                    tem_del_start_num = int(self.num_items * (1 - i / (self.num_servers - 1.0))) + 1
                    tem_del_end_num = int(self.num_items * (1 - (i - 1) / (self.num_servers - 1.0)))
                    self.gen_delete = self.get_doc_generator(tem_del_start_num,
                                                             tem_del_end_num)
                    num_of_items -= (tem_del_end_num - tem_del_start_num + 1)
                    if self.atomicity:
                        task = self.task.async_load_gen_docs_atomicity(
                            self.cluster, self.bucket_util.buckets,
                            self.gen_delete, "rebalance_delete", 0,
                            batch_size=10,
                            timeout_secs=self.sdk_timeout,
                            process_concurrency=8,
                            retries=self.sdk_retries,
                            transaction_timeout=self.transaction_timeout,
                            commit=self.transaction_commit,
                            durability=self.durability_level,
                            sync=self.sync, defer=self.defer)
                    else:
                        task = self.task.async_load_gen_docs(
                            self.cluster, bucket, self.gen_delete, "delete", 0,
                            batch_size=20, persist_to=self.persist_to,
                            replicate_to=self.replicate_to, pause_secs=5,
                            durability=self.durability_level,
                            timeout_secs=self.sdk_timeout,
                            retries=self.sdk_retries)
                if task:
                    if self.atomicity:
                        self.task.jython_task_manager.get_task_result(task)
                    else:
                        tasks_info[task] = dict()
                        tasks_info[task] = \
                            self.bucket_util.get_doc_op_info_dict(
                                bucket, op_type=op_type, exp=0,
                                replicate_to=self.replicate_to,
                                persist_to=self.persist_to,
                                durability=self.durability_level,
                                timeout=self.sdk_timeout,
                                retry_exceptions=retry_exceptions)

            # Wait for rebalance+doc_loading tasks to complete
            self.task_manager.get_task_result(rebalance_task)
            # Validate rebalance result
            if not rebalance_task.result:
                self.task_manager.abort_all_tasks()
                for task in tasks_info:
                    try:
                        for client in task.clients:
                            client.close()
                    except:
                        pass
                self.fail("Rebalance Failed")
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            # Validate + retry doc_ops outcomes
            if not self.atomicity:
                self.bucket_util.verify_doc_op_task_exceptions(
                    tasks_info, self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks_info)
                for task, task_info in tasks_info.items():
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc ops failed for task: %s" % task.thread_name)

            self.cluster.nodes_in_cluster.extend(self.cluster.servers[i:i + 2])
            self.sleep(60, "Wait for cluster to be ready after rebalance")
        if not self.atomicity:
            self.bucket_util.verify_unacked_bytes_all_buckets()
            self.bucket_util.verify_cluster_stats(num_of_items)

    def rebalance_in_with_queries(self):
        """
        Rebalances nodes into a cluster  during view queries.

        This test begins by loading a given number of items into the cluster.
        It creates num_views as development/production views with default
        map view funcs(is_dev_ddoc = True by default).
        It then adds nodes_in nodes at a time and rebalances that node into
        the cluster.
        During the rebalancing we perform view queries for all views and verify
        the expected number of docs for them. Perform the same view queries
        after cluster has been completed. Then we wait for the disk queues to
        drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        Once successful view queries the test is finished.

        Added reproducer for MB-6683
        """
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()

        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        reproducer = self.input.param("reproducer", False)
        num_tries = self.input.param("num_tries", 10)
        iterations_to_try = (1, num_tries)[reproducer]
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = dict()
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"

        views = list()
        tasks = list()

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write abort scenarios for all vbs")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name,
                    "replica")
                load_gen = doc_generator(self.key, 0, 5000,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        for bucket in self.bucket_util.buckets:
            temp = self.bucket_util.make_default_views(self.default_view,
                                                       self.default_view_name,
                                                       num_views, is_dev_ddoc,
                                                       different_map=reproducer)
            temp_tasks = self.bucket_util.async_create_views(
                self.cluster.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks

        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 4, len(self.bucket_util.buckets) * self.wait_timeout * self.num_items / 50000)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        for bucket in self.bucket_util.buckets:
            for view in views:
                # run queries to create indexes
                self.bucket_util.query_view(
                    self.cluster.master, prefix + ddoc_name, view.name, query,
                    bucket=bucket.name)

        active_tasks = self.cluster_util.async_monitor_active_task(
            self.cluster.servers[:self.nodes_init], "indexer",
            "_design/" + prefix + ddoc_name, wait_task=False)
        for active_task in active_tasks:
            self.task.jython_task_manager.get_task_result(active_task)
            self.assertTrue(active_task.result)

        expected_rows = self.num_items
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        for bucket in self.bucket_util.buckets:
            result = self.bucket_util.perform_verify_queries(
                num_views, prefix, ddoc_name, self.default_view_name,
                query, bucket=bucket, wait_time=timeout,
                expected_rows=expected_rows)
            self.assertTrue(result, "Failure in view query")

        for i in xrange(iterations_to_try):
            servs_in = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
            rebalance = self.task.async_rebalance([self.cluster.master],
                                                  servs_in, [])
            self.sleep(self.wait_timeout / 5)

            # See that the result of view queries are same as
            # the expected during the test
            for bucket in self.bucket_util.buckets:
                result = self.bucket_util.perform_verify_queries(
                    num_views, prefix, ddoc_name, self.default_view_name,
                    query, bucket=bucket, wait_time=timeout,
                    expected_rows=expected_rows)
                self.assertTrue(result, "Failure in view query")

            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance Failed")
            self.cluster.nodes_in_cluster.extend(servs_in)
            self.sleep(60)
            # verify view queries results after rebalancing
            for bucket in self.bucket_util.buckets:
                result = self.bucket_util.perform_verify_queries(
                    num_views, prefix, ddoc_name, self.default_view_name,
                    query, bucket=bucket, wait_time=timeout,
                    expected_rows=expected_rows)
                self.assertTrue(result, "Failure in view query")

            if not self.atomicity:
                self.bucket_util.verify_cluster_stats(self.num_items)

            if reproducer:
                rebalance = self.task.async_rebalance(self.cluster.servers, [], servs_in)
                self.task.jython_task_manager.get_task_result(rebalance)
                self.assertTrue(rebalance.result, "Rebalance Failed")
                self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_in))
                self.sleep(self.wait_timeout)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def incremental_rebalance_in_with_queries(self):
        """
        Rebalances nodes into a cluster incremental during view queries.

        This test begins by loading a given number of items into the cluster.
        It creates num_views as development/production view with default
        map view funcs(is_dev_ddoc = True by default).
        Then adds two nodes at a time & rebalances that node into the cluster.
        During the rebalancing we perform view queries for all views and verify
        the expected number of docs for them. Perform the same view queries
        after cluster has been completed. Then we wait for the disk queues to
        drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """

        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        views = self.bucket_util.make_default_views(self.default_view,
                                                    self.default_view_name,
                                                    num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        # increase timeout for big data
        timeout = max(self.wait_timeout * 4,
                      self.wait_timeout * self.num_items / 25000)
        query = dict()
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"

        tasks = self.bucket_util.async_create_views(
            self.cluster.master, ddoc_name, views, 'default')
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        for view in views:
            # run queries to create indexes
            self.bucket_util.query_view(
                self.cluster.master, prefix + ddoc_name, view.name, query)

        active_tasks = self.cluster_util.async_monitor_active_task(
            self.cluster.master, "indexer", "_design/" + prefix + ddoc_name,
            wait_task=False)
        for active_task in active_tasks:
            self.task.jython_task_manager.get_task_result(active_task)
            self.assertTrue(active_task.result)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        result = self.bucket_util.perform_verify_queries(
            num_views, prefix, ddoc_name, self.default_view_name, query,
            wait_time=timeout, expected_rows=expected_rows)
        self.assertTrue(result, "Failure in view query")

        query["stale"] = "update_after"
        for i in range(self.nodes_init, self.num_servers, 2):
            rebalance = self.task.async_rebalance(
                self.cluster.servers[:i], self.cluster.servers[i:i + 2], [])
            self.sleep(self.wait_timeout / 5)
            # Verify the result of view queries are same as expected during the test
            result = self.bucket_util.perform_verify_queries(
                num_views, prefix, ddoc_name, self.default_view_name, query,
                wait_time=timeout, expected_rows=expected_rows)
            self.assertTrue(result, "Failure in view query")

            # Verify view queries results after rebalancing
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance Failed")
            self.cluster.nodes_in_cluster.extend(self.cluster.servers[i:i + 2])
            self.sleep(60)
            result = self.bucket_util.perform_verify_queries(
                num_views, prefix, ddoc_name, self.default_view_name, query,
                wait_time=timeout, expected_rows=expected_rows)
            self.assertTrue(result, "Failure in view query")
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def rebalance_in_with_warming_up(self):
        """
        Rebalances nodes into a cluster when one node is warming up.

        This test begins by loading a given number of items into the node.
        Then it creates cluster with self.nodes_init nodes. Next steps are:
        stop the latest node in servs_init list(if list size==1, master node/
        cluster will be stopped), wait 20 sec and start the stopped node.
        Without waiting for the node to start up completely, rebalance in
        servs_in servers. Expect that rebalance is failed. Wait for warmup
        completed and strart rebalance with the same configuration.
        Once the cluster has been rebalanced we wait for the disk queues
        to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        """

        servs_in = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_init = self.cluster.servers[:self.nodes_init]
        warmup_node = servs_init[-1]

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write abort scenario for replica vbs")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name, "replica")
                load_gen = doc_generator(self.key, 0, 5000,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        rebalance = self.task.async_rebalance(servs_init, servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.cluster.nodes_in_cluster.extend(servs_in)
        if not rebalance.result:
            self.log.info("rebalance was failed as expected")
            for bucket in self.bucket_util.buckets:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [warmup_node], bucket,
                    wait_time=self.wait_timeout * 10))

            self.log.info("second attempt to rebalance")
            rebalance = self.task.async_rebalance(servs_init + servs_in, [], [])
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance Failed")
        self.sleep(60)
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(self.num_items)
            self.bucket_util.verify_unacked_bytes_all_buckets()

    def rebalance_in_with_ddoc_compaction(self):
        """
        Rebalances nodes into a cluster during ddoc compaction.

        This test begins by loading a given number of items into the cluster.
        It creates num_views as development/production view with default
        map view funcs(is_dev_ddoc = True by default). Then we disabled
        compaction for ddoc. While we don't reach expected fragmentation for
        ddoc we update docs and perform view queries. We rebalance in  nodes_in
        nodes and start compation when fragmentation was reached
        fragmentation_value. During the rebalancing we wait while compaction
        will be completed. After rebalancing and compaction we wait for the
        disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        """

        num_views = self.input.param("num_views", 5)
        fragmentation_value = self.input.param("fragmentation_value", 80)
        # now dev_ indexes are not auto-updated, doesn't work with dev view
        is_dev_ddoc = False
        views = self.bucket_util.make_default_views(self.default_view,
                                                    self.default_view_name,
                                                    num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = dict()
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows

        tasks = self.bucket_util.async_create_views(
            self.cluster.master, ddoc_name, views, 'default')
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.disable_compaction()
        fragmentation_monitor = self.task.async_monitor_view_fragmentation(
            self.cluster.master, prefix + ddoc_name, fragmentation_value,
            'default')

        end_time = time.time() + self.wait_timeout * 5
        # generate load until fragmentation reached
        while not fragmentation_monitor.result and end_time > time.time():
            # update docs to create fragmentation
            self._load_all_buckets(self.cluster, self.gen_update, "update", 0)
            for view in views:
                # run queries to create indexes
                result = self.bucket_util.query_view(self.cluster.master, prefix + ddoc_name, view.name, query)
                self.assertTrue(result, "Failure in view query")
        self.task.jython_task_manager.get_task_result(fragmentation_monitor)
        self.assertTrue(fragmentation_monitor.result,
                        "unable to reach compaction value {0} after {1} sec"
                        .format(fragmentation_value, time.time()-end_time))

        for _ in xrange(3):
            active_tasks = self.cluster_util.async_monitor_active_task(
                self.cluster.master, "indexer",
                "_design/" + prefix + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                self.task.jython_task_manager.get_task_result(active_task)
                self.assertTrue(active_task.result)
            self.sleep(2)

        query["stale"] = "false"

        result = self.bucket_util.perform_verify_queries(
            num_views, prefix, ddoc_name, self.default_view_name, query,
            wait_time=self.wait_timeout*3, expected_rows=expected_rows)
        self.assertTrue(result, "Failure in view query")

        compaction_task = self.task.async_compact_view(
            self.cluster.master, prefix + ddoc_name, 'default',
            with_rebalance=True)
        servs_in = self.cluster.servers[1:self.nodes_in + 1]
        rebalance = self.task.async_rebalance([self.cluster.master], servs_in, [])

        self.task.jython_task_manager.get_task_result(compaction_task)
        self.assertTrue(compaction_task.result, "Compaction did not happened")

        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")

        self.cluster.nodes_in_cluster.extend(servs_in)

        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def incremental_rebalance_in_with_mutation_and_deletion(self):
        """
        Rebalances nodes into a cluster while doing mutations and deletions.

        This test begins by loading a given number of items into the cluster.
        It then adds one node at a time & rebalances the node into the cluster.
        During the rebalance we update half of the items in the cluster and
        delete the other half. Once the cluster has been rebalanced we recreate
        the deleted items, wait for the disk queues to drain, and then verify
        that there has been no data loss.
        sum(curr_items) match the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """

        self.gen_delete = self.get_doc_generator(self.num_items / 2,
                                                 self.num_items)

        for i in range(self.num_servers)[self.nodes_init:]:
            rebalance = self.task.async_rebalance(self.cluster.servers[:i],
                                                  [self.cluster.servers[i]],
                                                  [])
            if self.atomicity:
                self._load_all_buckets_atomicty(self.gen_update, "rebalance_only_update")
                self.sleep(20)
                self._load_all_buckets_atomicty(self.gen_delete, "rebalance_delete")
                self.sleep(20)
            else:
                self._load_all_buckets(self.cluster, self.gen_update,
                                       "update", 0)
                self._load_all_buckets(self.cluster, self.gen_delete,
                                       "delete", 0)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance Failed")
            self.cluster.nodes_in_cluster.extend([self.cluster.servers[i]])
            self.sleep(20)
            if self.atomicity:
                self._load_all_buckets_atomicty(self.gen_delete, "create")
                self.sleep(20)
            else:
                self._load_all_buckets(self.cluster, self.gen_delete,
                                   "create", 0)
                self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def incremental_rebalance_in_with_mutation_and_expiration(self):
        """
        Rebalances nodes into a cluster while doing mutations and expirations.

        This test begins by loading a given number of items into the cluster.
        It then adds one node at a time & rebalances the node into the cluster.
        During the rebalance we update all items in the cluster. Half of the
        items updated are also given an expiration time of 5 seconds.
        Once the cluster has been rebalanced we recreate the expired items,
        wait for the disk queues to drain, and then verify that there has been
        no data loss, sum(curr_items) match the curr_items_total.
        Once all nodes have been rebalanced in the test is finished.
        """

        gen_2 = self.get_doc_generator(self.num_items / 2,
                                       self.num_items)
        for i in range(self.num_servers)[1:]:
            rebalance = self.task.async_rebalance(self.cluster.servers[:i],
                                                  [self.cluster.servers[i]], [])
            self._load_all_buckets(self.cluster, self.gen_update, "update", 0)
            self._load_all_buckets(self.cluster, gen_2, "update", 5)
            self.sleep(5)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance Failed")
            self._load_all_buckets(self.cluster, gen_2, "create", 0)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_rebalance_in_with_cluster_ramquota_change(self):
        '''
        test changes ram quota during rebalance.
        http://www.couchbase.com/issues/browse/CBQE-1649
        '''
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init],
            self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in],
            [])
        self.sleep(10, "Wait for rebalance have some progress")
        remote = RemoteMachineShellConnection(self.cluster.master)
        cli_command = "setting-cluster"
        options = "--cluster-ramsize=%s" % (3000)
        output, error = remote.execute_couchbase_cli(
            cli_command=cli_command, options=options, cluster_host="localhost",
            user=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password)
        self.assertTrue('\n'.join(output).find('SUCCESS') != -1,
                        'RAM wasn\'t chnged')
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
