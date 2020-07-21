from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from rebalance_new.rebalance_base import RebalanceBaseTest
from BucketLib.BucketOperations import BucketHelper
from rebalance_new import rebalance_base


class RebalanceInOutTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceInOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceInOutTests, self).tearDown()

    def test_rebalance_in_out_after_mutation(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations.
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        # Shuffle the nodes if zone > 1 is specified.
        if self.zone > 1:
            self.shuffle_nodes_between_zones_and_rebalance()
        gen = self.get_doc_generator(0, self.num_items)
        if self.atomicity:
            self._load_all_buckets_atomicty(gen, "rebalance_only_update")
        else:
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen, "update", 0)
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        servs_in = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_out = self.cluster.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        result_nodes = list(set(self.cluster.servers[:self.nodes_init] + servs_in) - set(servs_out))
        if not self.atomicity:
            self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
            self.bucket_util._wait_for_stats_all_buckets()
            self.sleep(20)
            prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(
                self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
            disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
                self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
            self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.add_remove_servers_and_rebalance(servs_in, servs_out)
        self.sleep(30)
        if not self.atomicity:
            self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
            self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
            new_failover_stats = self.bucket_util.compare_failovers_logs(prev_failover_stats, result_nodes, self.bucket_util.buckets)
            new_vbucket_stats = self.bucket_util.compare_vbucket_seqnos(prev_vbucket_stats, result_nodes, self.bucket_util.buckets,
                                                            perNode=False)
            self.bucket_util.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
            self.sleep(30)
            self.bucket_util.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, result_nodes, self.bucket_util.buckets,
                                                  path=None)
            self.bucket_util.verify_unacked_bytes_all_buckets()
            nodes = self.cluster.nodes_in_cluster
        #self.bucket_util.vb_distribution_analysis(servers=nodes, std=1.0, total_vbuckets=self.cluster_util.vbuckets)

    def test_rebalance_in_out_with_failover_addback_recovery(self):
        """
        Rebalances nodes out and in with failover and full/delta recovery add back of a node
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        recovery_type = self.input.param("recoveryType", "full")
        gen = self.get_doc_generator(0, self.num_items)

        if self.atomicity:
            self._load_all_buckets_atomicty(gen, "rebalance_only_update")
        else:
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen, "update", 0)

        servs_in = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_out = self.cluster.servers[self.nodes_init - self.nodes_out:self.nodes_init]

        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            self.bucket_util.verify_stats_all_buckets(self.num_items,
                                                      timeout=120)
            self.bucket_util._wait_for_stats_all_buckets()

        # Update replica value before performing rebalance in/out as given in conf file
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0],
                replicaNumber=self.replica_to_update)

#             self.bucket_util.buckets[0].replicaNumber = self.replica_to_update

        self.sleep(20)

        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)

        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        self.nodes = self.cluster.nodes_in_cluster

        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

        for node in servs_in:
            self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password, node.ip, node.port)

        # Mark Node for failover
        self.sleep(30)
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)

        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType=recovery_type)
        self.sleep(30)
        try:
            self.shuffle_nodes_between_zones_and_rebalance(servs_out)
        except Exception, e:
            if "deltaRecoveryNotPossible" not in e.__str__():
                self.fail("Rebalance did not fail. Rebalance has to fail since no delta recovery should be possible"
                          " while adding nodes too")

    def test_rebalance_in_out_with_failover(self):
        """
        Rebalances nodes out and in with failover
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        fail_over = self.input.param("fail_over", False)
        gen = self.get_doc_generator(0, self.num_items)

        if self.atomicity:
            self._load_all_buckets_atomicty(gen, "rebalance_only_update")
        else:
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen, "update", 0)
        servs_in = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servs_out = self.cluster.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
            self.bucket_util._wait_for_stats_all_buckets()
        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0], replicaNumber=self.replica_to_update)
#             self.bucket_util.buckets[0].replicaNumber = self.replica_to_update
        self.sleep(20)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        result_nodes = list(set(self.cluster.servers[:self.nodes_init] + servs_in) - set(servs_out))
        result_nodes = [node for node in result_nodes if node.ip != chosen[0].ip]
        for node in servs_in:
            self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password, node.ip, node.port)
        # Mark Node for failover
        self.rest.fail_over(chosen[0].id, graceful=fail_over)
        self.shuffle_nodes_between_zones_and_rebalance(servs_out)
        self.cluster.nodes_in_cluster = result_nodes
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(
                self.num_items,
                check_ep_items_remaining=True)
        self.bucket_util.compare_failovers_logs(prev_failover_stats,
                                                result_nodes,
                                                self.bucket_util.buckets)
        self.sleep(30)
        self.bucket_util.data_analysis_active_replica_all(
            disk_active_dataset, disk_replica_dataset, result_nodes,
            self.bucket_util.buckets, path=None)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster.nodes_in_cluster
        # self.bucket_util.vb_distribution_analysis(servers=nodes,
        # std=1.0, total_vbuckets=self.cluster_util.vbuckets)

    def test_incremental_rebalance_in_out_with_mutation(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations.
        Use 'zone' param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(self.cluster.servers[self.nodes_init:self.num_servers], [])
        self.doc_ops = "update"
        self.gen_update = self.get_doc_generator(0, self.num_items)

        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            # CRUDs while rebalance is running in parallel
            tasks_info = self.loadgen_docs(retry_exceptions=rebalance_base.retry_exceptions)
            self.add_remove_servers_and_rebalance([], self.cluster.servers[i:self.num_servers])
            self.sleep(10)
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            tasks_info = self.loadgen_docs(retry_exceptions=rebalance_base.retry_exceptions)

            self.add_remove_servers_and_rebalance(self.cluster.servers[i:self.num_servers], [])
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_in_out_with_mutation_and_compaction(self):
        """
        Rebalances nodes out and in of the cluster while doing mutations and compaction.
        Use 'zone' param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        removes one node, rebalances that node out the cluster, and then rebalances it back
        in. During the rebalancing we update all of the items in the cluster. Once the
        node has been removed and added back we  wait for the disk queues to drain, and
        then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
        We then remove and add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(self.cluster.servers[self.nodes_init:self.num_servers], [])
        gen = self.get_doc_generator(0, self.num_items)
        batch_size = 50
        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            if self.atomicity:
                self._load_all_buckets_atomicty(gen, "rebalance_only_update")
            else:
                tasks_info = self.bucket_util._async_load_all_buckets(
                    self.cluster, gen, "update", 0,
                    batch_size=batch_size, timeout_secs=60)
            compact_tasks = []
            for bucket in self.bucket_util.buckets:
                compact_tasks.append(self.task.async_compact_bucket(self.cluster.master, bucket))
            self.add_remove_servers_and_rebalance([], self.cluster.servers[i:self.num_servers])
            self.sleep(10)
            if not self.atomicity:
                self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                               self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks_info)
                for task, task_info in tasks_info.items():
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc ops failed for task: {}".format(task.thread_name))
            for task in compact_tasks:
                self.task.jython_task_manager.get_task_result(task)

            if self.atomicity:
                self._load_all_buckets_atomicty(gen, "rebalance_only_update")
            else:
                tasks_info = self.bucket_util._async_load_all_buckets(
                    self.cluster, gen, "update", 0,
                    batch_size=batch_size, timeout_secs=60)
            self.add_remove_servers_and_rebalance(self.cluster.servers[i:self.num_servers], [])
            if not self.atomicity:
                self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                               self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks_info)
                self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_out_in_with_mutation(self):
        """
        Rebalances nodes in and out of the cluster while doing mutations.
        Use 'zone' param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a initial number of nodes into the cluster.
        It then adds one node, rebalances that node into the cluster,
        and then rebalances it back out. During the rebalancing we update all  of
        the items in the cluster. Once the nodes have been removed and added back we
        wait for the disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.
        We then add and remove back two nodes at a time and so on until we have reached
        the point where we are adding back and removing at least half of the nodes.
        """
        self.doc_ops = "update"
        self.gen_update = self.get_doc_generator(0, self.num_items)

        for i in range(self.nodes_init, self.num_servers):
            tasks_info = self.loadgen_docs(retry_exceptions=rebalance_base.retry_exceptions)
            self.add_remove_servers_and_rebalance(self.cluster.servers[self.nodes_init:i], [])
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

            self.sleep(10)

            tasks_info = self.loadgen_docs(retry_exceptions=rebalance_base.retry_exceptions)
            self.add_remove_servers_and_rebalance([], self.cluster.servers[self.nodes_init:i])
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_in_out_with_mutation_and_deletion(self):
        """
        Rebalances nodes into and out of the cluster while doing mutations and
        deletions.
        Use 'zone' param to have nodes divided into server groups by having zone > 1.

        This test begins by loading a given number of items into the cluster. It then
        adds one node, rebalances that node into the cluster, and then rebalances it back
        out. During the rebalancing we update half of the items in the cluster and delete
        the other half. Once the node has been removed and added back we recreate the
        deleted items, wait for the disk queues to drain, and then verify that there has
        been no data loss, sum(curr_items) match the curr_items_total. We then remove and
        add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(self.cluster.servers[self.nodes_init:self.num_servers], [])
        gen_delete = self.get_doc_generator(self.num_items / 2 + 2000, self.num_items)
        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                pause_secs=5, batch_size=1, timeout_secs=60)
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen_delete, "delete", 0,
                pause_secs=5, batch_size=1, timeout_secs=60)
            tasks_info.update(tem_tasks_info.copy())

            self.add_remove_servers_and_rebalance([], self.cluster.servers[i:self.num_servers])
            self.sleep(60)
            self.add_remove_servers_and_rebalance(self.cluster.servers[i:self.num_servers], [])
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            self._load_all_buckets(self.cluster, gen_delete, "create", 0)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_incremental_rebalance_in_out_with_mutation_and_expiration(self):
        """
        Rebalances nodes into and out of the cluster while doing mutations and
        expirations. Use 'zone' param to have nodes divided into server groups
        by having zone > 1.

        This test begins by loading a given number of items into the cluster.
        It then adds one node, rebalances that node into the cluster, and then
        rebalances it back out. During the rebalancing we update half of the
        items in the cluster and expire the other half. Once the node has been
        removed and added back we recreate the expired items, wait for the
        disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total.We then remove and
        add back two nodes at a time and so on until we have reached the point
        where we are adding back and removing at least half of the nodes.
        """
        self.add_remove_servers_and_rebalance(self.cluster.servers[self.nodes_init:self.num_servers], [])
        gen_delete = self.get_doc_generator(self.num_items / 2 + 2000, self.num_items)
        self.bucket_util._expiry_pager(5)
        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                durability=self.durability_level,
                batch_size=10, timeout_secs=60,
                process_concurrency=4,
                retry_exceptions=rebalance_base.retry_exceptions)
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen_delete, "update", 10,
                durability=self.durability_level,
                batch_size=10, timeout_secs=60,
                process_concurrency=4,
                retry_exceptions=rebalance_base.retry_exceptions)
            tasks_info.update(tem_tasks_info.copy())

            self.add_remove_servers_and_rebalance([], self.cluster.servers[i:self.num_servers])
            self.sleep(10)
            self.add_remove_servers_and_rebalance(self.cluster.servers[i:self.num_servers], [])
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            self.sleep(12, "wait till the expiry time 10s")
            self.bucket_util.verify_cluster_stats(self.num_items - (gen_delete.end - gen_delete.start))
            self._load_all_buckets(self.cluster, gen_delete, "create", 0)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_rebalance_in_out_at_once(self):
        """
        PERFORMANCE:Rebalance in/out at once.
        Use different nodes_in and nodes_out params to have uneven add and deletion. Use 'zone'
        param to have nodes divided into server groups by having zone > 1.


        Then it creates cluster with self.nodes_init nodes. Further
        test loads a given number of items into the cluster. It then
        add  servs_in nodes and remove  servs_out nodes and start rebalance.
        Once cluster was rebalanced the test is finished.
        Available parameters by default are:
        nodes_init=1, nodes_in=1, nodes_out=1
        """
        servs_init = self.cluster.servers[:self.nodes_init]
        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        servs_out = [self.cluster.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0], replicaNumber=self.replica_to_update)
#             self.bucket_util.buckets[0].replicaNumber = self.replica_to_update
        rest = RestConnection(self.cluster.master)
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
        self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("adding nodes {0} to cluster".format(servs_in))
        self.log.info("removing nodes {0} from cluster".format(servs_out))
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.add_remove_servers_and_rebalance(servs_in, servs_out)
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(self.num_items)
            self.bucket_util.verify_unacked_bytes_all_buckets()


class RebalanceInOutDurabilityTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceInOutDurabilityTests, self).setUp()
        self.do_stop_start = self.input.param("stop_start", False)
        self.swap_orchestrator = self.input.param("swap-orchestrator", False)

    def tearDown(self):
        super(RebalanceInOutDurabilityTests, self).tearDown()

    def test_rebalance_inout_with_durability_check(self):
        """
        Perform irregular number of in_out nodes
        1. Swap-out 'self.nodes_out' nodes
        2. Add 'self.nodes_in' nodes into the cluster
        3. Perform swap-rebalance
        4. Make sure durability is not broken due to swap-rebalance

        Note: This is a Positive case. i.e: Durability should not be broken
        """
        master = self.cluster.master
        def_bucket = self.bucket_util.buckets[0]
        items = self.items
        create_from = items

        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                def_bucket, replicaNumber=self.replica_to_update)
            self.bucket_util.buckets[0].replicaNumber = self.replica_to_update

        # Rest connection to add/rebalance/monitor nodes
        rest = RestConnection(master)

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))

        toBeEjectedNodes = [self.cluster.servers[self.nodes_init - i - 1]
                            for i in range(self.nodes_out)]

        if self.swap_orchestrator:
            status, content = self.cluster_util.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            if self.nodes_out is len(current_nodes):
                toBeEjectedNodes.append(self.cluster.master)
            else:
                toBeEjectedNodes[0] = self.cluster.master

        for node in toBeEjectedNodes:
            self.log.info("removing node {0} and rebalance afterwards"
                          .format(node))

        servs_in = self.servers[self.nodes_init:self.nodes_init+self.nodes_in]

        if self.swap_orchestrator:
            rest = RestConnection(servs_in[0])
            master = servs_in[0]

        self.log.info("IN/OUT REBALANCE PHASE")
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, toBeEjectedNodes)

        # CRUDs while rebalance is running in parallel
        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + items)
        self.sleep(10, "wait for rebalance to start")
        tasks_info = self.loadgen_docs()
        if self.do_stop_start:
            # Rebalance is stopped at 20%, 40% and 60% completion
            retry = 0
            for expected_progress in (20, 40, 60):
                self.log.info("STOP/START SWAP REBALANCE PHASE WITH PROGRESS {0}%"
                              .format(expected_progress))
                while True:
                    progress = rest._rebalance_progress()
                    if progress < 0:
                        self.log.error("rebalance progress code : {0}"
                                       .format(progress))
                        break
                    elif progress == 100:
                        self.log.warn("Rebalance has already reached 100%")
                        break
                    elif progress >= expected_progress:
                        self.log.info("Rebalance will be stopped with {0}%"
                                      .format(progress))
                        stopped = rest.stop_rebalance()
                        self.assertTrue(stopped, msg="unable to stop rebalance")
                        self.sleep(20)
                        rebalance_task = self.task.async_rebalance(
                            self.cluster.servers[:self.nodes_init] + servs_in, [], toBeEjectedNodes)
#                         rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
#                                        ejectedNodes=optNodesIds)
                        break
                    elif retry > 100:
                        break
                    else:
                        retry += 1
                        self.sleep(1)

        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            for task, _ in tasks_info.items():
                self.task_manager.get_task_result(task)
            self.fail("Rebalance Failed")

        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(toBeEjectedNodes))

        self.bucket_util.verify_doc_op_task_exceptions(tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        for task, task_info in tasks_info.items():
            self.assertFalse(
                task_info["ops_failed"],
                "Doc ops failed for task: {}".format(task.thread_name))
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_rebalance_inout_with_durability_failure(self):
        """
        Perform irregular number of in_out nodes
        1. Swap-out 'self.nodes_out' nodes
        2. Add nodes using 'self.nodes_in' such that,
           replica_number > nodes_in_cluster
        3. Perform swap-rebalance
        4. Make sure durability is not broken due to swap-rebalance
        5. Add make a node and do CRUD on the bucket
        6. Verify durability works after node addition

        Note: This is a Negative case. i.e: Durability will be broken
        """
        if not self.durability_level:
            self.log.info("The test is not valid for Durability=None")
            return
        master = self.cluster.master
        creds = self.input.membase_settings
        def_bucket = self.bucket_util.buckets[0]
        items = self.num_items
        create_from = items
        # TODO: Enable verification
        """
        vbucket_info_dict = dict()

        # Cb stat object for verification purpose
        master_shell_conn = RemoteMachineShellConnection(master)
        master_node_cb_stat = Cbstats(master_shell_conn)

        # Fetch vb_seq_no after the CRUDs
        vb_info["afterCrud"] = \
            master_node_cb_stat.vbucket_seqno(def_bucket.name)
        """

        # Rest connection to add/rebalance/monitor nodes
        rest = RestConnection(master)

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = [self.cluster.servers[self.nodes_init - i - 1]
                            for i in range(self.nodes_out)]

        if self.nodes_out is len(current_nodes):
            self.swap_orchestrator = True
        elif self.swap_orchestrator:
            try:
                status, content = self.cluster_util.find_orchestrator(master)
                self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                                .format(status, content))
            except:
                pass
            toBeEjectedNodes[0] = self.cluster.master

        for node in toBeEjectedNodes:
            self.log.info("removing node {0} and rebalance afterwards"
                          .format(node))

        servs_in = self.servers[self.nodes_init:self.nodes_init+self.nodes_in]

        for bucket in self.bucket_util.buckets:
            durability_req = (bucket.replicaNumber + 1)/2 + 1
            self.assertTrue(durability_req >= len(current_nodes) - len(toBeEjectedNodes) + len(servs_in),
                            "bucket replica is less than the available nodes in the cluster")

        self.log.info("IN/OUT REBALANCE PHASE")
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, toBeEjectedNodes)
        self.sleep(10, "wait for rebalance to start")
        self.task.jython_task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance Failed")

        self.cluster.nodes_in_cluster = list(set(self.cluster.servers[:self.nodes_init] + servs_in) - set(toBeEjectedNodes))

        if self.swap_orchestrator:
            rest = RestConnection(self.cluster.nodes_in_cluster[0])
            self.cluster.master = self.cluster.nodes_in_cluster[0]

        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(toBeEjectedNodes))

        # CRUDs while durability is broken
        ignore_exceptions = ["com.couchbase.client.core.error.DurabilityImpossibleException"]
        self.check_temporary_failure_exception = False
        self.gen_create = self.get_doc_generator(create_from,
                                                 create_from + 1000)
        tasks_info = self.loadgen_docs(ignore_exceptions=ignore_exceptions)
        self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        for task, task_info in tasks_info.items():
            self.assertFalse(
                task_info["ops_failed"],
                "Doc ops failed for task: {}".format(task.thread_name))
        # Add back first ejected node back into the cluster
        self.task.rebalance(self.cluster.nodes_in_cluster,
                            toBeEjectedNodes, [])
        self.sleep(10, "wait for rebalance to start")
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(toBeEjectedNodes))
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items-1000)

        for vb_num in range(0, self.cluster_util.vbuckets, 128):
            self.target_vbucket = [vb_num]
            self.log.info("Targeting vBucket: {}".format(vb_num))
            self.gen_create = self.get_doc_generator(self.num_items,
                                                     100)
            self.check_temporary_failure_exception = False
            tasks_info = self.loadgen_docs(ignore_exceptions=ignore_exceptions,
                                           task_verification=True)
            for task, task_info in tasks_info.items():
                self.assertTrue(len(task_info["ignored"]) == 0,
                                "Still seeing DurabilityImpossibleException")
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
