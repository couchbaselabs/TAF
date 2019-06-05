from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from rebalance_new.rebalance_base import RebalanceBaseTest


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
        #self.bucket_util.vb_distribution_analysis(servers=nodes, std=1.0, total_vbuckets=self.vbuckets)

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
            self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
            self.bucket_util._wait_for_stats_all_buckets()
        self.sleep(20)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        self.nodes = self.cluster.nodes_in_cluster
        result_nodes = list(set(self.cluster.servers[:self.nodes_init] + servs_in) - set(servs_out))
        for node in servs_in:
            self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password, node.ip, node.port)
        chosen = RebalanceHelper.pick_nodes(self.cluster.master, howmany=1)
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
            self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
            self.bucket_util._wait_for_stats_all_buckets()
        self.sleep(20)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.cluster.servers[:self.nodes_init], self.bucket_util.buckets, path=None)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        chosen = RebalanceHelper.pick_nodes(self.cluster.master, howmany=1)
        result_nodes = list(set(self.cluster.servers[:self.nodes_init] + servs_in) - set(servs_out))
        result_nodes = [node for node in result_nodes if node.ip != chosen[0].ip]
        for node in servs_in:
            self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password, node.ip, node.port)
        # Mark Node for failover
        self.rest.fail_over(chosen[0].id, graceful=fail_over)
        self.shuffle_nodes_between_zones_and_rebalance(servs_out)
        self.cluster.nodes_in_cluster = result_nodes
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
        self.bucket_util.compare_failovers_logs(prev_failover_stats, result_nodes, self.bucket_util.buckets)
        self.sleep(30)
        self.bucket_util.data_analysis_active_replica_all(disk_active_dataset, disk_replica_dataset, result_nodes, self.bucket_util.buckets,
                                              path=None)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster.nodes_in_cluster
        #self.bucket_util.vb_distribution_analysis(servers=nodes, std=1.0, total_vbuckets=self.vbuckets)

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
        gen = self.get_doc_generator(0, self.num_items)
        batch_size = 50
        for i in reversed(range(self.num_servers)[self.num_servers / 2:]):
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen, "update", 0,
                batch_size=batch_size, timeout_secs=60)
            self.add_remove_servers_and_rebalance([], self.cluster.servers[i:self.num_servers])
            self.sleep(10)
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen, "update", 0,
                batch_size=batch_size, timeout_secs=60)
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
        init_num_nodes = self.input.param("init_num_nodes", 1)
        gen = self.get_doc_generator(0, self.num_items)
        for i in range(self.num_servers):
            tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen, "update", 0, batch_size=10, timeout_secs=60)
            self.add_remove_servers_and_rebalance(self.cluster.servers[init_num_nodes:init_num_nodes + i + 1], [])
            self.sleep(10)
            self.add_remove_servers_and_rebalance([], self.cluster.servers[init_num_nodes:init_num_nodes + i + 1])
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
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

            self._load_all_buckets(gen_delete, "create", 0)
            self.bucket_util.verify_cluster_stats(self.num_items)


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
