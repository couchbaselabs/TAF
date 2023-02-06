import Jython_tasks.task as jython_tasks
from BucketLib.bucket import Bucket
from Cb_constants import DocLoading
from cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection
from rebalance_base import RebalanceBaseTest
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection


class RebalanceOutTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceOutTests, self).tearDown()

    def test_rebalance_out_with_ops_durable(self):
        self.gen_create = doc_generator(self.key, self.num_items,
                                        self.num_items + self.items)
        self.gen_delete = doc_generator(self.key, self.items / 2,
                                        self.items)
        servs_out = [self.cluster.servers[len(self.cluster.nodes_in_cluster) - i - 1] for i in range(self.nodes_out)]
        rebalance_task = self.task.async_rebalance(self.cluster, [], servs_out)
        self.sleep(10)

        tasks_info = self.bucket_util._async_load_all_buckets(
            self.cluster, self.gen_create, "create", 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            durability=self.durability_level,
            ryow=self.ryow, check_persistence=self.check_persistence,
            sdk_client_pool=self.sdk_client_pool)

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

        self.sleep(60, "Wait for cluster to be ready after rebalance")
        tasks = list()
        for bucket in self.cluster.buckets:
            if self.doc_ops is not None:
                if "update" in self.doc_ops:
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency))
                if "create" in self.doc_ops:
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_create, "create", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency))
                if "delete" in self.doc_ops:
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_delete, "delete", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)

    def rebalance_out_with_ops(self):
        self.gen_create = doc_generator(self.key, self.num_items,
                                        self.num_items + self.items)
        self.gen_delete = doc_generator(self.key, self.items / 2,
                                        self.items)
        servs_out = [self.cluster.servers[self.nodes_init - i - 1]
                     for i in range(self.nodes_out)]
        tasks = list()
        rebalance_task = self.task.async_rebalance(self.cluster, [], servs_out)
        tasks_info = self.loadgen_docs()
        self.sleep(15, "Wait for rebalance to start")

        # Wait for rebalance + doc_loading tasks to complete
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.task_manager.abort_all_tasks()
            for task in tasks_info:
                try:
                    for client in task.clients:
                        client.close()
                except Exception:
                    pass
            self.fail("Rebalance Failed")

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: %s" % task.thread_name)

        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        self.sleep(20)
        if not self.atomicity:
            for bucket in self.cluster.buckets:
                if self.doc_ops is not None:
                    if "update" in self.doc_ops:
                        tasks.append(self.task.async_validate_docs(
                            self.cluster, bucket, self.gen_update, "update", 0,
                            batch_size=self.batch_size,
                            process_concurrency=self.process_concurrency,
                            check_replica=self.check_replica))
                    if "create" in self.doc_ops:
                        tasks.append(
                            self.task.async_validate_docs(
                                self.cluster, bucket, self.gen_create, "create",
                                0, batch_size=self.batch_size,
                                process_concurrency=self.process_concurrency,
                                check_replica=self.check_replica))
                    if "delete" in self.doc_ops:
                        tasks.append(
                            self.task.async_validate_docs(
                                self.cluster, bucket, self.gen_delete, "delete", 0,
                                batch_size=self.batch_size,
                                process_concurrency=self.process_concurrency,
                                check_replica=self.check_replica))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)

    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update.

        This test begins with all servers clustered together and  loads a user defined
        number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
        in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
        from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
        disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
        curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""

    def rebalance_out_after_ops(self):
        self.gen_delete = self.get_doc_generator(self.items / 2,
                                                 self.items)
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items / 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        self.check_temporary_failure_exception = False
        self.loadgen_docs(task_verification=True)

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write abort scenario for replica vbs")
            for server in self.cluster_util.get_kv_nodes(self.cluster):
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(server)
                replica_vbs = cbstats.vbucket_list(
                    self.cluster.buckets[0].name, "replica")
                load_gen = doc_generator(self.key, 0, 5000,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen], self.cluster,
                    self.cluster.buckets[0], self.durability_level,
                    DocLoading.Bucket.DocOps.UPDATE, "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        servs_out = [self.cluster.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster,
                timeout=self.wait_timeout)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
#         record_data_set = self.bucket_util.get_data_set_all(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.add_remove_servers_and_rebalance([], servs_out)
        if not self.atomicity:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)
            self.bucket_util.verify_cluster_stats(
                self.cluster, self.num_items,
                check_ep_items_remaining=True,
                timeout=self.wait_timeout)
        new_failover_stats = self.bucket_util.compare_failovers_logs(
            self.cluster, prev_failover_stats,
            self.cluster.servers[:self.nodes_init - self.nodes_out],
            self.cluster.buckets)
        new_vbucket_stats = self.bucket_util.compare_vbucket_seqnos(
            self.cluster, prev_vbucket_stats,
            self.cluster.servers[:self.nodes_init - self.nodes_out],
            self.cluster.buckets,
            perNode=False)
        self.sleep(60)
#         self.bucket_util.data_analysis_all(record_data_set, self.cluster.servers[:self.nodes_init - self.nodes_out], self.cluster.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
        self.bucket_util.vb_distribution_analysis(
            self.cluster,
            servers=nodes, buckets=self.cluster.buckets, std=1.0,
            total_vbuckets=self.cluster.vbuckets, num_replicas=self.num_replicas)

    """Rebalances nodes out with failover and full recovery add back of a node

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
    from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
    disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
    curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""

    def rebalance_out_with_failover_full_addback_recovery(self):
        self.gen_delete = self.get_doc_generator(self.items / 2,
                                                 self.items)
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items / 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks_info = self.loadgen_docs()
        servs_out = [self.cluster.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.rest = RestConnection(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        self.sleep(20)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        record_data_set = self.bucket_util.get_data_set_all(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="full")
        self.add_remove_servers_and_rebalance([], servs_out)

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                              check_ep_items_remaining=True,
                                              timeout=self.wait_timeout)
        self.bucket_util.compare_failovers_logs(
            self.cluster, prev_failover_stats,
            self.cluster.servers[:self.nodes_init - self.nodes_out],
            self.cluster.buckets)
        self.sleep(30)
        self.bucket_util.data_analysis_all(record_data_set, self.cluster.servers[:self.nodes_init - self.nodes_out], self.cluster.buckets)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
        self.bucket_util.vb_distribution_analysis(
            self.cluster, servers=nodes,
            buckets=self.cluster.buckets, std=1.0,
            total_vbuckets=self.cluster.vbuckets,
            num_replicas=self.num_replicas)

    """Rebalances nodes out with failover

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
    from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
    disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
    curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""

    def rebalance_out_with_failover(self):
        fail_over = self.input.param("fail_over", False)
        self.rest = RestConnection(self.cluster.master)
        self.gen_delete = self.get_doc_generator(self.items / 2,
                                                 self.items)
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items / 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks_info = self.loadgen_docs()
        ejectedNode = self.cluster_util.find_node_info(self.cluster.master, self.cluster.servers[self.nodes_init - 1])
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)
        self.sleep(20)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        record_data_set = self.bucket_util.get_data_set_all(self.cluster.servers[:self.nodes_init], self.cluster.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        new_server_list = self.cluster_util.add_remove_servers(
            self.cluster, self.cluster.servers[:self.nodes_init],
            [self.cluster.servers[self.nodes_init - 1], chosen[0]], [])
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=fail_over)
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[chosen[0].id])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance failed")
        self.cluster.nodes_in_cluster = new_server_list

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
            self.bucket_util.verify_cluster_stats(
                self.cluster, self.num_items,
                check_ep_items_remaining=True,
                timeout=self.wait_timeout)
        self.sleep(30)
        self.bucket_util.data_analysis_all(record_data_set, new_server_list, self.cluster.buckets)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
        self.bucket_util.vb_distribution_analysis(
            self.cluster,
            servers=nodes, buckets=self.cluster.buckets,
            num_replicas=self.num_replicas, std=1.0,
            total_vbuckets=self.cluster.vbuckets)


    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update along with compaction.

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. It then remove nodes_out from the cluster at a time
    and rebalances. During the rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced the test is finished."""

    def rebalance_out_with_compaction_and_ops(self):
        self.gen_delete = self.get_doc_generator(self.items / 2,
                                                 self.items)
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items / 2)
        servs_out = [self.cluster.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        rebalance_task = self.task.async_rebalance(self.cluster, [], servs_out)
        compaction_task = list()
        for bucket in self.cluster.buckets:
            compaction_task.append(self.task.async_compact_bucket(self.cluster.master, bucket))
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks_info = self.loadgen_docs()
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.task_manager.abort_all_tasks()
            for task in tasks_info:
                try:
                    for client in task.clients:
                        client.close()
                except Exception:
                    pass
            self.fail("Rebalance Failed")

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        for task in compaction_task:
            self.task_manager.get_task_result(task)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                              timeout=self.wait_timeout)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)

    """Rebalances nodes from a cluster during getting random keys.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. Then we send requests to all nodes in the cluster
    to get random key values. Next step is remove nodes_out from the cluster
    and rebalance it. During rebalancing we get random keys from all nodes and verify
    that are different every time. Once the cluster has been rebalanced
    we again get random keys from all new nodes in the cluster,
    than we wait for the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""

    def rebalance_out_get_random_key(self):
        servs_out = [self.cluster.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
        # get random keys for new added nodes
        rest_cons = [RestConnection(self.cluster.servers[i]) for i in xrange(self.nodes_init - self.nodes_out)]
        rebalance = self.task.async_rebalance(self.cluster, [], servs_out)
        self.sleep(2)
        result = []
        num_iter = 0
        # get random keys for each node during rebalancing
        while rest_cons[0]._rebalance_progress_status() == 'running' and num_iter < 100:
            list_threads = []
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
        self.assertTrue(rebalance.result,"Rebalance Failed")
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                                  timeout=self.wait_timeout)
            self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)


    """Rebalances nodes out of a cluster while doing docs' ops.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes two nodes at a time from the
    cluster and rebalances. During the rebalance we update(all of the items in the cluster)/
    delete( num_items/(num_servers -1) in each iteration)/
    create(a half of initial items in each iteration). Once the cluster has been rebalanced
    the test waits for the disk queues to drain and then verifies that there has been no data loss,
    sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""

    def incremental_rebalance_out_with_ops(self):
        items = self.items
        delete_from = items/2
        create_from = items
        majority = (self.num_replicas+1)/2+1
        for i in reversed(range(majority, self.nodes_init, 2)):
            self.gen_delete = self.get_doc_generator(delete_from,
                                                     delete_from+items/2)
            self.gen_create = self.get_doc_generator(create_from,
                                                     create_from+items)
            delete_from += items
            create_from += items
            rebalance_task = self.task.async_rebalance(self.cluster, [], self.cluster.servers[i:i + 2])
            tasks_info = self.loadgen_docs()
            self.task.jython_task_manager.get_task_result(rebalance_task)
            if not rebalance_task.result:
                self.task_manager.abort_all_tasks()
                for task in tasks_info:
                    try:
                        for client in task.clients:
                            client.close()
                    except Exception:
                        pass
                self.fail("Rebalance Failed")

            # Wait for rebalance+doc_loading tasks to complete
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            if not self.atomicity:
                self.bucket_util.verify_doc_op_task_exceptions(
                    tasks_info, self.cluster,
                    sdk_client_pool=self.sdk_client_pool)
                self.bucket_util.log_doc_ops_task_failures(tasks_info)
                for task, task_info in tasks_info.items():
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc ops failed for task: {}".format(task.thread_name))
                self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(self.cluster.servers[i:i + 2]))
                self.bucket_util.verify_cluster_stats(
                    self.cluster, self.num_items, timeout=self.wait_timeout)
                self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)

    """Rebalances nodes out of a cluster during view queries.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It creates num_views as
    development/production view with default map view funcs(is_dev_ddoc = True by default).
    It then removes nodes_out nodes at a time and rebalances that node from the cluster.
    During the rebalancing we perform view queries for all views and verify the expected number
    of docs for them. Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss,sum(curr_items) match
    the curr_items_total. Once successful view queries the test is finished."""

    def rebalance_out_with_queries(self):
        num_views = self.input.param("num_views", 2)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = dict()
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"

        views = list()
        tasks = list()

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write abort scenario for replica vbs")
            for server in self.cluster_util.get_kv_nodes(self.cluster):
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(server)
                replica_vbs = cbstats.vbucket_list(
                    self.cluster.buckets[0].name, "replica")
                load_gen = doc_generator(self.key, 0, 5000,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen], self.cluster,
                    self.cluster.buckets[0], self.durability_level,
                    DocLoading.Bucket.DocOps.UPDATE, "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        for bucket in self.cluster.buckets:
            temp = self.bucket_util.make_default_views(
                self.default_view,
                self.default_view_name,
                num_views, is_dev_ddoc)
            temp_tasks = self.bucket_util.async_create_views(
                self.cluster.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 4, len(self.cluster.buckets) * self.wait_timeout * self.num_items / 50000)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        for bucket in self.cluster.buckets:
            for view in views:
                # run queries to create indexes
                self.bucket_util.query_view(self.cluster.master, prefix + ddoc_name, view.name, query)

        active_tasks = self.cluster_util.async_monitor_active_task(
            self.cluster.servers, "indexer", "_design/" + prefix + ddoc_name,
            wait_task=False)
        for active_task in active_tasks:
            self.task_manager.get_task_result(active_task)
            self.assertTrue(active_task.result)

        expected_rows = self.num_items
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        for bucket in self.cluster.buckets:
            self.bucket_util.perform_verify_queries(
                self.cluster.master, num_views, prefix, ddoc_name,
                self.default_view_name, query, expected_rows,
                bucket=bucket, wait_time=timeout)

        servs_out = self.cluster.servers[-self.nodes_out:]
        rebalance = self.task.async_rebalance(self.cluster, [], servs_out)
        self.sleep(self.wait_timeout / 5)
        # see that the result of view queries are the same as expected during the test
        for bucket in self.cluster.buckets:
            self.bucket_util.perform_verify_queries(
                self.cluster.master, num_views, prefix, ddoc_name,
                self.default_view_name, query, expected_rows,
                bucket=bucket, wait_time=timeout)
        # verify view queries results after rebalancing
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        for bucket in self.cluster.buckets:
            self.bucket_util.perform_verify_queries(
                self.cluster.master, num_views, prefix, ddoc_name,
                self.default_view_name, query, expected_rows,
                bucket=bucket, wait_time=timeout)
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                                  timeout=self.wait_timeout)
            self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)

    """Rebalances nodes out of a cluster during view queries incrementally.

    This test begins with all servers clustered together and  loading a given number of items
    into the cluster. It creates num_views as development/production view with
    default map view funcs(is_dev_ddoc = True by default).  It then adds two nodes at a time and
    rebalances that node into the cluster. During the rebalancing we perform view queries
    for all views and verify the expected number of docs for them.
    Perform the same view queries after cluster has been completed. Then we wait for
    the disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match
    the curr_items_total. Once all nodes have been rebalanced in the test is finished."""

    def incremental_rebalance_out_with_queries(self):
        num_views = self.input.param("num_views", 2)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        views = self.bucket_util.make_default_views(self.default_view,
                                                    self.default_view_name,
                                                    num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        # increase timeout for big data
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 5, self.wait_timeout * self.num_items / 25000)
        query = {}
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"

        tasks = self.bucket_util.async_create_views(self.cluster.master, ddoc_name, views, 'default')
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        for view in views:
            # run queries to create indexes
            self.bucket_util.query_view(self.cluster.master, prefix + ddoc_name, view.name, query, timeout=self.wait_timeout * 2)

        for i in xrange(3):
            active_tasks = self.cluster_util.async_monitor_active_task(self.cluster.servers, "indexer",
                                                                  "_design/" + prefix + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                self.task_manager.get_task_result(active_task)
                self.assertTrue(active_task.result)
            self.sleep(2)

        expected_rows = self.num_items
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        self.bucket_util.perform_verify_queries(
                self.cluster.master, num_views, prefix, ddoc_name,
                self.default_view_name, query, expected_rows,
                wait_time=timeout)
        query["stale"] = "update_after"
        for i in reversed(range(1, self.nodes_init, 2)):
            rebalance = self.task.async_rebalance(self.cluster, [], self.cluster.servers[i:i + 2])
            self.sleep(self.wait_timeout / 5)
            # see that the result of view queries are the same as expected during the test
            self.bucket_util.perform_verify_queries(
                self.cluster.master, num_views, prefix, ddoc_name,
                self.default_view_name, query, expected_rows,
                wait_time=timeout)
            # verify view queries results after rebalancing
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance Failed")
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(self.cluster.servers[i:i + 2]))
            self.bucket_util.perform_verify_queries(
                self.cluster.master, num_views, prefix, ddoc_name,
                self.default_view_name, query, expected_rows,
                wait_time=timeout)
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                                  timeout=self.wait_timeout)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)

    """Rebalances nodes into a cluster when one node is warming up.

    This test begins with loads a user defined number of items into the cluster
    and all servers clustered together. Next steps are: stop defined
    node(master_restart = False by default), wait 20 sec and start the stopped node.
    Without waiting for the node to start up completely, rebalance out servs_out servers.
    Expect that rebalance is failed. Wait for warmup completed and start
    rebalance with the same configuration. Once the cluster has been rebalanced
    we wait for the disk queues to drain, and then verify that there has been no data loss,
    sum(curr_items) match the curr_items_total."""

    def rebalance_out_with_warming_up(self):
        master_restart = self.input.param("master_restart", False)
        if master_restart:
            warmup_node = self.cluster.master
        else:
            warmup_node = self.cluster.servers[len(self.cluster.servers) - self.nodes_out - 1]
        servs_out = self.cluster.servers[len(self.cluster.servers) - self.nodes_out:]

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write abort scenario for replica vbs")
            for server in self.cluster_util.get_kv_nodes(self.cluster):
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(server)
                replica_vbs = cbstats.vbucket_list(
                    self.cluster.buckets[0].name, "replica")
                load_gen = doc_generator(self.key, 0, 5000,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen], self.cluster,
                    self.cluster.buckets[0], self.durability_level,
                    DocLoading.Bucket.DocOps.UPDATE, "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(20)
        shell.start_couchbase()
        shell.disconnect()

        # Workaround for Eph case (MB-44682 - Not a bug)
        if self.bucket_type == Bucket.Type.EPHEMERAL:
            self.sleep(15, "Wait for couchbase server to start")

        rebalance = self.task.async_rebalance(
            self.cluster, [], servs_out)
        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        if rebalance.result is False:
            self.log.info("Rebalance was failed as expected")
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=self.cluster_util.get_kv_nodes(self.cluster),
                wait_time=self.wait_timeout * 10))

            self.log.info("Second attempt to rebalance")
            rebalance = self.task.async_rebalance(
                self.cluster, [], servs_out)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.assertTrue(rebalance.result, "Rebalance attempt failed again")
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        if not self.atomicity:
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                                  timeout=self.wait_timeout)
            self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)


    """Rebalances nodes out of a cluster while doing mutations and deletions.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update half of the items in the
    cluster and delete the other half. Once the cluster has been rebalanced the test
    recreates all of the deleted items, waits for the disk queues to drain, and then
    verifies that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""

    def incremental_rebalance_out_with_mutation_and_deletion(self):
        gen_2 = self.get_doc_generator(self.num_items / 2 + 2000,
                                       self.num_items)
        for i in reversed(range(self.nodes_init)[1:]):
            # don't use batch for rebalance out 2-1 nodes
            rebalance_task = self.task.async_rebalance(
                self.cluster, [], [self.cluster.servers[i]])
            self.sleep(5, "Wait for rebalance to start")
            tasks_info = dict()
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                sdk_client_pool=self.sdk_client_pool)
            tasks_info.update(tem_tasks_info.copy())
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, gen_2, "delete", 0,
                sdk_client_pool=self.sdk_client_pool)
            tasks_info.update(tem_tasks_info.copy())
            self.task.jython_task_manager.get_task_result(rebalance_task)
            if not rebalance_task.result:
                self.task_manager.abort_all_tasks()
                for task in tasks_info:
                    try:
                        for client in task.clients:
                            client.close()
                    except Exception:
                        pass
                self.fail("Rebalance Failed")
            self.cluster.nodes_in_cluster.remove(self.cluster.servers[i])
            for task in tasks_info.keys():
                self.task_manager.get_task_result(task)
            self.sleep(5, "Let the cluster relax for some time")
            self._load_all_buckets(self.cluster, gen_2, "create", 0)
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                                  timeout=self.wait_timeout)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)

    """Rebalances nodes out of a cluster while doing mutations and expirations.

    This test begins with all servers clustered together and loads a user defined number
    of items into the cluster. It then removes one node at a time from the cluster and
    rebalances. During the rebalance we update all of the items in the cluster and set
    half of the items to expire in 5 seconds. Once the cluster has been rebalanced the
    test recreates all of the expired items, waits for the disk queues to drain, and then
    verifies that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""

    def incremental_rebalance_out_with_mutation_and_expiration(self):
        gen_2 = self.get_doc_generator(self.num_items / 2 + 2000, self.num_items)
        batch_size = 1000
        for i in reversed(range(self.nodes_init)[2:]):
            # don't use batch for rebalance out 2-1 nodes
            rebalance = self.task.async_rebalance(self.cluster, [], [self.cluster.servers[i]])
            self.sleep(5, "Wait for rebalance to start")
            self._load_all_buckets(self.cluster, self.gen_update, "update", 0, batch_size=batch_size, timeout_secs=60)
            self._load_all_buckets(self.cluster, gen_2, "update", 5, batch_size=batch_size, timeout_secs=60)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - {self.cluster.servers[i]})
            self.sleep(5, "Let the cluster relax for some time")
            self._load_all_buckets(self.cluster, gen_2, "create", 0)
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items,
                                                  timeout=self.wait_timeout)
        self.bucket_util.verify_unacked_bytes_all_buckets(self.cluster)
