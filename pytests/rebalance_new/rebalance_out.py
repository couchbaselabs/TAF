import time

from membase.api.exception import RebalanceFailedException, SetViewInfoNotFound
from membase.api.rest_client import RestConnection
from rebalance_base import RebalanceBaseTest
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from remote.remote_util import RemoteMachineShellConnection


class RebalanceOutTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceOutTests, self).tearDown()

    def test_rebalance_out_with_ops(self):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first, start=self.num_items,
                                       end=self.num_items * 2)
        gen_delete = DocumentGenerator('test_docs', template, age, first, start=self.num_items / 2, end=self.num_items)
        servs_out = [self.cluster.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        tasks = []
        task = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], servs_out)
        tasks.append(task)
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(
                        self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "update", 0, batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0,
                                                               batch_size=10, process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(
                        self.task.async_load_gen_docs(self.cluster, bucket, gen_delete, "delete", 0, batch_size=10))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        tasks = []
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(self.cluster, bucket, gen_create, "update", 0,
                                                               batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(self.cluster, bucket, gen_create, "create", 0, batch_size=10,
                                                      process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(self.cluster, bucket, gen_delete, "delete", 0, batch_size=10))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)

    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update.

        This test begins with all servers clustered together and  loads a user defined
        number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
        in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
        from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
        disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
        curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""

    def rebalance_out_after_ops(self):
        gen_delete = self._get_doc_generator(self.num_items / 2, self.num_items)
        gen_create = self._get_doc_generator(self.num_items + 1, self.num_items * 3 / 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks = []
        if (self.doc_ops is not None):
            if ("update" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, self.gen_update, "update", 0)
            if ("create" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_create, "create", 0)
                self.num_items = self.num_items + 1 + (self.num_items * 3 / 2)
            if ("delete" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_delete, "delete", 0)
                self.num_items = self.num_items - (self.num_items / 2)
            for task in tasks:
                self.task_manager.get_task_result(task)
        servs_out = [self.cluster.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        self.bucket_util._verify_stats_all_buckets(self.num_items, timeout=120)
        self.bucket_util._wait_for_stats_all_buckets()
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.num_servers], self.bucket_util.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.num_servers], self.bucket_util.buckets)
        record_data_set = self.bucket_util.get_data_set_all(self.cluster.servers[:self.num_servers], self.bucket_util.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.add_remove_servers_and_rebalance([], servs_out)
        self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
        self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
        new_failover_stats = self.bucket_util.compare_failovers_logs(prev_failover_stats,
                                                         self.cluster.servers[:self.num_servers - self.nodes_out], self.bucket_util.buckets)
        new_vbucket_stats = self.bucket_util.compare_vbucket_seqnos(prev_vbucket_stats,
                                                        self.cluster.servers[:self.num_servers - self.nodes_out], self.bucket_util.buckets,
                                                        perNode=False)
        self.sleep(60)
        self.bucket_util.data_analysis_all(record_data_set, self.cluster.servers[:self.num_servers - self.nodes_out], self.bucket_util.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(new_vbucket_stats, new_failover_stats)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(servers=nodes, buckets=self.bucket_util.buckets, std=1.0, total_vbuckets=self.vbuckets)

    """Rebalances nodes out with failover and full recovery add back of a node

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. Before rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).It then remove nodes_out
    from the cluster at a time and rebalances.  Once the cluster has been rebalanced we wait for the
    disk queues to drain, and then verify that there has been no data loss, sum(curr_items) match the
    curr_items_total. We also check for data and its meta-data, vbucket sequene numbers"""

    def rebalance_out_with_failover_full_addback_recovery(self):
        gen_delete = self._get_doc_generator(self.num_items / 2, self.num_items)
        gen_create = self._get_doc_generator(self.num_items + 1, self.num_items * 3 / 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks = []
        if (self.doc_ops is not None):
            if ("update" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, self.gen_update, "update", 0)
            if ("create" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_create, "create", 0)
                self.num_items = self.num_items + 1 + (self.num_items * 3 / 2)
            if ("delete" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_delete, "delete", 0)
                self.num_items = self.num_items - (self.num_items / 2)
            for task in tasks:
                self.task_manager.get_task_result(task)
        servs_out = [self.cluster.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        self.bucket_util._verify_stats_all_buckets(self.num_items, timeout=120)
        self.bucket_util._wait_for_stats_all_buckets()
        self.rest = RestConnection(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        self.sleep(20)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.num_servers], self.bucket_util.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.num_servers], self.bucket_util.buckets)
        record_data_set = self.bucket_util.get_data_set_all(self.cluster.servers[:self.num_servers], self.bucket_util.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=False)
        # Mark Node for full recovery
        if success_failed_over:
            self.rest.set_recovery_type(otpNode=chosen[0].id, recoveryType="full")
        self.add_remove_servers_and_rebalance([], servs_out)
        self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
        self.bucket_util.compare_failovers_logs(prev_failover_stats, self.cluster.servers[:self.num_servers - self.nodes_out], self.bucket_util.buckets)
        self.sleep(30)
        self.bucket_util.data_analysis_all(record_data_set, self.cluster.servers[:self.num_servers - self.nodes_out], self.bucket_util.buckets)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(servers=nodes, buckets=self.bucket_util.buckets, std=1.0, total_vbuckets=self.vbuckets)

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
        gen_delete = self._get_doc_generator(self.num_items / 2, self.num_items)
        gen_create = self._get_doc_generator(self.num_items + 1, self.num_items * 3 / 2)
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        tasks = []
        if (self.doc_ops is not None):
            if ("update" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, self.gen_update, "update", 0)
            if ("create" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_create, "create", 0)
                self.num_items = self.num_items + 1 + (self.num_items * 3 / 2)
            if ("delete" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_delete, "delete", 0)
                self.num_items = self.num_items - (self.num_items / 2)
            for task in tasks:
                self.task_manager.get_task_result(task)
        ejectedNode = self.cluster_util.find_node_info(self.cluster.master, self.cluster.servers[self.nodes_init - 1])
        self.bucket_util.verify_stats_all_buckets(self.num_items, timeout=120)
        self.bucket_util._wait_for_stats_all_buckets()
        self.sleep(20)
        prev_failover_stats = self.bucket_util.get_failovers_logs(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        record_data_set = self.bucket_util.get_data_set_all(self.cluster.servers[:self.nodes_init], self.bucket_util.buckets)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats, prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        new_server_list = self.add_remove_servers(self.cluster.servers, self.cluster.servers[:self.nodes_init],
                                                  [self.cluster.servers[self.nodes_init - 1], chosen[0]], [])
        # Mark Node for failover
        success_failed_over = self.rest.fail_over(chosen[0].id, graceful=fail_over)
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[chosen[0].id, ejectedNode.id])
        self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance failed")
        self.cluster.nodes_in_cluster = new_server_list
        self.bucket_util.verify_cluster_stats(self.num_items, check_ep_items_remaining=True)
        self.sleep(30)
        self.bucket_util.data_analysis_all(record_data_set, new_server_list, self.bucket_util.buckets)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(servers=nodes, buckets=self.bucket_util.buckets, std=1.0, total_vbuckets=self.vbuckets)


    """Rebalances nodes out of a cluster while doing docs ops:create, delete, update along with compaction.

    This test begins with all servers clustered together and  loads a user defined
    number of items into the cluster. It then remove nodes_out from the cluster at a time
    and rebalances. During the rebalance we perform docs ops(add/remove/update/read)
    in the cluster( operate with a half of items that were loaded before).
    Once the cluster has been rebalanced we wait for the disk queues to drain,
    and then verify that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced the test is finished."""

    def rebalance_out_with_compaction_and_ops(self):
        gen_delete = self._get_doc_generator(self.num_items / 2, self.num_items)
        gen_create = self._get_doc_generator(self.num_items + 1, self.num_items * 3 / 2)
        servs_out = [self.cluster.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        tasks = [self.task.async_rebalance(self.cluster.servers[:1], [], servs_out)]
        compaction_task = []
        for bucket in self.bucket_util.buckets:
            compaction_task.append(self.cluster.async_compact_bucket(self.cluster.master, bucket))
        # define which doc's ops will be performed during rebalancing
        # allows multiple of them but one by one
        if (self.doc_ops is not None):
            if ("update" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, self.gen_update, "update", 0)
            if ("create" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_create, "create", 0)
                self.num_items = self.num_items + 1 + (self.num_items * 3 / 2)
            if ("delete" in self.doc_ops):
                tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_delete, "delete", 0)
                self.num_items = self.num_items - (self.num_items / 2)
        for task in tasks:
            self.task_manager.get_task_result(task)
        for task in compaction_task:
            self.task_manager.get_task_result(task)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

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
        servs_out = [self.cluster.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        # get random keys for new added nodes
        rest_cons = [RestConnection(self.cluster.servers[i]) for i in xrange(self.num_servers - self.nodes_out)]
        rebalance = self.task.async_rebalance(self.cluster.servers[:self.num_servers], [], servs_out)
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
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()


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
        gen_delete = self._get_doc_generator(self.num_items / 2, self.num_items)
        gen_create = self._get_doc_generator(self.num_items + 1, self.num_items * 3 / 2)
        for i in reversed(range(1, self.num_servers, 2)):
            tasks = [self.task.async_rebalance(self.cluster.servers[:i], [], self.cluster.servers[i:i + 2])]
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks += self.bucket_util._async_load_all_buckets(self.cluster, self.gen_update, "update", 0)
                if ("create" in self.doc_ops):
                    tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_create, "create", 0)
                    self.num_items = self.num_items + 1 + (self.num_items * 3 / 2)
                if ("delete" in self.doc_ops):
                    tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_delete, "delete", 0)
                    self.num_items = self.num_items - (self.num_items / 2)
                for task in tasks:
                    self.task_manager.get_task_result(task)
                self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(self.cluster.servers[i:i + 2]))
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

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
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", False)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]

        query = {}
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"

        views = []
        tasks = []
        for bucket in self.bucket_util.buckets:
            temp = self.bucket_util.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
            temp_tasks = self.bucket_util.async_create_views(self.cluster.master, ddoc_name, temp, bucket)
            views += temp
            tasks += temp_tasks
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 4, len(self.bucket_util.buckets) * self.wait_timeout * self.num_items / 50000)

        for task in tasks:
            self.task_manager.get_task_result(task)

        for bucket in self.bucket_util.buckets:
            for view in views:
                # run queries to create indexes
                self.cluster.query_view(self.cluster.master, prefix + ddoc_name, view.name, query)

        active_tasks = self.cluster.async_monitor_active_task(self.cluster.servers, "indexer", "_design/" + prefix + ddoc_name,
                                                              wait_task=False)
        for active_task in active_tasks:
            result = self.task_manager.get_task_result(active_task)
            self.assertTrue(result)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        for bucket in self.bucket_util.buckets:
            self.bucket_util.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout,
                                        expected_rows=expected_rows)

        servs_out = self.cluster.servers[-self.nodes_out:]
        rebalance = self.task.async_rebalance([self.cluster.master], [], servs_out)
        self.sleep(self.wait_timeout / 5)
        # see that the result of view queries are the same as expected during the test
        for bucket in self.bucket_util.buckets:
            self.bucket_util.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout,
                                        expected_rows=expected_rows)
        # verify view queries results after rebalancing
        self.task.jython_task_manager.get_task_result(rebalance)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        for bucket in self.bucket_util.buckets:
            self.bucket_util.perform_verify_queries(num_views, prefix, ddoc_name, query, bucket=bucket, wait_time=timeout,
                                        expected_rows=expected_rows)
        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

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
        num_views = self.input.param("num_views", 5)
        is_dev_ddoc = self.input.param("is_dev_ddoc", True)
        views = self.bucket_util.make_default_views(self.default_view_name, num_views, is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[is_dev_ddoc]
        # increase timeout for big data
        timeout = None
        if self.active_resident_threshold == 0:
            timeout = max(self.wait_timeout * 5, self.wait_timeout * self.num_items / 25000)
        query = {}
        query["connectionTimeout"] = 60000
        query["full_set"] = "true"
        tasks = []
        tasks = self.bucket_util.async_create_views(self.cluster.master, ddoc_name, views, 'default')
        for task in tasks:
            self.task_manager.get_task_result(task)
        for view in views:
            # run queries to create indexes
            self.cluster.query_view(self.cluster.master, prefix + ddoc_name, view.name, query, timeout=self.wait_timeout * 2)

        for i in xrange(3):
            active_tasks = self.cluster.async_monitor_active_task(self.cluster.servers, "indexer",
                                                                  "_design/" + prefix + ddoc_name, wait_task=False)
            for active_task in active_tasks:
                result = self.task_manager.get_task_result(active_task)
            self.sleep(2)

        expected_rows = None
        if self.max_verify:
            expected_rows = self.max_verify
            query["limit"] = expected_rows
        query["stale"] = "false"

        self.bucket_util.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout, expected_rows=expected_rows)
        query["stale"] = "update_after"
        for i in reversed(range(1, self.num_servers, 2)):
            rebalance = self.task.async_rebalance(self.cluster.servers[:i], [], self.cluster.servers[i:i + 2])
            self.sleep(self.wait_timeout / 5)
            # see that the result of view queries are the same as expected during the test
            self.bucket_util.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout,
                                        expected_rows=expected_rows)
            # verify view queries results after rebalancing
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(self.cluster.servers[i:i + 2]))
            self.bucket_util.perform_verify_queries(num_views, prefix, ddoc_name, query, wait_time=timeout,
                                        expected_rows=expected_rows)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

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
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        self.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        try:
            rebalance = self.task.async_rebalance(self.cluster.servers, [], servs_out)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        except RebalanceFailedException:
            self.log.info("rebalance was failed as expected")
            self.assertTrue(self.bucket_util._wait_warmup_completed(self, [warmup_node], \
                                                                          'default',
                                                                          wait_time=self.wait_timeout * 10))

            self.log.info("second attempt to rebalance")
            rebalance = self.task.async_rebalance(self.cluster.servers, [], servs_out)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()


    """Rebalances nodes out of a cluster while doing mutations and deletions.

    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then removes one node at a time from the
    cluster and rebalances. During the rebalance we update half of the items in the
    cluster and delete the other half. Once the cluster has been rebalanced the test
    recreates all of the deleted items, waits for the disk queues to drain, and then
    verifies that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""

    def incremental_rebalance_out_with_mutation_and_deletion(self):
        gen_2 = self._get_doc_generator(self.num_items / 2 + 2000,
                              self.num_items)
        for i in reversed(range(self.num_servers)[1:]):
            # don't use batch for rebalance out 2-1 nodes
            tasks = [self.task.async_rebalance(self.cluster.servers[:i], [], [self.cluster.servers[i]])]
            tasks += self.bucket_util._async_load_all_buckets(self.cluster, self.gen_update, "update", 0)
            tasks += self.bucket_util._async_load_all_buckets(self.cluster, gen_2, "delete", 0)
            for task in tasks:
                self.task_manager.get_task_result(task)
            self.sleep(5)
            self._load_all_buckets(self.cluster.master, gen_2, "create", 0)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    """Rebalances nodes out of a cluster while doing mutations and expirations.

    This test begins with all servers clustered together and loads a user defined number
    of items into the cluster. It then removes one node at a time from the cluster and
    rebalances. During the rebalance we update all of the items in the cluster and set
    half of the items to expire in 5 seconds. Once the cluster has been rebalanced the
    test recreates all of the expired items, waits for the disk queues to drain, and then
    verifies that there has been no data loss, sum(curr_items) match the curr_items_total.
    Once all nodes have been rebalanced out of the cluster the test finishes."""

    def incremental_rebalance_out_with_mutation_and_expiration(self):
        gen_2 = self._get_doc_generator(self.num_items / 2 + 2000, self.num_items)
        batch_size = 1000
        for i in reversed(range(self.num_servers)[2:]):
            # don't use batch for rebalance out 2-1 nodes
            rebalance = self.task.async_rebalance(self.cluster.servers[:i], [], [self.cluster.servers[i]])
            self._load_all_buckets(self.cluster.master, self.gen_update, "update", 0, batch_size=batch_size, timeout_secs=60)
            self._load_all_buckets(self.cluster.master, gen_2, "update", 5, batch_size=batch_size, timeout_secs=60)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - {self.cluster.servers[i]})
            self.sleep(5)
            self._load_all_buckets(self.cluster.master, gen_2, "create", 0)
            self.bucket_util.verify_cluster_stats(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()
