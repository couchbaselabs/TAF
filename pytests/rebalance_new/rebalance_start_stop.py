from Cb_constants import DocLoading
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection, RestHelper
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from rebalance_base import RebalanceBaseTest


class RebalanceStartStopTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceStartStopTests, self).setUp()
        extra_nodes_in = self.input.param("extra_nodes_in", 0)
        extra_nodes_out = self.input.param("extra_nodes_out", 0)
        self.servs_init = self.servers[:self.nodes_init]
        self.servs_in = [self.servers[i + self.nodes_init]
                         for i in range(self.nodes_in)]
        self.servs_out = [self.servers[self.nodes_init - i - 1]
                          for i in range(self.nodes_out)]
        self.extra_servs_in = [self.servers[i + self.nodes_init + self.nodes_in] for i in range(extra_nodes_in)]
        self.extra_servs_out = [self.servers[self.nodes_init - i - 1 - self.nodes_out] for i in range(extra_nodes_out)]
        self.withMutationOps = self.input.param("withMutationOps", True)
        self.sleep_before_rebalance = self.input.param("sleep_before_rebalance", 0)
        if self.spec_name is not None:
            self.num_items = 20000
            self.items = 20000
            init_doc_load_spec = \
                self.bucket_util.get_crud_template_from_package("initial_load")
            # Using the same key as defined in the loading spec
            self.gen_update = doc_generator(
                init_doc_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.COMMON_DOC_KEY],
                0, (self.items / 2),
                mutation_type="SET")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

    def tearDown(self):
        super(RebalanceStartStopTests, self).tearDown()

    def load_all_buckets(self, op_type, doc_load_percent):
        loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        if op_type == DocLoading.Bucket.DocOps.CREATE:
            loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] \
                = doc_load_percent
        elif op_type == DocLoading.Bucket.DocOps.UPDATE:
            loading_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] \
                = doc_load_percent
        return self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, self.bucket_util.buckets, loading_spec,
            mutation_num=0, async_load=True)

    def tasks_result(self, mutation_task):
        self.log.info("Waiting for data load to finish")
        self.task.jython_task_manager.get_task_result(mutation_task)
        if mutation_task.result is False:
            self.log.critical("Seeing failures in mutatate_from_spec")

    def validate_docs(self):
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_start_stop_rebalance(self):
        """
        Start-stop rebalance in/out with adding/removing aditional after stopping rebalance.

        This test begins by loading a given number of items into the cluster. It then
        add  servs_in nodes and remove  servs_out nodes and start rebalance. Then rebalance
        is stopped when its progress reached 20%. After we add  extra_nodes_in and remove
        extra_nodes_out. Restart rebalance with new cluster configuration. Later rebalance
        will be stop/restart on progress 40/60/80%. After each iteration we wait for
        the disk queues to drain, and then verify that there has been no data loss,
        sum(curr_items) match the curr_items_total. Once cluster was rebalanced the test is finished.
        The oder of add/remove nodes looks like:
        self.nodes_init|servs_in|extra_nodes_in|extra_nodes_out|servs_out
        """
        rest = RestConnection(self.cluster.master)
        self.bucket_util._wait_for_stats_all_buckets()
        self.log.info("Current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("Adding nodes {0} to cluster".format(self.servs_in))
        self.log.info("Removing nodes {0} from cluster".format(self.servs_out))
        add_in_once = self.extra_servs_in
        _ = set(self.servs_init + self.servs_in) - set(self.servs_out)
        # the latest iteration will be with i=5, for this case rebalance should be completed,
        # that also is verified and tracked
        for i in range(1, 6):
            if i == 1:
                rebalance = self.task.async_rebalance(
                    self.servs_init[:self.nodes_init],
                    self.servs_in, self.servs_out)
            else:
                rebalance = self.task.async_rebalance(
                    self.servs_init[:self.nodes_init] + self.servs_in,
                    add_in_once, self.servs_out + self.extra_servs_out)
                add_in_once = []
            self.sleep(20)
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))
            if not RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Stop the rebalance")
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
                self.assertTrue(stopped, msg="Unable to stop rebalance")
            self.task_manager.get_task_result(rebalance)
            if RestHelper(rest).is_cluster_rebalanced():
                self.validate_docs()
                self.log.info(
                    "Rebalance was completed when tried to stop rebalance on {0}%".format(str(expected_progress)))
                break
            else:
                # Trigger cb_collected with rebalance_stopped condition
                self.cbcollect_info(trigger=True, validate=True)
                self.log.info("Rebalance is still required. Verifying the data in the buckets")
                self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_start_stop_rebalance_with_mutations(self):
        """
            Start-stop rebalance in/out with adding/removing aditional after stopping rebalance with data mutations
            in background.

            This test begins by loading a given number of items into the cluster. It then
            add  servs_in nodes and remove  servs_out nodes and start rebalance. Then rebalance
            is stopped when its progress reached 20%. After we add  extra_nodes_in and remove
            extra_nodes_out. Restart rebalance with new cluster configuration. Later rebalance
            will be stop/restart on progress 40/60/80%.Before each iteration, we start data mutations
            and end the mutations before data validations. After each iteration we wait for
            the disk queues to drain, and then verify that there has been no data loss,
            sum(curr_items) match the curr_items_total. Once cluster was rebalanced the test is finished.
            The oder of add/remove nodes looks like:
            self.nodes_init|servs_in|extra_nodes_in|extra_nodes_out|servs_out
            """
        rest = RestConnection(self.cluster.master)
        self.bucket_util._wait_for_stats_all_buckets()
        self.log.info("Current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("Adding nodes {0} to cluster".format(self.servs_in))
        self.log.info("Removing nodes {0} from cluster".format(self.servs_out))
        add_in_once = self.extra_servs_in
        # the last iteration will be with i=5,for this case rebalance
        # should be completed, that also is verified and tracked
        for i in range(1, 6):
            if self.withMutationOps:
                task = self.load_all_buckets(DocLoading.Bucket.DocOps.UPDATE,
                                             50)
            if i == 1:
                rebalance = self.task.async_rebalance(
                    self.servs_init[:self.nodes_init],
                    self.servs_in, self.servs_out,
                    sleep_before_rebalance=self.sleep_before_rebalance)
            else:
                rebalance = self.task.async_rebalance(
                    self.servs_init[:self.nodes_init] + self.servs_in,
                    add_in_once, self.servs_out + self.extra_servs_out,
                    sleep_before_rebalance=self.sleep_before_rebalance)
                add_in_once = []
            self.sleep(20)
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))
            if not RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Stop the rebalance")
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout/3)
                self.assertTrue(stopped, msg="Unable to stop rebalance")
                # Trigger cb_collect with rebalance stopped and doc_ops running
                self.cbcollect_info(trigger=True, validate=True)
                if self.withMutationOps:
                    self.tasks_result(task)
                self.sleep(5)
            self.task.jython_task_manager.get_task_result(rebalance)
            if RestHelper(rest).is_cluster_rebalanced():
                self.validate_docs()
                self.log.info("Rebalance was completed when tried "
                              "to stop rebalance on %s%%" % expected_progress)
                break
            else:
                self.log.info("Rebalance is still required. "
                              "Verifying the data in the buckets")
                self.bucket_util._wait_for_stats_all_buckets()

        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_start_stop_rebalance_before_mutations(self):
        """
            Start-stop rebalance in/out with adding/removing aditional
            after stopping rebalance.

            This test begins by loading a given number of items into
            the cluster. It then add  servs_in nodes and remove  servs_out
            nodes and start rebalance. Then rebalance is stopped when its
            progress reached 20%. After we add  extra_nodes_in and remove
            extra_nodes_out. Restart rebalance with new cluster configuration.
            Later rebalance will be stop/restart on progress 40/60/80%.
            After each iteration we wait for the disk queues to drain,
            and then verify that there has been no data loss,
            sum(curr_items) match the curr_items_total. Once cluster was
            rebalanced the test is finished.
            The oder of add/remove nodes looks like:
            self.nodes_init|servs_in|extra_nodes_in|extra_nodes_out|servs_out
            """
        rest = RestConnection(self.cluster.master)
        self.bucket_util._wait_for_stats_all_buckets()
        self.log.info("Current nodes : {0}".format([node.id for node in rest.node_statuses()]))
        self.log.info("Adding nodes {0} to cluster".format(self.servs_in))
        self.log.info("Removing nodes {0} from cluster".format(self.servs_out))
        add_in_once = self.extra_servs_in
        self.gen_create = self.get_doc_generator(0, self.num_items)
        # The latest iteration will be with i=5. For this rebalance should be
        # completed. That also is verified & tracked
        for i in range(1, 6):
            if i == 1:
                rebalance = self.task.async_rebalance(
                    self.servs_init[:self.nodes_init],
                    self.servs_in, self.servs_out,
                    sleep_before_rebalance=self.sleep_before_rebalance)
            else:
                rebalance = self.task.async_rebalance(
                    self.servs_init[:self.nodes_init] + self.servs_in,
                    add_in_once, self.servs_out + self.extra_servs_out,
                    sleep_before_rebalance=self.sleep_before_rebalance)
                add_in_once = []
            self.sleep(20)
            expected_progress = 20 * i
            reached = RestHelper(rest).rebalance_reached(expected_progress)
            self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                            .format(expected_progress))
            if not RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Stop the rebalance")
                stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout/3)
                self.assertTrue(stopped, msg="Unable to stop rebalance")
                if self.withMutationOps:
                    task = self.load_all_buckets(
                        DocLoading.Bucket.DocOps.UPDATE, 50)
                    self.tasks_result(task)
                self.sleep(5)
            self.task.jython_task_manager.get_task_result(rebalance)
            if RestHelper(rest).is_cluster_rebalanced():
                self.validate_docs()
                self.log.info("Rebalance was completed when tried to "
                              "stop rebalance on %s%%" % expected_progress)
                break
            else:
                self.log.info("Rebalance is still required. "
                              "Verifying the data in the buckets.")
                self.bucket_util._wait_for_stats_all_buckets()

        self.bucket_util.verify_unacked_bytes_all_buckets()

    def test_start_stop_rebalance_after_failover(self):
        """
            Rebalances nodes out and in with failover
            Use different nodes_in and nodes_out params to have uneven add and
            deletion. Use 'zone' param to have nodes divided into server groups
            by having zone > 1.

            The test begin with loading the bucket with given number of items.
            It then fails over a node. We then rebalance the cluster,
            while adding or removing given number of nodes.
            Once the rebalance reaches 50%, we stop the rebalance and validate
            the cluster stats. We then restart the rebalance and
            validate rebalance was completed successfully.
            """
        fail_over = self.input.param("fail_over", False)
        task = self.load_all_buckets(DocLoading.Bucket.DocOps.UPDATE, 50)
        self.tasks_result(task)
        self.validate_docs()
        self.sleep(20)

        prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(
            self.servers[:self.nodes_init], self.bucket_util.buckets)
        prev_failover_stats = self.bucket_util.get_failovers_logs(
            self.servers[:self.nodes_init], self.bucket_util.buckets)
        _, _ = self.bucket_util.get_and_compare_active_replica_data_set_all(
            self.servers[:self.nodes_init], self.bucket_util.buckets,
            path=None)
        self.bucket_util.compare_vbucketseq_failoverlogs(prev_vbucket_stats,
                                                         prev_failover_stats)
        self.rest = RestConnection(self.cluster.master)
        chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)
        _ = list(set(self.servers[:self.nodes_init] + self.servs_in)
                 - set(self.servs_out))
        for node in self.servs_in:
            self.rest.add_node(self.cluster.master.rest_username,
                               self.cluster.master.rest_password,
                               node.ip, node.port)
        # Mark Node for failover
        self.rest.fail_over(chosen[0].id, graceful=fail_over)

        # Doc_mutation after failing over the nodes
        create_percent = 100
        task = self.load_all_buckets(DocLoading.Bucket.DocOps.CREATE,
                                     create_percent)
        self.tasks_result(task)
        self.task.async_rebalance(
            self.servers[:self.nodes_init], self.servs_in, self.servs_out)
        expected_progress = 50
        rest = RestConnection(self.cluster.master)
        reached = RestHelper(rest).rebalance_reached(expected_progress)
        self.assertTrue(reached, "Rebalance failed or did not reach {0}%"
                        .format(expected_progress))
        if not RestHelper(rest).is_cluster_rebalanced():
            self.log.info("Stop the rebalance")
            stopped = rest.stop_rebalance(wait_timeout=self.wait_timeout / 3)
            self.assertTrue(stopped, msg="Unable to stop rebalance")

        # Trigger cbcollect with halted failover
        self.cbcollect_info(trigger=True, validate=True)

        self.shuffle_nodes_between_zones_and_rebalance()
        self.validate_docs()
        self.sleep(30)
        self.bucket_util.verify_unacked_bytes_all_buckets()
        nodes = self.cluster_util.get_nodes_in_cluster(self.cluster.master)
        self.bucket_util.vb_distribution_analysis(
            servers=nodes, std=1.0,
            total_vbuckets=self.cluster_util.vbuckets)
