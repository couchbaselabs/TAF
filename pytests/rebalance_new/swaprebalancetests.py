import datetime

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from rebalance_base import RebalanceBaseTest
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import RebalanceFailedException
from remote.remote_util import RemoteMachineShellConnection
from BucketLib.BucketOperations import BucketHelper
from sdk_exceptions import SDKException
from rebalance_new import rebalance_base

retry_exceptions = rebalance_base.retry_exceptions +\
                    [SDKException.RequestCanceledException]


class SwapRebalanceBase(RebalanceBaseTest):
    def setUp(self):
        super(SwapRebalanceBase, self).setUp()
        self.log.info("=== SwapRebalanceBase setup started for test #%s %s ==="
                      % (self.case_number, self._testMethodName))
        self.cluster_run = False
        self.rest = RestConnection(self.cluster.master)
        if len(set([server.ip for server in self.servers])) == 1:
            ip = self.rest.get_nodes_self().ip
            for server in self.servers:
                server.ip = ip
            self.cluster_run = True
        self.failover_factor = self.num_swap = self.input.param("num-swap", 1)
        self.fail_orchestrator = self.swap_orchestrator = \
            self.input.param("swap-orchestrator", False)
        self.do_access = self.input.param("do-access", True)
        self.percentage_progress = self.input.param("percentage_progress", 50)
        self.load_started = False
        self.loaders = []
        self.replica_to_update = self.input.param("new_replica", None)
        try:
            # Make sure the test is setup correctly
            min_servers = int(self.nodes_init) + int(self.num_swap)
            msg = "minimum {0} nodes required for running swap rebalance"
            self.assertTrue(len(self.servers) >= min_servers,
                            msg=msg.format(min_servers))
            self.log.info('picking server : {0} as the master'
                          .format(self.cluster.master))
            self.enable_diag_eval_on_non_local_hosts(self.cluster.master)
            self.log_on_cluster("started")
        except Exception, e:
            self.fail(e)

        self.creds = self.input.membase_settings
        self.gen_create = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items)
        self.gen_update = self.get_doc_generator(self.num_items,
                                                 self.num_items + self.items)

        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0],
                replicaNumber=self.replica_to_update)
        self.log.info("=== SwapRebalanceBase setup done for test #%s %s ==="
                      % (self.case_number, self._testMethodName))

    def tearDown(self):
        super(SwapRebalanceBase, self).tearDown()

    def enable_diag_eval_on_non_local_hosts(self, master):
        """
        Enable diag/eval to be run on non-local hosts.
        :param master: Node information of the master node of the cluster
        :return: Nothing
        """
        remote = RemoteMachineShellConnection(master)
        output, _ = remote.enable_diag_eval_on_non_local_hosts()
        remote.disconnect()
        if "ok" not in output:
            self.log.error("Error enabling diag/eval on non-local host {}: {}"
                           .format(master.ip, output))
            raise Exception("Error in enabling diag/eval on non-local host {}"
                            .format(master.ip))
        else:
            self.log.info("Enabled diag/eval for non-local hosts from {}"
                          .format(master.ip))

    def log_on_cluster(self, progress):
        msg = "%s: %s %s" % (datetime.datetime.now(),
                             self._testMethodName,
                             progress)
        RestConnection(self.servers[0]).log_client_error(msg)

    # Used for items verification active vs. replica
    def items_verification(self, test, master):
        # Verify items count across all node
        timeout = 600
        for bucket in self.bucket_util.buckets:
            verified = self.bucket_util.wait_till_total_numbers_match(
                master, bucket, timeout_in_seconds=timeout)
            test.assertTrue(verified, "Lost items!!.. failing test in {0} secs"
                                      .format(timeout))

    def validate_docs(self):
        self.log.info("Validating docs")
        self.gen_create = self.get_doc_generator(0, self.num_items)
        tasks = []
        for bucket in self.bucket_util.buckets:
            tasks.append(self.task.async_validate_docs(
                self.cluster, bucket, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.validate_docs_per_collections_all_buckets()

    def _common_test_body_swap_rebalance(self, do_stop_start=False):
        self.loaders = super(SwapRebalanceBase, self).loadgen_docs(
            retry_exceptions=retry_exceptions)
        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(self.cluster.master)
        self.log.info("Current nodes: %s" % current_nodes)
        to_eject_nodes = self.cluster_util.pick_nodes(self.cluster.master,
                                                      howmany=self.num_swap)
        opt_nodes_ids = [node.id for node in to_eject_nodes]

        if self.swap_orchestrator:
            status, content = \
                self.cluster_util.find_orchestrator(self.cluster.master)
            self.assertTrue(status, msg="Unable to find orchestrator: %s:%s"
                                        % (status, content))
            if self.num_swap is len(current_nodes):
                opt_nodes_ids.append(content)
            else:
                opt_nodes_ids[0] = content

        for node in opt_nodes_ids:
            self.log.info("removing node {0} and rebalance afterwards"
                          .format(node))

        new_swap_servers = \
            self.servers[self.nodes_init:self.nodes_init + self.num_swap]
        for server in new_swap_servers:
            otp_node = self.rest.add_node(self.creds.rest_username,
                                          self.creds.rest_password,
                                          server.ip,
                                          server.port)
            msg = "Unable to add node %s to the cluster"
            self.assertTrue(otp_node, msg % server.ip)

        if self.swap_orchestrator:
            self.rest = RestConnection(new_swap_servers[0])
            self.cluster.master = new_swap_servers[0]

        if self.test_abort_snapshot:
            self.log.info("Creating abort scenarios for vbs before rebalance")
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

        self.log.info("Swap Rebalance phase")
        self.rest.rebalance(
            otpNodes=[node.id for node in self.rest.node_statuses()],
            ejectedNodes=opt_nodes_ids)

        if self.test_abort_snapshot:
            self.log.info("Creating abort scenarios during rebalance")
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

        if do_stop_start:
            # Rebalance is stopped at 20%, 40% and 60% completion
            retry = 0
            for expected_progress in (20, 40, 60):
                self.log.info("STOP/START Swap Rebalance phase during %s%%"
                              % expected_progress)
                while True:
                    progress = self.rest._rebalance_progress()
                    if progress < 0:
                        self.log.error("Rebalance progress code : {0}"
                                       .format(progress))
                        break
                    elif progress == 100:
                        self.log.warn("Rebalance has already reached 100%")
                        break
                    elif progress >= expected_progress:
                        self.log.info("Rebalance will be stopped with %s%%"
                                      % progress)
                        stopped = self.rest.stop_rebalance()
                        self.assertTrue(stopped,
                                        msg="Unable to stop rebalance")
                        self.sleep(20)
                        self.rest.rebalance(
                            otpNodes=[node.id
                                      for node in self.rest.node_statuses()],
                            ejectedNodes=opt_nodes_ids)
                        break
                    elif retry > 100:
                        break
                    else:
                        retry += 1
                        self.sleep(1)
        self.assertTrue(self.rest.monitorRebalance(),
                        msg="Rebalance operation failed after adding node {0}"
                        .format(opt_nodes_ids))
        status, _ = self.cluster_util.find_orchestrator(self.cluster.master)
        self.assertTrue(status, msg="Unable to find the cluster orchestrator")

        # Wait till load phase is over
        for task in self.loaders:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                self.loaders, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(self.loaders)
            for task, task_info in self.loaders.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.validate_docs()

    def _common_test_body_failed_swap_rebalance(self):
        # Start the swap rebalance
        retry_exceptions.append(SDKException.TemporaryFailureException)
        self.loaders = super(SwapRebalanceBase, self).loadgen_docs(
            retry_exceptions=retry_exceptions)
        current_nodes = RebalanceHelper.getOtpNodeIds(self.cluster.master)
        self.log.info("current nodes : {0}".format(current_nodes))
        to_eject_nodes = self.cluster_util.pick_nodes(self.cluster.master,
                                                      howmany=self.num_swap)
        opt_nodes_ids = [node.id for node in to_eject_nodes]
        if self.swap_orchestrator:
            status, content = \
                self.cluster_util.find_orchestrator(self.cluster.master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                opt_nodes_ids.append(content)
            else:
                opt_nodes_ids[0] = content

        for node in opt_nodes_ids:
            self.log.info("removing node {0} and rebalance afterwards"
                          .format(node))

        new_swap_servers = self.servers[self.nodes_init:
                                        self.nodes_init+self.num_swap]
        for server in new_swap_servers:
            otp_node = self.rest.add_node(self.creds.rest_username,
                                          self.creds.rest_password,
                                          server.ip,
                                          server.port)
            msg = "Unable to add node %s to the cluster"
            self.assertTrue(otp_node, msg % server.ip)

        if self.swap_orchestrator:
            self.rest = RestConnection(new_swap_servers[0])
            self.cluster.master = new_swap_servers[0]

        self.log.info("SWAP REBALANCE PHASE")
        self.rest.rebalance(
            otpNodes=[node.id for node in self.rest.node_statuses()],
            ejectedNodes=opt_nodes_ids)
        self.sleep(10, "Rebalance should start")
        self.log.info("Fail Swap Rebalance PHASE @ {0}"
                      .format(self.percentage_progress))
        reached = RestHelper(self.rest).rebalance_reached(
            self.percentage_progress)
        if reached and RestHelper(self.rest).is_cluster_rebalanced():
            # handle situation when rebalance failed at the beginning
            self.log.error('seems rebalance failed!')
            self.rest.print_UI_logs()
            self.fail("Rebalance failed even before killing memcached")
        bucket = self.bucket_util.buckets[0]
        pid = None
        if self.swap_orchestrator and not self.cluster_run:
            # get PID via remote connection if master is a new node
            shell = RemoteMachineShellConnection(self.cluster.master)
            pid = shell.get_memcache_pid()
            shell.disconnect()
        else:
            times = 2
            if self.cluster_run:
                times = 20
            for _ in xrange(times):
                try:
                    shell = RemoteMachineShellConnection(self.cluster.master)
                    pid = shell.get_memcache_pid()
                    shell.disconnect()
                    break
                except EOFError as e:
                    self.log.error("{0}.Retry in 2 sec".format(e))
                    self.sleep(2)
        if pid is None:
            self.fail("impossible to get a PID")
        command = "os:cmd(\"kill -9 {0} \")".format(pid)
        self.log.info(command)
        killed = self.rest.diag_eval(command)
        self.log.info("killed {0}:{1}??  {2} "
                      .format(self.cluster.master.ip, self.cluster.master.port,
                              killed))
        self.log.info("sleep for 10 sec after kill memcached")
        self.sleep(10)
        # we can't get stats for new node when rebalance falls
        if not self.swap_orchestrator:
            self.bucket_util._wait_warmup_completed([self.cluster.master],
                                                    bucket, wait_time=600)
        # we expect that rebalance will be failed
        try:
            self.rest.monitorRebalance()
        except RebalanceFailedException:
            # retry rebalance if it failed
            self.log.warn("Rebalance failed but it's expected")
            self.sleep(30)
            self.assertFalse(RestHelper(self.rest).is_cluster_rebalanced(),
                             msg="cluster need rebalance")
            known_nodes = self.rest.node_statuses()
            self.log.info("Nodes are still in cluster: %s"
                          % ([(node.ip, node.port) for node in known_nodes]))
            ejected_nodes = list(set(opt_nodes_ids)
                                 & set([node.id for node in known_nodes]))
            self.rest.rebalance(otpNodes=[node.id for node in known_nodes],
                                ejectedNodes=ejected_nodes)
            self.sleep(10, "Wait for rebalance to start")
            self.assertTrue(self.rest.monitorRebalance(),
                            msg="Rebalance failed after adding node {0}"
                            .format(to_eject_nodes))
        else:
            self.log.info("Rebalance completed successfully")

        # Wait till load phase is over
        for task in self.loaders:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                self.loaders, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(self.loaders)
            for task, task_info in self.loaders.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: %s" % task.thread_name)

        self.validate_docs()

    def _add_back_failed_node(self):
        self.loaders = super(SwapRebalanceBase, self).loadgen_docs(
            retry_exceptions=retry_exceptions)
        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(self.cluster.master)
        self.log.info("current nodes : {0}".format(current_nodes))

        to_eject_nodes = self.cluster_util.pick_nodes(
            self.cluster.master, howmany=self.failover_factor)
        opt_nodes_ids = [node.id for node in to_eject_nodes]
        self.log.info("To be removed nodes: %s" % to_eject_nodes)

        # List of servers that will not be failed over
        not_failed_over = []
        for server in self.cluster.nodes_in_cluster:
            if self.cluster_run:
                if server.port not in [node.port for node in to_eject_nodes]:
                    not_failed_over.append(server)
                    self.log.info("Node {0}:{1} not failed over"
                                  .format(server.ip, server.port))
            else:
                if server.ip not in [node.ip for node in to_eject_nodes]:
                    not_failed_over.append(server)
                    self.log.info("Node {0}:{1} not failed over"
                                  .format(server.ip, server.port))

        if self.fail_orchestrator:
            status, content = \
                self.cluster_util.find_orchestrator(self.cluster.master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                opt_nodes_ids.append(content)
            else:
                opt_nodes_ids[0] = content
                to_eject_nodes[0] = self.cluster.master

        self.log.info("To be removed nodes: {}".format(opt_nodes_ids))
        # Failover selected nodes
        for node in opt_nodes_ids:
            self.log.info("Failover node %s and rebalance afterwards" % node)
            if self.durability_level:
                self.rest.fail_over(node)
            else:
                self.rest.fail_over(node, graceful=True)
            self.assertTrue(self.rest.monitorRebalance(),
                            msg="Rebalance failed after failover of node {0}"
                            .format(node))

        self.rest.rebalance(
            otpNodes=[node.id for node in self.rest.node_statuses()],
            ejectedNodes=opt_nodes_ids)

        self.assertTrue(self.rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(opt_nodes_ids))

        self.cluster.nodes_in_cluster = \
            [node for node in self.cluster.nodes_in_cluster
             if node not in to_eject_nodes]

        # Make rest connection with node part of cluster
        self.rest = RestConnection(self.cluster.nodes_in_cluster[0])
        self.cluster.master = self.cluster.nodes_in_cluster[0]

        for server in to_eject_nodes:
            otp_node = self.rest.add_node(self.creds.rest_username,
                                          self.creds.rest_password,
                                          server.ip, server.port)
            msg = "Unable to add node %s to the cluster"
            self.assertTrue(otp_node, msg % server.ip)

        self.rest.rebalance(
            otpNodes=[node.id for node in self.rest.node_statuses()],
            ejectedNodes=[])

        self.assertTrue(self.rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(to_eject_nodes))

        # Wait till load phase is over
        for task in self.loaders:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                self.loaders, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(self.loaders)
            for task, task_info in self.loaders.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: %s" % task.thread_name)

        self.validate_docs()

    def _failover_swap_rebalance(self):
        self.loaders = super(SwapRebalanceBase, self).loadgen_docs(
            retry_exceptions=retry_exceptions)
        # Start the swap rebalance
        self.log.info("current nodes: %s"
                      % RebalanceHelper.getOtpNodeIds(self.cluster.master))
        to_eject_nodes = self.cluster_util.pick_nodes(
            self.cluster.master, howmany=self.failover_factor)
        opt_nodes_ids = [node.id for node in to_eject_nodes]
        if self.fail_orchestrator:
            status, content = \
                self.cluster_util.find_orchestrator(self.cluster.master)
            self.assertTrue(status, msg="Unable to find orchestrator: %s:%s"
                                        % (status, content))
            opt_nodes_ids[0] = content

        self.log.info("Failover phase")
        # Failover selected nodes
        for node in opt_nodes_ids:
            self.log.info("Failover node %s and rebalance afterwards" % node)
            if self.durability_level:
                self.rest.fail_over(node)
            else:
                self.rest.fail_over(node, graceful=True)
            self.assertTrue(self.rest.monitorRebalance(),
                            msg="Rebalance failed after failover of node {0}"
                            .format(node))

        new_swap_servers = \
            self.servers[self.nodes_init:self.nodes_init+self.failover_factor]
        for server in new_swap_servers:
            otp_node = self.rest.add_node(self.creds.rest_username,
                                          self.creds.rest_password,
                                          server.ip,
                                          server.port)
            msg = "Unable to add node %s to the cluster"
            self.assertTrue(otp_node, msg % server.ip)

        if self.fail_orchestrator:
            self.rest = RestConnection(new_swap_servers[0])
            self.cluster.master = new_swap_servers[0]

        self.rest.rebalance(
            otpNodes=[node.id for node in self.rest.node_statuses()],
            ejectedNodes=opt_nodes_ids)

        self.assertTrue(self.rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(new_swap_servers))

        # Wait till load phase is over
        for task in self.loaders:
            self.task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(
                self.loaders, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(self.loaders)
            for task, task_info in self.loaders.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.validate_docs()


class SwapRebalanceBasicTests(SwapRebalanceBase):
    def setUp(self):
        super(SwapRebalanceBasicTests, self).setUp()

    def tearDown(self):
        super(SwapRebalanceBasicTests, self).tearDown()

    def do_test(self):
        self._common_test_body_swap_rebalance(do_stop_start=False)


class SwapRebalanceStartStopTests(SwapRebalanceBase):
    def setUp(self):
        super(SwapRebalanceStartStopTests, self).setUp()

    def tearDown(self):
        super(SwapRebalanceStartStopTests, self).tearDown()

    def do_test(self):
        self._common_test_body_swap_rebalance(do_stop_start=True)


class SwapRebalanceFailedTests(SwapRebalanceBase):
    def setUp(self):
        super(SwapRebalanceFailedTests, self).setUp()

    def tearDown(self):
        super(SwapRebalanceFailedTests, self).tearDown()

    def test_failed_swap_rebalance(self):
        self._common_test_body_failed_swap_rebalance()

    # Not cluster_run friendly, yet
    def test_add_back_failed_node(self):
        self._add_back_failed_node()

    def test_failover_swap_rebalance(self):
        self._failover_swap_rebalance()
