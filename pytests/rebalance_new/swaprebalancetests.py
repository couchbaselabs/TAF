import time
import datetime
from math import floor

from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper
from membase.api.exception import RebalanceFailedException
from remote.remote_util import RemoteMachineShellConnection
from BucketLib.BucketOperations import BucketHelper
from java.util.concurrent import ExecutionException


class SwapRebalanceBase(BaseTestCase):
    def setUp(self):
        super(SwapRebalanceBase, self).setUp()
        self.cluster_run = False
        rest = RestConnection(self.cluster.master)
        if len(set([server.ip for server in self.servers])) == 1:
            ip = rest.get_nodes_self().ip
            for server in self.servers:
                server.ip = ip
            self.cluster_run = True
        self.failover_factor = self.num_swap = self.input.param("num-swap", 1)
        self.num_initial_servers = self.input.param("num-initial-servers", 3)
        self.fail_orchestrator = self.swap_orchestrator = self.input.param("swap-orchestrator", False)
        self.do_access = self.input.param("do-access", True)
        self.percentage_progress = self.input.param("percentage_progress", 50)
        self.load_started = False
        self.loaders = []
        self.replica_to_update = self.input.param("new_replica", None)
        try:
            # Clear the state from Previous invalid run
            if rest._rebalance_progress_status() == 'running':
                self.log.warning("Rebalance is still running, previous test should be verified")
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
            self.log.info("=== SwapRebalanceBase setup started for test #{0} {1} ==="
                          .format(self.case_number, self._testMethodName))
            # Make sure the test is setup correctly
            min_servers = int(self.num_initial_servers) + int(self.num_swap)
            msg = "minimum {0} nodes required for running swap rebalance"
            self.assertTrue(len(self.servers) >= min_servers,
                            msg=msg.format(min_servers))
            self.log.info('picking server : {0} as the master'
                          .format(self.cluster.master))
            node_ram_ratio = self.bucket_util.base_bucket_ratio(self.cluster.servers)
            info = rest.get_nodes_self()
            rest.init_cluster(username=self.cluster.master.rest_username,
                              password=self.cluster.master.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved*node_ram_ratio))
            self.enable_diag_eval_on_non_local_hosts(self.cluster.master)
            self.bucket_util.add_rbac_user()
            time.sleep(10)

            if self.standard_buckets > 10:
                self.bucket_util.change_max_buckets(self.standard_buckets)
            self.log.info("=== SwapRebalanceBase setup finished for test #{0} {1} ==="
                          .format(self.case_number, self._testMethodName))
            self._log_start()
        except Exception, e:
            self.fail(e)

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

    def _log_start(self):
        try:
            msg = "{0} : {1} started " \
                  .format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except Exception:
            pass

    def _log_finish(self):
        try:
            msg = "{0} : {1} finished " \
                  .format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except Exception:
            pass

    def _create_default_bucket(self):
        master = self.cluster.master
        node_ram_ratio = self.bucket_util.base_bucket_ratio(self.servers)
        info = RestConnection(master).get_nodes_self()
        available_ram = int(info.memoryQuota * node_ram_ratio)
        if available_ram < 100:
            available_ram = 100
        self.bucket_util.create_default_bucket(ram_quota=available_ram,
                                               replica=self.num_replicas)

    def _create_multiple_buckets(self):
        master = self.cluster.master
        buckets_created = self.bucket_util.create_multiple_buckets(
            master, self.num_replicas, bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type)
        self.assertTrue(buckets_created, "unable to create multiple buckets")

        for bucket in self.bucket_util.buckets:
            ready = self.bucket_util.wait_for_memcached(master, bucket)
            self.assertTrue(ready, msg="wait_for_memcached failed")

    # Used for items verification active vs. replica
    def items_verification(self, test, master):
        # Verify items count across all node
        timeout = 600
        for bucket in self.bucket_util.buckets:
            verified = self.bucket_util.wait_till_total_numbers_match(
                master, bucket, timeout_in_seconds=timeout)
            test.assertTrue(verified, "Lost items!!.. failing test in {0} secs"
                                      .format(timeout))

    def start_load_phase_atomicity(self): 
        gen_create = doc_generator('test_docs', 0, self.num_items)
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             gen_create, "create" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries, transaction_timeout=self.transaction_timeout, commit=self.transaction_commit)
        return task

    def start_access_phase_atomicity(self):
        gen_create = doc_generator('test_docs', 0, self.num_items)
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             gen_create, "validate" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries, transaction_timeout=self.transaction_timeout, commit=self.transaction_commit)
        return task

    def stop_load_atomicity(self, loaders, do_stop=True):
        if do_stop:
            self.task.jython_task_manager.stop_task(loaders)
        try:
            self.task.jython_task_manager.get_task_result(loaders)
        except ExecutionException:
            pass

    def start_load_phase(self):
        loaders = []
        gen_create = doc_generator('test_docs', 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            loaders.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", 0,
                batch_size=20, persist_to=self.persist_to,
                replicate_to=self.replicate_to, pause_secs=5,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        return loaders

    def start_access_phase(self):
        loaders = []
        gen_create = doc_generator('test_docs', 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            loaders.append(self.task.async_validate_docs(
                self.cluster, bucket, gen_create, "create", 0,
                batch_size=10, process_concurrency=8))
        return loaders

    def stop_load(self, loaders, do_stop=True):
        if do_stop:
            for loader in loaders:
                self.task.jython_task_manager.stop_task(loader)
        for loader in loaders:
            try:
                self.task.jython_task_manager.get_task_result(loader)
#                 for client in loader.clients:
#                     client.close()
            except ExecutionException:
                pass

    def create_buckets(self):
        if self.standard_buckets == 1:
            self._create_default_bucket()
        else:
            self._create_multiple_buckets()

    def verification_phase(self):
        # Stop loaders
        self.stop_load(self.loaders, do_stop=False)
        for loader in self.loaders:
            loader.client.close()
        self.log.info("DONE DATA ACCESS PHASE")

        self.log.info("VERIFICATION PHASE")
        rest = RestConnection(self.cluster.master)
        servers_in_cluster = []
        nodes = rest.get_nodes()
        for server in self.cluster.servers:
            for node in nodes:
                if node.ip == server.ip and node.port == server.port:
                    servers_in_cluster.append(server)
        # TODO: Need to write wait_for_replication to use different
        # verification strategy, since it was removed because for using 'tap'
        # RebalanceHelper.wait_for_replication(servers_in_cluster,
        #                                      self.bucket_util, self.task)
        if not self.atomicity:
            self.items_verification(self, self.cluster.master)

    def _common_test_body_swap_rebalance(self, do_stop_start=False):
        master = self.cluster.master
        rest = RestConnection(master)
        num_initial_servers = self.num_initial_servers
        creds = self.input.membase_settings
        intial_severs = self.servers[1:num_initial_servers]

        # Cluster all starting set of servers
        self.log.info("INITIAL REBALANCE PHASE")
        status = self.task.rebalance(self.cluster.servers[:self.nodes_init],
                                     intial_severs, [])
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("CREATE BUCKET PHASE")
        self.create_buckets()

        self.log.info("DATA LOAD PHASE")
        if self.atomicity:
            self.loaders = self.start_load_phase_atomicity()
        else:
            self.loaders = self.start_load_phase()

        # Wait till load phase is over
        if self.atomicity:
            self.stop_load_atomicity(self.loaders, do_stop=False)
        else:
            self.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0].name, replicaNumber=self.replica_to_update)
            self.bucket_util.buckets[0].replicaNumber = self.replica_to_update

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = self.cluster_util.pick_nodes(master,
                                                      howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        if self.swap_orchestrator:
            status, content = self.cluster_util.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content

        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards"
                          .format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers + self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        if self.do_access:
            self.log.info("DATA ACCESS PHASE")
            if self.atomicity:
                self.loaders = self.start_access_phase_atomicity()
            else:
                self.loaders = self.start_access_phase()

        self.log.info("SWAP REBALANCE PHASE")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=optNodesIds)

        if do_stop_start:
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
                        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                                       ejectedNodes=optNodesIds)
                        break
                    elif retry > 100:
                        break
                    else:
                        retry += 1
                        self.sleep(1)
        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(optNodesIds))
        self.cluster.update_master(master)
        self.verification_phase()

    def _common_test_body_failed_swap_rebalance(self):
        master = self.servers[0]
        rest = RestConnection(master)
        num_initial_servers = self.num_initial_servers
        creds = self.input.membase_settings
        intial_severs = self.servers[:num_initial_servers]

        self.log.info("CREATE BUCKET PHASE")
        self.create_buckets()

        # Cluster all starting set of servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, _ = RebalanceHelper.rebalance_in(intial_severs,
                                                 len(intial_severs)-1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        if self.atomicity:
            self.loaders = self.start_load_phase_atomicity()
        else:
            self.loaders = self.start_load_phase()

        # Wait till load phase is over
        if self.atomicity:
            self.stop_load_atomicity(self.loaders, do_stop=False)
        else:
            self.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0].name, replicaNumber=self.replica_to_update)
            self.bucket_util.buckets[0].replicaNumber = self.replica_to_update

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = self.cluster_util.pick_nodes(master,
                                                      howmany=self.num_swap)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.swap_orchestrator:
            status, content = self.cluster_util.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content

        for node in optNodesIds:
            self.log.info("removing node {0} and rebalance afterwards"
                          .format(node))

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers+self.num_swap]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.swap_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        self.log.info("DATA ACCESS PHASE")
        if self.atomicity:
                self.loaders = self.start_access_phase_atomicity()
        else:
            self.loaders = self.start_access_phase()

        self.log.info("SWAP REBALANCE PHASE")
        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=optNodesIds)
        self.sleep(10, "Rebalance should start")
        self.log.info("FAIL SWAP REBALANCE PHASE @ {0}"
                      .format(self.percentage_progress))
        reached = RestHelper(rest).rebalance_reached(self.percentage_progress)
        if reached and RestHelper(rest).is_cluster_rebalanced():
            # handle situation when rebalance failed at the beginning
            self.log.error('seems rebalance failed!')
            rest.print_UI_logs()
            self.fail("rebalance failed even before killing memcached")
        bucket = self.bucket_util.buckets[0]
        pid = None
        if self.swap_orchestrator and not self.cluster_run:
            # get PID via remote connection if master is a new node
            shell = RemoteMachineShellConnection(master)
            pid = shell.get_memcache_pid()
            shell.disconnect()
        else:
            times = 2
            if self.cluster_run:
                times = 20
            for _ in xrange(times):
                try:
                    shell = RemoteMachineShellConnection(master)
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
        killed = rest.diag_eval(command)
        self.log.info("killed {0}:{1}??  {2} "
                      .format(master.ip, master.port, killed))
        self.log.info("sleep for 10 sec after kill memcached")
        self.sleep(10)
        # we can't get stats for new node when rebalance falls
        if not self.swap_orchestrator:
            self.bucket_util._wait_warmup_completed([master], bucket,
                                                    wait_time=600)
        # we expect that rebalance will be failed
        try:
            rest.monitorRebalance()
        except RebalanceFailedException:
            # retry rebalance if it failed
            self.log.warn("Rebalance failed but it's expected")
            self.sleep(30)
            self.assertFalse(RestHelper(rest).is_cluster_rebalanced(),
                             msg="cluster need rebalance")
            knownNodes = rest.node_statuses()
            self.log.info("nodes are still in cluster: {0}"
                          .format([(node.ip, node.port) for node in knownNodes]))
            ejectedNodes = list(set(optNodesIds) & set([node.id for node in knownNodes]))
            rest.rebalance(otpNodes=[node.id for node in knownNodes],
                           ejectedNodes=ejectedNodes)
            self.assertTrue(rest.monitorRebalance(),
                            msg="Rebalance failed after adding node {0}"
                            .format(toBeEjectedNodes))
        else:
            self.log.info("rebalance completed successfully")
        self.cluster.update_master(master)
        self.verification_phase()

    def _add_back_failed_node(self, do_node_cleanup=False):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings

        self.log.info("CREATE BUCKET PHASE")
        self.create_buckets()

        # Cluster all servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, _ = RebalanceHelper.rebalance_in(self.servers,
                                                 len(self.servers)-1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        if self.atomicity:
            self.loaders = self.start_load_phase_atomicity()
        else:
            self.loaders = self.start_load_phase()

        # Wait till load phase is over
        if self.atomicity:
            self.stop_load_atomicity(self.loaders, do_stop=False)
        else:
            self.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0].name, replicaNumber=self.replica_to_update)
            self.bucket_util.buckets[0].replicaNumber = self.replica_to_update

        # Start the swap rebalance
        current_nodes = RebalanceHelper.getOtpNodeIds(master)
        self.log.info("current nodes : {0}".format(current_nodes))
        toBeEjectedNodes = self.cluster_util.pick_nodes(
            master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]

        # List of servers that will not be failed over
        not_failed_over = []
        for server in self.servers:
            if self.cluster_run:
                if server.port not in [node.port for node in toBeEjectedNodes]:
                    not_failed_over.append(server)
                    self.log.info("Node {0}:{1} not failed over"
                                  .format(server.ip, server.port))
            else:
                if server.ip not in [node.ip for node in toBeEjectedNodes]:
                    not_failed_over.append(server)
                    self.log.info("Node {0}:{1} not failed over"
                                  .format(server.ip, server.port))

        if self.fail_orchestrator:
            status, content = self.cluster_util.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            # When swapping all the nodes
            if self.num_swap is len(current_nodes):
                optNodesIds.append(content)
            else:
                optNodesIds[0] = content
            master = not_failed_over[-1]

        self.log.info("DATA ACCESS PHASE")
        if self.atomicity:
                self.loaders = self.start_access_phase_atomicity()
        else:
            self.loaders = self.start_access_phase()

        # Failover selected nodes
        for node in optNodesIds:
            self.log.info("failover node {0} and rebalance afterwards"
                          .format(node))
            rest.fail_over(node)

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(optNodesIds))

        # Add back the same failed over nodes

        # Cleanup the node, somehow
        # TODO: cluster_run?
        if do_node_cleanup:
            pass

        # Make rest connection with node part of cluster
        rest = RestConnection(master)

        # Given the optNode, find ip
        add_back_servers = []
        nodes = rest.get_nodes()
        for server in nodes:
            if isinstance(server.ip, unicode):
                add_back_servers.append(server)
        final_add_back_servers = []
        for server in self.servers:
            if self.cluster_run:
                if server.port not in [serv.port for serv in add_back_servers]:
                    final_add_back_servers.append(server)
            else:
                if server.ip not in [serv.ip for serv in add_back_servers]:
                    final_add_back_servers.append(server)
        for server in final_add_back_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=[])

        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(add_back_servers))
        self.cluster.update_master(master)
        self.verification_phase()

    def _failover_swap_rebalance(self):
        master = self.servers[0]
        rest = RestConnection(master)
        creds = self.input.membase_settings
        num_initial_servers = self.num_initial_servers
        intial_severs = self.servers[:num_initial_servers]

        self.log.info("CREATE BUCKET PHASE")
        self.create_buckets()

        # Cluster all starting set of servers
        self.log.info("INITIAL REBALANCE PHASE")
        status, _ = RebalanceHelper.rebalance_in(intial_severs,
                                                 len(intial_severs)-1)
        self.assertTrue(status, msg="Rebalance was failed")

        self.log.info("DATA LOAD PHASE")
        if self.atomicity:
            self.loaders = self.start_load_phase_atomicity()
        else:
            self.loaders = self.start_load_phase()

        # Wait till load phase is over
        if self.atomicity:
            self.stop_load_atomicity(self.loaders, do_stop=False)
        else:
            self.stop_load(self.loaders, do_stop=False)
        self.log.info("DONE LOAD PHASE")

        # Update replica value before performing rebalance in/out
        if self.replica_to_update:
            bucket_helper = BucketHelper(self.cluster.master)

            # Update bucket replica to new value as given in conf file
            self.log.info("Updating replica count of bucket to {0}"
                          .format(self.replica_to_update))
            bucket_helper.change_bucket_props(
                self.bucket_util.buckets[0].name, replicaNumber=self.replica_to_update)
            self.bucket_util.buckets[0].replicaNumber = self.replica_to_update

        # Start the swap rebalance
        self.log.info("current nodes : {0}"
                      .format(RebalanceHelper.getOtpNodeIds(master)))
        toBeEjectedNodes = self.cluster_util.pick_nodes(
            master, howmany=self.failover_factor)
        optNodesIds = [node.id for node in toBeEjectedNodes]
        if self.fail_orchestrator:
            status, content = self.cluster_util.find_orchestrator(master)
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}"
                            .format(status, content))
            optNodesIds[0] = content

        self.log.info("FAILOVER PHASE")
        # Failover selected nodes
        for node in optNodesIds:
            self.log.info("failover node {0} and rebalance afterwards"
                          .format(node))
            rest.fail_over(node)

        new_swap_servers = self.servers[num_initial_servers:num_initial_servers+self.failover_factor]
        for server in new_swap_servers:
            otpNode = rest.add_node(creds.rest_username, creds.rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip))

        if self.fail_orchestrator:
            rest = RestConnection(new_swap_servers[0])
            master = new_swap_servers[0]

        self.log.info("DATA ACCESS PHASE")
        if self.atomicity:
                self.loaders = self.start_access_phase_atomicity()
        else:
            self.loaders = self.start_access_phase()

        rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                       ejectedNodes=optNodesIds)

        self.assertTrue(rest.monitorRebalance(),
                        msg="rebalance operation failed after adding node {0}"
                        .format(new_swap_servers))
        self.cluster.update_master(master)
        self.verification_phase()


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
        self._add_back_failed_node(do_node_cleanup=False)

    def test_failover_swap_rebalance(self):
        self._failover_swap_rebalance()
