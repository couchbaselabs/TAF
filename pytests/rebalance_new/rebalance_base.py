import json

from basetestcase import BaseTestCase
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException


class RebalanceBaseTest(BaseTestCase):
    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.doc_ops = self.input.param("doc_ops", "create")
        self.key_size = self.input.param("key_size", 0)
        self.zone = self.input.param("zone", 1)
        self.replica_to_update = self.input.param("new_replica", None)
        self.default_view_name = "default_view"
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.max_verify = self.input.param("max_verify", None)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        self.key = 'test_docs'.rjust(self.key_size, '0')
        self.flusher_batch_split_trigger = self.input.param("flusher_batch_split_trigger", None)
        self.items = self.num_items
        node_ram_ratio = self.bucket_util.base_bucket_ratio(self.cluster.servers)
        info = self.rest.get_nodes_self()
        self.rest.init_cluster(username=self.cluster.master.rest_username,
                               password=self.cluster.master.rest_password)
        self.rest.init_cluster_memoryQuota(memoryQuota=int(info.mcdMemoryReserved*node_ram_ratio))
        self.check_temporary_failure_exception = False
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance([self.cluster.master], nodes_init, [])
            self.assertTrue(result, "Initial rebalance failed")
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.check_replica = self.input.param("check_replica", False)

        self.bucket_util.add_rbac_user()
        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.standard_buckets)
        self.create_buckets()
        if self.magma_storage:
            self.bucket_util.update_bucket_props("backend", "magma")
        self.sleep(20)

        if self.flusher_batch_split_trigger:
            self.bucket_util.set_flusher_batch_split_trigger(self.cluster.master,
                                                             self.flusher_batch_split_trigger,
                                                             self.bucket_util.buckets)

        self.gen_create = self.get_doc_generator(0, self.num_items)
        if self.active_resident_threshold < 100:
            self.check_temporary_failure_exception = True
        if not self.atomicity:
            tasks_info = self._load_all_buckets(self.cluster, self.gen_create, "create", 0)
            self.log.info("Verifying num_items counts after doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
        else:
            self.transaction_commit = True
            self._load_all_buckets_atomicty(self.gen_create, "create")
            self.transaction_commit = self.input.param("transaction_commit", True)

        # Initialize doc_generators
        self.active_resident_threshold = 100
        self.gen_create = None
        self.gen_delete = None
        self.gen_update = self.get_doc_generator(0, (self.items / 2))
        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to, persist_to=self.persist_to)
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.log.info("==========Finished rebalance base setup========")

    def _create_default_bucket(self):
        node_ram_ratio = self.bucket_util.base_bucket_ratio(self.servers)
        info = RestConnection(self.cluster.master).get_nodes_self()
        available_ram = int(info.memoryQuota * node_ram_ratio)
        if available_ram < 100 or self.active_resident_threshold < 100:
            available_ram = 100
        self.bucket_util.create_default_bucket(
            ram_quota=available_ram,
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            eviction_policy=self.bucket_eviction_policy)

    def _create_multiple_buckets(self):
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster.master,
            self.num_replicas,
            bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type,
            eviction_policy=self.bucket_eviction_policy)
        self.assertTrue(buckets_created, "Unable to create multiple buckets")

        for bucket in self.bucket_util.buckets:
            ready = self.bucket_util.wait_for_memcached(
                self.cluster.master,
                bucket)
            self.assertTrue(ready, msg="Wait_for_memcached failed")

    def create_buckets(self):
        if self.standard_buckets == 1:
            self._create_default_bucket()
        else:
            self._create_multiple_buckets()

    def tearDown(self):
        self.cluster_util.print_cluster_stats()
        super(RebalanceBaseTest, self).tearDown()

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1.
        Rebalance the nodes in the end.
        Nodes are divided into groups iteratively. i.e: 1st node in Group 1,
        2nd in Group 2, 3rd in Group 1 & so on, when zone=2
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        serverinfo = self.servers[0]
        rest = RestConnection(serverinfo)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [serverinfo.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                if not rest.is_zone_exist(zones[i]):
                    rest.add_zone(zones[i])
                nodes_in_zone[zones[i]] = []
            # Divide the nodes between zones.
            nodes_in_cluster = [node.ip for node in self.cluster_util.get_nodes_in_cluster()]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.servers)):
                if self.servers[i].ip in nodes_in_cluster and self.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(self.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses()
                           if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes, ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Verify replicas of one node should not be in the same zone as active vbuckets of the node.
        if self.zone > 1:
            self.cluster_util.verify_replica_distribution_in_zones(nodes_in_zone)

    def add_remove_servers_and_rebalance(self, to_add, to_remove):
        """
        Add and/or remove servers and rebalance.
        :param to_add: List of nodes to be added.
        :param to_remove: List of nodes to be removed.
        """
        serverinfo = self.cluster.master
        rest = RestConnection(serverinfo)
        for node in to_add:
            rest.add_node(user=serverinfo.rest_username,
                          password=serverinfo.rest_password,
                          remoteIp=node.ip)
        self.shuffle_nodes_between_zones_and_rebalance(to_remove)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster + to_add) - set(to_remove))

    def get_doc_generator(self, start, end):
        return doc_generator(self.key, start, end, doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.vbuckets)

    def _load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, compression=True):

        retry_exceptions = list([SDKException.RequestTimeoutException,
                                 SDKException.RequestCanceledException,
                                 SDKException.DurabilityImpossibleException,
                                 SDKException.DurabilityAmbiguousException])

        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, sdk_compression=compression,
            process_concurrency=8, retry_exceptions=retry_exceptions,
            active_resident_threshold=self.active_resident_threshold)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                self.num_items = task.doc_index
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in rebalance_base._load_all_buckets")
        return tasks_info

    def _load_all_buckets_atomicty(self, kv_gen, op_type):
        task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets, kv_gen, op_type, 0,
                    batch_size=10, process_concurrency=8, replicate_to=self.replicate_to,
                    persist_to=self.persist_to, timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                    transaction_timeout=self.transaction_timeout, commit=self.transaction_commit,
                    durability=self.durability_level, sync=self.sync)
        self.task.jython_task_manager.get_task_result(task)

    def start_parallel_cruds_atomicity(self, sync=True, task_verification=True):
        tasks_info = dict()
        if("update" in self.doc_ops):
            tasks_info.update({self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, self.gen_update,
                         "rebalance_only_update", 0, batch_size=20, process_concurrency=self.process_concurrency,
                          replicate_to=self.replicate_to, persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          update_count=self.update_count, commit=self.transaction_commit,
                          durability=self.durability_level, sync=sync, defer=self.defer):None})
        if("create" in self.doc_ops):
            tasks_info.update({self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, self.gen_create,
                         "create", 0, batch_size=20, process_concurrency=self.process_concurrency,
                          replicate_to=self.replicate_to, persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          commit=self.transaction_commit, durability=self.durability_level,
                          sync=sync, defer=self.defer):None})
        if("delete" in self.doc_ops):
            tasks_info.update({self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, self.gen_delete,
                         "rebalance_delete", 0, batch_size=20, process_concurrency=self.process_concurrency,
                          replicate_to=self.replicate_to, persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          commit=self.transaction_commit, durability=self.durability_level,
                          sync=sync, defer=self.defer):None})

        if task_verification:
            for task in tasks_info.keys():
                self.task.jython_task_manager.get_task_result(task)

        return tasks_info

    def start_parallel_cruds(self, retry_exceptions=[], ignore_exceptions=[],
                             task_verification=False):
        tasks_info = dict()
        if "update" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "create" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "delete" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if task_verification:
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        return tasks_info

    def loadgen_docs(self,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     task_verification=False):
        loaders = []
        retry_exceptions = list(set(retry_exceptions +
                                    [SDKException.RequestTimeoutException,
                                     SDKException.RequestCanceledException,
                                     SDKException.DurabilityImpossibleException,
                                     SDKException.DurabilityAmbiguousException]))
        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)
        if self.atomicity:
            loaders = self.start_parallel_cruds_atomicity(self.sync, task_verification)
        else:
            loaders = self.start_parallel_cruds(retry_exceptions, ignore_exceptions,
                                                task_verification)
        return loaders

    def induce_rebalance_test_condition(self, test_failure_condition):
        if test_failure_condition == "verify_replication":
            set_command = "testconditions:set(verify_replication, {fail, \"" + "default" + "\"})"
        elif test_failure_condition == "backfill_done":
            set_command = "testconditions:set(backfill_done, {for_vb_move, \"" + "default\", 1 , " + "fail})"
        else:
            set_command = "testconditions:set({0}, fail)".format(test_failure_condition)
        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(set_command)
            self.log.debug("Set Command : {0} Return : {1}".format(set_command, content))
            shell.disconnect()

    def start_rebalance(self, rebalance_operation):
        self.log.debug("Starting rebalance operation of type : {0}".format(rebalance_operation))
        if rebalance_operation == "rebalance_out":
            task = self.task.async_rebalance(self.servers[:self.nodes_init], [], [self.servers[self.nodes_init - 1]])
        elif rebalance_operation == "rebalance_in":
            task = self.task.async_rebalance(self.servers[:self.nodes_init],
                                             [self.servers[self.nodes_init]], [])
        elif rebalance_operation == "swap_rebalance":
            self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                               self.servers[self.nodes_init].ip, self.servers[self.nodes_init].port)
            task = self.task.async_rebalance(self.servers[:self.nodes_init], []
                                             , [self.servers[self.nodes_init - 1]])
        elif rebalance_operation == "graceful_failover":
            task = self.task.async_failover([self.cluster.master], failover_nodes=[self.servers[1]],
                                            graceful=True, wait_for_pending=120)
        return task

    def delete_rebalance_test_condition(self, test_failure_condition):
        delete_command = "testconditions:delete({0})".format(test_failure_condition)
        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(delete_command)
            self.log.debug("Delete Command : {0} Return : {1}".format(delete_command, content))
            shell.disconnect()

    def check_retry_rebalance_succeeded(self):
        result = json.loads(self.rest.get_pending_rebalance_info())
        self.log.debug("Result from get_pending_rebalance_info : {0}".format(result))
        retry_after_secs = result["retry_after_secs"]
        attempts_remaining = result["attempts_remaining"]
        retry_rebalance = result["retry_rebalance"]
        self.log.debug("Attempts remaining : {0}, Retry rebalance : {1}".format(attempts_remaining, retry_rebalance))
        while attempts_remaining:
            # wait for the afterTimePeriod for the failed rebalance to restart
            self.sleep(retry_after_secs, message="Waiting for the afterTimePeriod to complete")
            try:
                result = self.rest.monitorRebalance()
                msg = "monitoring rebalance {0}"
                self.log.debug(msg.format(result))
                self.assertTrue(result, "Retried rebalance did not succeed")
            except Exception:
                result = json.loads(self.rest.get_pending_rebalance_info())
                self.log.debug(result)
                try:
                    attempts_remaining = result["attempts_remaining"]
                    retry_rebalance = result["retry_rebalance"]
                    retry_after_secs = result["retry_after_secs"]
                except KeyError:
                    self.fail("Retrying of rebalance still did not help. All the retries exhausted...")
                self.log.debug("Attempts remaining : {0}, Retry rebalance : {1}".format(attempts_remaining,
                                                                                        retry_rebalance))
            else:
                self.log.info("Retry rebalanced fixed the rebalance failure")
                break

    def change_retry_rebalance_settings(self, enabled=True, afterTimePeriod=300, maxAttempts=1):
        # build the body
        body = dict()
        if enabled:
            body["enabled"] = "true"
        else:
            body["enabled"] = "false"
        body["afterTimePeriod"] = afterTimePeriod
        body["maxAttempts"] = maxAttempts
        rest = RestConnection(self.cluster.master)
        rest.set_retry_rebalance_settings(body)
        result = rest.get_retry_rebalance_settings()
        self.log.debug("Retry Rebalance settings changed to : {0}".format(json.loads(result)))

    def reset_retry_rebalance_settings(self):
        body = dict()
        body["enabled"] = "false"
        rest = RestConnection(self.cluster.master)
        rest.set_retry_rebalance_settings(body)
        self.log.debug("Retry Rebalance settings reset ....")
