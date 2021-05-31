import math
import sys

from BucketLib.BucketOperations_Rest import BucketHelper
from collections_helper.collections_spec_constants import MetaCrudParams
from bucket_collections.collections_base import CollectionBase

from couchbase_utils.bucket_utils.bucket_ready_functions import BucketUtils
from membase.api.rest_client import RestConnection, RestHelper
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException


class CollectionsQuorumLoss(CollectionBase):
    def setUp(self):
        super(CollectionsQuorumLoss, self).setUp()
        self.failover_action = self.input.param("failover_action", None)
        self.num_node_failures = self.input.param("num_node_failures", 3)
        self.failover_orchestrator = self.input.param("failover_orchestrator", False)
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]
        self.create_zones = self.input.param("create_zones", False)

    def tearDown(self):
        if self.failover_action:
            self.custom_remove_failure()
        super(CollectionsQuorumLoss, self).tearDown()

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    @staticmethod
    def get_common_spec():
        spec = {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 10,

            MetaCrudParams.SCOPES_TO_DROP: 0,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 0,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 0,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 0,

            # Only dropped scope/collection will be created.
            # While scope recreated all prev collection will also be created
            # In both the collection creation case, previous maxTTL value of
            # individual collection is considered
            MetaCrudParams.SCOPES_TO_RECREATE: 0,
            MetaCrudParams.COLLECTIONS_TO_RECREATE: 10,

            # Applies only for the above listed scope/collection operations
            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {

                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 50,

                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 10,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 5,
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 5,
            },
        }
        return spec

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    @staticmethod
    def set_ignore_exceptions(doc_loading_spec):
        ignore_exceptions = list()
        ignore_exceptions.append(SDKException.DocumentNotFoundException)
        doc_loading_spec[MetaCrudParams.IGNORE_EXCEPTIONS] = ignore_exceptions

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.validate_doc_loading_results(task)
        if task.result is False:
            self.fail("Doc_loading failed")

    def data_validation_collection(self):
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def data_load(self, async_load=True):
        doc_loading_spec = self.get_common_spec()
        self.set_retry_exceptions(doc_loading_spec)
        self.set_ignore_exceptions(doc_loading_spec)
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                        self.bucket_util.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size)
        return tasks

    def servers_to_fail(self):
        """
        Select the nodes to be failed in the tests, and
        update the master, rest object accordingly
        :return: nodes to fail
        """
        if self.failover_orchestrator:
            servers_to_fail = list()
            servers_to_fail.extend(self.nodes_in_cluster[0:self.num_node_failures])
            self.cluster.master = self.master = self.orchestrator = self.cluster.servers[self.num_node_failures]
            self.log.info("changing master to {0}".format(self.cluster.master))
            # Swap first node and last node in the list of current_servers.
            # Because first node is going to get failed - to avoid rest connection
            # to first node in rebalance/failover task
            first_node = self.nodes_in_cluster[0]
            self.nodes_in_cluster[0] = self.nodes_in_cluster[-1]
            self.nodes_in_cluster[-1] = first_node
            self.log.info("also modifying self.nodes.in.cluster to {0} ".format(self.nodes_in_cluster))
        else:
            servers_to_fail = self.nodes_in_cluster[1:self.num_node_failures + 1]
        self.rest = RestConnection(self.cluster.master)
        return servers_to_fail

    def custom_induce_failure(self, nodes=None):
        """
        Induce failure on nodes
        """
        if nodes is None:
            nodes = self.server_to_fail
        for node in nodes:
            if self.failover_action == "stop_server":
                self.cluster_util.stop_server(node)
            elif self.failover_action == "firewall":
                self.cluster_util.start_firewall_on_node(node)
            elif self.failover_action == "stop_memcached":
                self.cluster_util.stop_memcached_on_node(node)
            elif self.failover_action == "kill_erlang":
                remote = RemoteMachineShellConnection(node)
                remote.info = remote.extract_remote_info()
                if remote.info.type.lower() == "windows":
                    remote.kill_erlang(os="windows")
                else:
                    remote.kill_erlang(os="unix")
                remote.disconnect()

    def custom_remove_failure(self, nodes=None):
        """
        Remove failure
        """
        if nodes is None:
            nodes = self.server_to_fail
        for node in nodes:
            if self.failover_action == "stop_server":
                self.cluster_util.start_server(node)
            elif self.failover_action == "firewall":
                self.cluster_util.stop_firewall_on_node(node)
            elif self.failover_action == "stop_memcached":
                self.cluster_util.start_memcached_on_node(node)
            elif self.failover_action == "kill_erlang":
                self.cluster_util.stop_server(node)
                self.cluster_util.start_server(node)

    def shuffle_nodes_between_two_zones(self):
        """
        Creates 'Group 2' zone and shuffles nodes between
        Group 1 and Group 2 in an alternate manner ie;
        1st node in Group 1, 2nd node in Group 2, 3rd node in Group 1 and so on
        and finally rebalances the resulting cluster
        :return: nodes of 2nd zone
        """
        serverinfo = self.cluster.master
        rest = RestConnection(serverinfo)
        zones = ["Group 1", "Group 2"]
        rest.add_zone("Group 2")
        nodes_in_zone = {"Group 1": [serverinfo.ip], "Group 2": []}
        second_zone_servers = list()  # Keep track of second zone's nodes
        # Divide the nodes between zones.
        for i in range(1, len(self.nodes_in_cluster)):
            server_group = i % 2
            nodes_in_zone[zones[server_group]].append(self.nodes_in_cluster[i].ip)
            if zones[server_group] == "Group 2":
                second_zone_servers.append(self.nodes_in_cluster[i])
        # Shuffle the nodes
        node_in_zone = list(set(nodes_in_zone[zones[1]]) -
                            set([node for node in rest.get_nodes_in_zone(zones[1])]))
        rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[1])
        self.task.rebalance(self.nodes_in_cluster, [], [])
        return second_zone_servers

    def wipe_config_on_removed_nodes(self, removed_nodes=None):
        """
        Stop servers on nodes that were failed over and removed, and wipe config dir
        """
        if removed_nodes is None:
            removed_nodes = self.server_to_fail
        for node in removed_nodes:
            self.log.info("Wiping node config and restarting server on {0}".format(node))
            rest = RestConnection(node)
            data_path = rest.get_data_path()
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()
            self.sleep(10)
            shell.cleanup_data_config(data_path)
            shell.start_server()
            self.sleep(10)
            if not RestHelper(rest).is_ns_server_running():
                self.log.error("ns_server {0} is not running.".format(node.ip))
            shell.disconnect()

    def populate_uids(self, base_name="pre_qf"):
        """
        Creates a scope, collection in each bucket and
        returns a dict like:
        {bucket_name:{"uid":uid, "cid":cid, "sid":sid}, ..}
        """
        uids = dict()
        for bucket in self.bucket_util.buckets:
            scope_name = "custom_scope-" + base_name
            collection_name = "custom_collection" + base_name
            BucketUtils.create_scope(self.cluster.master, bucket,
                                     {"name": scope_name})
            BucketUtils.create_collection(self.cluster.master,
                                          bucket,
                                          scope_name,
                                          {"name": collection_name})
            uids[bucket.name] = dict()
            uids[bucket.name]["sid"] = BucketHelper(self.cluster.master).\
                get_scope_id(bucket.name, scope_name)
            uids[bucket.name]["cid"] = BucketHelper(self.cluster.master).\
                get_collection_id(bucket.name, scope_name, collection_name)
            uids[bucket.name]["uid"] = BucketHelper(self.cluster.master).\
                get_bucket_manifest_uid(bucket.name)
        return uids

    def validate_uids(self, pre_qf_ids, post_qf_ids):
        """
        Validates if manifest, scope, coll uids are bumped by 4096
        after qf
        """
        self.log.info("Validating UIDs after QF")
        uid_keys = ["uid", "sid", "cid"]
        for bucket in self.bucket_util.buckets:
            for uid_key in uid_keys:
                int_uid_diff = int(post_qf_ids[bucket.name][uid_key], 16) - \
                               int(pre_qf_ids[bucket.name][uid_key], 16)
                if uid_key == "uid":
                    expected_diff = 4096 + 2
                else:
                    expected_diff = 4096 + 1
                if int_uid_diff != expected_diff:
                    self.fail("For bucket: {0}, uid_key: {1}, "
                              "expected diff: {2} "
                              "but actual diff: {3}".
                              format(bucket.name, uid_key, expected_diff, int_uid_diff))

    def test_quorum_loss_failover(self):
        """
        With constant parallel data load(on docs and collections) do:
        0. Pick majority nodes for failover
        1. Induce failure on step0 nodes and fail over them at once
            OR
            manually failover without inducing failure
        2. Rebalance the cluster
        3. Remove failures if you had added them
        4. Wipe config dir of nodes that were failed over and removed out
        5. Rebalance-in the nodes that were failed over and removed out,
          with parallel data load
        """
        if self.create_zones:
            self.server_to_fail = self.shuffle_nodes_between_two_zones()
        else:
            self.server_to_fail = self.servers_to_fail()

        pre_qf_ids = self.populate_uids(base_name="pre_qf")
        if self.failover_action:
            self.log.info("Inducing failure {0} on nodes: {1}".
                          format(self.failover_action, self.server_to_fail))
            self.custom_induce_failure()
            self.sleep(20, "Wait before failing over")

        self.log.info("Failing over nodes explicitly {0}".format(self.server_to_fail))
        result = self.task.failover(self.nodes_in_cluster, failover_nodes=self.server_to_fail,
                                    graceful=False, wait_for_pending=120,
                                    allow_unsafe=True,
                                    all_at_once=True)
        self.assertTrue(result, "Failover Failed")
        post_qf_ids = self.populate_uids(base_name="post_qf")
        self.validate_uids(pre_qf_ids, post_qf_ids)

        tasks = self.data_load(async_load=True)
        self.log.info("Rebalancing the cluster")
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()
        if self.failover_action:
            self.custom_remove_failure()
            self.sleep(20, "wait after removing failure")

        self.wipe_config_on_removed_nodes()

        tasks = self.data_load(async_load=True)
        self.log.info("Adding back nodes which were failed and removed".
                      format(self.server_to_fail))
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, self.server_to_fail, [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()

    def test_quorum_loss_failover_with_already_failed_over_node(self):
        """
        With constant parallel data load(on docs and collections) do:
        0. Induce failure on one of the nodes and Hard safe failover that node
          but don't rebalance it out yet
        1. Pick majority nodes for failover
        2. Induce failure on step1 nodes
        3. Failover nodes from step 0 and step 1
        4. Rebalance the cluster with data load
        5. Wipe config dir of nodes that were failed over and removed out
        6. Rebalance-in the nodes that were failed over and removed out,
           with parallel data load
        See MB-45110 for more details
        """

        # Hard regular failover the last node with any failure
        failed_over_node = self.nodes_in_cluster[-1]
        self.log.info("Inducing failure {0} on nodes: {1}".
                      format(self.failover_action, failed_over_node))
        self.custom_induce_failure(nodes=[failed_over_node])
        self.sleep(10, "Wait before failing over")
        result = self.task.failover(self.nodes_in_cluster, failover_nodes=[failed_over_node],
                                    graceful=False, wait_for_pending=120)
        self.assertTrue(result, "Failover Failed")

        self.server_to_fail = self.servers_to_fail()

        pre_qf_ids = self.populate_uids(base_name="pre_qf")
        if self.failover_action:
            self.log.info("Inducing failure {0} on nodes: {1}".
                          format(self.failover_action, self.server_to_fail))
            self.custom_induce_failure()
            self.sleep(20, "Wait before failing over")

        self.server_to_fail.append(failed_over_node)
        self.log.info("Failing over nodes explicitly {0}".format(self.server_to_fail))
        result = self.task.failover(self.nodes_in_cluster, failover_nodes=self.server_to_fail,
                                    graceful=False, wait_for_pending=120,
                                    allow_unsafe=True,
                                    all_at_once=True)
        self.assertTrue(result, "Failover Failed")
        post_qf_ids = self.populate_uids(base_name="post_qf")
        self.validate_uids(pre_qf_ids, post_qf_ids)

        tasks = self.data_load(async_load=True)
        self.log.info("Rebalancing the cluster")
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()
        if self.failover_action:
            self.custom_remove_failure()
            self.sleep(20, "wait after removing failure")
        self.wipe_config_on_removed_nodes()

        tasks = self.data_load(async_load=True)
        self.log.info("Adding back nodes which were failed and rebalanced out".
                      format(self.server_to_fail))
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, self.server_to_fail, [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()

    def test_quorum_loss_failover_more_than_failed_nodes(self):
        """
        With constant parallel data load(on docs and collections) do:
        0. Pick majority nodes for failover
        1. Induce failure on step0 nodes
        2. Failover failed nodes + a healthy node (more nodes than failed nodes)
        2. Rebalance the cluster with data load
        4. Wipe config dir of nodes that were failed over and removed out
        5. Rebalance-in the nodes that were failed over and removed out,
           with parallel data load
        """
        if self.create_zones:
            self.server_to_fail = self.shuffle_nodes_between_two_zones()
        else:
            self.server_to_fail = self.servers_to_fail()

        pre_qf_ids = self.populate_uids(base_name="pre_qf")
        if self.failover_action:
            self.log.info("Inducing failure {0} on nodes: {1}".
                          format(self.failover_action, self.server_to_fail))
            self.custom_induce_failure()
            self.sleep(20, "Wait before failing over")

        failover_nodes = [node for node in self.server_to_fail]
        failover_nodes.append(self.nodes_in_cluster[-1])  # healthy node
        result = self.task.failover(self.nodes_in_cluster, failover_nodes=failover_nodes,
                                    graceful=False, wait_for_pending=120,
                                    allow_unsafe=True,
                                    all_at_once=True)
        self.assertTrue(result, "Failover Failed")
        post_qf_ids = self.populate_uids(base_name="post_qf")
        self.validate_uids(pre_qf_ids, post_qf_ids)

        tasks = self.data_load(async_load=True)
        self.log.info("Rebalancing the cluster")
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()
        if self.failover_action:
            self.custom_remove_failure()
            self.sleep(20, "wait after removing failure")

        removed_nodes = failover_nodes
        self.wipe_config_on_removed_nodes(removed_nodes)

        tasks = self.data_load(async_load=True)
        self.log.info("Adding back nodes which were failed and rebalanced out".
                      format(self.server_to_fail))
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, self.server_to_fail, [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()

    def test_multiple_quorum_failovers(self):
        """
        With constant parallel data load(on docs and collections) do:
        0. Pick majority nodes for failover
        1. Induce failure on step0 nodes
        2. Failover failed nodes
        3. Rebalance the cluster with data load
        4. Repeat the above steps on the remaining nodes of the cluster ie;
            qf on the remaining nodes
        5. Wipe config dir of nodes that were failed over and removed out
        6. Rebalance-in the nodes that were failed over and removed out,
           with parallel data load
        """
        self.server_to_fail = list()
        self.num_node_failures = int(math.ceil(len(self.nodes_in_cluster) / 2.0))
        for i in range(2):  # two quorum failovers
            this_step_failed_nodes = self.servers_to_fail()
            pre_qf_ids = self.populate_uids(base_name="pre_qf-"+str(i))
            self.log.info("Inducing failure {0} on nodes: {1}".
                          format(self.failover_action, this_step_failed_nodes))
            self.custom_induce_failure(this_step_failed_nodes)
            self.sleep(20, "Wait before failing over")

            self.log.info("Failing over nodes explicitly {0}".format(this_step_failed_nodes))
            result = self.task.failover(self.nodes_in_cluster, failover_nodes=this_step_failed_nodes,
                                        graceful=False, wait_for_pending=120,
                                        allow_unsafe=True,
                                        all_at_once=True)
            self.assertTrue(result, "Failover Failed")
            post_qf_ids = self.populate_uids(base_name="post_qf-"+str(i))
            self.validate_uids(pre_qf_ids, post_qf_ids)

            tasks = self.data_load(async_load=True)
            self.log.info("Rebalancing the cluster")
            rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, [], [],
                                                       retry_get_process_num=100)
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()

            # update the nodes in cluster
            self.nodes_in_cluster = \
                [node for node in self.nodes_in_cluster if node not in this_step_failed_nodes]
            for node in this_step_failed_nodes:
                self.server_to_fail.append(node)
            self.num_node_failures = int(math.ceil(len(self.nodes_in_cluster) / 2.0))

        self.custom_remove_failure()
        self.sleep(20, "wait after removing failure")
        self.wipe_config_on_removed_nodes()

        tasks = self.data_load(async_load=True)
        self.log.info("Adding back nodes which were failed and removed".
                      format(self.server_to_fail))
        rebalance_task = self.task.async_rebalance(self.nodes_in_cluster, self.server_to_fail, [],
                                                   retry_get_process_num=100)
        self.wait_for_rebalance_to_complete(rebalance_task)
        self.wait_for_async_data_load_to_complete(tasks)
        self.data_validation_collection()

    def test_negative_unsafe_failover_orchestrator(self):
        """
        Negative case: unsafe hard failover of healthy orchestrator node
        """
        self.failover_orchestrator = True
        self.server_to_fail = self.servers_to_fail()
        self.rest = RestConnection(self.cluster.master)
        self.log.info("Failing over nodes {0}".format(self.server_to_fail))
        otp_nodes = list()
        for node in self.server_to_fail:
            ip = "ns_1@" + node.ip
            otp_nodes.append(ip)
        try:
            self.rest.fail_over(otp_nodes, graceful=False, allowUnsafe=True, all_at_once=True)
        except:
            self.log.info("Failed as expected: {0} {1}"
                          .format(sys.exc_info()[0], sys.exc_info()[1]))
            # TODO add code to verify the error response after MB-45086 is resolved
        else:
            self.wipe_config_on_removed_nodes()
            self.fail("Unsafe failover of healthy orchestrator node did not fail")

    def test_negative_add_back_node_without_wiping(self):
        """
        0. Pick majority nodes for failover
        1. Induce failure on step0 nodes
        2. Failover failed nodes
        3. Rebalance the cluster
        4. Verify add back of failed-over nodes fails
        5. Wipe off nodes
        """
        self.server_to_fail = self.servers_to_fail()
        self.custom_induce_failure()
        self.sleep(20, "Wait before failing over")

        self.log.info("Failing over nodes explicitly {0}".format(self.server_to_fail))
        result = self.task.failover(self.nodes_in_cluster, failover_nodes=self.server_to_fail,
                                    graceful=False, wait_for_pending=120,
                                    allow_unsafe=True,
                                    all_at_once=True)
        self.assertTrue(result, "Failover Failed")

        self.log.info("Rebalancing the cluster")
        self.task.rebalance(self.nodes_in_cluster, [], [])
        self.custom_remove_failure()
        self.sleep(20, "wait after removing failure")

        self.log.info("Adding back nodes which were failed and removed".
                      format(self.server_to_fail))
        self.rest = RestConnection(self.cluster.master)
        for node in self.server_to_fail:
            try:
                self.rest.add_node(user=self.cluster.master.rest_username,
                                   password=self.cluster.master.rest_password,
                                   remoteIp=node.ip)
            except:
                self.log.info("Failed as expected: {0} {1}"
                              .format(sys.exc_info()[0], sys.exc_info()[1]))
            else:
                self.fail("Adding back nodes without wiping did not fail")
        self.wipe_config_on_removed_nodes()
