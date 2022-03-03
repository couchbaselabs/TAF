'''
Created on 11-October-2021
@author: umang.agrawal
'''

import random
from cbas.cbas_base import CBASBaseTest
from security_utils.security_utils import SecurityUtils
from Cb_constants import CbServer
from cbas_utils.cbas_utils import CBASRebalanceUtil, FlushToDiskTask
import copy
from rbac_utils.Rbac_ready_functions import RbacUtils
from remote.remote_util import RemoteMachineShellConnection
from SystemEventLogLib.analytics_events import AnalyticsEvents
from security_config import trust_all_certs
from membase.api.rest_client import RestConnection, RestHelper

rbac_users_created = {}

class CBASHighAvailability(CBASBaseTest):

    def setUp(self):

        super(CBASHighAvailability, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.security_util = SecurityUtils(self.log)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            vbucket_check=True, cbas_util=self.cbas_util)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        for server in self.servers:
            self.start_server(server)
        super(CBASHighAvailability, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_for_test(self, update_spec={}, wait_for_ingestion=True):
        if not update_spec:
            update_spec = {
                "no_of_dataverses": self.input.param('no_of_dv', 1),
                "no_of_datasets_per_dataverse": self.input.param('ds_per_dv',
                                                                 1),
                "no_of_synonyms": 0,
                "no_of_indexes": self.input.param('no_of_idx', 1),
                "max_thread_count": self.input.param('no_of_threads', 1),
                "dataset": {
                    "creation_methods": ["cbas_collection", "cbas_dataset"]}}
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            if update_spec:
                self.cbas_util.update_cbas_spec(self.cbas_spec, update_spec)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=wait_for_ingestion)
            if not cbas_infra_result[0]:
                self.fail(
                    "Error while creating infra from CBAS spec -- " +
                    cbas_infra_result[1])
        self.replica_num = self.input.param('replica_num', 0)
        set_result = self.cbas_util.set_replica_number_from_settings(
            self.cluster.master, replica_num=self.replica_num)
        if set_result != self.replica_num:
            self.fail("Error while setting replica for CBAS")

        self.log.info("Rebalancing for CBAS replica setting change to take "
                      "effect.")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
            cbas_nodes_out=0, available_servers=self.available_servers, exclude_nodes=[])
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
            rebalance_task, self.cluster):
            self.fail("Rebalance failed")

    def connect_disconnect_all_local_links(self, disconnect=False):
        for dv in self.cbas_util.dataverses.values():
            if disconnect:
                if not self.cbas_util.disconnect_link(
                        self.cluster, link_name=dv.name + ".Local"):
                    self.fail("Failed to disconnect link - {0}".format(
                        dv.name + ".Local"))
            else:
                if not self.cbas_util.connect_link(
                        self.cluster, link_name=dv.name + ".Local"):
                    self.fail("Failed to connect link - {0}".format(
                        dv.name + ".Local"))

    def test_getting_replica_number_for_cbas(self):
        testcases = [
            {
                "description": "Verify default value for replica number",
                "expected_result": 0
            }]

        rbac_util = RbacUtils(self.cluster.master)
        self.create_or_delete_users(rbac_util, rbac_users_created)

        for user in rbac_users_created:
            tc = {
                "description": "Get number of analytics replica using {0} user".format(
                    user),
                "expected_result": 0,
                "username": user
            }
            if user in [
                "analytics_manager", "query_manage_index",
                "replication_target", "query_system_catalog", "data_reader",
                "analytics_admin", "data_dcp_reader","data_monitoring",
                "mobile_sync_gateway", "query_select", "data_backup",
                "query_external_access", "data_writer", "query_insert",
                "query_update", "views_reader", "query_delete",
                "fts_searcher", "fts_admin", "analytics_select",
                "bucket_full_access", "analytics_reader"]:
                tc["validate_error_msg"] = True
                tc["expected_error"] = "Forbidden. User needs the following permissions"
            testcases.append(tc)

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                get_result = self.cbas_util.get_replica_number_from_settings(
                    self.cluster.master, method="GET",
                    username=testcase.get("username", None),
                    password=testcase.get("password", None),
                    validate_error_msg=testcase.get("validate_error_msg",
                                                    False),
                    expected_error=testcase.get("expected_error", None))

                if "validate_error_msg" in testcase:
                    if get_result != 4:
                        raise Exception("Error message is different than expected")
                elif get_result != testcase["expected_result"]:
                    raise Exception("Expected analytics replica number is "
                                    "different")
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_setting_replica_number_for_cbas(self):
        testcases = [
            {
                "description": "Verify setting replica number to 1",
                "replica_num": 1,
                "expected_result": 1
            },
            {
                "description": "Verify setting replica number to 2",
                "replica_num": 2,
                "expected_result": 2
            },
            {
                "description": "Verify setting replica number to 3",
                "replica_num": 3,
                "expected_result": 3
            },
            {
                "description": "Verify setting replica number to 0",
                "replica_num": 0,
                "expected_result": 0
            },
            {
                "description": "Verify setting replica number to string value",
                "replica_num": "\"1\"",
                "expected_error": "The value must be an integer",
                "validate_error_msg": True
            },
            {
                "description": "Verify setting replica number to boolean "
                               "value",
                "replica_num": True,
                "expected_error": "The value must be an integer",
                "validate_error_msg": True
            },
            {
                "description": "Verify setting replica number to null",
                "replica_num": None,
                "expected_error": "The value must be an integer",
                "validate_error_msg": True
            },
            {
                "description": "Verify setting replica number to less than 0",
                "replica_num": -1,
                "expected_error": "The value must be in range from 0 to 3",
                "validate_error_msg": True
            },
            {
                "description": "Verify setting replica number to greater "
                               "than 3",
                "replica_num": 4,
                "expected_error": "The value must be in range from 0 to 3",
                "validate_error_msg": True
            },
            {
                "description": "Verify setting replica number using node "
                               "other than master node.",
                "replica_num": 2,
                "expected_result": 2,
                "other_node": True
            },
            {
                "description": "Verify setting replica number using https",
                "replica_num": 2,
                "expected_result": 2,
                "use_https": True
            },
            {
                "description": "Verify setting replica number using invalid "
                               "password for user",
                "replica_num": 2,
                "expected_error": "Request Rejected",
                "password": "invalid",
                "validate_error_msg": True
            },
            {
                "description": "Verify setting replica number when one of "
                               "the CBAS nodes in the cluster is down",
                "replica_num": 2,
                "cbas_down": True,
                "expected_result": 2
            }
        ]
        rbac_util = RbacUtils(self.cluster.master)
        self.create_or_delete_users(rbac_util, rbac_users_created)
        for user in rbac_users_created:
            tc = {
                "description": "Set number of analytics replica using {0} "
                               "user".format(user),
                "replica_num": random.randrange(4),
                "username": user
            }
            if user in [
                "analytics_manager", "query_manage_index",
                "replication_target", "query_system_catalog", "data_reader",
                "analytics_admin", "data_dcp_reader","data_monitoring",
                "security_admin_external", "mobile_sync_gateway",
                "query_select", "data_backup", "query_external_access",
                "data_writer", "query_insert", "query_update",
                "views_reader", "query_delete", "fts_searcher",
                "bucket_admin", "fts_admin", "security_admin_local",
                "ro_admin", "analytics_select", "replication_admin",
                "views_admin", "bucket_full_access", "analytics_reader"]:
                tc["validate_error_msg"] = True
                tc["expected_error"] = "Forbidden. User needs the following permissions"
            else:
                tc["expected_result"] = tc["replica_num"]
            testcases.append(tc)
        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                node = self.cluster.master
                if "other_node" in testcase:
                    for new_node in self.cluster.nodes_in_cluster:
                        if new_node.ip != self.cluster.master.ip:
                            node = new_node
                            break
                if "use_https" in testcase:
                    CbServer.use_https = True
                    trust_all_certs()
                if "cbas_down" in testcase:
                    cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
                        self.cluster, service_type="cbas", get_all_nodes=True,
                        servers=self.cluster.nodes_in_cluster)
                    chosen = self.cluster_util.pick_nodes(
                        self.cluster.master, howmany=1,
                        target_node=cluster_cbas_nodes[0],
                        exclude_nodes=[self.cluster.master])
                    self.cluster.rest.fail_over(chosen[0].id, graceful=False)
                set_result = self.cbas_util.set_replica_number_from_settings(
                    node, replica_num=testcase["replica_num"],
                    username=testcase.get("username", None),
                    password=testcase.get("password", None),
                    validate_error_msg=testcase.get("validate_error_msg", False),
                    expected_error=testcase.get("expected_error", None))

                if "validate_error_msg" in testcase:
                    if set_result != 4:
                        raise Exception("Error message is different than expected")
                elif set_result in [0, 1, 2, 3]:
                    get_result = self.cbas_util.get_replica_number_from_settings(
                        self.cluster.master)
                    if not (set_result == get_result == testcase["expected_result"]):
                        raise Exception("Analytics replica number is "
                                        "different from what was set.")
                else:
                    raise Exception("Setting analytics replica number failed")

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])
            finally:
                if "use_https" in testcase:
                    CbServer.use_https = False
                if "cbas_down" in testcase:
                    self.cluster.rest.set_recovery_type(
                        otpNode=chosen[0].id, recoveryType="full")
        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def verify_all_indexes_are_used(self):
        for idx in self.cbas_util.list_all_index_objs():
            statement = "Select * from {0} where age > 5 limit 10".format(
                idx.full_dataset_name)
            if not self.cbas_util.verify_index_used(
                    self.cluster, statement, index_used=True,
                    index_name=idx.name):
                self.log.info("Index {0} on dataset {1} was not used while "
                              "executing query".format(idx.name,
                                                       idx.full_dataset_name))

    def post_replica_activation_verification(
            self, verify_index=True, run_parallel_cbas_queries=True,
            kv_data_mutation=True):
        self.log.info("Verifying doc count accross all datasets")
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Docs are missing after replicas become active")

        if verify_index:
            self.log.info("Verifying CBAS indexes are working")
            self.verify_all_indexes_are_used()

        if run_parallel_cbas_queries:
            self.log.info("Running parallel queries on CBAS")
            n1ql_query_task, cbas_query_task = self.rebalance_util.start_parallel_queries(
                self.cluster, False, True, parallelism=3)

        if kv_data_mutation:
            self.log.info("Starting data mutations on KV")
            doc_loading_task = self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=True,
                durability_level=self.durability_level)

            self.log.info("Connecting all the Local links")
            self.connect_disconnect_all_local_links(disconnect=False)

            if not self.rebalance_util.wait_for_data_load_to_complete(
                    doc_loading_task, False):
                self.log.info("Doc loading failed")

        if run_parallel_cbas_queries:
            self.rebalance_util.stop_parallel_queries(
                n1ql_query_task,cbas_query_task)

        if kv_data_mutation:
            if not self.cbas_util.validate_docs_in_all_datasets(
                    self.cluster, self.bucket_util, timeout=600):
                self.fail("Docs are missing")

            self.log.info("Verifying CBAS indexes are working")
            self.verify_all_indexes_are_used()

    def wait_for_rebalance_to_start_before_killing_server(
            self, cluster, nodes_to_kill):

        def stop_server(server):
            shell = RemoteMachineShellConnection(server)
            if shell.is_couchbase_installed():
                shell.stop_couchbase()
                self.log.debug("Couchbase stopped on {0}".format(server))
            else:
                shell.stop_membase()
                self.log.debug("Membase stopped on {0}".format(server))
            shell.disconnect()

        try:
            while True:
                if cluster.rest._rebalance_progress_status() == 'running':
                    self.sleep(3, "Waiting for rebalance to start before "
                                  "killing the server")
                    break
            if isinstance(nodes_to_kill, list):
                for node in nodes_to_kill:
                    self.log.info("Stopping Couchbase server on {0}".format(
                        node.ip))
                    stop_server(node)
            else:
                self.log.info("Stopping Couchbase server on {0}".format(
                    nodes_to_kill.ip))
                stop_server(nodes_to_kill)
            return True
        except Exception:
            return False

    def start_server(self, server):
        shell = RemoteMachineShellConnection(server)
        if shell.is_couchbase_installed():
            shell.start_couchbase()
            self.log.debug("Couchbase started on {0}".format(server))
        else:
            shell.start_membase()
            self.log.debug("Membase started on {0}".format(server))
        shell.disconnect()

    def set_ops_on_nodes(self, operation, setA, setB):
        a = copy.deepcopy(setA)
        b = copy.deepcopy(setB)
        if operation == "-":
            b = [x.ip for x in b]
            for node in a:
                if node.ip in b:
                    a.remove(node)
            return a
        elif operation == "|":
            x = a + b
            y = list(set([i.ip for i in x]))
            c = list()
            for node in x:
                if node.ip in y:
                    c.append(node)
                    y.remove(node.ip)
            return c
        elif operation == "&":
            x = a + b
            y = [i.ip for i in a]
            z = [i.ip for i in b]
            c = dict()
            for j in x:
                if (j.ip in y) and (j.ip in z) and (j.ip not in c):
                    c[j.ip] = j
            return c.values()

    def test_analytics_replica(self):
        self.log.info("Test started")
        self.setup_for_test()

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what "
                      "was set")

        self.log.info("Marking one of the CBAS nodes as failed over.")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=1, failover_type="Hard",
            action=None, timeout=7200, available_servers=self.available_servers,
            exclude_nodes=[], kv_failover_nodes=None, cbas_failover_nodes=None,
            all_at_once=True)

        self.post_replica_activation_verification()

        if self.input.param('recover_active_node', False):
            self.available_servers, kv_failover_nodes, cbas_failover_nodes =\
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

        self.log.info("Adding event for Partition Topology Updated events")
        self.system_events.add_event(AnalyticsEvents.partition_topology_updated(
            self.cluster.cbas_cc_node.ip, self.replica_num))

    def test_analytics_replica_when_data_flushed_to_disk_at_regular_interval(
            self):
        self.log.info("Test started")
        self.setup_for_test(wait_for_ingestion=False)

        self.log.info("Starting flush to disk task")
        flush_to_disk_task = FlushToDiskTask(
            self.cluster, self.cbas_util, datasets=[], run_infinitely=True,
            interval=5)
        self.task.jython_task_manager.add_new_task(flush_to_disk_task)

        self.log.info("Starting data mutations on KV")
        if not self.rebalance_util.data_load_collection(
            self.cluster, self.doc_spec_name, False, async_load=False,
            durability_level=self.durability_level):
            self.fail("Error while loading docs")

        self.log.info("Stopping flush to disk task")
        self.task_manager.stop_task(flush_to_disk_task)

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what was "
                      "set")

        self.log.info("Marking one of the CBAS nodes as failed over.")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=1, failover_type="Hard",
            action=None, timeout=7200, available_servers=self.available_servers,
            exclude_nodes=[], kv_failover_nodes=[], cbas_failover_nodes=[])

        self.post_replica_activation_verification()

        if self.input.param('recover_active_node', False):
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_active_node_fails_when_some_data_still_to_be_replicated(self):
        self.log.info("Test started")
        self.setup_for_test(wait_for_ingestion=False)

        self.log.info(
            "Flushing CBAS data to disk onoce so that atleast some data is replicated")
        for dataset in self.cbas_util.list_all_dataset_objs():
            if not self.cbas_util.force_flush_cbas_data_to_disk(
                    self.cluster.cbas_cc_node, dataset.dataverse_name,
                    dataset.name):
                self.fail("Failed to flush data to disk for dataset - {"
                          "0}".format(dataset.full_name))

        self.log.info("Starting data mutations on KV")
        doc_loading_task = self.rebalance_util.data_load_collection(
            self.cluster, self.doc_spec_name, False, async_load=True,
            durability_level=self.durability_level)

        self.log.info("Marking one of the CBAS nodes as failed over.")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=1, failover_type="Hard",
            action=None, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=[], kv_failover_nodes=[], cbas_failover_nodes=[])

        if not self.rebalance_util.wait_for_data_load_to_complete(
                doc_loading_task, False):
            self.log.info("Doc loading failed")

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        self.log.info("Verify CBAS indexes work on replicas")
        self.verify_all_indexes_are_used()

        if self.input.param('recover_active_node', False):
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    """def test_active_node_fails_when_no_data_replicated(self):
        self.log.info("Test started")
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 1),
            "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 1),
            "no_of_synonyms": 0,
            "no_of_indexes": self.input.param('no_of_idx', 1),
            "max_thread_count": self.input.param('no_of_threads', 1),
            "creation_methods": ["cbas_collection", "cbas_dataset"]}
        self.setup_for_test(update_spec, "dataset")

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what ")

        remove btree files hare

        self.log.info("Marking one of the CBAS nodes as failed over.")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=1, failover_type="Hard",
            action=None, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=[], kv_failover_nodes=[], cbas_failover_nodes=[])

        self.log.info("Verifying whether the replicas became active and "
                      "there was no data loss on CBAS")
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Docs are missing after replicas become active")

        def verify_index_usage():
            for idx in self.cbas_util.list_all_index_objs():
                statement = "Select * from {0} where age > 5 limit 10".format(
                    idx.full_dataset_name)
                if not self.cbas_util.verify_index_used(
                        self.cluster, statement, index_used=True,
                        index_name=idx.name):
                    self.log.info(
                        "Index {0} on dataset {1} was not used while "
                        "executing query".format(idx.name,
                                                 idx.full_dataset_name))

        self.log.info("Verify CBAS indexes work on replicas")
        verify_index_usage()

        self.log.info("Running parallel queries on CBAS")
        n1ql_query_task, cbas_query_task = self.rebalance_util.start_parallel_queries(
            self.cluster, False, True, parallelism=3)

        self.log.info("Starting data mutations on KV")
        doc_loading_task = self.rebalance_util.data_load_collection(
            self.cluster, self.doc_spec_name, False, async_load=True,
            durability_level=self.durability_level)

        self.log.info("Connecting all the Local links")
        self.connect_disconnect_all_local_links(disconnect=False)

        if not self.rebalance_util.wait_for_data_load_to_complete(
                doc_loading_task, False):
            self.log.info("Doc loading failed")

        self.rebalance_util.stop_parallel_queries(n1ql_query_task,
                                                  cbas_query_task)

        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Docs are missing after replicas become active")

        self.log.info("Verify CBAS indexes work on replicas")
        verify_index_usage()

        if self.input.param('recover_active_node', False):
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=0, failover_type="Hard",
                action="FullRecovery", timeout=7200,
                available_servers=self.available_servers,
                exclude_nodes=[], kv_failover_nodes=kv_failover_nodes,
                cbas_failover_nodes=cbas_failover_nodes)

            if not self.cbas_util.validate_docs_in_all_datasets(
                    self.cluster, self.bucket_util, timeout=600):
                self.fail("Docs are missing after replicas become active")

            self.log.info("Verify CBAS indexes work after recovering active "
                          "node")
            verify_index_usage()"""

    def test_increasing_number_of_replicas_when_sufficient_cbas_nodes_in_cluster(self):
        self.log.info("Test started")
        self.setup_for_test()

        for replica_num in range(1, 4):

            self.log.info("Setting Analytics replica number to {0}".format(
                replica_num))
            set_result = self.cbas_util.set_replica_number_from_settings(
                self.cluster.master, replica_num=replica_num)
            if set_result != replica_num:
                self.fail("Error while setting replica for CBAS")

            if len(self.cluster.cbas_nodes) < replica_num + 1:
                self.log.info("Number of CBAS nodes is less than what is "
                              "required to support {0} number of "
                              "replication, hence skipping "
                              "validation".format(replica_num))
            else:
                self.log.info(
                    "Rebalancing for CBAS replica setting change to take "
                    "effect.")
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                    cbas_nodes_out=0, available_servers=self.available_servers,
                    exclude_nodes=[])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalance failed")

                self.log.info("Disconnecting all the Local links, so that the "
                              "data can be persisted")
                self.connect_disconnect_all_local_links(disconnect=True)

                if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
                    self.fail("Replication could not complete before timeout")

                if not self.cbas_util.verify_actual_number_of_replicas(
                        self.cluster, replica_num):
                    self.fail("Actual number of replicas is different from what "
                              "was set")

                self.log.info("Marking {0} of the CBAS nodes as failed "
                              "over".format(replica_num))
                self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                    self.cluster, kv_nodes=0, cbas_nodes=replica_num,
                    failover_type="Hard", action=None, timeout=7200,
                    available_servers=self.available_servers, exclude_nodes=[],
                    kv_failover_nodes=[], cbas_failover_nodes=[],
                    all_at_once=True)

                self.post_replica_activation_verification()

                self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                    self.rebalance_util.perform_action_on_failed_over_nodes(
                        self.cluster, action="FullRecovery",
                        available_servers=self.available_servers,
                        kv_failover_nodes=kv_failover_nodes,
                        cbas_failover_nodes=cbas_failover_nodes)

                self.log.info("Verification after failed over node recovery ")
                self.post_replica_activation_verification(True, False, False)

    def test_increasing_number_of_replicas_with_addition_of_cbas_nodes_in_cluster(self):
        self.log.info("Test started")
        self.setup_for_test()

        for replica_num in range(1, 4):

            self.log.info("Setting Analytics replica number to {0}".format(
                replica_num))
            set_result = self.cbas_util.set_replica_number_from_settings(
                self.cluster.master, replica_num=replica_num)
            if set_result != replica_num:
                self.fail("Error while setting replica for CBAS")

            self.log.info(
                "Rebalancing IN a CBAS node so that the set replica number is supported")
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=1,
                cbas_nodes_out=0, available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance failed")

            if len(self.cluster.cbas_nodes) < replica_num + 1:
                self.log.info("Number of CBAS nodes is less than what is "
                              "required to support {0} number of "
                              "replication, hence skipping "
                              "validation".format(replica_num))
            else:
                self.log.info("Disconnecting all the Local links, so that the "
                              "data can be persisted")
                self.connect_disconnect_all_local_links(disconnect=True)

                if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
                    self.fail("Replication could not complete before timeout")

                if not self.cbas_util.verify_actual_number_of_replicas(
                        self.cluster, replica_num):
                    self.fail("Actual number of replicas is different from "
                              "what was set")

                self.log.info("Marking {0} of the CBAS nodes as failed "
                              "over".format(replica_num))
                self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                    self.cluster, kv_nodes=0, cbas_nodes=replica_num,
                    failover_type="Hard", action=None, timeout=7200,
                    available_servers=self.available_servers, exclude_nodes=[],
                    kv_failover_nodes=[], cbas_failover_nodes=[],
                    all_at_once=True)

                self.post_replica_activation_verification()

                self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                    self.rebalance_util.perform_action_on_failed_over_nodes(
                        self.cluster, action="FullRecovery",
                        available_servers=self.available_servers,
                        kv_failover_nodes=kv_failover_nodes,
                        cbas_failover_nodes=cbas_failover_nodes)

                self.log.info("Verification after failed over node recovery ")
                self.post_replica_activation_verification(True, False, False)

    def test_decreasing_number_of_replicas_when_sufficient_cbas_nodes_in_cluster(self):
        self.log.info("Test started")
        self.setup_for_test()

        for replica_num in range(3, -1, -1):
            if self.replica_num != replica_num:
                self.log.info("Setting Analytics replica number to {0}".format(
                    replica_num))
                set_result = self.cbas_util.set_replica_number_from_settings(
                    self.cluster.master, replica_num=replica_num)
                if set_result != replica_num:
                    self.fail("Error while setting replica for CBAS")

            if len(self.cluster.cbas_nodes) < replica_num + 1:
                self.log.info("Number of CBAS nodes is less than what is "
                              "required to support {0} number of "
                              "replication, hence skipping "
                              "validation".format(replica_num))
            elif self.replica_num != replica_num:
                self.log.info(
                    "Rebalancing for CBAS replica setting change to take "
                    "effect.")
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                    cbas_nodes_out=0, available_servers=self.available_servers,
                    exclude_nodes=[])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalance failed")

            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, replica_num):
                self.fail("Actual number of replicas is different from what "
                          "was set")

            if replica_num > 0:
                self.log.info("Marking {0} of the CBAS nodes as failed "
                              "over".format(replica_num))
                self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                    self.cluster, kv_nodes=0, cbas_nodes=replica_num,
                    failover_type="Hard", action=None, timeout=7200,
                    available_servers=self.available_servers, exclude_nodes=[],
                    kv_failover_nodes=[], cbas_failover_nodes=[],
                    all_at_once=True)

                self.post_replica_activation_verification()

                self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                    self.rebalance_util.perform_action_on_failed_over_nodes(
                        self.cluster, action="FullRecovery",
                        available_servers=self.available_servers,
                        kv_failover_nodes=kv_failover_nodes,
                        cbas_failover_nodes=cbas_failover_nodes)

                self.log.info("Verification after failed over node recovery ")
                self.post_replica_activation_verification(True, False, False)

    def test_decreasing_number_of_replicas_with_removing_of_cbas_nodes_from_cluster(self):
        self.log.info("Test started")
        self.setup_for_test()

        for replica_num in range(3, -1, -1):
            if self.replica_num != replica_num:
                self.log.info("Setting Analytics replica number to {0}".format(
                    replica_num))
                set_result = self.cbas_util.set_replica_number_from_settings(
                    self.cluster.master, replica_num=replica_num)
                if set_result != replica_num:
                    self.fail("Error while setting replica for CBAS")

                self.log.info(
                    "Rebalancing OUT a CBAS node so that the set replica "
                    "number is supported")
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                    cbas_nodes_in=0, cbas_nodes_out=1,
                    available_servers=self.available_servers, exclude_nodes=[])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster, True):
                    self.fail("Rebalance failed")

            self.cluster.cbas_nodes = self.cluster_util.get_nodes_from_services_map(
                self.cluster, service_type="cbas", get_all_nodes=True,
                servers=self.cluster.nodes_in_cluster)

            if len(self.cluster.cbas_nodes) < replica_num + 1:
                self.log.info("Number of CBAS nodes is less than what is "
                              "required to support {0} number of "
                              "replication, hence skipping "
                              "validation".format(replica_num))
            else:
                self.log.info("Disconnecting all the Local links, so that the "
                              "data can be persisted")
                self.connect_disconnect_all_local_links(disconnect=True)

                if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
                    self.fail("Replication could not complete before timeout")

                if not self.cbas_util.verify_actual_number_of_replicas(
                        self.cluster, replica_num):
                    self.fail("Actual number of replicas is different from "
                              "what was set")

                if replica_num > 0:
                    self.log.info("Marking {0} of the CBAS nodes as failed "
                                  "over".format(replica_num))
                    self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                        self.cluster, kv_nodes=0, cbas_nodes=replica_num,
                        failover_type="Hard", action=None, timeout=7200,
                        available_servers=self.available_servers, exclude_nodes=[],
                        kv_failover_nodes=[], cbas_failover_nodes=[],
                        all_at_once=True)

                    self.post_replica_activation_verification()

                    self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                        self.rebalance_util.perform_action_on_failed_over_nodes(
                            self.cluster, action="FullRecovery",
                            available_servers=self.available_servers,
                            kv_failover_nodes=kv_failover_nodes,
                            cbas_failover_nodes=cbas_failover_nodes)

                    self.log.info("Verification after failed over node recovery ")
                    self.post_replica_activation_verification(True, False, False)

    def test_doc_crud_effects_on_analytics_replica(self):
        self.log.info("Test started")
        self.setup_for_test()

        self.log.info("Starting CRUD on KV data")
        if not self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=False,
                durability_level=self.durability_level,
                create_percentage_per_collection=200,
                delete_percentage_per_collection=20,
                update_percentage_per_collection=40,
                replace_percentage_per_collection=10):
            self.fail("Error while performing CRUD operations on KV data")

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what "
                      "was set")

        self.log.info("Marking one of the CBAS nodes as failed over.")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=self.replica_num,
            failover_type="Hard", action=None, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=[], kv_failover_nodes=[], cbas_failover_nodes=[],
            all_at_once=True)

        self.post_replica_activation_verification()

        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.perform_action_on_failed_over_nodes(
                self.cluster, action="FullRecovery",
                available_servers=self.available_servers,
                kv_failover_nodes=kv_failover_nodes,
                cbas_failover_nodes=cbas_failover_nodes)

        self.log.info("Verification after failed over node recovery ")
        self.post_replica_activation_verification(True, False, False)

    def test_effects_of_rebalancing_IN_cbas_nodes_on_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        nodes_to_rebalance_in = self.input.param('nodes_in', 0)

        self.log.info("Rebalancing IN a CBAS node")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_rebalance_in, cbas_nodes_out=0,
            available_servers=self.available_servers, exclude_nodes=[])
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalance failed")

        if len(self.cluster.cbas_nodes) < self.replica_num + 1:
            self.log.info("Number of CBAS nodes is less than what is "
                          "required to support {0} number of "
                          "replication, hence skipping "
                          "validation".format(self.replica_num))
        else:
            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(
                    self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, self.replica_num):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Marking {0} of the CBAS nodes as failed "
                          "over".format(self.replica_num))
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=self.replica_num,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers, exclude_nodes=[],
                kv_failover_nodes=[], cbas_failover_nodes=[],
                all_at_once=True)

            self.post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_cbas_node_crash_while_rebalancing_IN_cbas_nodes_does_not_change_actual_number_of_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        _ = self.cluster.rest.update_autofailover_settings(True, 600)

        nodes_to_rebalance_in = self.input.param('nodes_in', 0)
        node_to_crash = self.input.param('node_to_crash', "existing")
        actual_replica = self.cbas_util.get_actual_number_of_replicas(self.cluster)

        if node_to_crash == "incoming":
            available_servers_before_rebalance = copy.deepcopy(self.available_servers)
        elif node_to_crash == "existing":
            for node in self.cluster.cbas_nodes:
                if node.ip != self.cluster.cbas_cc_node.ip:
                    selected_node = node
                    break

        self.log.info("Rebalancing IN {0} CBAS nodes".format(
            nodes_to_rebalance_in))
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_rebalance_in, cbas_nodes_out=0,
            available_servers=self.available_servers, exclude_nodes=[])

        if node_to_crash == "incoming":
            selected_node = random.choice(
                self.set_ops_on_nodes(
                    "-", available_servers_before_rebalance,
                    self.available_servers))
            self.log.debug("Selected nodes - {0}".format(selected_node))

        selected_node_rest = RestHelper(RestConnection(selected_node))
        server_stopped = self.wait_for_rebalance_to_start_before_killing_server(
            self.cluster, selected_node)

        if not server_stopped:
            self.fail("Error while stopping couchbase server on {"
                      "0}".format(selected_node.ip))
        else:
            if self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance passed when it should have failed")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, actual_replica):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Restarting the server and rebalancing again")
            try:
                self.start_server(selected_node)
            except Exception:
                self.fail("Error while restarting server on {0}".format(
                    selected_node))

            if node_to_crash == "existing" and \
                    self.rebalance_util.get_failover_count(self.cluster) > 0:
                selected_otpnode = self.cluster_util.pick_nodes(
                    self.cluster.master, target_node=selected_node)[0]
                if not self.cluster.rest.set_recovery_type(
                        otpNode=selected_otpnode.id, recoveryType="full"):
                    self.fail("Failed to add back node {0}".format(
                        selected_node.ip))

            self.log.info("Checking if server is running on {0}".format(
                selected_node.ip))
            if not selected_node_rest.is_ns_server_running():
                self.fail("Server failed to come up after timeout on node {"
                          "0}".format(selected_node.ip))

            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 360):
                self.fail("Cbas service failed to come up")

            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                cbas_nodes_in=0, cbas_nodes_out=0,
                available_servers=self.available_servers, exclude_nodes=[])

            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster, True):
                self.fail("Rebalance failed")

            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(
                    self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, self.replica_num):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Marking {0} of the CBAS nodes as failed "
                          "over".format(self.replica_num))
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=self.replica_num,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers, exclude_nodes=[],
                kv_failover_nodes=[], cbas_failover_nodes=[],
                all_at_once=True)

            self.post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_effects_of_SWAP_rebalancing_cbas_nodes_on_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        nodes_to_swap_rebalance = self.input.param('nodes_to_swap', 0)

        self.log.info("SWAP Rebalance CBAS nodes")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_swap_rebalance, cbas_nodes_out=nodes_to_swap_rebalance,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node])
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster, check_cbas_running=True):
            self.fail("Rebalance failed")

        if len(self.cluster.cbas_nodes) < self.replica_num + 1:
            self.log.info("Number of CBAS nodes is less than what is "
                          "required to support {0} number of "
                          "replication, hence skipping "
                          "validation".format(self.replica_num))
        else:
            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(
                    self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, self.replica_num):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Marking {0} of the CBAS nodes as failed "
                          "over".format(self.replica_num))
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=self.replica_num,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node],
                kv_failover_nodes=[], cbas_failover_nodes=[],
                all_at_once=True)

            self.post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_cbas_node_crash_while_swap_rebalancing_cbas_nodes_does_not_change_actual_number_of_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()
        _ = self.cluster.rest.update_autofailover_settings(True, 600)

        nodes_to_swap_rebalance = self.input.param('nodes_to_swap', 0)
        # Following values are accepted for node_to_crash - in, out, in-out, other
        node_to_crash = self.input.param('node_to_crash', "in")
        actual_replica = self.cbas_util.get_actual_number_of_replicas(self.cluster)

        available_servers_before_rebalance = copy.deepcopy(self.available_servers)
        cluster_cbas_nodes = copy.deepcopy(self.cluster.cbas_nodes)

        self.log.info("SWAP Rebalancing CBAS nodes")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_swap_rebalance,
            cbas_nodes_out=nodes_to_swap_rebalance,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node])

        selected_nodes = list()
        if node_to_crash in ["in", "in-out"]:
            selected_nodes.append(random.choice(self.set_ops_on_nodes(
                "-", available_servers_before_rebalance, self.available_servers)))
        if node_to_crash in ["out", "in-out"]:
            selected_nodes.append(random.choice(self.set_ops_on_nodes(
                "-", self.available_servers,
                available_servers_before_rebalance)))
        if node_to_crash == "other":
            in_node = self.set_ops_on_nodes(
                "-", available_servers_before_rebalance, self.available_servers)
            out_node = self.set_ops_on_nodes(
                "-", self.available_servers, available_servers_before_rebalance)
            for node in self.set_ops_on_nodes(
                    "-", cluster_cbas_nodes, self.set_ops_on_nodes(
                        "|", in_node, out_node)):
                if node.ip != self.cluster.cbas_cc_node.ip:
                    selected_nodes.append(node)
                    break

        selected_node_rest_helper = list()
        for node in selected_nodes:
            selected_node_rest_helper.append(RestHelper(RestConnection(node)))

        server_stopped = self.wait_for_rebalance_to_start_before_killing_server(
                self.cluster, selected_nodes)

        if not server_stopped:
            self.fail("Error while stopping couchbase server on one of the "
                      "cbas nodes")
        else:
            if self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance passed when it should have failed")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, actual_replica):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            server_started = True
            self.log.info("Restarting the server and rebalancing again")
            for node in selected_nodes:
                self.log.info("Starting Couchbase server on {0}".format(
                    node.ip))
                try:
                    self.start_server(node)
                    server_started = server_started and True
                except Exception:
                    server_started = server_started and False
                    self.log.error("Error while starting couchbase server on {"
                                   "0}.".format(node.ip))

            if not server_started:
                self.fail("Error while starting couchbase server on one of the cbas nodes")

            for rest_helper in selected_node_rest_helper:
                self.log.info("Checking if server is running on {0}".format(
                    rest_helper.rest.ip))
                if not rest_helper.is_ns_server_running():
                    self.fail("Server failed to come up after timeout on node {"
                              "0}".format(rest_helper.rest.ip))

            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 360):
                self.fail("Cbas service failed to come up")

            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                cbas_nodes_in=0, cbas_nodes_out=0,
                available_servers=self.available_servers, exclude_nodes=[])

            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster, True):
                self.fail("Rebalance failed")

            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(
                    self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, self.replica_num):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Marking {0} of the CBAS nodes as failed "
                          "over".format(self.replica_num - 1))
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=self.replica_num - 1,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node],
                kv_failover_nodes=[], cbas_failover_nodes=[],
                all_at_once=True)

            self.post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_effects_of_rebalancing_OUT_cbas_nodes_on_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        nodes_to_rebalance_out = self.input.param('nodes_out', 0)

        self.log.info("Rebalancing OUT a CBAS node")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=nodes_to_rebalance_out,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node])
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalance failed")

        self.log.info("Disconnecting all the Local links, so that the "
                      "data can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(
                self.cluster):
            self.fail("Replication could not complete before timeout")

        if len(self.cluster.cbas_nodes) > self.replica_num:
            actual_num_of_replicas = self.replica_num
        else:
            actual_num_of_replicas = len(self.cluster.cbas_nodes) - 1

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, actual_num_of_replicas):
            self.fail("Actual number of replicas is different from "
                      "what was set")

        self.post_replica_activation_verification()

    def test_cbas_node_crash_while_rebalancing_OUT_cbas_nodes_does_not_change_actual_number_of_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()
        _ = self.cluster.rest.update_autofailover_settings(True, 600)

        nodes_to_rebalance_out = self.input.param('nodes_out', 0)
        node_to_crash = self.input.param('node_to_crash', "other")
        actual_replica = self.cbas_util.get_actual_number_of_replicas(self.cluster)

        cbas_nodes_in_cluster = copy.deepcopy(self.cluster.cbas_nodes)

        self.log.info("Rebalancing OUT a CBAS node")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=nodes_to_rebalance_out,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node])

        rebalanced_out_nodes = self.set_ops_on_nodes(
            "&", cbas_nodes_in_cluster, self.available_servers)

        if node_to_crash == "outgoing":
            selected_node = random.choice(rebalanced_out_nodes)
        elif node_to_crash == "other":
            eligible_nodes = self.set_ops_on_nodes(
                "-", cbas_nodes_in_cluster, rebalanced_out_nodes)
            for node in eligible_nodes:
                if node.ip != self.cluster.cbas_cc_node.ip:
                    selected_node = node
                    break

        selected_node_rest_helper = RestHelper(RestConnection(selected_node))

        server_stopped = self.wait_for_rebalance_to_start_before_killing_server(
            self.cluster, selected_node)

        if not server_stopped:
            self.fail("Error while stopping couchbase server on {"
                      "0}".format(selected_node.ip))
        else:
            if self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance passed when it should have failed")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, actual_replica):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Restarting the server and rebalancing again")
            try:
                self.start_server(selected_node)
            except Exception:
                self.fail("Error while restarting server on {0}".format(
                    selected_node))

            self.sleep(30, "Waiting for node to warm before starting "
                           "rebalance")

            self.log.info("Checking if server is running on {0}".format(
                selected_node.ip))
            if not selected_node_rest_helper.is_ns_server_running():
                self.fail("Server failed to come up after timeout on node {"
                          "0}".format(selected_node.ip))

            cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
                self.cluster, service_type="cbas", get_all_nodes=True,
                servers=self.servers)
            self.cluster.nodes_in_cluster = self.set_ops_on_nodes(
                "|", self.cluster.nodes_in_cluster, cluster_cbas_nodes)

            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster + rebalanced_out_nodes, [],
                rebalanced_out_nodes, check_vbucket_shuffling=True,
                retry_get_process_num=200, services=None)

            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster, True):
                self.fail("Rebalance failed")

            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(
                    self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, self.replica_num - nodes_to_rebalance_out):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Marking {0} of the CBAS nodes as failed "
                          "over".format(self.replica_num))
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0,
                cbas_nodes=self.replica_num - nodes_to_rebalance_out,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node],
                kv_failover_nodes=[], cbas_failover_nodes=[],
                all_at_once=True)

            self.post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_effects_of_rebalancing_IN_OUT_cbas_nodes_on_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        nodes_to_rebalance_in = self.input.param('nodes_in', 0)
        nodes_to_rebalance_out = self.input.param('nodes_out', 0)

        self.log.info("Rebalancing IN-OUT CBAS nodes")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_rebalance_in,
            cbas_nodes_out=nodes_to_rebalance_out,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node])
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster, check_cbas_running=True):
            self.fail("Rebalance failed")

        self.log.info("Disconnecting all the Local links, so that the "
                      "data can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(
                self.cluster):
            self.fail("Replication could not complete before timeout")

        if len(self.cluster.cbas_nodes) > self.replica_num:
            replica_num = self.replica_num
        else:
            replica_num = len(self.cluster.cbas_nodes) - 1
        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, replica_num):
            self.fail("Actual number of replicas is different from "
                      "what was set")

        self.log.info("Marking {0} of the CBAS nodes as failed "
                      "over".format(replica_num))
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=replica_num,
            failover_type="Hard", action=None, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node],
            kv_failover_nodes=[], cbas_failover_nodes=[],
            all_at_once=True)

        self.post_replica_activation_verification()

        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.perform_action_on_failed_over_nodes(
                self.cluster, action="FullRecovery",
                available_servers=self.available_servers,
                kv_failover_nodes=kv_failover_nodes,
                cbas_failover_nodes=cbas_failover_nodes)

        self.log.info("Verification after failed over node recovery ")
        self.post_replica_activation_verification(True, False, False)

    def test_cbas_node_crash_while_rebalancing_IN_OUT_cbas_nodes_does_not_change_actual_number_of_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()
        _ = self.cluster.rest.update_autofailover_settings(True, 600)

        nodes_to_rebalance_in = self.input.param('nodes_in', 0)
        nodes_to_rebalance_out = self.input.param('nodes_out', 0)
        # Following values are accepted for node_to_crash - in, out, in-out, other
        node_to_crash = self.input.param('node_to_crash', "in").split("-")
        actual_replica = self.cbas_util.get_actual_number_of_replicas(
            self.cluster)

        available_servers_before_rebalance = copy.deepcopy(
            self.available_servers)
        cluster_cbas_nodes = copy.deepcopy(self.cluster.cbas_nodes)

        self.log.info("Rebalancing IN-OUT CBAS nodes")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_rebalance_in,
            cbas_nodes_out=nodes_to_rebalance_out,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node])

        selected_nodes = dict()
        out_node = list()
        for action in node_to_crash:
            if action == "in":
                in_nodes = self.set_ops_on_nodes(
                    "-", available_servers_before_rebalance, self.available_servers)
                while True:
                    selected_node = random.choice(in_nodes)
                    if selected_node.ip not in selected_nodes:
                        selected_nodes[selected_node.ip] = selected_node
                        break
            elif action == "out":
                out_node = self.set_ops_on_nodes(
                    "-", self.available_servers, available_servers_before_rebalance)
                while True:
                    selected_node = random.choice(out_node)
                    if selected_node.ip not in selected_nodes:
                        selected_nodes[selected_node.ip] = selected_node
                        break
            elif action == "other":
                in_node = self.set_ops_on_nodes(
                    "-", available_servers_before_rebalance, self.available_servers)
                out_node = self.set_ops_on_nodes(
                    "-", self.available_servers, available_servers_before_rebalance)
                eligible_nodes = self.set_ops_on_nodes(
                    "-", cluster_cbas_nodes, self.set_ops_on_nodes(
                        "|", in_node, out_node))
                for node in eligible_nodes:
                    if node.ip != self.cluster.cbas_cc_node.ip:
                        selected_nodes[node.ip] = node
                        break

        selected_node_rest_helper = list()
        for node in selected_nodes.values():
            selected_node_rest_helper.append(RestHelper(RestConnection(node)))

        server_stopped = self.wait_for_rebalance_to_start_before_killing_server(
            self.cluster, selected_nodes.values())

        if not server_stopped:
            self.fail("Error while stopping couchbase server on one of the "
                      "cbas nodes")
        else:
            if self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance passed when it should have failed")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, actual_replica):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            server_started = True
            self.log.info("Restarting the server and rebalancing again")
            for node in selected_nodes.values():
                self.log.info("Starting Couchbase server on {0}".format(
                    node.ip))
                try:
                    self.start_server(node)
                    server_started = server_started and True
                except Exception:
                    server_started = server_started and False
                    self.log.error("Error while starting couchbase server on {"
                                   "0}.".format(node.ip))

            if not server_started:
                self.fail(
                    "Error while starting couchbase server on one of the cbas nodes")

            for rest_helper in selected_node_rest_helper:
                self.log.info("Checking if server is running on {0}".format(
                    rest_helper.rest.ip))
                if not rest_helper.is_ns_server_running():
                    self.fail("Server failed to come up after timeout on node {"
                              "0}".format(rest_helper.rest.ip))

            if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 360):
                self.fail("Cbas service failed to come up")

            cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
                self.cluster, service_type="cbas", get_all_nodes=True,
                servers=self.servers)
            self.cluster.nodes_in_cluster = self.set_ops_on_nodes(
                "|", self.cluster.nodes_in_cluster, cluster_cbas_nodes)

            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster + out_node, [],
                out_node, check_vbucket_shuffling=True,
                retry_get_process_num=200, services=None)

            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance failed")

            self.log.info("Disconnecting all the Local links, so that the "
                          "data can be persisted")
            self.connect_disconnect_all_local_links(disconnect=True)

            if not self.cbas_util.wait_for_replication_to_finish(
                    self.cluster):
                self.fail("Replication could not complete before timeout")

            if len(self.cluster.cbas_nodes) > self.replica_num:
                actual_replica = self.replica_num
            else:
                actual_replica = len(self.cluster.cbas_nodes) - 1

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, actual_replica):
                self.fail("Actual number of replicas is different from "
                          "what was set")

            self.log.info("Marking {0} of the CBAS nodes as failed "
                          "over".format(actual_replica))
            self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=actual_replica,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers,
                exclude_nodes=[self.cluster.cbas_cc_node],
                kv_failover_nodes=[], cbas_failover_nodes=[],
                all_at_once=True)

            self.post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster, action="FullRecovery",
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            self.log.info("Verification after failed over node recovery ")
            self.post_replica_activation_verification(True, False, False)

    def test_effects_of_stop_and_restarting_rebalance_on_cbas_replica(self):
        self.log.info("Test started")
        self.setup_for_test()

        nodes_to_rebalance_in = self.input.param('nodes_in', 0)
        actual_replica = self.cbas_util.get_actual_number_of_replicas(
            self.cluster)

        self.log.info("Rebalancing IN a CBAS node")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=nodes_to_rebalance_in, cbas_nodes_out=0,
            available_servers=self.available_servers, exclude_nodes=[])

        while True:
            if self.cluster.rest._rebalance_progress_status() == 'running':
                self.sleep(3, "Waiting for rebalance to start before "
                              "killing the server")
                break

        if not self.cluster.rest.stop_rebalance():
            self.fail("Failed to stop rebalance")

        self.rebalance_util.wait_for_rebalance_task_to_complete(
            rebalance_task, self.cluster)

        response = self.cluster.rest.cluster_status()
        if response["balanced"]:
            self.fail("Rebalance passed when it should have failed")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, actual_replica):
            self.fail("Actual number of replicas is different from "
                      "what was set")

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=0,
            available_servers=self.available_servers, exclude_nodes=[])

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalance failed")

        self.log.info("Disconnecting all the Local links, so that the "
                      "data can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(
                self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from "
                      "what was set")

        self.log.info("Marking {0} of the CBAS nodes as failed "
                      "over".format(self.replica_num))
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=self.replica_num,
            failover_type="Hard", action=None, timeout=7200,
            available_servers=self.available_servers, exclude_nodes=[],
            kv_failover_nodes=[], cbas_failover_nodes=[],
            all_at_once=True)

        self.post_replica_activation_verification()

        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.perform_action_on_failed_over_nodes(
                self.cluster, action="FullRecovery",
                available_servers=self.available_servers,
                kv_failover_nodes=kv_failover_nodes,
                cbas_failover_nodes=cbas_failover_nodes)

        self.log.info("Verification after failed over node recovery ")
        self.post_replica_activation_verification(True, False, False)

    def test_effects_of_killing_memcached_on_cbas_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what "
                      "was set")

        self.cluster_util.kill_memcached(self.cluster)

        self.post_replica_activation_verification()

    def test_effects_of_killing_cbas_service_on_cbas_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what "
                      "was set")

        self.cluster.cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            self.cluster, service_type="cbas", get_all_nodes=True,
            servers=self.cluster.nodes_in_cluster)
        if not self.cluster_util.kill_cbas_process(
                self.cluster, cbas_nodes=self.cluster.cbas_nodes):
            self.fail("Failed to kill CBAS service")

        self.post_replica_activation_verification()

    def test_effects_of_cbas_node_failover_on_cbas_replicas(self):
        self.log.info("Test started")
        self.setup_for_test()

        nodes_to_failover = self.input.param('nodes_to_failover', 0)
        failover_action = self.input.param('failover_action', "FullRecovery")

        self.log.info("Disconnecting all the Local links, so that the data "
                      "can be persisted")
        self.connect_disconnect_all_local_links(disconnect=True)

        if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
            self.fail("Replication could not complete before timeout")

        if not self.cbas_util.verify_actual_number_of_replicas(
                self.cluster, self.replica_num):
            self.fail("Actual number of replicas is different from what "
                      "was set")

        if self.input.param('quorum_failover', False):
            nodes_to_failover = self.replica_num + 1

        self.log.info("Marking {0} of the CBAS nodes as failed "
                      "over".format(nodes_to_failover))
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=nodes_to_failover,
            failover_type="Hard", action=None, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=[self.cluster.cbas_cc_node],
            kv_failover_nodes=[], cbas_failover_nodes=[],
            all_at_once=True)

        if nodes_to_failover > self.replica_num:
            if self.cbas_util.is_analytics_running(self.cluster, timeout=300):
                self.fail("Analytics service is still available when it "
                          "should not be")
        else:
            self.post_replica_activation_verification()

        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.perform_action_on_failed_over_nodes(
                self.cluster, action=failover_action,
                available_servers=self.available_servers,
                kv_failover_nodes=kv_failover_nodes,
                cbas_failover_nodes=cbas_failover_nodes)

        self.log.info("Verification after failed over node recovery ")
        self.post_replica_activation_verification(True, False, False)

        if len(self.cluster.cbas_nodes) <= self.replica_num + 1:
            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, len(self.cluster.cbas_nodes) - 1):
                self.fail("Actual number of replicas is different from what "
                          "was set")
