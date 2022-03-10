"""
Created on March 4, 2022
@author: Umang Agrawal
"""

import json
import random
from Queue import Queue
from threading import Thread
import time
import copy

from BucketLib.BucketOperations import BucketHelper
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import Dataverse, Synonym, CBAS_Index
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cbas.cbas_base import CBASBaseTest
from cbas_utils.cbas_utils import CBASRebalanceUtil
from remote.remote_util import RemoteMachineShellConnection
from collections_helper.collections_spec_constants import MetaCrudParams
from security.rbac_base import RbacBase
from Jython_tasks.task import RunQueriesTask, CreateDatasetsTask, DropDatasetsTask


class MetadataReplication(CBASBaseTest):

    def setUp(self):

        super(MetadataReplication, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            vbucket_check=True, cbas_util=self.cbas_util)

        if self.input.param("add_all_cbas_nodes", False):
            cbas_nodes_in = len(self.available_servers)
        elif self.input.param("nc_nodes_to_add", 0):
            cbas_nodes_in = self.input.param("nc_nodes_to_add", 0)
        else:
            cbas_nodes_in = 0

        if cbas_nodes_in > 0:
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                cbas_nodes_in=cbas_nodes_in, cbas_nodes_out=0,
                available_servers=self.available_servers, exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance failed")

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(MetadataReplication, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_for_test(self, wait_for_ingestion=True):
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

    def test_rebalance(self):
        self.setup_for_test()

        self.rebalance_node = self.input.param('rebalance_node', 'CC')
        self.how_many = self.input.param('how_many', 1)
        self.replica_change = self.input.param('replica_change', 0)
        self.rebalance_type = self.input.param('rebalance_type', 'out')
        self.restart_rebalance = self.input.param('restart_rebalance', False)

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        query = "select sleep(count(*),50000) from {0};".format(
            dataset.full_name)
        handles = self.cbas_util._run_concurrent_queries(
            self.cluster, query, "async", 10, wait_for_execution=False)

        self.log.info("Starting data mutations on KV")
        if not self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=False,
                durability_level=self.durability_level):
            self.fail("Error while loading docs")

        if self.rebalance_node == "CC":
            exclude_nodes = [node for node in self.cluster.cbas_nodes if
                             node.ip != self.cluster.cbas_cc_node.ip]
        elif self.rebalance_node == "NC":
            exclude_nodes = [self.cluster.cbas_cc_node]
        else:
            exclude_nodes = []

        """ Partition ID for Metadata is -1"""
        replicas_before_rebalance = len(
            self.cbas_util.get_replicas_info(self.cluster)["-1"])

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        cbas_nodes_in = 0
        cbas_nodes_out = 0

        if self.rebalance_type == 'in':
            cbas_nodes_in = self.how_many
            replicas_before_rebalance += self.replica_change
        else:
            cbas_nodes_out = self.how_many
            replicas_before_rebalance -= self.replica_change

        cbas_nodes_in_cluster = copy.deepcopy(self.cluster.cbas_nodes)

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=cbas_nodes_in, cbas_nodes_out=cbas_nodes_out,
            available_servers=self.available_servers, exclude_nodes=exclude_nodes)

        if self.restart_rebalance:
            str_time = time.time()
            while self.cluster.rest._rebalance_progress_status() != "running" \
                    and time.time() < str_time + 120:
                self.sleep(1, "Waiting for rebalance to start")
            if self.cluster.rest._rebalance_progress_status() == "running":
                if not self.cluster.rest.stop_rebalance(wait_timeout=120):
                    self.fail("Failed while stopping rebalance.")

                self.sleep(30, "Wait for some tine after rebalance is "
                               "stopped.")
                available_servers = copy.deepcopy(self.available_servers)
                x = cbas_nodes_in_cluster + available_servers
                y = [i.ip for i in cbas_nodes_in_cluster]
                z = [i.ip for i in available_servers]
                c = dict()
                for j in x:
                    if (j.ip in y) and (j.ip in z) and (j.ip not in c):
                        c[j.ip] = j
                rebalanced_out_nodes = c.values()

                if self.rebalance_type == 'in':
                    rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                        self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                        cbas_nodes_in=0, cbas_nodes_out=0,
                        available_servers=[], exclude_nodes=[])
                else:
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.nodes_in_cluster + rebalanced_out_nodes,
                        [], rebalanced_out_nodes, check_vbucket_shuffling=True,
                        retry_get_process_num=200, services=None)
            else:
                self.fail("Rebalance completed before the test could have stopped rebalance.")

        str_time = time.time()
        while self.cluster.rest._rebalance_progress_status() == "running" \
                and time.time() < str_time + 300:
            replicas = self.cbas_util.get_replicas_info(self.cluster)["-1"]
            if len(replicas) > 0:
                for replica in replicas:
                    self.log.info(
                        "Replica state during rebalance: %s" % replica[
                            'status'])
            self.sleep(5)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster, check_cbas_running=False):
            self.fail("Rebalance failed after failover")

        replicas = self.cbas_util.get_replicas_info(self.cluster)["-1"]
        replicas_after_rebalance = len(replicas)

        if replicas_after_rebalance != replicas_before_rebalance:
            self.fail("Replica before rebalance - {0}, Replica after "
                      "rebalance - {1}".format(
                replicas_before_rebalance, replicas_after_rebalance))

        if replicas_after_rebalance > 0:
            for replica in replicas:
                self.log.info(
                    "Replica state after rebalance: %s" % replica['status'])
                if replica['status'] != "IN_SYNC":
                    self.fail("Replica state is incorrect")

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(
                self.cluster, self.cluster.cbas_cc_node, handle)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running." % handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed." % handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful." % handle)
            else:
                aborted_count += 1
                self.log.info("Queued job is deleted: %s" % status)

        self.log.info(
            "After service restart %s queued jobs are Running." % run_count)
        self.log.info(
            "After service restart %s queued jobs are Failed." % fail_count)
        self.log.info(
            "After service restart %s queued jobs are Successful." % success_count)
        self.log.info(
            "After service restart %s queued jobs are Aborted." % aborted_count)

        if self.rebalance_node == "NC":
            self.assertTrue(aborted_count == 0, "Some queries aborted")

    def test_chain_rebalance_out_cc(self):
        self.setup_for_test()

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        query = "select sleep(count(*),50000) from {0};".format(
            dataset.full_name)
        handles = self.cbas_util._run_concurrent_queries(
            self.cluster, query, "async", 10, wait_for_execution=False)

        self.log.info("Starting data mutations on KV")
        if not self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=False,
                durability_level=self.durability_level):
            self.fail("Error while loading docs")

        """ Partition ID for Metadata is -1"""
        replicas_before_rebalance = len(
            self.cbas_util.get_replicas_info(self.cluster)["-1"])

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            self.cluster, service_type="cbas", get_all_nodes=True,
            servers=self.cluster.nodes_in_cluster)

        for i in range(1, len(cluster_cbas_nodes)):
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                cbas_nodes_in=0, cbas_nodes_out=1,
                available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster, check_cbas_running=False):
                self.fail("Rebalance failed after failover")

            replicas_after_rebalance = len(
                self.cbas_util.get_replicas_info(self.cluster)["-1"])
            if replicas_after_rebalance != replicas_before_rebalance - i:
                self.fail("Replica number before and after the rebalance do "
                          "not match")

        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(
                self.cluster, self.cluster.cbas_cc_node, handle)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running." % handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed." % handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful." % handle)
            else:
                aborted_count += 1
                self.log.info("Queued job is deleted: %s" % status)

        self.log.info(
            "After service restart %s queued jobs are Running." % run_count)
        self.log.info(
            "After service restart %s queued jobs are Failed." % fail_count)
        self.log.info(
            "After service restart %s queued jobs are Successful." % success_count)
        self.log.info(
            "After service restart %s queued jobs are Aborted." % aborted_count)

    def test_cc_swap_rebalance(self):
        self.setup_for_test()

        swap_nc = self.input.param('swap_nc', False)
        self.restart_rebalance = self.input.param('restart_rebalance', False)

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        query = "select sleep(count(*),50000) from {0};".format(
            dataset.full_name)
        handles = self.cbas_util._run_concurrent_queries(
            self.cluster, query, "async", 10, wait_for_execution=False)

        self.log.info("Starting data mutations on KV")
        if not self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=False,
                durability_level=self.durability_level):
            self.fail("Error while loading docs")

        if swap_nc:
            exclude_nodes = [self.cluster.cbas_cc_node]
        else:
            exclude_nodes = [node for node in self.cluster.cbas_nodes if
                             node.ip != self.cluster.cbas_cc_node.ip]

        """ Partition ID for Metadata is -1"""
        replicas_before_rebalance = len(
            self.cbas_util.get_replicas_info(self.cluster)["-1"])

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        cbas_nodes_in_cluster = copy.deepcopy(self.cluster.cbas_nodes)

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=1,
            cbas_nodes_out=1, available_servers=self.available_servers,
            exclude_nodes=exclude_nodes)

        if self.restart_rebalance:
            str_time = time.time()
            while self.cluster.rest._rebalance_progress_status() != "running" \
                    and time.time() < str_time + 120:
                self.sleep(1, "Waiting for rebalance to start")
            if self.cluster.rest._rebalance_progress_status() == "running":
                if not self.cluster.rest.stop_rebalance(wait_timeout=120):
                    self.fail("Failed while stopping rebalance.")
                self.sleep(30, "Wait for some tine after rebalance is "
                               "stopped.")
                available_servers = copy.deepcopy(self.available_servers)
                x = cbas_nodes_in_cluster + available_servers
                y = [i.ip for i in cbas_nodes_in_cluster]
                z = [i.ip for i in available_servers]
                c = dict()
                for j in x:
                    if (j.ip in y) and (j.ip in z) and (j.ip not in c):
                        c[j.ip] = j
                rebalanced_out_nodes = c.values()

                rebalance_task = self.task.async_rebalance(
                    self.cluster.nodes_in_cluster + rebalanced_out_nodes,
                    [], rebalanced_out_nodes, check_vbucket_shuffling=True,
                    retry_get_process_num=200, services=None)
            else:
                self.fail("Rebalance completed before the test could have stopped rebalance.")

        str_time = time.time()
        while self.cluster.rest._rebalance_progress_status() == "running" \
                and time.time() < str_time + 300:
            replicas = self.cbas_util.get_replicas_info(self.cluster)["-1"]
            if len(replicas) > 0:
                for replica in replicas:
                    self.log.info(
                        "Replica state during rebalance: %s" % replica[
                            'status'])
            self.sleep(5)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster, check_cbas_running=False):
            self.fail("Rebalance failed after failover")

        replicas = self.cbas_util.get_replicas_info(self.cluster)["-1"]
        replicas_after_rebalance = len(replicas)

        if replicas_after_rebalance != replicas_before_rebalance:
            self.fail("Replica before rebalance - {0}, Replica after "
                      "rebalance - {1}".format(
                replicas_before_rebalance, replicas_after_rebalance))

        if replicas_after_rebalance > 0:
            for replica in replicas:
                self.log.info(
                    "Replica state after rebalance: %s" % replica['status'])
                if replica['status'] != "IN_SYNC":
                    self.fail("Replica state is incorrect")

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(
                self.cluster, self.cluster.cbas_cc_node, handle)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running." % handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed." % handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful." % handle)
            else:
                aborted_count += 1
                self.log.info("Queued job is deleted: %s" % status)

        self.log.info(
            "After service restart %s queued jobs are Running." % run_count)
        self.log.info(
            "After service restart %s queued jobs are Failed." % fail_count)
        self.log.info(
            "After service restart %s queued jobs are Successful." % success_count)
        self.log.info(
            "After service restart %s queued jobs are Aborted." % aborted_count)

    def test_reboot_nodes(self):
        self.setup_for_test()

        self.node_type = self.input.param('node_type', 'CC')

        self.log.info("Starting data mutations on KV")
        if not self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=False,
                durability_level=self.durability_level):
            self.fail("Error while loading docs")

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        """ Partition ID for Metadata is -1"""
        replicas_before_reboot = len(
            self.cbas_util.get_replicas_info(self.cluster)["-1"])

        if self.node_type == "CC":
            shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)
            shell.reboot_server_and_wait_for_cb_run(
                self.cluster_util, self.cluster.cbas_cc_node)
            shell.disconnect()
        elif self.node_type == "NC":
            for server in self.cluster.cbas_nodes:
                if server.ip != self.cluster.cbas_cc_node.ip:
                    shell = RemoteMachineShellConnection(server)
                    shell.reboot_server_and_wait_for_cb_run(
                        self.cluster_util, server)
                    shell.disconnect()
        else:
            for server in self.cluster.cbas_nodes:
                shell = RemoteMachineShellConnection(server)
                shell.reboot_server_and_wait_for_cb_run(
                    self.cluster_util, server)
                shell.disconnect()

        end_time = time.time() + 600
        while time.time() < end_time:
            replicas_after_reboot = len(
                self.cbas_util.get_replicas_info(self.cluster)["-1"])
            if replicas_after_reboot == replicas_before_reboot:
                break
            else:
                self.sleep(10, "Sleeping before refetching replicas")

        if replicas_before_reboot != replicas_after_reboot:
            self.fail("Number of Replica nodes changed after reboot. Before: %s , After : %s"
                      % (replicas_before_reboot, replicas_after_reboot))

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Data is missing after rebooting cbas servers.")

    def test_failover(self):
        self.setup_for_test()

        self.rebalance_node = self.input.param('rebalance_node', 'CC')
        self.how_many = self.input.param('how_many', 1)
        self.replica_change = self.input.param('replica_change', 0)

        dataset = self.cbas_util.list_all_dataset_objs()[0]
        query = "select sleep(count(*),50000) from {0};".format(
            dataset.full_name)
        handles = self.cbas_util._run_concurrent_queries(
            self.cluster, query, "async", 10, wait_for_execution=False)

        self.log.info("Starting data mutations on KV")
        if not self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, False, async_load=False,
                durability_level=self.durability_level):
            self.fail("Error while loading docs")

        if self.rebalance_node == "CC":
            exclude_nodes = [node for node in self.cluster.cbas_nodes if
                             node.ip != self.cluster.cbas_cc_node.ip]
        elif self.rebalance_node == "NC":
            exclude_nodes = [self.cluster.cbas_cc_node]
        else:
            exclude_nodes = []

        """ Partition ID for Metadata is -1"""
        replicas_before_rebalance = len(
            self.cbas_util.get_replicas_info(self.cluster)["-1"])

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        if self.input.param('add_back', False):
            action = "FullRecovery"
        else:
            action = "RebalanceOut"

        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.failover(
                self.cluster, kv_nodes=0, cbas_nodes=self.how_many,
                failover_type="Hard", action=None, timeout=7200,
                available_servers=self.available_servers, exclude_nodes=exclude_nodes,
                kv_failover_nodes=None, cbas_failover_nodes=None,
                all_at_once=True, reset_cbas_cc=False)

        rebalance_task = self.rebalance_util.perform_action_on_failed_over_nodes(
            self.cluster, action=action,
            available_servers=self.available_servers,
            kv_failover_nodes=kv_failover_nodes,
            cbas_failover_nodes=cbas_failover_nodes,
            wait_for_complete=False)
        if self.input.param('restart_rebalance', False):
            str_time = time.time()
            while self.cluster.rest._rebalance_progress_status() != "running" \
                    and time.time() < str_time + 120:
                self.sleep(1, "Waiting for rebalance to start")
            if self.cluster.rest._rebalance_progress_status() == "running":
                if not self.cluster.rest.stop_rebalance(wait_timeout=120):
                    self.fail("Failed while stopping rebalance.")

                if self.input.param('add_back', False):
                    rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                        self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                        cbas_nodes_in=0, cbas_nodes_out=0,
                        available_servers=self.available_servers,
                        exclude_nodes=[])
                else:
                    rebalance_task = \
                        self.rebalance_util.perform_action_on_failed_over_nodes(
                            self.cluster, action=action,
                            available_servers=self.available_servers,
                            kv_failover_nodes=kv_failover_nodes,
                            cbas_failover_nodes=cbas_failover_nodes,
                            wait_for_complete=False)
            else:
                self.fail(
                    "Rebalance completed before the test could have stopped rebalance.")

        replicas_before_rebalance -= self.replica_change

        str_time = time.time()
        while self.cluster.rest._rebalance_progress_status() == "running" \
                and time.time() < str_time + 300:
            replicas = self.cbas_util.get_replicas_info(self.cluster)["-1"]
            if len(replicas) > 0:
                for replica in replicas:
                    self.log.info(
                        "Replica state during rebalance: %s" % replica['status'])
            self.sleep(5)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster, check_cbas_running=False):
            self.fail("Rebalance failed after failover")

        replicas = self.cbas_util.get_replicas_info(self.cluster)["-1"]
        replicas_after_rebalance = len(replicas)

        if replicas_after_rebalance != replicas_before_rebalance:
            self.fail("Replica before rebalance - {0}, Replica after "
                      "rebalance - {1}".format(
                replicas_before_rebalance, replicas_after_rebalance))

        if replicas_after_rebalance > 0:
            for replica in replicas:
                self.log.info(
                    "Replica state after rebalance: %s" % replica['status'])
                if replica['status'] != "IN_SYNC":
                    self.fail("Replica state is incorrect")

        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util, timeout=600):
            self.fail("Ingestion into datasets failed.")

        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(
                self.cluster, self.cluster.cbas_cc_node, handle)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running." % handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed." % handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful." % handle)
            else:
                aborted_count += 1
                self.log.info("Queued job is deleted: %s" % status)

        self.log.info(
            "After service restart %s queued jobs are Running." % run_count)
        self.log.info(
            "After service restart %s queued jobs are Failed." % fail_count)
        self.log.info(
            "After service restart %s queued jobs are Successful." % success_count)
        self.log.info(
            "After service restart %s queued jobs are Aborted." % aborted_count)

        if self.rebalance_node == "NC":
            self.assertTrue(aborted_count == 0, "Some queries aborted")
