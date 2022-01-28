'''
Created on 7-December-2021
@author: umang.agrawal
'''

import random
from cbas.cbas_base import CBASBaseTest
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils import CBASRebalanceUtil, FlushToDiskTask
import copy
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from SystemEventLogLib.analytics_events import AnalyticsEvents
from CbasLib.CBASOperations import CBASHelper


class CBASSystemEventLogs(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        if self.input.param('setup_infra', True):
            if "bucket_spec" not in self.input.test_params:
                self.input.test_params.update(
                    {"bucket_spec": "analytics.default"})
            self.input.test_params.update(
                {"cluster_kv_infra": "bkt_spec"})
        if "cbas_spec" not in self.input.test_params:
            self.input.test_params.update(
                {"cbas_spec": "local_datasets"})
        if "validate_sys_event_logs" not in self.input.test_params:
            self.input.test_params.update(
                {"validate_sys_event_logs": True})

        super(CBASSystemEventLogs, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            vbucket_check=True, cbas_util=self.cbas_util)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASSystemEventLogs, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def test_process_events(self):
        self.log.info("Adding event for process_started event")
        self.system_events.add_event(AnalyticsEvents.process_started(
            self.cluster.cbas_cc_node.ip, "cbas"))

        self.log.info("Killing Java process on cbas node to trigger process "
                      "crash event")
        cbas_shell = RemoteMachineShellConnection(self.cluster.cbas_cc_node)
        output, error = cbas_shell.kill_process("java", "java", signum=9)
        cbas_shell.disconnect()
        if error:
            self.fail("Failed to kill Java process on CBAS node")
        self.log.info("Adding event for process_crashed event")
        self.system_events.add_event(AnalyticsEvents.process_crashed(
            self.cluster.cbas_cc_node.ip, "java"))

        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service failed to start after Java process "
                      "was killed")

        self.log.info("Restarting analytics cluster to trigger process exited event")
        status, _, _ = self.cbas_util.restart_analytics_cluster_uri(
            self.cluster, username=None, password=None)
        if not status:
            self.fail("Failed to restart analytics cluster")
        self.log.info("Adding event for process_exited event")
        self.system_events.add_event(AnalyticsEvents.process_exited(
            self.cluster.cbas_cc_node.ip, "java"))

        if not self.cbas_util.wait_for_cbas_to_recover(self.cluster, 300):
            self.fail("Analytics service failed to start after Java process "
                      "was killed")

    def test_topology_change_events(self):
        available_server_before_rebalance = copy.deepcopy(self.available_servers)
        try:
            self.log.info("Enabling firewall between Incoming node and CBAS CC "
                          "node to trigger topology_change_failed event")
            for node in available_server_before_rebalance:
                RemoteUtilHelper.enable_firewall(
                    node, bidirectional=False, xdcr=False,
                    action_on_packet="REJECT", block_ips=[self.cluster.cbas_cc_node.ip],
                    all_interface=True)

            self.log.info("Rebalancing IN CBAS node to trigger "
                          "topology_change_started event")
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                cbas_nodes_in=1, cbas_nodes_out=0,
                available_servers=self.available_servers, exclude_nodes=[])

            if self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster, check_cbas_running=False):
                raise Exception("Rebalance passed when it should have failed.")

            self.log.info("Disabling firewall between Incoming node and CBAS CC "
                          "node and retriggering rebalance to trigger "
                          "topology_change_completed event")
            for node in available_server_before_rebalance:
                remote_client = RemoteMachineShellConnection(node)
                remote_client.disable_firewall()
                remote_client.disconnect()

            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0,
                cbas_nodes_in=0, cbas_nodes_out=0,
                available_servers=self.available_servers, exclude_nodes=[])

            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster, check_cbas_running=False):
                raise Exception("Rebalance failed even after disabling "
                                "firewall")

            self.log.info("Adding event for topology_change_started event")
            self.system_events.add_event(AnalyticsEvents.topology_change_started(
                self.cluster.cbas_cc_node.ip, 2, 0))

            self.log.info("Adding event for topology_change_failed event")
            self.system_events.add_event(AnalyticsEvents.topology_change_failed(
                self.cluster.cbas_cc_node.ip, 2, 0))

            self.log.info("Adding event for topology_change_completed event")
            self.system_events.add_event(AnalyticsEvents.topology_change_completed(
                self.cluster.cbas_cc_node.ip, 2, 0))
        except Exception as err:
            self.log.info("Disabling Firewall")
            for node in available_server_before_rebalance:
                remote_client = RemoteMachineShellConnection(node)
                remote_client.disable_firewall()
                remote_client.disconnect()
            self.fail(str(err))

    def test_analytics_scope_events(self):
        dataverse_name = CBASHelper.format_name(
            self.cbas_util.generate_name(name_cardinality=2))
        if not self.cbas_util.create_dataverse(
                self.cluster, dataverse_name,
                analytics_scope=random.choice(["True", "False"])):
            self.fail("Error while creating dataverse")
        self.log.info(
            "Adding event for scope_created event")
        self.system_events.add_event(AnalyticsEvents.scope_created(
            self.cluster.cbas_cc_node.ip, CBASHelper.metadata_format(
                dataverse_name)))

        if not self.cbas_util.drop_dataverse(
                self.cluster, dataverse_name,
                analytics_scope=random.choice(["True", "False"])):
            self.fail("Error while dropping dataverse")
        self.log.info("Adding event for scope_dropped event")
        self.system_events.add_event(AnalyticsEvents.scope_dropped(
            self.cluster.cbas_cc_node.ip, CBASHelper.metadata_format(
                dataverse_name)))

    def test_analytics_collection_events(self):
        dataset_objs = self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False,
            no_of_objs=1)
        dataset_objs += self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=True,
            no_of_objs=1)
        for dataset in dataset_objs:
            if dataset.enabled_from_KV:
                if not self.cbas_util.enable_analytics_from_KV(
                        self.cluster, dataset.full_kv_entity_name):
                    self.fail("Error while mapping KV collection to analytics")
                self.system_events.add_event(AnalyticsEvents.collection_mapped(
                    self.cluster.cbas_cc_node.ip, dataset.kv_bucket.name,
                    dataset.kv_scope.name, dataset.kv_collection.name))
                if not self.cbas_util.disable_analytics_from_KV(
                        self.cluster, dataset.full_kv_entity_name):
                    self.fail("Error while unmapping KV collection from "
                              "analytics")
            else:
                if not self.cbas_util.create_dataset(
                        self.cluster, dataset.name, dataset.full_kv_entity_name,
                        dataverse_name=dataset.dataverse_name,
                        analytics_collection=random.choice(["True", "False"])):
                    self.fail("Error while creating analytics collection")
                self.system_events.add_event(AnalyticsEvents.collection_created(
                    self.cluster.cbas_cc_node.ip,
                    CBASHelper.metadata_format(dataset.dataverse_name),
                    CBASHelper.metadata_format(dataset.name),
                    CBASHelper.metadata_format(dataset.dataverse_name),
                    "Local", dataset.kv_bucket.name, dataset.kv_scope.name,
                    dataset.kv_collection.name))
                if not self.cbas_util.drop_dataset(
                        self.cluster, dataset.full_name,
                        analytics_collection=random.choice(["True", "False"])):
                    self.fail("Error while dropping datasets")
            self.system_events.add_event(AnalyticsEvents.collection_dropped(
                self.cluster.cbas_cc_node.ip,
                CBASHelper.metadata_format(dataset.dataverse_name),
                CBASHelper.metadata_format(dataset.name)))

    def test_analytics_index_events(self):
        dataset_obj = self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False,
            no_of_objs=1)[0]
        if not self.cbas_util.create_dataset(
                self.cluster, dataset_obj.name, dataset_obj.full_kv_entity_name,
                dataverse_name=dataset_obj.dataverse_name,
                analytics_collection=random.choice(["True", "False"])):
            self.fail("Error while creating analytics collection")
        index_name = CBASHelper.format_name(
            self.cbas_util.generate_name(name_cardinality=1))
        if not self.cbas_util.create_cbas_index(
                self.cluster, index_name, ["age:bigint"], dataset_obj.full_name,
                analytics_index=random.choice(["True", "False"])):
            self.fail("Error while creating analytics index")

        self.log.info("Adding event for index_created events")
        self.system_events.add_event(AnalyticsEvents.index_created(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.metadata_format(index_name),
            CBASHelper.metadata_format(dataset_obj.name)))

        if not self.cbas_util.drop_cbas_index(
                self.cluster, index_name, dataset_obj.full_name,
                analytics_index=random.choice(["True", "False"])):
            self.fail("Error while dropping analytics index")

        self.log.info("Adding event for index_dropped events")
        self.system_events.add_event(AnalyticsEvents.index_dropped(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.metadata_format(index_name),
            CBASHelper.metadata_format(dataset_obj.name)))

    def test_analytics_synonym_events(self):
        dataset_obj = self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False,
            no_of_objs=1)[0]
        if not self.cbas_util.create_dataset(
                self.cluster, dataset_obj.name,
                dataset_obj.full_kv_entity_name,
                dataverse_name=dataset_obj.dataverse_name,
                analytics_collection=random.choice(["True", "False"])):
            self.fail("Error while creating analytics collection")

        syn_name_1 = CBASHelper.format_name(
            self.cbas_util.generate_name(name_cardinality=1))
        if not self.cbas_util.create_analytics_synonym(
            self.cluster, CBASHelper.format_name(
                    dataset_obj.dataverse_name, syn_name_1), dataset_obj.full_name):
            self.fail("Error while creating Synonym")

        self.log.info("Adding event for synonym_created event")
        self.system_events.add_event(AnalyticsEvents.synonym_created(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.metadata_format(syn_name_1),
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.metadata_format(dataset_obj.name)))

        syn_name_2 = CBASHelper.format_name(
            self.cbas_util.generate_name(name_cardinality=1))
        self.log.info("Creating dangling Synonym")
        if not self.cbas_util.create_analytics_synonym(
            self.cluster, CBASHelper.format_name(
                    dataset_obj.dataverse_name, syn_name_2), "dangling"):
            self.fail("Error while creating Synonym")
        self.log.info("Adding event for synonym_created event for dangling "
                      "synonym")
        self.system_events.add_event(AnalyticsEvents.synonym_created(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.metadata_format(syn_name_2),
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.metadata_format("dangling")))

        for syn_name in [syn_name_1, syn_name_2]:
            if not self.cbas_util.drop_analytics_synonym(
                self.cluster, CBASHelper.format_name(
                    dataset_obj.dataverse_name, syn_name)):
                self.fail("Error while dropping synonym")

            self.log.info("Adding event for synonym_dropped events")
            self.system_events.add_event(AnalyticsEvents.synonym_dropped(
                self.cluster.cbas_cc_node.ip,
                CBASHelper.metadata_format(dataset_obj.dataverse_name),
                CBASHelper.metadata_format(syn_name)))

    def test_analytics_collection_attach_dettach_events(self):
        dataset_obj = self.cbas_util.create_dataset_obj(
            self.cluster, self.bucket_util, dataset_cardinality=3,
            bucket_cardinality=3, enabled_from_KV=False,
            no_of_objs=1, exclude_collection=["_default"])[0]
        if not self.cbas_util.create_dataset(
                self.cluster, dataset_obj.name,
                dataset_obj.full_kv_entity_name,
                dataverse_name=dataset_obj.dataverse_name,
                analytics_collection=random.choice(["True", "False"])):
            self.fail("Error while creating analytics collection")

        self.log.info("Dropping collection {0}".format(
            dataset_obj.full_kv_entity_name))
        self.bucket_util.drop_collection(
            self.cluster.master, dataset_obj.kv_bucket,
            scope_name=dataset_obj.kv_scope.name,
            collection_name=dataset_obj.kv_collection.name, session=None)
        if not self.cbas_util.wait_for_ingestion_complete(
                self.cluster, dataset_obj.full_name, 0, timeout=300):
            self.fail("Data is present in the dataset when it should not")

        self.log.info("Creating collection {0}".format(
            dataset_obj.full_kv_entity_name))
        self.bucket_util.create_collection(
            self.cluster.master, dataset_obj.kv_bucket,
            scope_name=dataset_obj.kv_scope.name,
            collection_spec=dataset_obj.kv_collection.get_dict_object(),
            session=None)
        if not self.cbas_util.wait_for_ingestion_complete(
                self.cluster, dataset_obj.full_name, 0, timeout=300):
            self.fail("Data ingestion failed.")

        self.log.info("Adding event for collection_detach events")
        self.system_events.add_event(AnalyticsEvents.collection_detached(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.unformat_name(dataset_obj.name)))
        self.log.info("Adding event for collection_attach events")
        self.system_events.add_event(AnalyticsEvents.collection_attached(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format(dataset_obj.dataverse_name),
            CBASHelper.unformat_name(dataset_obj.name)))

    def test_analytics_settings_change_events(self):
        status, content, response = \
            self.cbas_util.fetch_service_parameter_configuration_on_cbas(
                self.cluster)
        if not status:
            self.fail("Error while fetching the analytics service config")

        old_value = content["jobHistorySize"]
        new_value = 10

        status, content, response = \
            self.cbas_util.update_service_parameter_configuration_on_cbas(
                self.cluster, config_map={"jobHistorySize": 10})
        if not status:
            self.fail("Error while setting the analytics service config")

        self.log.info("Adding event for settings_change events")
        self.system_events.add_event(AnalyticsEvents.setting_changed(
            self.cluster.cbas_cc_node.ip, "jobHistorySize", old_value, new_value))
