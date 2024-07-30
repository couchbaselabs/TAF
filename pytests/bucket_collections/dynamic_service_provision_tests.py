from random import sample

from bucket_collections.collections_base import CollectionBase
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.connection import CBRestConnection
from py_constants import CbServer
from rebalance_utils.rebalance_util import RebalanceUtil


class DynamicServiceProvisionTests(CollectionBase):
    def setUp(self):
        super(DynamicServiceProvisionTests, self).setUp()

    def tearDown(self):
        super(DynamicServiceProvisionTests, self).tearDown()

    def __validate_service_map_against_cluster(self, expected_node_config):
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

        okay = True
        num_kv_nodes = len(self.cluster.kv_nodes)
        num_index_nodes = len(self.cluster.index_nodes)
        num_query_nodes = len(self.cluster.query_nodes)
        num_fts_nodes = len(self.cluster.fts_nodes)
        num_cbas_nodes = len(self.cluster.cbas_nodes)
        num_eventing_nodes = len(self.cluster.eventing_nodes)
        num_backup_nodes = len(self.cluster.backup_nodes)
        num_dummy_nodes = 0

        expected_kv_nodes = expected_node_config[CbServer.Services.KV]
        expected_index_nodes = expected_node_config[CbServer.Services.INDEX]
        expected_query_nodes = expected_node_config[CbServer.Services.N1QL]
        expected_fts_nodes = expected_node_config[CbServer.Services.FTS]
        expected_cbas_nodes = expected_node_config[CbServer.Services.CBAS]
        expected_eventing_nodes = \
            expected_node_config[CbServer.Services.EVENTING]
        expected_backup_nodes = expected_node_config[CbServer.Services.BACKUP]
        expected_serviceless_nodes = \
            expected_node_config[CbServer.Services.SERVICELESS]
        if num_kv_nodes != expected_kv_nodes:
            self.log.critical(f"KV nodes:: Expected: {expected_kv_nodes},"
                              f"Actual: {num_kv_nodes}")
            okay = False
        if num_index_nodes != expected_index_nodes:
            self.log.critical(f"Index nodes:: "
                              f"Expected: {expected_index_nodes},"
                              f"Actual: {num_index_nodes}")
            okay = False
        if num_query_nodes != expected_query_nodes:
            self.log.critical(f"Query nodes:: "
                              f"Expected: {expected_query_nodes},"
                              f"Actual: {num_query_nodes}")
            okay = False
        if num_fts_nodes != expected_fts_nodes:
            self.log.critical(f"FTS nodes:: Expected: {expected_fts_nodes},"
                              f"Actual: {num_fts_nodes}")
            okay = False
        if num_cbas_nodes != expected_cbas_nodes:
            self.log.critical(f"CBAS nodes:: Expected: {expected_cbas_nodes},"
                              f"Actual: {num_cbas_nodes}")
            okay = False
        if num_eventing_nodes != expected_eventing_nodes:
            self.log.critical(f"Eventing nodes:: "
                              f"Expected: {expected_eventing_nodes},"
                              f"Actual: {num_eventing_nodes}")
            okay = False
        if num_backup_nodes != expected_backup_nodes:
            self.log.critical(f"Backup nodes::"
                              f"Expected: {expected_backup_nodes},"
                              f"Actual: {num_backup_nodes}")
            okay = False
        if num_dummy_nodes != expected_serviceless_nodes:
            self.log.critical(f"Serviceless nodes:: "
                              f"Expected: {expected_serviceless_nodes},"
                              f"Actual: {num_dummy_nodes}")
            okay = False
        self.assertTrue(okay, "Service topology mismatch")

    def test_topology_keys(self):
        """
        - Test 'kv' service which is not supported
        - Multiple services in single key
        - Invalid service name
        - Check topology takes all valid services as keys
        - Passing invalid otp nodes like a list
        - Test with duplicate topology params
        """
        valid_services = [CbServer.Services.N1QL,
                          CbServer.Services.INDEX,
                          CbServer.Services.EVENTING,
                          CbServer.Services.FTS,
                          CbServer.Services.CBAS,
                          CbServer.Services.BACKUP]
        nodes = self.cluster_util.get_nodes(self.cluster.master)
        num_nodes = len(nodes)
        exception = None
        reb_util = RebalanceUtil(self.cluster.master)
        cluster_rest = ClusterRestAPI(self.cluster.master)
        known_nodes = [node.id for node in nodes]

        expected_service_config = {
            CbServer.Services.KV: 1,
            CbServer.Services.N1QL: 0,
            CbServer.Services.INDEX: 0,
            CbServer.Services.EVENTING: 0,
            CbServer.Services.FTS: 0,
            CbServer.Services.CBAS: 0,
            CbServer.Services.BACKUP: 0,
            CbServer.Services.SERVICELESS: self.nodes_init-1
        }

        rest = CBRestConnection()
        rest.set_server_values(self.cluster.master)
        rest.set_endpoint_urls(self.cluster.master)
        api = rest.base_url + "/controller/rebalance"

        self.log.info("Test topology[kv]")
        expected_err = "Cannot change topology for data service"
        t_known_nodes = ",".join(known_nodes)
        param_str = f"knownNodes={t_known_nodes}&topology[kv]={known_nodes[0]}"
        status, content, _ = rest.request(api, rest.POST, param_str)
        self.assertFalse(status, f"Able to trigger rebalance with kv service")
        self.assertEqual(content, expected_err, "Mismatch in server response")

        # Duplicate Key tests
        for service_name in valid_services:
            self.log.info(f"Test duplicate topology key: {service_name}")
            param_str = f"knownNodes={t_known_nodes}&"\
                + f"topology[{service_name}]={known_nodes[0]}&"\
                + f"topology[{service_name}]={known_nodes[0]}"

            status, content, _ = rest.request(api, rest.POST, param_str)
            self.assertFalse(status, f"Able to trigger rebalance with "
                                     f"duplicate topology[{service_name}]")
            self.assertEqual(
                content, b"Duplicate topology parameters",
                "Mismatch in error message for duplicate topology")

        self.log.info("Resetting all services apart from KV")
        service_topology = {}
        for service_name in valid_services:
            if service_name == CbServer.Services.KV:
                service_topology[service_name] = nodes[0].id
            else:
                service_topology[service_name] = ""
        okay = self.task.rebalance(self.cluster, [], [],
                                   service_topology=service_topology)
        self.assertTrue(okay, "Rebalance failed for resetting services")

        self.log.info("Testing multiple_services in single key")
        topology_key = ",".join(sample(valid_services, 2))
        service_topology = {topology_key: nodes[0].id}
        okay, content = cluster_rest.rebalance(known_nodes,
                                               topology=service_topology)
        if okay:
            exception = f"Multiple topology key {topology_key} was accepted"
            self.log.critical(exception)
            self.assertTrue(reb_util.monitor_rebalance(), "Rebalance failed")
            self.cluster_util.update_cluster_nodes_service_list(self.cluster)

        self.__validate_service_map_against_cluster(
            expected_service_config)

        self.log.info("Testing invalid topology key")
        topology_key = "invalid_service"
        service_topology = {topology_key: nodes[0].id}
        okay, content = cluster_rest.rebalance(known_nodes,
                                               topology=service_topology)
        if okay:
            exception = f"Multiple topology key {topology_key} was accepted"
            self.log.critical(exception)
            self.assertTrue(reb_util.monitor_rebalance(), "Rebalance failed")
            self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        else:
            if content != f"Unknown service {topology_key}":
                exception = f"Invalid message: {content}"
                self.log.critical(exception)
        self.__validate_service_map_against_cluster(
            expected_service_config)

        # Add services to the node
        if num_nodes != 1:
            expected_service_config[CbServer.Services.SERVICELESS] -= 1
        for service_name in valid_services:
            self.log.info(f"Adding service key {service_name}")
            if num_nodes == 1:
                service_topology = {service_name: nodes[0].id}
                if service_name != CbServer.Services.KV:
                    expected_service_config[service_name] += 1
            else:
                service_topology = {service_name: nodes[1].id}
                expected_service_config[service_name] += 1

            okay, content = cluster_rest.rebalance(known_nodes,
                                                   topology=service_topology)
            if okay:
                self.assertTrue(reb_util.monitor_rebalance(),
                                "Topology rebalance trigger failed")
            else:
                exception = f"Service update rebalance failed: {content}"
                self.log.critical(exception)
            self.__validate_service_map_against_cluster(
                expected_service_config)

        # Remove services from the cluster
        for service_name in valid_services:
            self.log.info(f"Removing service key {service_name}")
            if service_name == CbServer.Services.KV:
                service_topology = {service_name: nodes[0].id}
                if service_name != CbServer.Services.KV:
                    expected_service_config[service_name] -= 1
            else:
                service_topology = {service_name: ""}
                expected_service_config[service_name] -= 1

            okay, content = cluster_rest.rebalance(known_nodes,
                                                   topology=service_topology)
            if okay:
                self.assertTrue(reb_util.monitor_rebalance(),
                                "Topology rebalance failed")
            else:
                exception = f"Service update rebalance failed: {content}"
                self.log.critical(exception)

        if num_nodes != 1:
            expected_service_config[CbServer.Services.SERVICELESS] += 1
        self.__validate_service_map_against_cluster(
            expected_service_config)

        if exception:
            self.fail(exception)

    def test_topology_otp_node_value(self):
        """
        Negative tests
        - Pass invalid chars in otp_node (; ' " [ ])
        - Duplicate values within otp_nodes
        - Pass otp_nodes within '[]'
        """
        valid_services = [CbServer.Services.N1QL,
                          CbServer.Services.INDEX,
                          CbServer.Services.EVENTING,
                          CbServer.Services.FTS,
                          CbServer.Services.CBAS,
                          CbServer.Services.BACKUP]
        nodes = self.cluster_util.get_nodes(self.cluster.master)
        known_nodes = [node.id for node in nodes]
        rest = CBRestConnection()
        rest.set_server_values(self.cluster.master)
        rest.set_endpoint_urls(self.cluster.master)
        api = rest.base_url + "/controller/rebalance"
        t_known_nodes = ",".join(known_nodes)
        otp_node = known_nodes[0]
        for service_name in valid_services:
            invalid_otp_str = otp_node.replace("@", ";")
            duplicate_otp_str = f"{otp_node},{otp_node}"

            self.log.info(f"Testing topology[{service_name}]")
            # Test with invalid chars within otpNodes
            param_str = f"knownNodes={t_known_nodes}&" \
                        + f"topology[{service_name}]={invalid_otp_str}"
            status, content, _ = rest.request(api, rest.POST, param_str)
            self.assertFalse(status,
                             f"Rebalance triggered with invalid chars")
            self.assertEqual(
                content, b'Unknown or ejected nodes ["ns_1"]',
                "Mismatch in error message for invalid char")

            # Duplicate otp nodes within topology param
            param_str = f"knownNodes={t_known_nodes}&" \
                        + f"topology[{service_name}]={duplicate_otp_str}"
            status, content, _ = rest.request(api, rest.POST, param_str)
            self.assertFalse(status,
                             f"Rebalance triggered with duplicate otp_nodes")
            self.assertEqual(
                content, f'Unknown or ejected nodes ["{otp_node}"]'.encode(),
                "Mismatch in error message for duplicate topology")

            # otpNodes passed within '[]' chars
            param_str = f"knownNodes={t_known_nodes}&" \
                        + f"topology[{service_name}]=[{otp_node}]"
            status, content, _ = rest.request(api, rest.POST, param_str)
            self.assertFalse(status,
                             f"Rebalance triggered with [{otp_node}]")
            self.assertEqual(
                content, f'Unknown or ejected nodes ["[{otp_node}]"]'.encode(),
                f"Mismatch in error message for topology '[{otp_node}]'")

    def test_memory_violations(self):
        """
        Configure all memory to kv & add a service so that add fails
        """
        reb_util = RebalanceUtil(self.cluster.master)
        services_req_mem = [CbServer.Services.INDEX,
                            CbServer.Services.CBAS,
                            CbServer.Services.FTS,
                            CbServer.Services.EVENTING]
        nodes = self.cluster_util.get_nodes(self.cluster.master)
        known_nodes = [node.id for node in nodes]
        cb_rest = ClusterRestAPI(self.cluster.master)
        status, content = cb_rest.node_details()
        if not status:
            self.fail("Failed to fetch rest::nodes_details")

        self.log.info("Setting max memory for 'kv' service")
        total_mem = content["mcdMemoryReserved"]
        cb_rest.configure_memory({CbServer.Settings.KV_MEM_QUOTA: total_mem})

        expected_str = ("This server does not have sufficient memory to "
                        "support requested memory quota")
        for service_name in services_req_mem:
            self.log.info(f"Trying to add service {service_name}")
            status, content = cb_rest.rebalance(
                known_nodes, topology={service_name: known_nodes[0]})
            self.assertFalse(
                status, f"Rebalance started for {service_name}: {content}")
            self.assertTrue('total_quota_too_high' in content,
                            f"'total_quota_too_high' not present: {content}")
            self.assertTrue(expected_str in content["total_quota_too_high"],
                            f"Mismatch in error str: {content}")

        self.log.info("Setting mem_req for query service=0")
        status, _ = cb_rest.configure_memory(
            {CbServer.Settings.N1QL_MEM_QUOTA: 0})
        self.assertTrue(status, "Failed to update N1ql memory")
        self.log.info(f"Trying to add service {CbServer.Services.N1QL}")
        status, content = cb_rest.rebalance(
            known_nodes, topology={CbServer.Services.N1QL: known_nodes[0]})
        self.assertTrue(status,
                        f"Rebalance not started for query service: {content}")
        self.assertTrue(reb_util.monitor_rebalance(), "Rebalance failed")

        self.log.info("Removing N1QL service")
        status, content = cb_rest.rebalance(
            known_nodes, topology={CbServer.Services.N1QL: ""})
        self.assertTrue(status,
                        f"Rebalance not started for query service: {content}")
        self.assertTrue(reb_util.monitor_rebalance(), "Rebalance failed")

        self.log.info("Setting mem_req for query service=256")
        status, _ = cb_rest.configure_memory(
            {CbServer.Settings.N1QL_MEM_QUOTA: 256})
        self.assertTrue(status, "Failed to update N1ql memory")

        self.log.info(f"Trying to add service {CbServer.Services.N1QL}")
        status, content = cb_rest.rebalance(
            known_nodes, topology={CbServer.Services.N1QL: known_nodes[0]})
        self.assertFalse(
            status, f"Rebalance started for {CbServer.Services.N1QL}: {content}")
        self.assertTrue('total_quota_too_high' in content,
                        f"'total_quota_too_high' not present: {content}")
        self.assertTrue(expected_str in content["total_quota_too_high"],
                        f"Mismatch in error str: {content}")
