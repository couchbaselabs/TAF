from Cb_constants import CbServer
from couchbase_utils.cb_tools.cb_cli import CbCli
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from pytests.bucket_collections.collections_base import CollectionBase
from membase.api.rest_client import RestConnection
from security_config import trust_all_certs


class EnforceTls(CollectionBase):
    def setUp(self):
        super(CollectionBase, self).setUp()
        self.sample_urls_map = \
            {"http://%s:8091/nodes/self": "https://%s:18091/nodes/self",
             "http://%s:9102/api/v1/stats": "https://%s:19102/api/v1/stats",
             "http://%s:8093/admin/clusters": "https://%s:18093/admin/clusters",
             "http://%s:8094/api/cfg": "https://%s:18094/api/cfg",
             "http://%s:8096/api/v1/functions": "https://%s:18096/api/v1/functions",
             "http://%s:8095/analytics/node/agg/stats/remaining":
                 "https://%s:18095/analytics/node/agg/stats/remaining"}

        self.log.info("Disabling AF on all nodes before beginning the test")
        for node in self.cluster.servers:
            status = RestConnection(node).update_autofailover_settings(False, 120, False)
            self.assertTrue(status)
        self.log.info("Changing security settings to trust all CAs")
        trust_all_certs()

    def tearDown(self):
        self.disable_n2n_encryption_cli_on_nodes(nodes=self.cluster.servers)
        super(CollectionBase, self).tearDown()

    def set_n2n_encryption_level_on_nodes(self, nodes, level="control"):
        for node in nodes:
            self.log.info("Enabling n2n encryption and setting level to "
                          "{0} on node {1}".format(level, node))
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            cb_cli.enable_n2n_encryption()
            cb_cli.set_n2n_encryption_level(level=level)
            shell_conn.disconnect()

    def disable_n2n_encryption_cli_on_nodes(self, nodes):
        self.set_n2n_encryption_level_on_nodes(nodes=nodes, level="control")
        self.log.info("Disabling n2n encryption on all nodes")
        for node in nodes:
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            o = cb_cli.disable_n2n_encryption()
            self.log.info(o)
            shell_conn.disconnect()

    def enable_tls_encryption_cli_on_nodes(self, nodes):
        self.set_n2n_encryption_level_on_nodes(nodes=nodes, level="strict")

    @staticmethod
    def get_encryption_level_on_node(node):
        shell_conn = RemoteMachineShellConnection(node)
        cb_cli = CbCli(shell_conn, no_ssl_verify=True)
        level = cb_cli.get_n2n_encryption_level()
        shell_conn.disconnect()
        return level

    def test_add_strict_node_to_non_strict_node(self):
        """
        1. Don't enforce tls on master node
        2. Enforce tls on another node of a cluster
        3. Add the other node to master node's cluster and rebalance
        """
        non_strict_node = self.cluster.master
        strict_node = self.cluster.servers[self.nodes_init:self.nodes_init + 1][0]
        self.enable_tls_encryption_cli_on_nodes \
            (nodes=self.cluster.servers[self.nodes_init:self.nodes_init + 1])
        CbServer.use_https = True
        RestConnection(non_strict_node).add_node(user='Administrator', password='password',
                                                 port=CbServer.ssl_port,
                                                 remoteIp=strict_node.ip)
        CbServer.use_https = False
        rest = RestConnection(non_strict_node)
        nodes = rest.node_statuses()
        _ = rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[])
        result = rest.monitorRebalance()
        self.assertTrue(result, "Rebalance failed")

    def test_non_ssl_ports_after_enabling_tls(self):
        """
        1. Enforce TLS on cluster
        2. For each component make a GET request on non-ssl port,
        and validate that it fails.
        3. Make the same above request on TLS port and validate that it works
        4. Repeat for all components
        """
        self.enable_tls_encryption_cli_on_nodes(nodes=[self.cluster.master])
        for non_ssl_request in self.sample_urls_map.keys():
            api = non_ssl_request % self.cluster.master.ip
            try:
                CbServer.use_https = False
                rest = RestConnection(self.cluster.master)
                rest._http_request(api=api, timeout=10)
            except Exception as _:
                CbServer.use_https = True
                rest = RestConnection(self.cluster.master)
                ssl_request = self.sample_urls_map[non_ssl_request]
                api = ssl_request % self.cluster.master.ip
                status, content, response = rest._http_request(api=api, timeout=10)
                if not status:
                    self.fail("{0} failed".format(api))
            else:
                self.fail("{0} worked".format(api))

    def test_all_encrypted_and_non_encrypted_ports(self):
        """
        Enforce TLS on cluster
        check if services obey TLS
        """
        self.enable_tls_encryption_cli_on_nodes(nodes=[self.cluster.master])
        port_map = {"4369": None, "8091": "18091", "8092": "18092",
                    "8093": "18093", "8094": "18094", "9100": None,
                    "9101": None, "9102": "19102", "9103": None, "9104": None,
                    "9105": None, "9110": None, "9111": None, "9112": None,
                    "9113": None, "9114": None, "9115": None, "9116": None,
                    "9117": None, "9118": None, "9120": None, "9121": None,
                    "9122": None, "9130": "19130", "11209": None, "11210": "11207",
                    "21100": "21150", "8095": "18095", "8096": "18096", "8097": "18097",
                    "11211": "11207"}
        # ToDO assertTrue on status
        status = self.cluster_util.check_if_services_obey_tls(servers=[self.cluster.master],
                                                              port_map=port_map)

    def test_check_tls_after_restarting_nodes(self):
        """
        1. Enforce tls on the cluster
        2. Restart couchabse server on all nodes
        3. Validate the tls setting has persisted after restart
        """
        self.enable_tls_encryption_cli_on_nodes(nodes=[self.cluster.master])
        self.log.info("Restarting servers on nodes {0}".
                      format(self.cluster.servers[:self.nodes_init]))
        for node in self.cluster.servers[:self.nodes_init]:
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
        self.sleep(15, "Wait after restart of servers")
        for node in self.cluster.servers[:self.nodes_init]:
            level = self.get_encryption_level_on_node(node=node)
            if level != "strict":
                self.fail("Node {0} expected strict actual {1}".format(node, level))

    def test_enforce_tls_by_invalid_user(self):
        """
        1. Set cluster encryption level to control
        2. Create a cluster admin user
        3. Attempt to change cluster encryption level to strict
        using the cluster admin. Validate that it fails
        """
        self.set_n2n_encryption_level_on_nodes(nodes=[self.cluster.master], level="control")
        rbac_util = RbacUtils(self.cluster.master)
        self.log.info("Create a user with role cluster admin")
        rbac_util._create_user_and_grant_role("cluster_admin", "cluster_admin")
        rest = RestConnection(self.cluster.master)
        rest.username = "cluster_admin"
        rest.password = "cluster_admin"
        try:
            _ = rest.set_encryption_level(level="strict")
        except Exception as e:
            self.log.info("Enforcing TLS by invalid user failed as expected {0} ".format(e))
        else:
            self.fail("Enforcing TLS by invalid user did not fail")
