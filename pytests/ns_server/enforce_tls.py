import copy
import random

from BucketLib.bucket import TravelSample
from Cb_constants import CbServer
from couchbase_utils.cb_tools.cb_cli import CbCli
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.x509main import x509main
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from pytests.basetestcase import BaseTestCase
from security_config import trust_all_certs


class EnforceTls(BaseTestCase):
    def setUp(self):
        super(EnforceTls, self).setUp()
        self.sample_urls_map = \
            {"http://%s:8091/nodes/self": "https://%s:18091/nodes/self",
             "http://%s:9102/api/v1/stats": "https://%s:19102/api/v1/stats",
             "http://%s:8093/admin/clusters": "https://%s:18093/admin/clusters",
             "http://%s:8094/api/cfg": "https://%s:18094/api/cfg",
             "http://%s:8096/api/v1/functions": "https://%s:18096/api/v1/functions",
             "http://%s:8095/analytics/node/agg/stats/remaining":
                 "https://%s:18095/analytics/node/agg/stats/remaining"}
        self.port_map = {"8091": "18091", "8092": "18092",
                         "8093": "18093", "8094": "18094",
                         "9102": "19102", "9130": "19130", "11209": "11206", "11210": "11207",
                         "21100": "21150", "8095": "18095", "8096": "18096", "8097": "18097",
                         "11211": "11207"}

        # Make only 1 node provisioned
        nodes_rem = self.cluster.servers[1:]
        result = self.task.rebalance([self.cluster.master], nodes_rem, [])
        if result is False:
            self.fail("Initial rebalance1 failed")
        result = self.task.rebalance(self.cluster.servers, [], nodes_rem)
        if result is False:
            self.fail("Initial rebalance2 failed")

        # Initial rebalance for nodes_init (it is not done in basetest)
        services = None
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                services.append(service.replace(":", ","))
        services = services[1:] \
            if services is not None and len(services) > 1 else None
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance([self.cluster.master], nodes_init, [],
                                         services=services)
            if result is False:
                self.fail("Initial rebalance3 failed")

            self.cluster.nodes_in_cluster.extend(nodes_init)

        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]
        self.nodes_in_cluster_excluding_master = list(set(self.nodes_in_cluster) -
                                                      {self.cluster.master})

        self.log.info("Disabling AF on all nodes before beginning the test")
        for node in self.cluster.servers:
            status = RestConnection(node).update_autofailover_settings(False, 120, False)
            self.assertTrue(status)
        self.x509enable = self.input.param("x509enable", True)
        if self.x509enable:
            self.generate_x509_certs()
            self.upload_x509_certs()
        self.log.info("Changing security settings to trust all CAs")
        trust_all_certs()
        self.bucket_util.load_sample_bucket(TravelSample())

    def tearDown(self):
        self.disable_n2n_encryption_cli_on_nodes(nodes=self.cluster.servers)
        if self.x509enable:
            self.teardown_x509_certs()
        super(BaseTestCase, self).tearDown()

    def generate_x509_certs(self):
        """
        Generates x509 root, node, client cert on all servers of the cluster
        """
        # Input parameters for state, path, delimeters and prefixes
        self.client_cert_state = self.input.param("client_cert_state",
                                                  "enable")
        self.paths = self.input.param(
            'paths', "subject.cn:san.dnsname:san.uri").split(":")
        self.prefixs = self.input.param(
            'prefixs', 'www.cb-:us.:www.').split(":")
        self.delimeters = self.input.param('delimeter', '.:.:.').split(":")
        self.setup_once = self.input.param("setup_once", True)
        self.client_ip = self.input.param("client_ip", "172.16.1.174")

        copy_servers = copy.deepcopy(self.cluster.servers)
        x509main(self.cluster.master)._generate_cert(copy_servers, type='openssl',
                                                     encryption='',
                                                     key_length=1024,
                                                     client_ip=self.client_ip)

    def upload_x509_certs(self, servers=None):
        """
        1. Uploads root certs and client-cert settings on servers
        2. Uploads node certs on servers
        """
        if servers is None:
            servers = self.cluster.servers
        self.log.info("Uploading root cert to servers {0}".format(servers))
        for server in servers:
            x509main(server).setup_master(self.client_cert_state, self.paths,
                                          self.prefixs,
                                          self.delimeters)
        self.sleep(5, "Sleeping before uploading node certs to nodes {0}".
                   format(servers))
        x509main().setup_cluster_nodes_ssl(servers, reload_cert=True)

    def teardown_x509_certs(self):
        """
        1. Regenerates root cert and removes node certs and client
            cert settings from server
        2. Removes certs folder from slave
        """
        self._reset_original()
        shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
        self.log.info("Removing folder {0} from slave".
                      format(x509main.CACERTFILEPATH))
        self.log.info("Removing folder {0} from slave".
                      format(x509main.CACERTFILEPATH))
        shell.execute_command("rm -rf " + x509main.CACERTFILEPATH)

    def _reset_original(self):
        """
        1. Regenerates root cert on all servers
        2. Removes inbox folder (node certs) from all VMs
        """
        self.log.info(
            "Reverting to original state - regenerating certificate "
            "and removing inbox folder")
        tmp_path = "/tmp/abcd.pem"
        for servers in self.cluster.servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(servers)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(
                cli_command=cli_command, options=options,
                cluster_host=servers.ip, user="Administrator",
                password="password")
            x509main(servers)._delete_inbox_folder()

    def set_n2n_encryption_level_on_nodes(self, nodes, level="control"):
        for node in nodes:
            self.log.info("Enabling n2n encryption on node {0}".format(node))
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            o = cb_cli.enable_n2n_encryption()
            self.log.info(o)
            self.log.info(" setting level to "
                          "{0} on node {1}".format(level, node))
            o = cb_cli.set_n2n_encryption_level(level=level)
            self.log.info(o)
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

    def enable_tls_encryption_on_nodes(self, nodes):
        self.set_n2n_encryption_level_on_nodes(nodes)
        for node in nodes:
            self.log.info("Setting encryption level to strict on node {0}".format(node.ip))
            RestConnection(node).set_encryption_level(level="strict")

    @staticmethod
    def get_encryption_level_on_node(node):
        shell_conn = RemoteMachineShellConnection(node)
        cb_cli = CbCli(shell_conn, no_ssl_verify=True)
        level = cb_cli.get_n2n_encryption_level()
        shell_conn.disconnect()
        return level

    def validate_tls_ports(self, servers=None):
        if servers is None:
            servers = [self.cluster.master]
        status = self.cluster_util.check_if_services_obey_tls(servers=servers,
                                                              port_map=self.port_map)
        self.assertTrue(status, "services did not obey tls")

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def set_nodes(self, add_nodes=None, remove_nodes=None):
        """
        take cares of setting node_in_cluster and available_servers objects
        """
        if add_nodes is None:
            add_nodes = []
        if remove_nodes is None:
            remove_nodes = []
        for node in add_nodes:
            self.nodes_in_cluster.append(node)
            self.available_servers.remove(node)
        for node in remove_nodes:
            self.available_servers.append(node)
            self.nodes_in_cluster.remove(node)
        self.nodes_in_cluster_excluding_master = list(set(self.nodes_in_cluster) -
                                                      {self.cluster.master})

    def test_add_strict_node_to_non_strict_node(self):
        """
        1. Don't enforce tls on master node
        2. Enforce tls on another node of a cluster
        3. Add the other node to master node's cluster and rebalance
        """
        non_strict_node = self.cluster.master
        strict_node = self.cluster.servers[self.nodes_init:self.nodes_init + 1][0]
        self.enable_tls_encryption_on_nodes \
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
        5. Disable n2n encryption on all nodes
        6. For each component make a GET request on non-ssl port,
        and validate that it works
        """
        self.enable_tls_encryption_on_nodes(nodes=[self.cluster.master])
        CbServer.use_https = True
        rest = RestConnection(self.cluster.master)
        for non_ssl_request in self.sample_urls_map.keys():
            api = non_ssl_request % self.cluster.master.ip
            try:
                rest._http_request(api=api, timeout=10)
            except Exception as _:
                ssl_request = self.sample_urls_map[non_ssl_request]
                api = ssl_request % self.cluster.master.ip
                status, content, response = rest._http_request(api=api, timeout=10)
                if not status:
                    self.fail("{0} failed".format(api))
            else:
                self.log.error("{0} worked".format(api))
        self.validate_tls_ports(servers=self.cluster.servers[:self.nodes_init])

        self.disable_n2n_encryption_cli_on_nodes(nodes=[self.cluster.master])
        CbServer.use_https = False
        rest = RestConnection(self.cluster.master)
        for non_ssl_request in self.sample_urls_map.keys():
            api = non_ssl_request % self.cluster.master.ip
            status, content, response = rest._http_request(api=api, timeout=10)
            if not status:
                self.fail("{0} api failed with content {1}".format(api, content))

    def test_all_encrypted_and_non_encrypted_ports(self):
        """
        Enforce TLS on cluster with all services in the cluster
        check if services obey TLS
        """
        self.enable_tls_encryption_on_nodes(nodes=[self.cluster.master])
        self.validate_tls_ports(servers=self.cluster.servers[:self.nodes_init])

    def test_check_tls_after_restarting_nodes(self):
        """
        1. Enforce tls on the cluster
        2. Restart couchabse server on all nodes
        3. Validate the tls setting has persisted after restart
        """
        self.enable_tls_encryption_on_nodes(nodes=[self.cluster.master])
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
        self.validate_tls_ports(servers=self.cluster.servers[:self.nodes_init])

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

    def test_rebalance_with_security_options(self):
        """
        0. Enable/Disable x509 certs (root, node certs) with client cert enabled/disabled
            (done in setup)
        1. Disable n2n encryption
        2. Rebalance-in nodes
        3. Rebalance-out nodes
        4. Enable n2n encryption and repeat steps 2-3
        """
        for level in ["disable", "strict"]:
            add_nodes = random.sample(self.available_servers, self.nodes_in)
            self.upload_x509_certs(servers=add_nodes)
            if level in ["strict", "control", "all"]:
                if level == "strict":
                    self.enable_tls_encryption_on_nodes(nodes=self.cluster.servers)
                else:
                    self.set_n2n_encryption_level_on_nodes(nodes=[self.cluster.master],
                                                           level=level)
            self.log.info("Performing rebalance operation with x509: {0}, n2nencryption: {1}".
                          format(self.x509enable, level))
            operation = self.task.async_rebalance(self.nodes_in_cluster, add_nodes, [],
                                                  services=["kv,n1ql,index,cbas,fts,eventing"])
            self.wait_for_rebalance_to_complete(operation)
            self.set_nodes(add_nodes=add_nodes)

            remove_nodes = random.sample(self.nodes_in_cluster_excluding_master,
                                         self.nodes_out)
            operation = self.task.async_rebalance(self.nodes_in_cluster, [], remove_nodes)
            self.wait_for_rebalance_to_complete(operation)
            self.set_nodes(remove_nodes=remove_nodes)
        self.validate_tls_ports(servers=self.cluster.servers[:self.nodes_init])