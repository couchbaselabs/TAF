import copy
import random

from couchbase_utils.cb_tools.cb_cli import CbCli
from couchbase_utils.security_utils.x509main import x509main
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from pytests.bucket_collections.collections_base import CollectionBase


class N2nEncryptionX509(CollectionBase):
    def setUp(self):
        super(N2nEncryptionX509, self).setUp()

        self.x509enable = self.input.param("x509enable", True)
        if self.x509enable:
            self.generate_x509_certs()
            self.upload_x509_certs()
        self.n2n_encryption_level = self.input.param("n2n_encryption_level", "control")

        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.nodes_in
        self.data_load_spec = self.input.param("data_load_spec", "volume_test_load")

        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]
        self.nodes_in_cluster_excluding_master = list(set(self.nodes_in_cluster) -
                                                      {self.cluster.master})

    def tearDown(self):
        self.disable_n2n_encryption_cli_on_nodes(nodes=[self.cluster.master])
        self.teardown_x509_certs()
        super(CollectionBase, self).tearDown()

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
        self.log.info("Enabling n2n encryption and setting level to "
                      "{0} on nodes {1}".format(level, nodes))
        for node in nodes:
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            cb_cli.enable_n2n_encryption()
            cb_cli.set_n2n_encryption_level(level=level)
            shell_conn.disconnect()

    def disable_n2n_encryption_cli_on_nodes(self, nodes):
        self.set_n2n_encryption_level_on_nodes(nodes=nodes, level="control")
        self.log.info("Disabling n2n encryption on nodes {0}".format(nodes))
        for node in nodes:
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            o = cb_cli.disable_n2n_encryption()
            self.log.info(o)
            shell_conn.disconnect()

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

    def subsequent_data_load(self, async_load=False, data_load_spec=None):
        if data_load_spec is None:
            data_load_spec = self.data_load_spec
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(data_load_spec)
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                        self.bucket_util.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size)
        return tasks

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def async_data_load(self):
        tasks = self.subsequent_data_load(async_load=True)
        return tasks

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.validate_doc_loading_results(task)
        if task.result is False:
            self.fail("Doc_loading failed")

    def data_validation_collection(self):
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_rebalance_with_security_options(self):
        """
        0. Enable/Disable x509 certs (root, node certs) with client cert enabled/disabled
            (done in setup)
        1. Disable n2n encryption
        2. Rebalance-in nodes with async data load & collection drop/create
        3. Rebalance-out nodes with async data load & collection drop/create
        4. Enable n2n encryption and repeat steps 2-3
        """
        for level in ["disable", self.n2n_encryption_level]:
            add_nodes = random.sample(self.available_servers, self.nodes_in)
            if self.x509enable:
                self.upload_x509_certs(servers=add_nodes)
            if level in ["control", "all"]:
                self.set_n2n_encryption_level_on_nodes(nodes=[self.cluster.master],
                                                       level=level)
            self.log.info("Performing rebalance operation with x509: {0}, n2nencryption: {1}".
                          format(self.x509enable, level))

            operation = self.task.async_rebalance(self.nodes_in_cluster, add_nodes, [])
            tasks = self.async_data_load()
            self.wait_for_rebalance_to_complete(operation)
            self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()
            self.set_nodes(add_nodes=add_nodes)

            remove_nodes = random.sample(self.nodes_in_cluster_excluding_master,
                                         self.nodes_out)
            operation = self.task.async_rebalance(self.nodes_in_cluster, [], remove_nodes)
            tasks = self.async_data_load()
            self.wait_for_rebalance_to_complete(operation)
            self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()
            self.set_nodes(remove_nodes=remove_nodes)
