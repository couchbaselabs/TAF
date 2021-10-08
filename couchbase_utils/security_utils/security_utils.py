'''
Created on 16-Aug-2021

@author: Umang
'''

from shutil import copytree
import time
import copy
from couchbase_utils.cb_tools.cb_cli import CbCli
from couchbase_utils.security_utils.x509main import x509main
from platform_utils.remote.remote_util import RemoteMachineShellConnection

class SecurityUtils():

    def __init__(self, logger, client_cert_state="enable",
                 paths="subject.cn:san.dnsname:san.uri",
                 prefixs="www.cb-:us.:www.", delimeter=".:.:.",
                 client_ip="172.16.1.174", dns=None, uri=None,
                 ssltype="openssl", encryption_type="", key_length=1024):
        self.log = logger
        self.client_cert_state = client_cert_state
        self.paths = paths.split(":")
        self.prefixs = prefixs.split(":")
        self.delimeters = delimeter.split(":")
        self.client_ip = client_ip
        self.dns = dns
        self.uri = uri
        self.ssltype = ssltype
        self.encryption_type = encryption_type
        self.key_length = key_length

    def upload_x509_certs(self, cluster=None, servers=None, setup_once=False):
        """
        1. Uploads root certs and client-cert settings on servers
        2. Uploads node certs on servers
        """
        if cluster:
            servers = cluster.servers

        self.log.info("Uploading root cert to servers {0}".format(servers))

        if setup_once:
            cluster.x509.setup_master(
                self.client_cert_state, self.paths, self.prefixs, self.delimeters)
        else:
            for server in cluster.servers:
                x509main(server).setup_master(
                    self.client_cert_state, self.paths, self.prefixs, self.delimeters)
        self.log.info("Sleeping before uploading node certs to nodes {0}".format(
            servers))
        time.sleep(5)
        x509main().setup_cluster_nodes_ssl(servers, reload_cert=True)
        if cluster:
            copytree(cluster.x509.CACERTFILEPATH, cluster.CACERTFILEPATH)
            shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
            self.log.info("Removing folder {0} from slave".format(
                cluster.x509.CACERTFILEPATH))
            shell.execute_command("rm -rf " + cluster.x509.CACERTFILEPATH)

    def generate_x509_certs(self, cluster):
        """
        Generates x509 root, node, client cert on all servers of the cluster
        """
        self.log.info("Certificate creation started on cluster - {0}".format(
            cluster.name))
        cluster.x509 = x509main(cluster.master)
        cluster.CACERTFILEPATH = x509main.CACERTFILEPATH.rstrip(
            "/") + cluster.name + "/"
        cluster.root_ca_path = cluster.CACERTFILEPATH + x509main.CACERTFILE
        cluster.client_certs = dict()
        for server in cluster.servers:
            cluster.client_certs[server.ip] = {
                "cert_pem": cluster.CACERTFILEPATH + "long_chain" + self.client_ip + ".pem",
                "cert_key": cluster.CACERTFILEPATH + self.client_ip + ".key",
                }

        copy_servers = copy.deepcopy(cluster.servers)
        cluster.x509._generate_cert(
            copy_servers, root_cn='Root\ Authority', type=self.ssltype,
            encryption=self.encryption_type, key_length=self.key_length,
            client_ip=self.client_ip, alt_names='default', dns=self.dns,
            uri=self.uri)

    def _reset_original(self, servers):
        """
        1. Regenerates root cert on all servers
        2. Removes inbox folder (node certs) from all VMs
        """
        self.log.info(
            "Reverting to original state - regenerating certificate "
            "and removing inbox folder")
        tmp_path = "/tmp/abcd.pem"
        for server in servers:
            cli_command = "ssl-manage"
            remote_client = RemoteMachineShellConnection(server)
            options = "--regenerate-cert={0}".format(tmp_path)
            output, error = remote_client.execute_couchbase_cli(
                cli_command=cli_command, options=options,
                cluster_host=server.ip, user="Administrator",
                password="password")
            x509main(server)._delete_inbox_folder()

    def teardown_x509_certs(self, servers, CA_cert_file_path=None):
        """
        1. Regenerates root cert and removes node certs and client
            cert settings from server
        2. Removes certs folder from slave
        """
        self._reset_original(servers)
        shell = RemoteMachineShellConnection(x509main.SLAVE_HOST)
        if not CA_cert_file_path:
            CA_cert_file_path = x509main.CACERTFILEPATH
        self.log.info("Removing folder {0} from slave".
                      format(CA_cert_file_path))
        shell.execute_command("rm -rf " + CA_cert_file_path)

    def set_n2n_encryption_level_on_nodes(self, nodes, level="control"):
        self.log.info("Enabling n2n encryption and setting level to "
                      "{0} on nodes {1}".format(level, nodes))
        for node in nodes:
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            cb_cli.enable_n2n_encryption()
            cb_cli.set_n2n_encryption_level(level=level)
            shell_conn.disconnect()
        time.sleep(10)

    def disable_n2n_encryption_cli_on_nodes(self, nodes):
        self.set_n2n_encryption_level_on_nodes(nodes=nodes, level="control")
        self.log.info("Disabling n2n encryption on nodes {0}".format(nodes))
        for node in nodes:
            shell_conn = RemoteMachineShellConnection(node)
            cb_cli = CbCli(shell_conn, no_ssl_verify=True)
            o = cb_cli.disable_n2n_encryption()
            self.log.info(o)
            shell_conn.disconnect()
        time.sleep(10)
