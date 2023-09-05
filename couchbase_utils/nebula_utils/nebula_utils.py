"""
This utility contains methods to access functionality for goldfish nebula.
"""

import random
import socket
from global_vars import logger
from membase.api.rest_client import RestConnection
import requests
import json
import binascii


class NebulaUtils(object):

    def __init__(self, nebula):
        self.log = logger.get("nebula")
        self.nebula = nebula

    """
    Generates a random unused port number.
    """
    def generate_random_unused_port(self):
        used_port = list()
        while True:
            # Choose a random port number between 1024 and 65535
            port = random.randint(1024, 65535)
            if port in used_port:
                pass
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("127.0.0.1", port))
                sock.close()
                return port
            except OSError:
                self.log.warn("Port {0} already in use".format(port))
                pass  # Port is in use, try again

    """
    Generates a config to register a cluster on goldfish nebula.
    :param cluster <object> Object of a cluster which has to be registered 
    on goldfish nebula.
    :param tls_skip_verify <bool>
    :param disable_proxy_nontls <bool>
    :return registration_config <json> configuration to register 
    the cluster on goldfish nebula
    """
    def generate_cluster_registration_config(
            self, cluster, tls_skip_verify=True, disable_proxy_nontls=True):
        cert_path = r"/tmp/ca_{0}.pem".format(cluster.master.ip)
        with open(cert_path, "w") as fp:
            rest_conn = RestConnection(cluster.master)
            certificate = rest_conn.get_cluster_ceritificate()
            fp.write(certificate)

        registration_config = {
            "cluster_UUID": cluster.name,
            "caPath": cert_path,
            "tls_skip_verify": tls_skip_verify,
            "disable_proxy_nontls": disable_proxy_nontls,
            "servers": [server.ip for server in cluster.servers],
            "creds": {
                "user": cluster.master.rest_username,
                "password": cluster.master.rest_password
            },
            "svc_ports": {
                "kv": self.generate_random_unused_port(),
                "kv_ssl": self.generate_random_unused_port(),
                "cbas": self.generate_random_unused_port(),
                "cbas_ssl": self.generate_random_unused_port()
            }
        }
        return registration_config

    """
    Register's a cluster on goldfish nebula.
    :param cluster_registration_config <json> configuration to register 
    the cluster on goldfish nebula
    :return <bool> True, if cluster is registered successfully, otherwise 
    False.
    """
    def register_cluster_on_nebula(self, cluster_registration_config):
        self.log.info("Registering cluster {0} on Nebula".format(
            cluster_registration_config["cluster_UUID"]))
        url = "http://{0]:{1}}/api/v1/clusters/{2]/register".format(
            self.nebula.endpoint, self.nebula.port,
            cluster_registration_config["cluster_UUID"])

        cluster_UUID = cluster_registration_config.pop("cluster_UUID")
        headers = {'Content-Type': 'application/json'}

        response = requests.request(
            "POST", url, headers=headers, data=binascii.b2a_base64(
                json.dumps(cluster_registration_config).encode()))

        if response.status_code == 200:
            self.nebula.cluster_portmap[cluster_UUID] = (
                cluster_registration_config)["svc_ports"]
            return True
        else:
            return False

    """
    Unregister's a cluster on goldfish nebula.
    :param cluster_UUID <string> UUID of the cluster which has to 
    unregistered from goldfish nebula.
    :return <bool> True, if cluster is unregistered successfully, otherwise 
    False.
    """
    def unregister_cluster_from_nebula(self, cluster_UUID):
        self.log.info("Unregistering cluster {0} on Nebula".format(
            cluster_UUID))
        url = "http://{0]:{1}}/api/v1/clusters/{2]/".format(
            self.nebula.endpoint, self.nebula.port, cluster_UUID)

        response = requests.request("DELETE", url)

        if response.status_code == 200:
            del self.nebula.cluster_portmap[cluster_UUID]
            return True
        else:
            return False
