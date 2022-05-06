import unittest
import time

from TestInput import TestInputSingleton
from global_vars import logger
from Cb_constants import constants, CbServer
from platform_utils.remote.remote_util import RemoteMachineShellConnection


class ServerInfo:
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 memcached_port,
                 ssh_key=''):
        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.rest_username = "Administrator"
        self.rest_password = "password"
        self.ssh_key = ssh_key
        self.memcached_port = memcached_port
        self.remote_info = None


class CGroupBase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.log_level = self.input.param("log_level", "info").upper()
        self.log = logger.get("test")
        self.infra_log = logger.get("infra")
        self.infra_log_level = self.input.param("infra_log_level",
                                                "error").upper()
        self.log.setLevel(self.log_level)
        self.infra_log.setLevel(self.infra_log_level)
        self.servers = self.input.servers
        self.vm_ip = self.servers[0].ip
        self.rest_port = self.servers[0].port
        self.ssh_username = self.servers[0].ssh_username
        self.ssh_password = "couchbase"
        self.node = ServerInfo(ip=self.vm_ip, port=self.rest_port,
                               ssh_username=self.ssh_username, ssh_password=self.ssh_password,
                               memcached_port=CbServer.memcached_port)
        self.shell = RemoteMachineShellConnection(self.node)
        self.skip_setup_teardown = self.input.param("skip_setup_teardown", None)
        self.skip_teardown = self.input.param("skip_teardown", None)
        self.cpus = self.input.param("cpus", 2)
        self.mem = self.input.param("mem", 1073741824)
        self.limits = True
        if self.cpus is None and self.mem is None:
            self.limits = False
        if self.skip_setup_teardown is None:
            self.start_docker()
            self.remove_all_containers()
            self.start_couchbase_container(limits=self.limits, mem=self.mem, cpus=self.cpus)
            self.initialize_node()
        self.log.info("Finished CGroupBase")

    def initialize_node(self, sleep=15):
        self.log.info("initializing the node")
        cmd = "docker exec -d db " \
              "/opt/couchbase/bin/couchbase-cli cluster-init -c 127.0.0.1 --cluster-username Administrator " \
              "--cluster-password password --services data --cluster-ramsize 500"
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))
        self.log.info("Sleeping for {0} secs post initializing".format(sleep))
        time.sleep(sleep)

    def start_docker(self):
        self.log.info("Starting docker")
        cmd = "systemctl start docker"
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))

    def start_couchbase_container(self, limits=True, mem=1073741824, cpus=2, sleep=20):
        """
        Starts couchbase server inside a container on the VM
        (Assumes a docker image 'couchbase-neo' is present on the VM)
        :limits: Boolean - whether to start the container with memory & cpu limits
        :mem: int - max memory in bytes for the container
        :cpus: int - max number of cpus for the container
        """
        self.log.info("Starting couchbase server inside a container")
        if limits:
            cmd = "docker run -d --name db -m " + str(mem) + " --cpus=" + str(cpus) + \
                  " -p 8091-8096:8091-8096 -p 11210-11211:11210-11211 couchbase-neo"
        else:
            cmd = "docker run -d --name db -p 8091-8096:8091-8096 " \
                  "-p 11210-11211:11210-11211 couchbase-neo"
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))
        self.log.info("Sleeping for {0} secs post starting couchbase container".format(sleep))
        time.sleep(sleep)

    def remove_all_containers(self):
        self.log.info("Stopping all containers")
        cmd = "docker stop $(docker ps -a -q)"
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))
        self.log.info("Removing all containers")
        cmd = "docker rm $(docker ps -a -q)"
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))

    def tearDown(self):
        self.shell.disconnect()
        if self.skip_teardown is None:
            self.remove_all_containers()
