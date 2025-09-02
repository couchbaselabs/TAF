import json
import unittest
import time

from Jython_tasks.task_manager import TaskManager
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cluster_utils.cluster_ready_functions import CBCluster, ClusterUtils
from couchbase_helper.cluster import ServerTasks
from global_vars import logger
from cb_constants import CbServer
from StatsLib.StatsOperations import StatsHelper
import global_vars
from node_utils.node_ready_functions import NodeUtils
from shell_util.remote_connection import RemoteMachineShellConnection
from sirius_client_framework.sirius_setup import SiriusSetup
from upgrade_utils.upgrade_util import CbServerUpgrade


class Container(TestInputServer):
    def __init__(self, server_info, container_name, container_id, container_ip,
                 container_mem='', container_cpu=''):
        self.__dict__.update(server_info.__dict__)

        self.container_name = container_name
        self.container_id = container_id
        self.container_ip = container_ip
        self.container_mem = container_mem
        self.container_cpu = container_cpu

class CGgroupConstants:

    base_infra = "docker"

class CGroupBase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.log_level = self.input.param("log_level", "info").upper()
        self.log = logger.get("test")
        self.infra_log = logger.get("infra")
        self.infra_log_level = self.input.param("infra_log_level",
                                                "error").upper()
        self.base_infra = self.input.param("base_infra", "docker") # ["docker", "vm"]
        self.image_name = self.input.param("image_name", "couchbase-initial").lower()
        self.upgrade_image_name = self.input.param("upgrade_image_name", "couchbase-upgrade").lower()
        self.container_name = self.input.param("container_name", "db").lower()
        self.num_couchbase_nodes = self.input.param("num_couchbase_nodes", 1)
        self.load_docs_using = self.input.param("load_docs_using",
                                                "default_loader")
        self.thread_to_use = self.input.param("threads_to_use", 60)
        self.couchbase_version = self.input.param("couchbase_version", "8.0.0-2450")
        self.upgrade_version = self.input.param("upgrade_version", "8.0.0-2600")
        self.cluster_profile = self.input.param("cluster_profile", "provisioned")
        self.enable_cgroups = self.input.param("enable_cgroups", True)
        # Pass service1:soft:hard;service2:soft:hard
        self.overrides = self.input.param("overrides", None) # data:1024:1024;index:512:max

        self.skip_setup_teardown = self.input.param("skip_setup_teardown", None)
        self.skip_teardown = self.input.param("skip_teardown", None)
        self.cb_cpu_count_env = self.input.param("cb_cpu_count_env", None)  # COUCHBASE_CPU_COUNT env
        self.cpus = self.input.param("cpus", 2)  # cpus limit for the container
        self.mem = self.input.param("mem", 1073741824)  # mem limit for the container in bytes

        self.log.setLevel(self.log_level)
        self.infra_log.setLevel(self.infra_log_level)
        self.servers = self.input.servers
        self.host = self.servers[0]
        self.vm_ip = self.host.ip
        self.rest_port = self.host.port
        self.ssh_username = self.host.ssh_username
        self.ssh_password = self.host.ssh_password

        CGgroupConstants.base_infra = self.base_infra
        self.cgroup_name_mapping = {"data": "kv", "index": "index", "query": "n1ql",
                                    "eventing": "eventing",
                                    "analytics": "cbas", "fts": "fts"}

        self.service_process_mapping = {
            "data": ["/opt/couchbase/bin/memcached"],
            "query": ["/opt/couchbase/bin/cbq-engine", "/opt/couchbase/bin/js-evaluator"],
            "index": ["/opt/couchbase/bin/indexer"],
            "fts": ["/opt/couchbase/bin/cbft"],
            "eventing": ["/opt/couchbase/bin/eventing"],
            "analytics": ["/opt/couchbase/bin/cbas", "/opt/couchbase/lib/cbas/runtime/bin/java"]
        }

        self.shell_dict = dict()
        for server in self.servers[:self.num_couchbase_nodes]:
            self.shell_dict[server.ip] = RemoteMachineShellConnection(server)
        self.shell = RemoteMachineShellConnection(self.host)

        # Pass service_mem_alloc as service_name1:memory_to_be_allocated-service_name2:memory_to_be_allocated
        service_mem_alloc = self.input.param("service_mem_alloc", None)
        self.node_services = self.input.param("node_services", None)
        self.service_and_memory_allocation = dict()
        if service_mem_alloc:
            temp = service_mem_alloc.split("-")
            for service in temp:
                temp2 = service.split(":")
                self.service_and_memory_allocation[temp2[0]] = temp2[1]

        self.cgroup_limits = dict()

        self.task_manager = TaskManager(max_workers=self.thread_to_use)
        self.task = ServerTasks(self.task_manager)
        self.node_utils = NodeUtils(self.task_manager)

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

        if self.skip_setup_teardown is None:
            if self.base_infra == "docker":
                self.setup_docker_cluster()
            else:
                self.setup_vm_cluster()

        if float(self.couchbase_version[:3]) >= 8.0:
            self.toggle_cgroup_setting(enable=self.enable_cgroups)

        self.set_services_for_nodes()

        if self.overrides is not None:
            self.set_overrides(self.cluster.nodes_in_cluster)

        self.host_mem_bytes = self.get_host_mem_in_bytes()
        self.host_cpus = self.get_host_cpu_cores()
        self.cgroup_aware_stats = ["sys_cpu_utilization_rate", "sys_cpu_sys_rate",
                                   "sys_cpu_user_rate", "sys_cpu_cores_available",
                                   "sys_mem_cgroup_used", "sys_mem_cgroup_used",
                                   "sys_mem_limit"]
        self.host_aware_stats = ["sys_cpu_host_utilization_rate", "sys_cpu_host_sys_rate",
                                 "sys_cpu_host_user_rate", "sys_cpu_host_cores_available",
                                 "sys_mem_total"]
        self.log.info("Finished CGroupBase")


    def set_disk_paths(self, server):

        cmd = "/opt/couchbase/bin/couchbase-cli node-init " \
              "-c 127.0.0.1 --username Administrator --password password"

        # Create directories writeable by Couchbase user
        create_dir_cmd = "mkdir -p".format(server.container_name)
        permissions_cmd = "chown -R couchbase:couchbase".\
                            format(server.container_name)
        if server.data_path != '':
            cmd += " --node-init-data-path {}".format(server.data_path)
            create_dir_cmd += " {}".format(server.data_path)
            permissions_cmd += " {}".format(server.data_path)
        if server.index_path != '':
            cmd += " --node-init-index-path {}".format(server.index_path)
            create_dir_cmd += " {}".format(server.index_path)
            permissions_cmd += " {}".format(server.index_path)
        if server.cbas_path != '':
            cmd += " --node-init-analytics-path {}".format(server.cbas_path)
            create_dir_cmd += " {}".format(server.cbas_path)
            permissions_cmd += " {}".format(server.cbas_path)
        if server.eventing_path != '':
            cmd += " --node-init-eventing-path {}".format(server.eventing_path)
            create_dir_cmd += " {}".format(server.eventing_path)
            permissions_cmd += " {}".format(server.eventing_path)

        self.log.info("Create dir CMD = {}".format(create_dir_cmd))
        if self.base_infra == "docker":
            self.execute_command_docker(create_dir_cmd, server)
        else:
            self.shell_dict[server.ip].execute_command(create_dir_cmd)

        self.log.info("Permissions CMD = {}".format(permissions_cmd))
        if self.base_infra == "docker":
            self.execute_command_docker(permissions_cmd, server)
        else:
            self.shell_dict[server.ip].execute_command(permissions_cmd)

        self.log.info("Setting disk path")
        self.log.debug("CMD to set disk path : {}".format(cmd))
        if self.base_infra == "docker":
            self.execute_command_docker(cmd, server)
        else:
            o, e = self.shell_dict[server.ip].execute_command(cmd)
            self.log.info("Output:{0}, Error{1}".format(o, e))

    def initialize_node(self, server, sleep=15):
        self.log.info("initializing the node")
        cmd = "/opt/couchbase/bin/couchbase-cli cluster-init -c 127.0.0.1 --cluster-username Administrator " \
              "--cluster-password password "
        if self.service_and_memory_allocation:
            # services = ",".join(self.service_and_memory_allocation.keys())
            if self.node_services is not None:
                services = self.node_services.split("-")[0].replace(":", ",")
            else:
                services = "data"
            memory_allocations = ""
            for service, memory in self.service_and_memory_allocation.items():
                if service == "data":
                    memory_allocations += "--cluster-ramsize {0} ".format(memory)
                    self.cgroup_limits["data"] = dict()
                    self.cgroup_limits["data"]["soft"] = int(memory) * 1024
                    self.cgroup_limits["data"]["hard"] = float('inf')
                elif service == "index":
                    memory_allocations += "--cluster-index-ramsize {0} ".format(memory)
                    self.cgroup_limits["index"] = dict()
                    self.cgroup_limits["index"]["soft"] = int(memory) * 1024
                    self.cgroup_limits["index"]["hard"] = float('inf')
                elif service == "query":
                    memory_allocations += "--cluster-query-ramsize {0} ".format(memory)
                    self.cgroup_limits["query"] = dict()
                    self.cgroup_limits["query"]["soft"] = int(memory) * 1024
                    self.cgroup_limits["query"]["hard"] = int(memory) * 1024
                elif service == "fts":
                    memory_allocations += "--cluster-fts-ramsize {0} ".format(memory)
                    self.cgroup_limits["fts"] = dict()
                    self.cgroup_limits["fts"]["soft"] = int(memory) * 1024
                    self.cgroup_limits["fts"]["hard"] = int(memory) * 1024
                elif service == "eventing":
                    memory_allocations += "--cluster-eventing-ramsize {0} ".format(memory)
                    self.cgroup_limits["eventing"] = dict()
                    self.cgroup_limits["eventing"]["soft"] = int(memory) * 1024
                    self.cgroup_limits["eventing"]["hard"] = float('inf')
                elif service == "analytics":
                    memory_allocations += "--cluster-analytics-ramsize {0} ".format(memory)
                    self.cgroup_limits["analytics"] = dict()
                    self.cgroup_limits["analytics"]["soft"] = int(memory) * 1024
                    self.cgroup_limits["analytics"]["hard"] = float('inf')
            cmd += "--services {0} {1}".format(services, memory_allocations)
        else:
            cmd += "--services data --cluster-ramsize 500"

        self.log.info(f"Cgroup limits = {self.cgroup_limits}")

        self.log.info("Docker command run: {0}".format(cmd))
        if self.base_infra == "docker":
            self.execute_command_docker(cmd, server)
        else:
            o, e = self.shell_dict[server.ip].execute_command(cmd)
            self.log.info("Output:{0}, Error:{1}".format(o, e))
        self.log.info("Sleeping for {0} secs post initializing".format(sleep))
        time.sleep(sleep)

    def start_docker(self):
        self.log.info("Starting docker")
        cmd = "systemctl start docker"
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))

    def start_couchbase_container(self, host, offset, mem=1073741824, cpus=2,
                                  sleep=20, mount_data=False,
                                  image_name='couchbase', container_name='db'):
        """
        Starts couchbase server inside a container on the VM
        (Assumes a docker image 'couchbase' is present on the VM)
        :limits: Boolean - whether to start the container with memory & cpu limits
        :mem: int - max memory in bytes for the container
        :cpus: int - max number of cpus for the container
        """
        self.log.info("Starting couchbase server inside a container on host: {}".format(host.ip))
        flags = ""
        if self.mem not in [None, "None"]:
            flags = flags + "-m " + str(mem) + "b"
        if self.cpus not in [None, "None"]:
            flags = flags + " --cpus " + str(cpus)
        if self.cb_cpu_count_env not in [None, "None"]:
            flags = flags + " -e COUCHBASE_CPU_COUNT=" + str(self.cb_cpu_count_env)
        cmd = "docker run -d --name {} ".format(container_name) + flags
        for port in self.ports_to_expose:
            alternate_port = (offset*1000) + port
            cmd += " -p " + str(alternate_port) + ":" + str(port)
        if mount_data:
            host_dir = "/data/couchbase_data/" + container_name
            container_dir = "/opt/couchbase/var/lib/couchbase/"
            cmd += " -v {}:{}".format(host_dir, container_dir)
        cmd += " --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw"
        cmd +=  " {}".format(image_name)
        o, e = self.shell.execute_command(cmd)
        self.log.info("Docker command run: {0}".format(cmd))
        self.log.info("Output:{0}, Error{1}".format(o, e))
        self.log.info("Sleeping for {0} secs post starting couchbase container".format(sleep))
        time.sleep(sleep)
        return o[0].rstrip("\n")

    def remove_all_containers(self):
        self.log.info(f"Stopping all containers on {self.host.ip}")
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

    def update_container_cpu_limit(self, cpus):
        """
        Updates cpu cores limit of a running docker container
        :cpus: cpu cores limit
        """
        self.log.info("Updating the running container's cpu cores limit from {0} to {1}".
                      format(self.cpus, cpus))
        cmd = "docker update --cpus " + str(cpus) + "  db"
        o, e = self.shell.execute_command(cmd)
        print(o, e)
        self.cpus = cpus

    def update_container_mem_limit(self, mem):
        """
        Updates mem limit of a running docker container
        :mem: memory limit in bytes
        """
        self.log.info("Updating the running container's mem limit from {0} to {1}".
                      format(self.mem, mem))
        cmd = "docker update -m " + str(mem) + " --memory-swap -1 db"
        o, e = self.shell.execute_command(cmd)
        print(o, e)
        self.mem = mem

    def restart_server(self):
        self.log.info("Stopping and starting server...")
        self.shell.execute_command("docker exec db service couchbase-server stop")
        time.sleep(10)
        self.shell.execute_command("docker exec db service couchbase-server start")
        time.sleep(20)

    def get_host_mem_in_bytes(self):
        """
        returns mem(int) available by directly issuing a command on the VM
        """
        cmd = "awk '/MemTotal/ {print $2}' /proc/meminfo"
        o, e = self.shell.execute_command(cmd)
        kib = o[0]
        return int(kib) * 1024

    def get_host_cpu_cores(self):
        """
        returns cpu cores(int) available by directly issuing a command on the VM
        """
        cmd = "grep -c processor /proc/cpuinfo"
        o, e = self.shell.execute_command(cmd)
        return int(o[0])

    def verify_values_of_host_stats(self, stats_map):
        """
        Verifies if host's cpus and mem stats are correct
        """
        self.log.info("Verifying the cpus and mem stats of the host")
        if stats_map["sys_cpu_host_cores_available"] != str(self.host_cpus):
            self.fail("Mismatch, actual host cpus {0}, but sys_cpu_host_cores_available {1}".
                      format(self.host_cpus, stats_map["sys_cpu_host_cores_available"]))

        if stats_map["sys_mem_total"] != str(self.host_mem_bytes):
            self.fail("Mismatch, actual host mem {0}, but sys_mem_total {1}".
                      format(self.host_mem_bytes, stats_map["sys_mem_total"]))

    def verify_values_of_cgroup_stats(self, stats_map):
        """
        Verifies if cgroup aware cpu and mem stats are correct
        """
        expected_cpus = self.cpus
        expected_mem = self.mem
        if self.cpus in ["None", None]:
            expected_cpus = self.host_cpus
        if self.mem in ["None", None]:
            expected_mem = self.host_mem_bytes
        self.log.info("Verifying the cpus and mem stats of the cgroup")
        if stats_map["sys_cpu_cores_available"] != str(expected_cpus):
            self.fail("Mismatch, actual cgroup cpus {0}, but sys_cpu_cores_available {1}".
                      format(expected_cpus, stats_map["sys_cpu_cores_available"]))

        if stats_map["sys_mem_limit"] != str(expected_mem):
            self.fail("Mismatch, actual cgroup mem {0}, but sys_mem_limit {1}".
                      format(expected_mem, stats_map["sys_mem_limit"]))

    def read_latest_cgroup_aware_stats(self, node=None):
        """
        Reads cgroup aware stats reported by prometheus
        """
        if node is None:
            node = self.node
        latest_cgroup_aware_stats_map = dict()
        for stat in self.cgroup_aware_stats:
            content = StatsHelper(node).get_range_api_metrics(stat)
            val = content['data'][0]['values'][-1][1]
            latest_cgroup_aware_stats_map[stat] = val
        self.log.info("latest_cgroup_aware_stats_map {0}".format(latest_cgroup_aware_stats_map))
        return latest_cgroup_aware_stats_map

    def read_latest_host_aware_stats(self, node=None):
        """
        Reads host aware stats reported by prometheus
        """
        if node is None:
            node = self.node
        latest_host_aware_stats_map = dict()
        for stat in self.host_aware_stats:
            content = StatsHelper(node).get_range_api_metrics(stat)
            val = content['data'][0]['values'][-1][1]
            latest_host_aware_stats_map[stat] = val
        self.log.info("latest_host_aware_stats_map {0}".format(latest_host_aware_stats_map))
        return latest_host_aware_stats_map

    def read_logs(self, log_file, regex):
        """
        reads container logs that match a regex
        """
        cmd = "docker exec {} cat /opt/couchbase/var/lib/couchbase/logs/{} | grep {}".format(self.container_id,
                                                                                                 log_file,
                                                                                                 regex)
        cmd_out, cmd_error = self.shell.execute_command(cmd)
        self.log.info("Read logs command {} , Output:{}, Error{}".format(cmd, cmd_out, cmd_error))
        return cmd_out

    def fetch_container_ip(self, container_name):

        cmd = 'docker inspect {} | grep \'"IPAddress"\''.format(container_name)
        o, e = self.shell.execute_command(cmd)
        self.log.info("Output = {}, Error = {}".format(o, e))

        container_ip = o[0].strip().split(':')[1].strip()[1:-2]
        self.log.info("Container IP: {}".format(container_ip))
        return container_ip

    def launch_doc_loader_docker(self):

        self.log.info("Launching Sirius Java Loader")
        SiriusSetup.sirius_url = f"http://{self.vm_ip}:49000"

        # Launch Doc Loader on docker
        o, e = self.shell.execute_command(
            "docker run -d --name rest_loader -p 49000:49000 docloader-rest")
        doc_container_id = o[0].rstrip("\n")
        self.log.info("DocLoader Container ID: {}".format(doc_container_id))
        time.sleep(10)

        self.log.info("Checking if Sirius is online")
        if self.load_docs_using == "sirius_java_sdk":
            if not SiriusSetup.is_sirius_online(SiriusSetup.sirius_url):
                raise Exception("Sirius offline!")
            assert SiriusSetup.reset_java_loader_tasks(self.thread_to_use) == True, \
                    "reset_java_loader_tasks failed"

    def set_alternate_ports_for_server(self, server, offset, ssl=False):

        if not ssl:
            server.port = CbServer.port + (offset*1000)
            server.memcached_port = CbServer.memcached_port + (offset*1000)
            server.fts_port = CbServer.fts_port + (offset*1000)
            server.index_port = CbServer.index_port + (offset*1000)
            server.n1ql_port = CbServer.n1ql_port + (offset*1000)
            server.query_port = CbServer.n1ql_port + (offset*1000)
            server.cbas_port = CbServer.cbas_port + (offset*1000)
            server.eventing_port = CbServer.eventing_port + (offset*1000)

        else:
            server.port = CbServer.ssl_port + (offset*1000)
            server.memcached_port = CbServer.ssl_memcached_port + (offset*1000)
            server.fts_port = CbServer.ssl_fts_port + (offset*1000)
            server.index_port = CbServer.ssl_index_port + (offset*1000)
            server.n1ql_port = CbServer.ssl_n1ql_port + (offset*1000)
            server.query_port = CbServer.ssl_n1ql_port + (offset*1000)
            server.cbas_port = CbServer.ssl_cbas_port + (offset*1000)
            server.eventing_port = CbServer.ssl_eventing_port + (offset*1000)

    def toggle_cgroup_setting(self, enable=True):

        rest = ClusterRestAPI(self.cluster.master)
        value = "true" if enable else "false"
        status, content = rest.set_internal_settings("enableCgroups", value)
        self.log.info(f"Status: {status}, Content: {content}")
        return status, content

    def set_services_for_nodes(self):

        self.node_service_dict = {"data": [], "index": [], "query": [], "fts": [],
                                  "eventing": [], "analytics": []}

        index = 0
        for server in self.cluster.nodes_in_cluster:
            services = self.node_services.split("-")[index].replace(":", ",")
            server.services = services

            node_services = services.split(",")
            for node_service in node_services:
                self.node_service_dict[node_service].append(server)

            index += 1

        self.log.info(f"Node service dict = {self.node_service_dict}")

        self.cluster.kv_nodes = self.node_service_dict["data"]
        self.cluster.fts_nodes = self.node_service_dict["fts"]
        self.cluster.index_nodes = self.node_service_dict["index"]
        self.cluster.query_nodes = self.node_service_dict["query"]
        self.cluster.eventing_nodes = self.node_service_dict["eventing"]
        self.cluster.cbas_nodes = self.node_service_dict["analytics"]

        self.log.info(f"Cluster obj = {self.cluster.__dict__}")

        self.get_service_pids()

    def get_service_pids(self):

        self.node_service_pid = dict()
        self.service_process_count = dict()

        for server in self.cluster.nodes_in_cluster:
            self.node_service_pid[server.unique_ip] = dict()

            for service in ["data", "query", "index", "fts", "eventing", "analytics"]:
                if service in server.services:
                    pid_list = list()
                    for process in self.service_process_mapping[service]:
                        pid_cmd = f"pgrep -af {process} | awk '{{print $1}}'"
                        if self.base_infra == "docker":
                            pids, e = self.execute_command_docker(pid_cmd, server)
                        else:
                            pids, e = self.shell_dict[server.ip].execute_command(pid_cmd)
                        pid_list.extend(pids)

                    self.node_service_pid[server.unique_ip][service] = pid_list
                    self.service_process_count[service] = len(pid_list)

        self.log.info("Node PID dict = {}".format(self.node_service_pid))
        self.log.info("Service process count = {}".format(self.service_process_count))

    def update_service_pid(self, server, service):

        self.log.info(f"Updating PIDs for service: {service} on {server.unique_ip}")

        pid_list = list()
        for process in self.service_process_mapping[service]:
            pid_cmd = f"pgrep -af {process} | awk '{{print $1}}'"
            if self.base_infra == "docker":
                pids, e = self.execute_command_docker(pid_cmd, server)
            else:
                pids, e = self.shell_dict[server.ip].execute_command(pid_cmd)
            pid_list.extend(pids)

        self.node_service_pid[server.unique_ip][service] = pid_list

        self.log.info("Node PID dict = {}".format(self.node_service_pid))
        return pid_list

    def setup_cgroup_folders(self, server):

        # Stop the server
        stop_cmd = "systemctl stop couchbase-server"
        self.log.info(f"Executing command: {stop_cmd}")
        if self.base_infra == "docker":
            self.execute_command_docker(stop_cmd, server)
        else:
            self.shell_dict[server.ip].execute_command(stop_cmd)

        time.sleep(5)

        # Edit cgroup_base_path in /opt/couchbase/etc/couchbase/provisioned_profile
        self.cgroup_base_path = "/sys/fs/cgroup/system.slice/couchbase-server.service"
        if self.base_infra == "docker":
            self.cgroup_base_path = f"/sys/fs/cgroup/system.slice/docker-{server.container_id}.scope/system.slice/couchbase-server.service"
            pattern_to_match = "/sys/fs/cgroup/system.slice/couchbase-server.service"
            provisioned_file = "/opt/couchbase/etc/couchbase/provisioned_profile"

            cmd1 = f"sed -i 's|{pattern_to_match}|{self.cgroup_base_path}|g' {provisioned_file}"
            self.log.info(f"Executing command: {cmd1}")
            self.shell.execute_command(cmd1)
            self.execute_command_docker(cmd1, server)

            time.sleep(2)

        # Edit systemd file in override.conf
        couchbase_cgroup_folder = self.cgroup_base_path + "/"
        systemd_file = "/etc/systemd/system/couchbase-server.service.d/override.conf"
        folder_cmd = f"mkdir -p {'/'.join(systemd_file.split('/')[:-1])}"
        self.log.info(f"Executing command: {folder_cmd}")
        if self.base_infra == "docker":
                self.execute_command_docker(folder_cmd, server)
        else:
            self.shell_dict[server.ip].execute_command(folder_cmd)

        arr = ["[Service]",
            "Delegate=yes",
            f"ExecStartPre=/opt/couchbase/bin/create-provisioned-cgroups.sh '\\'{couchbase_cgroup_folder}\\''",
            "DelegateSubgroup=babysitter"]

        for line in arr:
            cmd2 = f"bash -c \"echo '{line}' >> {systemd_file}\""
            self.log.info(f"Executing command: {cmd2}")
            if self.base_infra == "docker":
                self.execute_command_docker(cmd2, server)
            else:
                self.shell_dict[server.ip].execute_command(cmd2)

        time.sleep(2)

        # Reload daemon
        cmd3 = "systemctl daemon-reload"
        self.log.info(f"Executing command: {cmd3}")
        if self.base_infra == "docker":
            self.execute_command_docker(cmd3, server)
        else:
            self.shell_dict[server.ip].execute_command(cmd3)

        time.sleep(2)

        # Start couchbase server
        cmd4 = "systemctl start couchbase-server"
        self.log.info(f"Executing command: {cmd4}")
        if self.base_infra == "docker":
            self.execute_command_docker(cmd4, server)
        else:
            self.shell_dict[server.ip].execute_command(cmd4)

        time.sleep(20)

    def setup_docker_cluster(self):

        self.start_docker()
        self.remove_all_containers()
        self.launch_doc_loader_docker()
        self.containers = list()
        self.ports_to_expose = [8091, 8092, 8093, 8094, 8095, 8096, 8097,
                                9100, 9101, 9102, 9103, 9104, 9105,
                                9123,
                                11207, 11210, 11280,
                                18091, 18092, 18093, 18094, 18095, 18096, 18097,
                                19100, 19101, 19102, 19103, 19104, 19105]

        for i in range(self.num_couchbase_nodes):
            container_name = self.container_name+str(i)
            container_id = self.start_couchbase_container(host=self.host,
                                                          offset=i,
                                                          mem=self.mem,
                                                          cpus=self.cpus,
                                                          image_name=self.image_name,
                                                          container_name=container_name)
            self.log.info("Container ID:{}".format(container_id))
            container_ip = self.fetch_container_ip(container_name)
            container = Container(server_info=self.host,
                                container_name=container_name,
                                container_id=container_id,
                                container_ip=container_ip,
                                container_mem=self.mem,
                                container_cpu=self.cpus)
            container.unique_ip = container_ip

            # SetUp CGroup Folders
            if float(self.couchbase_version[:3]) >= 8.0:
                self.setup_cgroup_folders(container)

            self.set_alternate_ports_for_server(container, offset=i)
            self.log.info(f"Container_obj: {container.__dict__}")
            self.containers.append(container)
            time.sleep(15)
            self.set_disk_paths(container)
        self.master_server = self.containers[0]
        self.initialize_node(self.master_server)

        self.add_nodes_and_rebalance()

    def setup_vm_cluster(self):

        self.upgrade_helper = CbServerUpgrade(self.log, self.product)

        # Install the necessary version on VMs
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=self.servers[:self.num_couchbase_nodes],
            version=self.couchbase_version,
            cluster_profile=self.cluster_profile)

        self.log.info("Wait after installation on nodes")
        time.sleep(60)

        for server in self.servers:
            server.unique_ip = server.ip
            self.log.info(f"Server: {server.__dict__}")
            if float(self.couchbase_version[:3]) >= 8.0:
                self.setup_cgroup_folders(server)

            # Init nodes with disk_paths
            self.set_disk_paths(server)

        # Cluster init
        self.master_server = self.servers[0]
        self.initialize_node(self.master_server)

        self.add_nodes_and_rebalance()

    def launch_couchbase_container(self, container_name, offset):

        container_id = self.start_couchbase_container(host=self.host,
                                                      offset=offset,
                                                      mem=self.mem,
                                                      cpus=self.cpus,
                                                      image_name=self.upgrade_image_name,
                                                      container_name=container_name)
        self.log.info("Container ID: {}".format(container_id))
        container_ip = self.fetch_container_ip(container_name)
        new_container = Container(server_info=self.host,
                                  container_name=container_name,
                                  container_id=container_id,
                                  container_ip=container_ip,
                                  container_mem=self.mem,
                                  container_cpu=self.cpus)
        new_container.unique_ip = container_ip
        return new_container

    def add_nodes_and_rebalance(self):

        if self.num_couchbase_nodes > 1:
            for i in range(1, self.num_couchbase_nodes):
                services = self.node_services.split("-")[i].replace(":", ",")
                if self.base_infra == "docker":
                    self.add_server_to_cluster(self.master_server, self.containers[i], services)
                else:
                    self.add_server_to_cluster(self.master_server, self.servers[i], services)

            self.cluster_rebalance(self.master_server)

        if self.base_infra == "docker":
            self.cluster = CBCluster(name="C1", servers=self.containers)
            self.cluster.nodes_in_cluster = self.containers
        else:
            self.cluster = CBCluster(name="C1", servers=self.servers[:self.num_couchbase_nodes])
            self.cluster.nodes_in_cluster = self.servers
        self.cluster.master = self.master_server

    def set_overrides(self, servers):

        # kv:1024:1024;index:512:max
        service_overrides = self.overrides.split(";") # ["data:1024:1024", "index:512:max"]
        for service in service_overrides:
            override_split = service.split(":")  # ["data", "1024", "1024"]
            service_name = self.cgroup_name_mapping[override_split[0]] # data -> kv, query -> n1ql
            soft_limit = override_split[1]
            hard_limit = override_split[2]
            self.log.info(f"Setting override: {service_name}," \
                          f" Soft limit: {soft_limit}," \
                          f" Hard Limit: {hard_limit}")
            for server in servers:
                status, content = ClusterRestAPI(server).\
                    set_cgroup_overrides(service_name, soft_limit, hard_limit)
                self.log.info(f"Status: {status}, Content: {content}")
            self.cgroup_limits[override_split[0]]["soft"] = float('inf') if soft_limit == "max" else int(soft_limit) * 1024
            self.cgroup_limits[override_split[0]]["hard"] = float('inf') if hard_limit == "max" else int(hard_limit) * 1024

        self.log.info(f"Cgroup limits = {self.cgroup_limits}")

    def add_server_to_cluster(self, master_server, server_to_add, services):

        add_server_ip = server_to_add.unique_ip
        master_server_ip = master_server.unique_ip
        self.log.info("Adding server: {} to the cluster".format(add_server_ip))
        add_node_cmd = "/opt/couchbase/bin/couchbase-cli server-add -c {}".format(
                            master_server_ip) + \
                        " --username Administrator --password password" \
                        " --server-add {}".format(add_server_ip) + \
                        " --server-add-username Administrator --server-add-password password" \
                        " --services {}".format(services)
        self.log.info("Add node cmd: {}".format(add_node_cmd))
        if self.base_infra == "docker":
            o, e = self.execute_command_docker(add_node_cmd, master_server)
        else:
            o, e = self.shell_dict[master_server_ip].execute_command(add_node_cmd)
        self.log.info("Add node output = {}".format(o))

    def cluster_rebalance(self, cluster_master, servers_to_remove=[], services_to_add=None, services_to_remove=None):

        # Rebalance after adding containers
        self.log.info("Rebalancing after adding containers")
        rebalance_cmd = "/opt/couchbase/bin/couchbase-cli rebalance -c {}".format(
                            cluster_master.unique_ip)
        rebalance_cmd += " --username Administrator --password password"
        remove_server_str = ""
        for server in servers_to_remove:
            remove_server_str += server.unique_ip + ","
        if len(remove_server_str) > 0:
            rebalance_cmd += " --server-remove {}".format(remove_server_str[:-1])

        if services_to_add:
            rebalance_cmd += " --update-services"
            for service in services_to_add:
                nodes_str = ""
                for node in services_to_add[service]:
                    nodes_str += node.unique_ip + ","
                nodes_str = nodes_str[:-1]
                rebalance_cmd += f" --{service}-add {nodes_str}"

        if services_to_remove:
            rebalance_cmd += " --update-services"
            for service in services_to_remove:
                nodes_str = ""
                for node in services_to_remove[service]:
                    nodes_str += node.unique_ip + ","
                nodes_str = nodes_str[:-1]
                rebalance_cmd += f" --{service}-remove {nodes_str}"

        self.log.info("Rebalance command = {}".format(rebalance_cmd))
        if self.base_infra == "docker":
            o, e = self.execute_command_docker(rebalance_cmd, cluster_master)
        else:
            o, e = self.shell_dict[cluster_master.unique_ip].execute_command(rebalance_cmd)
        self.log.info("Rebalance command output: {}".format(o))

    def execute_command_docker(self, cmd, server):

        cmd = f"docker exec {server.container_name} {cmd}"
        o, e = self.shell.execute_command(cmd)
        return o, e
