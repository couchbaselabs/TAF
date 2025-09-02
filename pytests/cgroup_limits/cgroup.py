import copy
import os
import threading
import time
from math import ceil

from BucketLib.bucket import Bucket
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cgroup_limits.helper.cbas_helper import CGroupCBASHelper
from cgroup_limits.helper.kv_helper import CGroupDataHelper
from cgroup_limits.helper.eventing_helper import CGroupEventingHelper
from cgroup_limits.helper.fts_helper import CGroupFTSHelper
from cgroup_limits.helper.query_index_helper import CGroupQueryHelper
from pytests.cgroup_limits.cgroup_base import CGroupBase, Container
from shell_util.remote_connection import RemoteMachineShellConnection
from upgrade_utils.upgrade_util import CbServerUpgrade


class CGroup(CGroupBase):
    def setUp(self):
        super(CGroup, self).setUp()
        self.baseline_cores = 4  # A typical VM we use
        # Total thread count of processes on a container having baseline_cores
        self.baseline_core_threads = {"beam.smp": 106, "memcached": 29}
        # by how much they increase/decrease per core increase/decrease
        self.threads_factor = {"beam.smp": 4, "memcached": 1}

        # Bucket params
        self.num_buckets = self.input.param("num_buckets", 2)
        self.bucket_prefix = self.input.param("bucket_prefix", "bucket")
        self.ram_quota = self.input.param("ram_quota", 1024)
        self.replicas = self.input.param("replicas", 1)
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.magma)
        self.num_items_per_coll = self.input.param("num_items_per_coll", 500000)
        self.num_scopes = self.input.param("num_scopes", 2)
        self.num_collections = self.input.param("num_collections", 5)

        # Query params
        self.query_num_clients = self.input.param("query_num_clients", 200)
        self.query_iter = self.input.param("query_iter", 5)

        # Lock for calculating max mem_usage across files
        self.lock = threading.Lock()

    def tearDown(self):
        pass

    def fetch_memory_profile(self, service):

        self.log.info(f"Wait before fetching memory profile for service: {service}")
        time.sleep(30)
        node = self.node_service_dict[service][0]
        if service == "query":
            port = node.query_port
        elif service == "fts":
            port = node.fts_port
        elif service == "index":
            port = node.index_port

        prof_file_name = "/tmp/memory_profile_" + service + ".txt"

        self.shell.execute_command("rm {}".format(prof_file_name))
        curl_cmd = "curl 'http://{}:{}/debug/pprof/heap?debug=1' -u Administrator:password " \
                    "-o {}".format(node.ip, port, prof_file_name)
        self.log.info("Memory profile Curl command = {}".format(curl_cmd))
        self.shell.execute_command(curl_cmd)

        grep_str = "MaxRSS"
        grep_cmd = "grep {} {}".format(grep_str, prof_file_name)
        self.log.info("Grep CMD = {}".format(grep_cmd))
        o, e = self.shell.execute_command(grep_cmd)
        self.log.info("Service: {}, Max Memory Usage: {}".format(service, o))

    def record_iteration(self, service, pid, file_dir, server, interval=0.2, file_size=20):

        shell = RemoteMachineShellConnection(self.host)
        # Create directory
        shell.execute_command(f"mkdir {file_dir}")
        file_offset = 1
        stat_counter = 0
        file_name = file_dir + "/memory" + str(file_offset) + ".log"
        self.log.info(f"Recording memory usage in {file_name}")

        while self.record:
            if service == "eventing":
                pid = self.node_service_pid[server.unique_ip][service]

            pid_str = ",".join(pid)
            cmd = f"ps -o rss= -p {pid_str}"
            if self.base_infra == "docker":
                o, e = self.execute_command_docker(cmd, server)
            else:
                o, e = self.shell_dict[server.ip].execute_command(cmd)
            if len(o) == len(pid):
                mem = sum(int(m) for m in o)
                echo_cmd = f"echo {mem} >> {file_name}"
                if self.base_infra == "docker":
                    o, e = self.execute_command_docker(echo_cmd, server)
                else:
                    o, e = self.shell.execute_command(echo_cmd)
            else:
                while True:
                    pid = self.update_service_pid(server, service)
                    if len(pid) == self.service_process_count[service]:
                        break
                    time.sleep(0.5)
                continue

            stat_counter += 1
            if stat_counter >= file_size:
                file_offset += 1
                stat_counter = 0
                file_name = file_dir + "/memory" + str(file_offset) + ".log"
            time.sleep(interval)

    def record_memory_usage(self):

        record_threads = list()
        directories = list()

        # Create a parent directory which houses all the sub-directories
        self.shell.execute_command("rm -rf /tmp/memory_stats/*")
        self.shell.execute_command("mkdir /tmp/memory_stats")

        for server in self.cluster.nodes_in_cluster:
            node_dir = "/tmp/memory_stats/" + server.unique_ip
            self.shell.execute_command(f"mkdir {node_dir}")

            for service in server.services.split(","):
                file_dir = node_dir + "/" + service
                pid = self.node_service_pid[server.unique_ip][service]
                self.log.info(f"Recording {service} ({pid}) memory usage in {file_dir}")
                th = threading.Thread(target=self.record_iteration, args=[
                                        service, pid, file_dir, server])
                record_threads.append(th)
                directories.append(file_dir)
                th.start()

        return record_threads, directories

    def record_memory_dict(self, server, pid, service, interval=0.2):

        violation_counter = 0
        violation_list = list()
        time_list = list()

        while self.record:
            if service == "eventing":
                pid = self.node_service_pid[server.unique_ip][service]

            pid_str = ",".join(pid)
            cmd = f"ps -o rss= -p {pid_str}"
            if self.base_infra == "docker":
                o, e = self.execute_command_docker(cmd, server)
            else:
                o, e = self.shell_dict[server.ip].execute_command(cmd)
            # If the output is missing values for some PIDs,
            # it indicates that a process has re-started with a new PID
            # Hence, we have fetch new PIDs for those processes
            if len(o) == len(pid):
                mem = sum(int(m) for m in o)
            else:
                # Keep re-trying if the process has not booted yet
                while True:
                    pid = self.update_service_pid(server, service)
                    if len(pid) == self.service_process_count[service]:
                        break
                    time.sleep(0.5)
                continue

            timestamp = time.time()
            self.node_mem_max_dict[server.unique_ip][service] = \
                    max(mem, self.node_mem_max_dict[server.unique_ip][service])

            if mem >= self.cgroup_limits[service]["hard"]:
                if violation_counter == 0:
                    time_list = list()
                if len(time_list) <= 1:
                    time_list.append(timestamp)
                    violation_counter += 1
                else:
                    time_list[-1] = timestamp
                    violation_counter += 1
            if mem < self.cgroup_limits[service]["hard"] and violation_counter != 0:
                violation_list.append(time_list)
                time_list = list()
                violation_counter = 0
            time.sleep(interval)

        if len(time_list) != 0:
            violation_list.append(time_list)
        self.log.info(f"Service = {service}, Violation list = {violation_list}")

        total_violation_time = 0
        for arr in violation_list:
            if len(arr) > 1:
                total_violation_time += arr[1] - arr[0]

        self.log.info(f"Service = {service}, Total violation time = {total_violation_time}")
        with self.lock:
            self.limit_violation_duration[service] = total_violation_time

    def record_memory(self):

        self.node_mem_max_dict = dict()
        self.limit_violation_duration = dict()
        record_threads = list()

        for server in self.cluster.nodes_in_cluster:
            self.node_mem_max_dict[server.unique_ip] = dict()
            container_services = server.services.split(",")
            for service in container_services:
                if service in ["data", "query", "index", "fts", "analytics", "eventing"]:
                    self.node_mem_max_dict[server.unique_ip][service] = -1
                    self.log.info(f"Node mem max dict = {self.node_mem_max_dict}")
                    pid = self.node_service_pid[server.unique_ip][service]
                    self.log.info(f"Recording {service} ({pid}) memory usage in dict")
                    th = threading.Thread(target=self.record_memory_dict, args=[server, pid, service])
                    record_threads.append(th)
                    th.start()

        return record_threads

    def find_max_file(self, file):

        cat_cmd = f"cat {file}"
        shl = RemoteMachineShellConnection(self.host)
        num_list, e = shl.execute_command(cat_cmd)
        shl.disconnect()

        # Find max in the file
        local_max = -1
        for num in num_list:
            num = int(num.strip())
            local_max = max(local_max, num)
        self.log.info("File: {}, Max: {}".format(file, local_max))

        # Change global_max if local_max > global_max
        # Acquire a lock while changing the variable
        if local_max > self.global_max:
            with self.lock:
                self.global_max = local_max

    def find_max_memory_usage(self, dir):

        self.global_max = -1
        process_threads = list()

        # List all files that are part of the directory
        ls_cmd = f"ls {dir}"
        files, e = self.shell.execute_command(ls_cmd)
        self.log.info(f"Files in {dir} = {files}")
        for file in files:
            full_file_path = os.path.join(dir, file)
            th = threading.Thread(target=self.find_max_file, args=[full_file_path])
            process_threads.append(th)
            th.start()

        for th in process_threads:
            th.join()

        self.log.info(f"Max memory usage in dir {dir} = {self.global_max}")

    def get_total_threads_of_process(self):
        """
        Get the total thread count of each process running
        If a process has more than 1 copy (for eg: beam.smp has 3 copies
        - babysitter, ns_server, ns_couchdb), then we total them
        Returns a dict with key as the process name and value as it's total thread count
        """
        # TODo add other golang & java processes to the list
        processes = ["beam.smp", "memcached"]
        thread_map = dict()
        for process in processes:
            cmd = "ps huH p $(pidof " + process + ") | wc -l"
            self.log.info("Running cmd {0}".format(cmd))
            o, e = self.shell.execute_command(cmd)
            thread_map[process] = o[0].strip()
        self.log.info("Process-thread_total map {0}".format(thread_map))
        return thread_map

    def verify_total_threads_of_process(self, actual_thread_map, cores=None):
        """
        Verifies if the total_threads used by processes match with expected thread counts
        :actual_thread_map: - dict returned by get_total_threads_of_process
        :cores: (optional) - the number of cores being used by couchbase.
        """
        if cores is None:
            if self.cb_cpu_count_env not in ["None", None]:
                cores = self.cb_cpu_count_env
            elif self.cpus not in ["None", None]:
                cores = self.cpus
            else:
                cores = self.host_cpus
        # Number of threads spun by a CB process depends on the ceil(cores) used by CB
        cores = int(ceil(cores))
        self.log.info("CB server is using {0} cores".format(cores))
        for process in actual_thread_map.keys():
            actual_val = actual_thread_map[process]
            if cores > self.baseline_cores:
                expected_val = self.baseline_core_threads[process] \
                               + (cores-self.baseline_cores)*self.threads_factor[process]
            elif cores < self.baseline_cores:
                expected_val = self.baseline_core_threads[process] \
                               - (self.baseline_cores - cores)*self.threads_factor[process]
            else:
                expected_val = self.baseline_core_threads[process]
            if int(actual_val) != int(expected_val):
                self.fail("Mismatch! Expected thread_count {0}, Actual thread_count {1} "
                          "for process {2}".format(expected_val, actual_val, process))
            else:
                self.log.info("Matches fine, Expected thread_count {0}, Actual thread_count {1} "
                              "for process {2}".format(expected_val, actual_val, process))

    def test_nsserver_resource_stats(self):
        """
        1. Start a container with/without cpu container limit & witht/without mem container limit
        2. Verify prom stats
        3. Verify threads count of various processes
        """
        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        self.log.info("Stats verification successful!")

        actual_thread_map = self.get_total_threads_of_process()
        self.verify_total_threads_of_process(actual_thread_map)

    def test_dynamic_updation_of_mem_limit(self):
        """
        1. Start a container with/without mem limit of container
        2. Update the running container' mem limit
        3. Verify if prometheus stats get updated dynamically
        """
        dynamic_update_mem = self.input.param("dynamic_update_mem", 1073741824)

        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)

        self.update_container_mem_limit(mem=dynamic_update_mem)

        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        self.log.info("Stats verification successful!")

    def test_dynamic_updation_of_cpus_limit(self):
        """
        1. Start a container with/without cpus limit of container
        2. Update the running container' cpus limit
        3. Restart server
        4. Verify if prometheus stats get updated dynamically
        5. Verify if threads of processes are reduced/increased appropriately
        """
        dynamic_update_cpus = self.input.param("dynamic_update_cpus", 2)

        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        actual_thread_map = self.get_total_threads_of_process()
        self.verify_total_threads_of_process(actual_thread_map)

        self.update_container_cpu_limit(cpus=dynamic_update_cpus)
        self.restart_server()

        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        self.log.info("Stats verification successful!")

        updated_cores_used_by_cb = dynamic_update_cpus
        if self.cb_cpu_count_env:
            updated_cores_used_by_cb = self.cb_cpu_count_env
        actual_thread_map = self.get_total_threads_of_process()
        self.verify_total_threads_of_process(actual_thread_map, cores=updated_cores_used_by_cb)


    def test_cgroup_services(self):

        self.data_helper = CGroupDataHelper(self.cluster, self.host, self.task_manager,
                                            self.task, self.log, self.bucket_util)
        self.index_query_helper = CGroupQueryHelper(self.cluster, self.log)
        self.fts_helper = CGroupFTSHelper(self.cluster, self.task, self.log)
        self.eventing_helper = CGroupEventingHelper(self.cluster, self.bucket_util, self.log)
        self.cbas_helper = CGroupCBASHelper(self.cluster, self.log)

        self.record_on_disk = self.input.param("record_on_disk", False)

        # Test CGroup defaults
        self.get_cgroup_stats()
        for server in self.cluster.nodes_in_cluster:
            self.validate_cgroup_stats(server)

        self.dynamic_update = self.input.param("dynamic_update", False)

        if self.dynamic_update:
            override_th = threading.Thread(target=self.dynamic_update_cgroup_overrides)
            override_th.start()

        # Start threads which will record memory usage of services
        self.record = True
        if self.record_on_disk:
            record_th_array, directories = self.record_memory_usage()

        # Start thread that will record memory usage in dict
        threads = self.record_memory()

        ###
        self.data_helper.create_buckets(self.cluster.master, self.num_buckets,
                                        self.ram_quota, self.replicas,
                                        self.bucket_storage, self.bucket_prefix)
        self.data_helper.create_scopes_collections_for_bucket(self.num_scopes, self.num_collections)

        self.log.info("Starting initial data load to buckets")
        self.data_helper.load_data_to_bucket(0, self.num_items_per_coll)
        ###

        ###
        self.fts_helper.create_fts_indexes()
        self.log.info("Wait after building FTS indexes")
        time.sleep(60)
        self.fts_helper.run_concurrent_fts_queries()
        ###

        ###
        self.eventing_helper.create_eventing_functions()
        self.log.info("Fetching PIDs of new eventing consumers")
        for eventing_node in self.cluster.eventing_nodes:
            eventing_pids = self.update_service_pid(eventing_node, "eventing")
        self.service_process_count["eventing"] = len(eventing_pids)
        self.log.info("Service process count = {}".format(self.service_process_count))
        time.sleep(10)
        ####

        ###
        self.cbas_helper.create_cbas_functions()
        ###

        ###
        self.index_query_helper.create_secondary_indexes()
        ###

        self.log.info("Wait after creating indexes")
        time.sleep(30)

        ####
        self.index_query_helper.run_queries(num_clients=self.query_num_clients, iter=self.query_iter)
        ####

        ####
        self.log.info("Performing update workload on buckets")
        self.data_helper.load_data_to_bucket(0, self.num_items_per_coll, doc_op="update")
        ####

        if self.dynamic_update:
            self.log.info("Stopping dynamic override update thread")
            self.override_update = False
            override_th.join()

        for service in ["query", "index", "fts"]:
            self.fetch_memory_profile(service)

        self.record = False
        if self.record_on_disk:
            for th in record_th_array:
                th.join()
        for th in threads:
            th.join()

        self.log.info(f"Node Max memory dict = {self.node_mem_max_dict}")

        if self.record_on_disk:
            self.log.info("Finding max memory by using the stats recorded by ps cmd")
            for dir in directories:
                self.find_max_memory_usage(dir)

        self.log.info("Limit violation durations = {}".format(self.limit_violation_duration))


    def test_cgroup_restart_services(self):

        for server in self.cluster.nodes_in_cluster:

            arr_services = server.services.split(",")
            for service in arr_services:
                service_pids = self.node_service_pid[server.unique_ip][service]
                pid_str = " ".join(service_pids)
                kill_cmd = f"kill -9 {pid_str}"
                self.log.info(f"Killing {service} on {server.unique_ip} CMD: {kill_cmd}")
                if self.base_infra == "docker":
                    self.execute_command_docker(kill_cmd, server)
                else:
                    self.shell_dict[server.ip].execute_command(kill_cmd)

        self.log.info("Sleep after killing services")
        time.sleep(60)

        # Update node_service_pid dict with new PIDs
        self.get_service_pids()

        # Validate if the correct PIDs are populated into the cgroup folders
        self.get_cgroup_stats()

        self.log.info(f"CGroup Stats = {self.cgroup_stats}")

        for server in self.cluster.nodes_in_cluster:
            self.validate_cgroup_stats(server)


    def test_cgroup_add_node(self):

        if self.base_infra == "docker":
            # Launch a new Couchbase docker container which will be added to the existing cluster
            offset = self.num_couchbase_nodes
            container_name = self.container_name + str(offset)
            container_id = self.start_couchbase_container(host=self.host, offset=offset,
                                                          mem=self.mem, cpus=self.cpus,
                                                          image_name=self.image_name,
                                                          container_name=container_name)
            self.log.info("Container ID: {}".format(container_id))
            container_ip = self.fetch_container_ip(container_name)
            server = Container(server_info=self.host,
                               container_name=container_name,
                               container_id=container_id,
                               container_ip=container_ip,
                               container_mem=self.mem,
                               container_cpu=self.cpus)
            server.unique_ip = container_ip

            self.set_alternate_ports_for_server(server, offset=offset)
            self.log.info(f"Container_obj: {server.__dict__}")

        else:
            server = self.servers[self.num_couchbase_nodes]
            upgrade_helper = CbServerUpgrade(self.log, self.product)

            # Install the necessary version on the VM
            self.log.info(f"Installing {self.couchbase_version} on {server.ip}")
            upgrade_helper.new_install_version_on_all_nodes(
                nodes=[server],
                version=self.couchbase_version,
                cluster_profile=self.cluster_profile)

            self.log.info("Wait after installation on nodes")
            time.sleep(60)
            server.unique_ip = server.ip

        self.setup_cgroup_folders(server)
        self.set_disk_paths(server)

        # Currently, overrides are not cluster wide and needs to be set for nodes individually
        if self.overrides is not None:
            self.set_overrides([server])

        services = "data,index,query,eventing,analytics,fts"
        self.add_server_to_cluster(self.cluster.master, server, services)

        self.cluster_rebalance(self.cluster.master)

        self.cluster.nodes_in_cluster.append(server)
        server.services = services

        # Get PIDs of services on the new node
        self.node_service_pid[server.unique_ip] = dict()
        for service in server.services.split(","):
            self.update_service_pid(server, service)

        self.get_cgroup_stats()
        self.log.info(f"CGroup Stats = {self.cgroup_stats}")

        self.validate_cgroup_stats(server)


    def test_cgroup_modify_services(self):

        services = "index,query,fts,analytics"

        # Build a dict for service addition
        services_to_add = dict()
        for service in services.split(","):
            services_to_add[service] = self.cluster.nodes_in_cluster

        self.cluster_rebalance(self.cluster.master, services_to_add=services_to_add)

        for server in self.cluster.nodes_in_cluster:
            server.services += "," + services

        time.sleep(30)

        self.get_service_pids()

        self.get_cgroup_stats()
        self.log.info(f"CGroup Stats after adding services = {self.cgroup_stats}")

        for server in self.cluster.nodes_in_cluster:
            self.validate_cgroup_stats(server)

        # Remove services
        # Build a dict for service removal
        services_to_remove = dict()
        for service in services.split(","):
            services_to_remove[service] = self.cluster.nodes_in_cluster

        self.cluster_rebalance(self.cluster.master, services_to_remove=services_to_remove)

        time.sleep(30)

        self.get_service_pids()

        self.get_cgroup_stats()
        self.log.info(f"CGroup Stats after removing services = {self.cgroup_stats}")

        for server in self.cluster.nodes_in_cluster:
            self.validate_cgroup_stats(server)


    def test_cgroup_docker_upgrade(self):

        self.validate_mixed_mode = self.input.param("validate_mixed_mode", True)

        current_nodes = copy.deepcopy(self.cluster.nodes_in_cluster)

        # Swap Rebalance Upgrade Iteratively
        # Launch another container which is running couchbase-morpheus
        for i in range(self.num_couchbase_nodes):
            offset = i + self.num_couchbase_nodes
            container_name = self.container_name + str(offset)

            new_container = self.launch_couchbase_container(container_name, offset)

            # SetUp CGroup Folders
            self.setup_cgroup_folders(new_container)

            self.set_alternate_ports_for_server(new_container, offset)
            self.log.info(f"Container_obj: {new_container.__dict__}")
            time.sleep(15)
            self.set_disk_paths(new_container)

            container_to_upgrade = None
            container_name_to_upgrade = self.container_name + str(i)
            for container in current_nodes:
                if container.container_name == container_name_to_upgrade:
                    container_to_upgrade = container
            self.log.info(f"Container to upgrade = {container_to_upgrade.__dict__}")

            if container_to_upgrade.container_ip == self.cluster.master.container_ip:
                for server in self.cluster.nodes_in_cluster:
                    if server.container_ip != container_to_upgrade.container_ip:
                        self.cluster.master = server
                self.log.info(f"New master = {self.cluster.master.container_ip}")

            # Swap Rebalance
            self.add_server_to_cluster(self.cluster.master, new_container,
                                       container_to_upgrade.services)
            self.cluster_rebalance(self.cluster.master,
                                   servers_to_remove=[container_to_upgrade])

            for container in self.cluster.nodes_in_cluster:
                if container.container_ip == container_to_upgrade.container_ip:
                    self.cluster.nodes_in_cluster.remove(container)
            self.cluster.nodes_in_cluster.append(new_container)
            self.cluster.master = new_container

            self.remove_docker_container(container_to_upgrade)

            # Validate Mixed Mode State
            if self.validate_mixed_mode and i < self.num_couchbase_nodes - 1:
                status, _ = self.toggle_cgroup_setting(enable=True)
                assert status == False, "Enabling Cgroups succeeded in mixed mode state"

        # Validations post upgrade
        # Enable cgroups after the complete cluster upgrade
        status, _ = self.toggle_cgroup_setting(enable=True)
        assert status == True, "Enabling Cgroups failed after cluster upgrade"


    def get_cgroup_stats(self):

        self.cgroup_stats = dict()

        for server in self.cluster.nodes_in_cluster:
            self.cgroup_stats[server.unique_ip] = dict()
            server_services = server.services.split(",")
            for service in server_services:
                self.cgroup_stats[server.unique_ip][service] = dict()
                if self.base_infra == "docker":
                    self.cgroup_base_path = f"/sys/fs/cgroup/system.slice/docker-{server.container_id}.scope/system.slice/couchbase-server.service"
                else:
                    self.cgroup_base_path = f"/sys/fs/cgroup/system.slice/couchbase-server.service"

                cgroup_service_name = self.cgroup_name_mapping[service]
                service_cgroup_path = os.path.join(self.cgroup_base_path, "services", cgroup_service_name)
                if cgroup_service_name == "n1ql":
                    service_cgroup_path = os.path.join(service_cgroup_path, cgroup_service_name)
                self.log.info(f"Service cgroup path = {service_cgroup_path}")

                hard_limit_file = os.path.join(service_cgroup_path, "memory.max")
                soft_limit_file = os.path.join(service_cgroup_path, "memory.high")
                procs_file = os.path.join(service_cgroup_path, "cgroup.procs")

                cmd = f"cat {hard_limit_file}"
                if self.base_infra == "docker":
                    o, e = self.execute_command_docker(cmd, server)
                else:
                    o, e = self.shell_dict[server.ip].execute_command(cmd)
                self.cgroup_stats[server.unique_ip][service]["hard_limit"] = o[0]
                self.log.info(f"{hard_limit_file} = {o}")

                cmd = f"cat {soft_limit_file}"
                if self.base_infra == "docker":
                    o, e = self.execute_command_docker(cmd, server)
                else:
                    o, e = self.shell_dict[server.ip].execute_command(cmd)
                self.cgroup_stats[server.unique_ip][service]["soft_limit"] = o[0]
                self.log.info(f"{soft_limit_file} = {o}")

                cmd = f"cat {procs_file}"
                if self.base_infra == "docker":
                    o, e = self.execute_command_docker(cmd, server)
                else:
                    o, e = self.shell_dict[server.ip].execute_command(cmd)
                self.cgroup_stats[server.unique_ip][service]["procs"] = o
                self.log.info(f"{procs_file} = {o}")

    def dynamic_update_cgroup_overrides(self, interval=60):

        cgroup_overrides = copy.deepcopy(self.cgroup_limits)

        self.override_update = True
        count = 0

        while self.override_update:
            ratio = 0.75 if count % 3 == 0 else (1 if count % 3 == 1 else 1.25)
            self.log.info(f"Dynamically updating cgroup overrides, Ratio: {ratio}")
            for service in cgroup_overrides:
                new_soft_limit = "max" if cgroup_overrides[service]['soft'] == float('inf') \
                                else int((cgroup_overrides[service]['soft'] // 1024) * ratio)
                new_hard_limit = "max" if cgroup_overrides[service]['hard'] == float('inf') \
                                else int((cgroup_overrides[service]['hard'] // 1024) * ratio)
                service_name = self.cgroup_name_mapping[service]
                self.log.info(f"Setting: {service_name}, Soft Limit: {new_soft_limit}, Hard Limit: {new_hard_limit}, Timestamp: {time.time()}")
                for server in self.cluster.nodes_in_cluster:
                    status, content = ClusterRestAPI(server).\
                        set_cgroup_overrides(service_name, new_soft_limit, new_hard_limit)
                    self.log.info(f"Status: {status}, Content: {content}")
                self.cgroup_limits[service]["soft"] = float('inf') if new_soft_limit == "max" else new_soft_limit * 1024
                self.cgroup_limits[service]["hard"] = float('inf') if new_hard_limit == "max" else new_hard_limit * 1024
            self.log.info(f"Cgroup limits = {self.cgroup_limits}")
            count += 1
            time.sleep(interval)

    def validate_cgroup_stats(self, server):

        self.log.info(f"Validating Cgroup config on server: {server.unique_ip}")

        server_services = server.services.split(",")
        for service in server_services:

            # Validate soft limits
            self.log.info(f"Validating soft limit values for service: {service}")
            expected_soft_limit = self.cgroup_limits[service]["soft"] * 1024 \
                if self.cgroup_limits[service]["soft"] != float('inf') else 'max' # Converting to bytes
            actual_soft_limit = int(self.cgroup_stats[server.unique_ip][service]["soft_limit"]) \
                if self.cgroup_stats[server.unique_ip][service]["soft_limit"] != 'max' else 'max'

            assert expected_soft_limit == actual_soft_limit, \
            f"Node: {server.unique_ip}, Service: {service}, Expected_soft_limit: {expected_soft_limit}, " + \
            f"Actual_soft_limit: {actual_soft_limit}"

            # Validate hard limits
            self.log.info(f"Validating hard limit values for service: {service}")
            expected_hard_limit = self.cgroup_limits[service]["hard"] * 1024 \
                if self.cgroup_limits[service]["hard"] != float('inf') else 'max' # Converting to bytes
            actual_hard_limit = int(self.cgroup_stats[server.unique_ip][service]["hard_limit"]) \
                if self.cgroup_stats[server.unique_ip][service]["hard_limit"] != 'max' else 'max'

            assert expected_hard_limit == actual_hard_limit
            f"Node: {server.unique_ip}, Service: {service}, Expected_hard_limit: {expected_hard_limit}, " + \
            f"Actual_hard_limit: {actual_hard_limit}"

            # Validate procs file
            self.log.info(f"Validating Cgroup procs for service: {service}")
            expected_pids = self.node_service_pid[server.unique_ip][service]
            actual_pids = self.cgroup_stats[server.unique_ip][service]["procs"]

            assert set(expected_pids) == set(actual_pids)

    def remove_docker_container(self, container):

        self.log.info(f"Stopping container {container.container_name}")
        stop_cmd = f"docker stop {container.container_name}"
        o, e = self.shell.execute_command(stop_cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))
        self.log.info(f"Removing containers {container.container_name}")
        rm_cmd = f"docker rm {container.container_name}"
        o, e = self.shell.execute_command(rm_cmd)
        self.log.info("Output:{0}, Error{1}".format(o, e))