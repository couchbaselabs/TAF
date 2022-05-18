import time
from math import ceil

from StatsLib.StatsOperations import StatsHelper
from pytests.cgroup_limits.cgroup_base import CGroupBase


class CGroup(CGroupBase):
    def setUp(self):
        super(CGroup, self).setUp()
        self.cgroup_aware_stats = ["sys_cpu_utilization_rate", "sys_cpu_sys_rate",
                                   "sys_cpu_user_rate", "sys_cpu_cores_available",
                                   "sys_mem_limit"]
        self.host_aware_stats = ["sys_cpu_host_utilization_rate", "sys_cpu_host_sys_rate",
                                 "sys_cpu_host_user_rate", "sys_cpu_host_cores_available",
                                 "sys_mem_total"]
        self.baseline_cores = 4  # A typical VM we use
        # Total thread count of processes on a container having baseline_cores
        self.baseline_core_threads = {"beam.smp": 106, "memcached": 29}
        # by how much they increase/decrease per core increase/decrease
        self.threads_factor = {"beam.smp": 4, "memcached": 1}
        self.host_mem_bytes = self.get_host_mem_in_bytes()
        self.host_cpus = self.get_host_cpu_cores()

    def tearDown(self):
        pass

    def restart_server(self):
        self.log.info("Stopping and starting server...")
        self.shell.execute_command("docker exec db service couchbase-server stop")
        time.sleep(10)
        self.shell.execute_command("docker exec db service couchbase-server start")
        time.sleep(20)

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

    def read_latest_cgroup_aware_stats(self):
        """
        Reads cgroup aware stats reported by prometheus
        """
        latest_cgroup_aware_stats_map = dict()
        for stat in self.cgroup_aware_stats:
            content = StatsHelper(self.node).get_range_api_metrics(stat)
            val = content['data'][0]['values'][-1][1]
            latest_cgroup_aware_stats_map[stat] = val
        self.log.info("latest_cgroup_aware_stats_map {0}".format(latest_cgroup_aware_stats_map))
        return latest_cgroup_aware_stats_map

    def read_latest_host_aware_stats(self):
        """
        Reads host aware stats reported by prometheus
        """
        latest_host_aware_stats_map = dict()
        for stat in self.host_aware_stats:
            content = StatsHelper(self.node).get_range_api_metrics(stat)
            val = content['data'][0]['values'][-1][1]
            latest_host_aware_stats_map[stat] = val
        self.log.info("latest_host_aware_stats_map {0}".format(latest_host_aware_stats_map))
        return latest_host_aware_stats_map

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
