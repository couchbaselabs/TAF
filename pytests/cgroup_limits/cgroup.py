import time
from math import ceil

from StatsLib.StatsOperations import StatsHelper
from pytests.cgroup_limits.cgroup_base import CGroupBase


class CGroup(CGroupBase):
    def setUp(self):
        super(CGroup, self).setUp()
        self.baseline_cores = 4  # A typical VM we use
        # Total thread count of processes on a container having baseline_cores
        self.baseline_core_threads = {"beam.smp": 106, "memcached": 29}
        # by how much they increase/decrease per core increase/decrease
        self.threads_factor = {"beam.smp": 4, "memcached": 1}

    def tearDown(self):
        pass

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
