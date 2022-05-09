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
        self.host_mem_bytes = self.get_host_mem_in_bytes()
        self.host_cpus = self.get_host_cpu_cores()

    def tearDown(self):
        pass

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

    def test_nsserver_resource_stats(self):
        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        self.log.info("latest_host_aware_stats_map {0}".format(latest_host_aware_stats_map))
        self.log.info("latest_cgroup_aware_stats_map {0}".format(latest_cgroup_aware_stats_map))
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        self.log.info("Stats verification successful!")
