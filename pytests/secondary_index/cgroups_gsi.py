import time
import json
import re

from math import ceil

from StatsLib.StatsOperations import StatsHelper
from pytests.cgroup_limits.cgroup_base import CGroupBase
from gsiLib.gsiHelper import GsiHelper

LOG_FILE_INDEXER = "indexer.log"
LOG_FILE_PROJECTOR = "projector.log"
REGEX_INDEXER = "setGlobalSettings"
REGEX_PROJECTOR = "Projector"


class CGroup(CGroupBase):
    def setUp(self):
        super(CGroup, self).setUp()
        if self.cpus in [None, "None"]:
            self.cpus = self.get_host_cpu_cores()
        if self.mem in [None, "None"]:
            self.mem = self.get_host_mem_in_bytes()

    def tearDown(self):
        pass

    def _parse_log(self, matched_lines, re_list):
        matches = {}
        for line in matched_lines:
            for r in re_list:
                match = re.findall(r, line)
                if match:
                    if "memory" in r:
                        matches['memory'] = int(match[0])
                    if "CPU" in r:
                        matches['cpu'] = int(match[0])
        return matches

    def parse_indexer_log(self, matched_lines):
        """
        accepts the logged lines as input and returns a dict of
        {"cpu":num_cpu_cores, "memory":memory_total}
        """
        re_list = ['CPU cores: ([0-9]*)', 'memory quota: ([0-9]*) bytes']
        return self._parse_log(matched_lines, re_list)

    def parse_projector_log(self, matched_lines):
        """
        accepts the logged lines as input and returns a dict of
        {"cpu":num_cpu_cores, "memory":memory_total}
        """
        # re_cpu, re_mem = 'Projector CPU set at ([0-9]*)', '"memory_total":([0-9]*)'
        re_list = ['"memory_total":([0-9]*)', 'Projector CPU set at ([0-9]*)']
        return self._parse_log(matched_lines, re_list)

    def test_indexer_process(self):
        """
        1. Start a container with/without cpu container limit & witht/without mem container limit
        2. Verify indexer stats
        3. Verify allocated memory and cpu cores from the logs
        """
        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        print("Latest host aware stats:{} \n Latest cgroup aware stats:{}".format(latest_host_aware_stats_map,
                                                                                  latest_cgroup_aware_stats_map))
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        self.log.info("Stats verification successful!")
        matched_lines = self.read_logs(log_file=LOG_FILE_INDEXER, regex=REGEX_INDEXER)
        print("Matched lines:{}".format(matched_lines))
        gsi_helper_obj = GsiHelper(self.node, self.log)
        time.sleep(30)
        resp_json = (gsi_helper_obj.get_index_stats())
        print("Response is {}".format(resp_json))
        if resp_json['memory_total'] != self.mem:
            self.fail("Mismatch --- memory from stats {} , but container runs with {}".
                      format(resp_json['memory_total'], self.mem))
        if resp_json['num_cpu_core'] != self.cpus:
            self.fail("Mismatch --- cpu from stats {} , but container runs with {}".
                      format(resp_json['num_cpu_core'], self.cpus))
        resource_dict = self.parse_indexer_log(matched_lines)
        cpu_log, mem_log = resource_dict['cpu'], resource_dict['memory']
        print("Service memory map:{}".format(self.service_and_memory_allocation.iteritems()))
        for service, memory in self.service_and_memory_allocation.iteritems():
            if service == "index":
                allocated_mem = int(memory) * 1024 * 1024
        if cpu_log != self.cpus:
            self.fail("Mismatch in the indexer log--- CPU from log {} , but container is running with {}".
                      format(cpu_log, self.cpus))
        if mem_log != allocated_mem:
            self.fail(
                "Mismatch in the indexer log--- memory in bytes allocated from the log {} , but container is running with {}".
                format(mem_log, allocated_mem))

    def test_projector_process(self):
        """
        1. Start a container with/without cpu container limit & witht/without mem container limit
        2. Verify memory and cpu allocated from the projector stats
        """
        latest_host_aware_stats_map = self.read_latest_host_aware_stats()
        latest_cgroup_aware_stats_map = self.read_latest_cgroup_aware_stats()
        print("Latest host aware stats:{} \n Latest cgroup aware stats:{}".format(latest_host_aware_stats_map,
                                                                                  latest_cgroup_aware_stats_map))
        self.verify_values_of_host_stats(latest_host_aware_stats_map)
        self.verify_values_of_cgroup_stats(latest_cgroup_aware_stats_map)
        self.log.info("Stats verification successful!")
        time.sleep(30)
        matched_lines = self.read_logs(log_file=LOG_FILE_PROJECTOR, regex=REGEX_PROJECTOR)
        print("Matched lines:{}".format(matched_lines))
        resource_dict = self.parse_projector_log(matched_lines)
        cpu_log, mem_log = resource_dict['cpu'], resource_dict['memory']
        print("CPU from logs is {}. Memory from logs is {}".format(cpu_log, mem_log))
        if cpu_log != 100 * int(self.cpus):
            self.fail("Mismatch in the projector log--- CPU from log {} , but container is running with {}".
                      format(cpu_log, self.cpus))
        if mem_log != self.mem:
            self.fail(
                "Mismatch in the projector log--- memory in bytes allocated from the log {} , but container is running with {}".
                format(mem_log, allocated_mem))
