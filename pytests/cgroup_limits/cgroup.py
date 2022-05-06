from StatsLib.StatsOperations import StatsHelper
from pytests.cgroup_limits.cgroup_base import CGroupBase


class CGroup(CGroupBase):
    def setUp(self):
        super(CGroup, self).setUp()

    def tearDown(self):
        pass

    def test_nsserver_resource_stats(self):
        content = StatsHelper(self.node).get_range_api_metrics("sys_cpu_cores_available")
        act_val = content['data'][0]['values'][-1][1]
        if act_val != str(self.cpus):
            self.fail("sys_cpu_cores_available stat {0} does not match container's cpus limit {1}".
                      format(act_val, self.cpus))

        content = StatsHelper(self.node).get_range_api_metrics("sys_mem_limit")
        act_val = content['data'][0]['values'][-1][1]
        if act_val != str(self.mem):
            self.fail("sys_mem_limit stat {0} does not match container's mem limit {1}".
                      format(act_val, self.mem))

        self.log.info("Both sys_cpu_cores_available and sys_mem_limit stats obeyed container limits")


