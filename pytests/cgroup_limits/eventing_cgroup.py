from EventingLib.EventingOperations_Rest import EventingHelper
from pytests.cgroup_limits.cgroup_base import CGroupBase
from membase.api.rest_client import RestConnection
from Cb_constants import CbServer


class EventingCgroup(CGroupBase):
    def setUp(self):
        super(EventingCgroup, self).setUp()
        self.eventing_helper = EventingHelper(self.servers[0])
        self.rest = RestConnection(self.servers[0])

    def tearDown(self):
        pass

    def verify_cpu_count_value(self, cpu_count):
        actual_value = int(self.eventing_helper.get_cpu_count())
        if actual_value != cpu_count:
            self.fail("actual cgroup cpus {0}, but expected cpu count {1}".
                format(actual_value, cpu_count))

    def test_dynamic_updation_of_cpus_limit(self):
        dynamic_update_cpus = self.input.param("dynamic_update_cpus", 2)
        self.verify_cpu_count_value(self.cpus)
        self.update_container_cpu_limit(cpus=dynamic_update_cpus)
        self.restart_server()
        self.verify_cpu_count_value(dynamic_update_cpus)

    def test_dynamic_updation_of_memory_limit(self):
        dynamic_update_mem = self.input.param("dynamic_update_mem", 805306368)
        self.update_container_mem_limit(mem=dynamic_update_mem)
        try:
            self.rest.set_service_mem_quota(
                {CbServer.Settings.EVENTING_MEM_QUOTA:
                     self.self.service_and_memory_allocation['eventing']})
        except Exception:
            self.log.info("updating eventing ram quota beyond cgroup limits "
                          "failed as expected")