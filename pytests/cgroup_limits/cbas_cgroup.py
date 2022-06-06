from pytests.cgroup_limits.cgroup_base import CGroupBase
from CbasLib.CBASOperations import CBASHelper


class CBASCGroup(CGroupBase):
    def setUp(self):
        super(CBASCGroup, self).setUp()
        self.cbas_helper = CBASHelper(self.servers[0])

    def tearDown(self):
        pass

    def get_cpu_count_from_cbas_diagnostics(self):
        response = self.cbas_helper.get_analytics_diagnostics(timeout=120)
        input_args = response["nodes"][0]["runtime"]["inputArguments"]
        for arguement in input_args:
            if "ActiveProcessorCount" in arguement:
                return int(arguement.split("=")[1])
        return 0

    def get_actual_number_of_partitions(self):
        response = self.cbas_helper.get_analytics_diagnostics(timeout=120)
        partition_info = response["nodes"][0]["cc"]["partitions"]
        return len(partition_info)

    def get_expected_number_of_partitions(self, cpu_count, memory):
        max_partition_to_create = min((int(memory)/1024), 16)
        actual_partitions_created = min(cpu_count, max_partition_to_create)
        return actual_partitions_created

    def get_cbas_memory_allocated(self):
        return self.service_and_memory_allocation["analytics"]

    def test_cbas_autopartioning(self):
        cpu_count = self.get_cpu_count_from_cbas_diagnostics()
        expected_number_of_partitions = self.get_expected_number_of_partitions(
            cpu_count, self.get_cbas_memory_allocated())
        actual_number_of_partitions = self.get_actual_number_of_partitions()
        self.log.info("Expected number of partitions : {0} Actual number of "
                      "partitions : {1}".format(
            expected_number_of_partitions, actual_number_of_partitions))
        if expected_number_of_partitions != actual_number_of_partitions:
            self.fail("Expected number of partitions is not equal to Actual "
                      "number of partitions")

    def test_cbas_multiple_disk_paths(self):
        expected_number_of_partitions = self.service_disk_paths["analytics"]
        actual_number_of_partitions = self.get_actual_number_of_partitions()
        self.log.info("Expected number of partitions : {0} Actual number of "
                      "partitions : {1}".format(
            expected_number_of_partitions, actual_number_of_partitions))
        if expected_number_of_partitions != actual_number_of_partitions:
            self.fail("Expected number of partitions is not equal to Actual "
                      "number of partitions")

    def test_effects_of_dynamic_updation_of_cpus_limit(self):
        dynamic_update_cpus = self.input.param("dynamic_update_cpus", 2)
        cpu_count = self.get_cpu_count_from_cbas_diagnostics()
        expected_number_of_partitions = self.get_expected_number_of_partitions(
          cpu_count, self.get_cbas_memory_allocated())
        actual_number_of_partitions = self.get_actual_number_of_partitions()
        self.log.info("Expected number of partitions : {0} Actual number of "
                      "partitions : {1}".format(
            expected_number_of_partitions, actual_number_of_partitions))
        if expected_number_of_partitions != actual_number_of_partitions:
            self.fail("Expected number of partitions is not equal to Actual "
                      "number of partitions before dynamic updation of CPU "
                      "limit")

        self.update_container_cpu_limit(cpus=dynamic_update_cpus)
        self.restart_server()

        actual_number_of_partitions = self.get_actual_number_of_partitions()
        self.log.info("Expected number of partitions : {0} Actual number of "
                      "partitions : {1}".format(
            expected_number_of_partitions, actual_number_of_partitions))
        if expected_number_of_partitions != actual_number_of_partitions:
            self.fail("Expected number of partitions is not equal to Actual "
                      "number of partitions after dynamic updation of CPU "
                      "limit")
