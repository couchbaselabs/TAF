'''
Created on Apr 20, 2022

@author: ritesh.agarwal
'''
from Capella.capella_base import CapellaBase
from Cb_constants.CBServer import CbServer
# from TestInput import TestInputServer
# from capella.internal_api import CapellaUtils as CapellaAPI


class ScalingTests(CapellaBase):
    def setUp(self):
        super(ScalingTests, self).setUp()

    def tearDown(self):
        super(ScalingTests, self).tearDown()

    def test_scaling(self):
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.new_loader(wait=True)
        self.data_validation()
        
        self.delete_start = self.create_start
        self.delete_end = self.create_end
        self.delete_perc = 50
        self.create_start = self.create_end
        self.create_end += self.init_items_per_collection
        self.create_perc = 50
        tasks = self.new_loader(wait=False)
        initial_services = self.input.param("services", CbServer.Services.KV)
        services = self.input.param("rebl_services", initial_services)
        server_group_list = list()
        for service_group in services.split("-"):
            service_group = service_group.split(":")
            num_nodes = 4
            config = {
                "size": num_nodes,
                "services": service_group,
                "compute": self.input.param("compute", "n2-custom-16-32768"),
                "storage": {
                    "type": self.input.param("type", "PD-SSD"),
                    "size": self.input.param("size", 50),
                    "iops": self.input.param("iops", 3000)
                }
            }
            if self.capella_cluster_config["place"]["hosted"]["provider"] != "aws":
                config["storage"].pop("iops")
            server_group_list.append(config)

        rebalance_task = self.task.async_rebalance_capella(self.pod,
                                                           self.tenant,
                                                           self.cluster,
                                                           server_group_list)

        self.task_manager.get_task_result(rebalance_task)
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()
        self.retry_failures(tasks)
