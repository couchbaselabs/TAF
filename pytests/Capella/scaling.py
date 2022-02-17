'''
Created on Apr 20, 2022

@author: ritesh.agarwal
'''
from Capella.capella_base import CapellaBase
# from TestInput import TestInputServer
# from capella.internal_api import capella_utils as CapellaAPI

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
        new_config = self.cluster.details
        new_config["specs"][0]["count"] += 2
        rebalance_task = self.task.async_rebalance_capella(self.pod,
                                                           self.tenant,
                                                           self.cluster,
                                                           new_config["specs"])
        self.task_manager.get_task_result(rebalance_task)
        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()
        self.retry_failures(tasks)
