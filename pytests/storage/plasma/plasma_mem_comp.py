'''
Created on 22-Aug-2021

@author: sanjit chauhan
'''
from storage.plasma.plasma_base import PlasmaBaseTest


class PlasmaMemCompTest(PlasmaBaseTest):
    def setUp(self):
        super(PlasmaMemCompTest, self).setUp()
        query_node = self.cluster.query_nodes[0]
        if self.in_mem_comp:
            self.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval}, query_node)
            self.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval}, query_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": True}, query_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": True}, query_node)

    def tearDown(self):
        super(PlasmaMemCompTest, self).tearDown()

    def test_compression_data_integrity(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.num_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.num_item_per_coll = self.input.param("num_item_per_coll", 150)
        compare_task = self.validate_index_data(indexMap, self.num_item_per_coll, field)
        for taskInstance in compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        self.index_count = self.input.param("index_count", 2)
        self.num_replicas = self.input.param("num_replicas", 1)
        self.items_add = self.input.param("items_add", 500)
        start = self.num_items
        i = 0
        self.mem_used = self.input.param("mem_used", 10)
        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start = start + (i * self.items_add)
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            self.data_load()
            i += 1

        if self.in_mem_comp:
            self.sleep(60)
            if self.verify_compression_stat(self.cluster.index_nodes):
                self.fail("Seeing index data compressed though mem_used_storage is less than 50 percent")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.num_items)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)