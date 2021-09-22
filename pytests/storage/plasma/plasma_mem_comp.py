'''
Created on 22-Aug-2021

@author: sanjit chauhan
'''
from storage.plasma.plasma_base import PlasmaBaseTest
from gsiLib.gsiHelper import GsiHelper


class PlasmaMemCompTest(PlasmaBaseTest):
    def setUp(self):
        super(PlasmaMemCompTest, self).setUp()
        query_node = self.cluster.query_nodes[0]
        if self.in_mem_comp:
            self.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval}, query_node)
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
        compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field)
        for taskInstance in compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        self.index_count = self.input.param("index_count", 2)
        self.num_replicas = self.input.param("num_replicas", 1)
        self.items_add = self.input.param("items_add", 500)
        start = self.num_items
        i = 0
        self.mem_used = self.input.param("mem_used", 10)
        self.time_out = self.input.param("time_out",60)
        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start = start + (i * self.items_add)
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            self.data_load()
            i += 1

        if self.in_mem_comp:
            self.sleep(self.time_out)
            if self.verify_compression_stat(self.cluster.index_nodes):
                self.fail("Seeing index data compressed though mem_used_storage is less than 50 percent")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    '''
    1. Enable compression
        a. indexer.plasma.backIndex.enableInMemoryCompression
        b. indexer.plasma.mainIndex.enableInMemoryCompression
    2. Perform delete operation
    3. Wait for pages to merge
    4. Verify using stat - 'merges' > 0, as pages should merge after delete 
    5. Trigger compression and verify num_compressed_pages stat >0
    6. Run full scan and validate the result
    '''

    def test_merges_data_integrity(self):
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
        self.delete_start = 0
        self.delete_perc = self.input.param("delete_perc", 50)
        self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
        self.log.info("Delete items count is:"+str(self.delete_end))
        self.gen_create = None
        self.generate_docs(doc_ops="delete")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        stat_obj_list = self.create_Stats_Obj_list()
        self.counter = self.input.param("counter",30)
        self.time_out = self.input.param("time_out", 60)
        self.perform_plasma_mem_ops("compactAll")
        isMerge = False
        for count in range(self.counter):
            field_value_list = self.get_plasma_index_stat_value("merges", stat_obj_list)
            for field_instance in field_value_list:
                self.log.info("Field Value instance is:"+str(field_instance))
            for field_value in field_value_list:
                if field_value > 0:
                    isMerge = True
                    break
            if isMerge:
                break
            else:
                self.sleep(self.counter, "waiting for merge to go more than 0")
        self.assertTrue(isMerge, "Merge has not been started")
        self.perform_plasma_mem_ops("persistAll")
        self.create_start = self.init_items_per_collection
        self.create_end = 2 * self.init_items_per_collection
        self.gen_delete = None
        self.generate_docs(doc_ops="create")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)

        # Perform explicit compress operation
        self.perform_plasma_mem_ops("compressAll")
        for count in range(self.counter):
            if self.verify_compression_stat(self.cluster.index_nodes):
                break
            else:
                self.sleep(self.counter, "waiting for compression to complete")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    """
    1. Enable compression
        a. indexer.plasma.backIndex.enableInMemoryCompression
        b. indexer.plasma.mainIndex.enableInMemoryCompression
    2. Perform swapout operation explicitly for all plasma instance using explict call curl <index-node>:9102/plasmaDiag till rr =0
    3. Perform swapin operation till rr =1
    4. Trigger the compression explicitly
    5. Validate the num_compressed_pages >0
    6. Run full scan and validate the result
    """

    def test_swap_in_data_integrity(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.num_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field)
        for taskInstance in compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.counter = self.input.param("counter", 30)
        # Perform explicit swapout operation
        self.perform_plasma_mem_ops("evictAll")
        stat_obj_list = self.create_Stats_Obj_list()
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 0),
                        "Resident ratio not getting back to 0 after swap-out")

        # Perform full scan to perform swap-in
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body')
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1, timeout= 100),
                        "Resident ratio not getting back to 1 after swap-in")
        self.perform_plasma_mem_ops("compressAll")
        for count in range(self.counter):
            if self.verify_compression_stat(self.cluster.index_nodes):
                break
            else:
                self.sleep(self.counter, "waiting for compression to complete")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    """
        1. Enable compression
         a. indexer.plasma.backIndex.enableInMemoryCompression
         b. indexer.plasma.mainIndex.enableInMemoryCompression
        2. Perform swapout operation explicitly for all plasma instance using explict call curl <index-node>:9102/plasmaDiag till rr =0
        3. Perform swapin operation till rr =1
        4. Perform delete operation sequentially to trigger page merges. Verify using stat - 'merges' > 0, as pages should merge after delete
        5. Trigger the compression explicitly
        6. Validate the num_compressed_pages >0
        7. Run full scan and validate the result
        """

    def test_swap_in_with_merge(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.num_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.query_limit = self.input.param("query_limit", 50)
        compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field, limit=self.query_limit)
        for taskInstance in compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # Perform explicit swapout operation
        self.perform_plasma_mem_ops("evictAll")
        stat_obj_list = self.create_Stats_Obj_list()
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 0),
                        "Resident ratio back to 0 after swap-out")

        # Perform full scan to perform swap-in
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body')
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1),
                        "Resident ratio back to 1 after swap-in")
        self.counter = self.input.param("counter", 30)
        self.delete_start = 0
        self.delete_perc = self.input.param("delete_perc", 70)
        self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
        self.log.info("delete item count is:"+str(self.delete_end))
        self.log.info("num item per collection:"+str(self.init_items_per_collection))
        self.gen_create = None
        self.generate_docs(doc_ops="delete")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        stat_obj_list = self.create_Stats_Obj_list()

        isMerge = False
        for count in range(self.counter):
            field_value_list = self.get_plasma_index_stat_value("merges", stat_obj_list)
            for field_value in field_value_list:
                if field_value > 0:
                    isMerge = True
                    break
            if isMerge:
                break
            else:
                self.sleep(self.counter, "waiting for merge to go more than 0")
        self.assertTrue(isMerge, "Merge has not been started")

        self.perform_plasma_mem_ops("compressAll")
        for count in range(self.counter):
            if self.verify_compression_stat(self.cluster.index_nodes):
                break
            else:
                self.sleep(self.counter, "waiting for compression to complete")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    def test_MB_47503(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.num_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        stat_obj_list = self.create_Stats_Obj_list()
        lss_data_size_list = self.get_plasma_index_stat_value("lss_data_size", stat_obj_list)
        compacts_value_list = self.get_plasma_index_stat_value("compacts", stat_obj_list)

        for node in self.cluster.index_nodes:
            self.kill_indexer(node, 1)
        self.delete_start = 0
        self.delete_perc = self.input.param("delete_perc", 100)
        self.delete_end = self.delete_perc * .01 * self.init_items_per_collection
        self.gen_create = None
        self.generate_docs(doc_ops="delete")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.sleep(600, "waiting before triggering mutation")
        self.gen_delete = None
        self.create_start = 0
        self.create_end = 100
        self.generate_docs(doc_ops="create")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.sleep(600, "waiting before triggering compact")
        # Perform explicit compress operation
        self.perform_plasma_mem_ops("compactAll")
        isTotal_record = False
        for count in range(self.counter):
            field_value_list = self.get_plasma_index_stat_value("total_records", stat_obj_list)
            for field_value in field_value_list:
                if field_value == self.create_end:
                    isTotal_record = True
                    break
            if isTotal_record:
                break
            else:
                self.sleep(self.counter, "waiting for total record value to come down")
