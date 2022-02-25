'''
Created on 22-Aug-2021

@author: sanjit chauhan
'''
import time

from Cb_constants import CbServer
from membase.api.rest_client import RestConnection
from storage.plasma.plasma_base import PlasmaBaseTest
from gsiLib.gsiHelper import GsiHelper

class PlasmaMemCompTest(PlasmaBaseTest):
    def setUp(self):
        super(PlasmaMemCompTest, self).setUp()
        index_node = self.cluster.index_nodes[0]
        if self.in_mem_comp:
            self.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": True}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": True}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableCompressDuringBurst": True}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableCompressDuringBurst": True}, index_node)
            self.set_index_settings({"indexer.settings.compaction.plasma.manual": self.manual},
                                    index_node)
            self.set_index_settings({"indexer.plasma.purger.enabled": self.purger_enabled},
                                    index_node)
        self.items_add = self.input.param("items_add", 500)
        self.query_limit = self.input.param("query_limit", 30000)
        self.time_out = self.input.param("time_out", 60)
        self.resident_ratio = \
            float(self.input.param("resident_ratio", .99))
        self.exp_compression_count = self.input.param("exp_compression_count", 1000)

    def tearDown(self):
        super(PlasmaMemCompTest, self).tearDown()

    def test_compression_data_integrity(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        self.index_count = self.input.param("index_count", 2)
        self.items_add = self.input.param("items_add", 500)
        start = self.init_items_per_collection
        i = 0
        self.mem_used = self.input.param("mem_used", 50)
        self.time_out = self.input.param("time_out",60)
        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start = int(start + (i * self.items_add))
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            self.data_load()
            i += 1
        totalCount = self.create_end
        self.assertTrue(self.verify_bucket_count_with_index_count(indexMap, totalCount, field))
        self.wait_for_compression = self.input.param("wait_for_compression", False)
        if self.wait_for_compression:
            self.sleep(2 * self.sweep_interval, "Waiting for items to compress")
        else:
            self.perform_plasma_mem_ops("compressAll")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "Compression not triggered")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field, limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    '''
    1. Enable compression
        a. indexer.plasma.backIndex.enableInMemoryCompression
        b. indexer.plasma.mainIndex.enableInMemoryCompression
    2. Perform delete operation
    3. Perform compaction
    4. Add more document
    5. Wait for pages to merge
    6. Verify using stat - 'merges' > 0, as pages should merge after delete 
    7. Trigger compression and verify num_compressed_pages stat >0
    8. Run full scan and validate the result
    '''

    def test_merges_data_integrity(self):
        field = 'body'
        self.counter = self.input.param("counter", 30)
        self.items_add = self.input.param("items_add", 500)
        stat_obj_list = self.create_Stats_Obj_list()
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        self.mem_used = self.input.param("mem_used", 50)
        self.time_out = self.input.param("time_out", 60)
        start = self.init_items_per_collection
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
        field_value_list = self.get_plasma_index_stat_value("merges", stat_obj_list)
        self.delete_by_query = self.input.param("delete_by_query", True)

        if self.delete_by_query:
            self.indexUtil.delete_docs_with_field(self.cluster, indexMap)
            self.log.debug("After delete")
        else:
            self.delete_start = 0
            self.delete_perc = self.input.param("delete_perc", 100)
            self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
            self.log.info("Delete items count is:" + str(self.delete_end))
            self.gen_create = None
            self.generate_docs(doc_ops="delete")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
        field_value_list = self.get_plasma_index_stat_value("merges", stat_obj_list)
        self.time_out = self.input.param("time_out", 60)
        self.perform_plasma_mem_ops("compactAll")

        # performing more mutations by adding more documents
        self.log.debug("Adding more documents")
        self.create_start = self.init_items_per_collection
        self.create_end = 2 * self.init_items_per_collection
        self.gen_delete = None
        self.generate_docs(doc_ops="create")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        isMerge = False
        for count in range(self.counter):
            field_value_list = self.get_plasma_index_stat_value("merges", stat_obj_list)
            for field_instance in field_value_list:
                self.log.info("Field Value instance is:" + str(field_instance))
            for field_value in field_value_list:
                if field_value > 0:
                    isMerge = True
                    break
            if isMerge:
                break
            else:
                self.sleep(self.counter, "waiting for merge to go more than 0")
        self.assertTrue(isMerge, "Merge has not been started")
        self.modified_moi_snapshot_interval = self.input.param("modified_moi_snapshot_interval", 15000)
        self.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.modified_moi_snapshot_interval},
                                self.cluster.query_nodes[0])
        i = 0
        self.create_start = self.create_end
        self.gen_delete = None

        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start +=  (i * self.items_add)
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            self.log.debug("Tried items to load items from {} to {}".format(self.create_start, self.create_end))
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            i += 1
        # Perform explicit compress operation
        self.log.debug("triggering compress All operation")
        self.perform_plasma_mem_ops("compressAll")
        for count in range(self.counter):
            if self.verify_compression_stat(self.cluster.index_nodes):
                break
            else:
                self.sleep(self.counter, "waiting for compression to complete")
        self.log.info("query limit is:"+str(self.query_limit))
        self.log.info("total is:"+str(self.init_items_per_collection))
        mem_comp_compare_task = self.validate_index_data(indexMap=indexMap, totalCount=self.create_end, field=field, limit=self.query_limit)
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
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        # Perform explicit swapout operation
        self.sleep(self.wait_timeout, "waiting for {} secs to before triggering evictAll".format(self.wait_timeout))
        self.perform_plasma_mem_ops("evictAll")

        stat_obj_list = self.create_Stats_Obj_list()
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 0, ops='equal'),
                        "Resident ratio not getting back to 0 after swap-out")

        # Perform full scan to perform swap-in
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body', totalCount=self.init_items_per_collection, limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1, ops='equal', timeout= 100),
                        "Resident ratio not getting back to 1 after swap-in")
        self.perform_plasma_mem_ops("compressAll")
        for count in range(self.counter):
            if self.verify_compression_stat(self.cluster.index_nodes):
                break
            else:
                self.sleep(self.counter, "waiting for compression to complete")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "Compression not triggered")
        mem_comp_compare_task = self.validate_index_data(indexMap=indexMap, totalCount=self.create_end, field=field,
                                                         limit=self.query_limit)
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
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        # Perform explicit swapout operation
        self.perform_plasma_mem_ops("evictAll")
        stat_obj_list = self.create_Stats_Obj_list()
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 0, ops='equal'),
                        "Resident ratio back to 0 after swap-out")

        # Perform perform swap-in
        self.perform_plasma_mem_ops("swapinAll")
        stat_obj_list = self.create_Stats_Obj_list()
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1, ops='equal'),
                         "Resident ratio back to 1 after swap-in")
        self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1, ops='equal')

        self.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": 120}, self.cluster.index_nodes[0])
        self.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": 120}, self.cluster.index_nodes[0])
        self.sleep(60, "waiting for settings to implement")
        self.counter = self.input.param("counter", 30)
        self.delete_by_query = self.input.param("delete_by_query", True)
        self.create_start=0
        self.gen_create = None
        if self.delete_by_query:
            self.indexUtil.delete_docs_with_field(self.cluster, indexMap)
            self.log.debug("After delete")
            self.create_start = self.init_items_per_collection
        else:
            self.delete_start = 0
            self.delete_perc = self.input.param("delete_perc", 70)
            self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
            self.log.info("delete item count is:"+str(self.delete_end))
            self.generate_docs(doc_ops="delete")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.create_start = self.delete_end
        stat_obj_list = self.create_Stats_Obj_list()
        self.gen_delete = None

        # Adding more items
        self.create_end = self.create_start + self.items_add
        self.generate_docs(doc_ops="create")
        self.log.debug("create start is {} and create end is {}".format(self.create_start, self.create_end))
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.sleep(self.wait_timeout, "waiting for indexes to settle down")
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

        total_items = self.create_end - self.create_start

        self.perform_plasma_mem_ops("compressAll")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "Compression not triggered")
        mem_comp_compare_task = self.validate_index_data(indexMap, total_items, field, limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    def test_MB_47503(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
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
        self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
        self.gen_create = None
        self.generate_docs(doc_ops="delete")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.gen_delete = None
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
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "total_records", self.create_end, ops='equal', timeout=100),
                        "Total records stats not settled down")
        self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_data_size", lss_data_size_list,
                                                             ops='equalOrLessThan', timeout=100),
                        "data_size stats not settled down")
        self.assertTrue(self.compare_aggregate_stat(stat_obj_list, 'compacts', 0, ops='greater', retry=10, sleep=20), "Compacts count is not as expected")

    """
    1. Enable compression
        a. indexer.plasma.backIndex.enableInMemoryCompression
        b. indexer.plasma.mainIndex.enableInMemoryCompression
    2. Trigger compression
    3. Validate num_compressed_pages > 0
    4. Trigger compaction
    5. Validate compacts stats > 0 and num_compressed_page should be less than step 3.
    6. Run full scan and validate the result
    """
    def test_compaction(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False, timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        i = 0
        self.mem_used = self.input.param("mem_used", 60)
        self.items_add = self.input.param("items_add", 500)
        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start = self.create_end + (i * self.items_add)
            self.create_end = self.create_start + self.items_add
            self.different_field = True
            self.generate_docs(doc_ops="create")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            i += 1

        # Perform explicit compress operation
        self.perform_plasma_mem_ops("compressAll")
        self.sleep(self.wait_timeout, "waiting for {} secs to complete the compression".format(self.wait_timeout))
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "Compression not triggered")
        compressed_items = self.get_plasma_index_stat_value("num_rec_compressed", stat_obj_list)
        # Perform explicit compaction operation
        self.perform_plasma_mem_ops("compactAll")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list,"compacts", 0, ops='greater', timeout=100), "compaction not triggering")
        self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "num_rec_compressed", compressed_items, ops='equalOrLessThan',timeout=100),
                        "compression not coming down after compaction")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field, limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)
    """
        1. Set indexer.settings.compaction.plasma.manual to true 
        2. Trigger log cleaner
        3. Trigger compression and validate num_compressed_pages > 0
        4. Trigger log cleaner again
        5. Perform full index scan and verify that scan results are expected.
    """
    def test_log_cleaning(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False, timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.sleep(self.wait_timeout, "waiting for {} secs to complete the compression".format(self.wait_timeout))
        #perform log cleaning
        self.perform_plasma_mem_ops("logClean")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list,"lss_gc_num_reads", 0, ops='greater'), "lss_gc_num_reads value still showing 0")

        # Perform explicit compress operation
        self.perform_plasma_mem_ops("compressAll")
        self.sleep(self.wait_timeout, "waiting for {} secs to complete the compression".format(self.sweep_interval))
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "failing due to items not compressed")

        # perform log cleaning
        self.perform_plasma_mem_ops("logClean")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "lss_gc_num_reads", 0, ops='greater'),
                        "lss_gc_num_reads value still showing 0")

        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field,
                                                         limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    """
            1. Enable compression
                a. indexer.plasma.backIndex.enableInMemoryCompression
                b. indexer.plasma.mainIndex.enableInMemoryCompression
            2. perform full index scan and verify that scan results are expected
            3. Start performing mutations on limited dataset around 20% (hot data) for around more than half an hour
            4. Check num_compressed
            6. Make sure you keep scanning this hot data so that no eviction could happen. 
            7. Check for num_compressed stat should not have changed. Wait for few cycles and validate. 
            8. Make the indexer_memory_quota half to decrease rr
            9. scan hot data range - verify that cache hit ratio is 1.0 and compress hit ratio is 0.0
            10. scan compressed range - verify that cache hit ratio is 1.0 and compress hit ratio is 1.0
            11. scan cold data range - verify that cache hit ratio is 0.0 and compress hit ratio is 0.0
    """

    def test_Swapper_with_reducing_quota(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        while not self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", self.resident_ratio, ops='equalOrLessThan', timeout=5):
            self.create_start = self.create_end
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.debug("Added items from {} to {}".format(self.create_start, self.create_end))

        self.validate_plasma_stat_field_value(stat_obj_list, "cache_hit_ratio", 1.00, ops='equal')
        field = 'mod_body'
        new_indexMap, indexTaskList = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                   replica=self.index_replicas,
                                                                                   defer=False,
                                                                                   number_of_indexes_per_coll=2 * self.index_count,
                                                                                   count=self.index_count,
                                                                                   field=field, sync=False,
                                                                                   timeout=self.wait_timeout)

        for taskInstance in indexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)
        start = time.time()
        self.gen_create = None
        self.upsert_start = 0
        self.upsert_end = self.create_end / 5
        self.generate_subDocs(sub_doc_ops="upsert")
        self.different_field = True
        data_load_task = self.data_load()
        self.log.debug("update from {} to {}".format(self.upsert_start, self.upsert_end))
        self.wait_for_doc_load_completion(data_load_task)
        compressed_items_dict = self.get_plasma_index_stat_value("num_rec_compressed", stat_obj_list)
        totalCount = self.upsert_end - self.upsert_start
        self.compare_plasma_stat_field_value(stat_obj_list, "num_rec_compressed", compressed_items_dict,
                                                             ops='equalOrLessThan')
        index_mem_quota = self.indexUtil.get_indexer_mem_quota(self.cluster.index_nodes[0])
        self.log.debug("Index memory quota is {}".format(index_mem_quota))
        self.indexer_client = RestConnection(self.cluster.index_nodes[0])
        self.indexer_client.set_service_mem_quota(
            {CbServer.Settings.INDEX_MEM_QUOTA: index_mem_quota / 2})
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='mod_body', totalCount=totalCount, limit=totalCount)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        index_list = self.extract_index_list(new_indexMap)
        self.validate_plasma_stat_index_specific(stat_obj_list, "cache_hit_ratio", index_list, 1.00, ops='lesser')


    """
         1. Create indexer with num_replica as 2 
         2. Perform delete operation.
         3. Wait for sleep interval
         4. Verify using stat - 'merges' > 0, as pages should merge after delete 
         5. Check for  memory_used_storage and memory_size stat in /storage. It should be < 50
         6. Check for num_compressed count as 0.
         7. Perform CRUD operation with 20% delete,  1000% add, 20% update ( Raise from 50K items to 5M) 
         8. Check for num_compressed count > 1
         9. Perform swapout page till rr = 0. Use endpoint <index-node>:9102/plasmaDiag.
         10. Perform full-scan. rr should back 1. Add more documents 
         11. Check for split pages records. It should > 1.
         12. Perform delete operations once again.
         13. Check for merges stat. The count should be more than step 5.
         14. Perform full scan and validate the result           
    """

    def test_Swapper_in_out_with_CRUD(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.delete_by_query = self.input.param("delete_by_query", True)

        if self.delete_by_query:
            self.indexUtil.delete_docs_with_field(self.cluster, indexMap)
            self.log.debug("After delete")
        else:
            self.delete_start = 0
            self.delete_perc = self.input.param("delete_perc", 100)
            self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
            self.log.info("Delete items count is:" + str(self.delete_end))
            self.gen_create = None
            self.generate_docs(doc_ops="delete")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
        self.sleep(self.wait_timeout, "Waiting for merges to settle down")
        start = self.create_end
        self.gen_delete = None
        while not self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", self.resident_ratio, ops='equalOrLessThan', timeout=5):
            self.create_start = self.create_end
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.debug("Added items from {} to {}".format(self.create_start, self.create_end))
        self.validate_plasma_stat_field_value(stat_obj_list, "merges", 0, ops='greater')
        merges_dict = self.get_plasma_index_stat_value("merges", stat_obj_list)
        end = self.create_end
        self.gen_create = None
        self.create_start = self.create_end
        self.create_end = self.create_start + self.items_add
        self.delete_start = start
        self.delete_end = start + ((end - start) * .2)
        self.update_start = self.delete_end
        self.update_end = self.delete_end + ((end - start) * .4)
        self.generate_docs(doc_ops="create:update:delete")
        totalCount = self.create_end - self.delete_end
        self.log.debug(
            "Delete start: {} Delete end: {} create start {} create end {} update start {} update end {}".format(
                self.delete_start, self.delete_end, self.create_start, self.create_end, self.update_start,
                self.update_end))
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.sleep(2 * self.sweep_interval, "Waiting for compression to complete")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "Compression not triggered")
        # Perform explicit swapout operation
        stat_obj_list = self.create_Stats_Obj_list()
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "lss_fragmentation")
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "purges")
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "inserts")
        self.perform_plasma_mem_ops("evictAll")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 0, ops='equal'),
                        "Resident ratio not getting back to 0 after swap-out")
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body',totalCount=totalCount, limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.assertTrue(
            self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1, ops='equal', timeout=100),
            "Resident ratio not getting back to 1 after swap-in")
        self.gen_create = None
        self.gen_update = None
        if self.delete_by_query:
            self.indexUtil.delete_docs_with_field(self.cluster, indexMap)
            self.log.debug("After delete")
        else:
            self.delete_start = self.delete_end
            self.delete_perc = self.input.param("delete_perc", 100)
            self.delete_end = totalCount
            self.log.info("Delete items count is:" + str(self.delete_end))
            self.generate_docs(doc_ops="delete")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
        self.sleep(self.wait_timeout, "Waiting for merges to settle down")
        self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "merges", merges_dict,
                                                             ops='greater'))

    """
        1. Set indexer.settings.compaction.plasma.manual to true 
        2. Trigger compression and validate num_compressed_pages > 0
        3. Trigger MVCC Purger - verify using stas - 'purges', 'mvcc_purge_ratio' and 'num_compressed_pages'
        4. Perform full index scan and verify that scan results are expected
    """

    def test_MVCC_purger(self):
        self.compact_ratio = self.input.param("compact_ratio", 1.0)
        self.set_index_settings({"indexer.plasma.purger.compactRatio": self.compact_ratio},
                                self.cluster.index_nodes[0])

        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.sleep(2 * self.sweep_interval, "waiting for {} secs to trigger the compression".format(self.wait_timeout))

        # Perform explicit compress operation
        self.perform_plasma_mem_ops("compressAll")
        self.sleep(self.wait_timeout, "waiting for {} secs to complete the compression".format(self.wait_timeout))
        for count in range(self.counter):
            if self.verify_compression_stat(self.cluster.index_nodes):
                break
            else:
                self.sleep(self.counter, "waiting for compression to complete")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes), "failing due to items not compressed")

        # Perform explicit compress operation
        stat_obj_list = self.create_Stats_Obj_list()
        self.perform_plasma_mem_ops("purge")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "num_rec_compressed", 0, ops='equal'),
                        "Compressed item is not showing 0")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "purges", 0, ops='greater'),
                        "purges value still showing 0")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "mvcc_purge_ratio", 1.0, ops='equal'),
                        "mvcc_purging_ratio not showing 1.0")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.init_items_per_collection, field,
                                                         limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)
    """
        1. Enable compression
            a. indexer.plasma.backIndex.enableInMemoryCompression
            b. indexer.plasma.mainIndex.enableInMemoryCompression
        2. Set sweep interval  
        3. Restart the indexer service
        4. Add documents so that memory_used_storage >50
        5. Wait for compression to complete
        6. verify using stat - 'num_compressed_pages' > 0
    """

    def test_compression_with_index_restart(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        for node in self.cluster.index_nodes:
            self.kill_indexer(node, 1)

        self.log.info("Wait for indexer service to up")
        self.sleep(self.wait_timeout, "Waiting for indexer service to up")
        self.time_out = self.input.param("time_out", 10)
        self.indexer_rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        self.wait_for_indexer_service_to_Active(self.indexer_rest, self.cluster.index_nodes, time_out=self.time_out)
        start = self.init_items_per_collection
        i = 0
        self.mem_used = self.input.param("mem_used", 10)
        self.perform_scan = self.input.param("perform_scan", False)
        self.time_out = self.input.param("time_out", 60)
        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start = start + (i * self.items_add)
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            self.data_load()
            i += 1

        if self.perform_scan:
            query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body',
                                                            totalCount=self.init_items_per_collection,
                                                            limit=self.query_limit)
            for taskInstance in query_tasks_info:
                self.task.jython_task_manager.get_task_result(taskInstance)
        else:
            self.perform_plasma_mem_ops("swapinAll")
            self.perform_plasma_mem_ops("persistAll")
        self.sleep(2 * self.sweep_interval, "waiting for compression to happen")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes),"valid compressed items are coming for both MainStore and Backstore")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.create_end, field, limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    """
            1. Enable compression
                a. indexer.plasma.backIndex.enableInMemoryCompression
                b. indexer.plasma.mainIndex.enableInMemoryCompression
            2. Set sweep internal  
            3. Create index with num_replica as 1
            4. Add documents so that memory_used_storage >50
            5. Wait for compression to complete
            6. verify using stat - 'num_compressed_pages' > 0
            7. Fail-over 1 index node.
            8. Rebalance-in 1 index node
            9. Create more index with num_replica as 0 against newly added node
            10. Wait for compression to complete in newly added index node
            11. verify using stat - 'num_compressed_pages' > 0 for newly added index node
    """

    def test_compression_with_rebalance(self):
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        for node in self.cluster.index_nodes:
            self.kill_indexer(node, 1)

        self.log.info("Starting rebalance in and rebalance out task")
        self.nodes_in = self.input.param("nodes_in", 1)
        count = len(self.dcp_services) + self.nodes_init
        self.log.debug("count value is {}".format(count))
        nodes_in = self.cluster.servers[count:count + self.nodes_in]
        services = ["index"]
        rebalance_in_task_result = self.task.rebalance([self.cluster.master],
                                                       nodes_in,
                                                       [],
                                                       services=services)
        indexer_nodes_list = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                           get_all_nodes=True)
        self.assertTrue(rebalance_in_task_result, "Rebalance in task failed")
        self.log.info("Rebalance in task completed, starting Rebalance-out task")
        self.num_failed_nodes = self.input.param("num_failed_nodes", 1)
        self.nodes_out = indexer_nodes_list[:self.num_failed_nodes]
        rebalance_out_task_result = self.task.rebalance([self.cluster.master],
                                                        [],
                                                        to_remove=self.nodes_out)
        self.assertTrue(rebalance_out_task_result, "Rebalance out task failed")

        start = self.init_items_per_collection
        i = 0
        self.mem_used = self.input.param("mem_used", 10)
        self.time_out = self.input.param("time_out", 60)
        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                           get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                           get_all_nodes=True)
        while (self.mem_used_reached(self.mem_used, self.create_Stats_Obj_list())):
            self.create_start = start + (i * self.items_add)
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            self.log.debug("initial_items_in_each_collection {}".format(self.init_items_per_collection))
            self.data_load()
            i += 1

        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body',
                                                        totalCount=self.init_items_per_collection,
                                                        limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.sleep(2 * self.sweep_interval, "waiting for compression to happen")

        stat_obj_list = self.create_Stats_Obj_list()
        self.assertTrue(self.compare_aggregate_stat(stat_obj_list, 'num_rec_compressed', self.exp_compression_count, ops='greater',
                                        retry=10, sleep=20),
            "Compression count is not as expected")
        self.assertTrue(self.verify_compression_stat(self.cluster.index_nodes),
                        "Compressed items count is either 0 or less than 0")
        mem_comp_compare_task = self.validate_index_data(indexMap, self.create_end, field, limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    """
    1. Enable compression
        a. indexer.plasma.backIndex.enableInMemoryCompression
        b. indexer.plasma.mainIndex.enableInMemoryCompression
    2. Create multiple indexes specific to different index nodes ( total = 3 index nodes)
    3. Add document related to 2 indexes so that it have impact only 2 index nodes
    4. Flush all pages. Use below endpoint: curl <index-node>:9102/plasmaDiag
    5. Check for  memory_used_storage and memory_size stat in /storage > 50 for only 2 index nodes but for the 1 index node memory_used_storage should be < 50
    4. Set sweep internal  
    5. Wait for compression to take place
    6. Validate Compress pages - 
        a. verify using stat - 'num_compressed_pages'. 
    7. Perform full index scan and verify that scan results are expected.
    8. verify using stat - 'num_compressed_pages'. It should come down as full scan decompressed the pages
    """

    def test_compress_with_different_indexes(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        while not self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", self.resident_ratio,
                                                        ops='equalOrLessThan', timeout=5):
            self.create_start = self.create_end
            self.create_end = self.create_start + self.items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.debug("Added items from {} to {}".format(self.create_start, self.create_end))

        field = 'mod_body'
        new_indexMap, indexTaskList = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                   replica=self.index_replicas,
                                                                                   defer=False,
                                                                                   number_of_indexes_per_coll=2 * self.index_count,
                                                                                   count=self.index_count,
                                                                                   field=field, sync=False,
                                                                                   timeout=self.wait_timeout)

        for taskInstance in indexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.gen_create = None
        self.upsert_start = 0
        self.upsert_end = self.create_end / 5
        self.generate_subDocs(sub_doc_ops="upsert")
        self.different_field = True
        data_load_task = self.data_load()
        self.log.debug("update from {} to {}".format(self.upsert_start, self.upsert_end))
        self.wait_for_doc_load_completion(data_load_task)
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "lss_fragmentation")
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "purges")
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "inserts")
        self.sleep(2 * self.sweep_interval, "waiting for compression to complete")
        self.assertTrue(self.compare_aggregate_stat(stat_obj_list, 'num_rec_compressed', self.exp_compression_count, ops='greater', retry=10, sleep=20),
                        "Compression count is not as expected")
        compressed_items_dict = self.get_plasma_index_stat_value("num_rec_compressed", stat_obj_list)

        new_Count = self.upsert_end - self.upsert_start
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, new_indexMap, key='mod_body',
                                                        totalCount=new_Count,
                                                        limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        totalCount = self.create_end - self.update_end
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body',
                                                        totalCount=totalCount,
                                                        limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        self.compare_plasma_stat_field_value(stat_obj_list, "num_rec_compressed", compressed_items_dict,
                                             ops='equalOrLessThan')
        mem_comp_compare_task = self.validate_index_data(indexMap, totalCount, 'body',
                                                         limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)
        mem_comp_compare_task = self.validate_index_data(new_indexMap, new_Count, 'mod_body',
                                                         limit=self.query_limit)
        for taskInstance in mem_comp_compare_task:
            self.task.jython_task_manager.get_task_result(taskInstance)

    """
        1. Create indexer with num_replica as 2 
        2. Perform delete operation. 
        3. Wait for sleep interval
        4. Verify using stat - 'merges' > 0, as pages should merge after delete 
        5. Perform CRUD operation with 20% delete,  1000% add, 20% update ( Raise from 50K items to 5M) 
        6. Check for num_compressed count > 1
        7. Perform swapout page till rr = 0. Use endpoint <index-node>:9102/plasmaDiag.
        8. Perform full-scan. rr should back 1
        9. Add more documents 
        10. Check for split pages records. It should > 1.
        11. Perform delete operations once again.
        12. Check for merges stat. The count should be more than step 5.
        13. Perform full scan and validate the result
    """

    def test_Swapper_with_CRUD(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.delete_by_query = self.input.param("delete_by_query", True)

        if self.delete_by_query:
            self.indexUtil.delete_docs_with_field(self.cluster, indexMap)
            self.log.debug("After delete")
        else:
            self.delete_start = 0
            self.delete_perc = self.input.param("delete_perc", 100)
            self.delete_end = int(self.delete_perc * .01 * self.init_items_per_collection)
            self.log.info("Delete items count is:" + str(self.delete_end))
            self.gen_create = None
            self.generate_docs(doc_ops="delete")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.gen_delete = None
        self.sleep(self.wait_timeout, "Waiting for merges to settle down")
        start = self.create_end
        self.gen_delete = None

        self.create_start = start
        self.create_end = self.create_start + self.items_add
        self.generate_docs(doc_ops="create")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.log.info("items loaded from {} to {}".format(self.create_start, self.create_end))
        self.wait_for_mutuations_to_settle_down(stat_obj_list)
        self.sleep(self.wait_timeout, "Wait for merges to complete")
        self.exp_merge_count= self.input.param("exp_merge_count", 100)
        self.assertTrue(
            self.compare_aggregate_stat(stat_obj_list, 'merges', self.exp_merge_count, ops='greater',
                                        retry=10, sleep=20),
            "Merging count is not as expected")
        merges_dict = self.get_plasma_index_stat_value("merges", stat_obj_list)

        end = self.create_end
        self.gen_create = None
        self.create_start = self.create_end
        self.create_end = int (self.create_start + self.items_add)
        self.delete_start = start
        self.delete_end = int (start + ((end - start) * .2))
        self.update_start = self.delete_end
        self.update_end = int(self.delete_end + ((end - start) * .4))
        self.generate_docs(doc_ops="create:update:delete")
        totalCount = self.create_end - self.delete_end
        self.log.info(
            "Delete start: {} Delete end: {} create start {} create end {} update start {} update end {}".format(
                self.delete_start, self.delete_end, self.create_start, self.create_end, self.update_start,
                self.update_end))
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.wait_for_mutuations_to_settle_down(stat_obj_list)

        self.sleep(2 * self.sweep_interval, "Waiting for compression to complete")
        self.assertTrue(
            self.compare_aggregate_stat(stat_obj_list, 'num_rec_compressed', self.exp_compression_count, ops='greater',
                                        retry=10, sleep=20),
            "Compression count is not as expected")
        # Perform explicit swapout operation
        self.sleep(self.wait_timeout, "waiting before triggering evictAll")
        self.perform_plasma_mem_ops("evictAll")
        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 0, ops='equal'),
                        "Resident ratio not getting back to 0 after swap-out")

        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body', totalCount=totalCount,
                                                        limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.assertTrue(
            self.validate_plasma_stat_field_value(stat_obj_list, "resident_ratio", 1, ops='equal', timeout=100),
            "Resident ratio not getting back to 1 after swap-in")

        self.assertTrue(self.validate_plasma_stat_field_value(stat_obj_list, "splits", 0, ops='greater'), "splits value is less than 0")

        self.gen_create = None
        self.gen_update = None
        self.indexUtil.delete_docs_with_field(self.cluster, indexMap)
        self.log.debug("After delete")
        self.sleep(self.wait_timeout, "Waiting for merges to settle down")
        self.assertTrue(
            self.compare_aggregate_stat(stat_obj_list, 'merges', self.exp_compression_count, ops='greater',
                                        retry=10, sleep=20),
            "Merging count is not as expected")

    """
            1. Create indexer with num_replica as 2 
            2. Perform delete operation. 
            3. Wait for sleep interval
            4. Verify using stat - 'merges' > 0, as pages should merge after delete 
            5. Perform CRUD operation with 20% delete,  1000% add, 20% update ( Raise from 50K items to 5M) 
            6. Check for num_compressed count > 1
            7. Perform swapout page till rr = 0. Use endpoint <index-node>:9102/plasmaDiag.
            8. Perform full-scan. rr should back 1
            9. Add more documents 
            10. Check for split pages records. It should > 1.
            11. Perform delete operations once again.
            12. Check for merges stat. The count should be more than step 5.
            13. Perform full scan and validate the result
        """

    def test_Compression_with_TTL(self):
        field = 'body'
        stat_obj_list = self.create_Stats_Obj_list()
        self.timer = self.input.param("timer", 600)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False,
                                                                                     timeout=self.wait_timeout)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.timer = self.input.param("maxttl", 1000)
        self.gen_create = None
        self.expiry_start = self.create_start
        self.maxttl = 1000
        self.expiry_end = self.create_end
        self.generate_docs(doc_ops="expiry")
        data_load_task = self.data_load()
        self.wait_for_doc_load_completion(data_load_task)
        self.log.info("TTL experiment done")
        self.wait_for_mutuations_to_settle_down(stat_obj_list)
        data_dict = self.get_plasma_index_stat_value("lss_data_size", stat_obj_list)
        disk_dict = self.get_plasma_index_stat_value("lss_disk_size", stat_obj_list)
        used_space_dict = self.get_plasma_index_stat_value("lss_used_space", stat_obj_list)
        self.sleep(1000, "Waiting for items to delete")
        timeout = 1000
        start_time = time.time()
        stop_time = start_time + timeout

        while not self.verify_bucket_count_with_index_count(indexMap, 0, field) and time.time() > stop_time:
            self.sleep(100, "wait for items to get delete")

        self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_data_size", data_dict,
                                            ops='lesser'),"data size is not going down")
        self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_disk_size", disk_dict,
                                             ops='lesser'),"disk size is not coming down")
        self.assertTrue(self.compare_plasma_stat_field_value(stat_obj_list, "lss_used_space", used_space_dict,
                                             ops='lesser'),"used space is not coming down")
