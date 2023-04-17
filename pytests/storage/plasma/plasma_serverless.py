import copy
import threading
import urllib
import time

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from Jython_tasks.task_manager import TaskManager
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest
from storage.plasma.plasma_base import PlasmaBaseTest


class PlasmaServerless(PlasmaBaseTest, ServerlessOnPremBaseTest):
    def setUp(self):
        super(PlasmaServerless, self).setUp()
        self.b_create_endpoint = "pools/default/buckets"
        self.discretionaryQuotaThreshold = self.input.param("discretionaryQuotaThreshold", 0.0)
        with_default_bucket = self.input.param("with_default_bucket", False)
        self.enableInMemoryCompression = self.input.param("enableInMemoryCompression", True)
        if with_default_bucket:
            old_weight = self.bucket_weight
            self.bucket_weight = 1
            self.create_bucket(self.cluster)
            self.bucket_weight = old_weight
        self.bucket_util.print_bucket_stats(self.cluster)
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        index_node = self.cluster.index_nodes[0]
        if self.in_mem_comp:
            self.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.plasma.backIndex.enableCompressDuringBurst": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.plasma.mainIndex.enableCompressDuringBurst": self.enableInMemoryCompression}, index_node)
            self.set_index_settings({"indexer.settings.compaction.plasma.manual": self.manual},
                                    index_node)
            self.set_index_settings({"indexer.plasma.purger.enabled": self.purger_enabled},
                                    index_node)
            self.set_index_settings({"indexer.plasma.serverless.discretionaryQuotaThreshold": self.discretionaryQuotaThreshold},
                                    index_node)
        self.stat_field = 'resident_ratio'
        self.stat_obj_list = self.create_Stats_Obj_list()
        self.indexer_client = RestConnection(self.cluster.index_nodes[0])
        self.index_mem_quota = self.input.param("index_mem_quota", 1024)
        self.indexer_client.set_service_mem_quota(
            {CbServer.Settings.INDEX_MEM_QUOTA: self.index_mem_quota})
        self.append_items = self.input.param("append_items", 40000)
        self.timeout = self.input.param("timeout", 1000)
        self.sync_create_index = self.input.param("sync_create_index", False)
        self.retry = self.input.param("retry", 3)
        self.scan_in_sync = self.input.param("scan_in_sync", True)
        self.scan_item_count = self.input.param("scan_item_count", 20000)

        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                    replica=self.index_replicas,
                                                                                    defer=True,
                                                                                    number_of_indexes_per_coll=self.index_count,
                                                                                    field=field,
                                                                                    sync=self.sync_create_index,
                                                                                    retry=self.retry)
        self.index_map = indexMap
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.indexUtil.build_deferred_indexes(self.cluster, indexMap)
        self.assertTrue(self.polling_for_All_Indexer_to_Ready(indexMap, timeout=self.timeout),
                        "polling for deferred indexes failed")
        self.wait_for_stats_to_settle_down(self.stat_obj_list, "equal", "lss_fragmentation")
        self.bucket_util.print_bucket_stats(self.cluster)


    def __get_bucket_params(self, b_name, ram_quota=256, width=1, weight=1):
        self.log.debug("Creating bucket param")
        return {
            Bucket.name: b_name,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: ram_quota,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.width: width,
            Bucket.weight: weight
        }


    def print_stats(self, stats_map):
        total_tenants_quota = 0
        for node in stats_map:
            self.log.info(node)
            for buck in stats_map[node]:
                self.log.info(buck.upper())
                total_tenants_quota += stats_map[node][buck]['quota']
                self.log.info("QUOTA {}".format(stats_map[node][buck]['quota']))
                self.log.info("MANDATORY QUOTA {}".format(stats_map[node][buck]['mandatory_quota']))
                self.log.info("PRESSURE {}".format(stats_map[node][buck]['quota'] - stats_map[node][buck]['mandatory_quota'] < 0))
                self.log.info("RR {}".format(stats_map[node][buck]['resident_ratio']))
        self.log.info("TOTAL TENANT ACTIVE QUOTA: {}".format(total_tenants_quota)) 


    def create_workload(self, start, bucket_list):
        end = start + self.append_items
        load_gen = doc_generator(self.key, start, end)
        start = end
        self.load_to_bucket_list(bucket_list, load_gen)
        self.bucket_util.print_bucket_stats(self.cluster)
        return start


    def total_tenants_quota(self, bucket_list=None):
        if bucket_list == None:
            bucket_list = self.cluster.buckets
        total_tenants_quota = 0
        bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=bucket_list)
        for node in bucket_quota_map:
            for buck in bucket_quota_map[node]:
                total_tenants_quota += bucket_quota_map[node][buck]['quota']
        self.log.info("Total tenant active quota: {}".format(total_tenants_quota)) 
        return bucket_quota_map, total_tenants_quota


    def full_scan(self, bucket_list, totalCount, offset, numThreadsPerNode=2, lightLoad=True):
        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, self.index_map, key='body',
                                                        bucket_list=bucket_list,
                                                        totalCount=totalCount,
                                                        limit=self.query_limit, is_sync=self.scan_in_sync, 
                                                        offSetBound=offset, numThreadsPerNode=numThreadsPerNode,
                                                        lightLoad=lightLoad)
        for taskInstance in query_tasks_info:
            self.log.debug("Checking for full scan status")
            self.task.jython_task_manager.get_task_result(taskInstance)


    def test_quota_extended_to_under_allocated_tenants(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Continue the creates on half the tenants while keeping other tenants idle for 15 mins
        6. Capture quota stats again. 
        7. Expect increase in quota of busy tenants and decrese in quota of light tenants.
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        self.append_items = self.input.param("append_items", 40000)
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota and pressure
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])

            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test with higher workload.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        # for the next 15 mins, keep the even tenants idle and odd tenants occupied
        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))
            # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)
            self.log.debug("Perform load {} times ".format(counter))
            counter+=1
           
        final_even_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, even_bucket_list)
        final_odd_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)
    
        self.log.debug("Expecting dip in quota")
        res1 = self.compare_RR_for_nodes(final_even_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='equalsOrLesserThan')
        self.log.debug("Expecting increase in quota")
        res2 = self.compare_RR_for_nodes(final_odd_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='equalsOrGreaterThan')
        self.assertTrue(res1, "Dip failed")
        self.assertTrue(res2, "Raise failed")


    def test_no_impact_between_tenants(self):
        """
        1. Load data into buckets and create indexes
        2. Load data till all indexes are around 10% RR
        3. Perform full scans for half an hour
        4. Ensure that there is not much significant change in quotas
        """
        counter = 1
        start = self.init_items_per_collection
        rr_map = [1]

        def rr_within_range(rr_map, threshold):
            for i in rr_map:
                if i > threshold + 0.03:
                    return False 
            return True

        # Load data till all buckets are within 10%-13% RR
        while not rr_within_range(rr_map, 0.1) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            counter += 1
            del rr_map[:]

           # loads for all buckets
            start = self.create_workload(start, self.cluster.buckets)

            # check rr
            bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)
            for node in bucket_quota_map:
                for buck in bucket_quota_map[node]:
                    rr_map.append(bucket_quota_map[node][buck]['resident_ratio'])

            self.bucket_util.print_bucket_stats(self.cluster)


        # START FULL SCANS FOR HALF AN HOUR
        totalCount = self.scan_item_count
        offset=0
        init_bucket_quota_map = {}
        end_time = time.time() + self.wait_timeout
        half_time = time.time() + (self.wait_timeout // 2)
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        counter = 1
        flag = True
        while time.time() < end_time:            
            self.full_scan(self.cluster.buckets, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            if time.time() >= half_time and flag:
                init_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)
                flag = False
                self.log.info("Taking initial stats...")
            counter+=1

            self.print_stats(self.create_nodes_tenant_stat_map(self.cluster.index_nodes))

        final_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes)

        result = self.compare_RR_for_nodes(final_bucket_quota_map, init_bucket_quota_map, field='quota', comparisonType='percent', threshold=5)
        self.assertTrue(result, "Quotas changing more than 5%")
       

    def test_quota_given_to_active_tenant(self):
        """
        1. Create buckets, load data and create indexes.
        2. Keep loading data for 1 tenant while performing scans on others
        3. Ensure that the quota allotted to the heavy tenant goes up and the total quota allotted goes up
        """
        init_bucket_quota_map = {}
        init_total_quota = 0

        total_tenants_quota = 0
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        totalCount = self.scan_item_count
        offset = 0
        self.append_items = self.input.param("append_items", 40000)
        start = self.init_items_per_collection
        heavy_tenant = self.cluster.buckets[0].name
        self.log.info("Heavy tenant:", heavy_tenant)
        counter = 0
        end_time = time.time() + self.wait_timeout
        half_time = time.time() + (self.wait_timeout//2)
        half_time_flag = True
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
    
        while total_tenants_quota < plasma_quota and time.time() < end_time:
            counter += 1
            print("Loop number ", counter)
            # full scans for all except 1 bucket
            self.full_scan(self.cluster.buckets[1:], totalCount, offset)

            # loads for 1 bucket
            bucket_list = [self.cluster.buckets[0]]
            start = self.create_workload(start, bucket_list)

            # calculate total active quota
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            self.print_stats(inter_bucket_quota_map)
            self.bucket_util.print_bucket_stats(self.cluster)

            if half_time_flag and time.time() >= half_time:
                init_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=self.cluster.buckets)
                init_total_quota = total_tenants_quota
                self.log.info("Initial map created")
                half_time_flag = False

        if total_tenants_quota >= plasma_quota:
            self.log.error("No unused memory. Re-run test with lesser workload.")
            self.assertTrue(False, "No unused memory. Re-run test with lesser workload.")
        final_bucket_heavy_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, buckets=[self.cluster.buckets[0]])

        self.log.info("INITIAL TOTAL TENANTS QUOTA: {}".format(init_total_quota))
        self.log.info("FINAL TOTAL TENANTS QUOTA: {}".format(total_tenants_quota))

        res = self.compare_RR_for_nodes(final_bucket_heavy_quota_map, init_bucket_quota_map, field='quota', comparisonType='normal', ops='greater')
        self.assertTrue(res, "Quota did not increase for tenant 1")
        self.assertTrue(total_tenants_quota >= init_total_quota, "Total quota has not increased")


    def test_quota_increase_on_index_drop(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Keep doing creates on half the tenants while doing scans on the other half till tenants under pressure and no unused memory left.
        4. Capture quota stats
        5. Drop some indexes from the light tenants
        5. Repeat step 3 for 15 mins
        6. Capture quota stats again. 
        7. Expect increase in quota of heavy tenants.
        """
        odd_bucket_list = self.filter_buckets(self.cluster.buckets, filter='odd')
        even_bucket_list = self.filter_buckets(self.cluster.buckets, filter='even')
        totalCount = self.scan_item_count
        counter = 0
        self.append_items = self.input.param("append_items", 40000)
        offset = 0
        total_tenants_quota = 0
        start = self.init_items_per_collection

        # till unused quota is used up and all odd tenants go under pressure, keep pushing data into odd tenants while keeping the even tenants active
        inter_bucket_quota_map = {}
        pressure = [1]
        plasma_quota = 0.89 * self.index_mem_quota * len(self.cluster.index_nodes) * 1024 * 1024
        while (total_tenants_quota < plasma_quota or max(pressure)>=0) and counter < 100:
            self.log.info("LOOP NUMBER: {}".format(counter))
            del pressure[:]
            counter += 1

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))
            
           # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)

            # calculate total active quota
            inter_bucket_quota_map, total_tenants_quota = self.total_tenants_quota()
            for node in inter_bucket_quota_map:
                for buck in odd_bucket_list:
                    pressure.append(inter_bucket_quota_map[node][buck.name]['quota'] - inter_bucket_quota_map[node][buck.name]['mandatory_quota'])

            self.bucket_util.print_bucket_stats(self.cluster)

        if counter == 100:
            self.log.error("Loopbreaker activated. Rerun the test.")
        self.log.info("No unused quota left and odd tenants under pressure.")

        # Drop some indexes from all even buckets
        newIndexMap = {}
        for bucket in even_bucket_list:
            scope_dict = self.index_map[bucket.name]
            newIndexMap[bucket.name] = dict(list(scope_dict.items())[len(scope_dict)//2:])
        dropIndexTaskList, indexDict = self.indexUtil.async_drop_indexes(self.cluster, newIndexMap,
                                                                         buckets=even_bucket_list, 
                                                                         drop_only_given_indexes=True)

        for taskInstance in dropIndexTaskList:
            self.task.jython_task_manager.get_task_result(taskInstance)

        # for the next 15 mins, scan even tenants and keep odd tenants occupied
        counter = 1
        end_time = time.time() + self.wait_timeout
        self.log.debug("current time is {} end time is {}".format(time.time(), end_time))
        while time.time() < end_time:
            self.log.info("LOOP NUMBER: {}".format(counter))

            # full scans for even buckets
            self.full_scan(even_bucket_list, totalCount, offset)
            self.log.debug("Perform full scan {} times ".format(counter))

            # loads for odd buckets
            start = self.create_workload(start, odd_bucket_list)
            self.log.debug("Perform load {} times ".format(counter))
            counter+=1
            self.bucket_util.print_bucket_stats(self.cluster)
           
        final_odd_bucket_quota_map = self.create_nodes_tenant_stat_map(self.cluster.index_nodes, odd_bucket_list)
    
        self.log.debug("Expecting increase in quota")
        res = self.compare_RR_for_nodes(final_odd_bucket_quota_map, inter_bucket_quota_map, comparisonType='normal', field='quota', ops='greater')
        self.assertTrue(res, "Raise failed")
