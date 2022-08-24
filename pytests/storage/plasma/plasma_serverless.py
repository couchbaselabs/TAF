import copy
import urllib

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from Jython_tasks.task_manager import TaskManager
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
from serverless.serverless_onprem_basetest import ServerlessOnPremBaseTest
from storage.plasma.plasma_base import PlasmaBaseTest


class PlasmaServerless(PlasmaBaseTest,ServerlessOnPremBaseTest):
    def setUp(self):
        super(PlasmaServerless, self).setUp()
        self.b_create_endpoint = "pools/default/buckets"

        with_default_bucket = self.input.param("with_default_bucket", False)
        if with_default_bucket:
            old_weight = self.bucket_weight
            self.bucket_weight = 1
            self.create_bucket(self.cluster)
            self.bucket_weight = old_weight
        self.bucket_util.print_bucket_stats(self.cluster)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                 get_all_nodes=True)
        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                                 get_all_nodes=True)


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

    def test_no_impact_between_tenants(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Store RR for each index
        4. Perform full scan against all indexes
        5. Perform some full scan and perform mutations
        6. Make sure RR should not impact across indexes
        """
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.log.debug("Getting initial RR values")
        stat_obj_list = self.create_Stats_Obj_list()
        stat_field = 'resident_ratio'
        initial_rr_value_map = self.get_plasma_index_stat_value(stat_field, stat_obj_list)

        bucket_list = []
        bucket_list.append(self.cluster.buckets[0])

        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body', totalCount=self.init_items_per_collection,
                                                        limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        newIndexMap, newcreateIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,buckets=bucket_list,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=2 * self.index_count,
                                                                                     count=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in newcreateIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        final_rr_value_map = self.get_plasma_index_stat_value(stat_field, stat_obj_list)
        self.compareRR(initial_rr_value_map, final_rr_value_map)


    def test_no_impact_between_tenants(self):
        """
        1. Create multiple buckets and validate the bucket distribution
        2. Create multiple indexes
        3. Store RR for each index
        4. Perform full scan against all indexes
        5. Perform some full scan and perform mutations
        6. Make sure RR should not impact across indexes
        """
        field = 'body'
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        self.log.debug("Getting initial RR values")
        stat_obj_list = self.create_Stats_Obj_list()
        stat_field = 'resident_ratio'
        initial_rr_value_map = self.get_plasma_index_stat_value(stat_field, stat_obj_list)

        bucket_list = []
        bucket_list.append(self.cluster.buckets[0])

        query_tasks_info = self.indexUtil.run_full_scan(self.cluster, indexMap, key='body', totalCount=self.init_items_per_collection,
                                                        limit=self.query_limit)
        for taskInstance in query_tasks_info:
            self.task.jython_task_manager.get_task_result(taskInstance)

        newIndexMap, newcreateIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,buckets=bucket_list,
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=2 * self.index_count,
                                                                                     count=self.index_count,
                                                                                     field=field, sync=False)
        for taskInstance in newcreateIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        final_rr_value_map = self.get_plasma_index_stat_value(stat_field, stat_obj_list)
        self.compareRR(initial_rr_value_map, final_rr_value_map)
