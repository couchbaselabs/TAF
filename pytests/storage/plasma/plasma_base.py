import json
from decimal import Decimal

from Cb_constants import constants, CbServer, DocLoading
from index_utils.index_ready_functions import IndexUtils
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
import math

from index_utils.plasma_stats_util import PlasmaStatsUtil
from storage.storage_base import StorageBase
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from gsiLib.gsiHelper import GsiHelper

class PlasmaBaseTest(StorageBase):
    def setUp(self):
        super(PlasmaBaseTest, self).setUp()
        self.index_replicas = self.input.param("index_replicas", 0)
        self.retry_exceptions = list([SDKException.AmbiguousTimeoutException,
                                      SDKException.DurabilityImpossibleException,
                                      SDKException.DurabilityAmbiguousException])
        max_clients = min(self.task_manager.number_of_threads, 20)
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.moi_snapshot_interval = self.input.param("moi_snapshot_interval", 120)
        self.manual = self.input.param("manual", False)
        self.purger_enabled = self.input.param("purger_enabled", True)
        self.init_sdk_pool_object()
        self.log.info("Creating SDK clients for client_pool")
        max_clients = min(self.task_manager.number_of_threads, 20)
        clients_per_bucket = int(math.ceil(max_clients / self.standard_buckets))
        for bucket in self.cluster.buckets:
            self.sdk_client_pool.create_clients(
                bucket, [self.cluster.master],
                clients_per_bucket,
                compression_settings=self.sdk_compression)
        self.initial_load()
        self.indexUtil = IndexUtils(server_task=self.task)
        self.index_port = CbServer.index_port
        self.in_mem_comp = self.input.param("in_mem_comp", None)
        self.sweep_interval = self.input.param("sweep_interval", 120)
        self.index_count = self.input.param("index_count", 1)
        self.counter = self.input.param("counter", 30)
        self.query_limit = self.input.param("query_limit", 20000)
        self.resident_ratio = \
            float(self.input.param("resident_ratio", .99))

    def print_plasma_stats(self, plasmaDict, bucket, indexname):
        bucket_Index_key = bucket.name + ":" + indexname
        if plasmaDict.get(bucket_Index_key + "_memory_size") is None:
            self.log.info("Not finding details for the index")
            return
        self.log.info("================ Plasma logs for index:" + indexname)
        self.log.info("Memory Size:" + str(plasmaDict[bucket_Index_key + "_memory_size"]))
        self.log.info("Fragmentation:" + str(plasmaDict[bucket_Index_key + "_lss_stats"]['lss_fragmentation']))
        self.log.info("lss_User_Space:" + str(plasmaDict[bucket_Index_key + "_lss_stats"]['lss_used_space']))
        self.log.info("lss_Data_Size:" + str(plasmaDict[bucket_Index_key + "_lss_stats"]['lss_data_size']))
        self.log.info("Page_bytes:" + str(plasmaDict[bucket_Index_key + "_page_bytes"]))
        self.log.info("Avg_item_size:" + str(plasmaDict[bucket_Index_key + "_avg_item_size"]))
        self.log.info("Bytes incoming" + str(plasmaDict[bucket_Index_key + "_bytes_incoming"]))

    def isServerListContainsNode(self, serverList, ip):
        for node in serverList:
            if node.ip + ":" + node.port == ip:
                return True
        return False

    def kill_indexer(self, server, timeout=10, kill_sleep_time=20):
        self.stop_killIndexer = False
        counter = 0
        while not self.stop_killIndexer:
            indexerkill_shell = RemoteMachineShellConnection(server)
            counter += 1
            if counter > timeout:
                break
            output, error = indexerkill_shell.kill_indexer()
            # output, error = remote_client.execute_command("pkill -f indexer")
            self.log.info("Output value is:" + str(output))
            self.log.info("Counter value is {0} and max count is {1}".format(str(counter), str(timeout)))
            indexerkill_shell.disconnect()
            self.sleep(kill_sleep_time)
        self.log.info("Kill indexer process for node: {} completed".format(str(server.ip)))

    def polling_for_All_Indexer_to_Ready(self, indexes_to_build, buckets=None, timeout=600, sleep_time=10):
        if buckets is None:
            buckets = self.buckets
        for _, scope_data in indexes_to_build.items():
            for _, collection_data in scope_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        self.assertTrue(
                            self.indexUtil.wait_for_indexes_to_go_online(self.cluster, buckets, gsi_index_name,timeout=timeout, sleep_time=sleep_time),
                            "Index {} is not up".format(gsi_index_name))
        return True

    def wait_for_indexer_service_to_Active(self, indexer_rest, indexer_nodes_list, time_out):
        for node in indexer_nodes_list:
            indexCounter = 0
            while indexCounter < time_out:
                generic_url = "http://%s:%s/"
                ip = node.ip
                port = constants.index_port
                baseURL = generic_url % (ip, port)
                self.log.info("Try to get index from URL as {}".format(baseURL))
                indexCounter += 1
                indexStatMap = indexer_rest.get_index_stats(URL=baseURL)
                self.sleep(1)
                if indexStatMap['indexer_state'] == "Active":
                    self.log.info("Indexer state is Active for node with ip:{}".format(baseURL))
                    break
                else:
                    self.log.info("Indexer state is still {}".format(indexStatMap['indexer_state']))

    def set_index_settings(self, setting_json, index_node):
        plasma_obj = PlasmaStatsUtil(index_node, server_task=self.task)
        api = plasma_obj.get_index_baseURL() + 'settings'
        rest_client = RestConnection(index_node)
        status, content, header = rest_client._http_request(api, 'POST', json.dumps(setting_json))
        if not status:
            raise Exception(content)
        self.log.info("{0} set".format(setting_json))

    def mem_used_reached(self, exp_percent, plasma_obj_dict):
        for plasma_obj in plasma_obj_dict.values():
            index_stat = plasma_obj.get_all_index_stat_map()
            percent = self.find_mem_used_percent(index_stat)
            if (percent > exp_percent):
                return False
        return True

    def get_plasma_index_stat_value(self, plasma_stat_field, plasma_obj_dict):
        field_value_map = dict()
        for plasma_obj in plasma_obj_dict.values():
            index_stat = plasma_obj.get_index_storage_stats()
            for bucket in index_stat.keys():
                for index in index_stat[bucket].keys():
                    index_stat_map = index_stat[bucket][index]
                    if plasma_stat_field in index_stat_map["MainStore"]:
                        field_value_map[index] = index_stat_map["MainStore"][plasma_stat_field]
                    elif plasma_stat_field in index_stat_map['MainStore']['lss_stats']:
                        field_value_map[index] = index_stat_map["MainStore"]['lss_stats'][plasma_stat_field]
                    else:
                        self.fail("Negative digit in compressed count")
                    self.log.debug("index name is:{} field is:{} field value is:{}".format(index, plasma_stat_field,
                                                                                           field_value_map[index]))
        return field_value_map

    def check_negative_plasma_stats(self, plasma_obj_dict):
        for plasma_obj in plasma_obj_dict.values():
            index_stat = plasma_obj.get_index_storage_stats()
            for bucket in index_stat.keys():
                for index in index_stat[bucket].keys():
                    index_stat_map = index_stat[bucket][index]
                    for key in index_stat_map["MainStore"].keys():
                        self.log.debug("Key considered {}".format(key))
                        if type(index_stat_map['MainStore'].get(key)) == int or type(
                                index_stat_map['MainStore'].get(key)) == float and index_stat_map['MainStore'].get(
                                key) < 0:
                            self.fail("Negative field value for key {} in MainStore".format(key))
                    for key in index_stat_map["BackStore"].keys():
                        if type(index_stat_map['BackStore'].get(key)) == int or type(
                                index_stat_map['BackStore'].get(key)) == float and index_stat_map['BackStore'].get(
                                key) < 0:
                            self.fail("Negative field value for key {} in BackStore".format(key))

    def find_mem_used_percent(self, index_stats_map):
        self.log.debug("memory total storage {}".format(index_stats_map['memory_total_storage']))
        self.log.debug("memory used storage {}".format(index_stats_map['memory_used_storage']))
        mem_used_percent = int(
            (Decimal(index_stats_map['memory_used_storage']) / index_stats_map['memory_total_storage']) * 100)
        self.log.debug("mem used percent is {}".format(mem_used_percent))
        return mem_used_percent

    def create_Stats_Obj_list(self):
        stats_obj_dict = dict()
        for node in self.cluster.index_nodes:
            stat_obj = PlasmaStatsUtil(node, server_task=self.task, cluster=self.cluster)
            stats_obj_dict[str(node.ip)] = stat_obj
        return stats_obj_dict

    def validate_plasma_stat_field_value(self, stat_obj_list, field, value, ops='lesser', timeout=30, check_single_collection=False, sleep_time=10):
        isFound = True
        value = "{:.2f}".format(value)
        for count in range(timeout):
            isFound = True
            field_value_list = self.get_plasma_index_stat_value(field, stat_obj_list)
            self.log.debug("size is:{}".format(len(field_value_list.values())))
            for field_value in field_value_list.values():
                field_value = "{:.2f}".format(field_value)
                self.log.debug("field value: {} and expected value: {}".format(field_value, value))
                if ops == 'equal':
                    self.log.debug("Equal operation")
                    if not field_value == value:
                        isFound = False
                elif ops == 'greater':
                    self.log.debug("greater operation")
                    if not field_value > value:
                        isFound = False
                elif ops == 'equalOrLessThan':
                    self.log.debug("greater operation")
                    if not field_value <= value:
                        isFound = False
                else:
                    self.log.debug("lesser operation")
                    if not field_value < value:
                        isFound = False
                if not isFound and not check_single_collection:
                    break
                if isFound and check_single_collection:
                    return True
            if isFound:
                break
            else:
                self.sleep(sleep_time, "waiting to settle down the plasma stat value")
        return isFound

    def validate_plasma_stat_index_specific(self, stat_obj_list, field, index_list, field_value, ops='lesser', timeout=30):
        isCompare = False
        field_value = "{:.2f}".format(field_value)
        for index in index_list:
            for count in range(timeout):
                field_value_map = self.get_plasma_index_stat_value(field, stat_obj_list)
                isCompare = False
                actual_field_value = "{:.2f}".format(field_value_map[index])
                self.log.debug("Actual field value: {}".format(field_value_map[index]))
                if ops == 'equal':
                    if actual_field_value == field_value:
                        isCompare = True
                        break
                elif ops == 'greater':
                    if actual_field_value > field_value:
                        isCompare = True
                        break
                elif ops == 'lesser':
                    if actual_field_value < field_value:
                        isCompare = True
                        break
                elif ops == 'equalOrLessThan':
                    if actual_field_value <= field_value:
                        isCompare = True
                        break
                else:
                    self.log.info("Operation {} not supported".format(ops))
                    return False
                if isCompare:
                    break
                else:
                    self.sleep(10, "waiting to settle down the plasma stat value")
            if not isCompare:
                return isCompare

        return isCompare

    def compare_plasma_stat_field_value(self, stat_obj_list, field, value_map, ops='equal', timeout=30):
        isCompare = False
        for key in value_map.keys():
            for count in range(timeout):
                field_value_map = self.get_plasma_index_stat_value(field, stat_obj_list)
                isCompare = False
                self.log.debug("Actual field value: {}".format(value_map[key]))
                self.log.debug("Comparing field value: {}".format(field_value_map[key]))
                if ops == 'equal':
                    if field_value_map[key] == value_map[key]:
                         isCompare = True
                         break
                elif ops == 'greater':
                    if field_value_map[key] > value_map[key]:
                        isCompare = True
                        break
                elif ops == 'lesser':
                    if field_value_map[key] < value_map[key]:
                        isCompare = True
                        break
                elif ops == 'equalOrLessThan':
                    if field_value_map[key] <= value_map[key]:
                        isCompare = True
                        break
                elif ops == 'equalOrGreaterThan':
                    if field_value_map[key] >= value_map[key]:
                        isCompare = True
                        break
                else:
                    self.log.info("Operation {} not supported".format(ops))
                    return False
                if isCompare:
                    break
                else:
                    self.sleep(10, "waiting to settle down the plasma stat value")
            if not isCompare:
                return isCompare

        return isCompare

    def extract_index_list(self, indexMap):
        index_list = list()
        for bucket, bucket_data in indexMap.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        index_list.append(gsi_index_name)
        return index_list

    def check_compression_stat(self, index_nodes_list):
        comp_stat_verified = True
        for node in index_nodes_list:
            plasma_stats_obj = PlasmaStatsUtil(node, server_task=self.task)
            index_storage_stats = plasma_stats_obj.get_index_storage_stats()
            for bucket in index_storage_stats.keys():
                for index in index_storage_stats[bucket].keys():
                    index_stat_map = index_storage_stats[bucket][index]
                    self.assertTrue(index_stat_map["MainStore"]["num_rec_compressible"] <= (
                                index_stat_map["MainStore"]["num_rec_allocs"] - index_stat_map["MainStore"]["num_rec_frees"] + index_stat_map["MainStore"][
                            "num_rec_compressed"]),
                                    "For MainStore num_rec_compressible is {} num_rec_allocs is {} num_rec_frees is {} num_rec_compressed is {}".format(
                                        index_stat_map["MainStore"]["num_rec_compressible"], index_stat_map["MainStore"]["num_rec_allocs"],
                                        index_stat_map["MainStore"]["num_rec_frees"], index_stat_map["MainStore"]["num_rec_compressed"]))
                    self.assertTrue(index_stat_map["BackStore"]["num_rec_compressible"] <= (
                            index_stat_map["BackStore"]["num_rec_allocs"] - index_stat_map["BackStore"][
                        "num_rec_frees"] + index_stat_map["BackStore"][
                                "num_rec_compressed"]),
                                    "For BackStore num_rec_compressible is {} num_rec_allocs is {} num_rec_frees is {} num_rec_compressed is {}".format(
                                        index_stat_map["BackStore"]["num_rec_compressible"], index_stat_map["BackStore"]["num_rec_allocs"],
                                        index_stat_map["BackStore"]["num_rec_frees"], index_stat_map["BackStore"]["num_rec_compressed"]))
                    self.log.debug("Compression value is: {}".format(index_stat_map["MainStore"]["num_rec_compressed"]))
                    if index_stat_map["MainStore"]["num_rec_compressed"] == 0:
                        if index_stat_map["MainStore"]["num_rec_compressible"] > 0:
                            self.log.debug("num_rec_compressible value is {}".format(index_stat_map["MainStore"]["num_rec_compressible"]))
                            return False
                        else:
                            self.log.debug("Items not compressing as num_rec_compressed is 0")
                    elif index_stat_map["MainStore"]["num_rec_compressed"] < 0 or index_stat_map["BackStore"]["num_rec_compressed"] < 0:
                        self.fail("Negative digit in compressed count")
        return comp_stat_verified

    def verify_compression_stat(self, index_nodes_list, retry=10):
        for x in range(retry):
            if self.check_compression_stat(index_nodes_list):
                return True
            self.sleep(5, "Waiting for compression to complete")
        return False
    def findBucket(self, bucket_name, cluster=None):
        if cluster is None:
            cluster = self.cluster
        for bucket in cluster.buckets:
            if bucket.name == bucket_name:
                return bucket

    def findScope(self, scope_name, bucket):
        for _, scope in bucket.scopes.items():
            if scope.name == scope_name:
                return scope

    def findCollection(self, collection_name, scope):
        for _, collection in scope.collections.items():
            if collection.name == collection_name:
                return collection

    def validate_index_data(self, indexMap, totalCount, field='body', limit=5000, is_sync=True):
        query_len = len(self.cluster.query_nodes)
        x = 0
        self.log.debug("Inside validate index")
        self.log.debug("total count is {}".format(totalCount))
        self.log.debug("limit is {}".format(limit))
        query_task_list = list()
        for bucket_name, bucket_data in indexMap.items():
            bucket = self.findBucket(bucket_name)
            self.log.debug("bucket name is:{}".format(bucket_name))
            for scope_name, collection_data in bucket_data.items():
                scope = self.findScope(scope_name, bucket)
                self.log.debug("scope name is:{}".format(scope_name))
                for collection_name, gsi_index_names in collection_data.items():
                    collection = self.findCollection(collection_name, scope)
                    self.log.debug("Total count is: {}".format(totalCount))
                    for gsi_index_name in gsi_index_names:
                        offset = 0
                        while True:
                            self.log.debug("Inside validate index")
                            self.log.debug("offset is:" + str(offset))
                            self.log.debug("limit is:" + str(limit))
                            query = "select meta().id,%s from `%s`.`%s`.`%s` data USE INDEX (%s  USING GSI) where %s " \
                                    "is not missing order by meta().id limit %s offset %s" % (
                                        field, bucket_name, scope_name, collection_name, gsi_index_name, field, limit,
                                        offset)
                            self.log.debug("Executed query is {}".format(query))
                            query_node_index = x % query_len
                            self.log.debug("Query node is".format(self.cluster.query_nodes[query_node_index]))
                            task = self.task.compare_KV_Indexer_data(self.cluster,
                                                                     self.cluster.query_nodes[query_node_index],
                                                                     self.task_manager, query, self.sdk_client_pool,
                                                                     bucket, scope, collection,
                                                                     index_name=gsi_index_name,
                                                                     offset=offset, field=field)
                            query_task_list.append(task)
                            x += 1
                            if is_sync:
                                self.log.debug("Is sync is true")
                                if x == query_len:
                                    self.log.debug("Getting status for each query")
                                    for task in query_task_list:
                                        self.task_manager.get_task_result(task)
                                    self.log.debug("Resetting the list")
                                    query_task_list = list()
                                    x = 0
                            if offset >= totalCount:
                                break
                            offset += limit
                            if offset > totalCount:
                                offset = totalCount
            return query_task_list

    def perform_plasma_mem_ops(self,ops='compactAll'):
        for index_node in self.cluster.index_nodes:
            self.cluster_util.indexer_id_ops(node=index_node, ops=ops)

    def verify_bucket_count_with_index_count(self, indexMap, totalCount, field):
        """
        :param query_definitions: Query definition
        :param buckets: List of bucket objects to verify
        :return:
        """
        count = 0
        indexer_rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        for bucket, bucket_data in indexMap.items():
            indexer_rest.wait_for_indexing_to_complete(bucket)
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        count_query = "select count(*) from `%s`.`%s`.`%s` use index(`%s`) where %s is not missing" \
                                             % (bucket,
                                                scope, collection, gsi_index_name, field)
                        self.log.debug("Count query is {}".format(count_query))
                        status, content, header = indexer_rest.execute_query(server=self.cluster.query_nodes[0],
                                                                             query=count_query)
                        index_count = int(json.loads(content)['results'][0]['$1'])
                        if (int(index_count) != int(totalCount)):
                            self.log.info("Expected count is {} and actual count is {}".format(index_count, totalCount))
                            return False
        self.log.info("Items Indexed Verified with bucket count...")
        return True
    def _verify_items_count(self):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        indexer_rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        index_map = indexer_rest.get_index_stats()
        for bucket_name in index_map.keys():
            self.log.info("Bucket: {0}".format(bucket_name))
            for index_name, index_val in index_map[bucket_name].items():
                self.log.info("Index: {0}".format(index_name))
                self.log.info("number of docs pending: {0}".format(index_val["num_docs_pending"]))
                self.log.info("number of docs queued: {0}".format(index_val["num_docs_queued"]))
                if index_val["num_docs_pending"] and index_val["num_docs_queued"]:
                    return False
        return True

    def wait_for_mutuations_to_settle_down(self, stat_obj_list):
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "lss_fragmentation")
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "purges")
        self.wait_for_stats_to_settle_down(stat_obj_list, "equal", "inserts")

    def wait_for_stats_to_settle_down(self, stats_obj_list, ops, field, retry=10):
        for x in range(retry):
            if not self.check_stats_values_changing(stats_obj_list, ops, field):
                self.sleep(10, "wait for values to settle down")

    def check_stats_values_changing(self, stat_obj_list, ops='greater', field='merges'):
        field_dict = self.get_plasma_index_stat_value(field, stat_obj_list)
        return self.compare_plasma_stat_field_value(stat_obj_list, field, field_dict,
                                             ops, timeout=1)

    def compareField(self, actual_field, expected_field, ops):
        if ops == 'equal':
            self.log.debug("Equal operation")
            if actual_field == expected_field:
                return True
        elif ops == 'greater':
            self.log.debug("greater operation")
            if actual_field > expected_field:
                return True
        elif ops == 'equalOrLessThan':
            self.log.debug("greater operation")
            if actual_field <= expected_field:
                return True
        else:
            self.log.debug("lesser operation")
            if actual_field < expected_field:
                return True
        return False

    def check_for_stat_field(self, stats_obj_list, field='resident_ratio', fieldValue=1.00, ops='equalOrLessThan', avg=False):
        if avg:
            fieldValue = "{:.2f}".format(fieldValue)
            for count in range(5):
                field_value_map = self.get_plasma_index_stat_value(field, stats_obj_list)
                self.log.debug("size is:{}".format(len(field_value_map.values())))
                avgValue = 0
                for field_value in field_value_map.values():
                    avgValue = avgValue + field_value
                avgValue = avgValue / len(field_value_map.values())
                avgValue = "{:.2f}".format(avgValue)
                if self.compareField(avgValue, fieldValue, ops):
                    return True
                else:
                    self.sleep(200, "waiting for stat to settle down to check the resident ratio")
        else:
            return self.validate_plasma_stat_field_value(stats_obj_list, field, fieldValue,
                                                         ops, timeout=2, check_single_collection=False, sleep_time=200)

    def load_item_till_dgm_reached(self, stats_obj_list, resident_ratio, start_item=0, items_add=30000, avg=False):
        initial_count = start_item
        self.create_end = start_item
        while not self.check_for_stat_field(stats_obj_list, 'resident_ratio', resident_ratio, 'equalOrLessThan',avg):
            self.create_start = self.create_end
            self.create_end = self.create_start + items_add
            self.generate_docs(doc_ops="create")
            data_load_task = self.data_load()
            self.wait_for_doc_load_completion(data_load_task)
            self.log.debug("Added items from {} to {}".format(self.create_start, self.create_end))
        return self.create_end - start_item
