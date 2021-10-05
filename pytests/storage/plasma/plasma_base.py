import json
from decimal import Decimal

from Cb_constants import constants, CbServer
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
        self.num_replicas = self.input.param("num_replicas", 1)
        self.retry_exceptions = list([SDKException.AmbiguousTimeoutException,
                                      SDKException.DurabilityImpossibleException,
                                      SDKException.DurabilityAmbiguousException])
        max_clients = min(self.task_manager.number_of_threads, 20)
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
        self.moi_snapshot_interval = self.input.param("moi_snapshot_interval", 120)
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
        self.index_count = self.input.param("index_count", 2)
        self.counter = self.input.param("counter", 30)

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

    def kill_indexer(self, server, timeout=10):
        self.stop_killIndexer = False
        counter = 0
        indexerkill_shell = RemoteMachineShellConnection(server)
        output, error = indexerkill_shell.kill_indexer()
        while not self.stop_killIndexer:
            counter += 1
            if counter > timeout:
                break
            output, error = indexerkill_shell.kill_indexer()
            # output, error = remote_client.execute_command("pkill -f indexer")
            self.log.info("Output value is:" + str(output))
            self.log.info("Counter value is {0} and max count is {1}".format(str(counter), str(timeout)))
            indexerkill_shell.disconnect()
            self.sleep(1)
        self.log.info("Kill indexer process for node: {} completed".format(str(server.ip)))

    def polling_for_All_Indexer_to_Ready(self, indexes_to_build, buckets=None):
        if buckets is None:
            buckets = self.buckets
        for _, scope_data in indexes_to_build.items():
            for _, collection_data in scope_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        self.assertTrue(
                            self.indexUtil.wait_for_indexes_to_go_online(self.cluster, buckets, gsi_index_name),
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

    def  mem_used_reached(self, exp_percent, plasma_obj_dict):
        for plasma_obj in plasma_obj_dict.values():
            index_stat = plasma_obj.get_all_index_stat_map()
            percent = self.find_mem_used_percent(index_stat)
            if (percent > exp_percent):
                return False
        return True

    def get_plasma_index_stat_value(self, plasma_stat_field, plasma_obj_dict):
        field_value_list = list()
        for plasma_obj in plasma_obj_dict.values():
            index_stat = plasma_obj.get_index_storage_stats()
            for bucket in index_stat.keys():
                for index in index_stat[bucket].keys():
                    self.log.debug("index name is:"+str(index))
                    index_stat_map = index_stat[bucket][index]
                    field_value_list.append(index_stat_map["MainStore"][plasma_stat_field])
                    self.log.debug("field is:"+str(plasma_stat_field))
                    self.log.debug("field value is: {}".format(index_stat_map["MainStore"][plasma_stat_field]))
        return field_value_list

    def find_mem_used_percent(self, index_stats_map):
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

    def verify_compression_stat(self, index_nodes_list):
        comp_stat_verified = True
        for node in index_nodes_list:
            plasma_stats_obj = PlasmaStatsUtil(node, server_task=self.task)
            index_storage_stats = plasma_stats_obj.get_index_storage_stats()
            for bucket in index_storage_stats.keys():
                for index in index_storage_stats[bucket].keys():
                    index_stat_map = index_storage_stats[bucket][index]
                    if index_stat_map["MainStore"]["num_rec_compressed"] == 0:
                        comp_stat_verified = False
                    elif index_stat_map["MainStore"]["num_rec_compressed"] < 0:
                        self.fail("Negative digit in compressed count")
        return comp_stat_verified

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

    def validate_index_data(self, indexMap, totalCount, field='body', limit=50):
        query_len = len(self.cluster.query_nodes)
        x = 0
        self.log.debug("Inside validate index")
        self.log.debug("total count is {}".format(totalCount))
        self.log.debug("limit is {}".format(limit))
        query_task_list = list()
        for bucket_name, bucket_data in indexMap.items():
            bucket = self.findBucket(bucket_name)
            self.log.debug("bucket name is:".format(bucket.name))
            for scope_name, collection_data in bucket_data.items():
                scope = self.findScope(scope_name, bucket)
                self.log.debug("bucket name is:".format(scope_name))
                for collection_name, gsi_index_names in collection_data.items():
                    collection = self.findCollection(collection_name, scope)
                    offset = 0
                    for gsi_index_name in gsi_index_names:
                        while offset < totalCount:
                            self.log.debug("Inside validate index")
                            self.log.info("offset is:"+str(offset))
                            self.log.info("limit is:"+str(totalCount))
                            query = "select meta().id,%s from `%s`.`%s`.`%s` data USE INDEX (%s  USING GSI) where body is not missing order by meta().id limit %s offset %s" % (
                                    field, bucket_name, scope_name, collection_name, gsi_index_name, limit, offset)
                            query_node_index = x % query_len
                            task = self.task.compare_KV_Indexer_data(self.cluster,
                                                                         self.cluster.query_nodes[query_node_index],
                                                                         self.task_manager, query, self.sdk_client_pool,
                                                                         bucket, scope, collection, index_name=gsi_index_name,
                                                                         offset=offset)
                            query_task_list.append(task)
                            x += 1
                            offset += limit
            return query_task_list

    def perform_plasma_mem_ops(self,ops='compactAll'):
        for index_node in self.cluster.index_nodes:
            self.cluster_util.indexer_id_ops(node=index_node, ops=ops)

    def validate_plasma_stat_field_value(self, stat_obj_list, field, value, ops='equal', timeout=30):
        isFound = False
        for count in range(timeout):
            field_value_list = self.get_plasma_index_stat_value(field, stat_obj_list)
            for field_value in field_value_list:
                if ops == 'equal':
                    if field_value == value:
                        isFound = True
                        break
                elif ops == 'greater':
                    if field_value > value:
                        isFound = True
                        break
                else:
                    if field_value < value:
                        isFound = True
                        break
            if isFound:
                break
            else:
                self.sleep(10, "waiting to settle down the plasma stat value")
        return isFound
