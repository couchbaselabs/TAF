from Cb_constants import constants
from index_utls.index_ready_functions import IndexUtils
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
import string, random, math
from storage.storage_base import StorageBase
from gsiLib.gsiHelper import GsiHelper
from platform_utils.remote.remote_util import RemoteMachineShellConnection


class PlasmaBaseTest(StorageBase):
    def setUp(self):
        super(PlasmaBaseTest, self).setUp()
        self.retry_exceptions = list([SDKException.AmbiguousTimeoutException,
                                      SDKException.DurabilityImpossibleException,
                                      SDKException.DurabilityAmbiguousException])
        max_clients = min(self.task_manager.number_of_threads, 20)
        self.sdk_timeout = self.input.param("sdk_timeout", 60)
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
            indexerkill_shell.disconnect()
            counter = 0
            self.sleep(1)
        self.log.info("Kill indexer process for node: {} completed".format(str(server.ip)))

    def polling_for_All_Indexer_to_Ready(self, indexes_to_build, buckets=None):
        if buckets is None:
            buckets = self.buckets
        for _, scope_data in indexes_to_build.items():
            for _, collection_data in scope_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        self.assertTrue(self.indexUtil.wait_for_indexes_to_go_online(self.cluster, buckets, gsi_index_name),
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
