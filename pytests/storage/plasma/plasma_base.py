from sdk_exceptions import SDKException
import string, random, math
from storage.storage_base import StorageBase


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

    def randStr(self, Num=10):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(Num))

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
            if node.ip+":"+node.port == ip:
                return True
        return False