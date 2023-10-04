import math
import os
import random
import subprocess
import time
import copy

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants import DocLoading
from Cb_constants.CBServer import CbServer
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator, SubdocDocumentGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from sdk_constants.java_client import SDKConstants
from com.couchbase.test.taskmanager import TaskManager
from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.docgen import WorkLoadSettings,\
    DocumentGenerator
from com.couchbase.test.sdk import Server
from com.couchbase.test.loadgen import WorkLoadGenerate
from Jython_tasks.task import PrintBucketStats
from java.util import HashMap
from com.couchbase.test.docgen import DocRange
from couchbase.test.docgen import DRConstants
from com.couchbase.client.core.error import ServerOutOfMemoryException,\
    DocumentExistsException, DocumentNotFoundException, TimeoutException
from storage.storage_base import StorageBase


class GuardrailsBase(StorageBase):
    def setUp(self):
        super(GuardrailsBase, self).setUp()

        self.couchstore_max_data_per_node = self.input.param("couchstore_max_data_per_node", 1.6)
        self.magma_max_data_per_node = self.input.param("magma_max_data_per_node", 16)
        self.max_disk_usage = self.input.param("max_disk_usage", 85)
        self.autoCompactionDefined = str(self.input.param("autoCompactionDefined", "false")).lower()

        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                       self.cluster.nodes_in_cluster)
        self.log.info("KV nodes {}".format(self.cluster.kv_nodes))

    def check_resident_ratio(self, cluster):
        """
        This function returns a dictionary which contains resident ratios
        of all the buckets in the cluster across all nodes.
        key = bucket name
        value = list of resident ratios of the bucket on different nodes
        Ex:  bucket_rr = {'default': [100, 100], 'bucket-1': [45.8, 50.1]}
        """
        bucket_rr = dict()
        for server in cluster.kv_nodes:
            kv_ep_max_size = dict()
            _, res = RestConnection(server).query_prometheus("kv_ep_max_size")
            for item in res["data"]["result"]:
                bucket_name = item["metric"]["bucket"]
                kv_ep_max_size[bucket_name] = float(item["value"][1])

            _, res = RestConnection(server).query_prometheus("kv_logical_data_size_bytes")
            for item in res["data"]["result"]:
                if item["metric"]["state"] == "active":
                    bucket_name = item["metric"]["bucket"]
                    logical_data_bytes = float(item["value"][1])
                    resident_ratio = (kv_ep_max_size[bucket_name] / logical_data_bytes) * 100
                    resident_ratio = min(resident_ratio, 100)
                    if bucket_name not in bucket_rr:
                        bucket_rr[bucket_name] = [resident_ratio]
                    else:
                        bucket_rr[bucket_name].append(resident_ratio)

        return bucket_rr

    def check_if_rr_guardrail_breached(self, bucket, current_rr, threshold):

        rr_bucket = current_rr[bucket.name]
        for rr_val in rr_bucket:
            if rr_val < threshold:
                return True

        return False

    def tearDown(self):
        self.cluster_util.print_cluster_stats(self.cluster)

        super(GuardrailsBase, self).tearDown()
