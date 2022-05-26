import traceback
import unittest
from collections import OrderedDict
from datetime import datetime

import global_vars
from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from Jython_tasks.task_manager import TaskManager
from SystemEventLogLib.Events import EventHelper
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from common_lib import sleep
from couchbase_helper.cluster import ServerTasks
from couchbase_helper.durability_helper import BucketDurability
from global_vars import logger
from node_utils.node_ready_functions import NodeUtils
from sdk_client3 import SDKClientPool
from test_summary import TestSummary


class CouchbaseBaseTest(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input

        # Framework level params
        self.log_level = self.input.param("log_level", "info").upper()
        self.infra_log_level = self.input.param("infra_log_level",
                                                "error").upper()
        self.test_timeout = self.input.param("test_timeout", 3600)
        self.thread_to_use = self.input.param("threads_to_use", 30)
        self.case_number = self.input.param("case_number", 0)

        self.skip_teardown_cleanup = self.input.param("skip_teardown_cleanup",
                                                      False)
        # End of Framework params

        # Cluster level params
        self.cb_clusters = OrderedDict()
        # End of cluster params

        # Bucket specific params
        self.bucket_type = self.input.param("bucket_type",
                                            Bucket.Type.MEMBASE)
        self.bucket_ttl = self.input.param("bucket_ttl", 0)
        self.bucket_size = self.input.param("bucket_size", None)
        self.bucket_conflict_resolution_type = \
            self.input.param("bucket_conflict_resolution",
                             Bucket.ConflictResolution.SEQ_NO)
        self.flush_enabled = self.input.param("flushEnabled",
                                              Bucket.FlushBucket.DISABLED)
        self.bucket_time_sync = self.input.param("bucket_time_sync", False)
        self.standard_buckets = self.input.param("standard_buckets", 1)
        self.num_replicas = self.input.param("replicas",
                                             Bucket.ReplicaNum.ONE)
        self.bucket_durability_level = self.input.param(
            "bucket_durability", Bucket.DurabilityLevel.NONE).upper()
        self.active_resident_threshold = \
            int(self.input.param("active_resident_threshold", 100))
        self.compression_mode = \
            self.input.param("compression_mode",
                             Bucket.CompressionMode.PASSIVE)
        self.bucket_storage = \
            self.input.param("bucket_storage",
                             Bucket.StorageBackend.couchstore)

        self.scope_name = self.input.param("scope", CbServer.default_scope)
        self.collection_name = self.input.param("collection",
                                                CbServer.default_collection)
        self.bucket_purge_interval = self.input.param("bucket_purge_interval",
                                                      1)
        self.bucket_durability_level = \
            BucketDurability[self.bucket_durability_level]
        # End of bucket params

        # Doc specific params
        self.key = self.input.param("key", "test_docs")
        self.key_size = self.input.param("key_size", 8)
        self.doc_size = self.input.param("doc_size", 256)
        self.sub_doc_size = self.input.param("sub_doc_size", 10)
        self.doc_type = self.input.param("doc_type", "json")
        self.num_items = self.input.param("num_items", 100000)
        self.target_vbucket = self.input.param("target_vbucket", None)
        self.maxttl = self.input.param("maxttl", 0)
        self.random_exp = self.input.param("random_exp", False)
        self.randomize_doc_size = self.input.param("randomize_doc_size", False)
        self.randomize_value = self.input.param("randomize_value", False)
        self.rev_write = self.input.param("rev_write", False)
        self.rev_read = self.input.param("rev_read", False)
        self.rev_update = self.input.param("rev_update", False)
        self.rev_del = self.input.param("rev_del", False)
        self.random_key = self.input.param("random_key", False)
        self.mix_key_size = self.input.param("mix_key_size", False)
        # End of doc specific parameters

        # Transactions parameters
        self.transaction_timeout = self.input.param("transaction_timeout", 100)
        self.transaction_commit = self.input.param("transaction_commit", True)
        self.transaction_durability_level = \
            self.input.param("transaction_durability", "")
        self.update_count = self.input.param("update_count", 1)
        self.sync = self.input.param("sync", True)
        self.default_bucket = self.input.param("default_bucket", True)
        self.num_buckets = self.input.param("num_buckets", 0)
        self.atomicity = self.input.param("atomicity", False)
        self.defer = self.input.param("defer", False)
        # end of transaction parameters

        # Client specific params
        self.sdk_client_type = self.input.param("sdk_client_type", "java")
        self.replicate_to = self.input.param("replicate_to", 0)
        self.persist_to = self.input.param("persist_to", 0)
        self.sdk_retries = self.input.param("sdk_retries", 5)
        self.sdk_timeout = self.input.param("sdk_timeout", 5)
        self.time_unit = self.input.param("time_unit", "seconds")
        self.durability_level = self.input.param("durability", "NONE").upper()
        self.sdk_client_pool = self.input.param("sdk_client_pool", None)
        self.sdk_pool_capacity = self.input.param("sdk_pool_capacity", 1)
        # Client compression settings
        self.sdk_compression = self.input.param("sdk_compression", None)
        compression_min_ratio = self.input.param("min_ratio", None)
        compression_min_size = self.input.param("min_size", None)
        if type(self.sdk_compression) is bool:
            self.sdk_compression = {"enabled": self.sdk_compression}
            if compression_min_size:
                self.sdk_compression["minSize"] = compression_min_size
            if compression_min_ratio:
                self.sdk_compression["minRatio"] = compression_min_ratio

        # Doc Loader Params
        self.process_concurrency = self.input.param("process_concurrency", 20)
        self.batch_size = self.input.param("batch_size", 2000)
        self.dgm_batch = self.input.param("dgm_batch", 5000)
        self.ryow = self.input.param("ryow", False)
        self.check_persistence = self.input.param("check_persistence", False)
        self.ops_rate = self.input.param("ops_rate", 10000)
        # End of client specific parameters

        # initial number of items in the cluster
        self.services_init = self.input.param("services_init", None)
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)

        # variable for log collection using cbCollect
        self.get_cbcollect_info = self.input.param("get-cbcollect-info", False)

        # Initiate logging variables
        self.log = logger.get("test")
        self.infra_log = logger.get("infra")
        global_vars.system_event_logs = EventHelper()
        self.system_events = global_vars.system_event_logs

        # Configure loggers
        self.log.setLevel(self.log_level)
        self.infra_log.setLevel(self.infra_log_level)

        # Alias to the common sleep function
        self.sleep = sleep

        # Support lib objects for testcase execution
        self.task_manager = TaskManager(self.thread_to_use)
        self.task = ServerTasks(self.task_manager)
        self.node_utils = NodeUtils(self.task_manager)
        # End of library object creation

        # SDKClientPool object for creating generic clients across tasks
        if self.sdk_client_pool is True:
            self.init_sdk_pool_object()

        # Variable for initializing the current (start of test) timestamp
        self.start_timestamp = datetime.now()

        self.test_failure = None
        self.summary = TestSummary(self.log)

    def tearDown(self):
        pass

    def log_setup_status(self, class_name, status, stage="setup"):
        self.log.info(
            "========= %s %s %s for test #%d %s ========="
            % (class_name, stage, status, self.case_number,
               self._testMethodName))

    def is_test_failed(self):
        return (hasattr(self, '_resultForDoCleanups')
                and len(self._resultForDoCleanups.failures
                        or self._resultForDoCleanups.errors)) \
               or (hasattr(self, '_exc_info')
                   and self._exc_info()[1] is not None)

    def handle_setup_exception(self, exception_obj):
        # Shutdown client pool in case of any error before failing
        if self.sdk_client_pool is not None:
            self.sdk_client_pool.shutdown()
        # print the tracback of the failure
        traceback.print_exc()
        # Throw the exception so that the test will fail at setUp
        raise exception_obj

    def log_failure(self, message):
        self.log.error(message)
        self.summary.set_status("FAILED")
        if self.test_failure is None:
            self.test_failure = message

    def validate_test_failure(self):
        if self.test_failure is not None:
            self.fail(self.test_failure)

    def get_clusters(self):
        return [self.cb_clusters[name] for name in self.cb_clusters.keys()]

    def get_task(self):
        return self.task

    def get_task_mgr(self):
        return self.task_manager

    def init_sdk_pool_object(self):
        self.sdk_client_pool = SDKClientPool()
        DocLoaderUtils.sdk_client_pool = self.sdk_client_pool

    def shutdown_task_manager(self):
        self.task_manager.shutdown_task_manager()
        self.task.shutdown(force=True)
        self.task_manager.abort_all_tasks()
