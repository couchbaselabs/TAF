'''
Post-Expiration Purging: Storage will have nothing to do once the expiration
surpasses for items present in the storage. It is KV which inserts the deletes
markers called tombstone in the storage such that these expired docs are not
accessible anymore.

When its expiration time is reached, an item is deleted by KV as soon as one of the following occurs:
1. An attempt is made to access the item.
2. The expiry pager is run.
3. Compaction is run.

# exp_pager_stime:
The cbepctl flush_param exp_pager_stime command sets the time interval for
disk cleanup. Couchbase Server does lazy expiration, that is, expired items
are flagged as deleted rather than being immediately erased.
Couchbase Server has a maintenance process that periodically looks through all
information and erases expired items. By default, this maintenance process
runs every 60 minutes, but it can be configured to run at a different interval.

Expiry Pager
Scans for items that have expired, and erases them from memory and disk;
after which, a tombstone remains for a default period of 3(Metadata Purge Interval) days.
The expiry pager runs every 60 minutes by default: for information
on changing the interval, see cbepctl set flush_param.
'''

import copy
import os
import random
import time

from Cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from com.couchbase.client.core.error import DocumentUnretrievableException
from com.couchbase.client.java.kv import GetAnyReplicaOptions
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient


class MagmaExpiryTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaExpiryTests, self).setUp()
        self.gen_delete = None
        self.gen_create = None
        self.gen_update = None
        self.gen_expiry = None
        self.exp_pager_stime = self.input.param("exp_pager_stime", 10)
        self.iterations = self.input.param("iterations", 3)
        self.expiry_perc = self.input.param("expiry_perc", 100)
        self.meta_purge_interval = self.input.param("meta_purge_interval", 180)
        self.items = self.num_items
    def load_bucket(self):
        tasks = dict()
        for collection in self.collections:
            self.generate_docs(doc_ops="create", target_vbucket=None)
            tasks.update(self.bucket_util._async_load_all_buckets(
                    self.cluster, self.gen_create, "create", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                    retry_exceptions=self.retry_exceptions,
                    ignore_exceptions=self.ignore_exceptions,
                    skip_read_on_error=False,
                    scope=CbServer.default_scope,
                    collection=collection,
                    monitor_stats=self.monitor_stats))
        for task in tasks:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)

    def tearDown(self):
        super(MagmaExpiryTests, self).tearDown()

    def test_read_expired_replica(self):
        result = True
        self.gen_create = doc_generator(
            self.key, 0, 10,
            doc_size=20,
            doc_type=self.doc_type,
            key_size=self.key_size)

        tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", exp=10,
                batch_size=10,
                process_concurrency=1,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                )
        self.task.jython_task_manager.get_task_result(tasks_info.keys()[0])
        self.sleep(20)
        self.client = SDKClient([self.cluster.master],
                                self.cluster.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)
        for i in range(10):
            key = (self.key + "-" + str(i).zfill(self.key_size-len(self.key)))
            try:
                getReplicaResult = self.client.collection.getAnyReplica(
                    key, GetAnyReplicaOptions.getAnyReplicaOptions())
                if getReplicaResult:
                    result = False
                    try:
                        self.log.info("Able to retreive: %s" %
                                      {"key": key,
                                       "value": getReplicaResult.contentAsObject(),
                                       "cas": getReplicaResult.cas()})
                    except Exception as e:
                        self.log.error(str(e))
            except DocumentUnretrievableException as e:
                pass
            if len(self.client.get_from_all_replicas(key)) > 0:
                result = False
        self.client.close()
        self.assertTrue(result, "SDK is able to retrieve expired documents")

    def test_expiry(self):
        '''
        Test Focus: Expire items (for n iterations)
         and verify tombstone count after expiry pagers time
         and meta data purge interval

        Steps:

           --- Expired already created items/create expiry load
           --- Wait for docs to expire
           --- Check for tombstones count
           --- Verify doc count
           --- Set meta data purge age and verify tomb stone count
           --- Repeat above steps n times
        '''
        self.log.info("test_expiry starts")
        dgm_prcnt = self.get_bucket_dgm(self.buckets[0])
        self.log.info("DGM percentage is {}".format(dgm_prcnt))
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.doc_ops = "expiry"
        for it in range(self.iterations):
            self.log.info("Iteration {}".format(it+1))
            self.expiry_perc = self.input.param("expiry_perc", 100)

            self.generate_docs(doc_ops="expiry")
            tasks_info = dict()
            for collection in self.collections:
                task_in = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  collection=collection,
                                  _sync=False)
                tasks_info.update(task_in.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)

            self.sleep(self.maxttl, "Wait for docs to expire")
            # exp_pager_stime
            self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
            self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
             to kickoff")
            self.sleep(self.exp_pager_stime*30, "Wait for KV purger to scan expired docs and add \
            tombstones.")
            if dgm_prcnt < 100:
                self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets,
                cbstat_cmd="all",
                stat_name="vb_replica_queue_size")
            expected_ts_count = self.items*self.expiry_perc/100*(self.num_replicas+1)
            # Check for tombstone count in Storage
            count = 0
            time_end = time.time() + 60 * 20
            while time.time() < time_end:
                self.log.info("Iteration=={}, ts_check_Count=={}".format(it+1, count+1))
                ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
                if ts >= expected_ts_count:
                    break
                count += 1
            self.log.info("Tombstones after exp_pager_stime: {}".format(ts))
            self.log.info("Iterations - {}, expected_ts_count - {}".format(it+1, expected_ts_count))

            '''
                Commenting Asserts temporarily
                until we get clarity on time to fetch
                tombstones count
            '''
            #self.assertTrue(expected_ts_count <= ts <= expected_ts_count+(self.vbuckets *(self.num_replicas+1)),
            #                "Incorrect tombstone count in storage,\
            #                Expected: {}, Found: {}".
            #                format(expected_ts_count, ts))

            self.log.info("Verifying doc counts after create doc_ops")
            self.bucket_util.verify_stats_all_buckets(self.cluster, items=0)

            # Metadata Purge Interval
            self.meta_purge_interval_in_days = self.meta_purge_interval / 86400.0

            self.set_metadata_purge_interval(
                value=self.meta_purge_interval_in_days, buckets=self.buckets)
            self.sleep(self.meta_purge_interval, "sleeping after setting metadata purge interval using diag/eval")
            self.bucket_util.cbepctl_set_metadata_purge_interval(
                self.cluster, self.buckets, value=self.meta_purge_interval)
    #         self.bucket_util.set_metadata_purge_interval(str(self.meta_purge_interval),
    #                                                      buckets=self.buckets)
    #         self.sleep(self.meta_purge_interval*60*60*2/0.04, "Wait for Metadata Purge Interval to drop \
    #         tomb-stones from storage")
            self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
            tomb-stones from storage")
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

            #Check for tombs-tones removed
            self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            self.log.info("Tombstones after bucket compaction: {}".format(ts))
            self.assertTrue(self.vbuckets * (self.num_replicas+1)>=ts,
                             "Incorrect tombstone count in storage,\
                             Expected: {}, Found: {}".format(self.vbuckets * (self.num_replicas+1), ts))
            self.set_metadata_purge_interval(
                value=3.0, buckets=self.buckets)

    def test_create_expire_same_items(self):
        '''
        Test Focus: Create and expire n items

        Steps:

           --- Create items == num_items
                    (init_loading will be set to False)
           --- Check Disk Usage after creates
           --- Expire all the items
           --- Check for tombstones count
           --- Check Disk Usage
           --- Repeat above steps n times
        '''
        self.log.info("test_create_expire_same_items starts")
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.items = self.init_items_per_collection * self.num_collections
        #self.create_perc = 100
        self.expiry_perc = 100
        for _iter in range(self.iterations):
            self.maxttl = random.randint(5, 20)
            self.log.info("Test Iteration: {}".format(_iter+1))
            # Create items which are expired
            self.generate_docs(doc_ops="create")
            tasks_info = dict()
            for collection in self.collections:
                task_in = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=False,
                                  doc_ops="create",
                                  collection=collection)
                tasks_info.update(task_in.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)

            self.log.info("Disk usage after creates {}".format(disk_usage))
            size_before = disk_usage[0]
            dgm_prcnt = self.get_bucket_dgm(self.buckets[0])
            self.log.info("DGM percentage is {}".format(dgm_prcnt))

            self.generate_docs(doc_ops="expiry")
            tasks_info = dict()
            for collection in self.collections:
                task_in = self.loadgen_docs(self.retry_exceptions,
                                            self.ignore_exceptions,
                                            _sync=False,
                                            doc_ops="expiry",
                                            collection=collection)
                tasks_info.update(task_in.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)

            self.sleep(self.maxttl, "Wait for docs to expire")

            # exp_pager_stime
            self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
            self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
             to kickoff")
            self.sleep(self.exp_pager_stime*30, "Wait for KV purger to scan expired docs and add \
            tombstones.")
            if dgm_prcnt < 100:
                self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets,
                cbstat_cmd="all",
                stat_name="vb_replica_queue_size")
            # Check for tombstone count in Storage
            expected_ts_count = self.items*self.expiry_perc/100*(self.num_replicas+1)
            count = 0
            time_end = time.time() + 60 * 20
            while time.time() < time_end:
                self.log.info("Iteration=={}, ts_check_Count=={}".format(_iter+1, count+1))
                ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
                if ts >= expected_ts_count:
                    break
                count += 1
            self.log.info("Tombstones after exp_pager_stime: {}".format(ts))
            self.log.info("Iterations - {}, expected_ts_count - {}".format(_iter+1, expected_ts_count))
            '''
               Commenting Asserts temporarily
               until we get clarity on time
               to fetch tombstones count
            '''
            #self.assertTrue(expected_ts_count <= ts <= expected_ts_count+(self.vbuckets *(self.num_replicas+1)),
            #                "Incorrect tombstone count in storage,\
            #                Expected: {}, Found: {}".
            #                format(expected_ts_count, ts))

            # Space amplification check
            msg_stats = "Fragmentation value for {} stats exceeds\
            the configured value"
            result = self.check_fragmentation_using_magma_stats(self.buckets[0],
                                                                self.cluster.nodes_in_cluster)
            self.assertIs(result, True, msg_stats.format("magma"))

            result = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(result, True, msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            self.log.info("Disk usage after expiry {}".format(disk_usage))
            size_after = disk_usage[0]

            #self.assertTrue(size_after < size_before * 2.5,
            #                "Data Size before(%s) and after expiry(%s)"
            #                .format(size_before, size_after))
            # Metadata Purge Interva
            self.meta_purge_interval = 180
            self.meta_purge_interval_in_days = 180 / 86400.0
            self.set_metadata_purge_interval(
                value=self.meta_purge_interval_in_days, buckets=self.buckets)
            self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")
            self.bucket_util.cbepctl_set_metadata_purge_interval(
                self.cluster, self.buckets, value=self.meta_purge_interval)
            self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
            tomb-stones from storage")
            self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

            disk_usage_after_compaction = self.get_disk_usage(self.buckets[0],
                                                              self.cluster.nodes_in_cluster)[0]
            self.log.info("Iteration--{}, disk usage after compaction--{}".
                          format(_iter, disk_usage_after_compaction))

            #self.assertTrue(disk_usage_after_compaction <= 500,
            #                "Disk size after compaction=={} exceeds expected size=={}".
            #                format(disk_usage_after_compaction, 500))
            self.set_metadata_purge_interval(
                value=3.0, buckets=self.buckets)

    def test_expiry_no_wait_update(self):
        '''
        Test Focus: Upsert and expire n items

        Steps:

           --- Upsert items == num_items
                    (init_loading will be in magma base)
           --- Expire all the load
           -- Wait for docs to expire and conver to tomb stones
           --- Check Disk Usage after expiry
           --- Check for tombstones count
           --- Set meta data purge age and verify tomb stone count
           --- Check Disk Usage
           --- Repeat above steps n times
        '''
        self.log.info(" test_expiry_no_wait_update starts")
        self.update_start = 0
        self.update_end = self.init_items_per_collection
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.update_perc = 100
        self.expiry_perc = 100
        for _iter in range(self.iterations):
            self.log.info("Iteration--{}".format(_iter))
            self.generate_docs(doc_ops="update")
            tasks_info = dict()
            for collection in self.collections:
                task_in = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=False,
                                  doc_ops="update",
                                  collection=collection)
                tasks_info.update(task_in.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)
            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            self.log.debug("Disk usage after updates {}".format(disk_usage))
            size_before = disk_usage[0]
            dgm_prcnt = self.get_bucket_dgm(self.buckets[0])
            self.log.info("DGM percentage is {}".format(dgm_prcnt))

            self.generate_docs(doc_ops="expiry")
            tasks_info = dict()
            for collection in self.collections:
                task_in = self.loadgen_docs(self.retry_exceptions,
                                            self.ignore_exceptions,
                                            _sync=False,
                                            doc_ops="expiry",
                                            collection=collection)
                tasks_info.update(task_in.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)

            self.sleep(self.maxttl, "Wait for docs to expire")
            # exp_pager_stime
            self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
            self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
             to kickoff")
            self.sleep(self.exp_pager_stime*30, "Wait for KV purger to scan expired docs and add \
            tombstones.")
            if dgm_prcnt < 100:
                self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets,
                cbstat_cmd="all",
                stat_name="vb_replica_queue_size")
            # Check for tombstone count in Storage
            expected_ts_count = self.items*self.expiry_perc/100*(self.num_replicas+1)
            count = 0
            time_end = time.time() + 60 * 20
            while time.time() < time_end:
                self.log.info("Iteration=={}, ts_check_count=={}".format(_iter+1, count+1))
                ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
                if ts >= expected_ts_count:
                    break
                count += 1
            self.log.info("Tombstones after exp_pager_stime: {}".format(ts))
            self.log.info("Expected ts count is {}".format(expected_ts_count))
            '''
               Commenting Asserts temporarily
               until we get clarity on time
               to fetch tombstones count
            '''
            #self.assertTrue(expected_ts_count <= ts <= expected_ts_count+(self.vbuckets *(self.num_replicas+1)),
            #                "Incorrect tombstone count in storage,\
            #                Expected: {}, Found: {}".
            #                format(expected_ts_count, ts))
            #self.assertEqual(expected_ts_count, ts, "Incorrect tombstone count in storage,\
            #                  Expected: {}, Found: {}".
            #                  format(expected_ts_count, ts))

            self.log.info("Verifying doc counts after create doc_ops")
            self.bucket_util.verify_stats_all_buckets(self.cluster, items=0)
            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            self.log.info("Disk usage after expiry {}".format(disk_usage))
            size_after = disk_usage[0]

            #self.assertTrue(size_after < self.disk_usage[self.disk_usage.keys()[0]] * 2.5,
            #                "Data Size before(%s) and after expiry(%s)"
            #                .format(self.disk_usage[self.disk_usage.keys()[0]], size_after))

            # Metadata Purge Interval
            self.meta_purge_interval = 180
            self.meta_purge_interval_in_days = 180 / 86400.0

            self.set_metadata_purge_interval(
                value=self.meta_purge_interval_in_days, buckets=self.buckets)
            self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")
            self.bucket_util.cbepctl_set_metadata_purge_interval(
                self.cluster, self.buckets, value=self.meta_purge_interval)

            self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
            tomb-stones from storage")

            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

            # Check for tombs-tones removed
            self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            self.log.info("Tombstones after bucket compaction: {}".format(ts))

            time_end = time.time() + 1200
            while time.time() < time_end:
                disk_usage_after_compaction = self.get_disk_usage(self.buckets[0],
                                                                  self.cluster.nodes_in_cluster)[0]
                if disk_usage_after_compaction <= 500:
                    break
            self.log.info("disk usage after compaction {}".format(disk_usage_after_compaction))

            # below assert is only applicable if we expire all the items
            #self.assertTrue(disk_usage_after_compaction  < 500,
            #               "size after compaction shouldn't be more than 500")
            self.set_metadata_purge_interval(
                value=3.0, buckets=self.buckets)

    def test_docs_expired_wait_for_magma_purge(self):
        pass

    def test_expiry_disk_full(self):
        self.expiry_perc = self.input.param("expiry_perc", 100)
        self.doc_ops = "expiry"
        self.generate_docs(doc_ops="expiry")
        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.sleep(self.maxttl, "Wait for docs to expire")

        def _get_disk_usage_in_MB(remote_client, path):
            disk_info = remote_client.get_disk_info(in_MB=True, path=path)
            disk_space = disk_info[1].split()[-3][:-1]
            return disk_space

        # Fill up the disk
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        du = int(_get_disk_usage_in_MB(remote_client, self.cluster.master.data_path)) - 50
        _file = os.path.join(self.cluster.master.data_path, "full_disk_")
        cmd = "dd if=/dev/zero of={0}{1} bs=1024M count=1"
        while int(du) > 0:
            cmd = cmd.format(_file, str(du) + "MB_" + str(time.time()))
            output, error = remote_client.execute_command(cmd, use_channel=True)
            remote_client.log_command_output(output, error)
            du -= 1024
            if du < 1024:
                cmd = "dd if=/dev/zero of={0}{1} bs=" + str(du) + "M count=1"

        # exp_pager_stime
        self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
        self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
         to kickoff")
        self.sleep(self.exp_pager_stime*10, "Wait for KV purger to scan expired docs and add \
        tombstones.")

        # Check for tombstone count in Storage
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after exp_pager_stime: {}".format(ts))

        self.log.info("Verifying doc counts after create doc_ops")
        self.bucket_util.verify_stats_all_buckets(self.cluster, items=0)

        # Metadata Purge Interval
        self.meta_purge_interval = 60
        self.bucket_util.cbepctl_set_metadata_purge_interval(
            self.cluster, self.buckets, value=self.meta_purge_interval)
        self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
        tomb-stones from storage")

        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

        # free up the disk
        output, error = remote_client.execute_command("rm -rf full_disk*",
                                                      use_channel=True)

        # Wait for expiry pager to insert tombstones again
        self.sleep(self.exp_pager_stime*10, "Wait for KV purger to scan expired docs and add \
        tombstones.")

        # Check for tombstone count in Storage after disk is available
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after exp_pager_stime: {}".format(ts))

        # Metadata Purge Interval after disk space is available
        self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
        tomb-stones from storage")

        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

        # Check for tombs-tones removed
        self.bucket_util._run_compaction(self.cluster)
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after bucket compaction: {}".format(ts))

    def test_random_expiry(self):
        '''
        Test Focus: Expire items (Items will have different ttl value)
         and verify tombstone count after expiry pagers time
         and meta data purge interval

        Steps:

           --- Expired already created items/create expiry load
                  (Items will have different ttl values)
           --- Wait for docs to expire
           --- Check for tombstones count
           --- Verify doc count
           --- Set meta data purge age and verify tomb stone count
           --- Repeat above steps n times
        '''
        self.random_exp = True
        dgm_prcnt = self.get_bucket_dgm(self.buckets[0])
        self.log.info("DGM percentage is {}".format(dgm_prcnt))
        self.doc_ops = "expiry"
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.expiry_perc = self.input.param("expiry_perc", 100)
        self.generate_docs(doc_ops="expiry")
        tasks_info = dict()
        for collection in self.collections:
            task_in = self.loadgen_docs(self.retry_exceptions,
                                        self.ignore_exceptions,
                                        _sync=False,
                                        collection=collection)
            tasks_info.update(task_in.items())
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)

        self.sleep(self.maxttl, "Wait for docs to expire")

        # exp_pager_stime
        self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
        self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
         to kickoff")
        self.sleep(self.exp_pager_stime*30, "Wait for KV purger to scan expired docs and add \
        tombstones.")
        if dgm_prcnt < 100:
            self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     cbstat_cmd="all",
                                                     stat_name="vb_replica_queue_size")

        # Check for tombstone count in Storage
        expected_ts_count = self.items*self.expiry_perc/100*(self.num_replicas+1)
        count = 0
        time_end = time.time() + 60 * 20
        while time.time() < time_end:
            self.log.info("ts_check_count=={}".format(count+1))
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            if ts >= expected_ts_count:
                break
            count += 1
        self.log.info("Tombstones after exp_pager_stime: {}".format(ts))
        self.log.info("Expected ts count is {}".format(expected_ts_count))
        '''
           Commenting Assert temporarily
           until we get clarity on time
           to fetch tombstones count
        '''
        #self.assertEqual(expected_ts_count, ts, "Incorrect tombstone count in storage,\
        #                      Expected: {}, Found: {}".
        #                      format(expected_ts_count, ts))
        self.log.info("Verifying doc counts after create doc_ops")
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        # Metadata Purge Interval
        self.meta_purge_interval = 180
        self.meta_purge_interval_in_days = 180 / 86400.0

        self.set_metadata_purge_interval(
            value=self.meta_purge_interval_in_days, buckets=self.buckets)
        self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")
        self.bucket_util.cbepctl_set_metadata_purge_interval(
            self.cluster, self.buckets, value=self.meta_purge_interval)

        self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
        tomb-stones from storage")

        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

        # Check for tombs-tones removed
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after bucket compaction: {}".format(ts))
        self.assertTrue(self.vbuckets * (self.num_replicas+1)>=ts,
                        "Incorrect tombstone count in storage,\
                        Expected: {}, Found: {}".format(self.vbuckets * (self.num_replicas+1), ts))
        self.set_metadata_purge_interval(
                value=3.0, buckets=self.buckets)

    def test_expire_read_validate_meta(self):
        self.expiry_perc = self.input.param("expiry_perc", 100)
        self.doc_ops = "expiry"
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.bucket_util._expiry_pager(self.cluster, 216000)
        self.generate_docs(doc_ops="expiry")
        tasks_info = dict()
        for collection in self.collections:
            task_in = self.loadgen_docs(self.retry_exceptions,
                                        self.ignore_exceptions,
                                        _sync=False,
                                        collection=collection)
            tasks_info.update(task_in.items())
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
             tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)

        self.sleep(self.maxttl, "Wait for docs to expire")
        self.sigkill_memcached()
        self.bucket_util._expiry_pager(self.cluster, 216000)
        # Read all the docs to ensure they get converted to tombstones
        self.generate_docs(doc_ops="read",
                           read_start=self.expiry_start,
                           read_end=self.expiry_end)
        self.gen_delete = copy.deepcopy(self.gen_read)
        tasks_info = dict()
        for collection in self.collections:
            task_in = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=False,
                                  doc_ops="delete",
                                  skip_read_on_error=True,
                                  suppress_error_table=True,
                                  track_failures=False,
                                  collection=collection)
            tasks_info.update(task_in.items())
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.sleep(600, "wait after get ops")
        #data_validation = self.task.async_validate_docs(
        #        self.cluster, self.cluster.buckets[0],
        #        self.gen_read, "delete", 0,
        #        batch_size=self.batch_size,
        #        process_concurrency=self.process_concurrency,
        #        timeout_secs=self.sdk_timeout)
        #self.task.jython_task_manager.get_task_result(data_validation)

        # All docs converted to tomb-stone
        # Check for tombstone count in Storage
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after exp_pager_stime: {}".format(ts))
        expected_ts_count = self.items*self.expiry_perc/100*(self.num_replicas+1)
        self.log.info("Expected ts count is {}".format(expected_ts_count))
        '''
           Commenting Assert temporarily
           until we get clarity on time
           to fetch tombstones count
        '''
        #self.assertEqual(expected_ts_count, ts, "Incorrect tombstone count in storage,\
        #                Expected: {}, Found: {}".format(expected_ts_count, ts))

        # Metadata Purge Interval
        self.meta_purge_interval = 180
        self.meta_purge_interval_in_days = 180 / 86400.0

        self.set_metadata_purge_interval(
            value=self.meta_purge_interval_in_days, buckets=self.buckets)
        self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")
        self.bucket_util.cbepctl_set_metadata_purge_interval(
            self.cluster, self.buckets, value=self.meta_purge_interval)
        self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
        tomb-stones from storage")
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after persistent_metadata_purge_age: {}".format(ts))

        #Check for tombs-tones removed
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after bucket compaction: {}".format(ts))
        self.assertTrue(self.vbuckets * (self.num_replicas+1)>=ts,
                        "Incorrect tombstone count in storage,\
                        Expected: {}, Found: {}".format(self.vbuckets * (self.num_replicas+1), ts))
        self.set_metadata_purge_interval(
            value=3.0, buckets=self.buckets)

    def test_wait_for_expiry_read_repeat(self):
        for _iter in range(self.iterations):
            self.maxttl = random.randint(20, 60)
            self.log.info("Test Iteration: {}".format(_iter))
            self.test_expire_read_validate_meta()

    def test_expiry_full_compaction(self):
        self.doc_ops = "expiry"
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.generate_docs(doc_ops="expiry")
        tasks_info = dict()
        for collection in self.collections:
            task_in = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=False,
                              collection=collection)
            tasks_info.update(task_in.items())
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
             tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)

        self.sleep(self.maxttl, "Wait for docs to expire")

        # exp_pager_stime
        self.bucket_util._expiry_pager(self.cluster, 21600)
        self.log.info("Starting compaction for each bucket to add tombstones")
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

        self.sleep(300, "sleep after triggering compaction")
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after compaction: {}".format(ts))
        expected_ts_count = self.items*self.expiry_perc/100*(self.num_replicas+1)
        self.log.info("Expected ts count after compaction is {}".format(expected_ts_count))

        '''
           Commenting Assert temporarily
           until we get clarity on time
           to fetch tombstones count
        '''
        #self.assertEqual(expected_ts_count, ts, "Incorrect tombstone count in storage,\
        #Expected: {}, Found: {}".format(expected_ts_count, ts))

        # Metadata Purge Interval
        self.meta_purge_interval = 180
        self.meta_purge_interval_in_days = 180 / 86400.0
        self.set_metadata_purge_interval(
            value=self.meta_purge_interval_in_days, buckets=self.buckets)
        self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")
        self.bucket_util.cbepctl_set_metadata_purge_interval(
            self.cluster, self.buckets, value=self.meta_purge_interval)
        self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
        tomb-stones from storage")

        self.log.info("Starting compaction for each bucket")
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        self.sleep(300, "sleep after triggering compaction, to drop tombstones")

        # All docs and tomb-stone should be dropped from the storage
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after full compaction: {}".format(ts))
        self.log.info("Expected {}".format(self.vbuckets * (self.num_replicas+1)))

        self.assertTrue(self.vbuckets * (self.num_replicas+1) >= ts,
                        "Incorrect tombstone count in storage,\
                        Expected: {}, Found: {}".format(self.vbuckets * (self.num_replicas+1), ts))
        self.set_metadata_purge_interval(
                value=3.0, buckets=self.buckets)

    def test_drop_collection_expired_items(self):
        self.log.info("test_drop_collection_expired_items starts")
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.generate_docs(doc_ops="expiry")
        tasks = dict()
        for collection in self.collections[::2]:
            task = self.loadgen_docs(scope=CbServer.default_scope,
                                     collection=collection,
                                     _sync=False,
                                     doc_ops="expiry")
            tasks.update(task.items())
        for task in tasks:
            self.task_manager.get_task_result(task)

        self.sleep(self.maxttl, "Wait for docs to expire")

        # Convert to tomb-stones
        self.gen_delete = copy.deepcopy(self.gen_expiry)
        tasks_info = dict()
        for collection in self.collections[::2]:
            task_in = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=False,
                                  doc_ops="delete",
                                  skip_read_on_error=True,
                                  suppress_error_table=True,
                                  track_failures=False,
                                  collection=collection)
            tasks_info.update(task_in.items())
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        for collection in self.collections[::2]:
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.buckets[0],
                                             scope_name=CbServer.default_scope,
                                             collection_name=collection)
            self.buckets[0].scopes[CbServer.default_scope].collections.pop(collection)
            self.collections.remove(collection)
        self.sleep(180, "sleep after dropping collections")
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("tombstone count is {}".format(ts))
        self.bucket_util._run_compaction(self.cluster)
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("tombstone count after compaction  {}".format(ts))
        expected_ts = self.vbuckets * (self.num_replicas+1) * int(self.num_collections)//2
        self.log.info("expected_ts {}".format(expected_ts))
        self.assertEqual(ts, expected_ts, "Incorrect number of tombstones found.")

    def test_drop_collection_during_tombstone_creation(self):
        self.log.info("test_drop_collection_during_tombstone_creation")
        self.expiry_start = 0
        self.expiry_end = self.init_items_per_collection
        self.generate_docs(doc_ops="expiry")
        tasks = dict()
        for collection in self.collections[::2]:
            self.generate_docs(doc_ops="expiry")
            task = self.loadgen_docs(scope=CbServer.default_scope,
                                     collection=collection,
                                     _sync=False,
                                     doc_ops="expiry")
            tasks.update(task.items())
        self.sleep(self.maxttl, "Wait for docs to expire")
        # exp_pager_stime
        self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
        self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
         to kickoff")

        for task in tasks:
            self.task_manager.get_task_result(task)

        for collection in self.collections[::2]:
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.buckets[0],
                                             scope_name=CbServer.default_scope,
                                             collection_name=collection)
            self.buckets[0].scopes[CbServer.default_scope].collections.pop(collection)
            self.collections.remove(collection)
        self.sleep(self.maxttl, "Wait for docs to expire")
        self.sleep(self.exp_pager_stime, "Wait Again until exp_pager_stime for kv_purger\
         to kickoff")
        self.sleep(60, "sleep before checking ts")
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("tombstone count is {}".format(ts))
        self.bucket_util._run_compaction(self.cluster)
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("tombstone count after compaction is {}".format(ts))
        expected_ts = self.vbuckets * (self.num_replicas+1) * int(self.num_collections)//2
        self.log.info("expected_ts {}".format(expected_ts))
        self.assertEqual(ts, expected_ts, "Incorrect number of tombstones found.")

    def test_failover_expired_items_in_vB(self):
        self.maxttl = 120
        self.doc_ops = "expiry"
        self.expiry_perc = self.input.param("expiry_perc", 100)

        shell_conn = RemoteMachineShellConnection(self.cluster.nodes_in_cluster[-1])
        cbstats = Cbstats(shell_conn)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)

        self.generate_docs(target_vbucket=self.target_vbucket)

        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # exp_pager_stime
        self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
        self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
         to kickoff")
        self.sleep(self.exp_pager_stime*10, "Wait for KV purger to scan expired docs and add \
        tombstones.")

        self.task.async_failover(self.cluster.nodes_in_cluster,
                                 self.cluster.nodes_in_cluster[-1],
                                 graceful=True)

        self.nodes = self.rest.node_statuses()
        self.task.rebalance(self.cluster.nodes_in_cluster,
                            to_add=[],
                            to_remove=[self.cluster.nodes_in_cluster[-1]])

        # Metadata Purge Interval
        self.meta_purge_interval = 60
        self.bucket_util.cbepctl_set_metadata_purge_interval(
            self.cluster, self.buckets, value=self.meta_purge_interval)
        self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
        tomb-stones from storage")

        self.log.info("Starting compaction for each bucket")
        self.bucket_util._run_compaction(self.cluster)

        # All docs and tomb-stone should be dropped from the storage
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after full compaction: {}".format(ts))

    def test_random_update_expire_same_docrange(self):
        pass

    def test_expiry_heavy_reads(self):
        pass

    def test_magma_purge_after_kv_purge(self):
        '''Need a way to trigger magma purging internally'''
        pass

    def test_magma_purge_before_kv_purge(self):
        '''Need a way to trigger magma purging internally'''
        pass

    def test_expire_data_key_tree(self):
        self.doc_size = 32
        self.test_expiry()
        seqTree_update = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)[-1]
        self.log.info("For upsert_size > 32 seqIndex usage-{}\
        ".format(seqTree_update))

    def test_random_key_expiry(self):
        self.key = "random_key"
        self.test_random_expiry()

    def test_expire_cold_data(self):
        '''Ensure that data expiring is from level 4'''
        pass

    def test_update_cold_data_with_ttl(self):
        '''Ensure that data expiring is from level 4'''
        pass

    def test_full_compaction_before_metadata_purge_interval(self):
        # if i am doing full compaction and there are expired items present
        # then such items will be converted into tombstones and on next compaction
        # those tombstones will be dropped if they satisfy above condition.
        self.doc_ops = "expiry"
        self.generate_docs(doc_ops="expiry")
        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.sleep(self.maxttl, "Wait for docs to expire")

        self.log.info("Starting compaction for each bucket")
        self.bucket_util._run_compaction(self.cluster)

        # All docs and tomb-stone should be dropped from the storage
        ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info("Tombstones after full compaction: {}".format(ts))

        self.assertTrue(1 >= ts, "Incorrect tombstone count in storage,\
        Expected: {}, Found: {}".format("<=1", ts))

    def test_items_partially_greater_than_purge_interval(self):
        # Only tombstones greater than purge interval will be dropped.
        # remaining expired items should get converted to tombstones
        pass
