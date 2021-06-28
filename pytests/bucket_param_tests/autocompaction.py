import time
import json
import datetime
from threading import Thread, Event

from BucketLib.BucketOperations import BucketHelper
from Cb_constants import DocLoading
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import doc_generator
from memcached.helper.data_helper import MemcachedClientHelper, \
                                         VBucketAwareMemcached
from testconstants import MIN_COMPACTION_THRESHOLD
from BucketLib.bucket import Bucket
from bucket_collections.collections_base import CollectionBase
from cb_tools.cbstats import Cbstats


class AutoCompactionTests(CollectionBase):
    def setUp(self):
        super(AutoCompactionTests, self).setUp()
        self.key = self.input.param("key", "test_collections")
        self.is_crashed = Event()
        self.autocompaction_value = self.input.param("autocompaction_value", 0)
        self.during_ops = self.input.param("during_ops", None)
        self.load_spec = self.input.param("load_spec", None)
        self.gen_load = doc_generator(self.key, 0, self.num_items,
                                      doc_size=self.doc_size,
                                      doc_type=self.doc_type)
        self.gen_update = doc_generator(self.key, 0, (self.num_items/2),
                                        doc_size=self.doc_size,
                                        doc_type=self.doc_type,
                                        mutation_type="update")
        self.stop_loading_thread = False
        self.bucket = self.cluster.buckets[0]
        self.log.info("======= Finished Autocompaction base setup =========")

    def tearDown(self):
        super(AutoCompactionTests, self).tearDown()

    @staticmethod
    def insert_key(node, bucket_name, count, size):
        rest = RestConnection(node)
        smart = VBucketAwareMemcached(rest, bucket_name)
        for i in xrange(count * 1000):
            key = "key_" + str(i)
            value = {"value": MemcachedClientHelper.create_value("*", size)}
            smart.memcached(key).set(key, 0, 0, json.dumps(value))

    def load(self, server, compaction_value, bucket_name, gen):
        self.log.info('In load, wait time is {0}'.format(self.wait_timeout))
        monitor_fragm = self.task.async_monitor_db_fragmentation(
            server, bucket_name, compaction_value)
        end_time = time.time() + self.wait_timeout * 5
        # generate load until fragmentation reached
        while not monitor_fragm.completed and not self.stop_loading_thread:
            if self.is_crashed.is_set():
                self.cluster.shutdown(force=True)
                return

            if end_time < time.time():
                self.err = "Fragmentation level is not reached in %s sec" \
                           % self.wait_timeout * 5
                return
            # update docs to create fragmentation
            try:
                for bucket in self.cluster.buckets:
                    for _, scope in bucket.scopes.items():
                        for _, collection in scope.collections.items():
                            task = self.task.async_load_gen_docs(
                                self.cluster, bucket, gen, "update", 0,
                                durability=self.durability_level,
                                timeout_secs=self.sdk_timeout,
                                batch_size=10,
                                process_concurrency=8,
                                scope=scope.name,
                                collection=collection.name,
                                sdk_client_pool=self.sdk_client_pool)
                            self.task.jython_task_manager.get_task_result(task)
            except Exception, ex:
                self.is_crashed.set()
                self.log.error("Load cannot be performed: %s" % str(ex))
        if monitor_fragm.result is False:
            self.err = "Monitor fragmentation task failed"

    def _load_all_buckets(self, generator, op_type, batch_size=10,
                          process_concurrency=8, items=0):
        for bucket in self.cluster.buckets:
            for _, scope in bucket.scopes.items():
                for _, collection in scope.collections.items():
                    task = self.task.async_load_gen_docs(
                        self.cluster, bucket, generator, op_type, 0,
                        durability=self.durability_level,
                        timeout_secs=self.sdk_timeout,
                        batch_size=batch_size,
                        process_concurrency=process_concurrency,
                        scope=scope.name,
                        collection=collection.name,
                        sdk_client_pool=self.sdk_client_pool)
                    self.task.jython_task_manager.get_task_result(task)
                    bucket.scopes[scope.name] \
                        .collections[collection.name] \
                        .num_items += items

    def test_database_fragmentation(self):
        self.err = None
        max_run = 100
        item_size = 1024
        server_info = self.servers[0]
        new_port = self.input.param("new_port", "9090")
        new_passwd = self.input.param("new_password", "new_pass")
        old_pass = self.cluster.master.rest_password

        self.bucket_util.delete_all_buckets(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        percent_threshold = self.autocompaction_value
        update_item_size = item_size * ((float(100 - percent_threshold)) / 100)

        self.log.info("Creating Rest connection to {0}".format(server_info))
        bucket_helper = BucketHelper(server_info)
        output, _ = self.bucket_util.set_auto_compaction(
            bucket_helper,
            dbFragmentThresholdPercentage=percent_threshold,
            viewFragmntThresholdPercentage=None)

        rest = RestConnection(server_info)
        items = 0
        if (output and
                MIN_COMPACTION_THRESHOLD <= percent_threshold <= max_run):
            node_ram_ratio = self.bucket_util.base_bucket_ratio(self.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio / 2
            items = (int(available_ram * 1000) / 2) / item_size

            self.bucket = Bucket({Bucket.name: self.bucket.name,
                                  Bucket.ramQuotaMB: int(available_ram),
                                  Bucket.storageBackend: self.bucket_storage,
                                  Bucket.replicaNumber: self.num_replicas})
            self.bucket_util.create_bucket(self.cluster, self.bucket)
            self.bucket_util.wait_for_memcached(server_info, self.bucket)
            self.bucket_util.wait_for_vbuckets_ready_state(server_info,
                                                           self.bucket)
            self.bucket_util.print_bucket_stats(self.cluster)

            self.log.info("Start to load {0}K keys with {1} bytes/key"
                          .format(items, item_size))
            if self.load_spec:
                doc_loading_spec = \
                    self.bucket_util.get_crud_template_from_package(
                        self.load_spec)
                self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    self.cluster.buckets,
                    doc_loading_spec,
                    mutation_num=0)
            shell_conn = RemoteMachineShellConnection(self.cluster.master)
            cb_stat = Cbstats(shell_conn)
            collection_count = cb_stat.get_collections(self.bucket)["count"]
            items = items/collection_count
            self.log.info("Start to load %s keys with %s bytes/key"
                          % (items, item_size))
            generator = doc_generator(self.key, 0, (items*1000),
                                      doc_size=int(item_size))
            self._load_all_buckets(generator, "create", items=items*1000)

            self.sleep(10, "Wait before next run")

            self.log.info("Update {0}K keys with smaller value {1} bytes/key"
                          .format(items, int(update_item_size)))
            generator_update = doc_generator(self.key, 0, (items*1000),
                                             doc_size=int(update_item_size))
            if self.during_ops:
                if self.during_ops == "change_port":
                    self.cluster_util.change_port(new_port=new_port)
                    self.cluster.master.port = new_port
                elif self.during_ops == "change_password":
                    self.cluster_util.change_password(new_password=new_passwd)
                    self.cluster.master.rest_password = new_passwd
                rest = RestConnection(self.cluster.master)

            insert_thread = Thread(target=self.load,
                                   name="insert",
                                   args=(self.cluster.master,
                                         self.autocompaction_value,
                                         self.bucket.name, generator_update))
            try:
                self.log.info('Starting the load thread')
                insert_thread.start()
                remote_client = RemoteMachineShellConnection(server_info)
                compact_run = remote_client.wait_till_compaction_end(
                    rest, self.bucket.name,
                    timeout_in_seconds=(self.wait_timeout * 10))
                remote_client.disconnect()

                if not compact_run:
                    self.fail("Auto compaction does not run")
                elif compact_run:
                    self.log.info("Auto compaction run successfully")
            except Exception as ex:
                self.log.error("Exception in auto compaction: %s" % ex)
                if self.during_ops:
                    if self.during_ops == "change_password":
                        self.cluster_util.change_password(
                            new_password=old_pass)
                    elif self.during_ops == "change_port":
                        self.cluster_util.change_port(new_port='8091',
                                                      current_port=new_port)
                self.stop_loading_thread = True
                if str(ex).find("enospc") != -1:
                    self.is_crashed.set()
                    self.log.error("Disk is out of space, unable to load data")
                else:
                    raise ex
            else:
                compaction_task = self.task.async_monitor_compaction(
                    self.cluster, self.bucket)
                insert_thread.join()
                self.task_manager.get_task_result(compaction_task)
                if self.err is not None:
                    self.fail(self.err)
        else:
            self.log.error("Unknown error")
        if self.during_ops:
            if self.during_ops == "change_password":
                self.cluster_util.change_password(new_password=old_pass)
            elif self.during_ops == "change_port":
                self.cluster_util.change_port(new_port='8091',
                                              current_port=new_port)

        self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, items*1000)

    def rebalance_in_with_DB_compaction(self):
        self.bucket_util.disable_compaction(self.cluster,
                                            bucket=self.bucket.name)
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        self._monitor_DB_fragmentation(self.bucket)
        servs_in = self.servers[self.nodes_init:self.nodes_in + 1]
        rebalance = self.task.async_rebalance(
                                [self.cluster.master], servs_in, [])
        self.sleep(5)
        compaction_task = self.task.async_compact_bucket(
                                self.cluster.master, self.bucket)
        result = compaction_task.result(self.wait_timeout * 5)
        self.assertTrue(result, "Compaction didn't finished correctly. "
                                "Please check diags")
        self.task.jython_task_manager.get_task_result(rebalance)
        self.bucket_util.verify_cluster_stats(
            self.cluster, self.servers[:self.nodes_in + 1])

    def rebalance_in_with_auto_DB_compaction(self):
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        bucket_helper = BucketHelper(self.cluster.master)
        self.bucket_util.set_auto_compaction(
            bucket_helper,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            bucket=self.bucket.name)
        compaction_task = self.task.async_monitor_compaction(self.cluster,
                                                             self.bucket)
        self._monitor_DB_fragmentation(self.bucket)
        bucket_obj = self.bucket_util.get_bucket_obj(
            self.cluster.buckets, self.bucket.name)
        collections = self.bucket_util.get_random_collections(
            [bucket_obj], 1, 1, 1)
        scope_dict = collections[self.bucket.name]["scopes"]
        scope_name = scope_dict.keys()[0]
        collection_name = scope_dict[scope_name]["collections"].keys()[0]
        doc_update_task = self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_update,
            op_type=DocLoading.Bucket.DocOps.UPDATE,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=300,
            process_concurrency=4,
            scope=scope_name,
            collection=collection_name,
            sdk_client_pool=self.sdk_client_pool)
        servs_in = self.servers[self.nodes_init:self.nodes_init+self.nodes_in]
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init],
            servs_in, [])
        self.task_manager.get_task_result(compaction_task)
        self.task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance failed with compaction")
        monitor_fragm = self.task.async_monitor_db_fragmentation(
            self.cluster.master, self.bucket.name, 0)
        self.task.jython_task_manager.get_task_result(monitor_fragm)
        doc_update_task.end_task()
        self.task_manager.get_task_result(doc_update_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
        remote_client.disconnect()

        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.validate_test_failure()

    def rebalance_out_with_DB_compaction(self):
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        self.bucket_util.disable_compaction(self.cluster,
                                            bucket=self.bucket.name)
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        self._monitor_DB_fragmentation(self.bucket)
        servs_out = [self.servers[self.num_servers - i - 1]
                     for i in range(self.nodes_out)]
        rebalance = self.task.async_rebalance([self.cluster.master], [],
                                              servs_out)
        compaction_task = self.task.async_compact_bucket(
            self.cluster.master, self.bucket.name)
        result = compaction_task.result(self.wait_timeout * 5)
        self.assertTrue(result, "Compaction didn't finished correctly. "
                                "Please check diags")
        self.task.jython_task_manager.get_task_result(rebalance)
        self.bucket_util.verify_cluster_stats(
            self.cluster, self.servers[:self.num_servers - self.nodes_out])

        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.validate_test_failure()

    def rebalance_out_with_auto_DB_compaction(self):
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])
        self.bucket_util.set_auto_compaction(
            rest,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            bucket=self.bucket.name)
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        self._monitor_DB_fragmentation(self.bucket)
        servs_out = [self.servers[self.num_servers - i - 1]
                     for i in range(self.nodes_out)]
        rebalance = self.task.async_rebalance([self.cluster.master],
                                              [], servs_out)
        compact_run = remote_client.wait_till_compaction_end(
            rest,
            self.bucket.name,
            timeout_in_seconds=(self.wait_timeout*5))
        self.task.jython_task_manager.get_task_result(rebalance)
        monitor_fragm = self.task.async_monitor_db_fragmentation(
            self.cluster.master, self.bucket.name, 0)
        result = monitor_fragm.result()
        if compact_run:
            self.log.info("auto compaction ran successfully")
        elif result:
            self.log.info("Compaction is already completed")
        else:
            self.fail("auto compaction does not run")
        self.bucket_util.verify_cluster_stats(
            self.cluster,
            self.servers[:self.num_servers - self.nodes_out])
        remote_client.disconnect()
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.validate_test_failure()

    def rebalance_in_out_with_DB_compaction(self):
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                        "ERROR: Not enough nodes to do rebalance in and out")
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1]
                     for i in range(self.nodes_out)]
        result_nodes = set(servs_init + servs_in) - set(servs_out)
        self.bucket_util.disable_compaction(self.cluster,
                                            bucket=self.bucket.name)
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        rebalance = self.task.async_rebalance(servs_init, servs_in, servs_out)
        while rebalance.state != "FINISHED":
            self._monitor_DB_fragmentation(self.bucket)
            compaction_task = self.cluster.async_compact_bucket(
                    self.cluster.master, self.bucket.name)
            result = compaction_task.result(self.wait_timeout * 5)
            self.assertTrue(result, "Compaction didn't finished correctly. "
                                    "Please check diags")
        self.task.jython_task_manager.get_task_result(rebalance)
        self.bucket_util.verify_cluster_stats(self.cluster, result_nodes)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.validate_test_failure()

    def rebalance_in_out_with_auto_DB_compaction(self):
        bucket_helper = BucketHelper(self.cluster.master)
        self.assertTrue(
            self.num_servers > self.nodes_in + self.nodes_out,
            "ERROR: Not enough nodes to do rebalance in and out")
        servs_init = self.servers[:self.nodes_init]
        servs_in = [self.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        servs_out = [self.servers[self.nodes_init - i - 1]
                     for i in range(self.nodes_out)]
        self.bucket_util.set_auto_compaction(
            bucket_helper,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            bucket=self.bucket.name)
        compaction_task = self.task.async_monitor_compaction(self.cluster,
                                                             self.bucket)
        self._monitor_DB_fragmentation(self.bucket)
        bucket_obj = self.bucket_util.get_bucket_obj(
            self.cluster.buckets, self.bucket.name)
        collections = self.bucket_util.get_random_collections(
            [bucket_obj], 1, 1, 1)
        scope_dict = collections[self.bucket.name]["scopes"]
        scope_name = scope_dict.keys()[0]
        collection_name = scope_dict[scope_name]["collections"].keys()[0]
        doc_update_task = self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_update,
            op_type=DocLoading.Bucket.DocOps.UPDATE,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=300,
            process_concurrency=4,
            scope=scope_name,
            collection=collection_name,
            sdk_client_pool=self.sdk_client_pool)
        rebalance = self.task.async_rebalance(servs_init,
                                              servs_in, servs_out,
                                              check_vbucket_shuffling=False)
        self.task_manager.get_task_result(compaction_task)
        self.task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance failed with compaction")
        monitor_fragm = self.task.async_monitor_db_fragmentation(
            self.cluster.master, self.bucket.name, 0)
        self.task.jython_task_manager.get_task_result(monitor_fragm)
        doc_update_task.end_task()
        self.task_manager.get_task_result(doc_update_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.validate_test_failure()

    def test_database_time_compaction(self):
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        curr_time = datetime.datetime.now()
        from_time = curr_time + datetime.timedelta(hours=1)
        to_time = curr_time + datetime.timedelta(hours=10)
        self.bucket_util.set_auto_compaction(
            rest,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            allowedTimePeriodFromHour=from_time.hour,
            allowedTimePeriodFromMin=from_time.minute,
            allowedTimePeriodToHour=to_time.hour,
            allowedTimePeriodToMin=to_time.minute,
            allowedTimePeriodAbort="false")
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        self._monitor_DB_fragmentation(self.bucket)
        for i in xrange(10):
            active_tasks = self.cluster.async_monitor_active_task(
                self.cluster.master,
                "bucket_compaction",
                "bucket",
                wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
                self.sleep(2)
        curr_time = datetime.datetime.now()
        # Need to make it configurable
        new_time = curr_time + datetime.timedelta(minutes=5)
        self.bucket_util.set_auto_compaction(
            rest,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            allowedTimePeriodFromHour=curr_time.hour,
            allowedTimePeriodFromMin=curr_time.minute,
            allowedTimePeriodToHour=new_time.hour,
            allowedTimePeriodToMin=new_time.minute,
            allowedTimePeriodAbort="false")
        compact_run = remote_client.wait_till_compaction_end(
            rest,
            self.bucket.name,
            timeout_in_seconds=(self.wait_timeout * 5))
        if compact_run:
            self.log.info("auto compaction run successfully")
        else:
            self.fail("auto compaction does not run")
        remote_client.disconnect()

    def rebalance_in_with_DB_time_compaction(self):
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        curr_time = datetime.datetime.now()
        from_time = curr_time + datetime.timedelta(hours=1)
        to_time = curr_time + datetime.timedelta(hours=24)
        self.bucket_util.set_auto_compaction(
            rest,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            allowedTimePeriodFromHour=from_time.hour,
            allowedTimePeriodFromMin=from_time.minute,
            allowedTimePeriodToHour=to_time.hour,
            allowedTimePeriodToMin=to_time.minute,
            allowedTimePeriodAbort="false")
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        self._monitor_DB_fragmentation(self.bucket)
        for i in xrange(10):
            active_tasks = self.cluster.async_monitor_active_task(
                self.cluster.master,
                "bucket_compaction",
                "bucket",
                wait_task=False)
            for active_task in active_tasks:
                result = active_task.result()
                self.assertTrue(result)
                self.sleep(2)
        curr_time = datetime.datetime.now()
        # Need to make it configurable
        new_time = curr_time + datetime.timedelta(minutes=5)
        self.bucket_util.set_auto_compaction(
            rest,
            dbFragmentThresholdPercentage=self.autocompaction_value,
            allowedTimePeriodFromHour=curr_time.hour,
            allowedTimePeriodFromMin=curr_time.minute,
            allowedTimePeriodToHour=new_time.hour,
            allowedTimePeriodToMin=new_time.minute,
            allowedTimePeriodAbort="false")
        servs_in = self.servers[self.nodes_init:self.nodes_in + 1]
        rebalance = self.task.async_rebalance([self.cluster.master],
                                              servs_in, [])
        compact_run = remote_client.wait_till_compaction_end(
            rest,
            self.bucket.name,
            timeout_in_seconds=self.wait_timeout*5)
        self.task.jython_task_manager.get_task_result(rebalance)
        if compact_run:
            self.log.info("auto compaction run successfully")
        else:
            self.fail("auto compaction does not run")
        remote_client.disconnect()

    def test_database_size_compaction(self):
        rest = RestConnection(self.cluster.master)
        percent_threshold = self.autocompaction_value * 1048576
        self.bucket_util.set_auto_compaction(
            rest,
            dbFragmentThreshold=percent_threshold)
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        end_time = time.time() + self.wait_timeout * 5
        monitor_fragm = self.task.async_monitor_disk_size_fragmentation(
                                    self.cluster.master, percent_threshold,
                                    self.bucket.name)
        while monitor_fragm.state != "FINISHED":
            if end_time < time.time():
                self.fail("Fragmentation level is not reached in %s sec"
                          % self.wait_timeout * 5)
            try:
                monitor_fragm = \
                    self.task.async_monitor_disk_size_fragmentation(
                        self.cluster.master, percent_threshold,
                        self.bucket.name)
                self._load_all_buckets(self.gen_update, "update")
                active_tasks = self.task.async_monitor_active_task(
                                    self.cluster.master, "bucket_compaction",
                                    "bucket", wait_task=False)
                for active_task in active_tasks:
                    result = active_task.result()
                    self.assertTrue(result)
                    self.sleep(2)
            except Exception, ex:
                self.log.error("Load cannot be performed: %s" % str(ex))
                self.fail(ex)
        monitor_fragm.result()

    def test_start_stop_DB_compaction(self):
        rest = RestConnection(self.cluster.master)
        self.log.info('Disabling auto-compaction')
        self.bucket_util.disable_compaction(self.cluster,
                                            bucket=self.bucket.name)
        self._monitor_DB_fragmentation(self.bucket)
        compaction_monitor_task = self.task.async_monitor_compaction(
            self.cluster, self.bucket)
        self.log.info('Async compact the bucket')
        compaction_task = self.task.async_compact_bucket(
            self.cluster.master, self.bucket)
        self.log.info("Wait for compaction task to start running")
        while compaction_monitor_task.status != "RUNNING":
            pass

        self.log.info('Cancel bucket compaction')
        self._cancel_bucket_compaction(rest, self.bucket)
        self.task_manager.stop_task(compaction_task)
        self.task_manager.stop_task(compaction_monitor_task)

        self.log.info('Start compaction again')
        compaction_task = self.task.async_compact_bucket(
            self.cluster.master, self.bucket)
        self.log.info('Waiting for compaction to end')
        self.task_manager.get_task_result(compaction_task)
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        compact_run = remote_client.wait_till_compaction_end(
            rest,
            self.bucket.name,
            timeout_in_seconds=self.wait_timeout)
        remote_client.disconnect()
        if compact_run:
            self.log.info("Compaction run successfully")
        else:
            self.fail("Compaction does not run")

    def test_large_file_version(self):
        # Created for MB-14976 - We need more than 65536 file revisions
        # to trigger this problem.
        compact_run = False
        rest = RestConnection(self.cluster.master)
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        remote_client.extract_remote_info()

        self.bucket_util.disable_compaction(self.cluster,
                                            bucket=self.bucket.name)
        self._monitor_DB_fragmentation(self.bucket)

        # Rename here and restart Couchbase server
        remote_client.stop_couchbase()
        self.sleep(5)
        remote_client.execute_command(
            "cd /opt/couchbase/var/lib/couchbase/data/default; "
            "rename .1 .65535 *.1")
        remote_client.execute_command(
            "cd /opt/couchbase/var/lib/couchbase/data/default; "
            "rename .2 .65535 *.2")
        remote_client.start_couchbase()

        for _ in range(5):
            self.log.info("starting a compaction iteration")
            compaction_task = self.cluster.async_compact_bucket(
                self.cluster.master, self.bucket.name)

            compact_run = remote_client.wait_till_compaction_end(
                rest,
                self.bucket.name,
                timeout_in_seconds=self.wait_timeout)
            _ = compaction_task.result(self.wait_timeout)

        if compact_run:
            self.log.info("Auto compaction run successfully")
        else:
            self.fail("Auto compaction does not run")
        remote_client.disconnect()

    def test_start_stop_auto_DB_compaction(self):
        rest = RestConnection(self.cluster.master)
        bucket_helper = BucketHelper(self.cluster.master)
        self.bucket_util.set_auto_compaction(
            bucket_helper,
            dbFragmentThresholdPercentage=self.autocompaction_value)

        compaction_monitor_task = self.task.async_monitor_compaction(
            self.cluster, self.bucket)

        doc_update_task = self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_update,
            op_type="update",
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=4)

        self.log.info("Wait for bucket compaction to start")
        while compaction_monitor_task.status != "RUNNING":
            pass

        self.log.info("Stopping doc-update task")
        doc_update_task.end_task()

        self.log.info("Stop bucket compaction")
        self._cancel_bucket_compaction(rest, self.bucket)

        self.task_manager.get_task_result(doc_update_task)
        self.task_manager.get_task_result(compaction_monitor_task)
        self._load_all_buckets(self.gen_update, "update")

        if self.is_crashed.is_set():
            self.fail("Error occurred during test run")

    def _cancel_bucket_compaction(self, rest, bucket):
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        bucket_helper = BucketHelper(self.cluster.master)
        try:
            result = bucket_helper.cancel_bucket_compaction(bucket.name)
            self.assertTrue(result)
            remote_client.wait_till_compaction_end(rest,
                                                   self.bucket.name,
                                                   self.wait_timeout)
        except Exception, ex:
            self.is_crashed.set()
            self.log.error("Failed to cancel compaction: %s" % str(ex))
        remote_client.disconnect()

    def test_auto_compaction_with_multiple_buckets(self):
        remote_client = RemoteMachineShellConnection(self.cluster.master)
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            if bucket.name == "default":
                self.bucket_util.disable_compaction(self.cluster,
                                                    bucket=bucket.name)
            else:
                self.bucket_util.set_auto_compaction(
                    rest,
                    dbFragmentThresholdPercentage=self.autocompaction_value,
                    bucket=bucket.name)

        # Load bucket with docs with updated compaction value
        self._load_all_buckets(self.gen_load, "create", items=self.num_items)
        end_time = time.time() + self.wait_timeout * 30
        for bucket in self.cluster.buckets:
            self.cluster_util._monitor_DB_fragmentation(bucket)
            monitor_fragm = self.task.async_monitor_db_fragmentation(
                self.cluster.master, bucket.name, self.autocompaction_value)
            while monitor_fragm.state != "FINISHED":
                if end_time < time.time():
                    self.fail("Fragmentation level is not reached in %s sec"
                              % self.wait_timeout * 30)
                try:
                    self._load_all_buckets(self.gen_update, "update")
                except Exception, ex:
                    self.log.error("Load cannot be performed: %s" % str(ex))
                    self.fail(ex)
            self.task.jython_task_manager.get_task_result(monitor_fragm)
            compact_run = remote_client.wait_till_compaction_end(
                rest, bucket.name, timeout_in_seconds=(self.wait_timeout * 5))
            if compact_run:
                self.log.info("auto compaction run successfully")
        remote_client.disconnect()

    def _monitor_DB_fragmentation(self, bucket):
        monitor_fragm = self.task.async_monitor_db_fragmentation(
            self.cluster.master,
            bucket.name,
            self.autocompaction_value)
        end_time = time.time() + self.wait_timeout * 30
        failure_msg = None
        bucket_obj = self.bucket_util.get_bucket_obj(
                    self.cluster.buckets, bucket.name)
        collections = self.bucket_util.get_random_collections(
                                [bucket_obj], 1, 1, 1)
        scope_dict = collections[self.bucket.name]["scopes"]
        scope_name = scope_dict.keys()[0]
        collection_name = scope_dict[scope_name]["collections"].keys()[0]

        doc_update_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, self.gen_update,
            op_type=DocLoading.Bucket.DocOps.UPDATE,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=300,
            process_concurrency=4,
            scope=scope_name,
            collection=collection_name,
            sdk_client_pool=self.sdk_client_pool)

        while monitor_fragm.completed is False:
            if end_time < time.time():
                failure_msg = "Fragmentation level is not reached in %s sec" \
                              % self.wait_timeout * 30
                break
        doc_update_task.end_task()
        self.task_manager.get_task_result(doc_update_task)

        if failure_msg is not None:
            self.task_manager.stop_task(monitor_fragm)
            self.fail(failure_msg)
        self.task_manager.get_task_result(monitor_fragm)
        if doc_update_task.fail:
            self.fail("Failures during doc updates")
