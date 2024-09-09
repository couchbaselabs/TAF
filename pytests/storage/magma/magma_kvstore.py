import os
import threading
import time

from cb_constants.CBServer import CbServer
from magma_base import MagmaBaseTest
from sdk_exceptions import SDKException
from shell_util.remote_connection import RemoteMachineShellConnection


class KVStoreTests(MagmaBaseTest):
    def setUp(self):
        super(KVStoreTests, self).setUp()
        self.crash_memcached = self.input.param("crash_memcached", False)
        self.graceful = self.input.param("graceful", False)
        self.crash_th = None

    def tearDown(self):
        super(KVStoreTests, self).tearDown()

    def crash(self, nodes=None, kill_itr=2, graceful=False):
        nodes = nodes or self.cluster.nodes_in_cluster
        self.stop_crash = False
        count = kill_itr
        loop_itr = 0
        connections = dict()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            connections.update({node: shell})

        while not self.stop_crash:
            loop_itr += 1
            self.log.info("Iteration:{} to kill memcached on all nodes".format(loop_itr))
            for node, shell in connections.items():
                if "kv" in node.services:
                    if graceful:
                        shell.restart_couchbase()
                    else:
                        while count > 0:
                            shell.kill_memcached()
                            self.sleep(5, "Sleep before killing memcached on same node again.")
                            count -= 1
                        count = kill_itr

            for _, shell in connections.items():
                shell.disconnect()

    def loadgen_docs_per_bucket(self, bucket,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     skip_read_on_error=True,
                     suppress_error_table=True,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection,
                     _sync=True,
                     track_failures=False,
                     doc_ops=None):
        doc_ops = doc_ops or self.doc_ops

        tasks_info = dict()
        tem_tasks_info = dict()
        read_tasks_info = dict()
        read_task = False

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)

        if "update" in doc_ops and self.gen_update is not None:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_update,  "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "update", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "create" in doc_ops and self.gen_create is not None:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "create", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "expiry" in doc_ops and self.gen_expiry is not None and self.maxttl:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_expiry, "update", self.maxttl,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "update", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_expiry.end - self.gen_expiry.start)
        if "read" in doc_ops and self.gen_read is not None:
            read_tasks_info = self.bucket_util.async_validate_docs(
               self.cluster, bucket, self.gen_read, "read", 0,
               batch_size=self.batch_size,
               process_concurrency=self.process_concurrency,
               timeout_secs=self.sdk_timeout,
               retry_exceptions=retry_exceptions,
               ignore_exceptions=ignore_exceptions,
               scope=scope,
               collection=collection)
            read_task = True
        if "delete" in doc_ops and self.gen_delete is not None:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_delete, "delete", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "delete", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if _sync:
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster, load_using=self.load_docs_using)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        if read_task:
            # TODO: Need to converge read_tasks_info into tasks_info before
            #       itself to avoid confusions during _sync=False case
            tasks_info.update(read_tasks_info.items())
            if _sync:
                for task in read_tasks_info:
                    self.task_manager.get_task_result(task)

        return tasks_info

    def test_create_delete_bucket_n_times(self):
        """
        Test Focus: Create and Delete bucket multiple times.

                    Since buckets are already created in magma base
                    we'll start by deleting the buckets, then will recreate

        STEPS:
             -- Doc ops on bucket which we'll not be deleting(optional)
             -- Delete already exisiting buckets
             -- Recreate new buckets
             -- Doc ops on buckets
             -- Repaeat all the above steps
        """
        self.log.info("=====test_create_delete_bucket_n_times starts=====")
        count = 0
        self.num_delete_buckets = self.input.param("num_delete_buckets", 1)

        '''
        Sorting bucket list
        '''
        bucket_lst = []
        for bucket in self.cluster.buckets:
            bucket_lst.append((bucket, bucket.name))
        bucket_lst = sorted(bucket_lst, key = lambda x : x[-1])
        self.log.debug ("bucket list is {} ".format(bucket_lst))
        bucket_ram_quota = bucket_lst[0][0].ramQuotaMB
        self.log.debug("ram_quota is {}".format(bucket_ram_quota))

        scope_name = CbServer.default_scope
        start = self.init_items_per_collection

        while count < self.test_itr:
            self.log.info("Iteration=={}".format(count+1))
            '''
            Step 1
              -- Doc loading to buckets, which will not be getting deleted
              -- This step is optional.
              -- In case we pass parameter to delete all buckets.
                  it will be skipped
            '''
            if self.num_delete_buckets <  self.standard_buckets:
                self.compute_docs_ranges(start)
                self.generate_docs(doc_ops=self.doc_ops, target_vbucket=None)
                tasks_info = dict()
                for bucket, _ in bucket_lst[self.num_delete_buckets:]:
                    self.log.debug("Iteration=={}, Bucket=={}".format(count+1, bucket.name))
                    for collection in self.collections:
                        tem_tasks_info = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                      self.ignore_exceptions,
                                                                      scope=scope_name,
                                                                      collection=collection,
                                                                      _sync=False,
                                                                      doc_ops=self.doc_ops)
                        tasks_info.update(tem_tasks_info.items())
            '''
            Step 2
             -- Deletion of buckets
            '''

            for bucket, _ in bucket_lst[:self.num_delete_buckets]:
                self.log.info("Iteration=={}, Deleting bucket=={}".format(count+1, bucket.name))
                self.bucket_util.delete_bucket(self.cluster, bucket)
                self.sleep(30, "waiting for 30 seconds after deletion of bucket")

            if self.num_delete_buckets <  self.standard_buckets:
                for task in tasks_info:
                    self.task_manager.get_task_result(task)
                if "create" in self.doc_ops:
                    start = self.create_end
                    self.log.info("Iteration=={}, start=={}".format(count+1, start))
            '''
            Step 3
            -- Recreate docs which got deleted/expired
            -- This step will also be optional
            '''

            self.gen_create = None
            self.gen_update = None

            if "delete" in self.doc_ops and "expriy" in self.doc_ops:
                self.create_start = self.delete_start
                self.create_end = self.delete_end
                self.update_start = self.expiry_start
                self.update_end = self.expiry_end
                self.generate_docs(doc_ops="create:update", target_vbucket=None)
            elif "expiry" in self.doc_ops:
                self.update_start = self.expiry_start
                self.update_end = self.expiry_end
                self.generate_docs(doc_ops="update", target_vbucket=None)
            elif "delete" in self.doc_ops:
                self.create_start = self.delete_start
                self.create_end = self.delete_end
                self.generate_docs(doc_ops="create", target_vbucket=None)

            if self.gen_create is not None or self.gen_update is not None:
                tasks_in = dict()
                for bucket in self.cluster.buckets:
                    self.log.debug("Iteration=={}, Loading earlier deleted/expirted items in bucket=={}"
                                   .format(count+1, bucket.name))
                    for collection in self.collections:
                        tem_tasks_in = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                      self.ignore_exceptions,
                                                                      scope=scope_name,
                                                                      collection=collection,
                                                                      _sync=False,
                                                                      doc_ops="create:update")
                    tasks_in.update(tem_tasks_in.items())
                for task in tasks_in:
                    self.task_manager.get_task_result(task)

            '''
            Step 4
            -- Bucket recreation steps
            '''

            if self.crash_memcached:
                self.crash_th = \
                    threading.Thread(target=self.crash,
                                     kwargs=dict(graceful=self.graceful))
                self.crash_th.start()

            buckets_created = self.bucket_util.create_multiple_buckets(
                self.cluster, self.num_replicas,
                bucket_count=self.num_delete_buckets,
                bucket_type=self.bucket_type,
                storage={"couchstore": 0,
                         "magma": self.num_delete_buckets},
                eviction_policy=self.bucket_eviction_policy,
                ram_quota=bucket_ram_quota,
                bucket_name=self.bucket_name)

            if self.crash_memcached:
                self.stop_crash = True
                self.crash_th.join()

            if self.crash_th and self.crash_th.is_alive():
                self.crash_th.join()

            if not buckets_created:
                buckets_created = self.bucket_util.create_multiple_buckets(
                    self.cluster, self.num_replicas,
                    bucket_count=self.num_delete_buckets,
                    bucket_type=self.bucket_type,
                    storage={"couchstore": 0,
                             "magma": self.num_delete_buckets},
                    eviction_policy=self.bucket_eviction_policy,
                    ram_quota=bucket_ram_quota,
                    bucket_name=self.bucket_name)

            self.assertTrue(buckets_created, "Unable to create multiple buckets after bucket deletion")

            for bucket in self.cluster.buckets:
                ready = self.bucket_util.wait_for_memcached(
                    self.cluster.master,
                    bucket)
                self.assertTrue(ready, msg="Wait_for_memcached failed")

            task_info = dict()
            bucket_lst = []
            for bucket in self.cluster.buckets:
                bucket_lst.append((bucket, bucket.name))
                bucket_lst = sorted(bucket_lst, key = lambda x : x[-1])
            self.log.debug("Iteration=={}, Bucket list after recreation of bucket =={} ".
                           format(count+1, bucket_lst))

            '''
            Step 5
            -- Doc loading in recreated buckets
            '''
            self.generate_docs(create_end=self.init_items_per_collection,
                               create_start=0,
                               doc_ops="create",
                               target_vbucket=None)
            for bucket, _ in bucket_lst[:self.num_delete_buckets]:
                self.log.info("Iteration=={}, doc loading  to bucket=={}".format(count+1, bucket.name))
                for collection in self.collections:
                    tem_task_info = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                 self.ignore_exceptions,
                                                                 scope=scope_name,
                                                                 collection=collection,
                                                                 _sync=False,
                                                                 doc_ops="create")
                    task_info.update(tem_task_info.items())

            for task in task_info:
                self.task_manager.get_task_result(task)
            count += 1

    def test_cbstats_slow_getStats_ops(self):

        def run_cbstats_loop(server, cmd, iter):
            cbstats_shell = RemoteMachineShellConnection(server)
            self.PrintStep("Running cbstats all command in a loop")
            for i in range(iter):
                self.log.info("Executing cbstats command iteration: {}".format(i+1))
                cbstats_shell.execute_command(cmd)
                time.sleep(1)
            cbstats_shell.disconnect()

        bucket = self.cluster.buckets[0]
        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                               self.cluster.nodes_in_cluster)
        cbstats_iter = self.input.param("cbstats_iter", 300)
        data_load_items = self.input.param("data_load_items", 15000000)
        max_getStats_time = self.input.param("max_getStats_time", 10000) # unit in microSeconds

        self.create_start = 0
        self.create_end = data_load_items
        self.PrintStep("Inserting {} items in each collection".format(self.create_end))
        self.new_loader()

        # Running cbstats commands in parallel during data load
        cbstats_cmd = "/opt/couchbase/bin/cbstats {0}:11210 -u Administrator -p password -b {1} all" \
                        .format(self.cluster.master.ip, bucket.name)
        run_cbstats_loop(self.cluster.master, cbstats_cmd, cbstats_iter)

        self.doc_loading_tm.getAllTaskResult()
        self.printOps.end_task()

        self.bucket_util.print_bucket_stats(self.cluster)

        max_getStats_times = self.parse_logs_stat_histogram(self.cluster.master, "getStatsTime")
        for time_taken in max_getStats_times:
            err_msg = "Slow getStats operations with duration {} microseconds".format(time_taken)
            self.assertTrue(time_taken < max_getStats_time, err_msg)
        self.log.info("No slow getStats operations were found")


    def parse_logs_stat_histogram(self, server, stat_name):

        logs_path = "/cb_logs"
        file_name = "stats.log"

        self.PrintStep("Parsing stat '{0}' histogram in {1}".format(stat_name, file_name))

        shell = RemoteMachineShellConnection(server)

        # Install unzip on node
        shell.execute_command("apt-get install unzip")

        # Create a directory to store cbcollect_info logs
        shell.execute_command("mkdir {}".format(logs_path))

        # Trigger cbcollect
        self.log.info("Collecting cbcollect_info logs")
        shell.execute_command("/opt/couchbase/bin/cbcollect_info {}/logs1.zip".format(logs_path))
        shell.execute_command("unzip {0}/logs1.zip -d {1}".format(logs_path, logs_path))

        # Identify the unzipped cbcollect_info folder
        o, e = shell.execute_command("ls {}".format(logs_path))
        for path in o:
            if "zip" not in path:
                cb_logs_folder = path[:-1].strip()

        complete_path = os.path.join(logs_path, cb_logs_folder, file_name)

        # Grep for lines containing the stat keyword
        grep_cmd = "grep -n -i '{0}' {1}".format(stat_name, complete_path)
        grep_output, e = shell.execute_command(grep_cmd)

        # Store file data in a variable to parse it
        lines, err = shell.execute_command("cat {}".format(complete_path))

        # Extract line numbers from grep output
        line_nums = []
        for line in grep_output:
            line_nums.append(int(line.split(":")[0].strip()))

        # Iterate from the start to the end of the histogram to find max range
        max_get_time_range = []
        for num in line_nums:
            c = num
            while 1:
                st = lines[c].split(':')[0].strip()
                if st == "Avg":
                    max_get_time_range.append(lines[c-1])
                    break
                c += 1

        max_get_times = []
        for time_range in max_get_time_range:
            range_time = time_range.split(":")[0].strip()
            max_get_times.append(int(range_time.split("-")[1].strip()[:-2]))

        self.log.info("Max getStats time duration in microSeconds = {}".format(max_get_times))

        # Delete logs directory
        shell.execute_command("rm -rf {}".format(logs_path+"/"))

        return max_get_times
