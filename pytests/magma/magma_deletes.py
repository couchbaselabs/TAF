import copy
import time

from magma_basic_crud import BasicCrudTests


class BasicDeleteTests(BasicCrudTests):
    def test_create_delete_n_times(self):
        """
        STEPS:
           -- Create n items
           -- Delete all n items
           -- Check Space Amplification
           -- Repeat above step n times
        """
        self.log.info("test_create_delete_n_times starts ")

        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"

        self.delete_start = 0
        self.delete_end = self.num_items
        self.generate_docs(doc_ops="delete")
        count = 0
        while count < self.test_itr:
            #######################################################################
            '''
            STEP - 1, Delete all the items

            '''
            self.log.debug("Step 1, Iteration= {}".format(count+1))
            self.doc_ops = "delete"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            ######################################################################
            '''
            STEP - 2
              -- Space Amplification check after
                 deleting all the items
            '''
            self.log.debug("Step 2, Iteration= {}".format(count+1))
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            time_end = time.time() + 60 * 2
            while time.time() < time_end:
                disk_usage = self.get_disk_usage(self.buckets[0],
                                                 self.cluster.nodes_in_cluster)
                _res = disk_usage[0]
                self.log.info("DeleteIteration-{}, Disk Usage at time {} is {}MB \
                ".format(count+1, time_end - time.time(), _res))
                if _res < 1 * self.disk_usage[self.disk_usage.keys()[0]]:
                    break

            msg = "Disk Usage={}MB > {} * init_Usage={}MB"
            self.assertIs(_res > 1 * self.disk_usage[
                self.disk_usage.keys()[0]], False,
                msg.format(disk_usage[0], 1,
                           self.disk_usage[self.disk_usage.keys()[0]]))
            self.run_compaction(compaction_iterations=1)
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            expected_ts_count = self.items*(self.num_replicas+1)*(count+1)
            self.log.info("Iterations == {}, Actual tomb stone count == {},\
            expected_ts_count == {}".format(count+1, ts, expected_ts_count))
            self.sleep(60, "sleep after triggering full compaction")

            # 64 byte is size of meta data
            expected_tombstone_size = float(expected_ts_count * (self.key_size+ 64)) / 1024 / 1024
            self.log.info("expected tombstone size {}".format(expected_tombstone_size))
            disk_usage_after_compaction = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)[0]
            #  1.1 factor is for 10 percent buffer on calculated tomb stone size
            expected_size = 1.1 * (expected_tombstone_size + self.disk_usage_before_loading)
            self.log.info("Iteration=={}, disk usage after compaction=={}\
            expected_size=={}".format(count+1, disk_usage_after_compaction, expected_size))
            self.assertTrue(disk_usage_after_compaction <= expected_size ,
                            "Disk size=={} after compaction exceeds expected size=={}".
                            format(disk_usage_after_compaction, expected_size))
            ######################################################################
            '''
            STEP - 3
              -- Recreate n items
            '''

            if count != self.test_itr - 1:
                self.log.debug("Step 2, Iteration= {}".format(count+1))
                self.doc_ops = "create"
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
                self.bucket_util.verify_stats_all_buckets(self.num_items)
            count += 1

            ######################################################################
        self.log.info("====test_basic_create_delete ends====")


    def test_parallel_creates_deletes(self):
        """
        STEPS:
          -- Create new items and deletes already
             existing items
          -- Check disk_usage after each Iteration

        """
        self.log.info("test_parallel_create_delete starts")
        count = 0
        init_items = copy.deepcopy(self.num_items)
        self.doc_ops = "create:delete"
        self.delete_start = 0
        self.delete_end = self.num_items
        if self.rev_del:
            self.delete_start = -int(self.num_items -1)
            self.delete_end = 1

        while count < self.test_itr:
            self.log.info("Iteration {}".format(count+1))
            self.create_start = (count+1) * init_items
            self.create_end = self.create_start + init_items

            if self.rev_write:
                self.create_end =  -int(((count+1) * init_items) - 1)
                self.create_start = -int(-(self.create_end) + init_items)
            self.log.info("Iteration={}, del_s={}, del_e={},create_s={},create_e={}".
                          format(count+1, self.delete_start, self.delete_end,
                                 self.create_start,self.create_end))

            self.generate_docs()
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            self.delete_start = (count+1) * init_items
            self.delete_end = self.delete_start + init_items
            if self.rev_del:
                self.delete_end = -int(((count+1) * init_items) - 1)
                self.delete_start = -int(-(self.delete_end) + init_items)

            disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)

            # Space Amplification Check
            msg_stats = "{} stats fragmentation exceeds configured value"
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_result, True, msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True, msg_stats.format("KV"))

            time_end = time.time() + 60 * 2
            while time.time() < time_end:
                disk_usage = self.get_disk_usage(self.buckets[0],
                                                 self.cluster.nodes_in_cluster)
                _res = disk_usage[0]
                self.log.info("Iteration-{}, Disk Usage at time {} is {}MB \
                ".format(count+1, time_end - time.time(), _res))
                if _res < 2 * self.disk_usage[self.disk_usage.keys()[0]]:
                    break

            msg = "Disk Usage={}MB > {} * init_Usage={}MB"
            self.assertIs(_res > 2 * self.disk_usage[
                self.disk_usage.keys()[0]], False,
                msg.format(disk_usage[0], 2,
                           self.disk_usage[self.disk_usage.keys()[0]]))
            ts = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
            expected_ts_count = self.items*(self.num_replicas+1)*(count+1)
            self.log.info("Iterations - {}, Actual tomb stone count == {} expected_ts_count == {}"
                          .format(count+1, ts, expected_ts_count))

            self.run_compaction(compaction_iterations=1)
            self.sleep(300, "sleep after triggering full compaction")
            disk_usage_after_compaction = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)[0]
            expected_tombstone_size = float(expected_ts_count * (self.key_size+ 64)) / 1024 / 1024
            #  1.1 factor is for 10 percent buffer on calculated tomb stone size
            expected_size = 1.1 *(self.disk_usage[self.disk_usage.keys()[0]] + expected_tombstone_size)
            self.log.info("Iteration--{}, disk usage after compaction--{}\
            expected size == {},expected_tombstone_size =={} ".
            format(count+1, disk_usage_after_compaction,
                   expected_size, expected_tombstone_size))
            self.assertTrue(disk_usage_after_compaction <=  expected_size,
                            "Disk size after compaction == {} exceeds  expected size == {}".
                            format(disk_usage_after_compaction, expected_size))
            #Space Amplifacation check Ends
            count += 1
        self.change_swap_space(self.cluster.nodes_in_cluster, disable=False)
        self.log.info("====test_parallel_create_deletes ends====")
