import copy
import threading
import time

from Cb_constants.CBServer import CbServer
import json as Json
from magma_basic_crud import BasicCrudTests
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient


class BasicUpsertTests(BasicCrudTests):
    def test_update_n_times(self):
        """
        Test Focus: Update items n times and
                    test space amplification
        STEPS:
          -- Update items n times (where n gets calculated
             from fragmentation value
          -- Check space amplification
          -- Repeat the above steps n times
          -- After all iterations validate the data
        """

        self.log.info("test_update_n_times starts")
        upsert_doc_list = self.get_fragmentation_upsert_docs_list()
        self.mutate = 0
        count = 0

        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count+1))
            #######################################################################
            '''
            STEP - 1, Update Items

            '''
            for itr in upsert_doc_list:
                self.doc_ops = "update"
                self.update_start = 0
                self.update_end = itr

                if self.rev_update:
                    self.update_start = -int(itr - 1)
                    self.update_end = 1

                self.generate_docs(doc_ops="update")
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets(timeout=3600)

            #######################################################################
            '''
            STEP - 2, Space Amplification Check

            '''
            msg = "Fragmentation value for {} stats exceeds\
            the configured value"

            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg.format("KV"))

            usage_factor = ((float(
                    self.num_items + sum(upsert_doc_list)
                    ) / self.num_items) + 0.5)
            self.log.debug("Disk usage factor = {}".format(usage_factor))

            time_end = time.time() + 60 * 2
            while time.time() < time_end:
                disk_usage = self.get_disk_usage(self.buckets[0],
                                            self.cluster.nodes_in_cluster)
                _res = disk_usage[0]
                self.log.debug("usage at time {} is {}".format((time_end - time.time()), _res))
                if _res < usage_factor * self.disk_usage[self.disk_usage.keys()[0]]:
                    break

            msg = "Iteration= {}, Disk Usage = {}MB\
            exceeds {} times from Actual disk usage = {}MB"
            self.assertIs(_res > usage_factor * self.disk_usage[
                self.disk_usage.keys()[0]],
                False, msg.format(count+1, _res, usage_factor,
                                  self.disk_usage[self.disk_usage.keys()[0]]))

            count += 1
        #######################################################################
        '''
        STEP - 3, Data Validation

        '''
        self.validate_data("update", self.gen_update)

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.log.info("====test_update_n_times ends====")

    def test_multi_update_delete(self):
        """
        STEPS:
          -- Update items x times
          -- Check space amplification
          -- Delete half of the items
          -- Check space Amplification
          -- Recreate deleted items
          -- Check Space Amplification
          -- Repeat above steps for n times
          -- After all iterations validate the data
        """
        self.log.info("==== test_multi_update_delete starts =====")

        count = 0
        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"
        msg = "{} Iteration= {}, Disk Usage = {}MB\
         exceeds 2.5 times from Actual disk usage = {}MB"

        self.mutate = 0
        for i in range(self.test_itr):
            self.log.info("Step 1, Iteration= {}".format(i+1))
            #######################################################################
            '''
            STEP - 1, Update Items, update_itr times

            '''
            while count < self.update_itr:
                self.doc_ops = "update"
                self.update_start = 0
                self.update_end = self.num_items
                if self.rev_update:
                    self.update_start = -int(self.num_items - 1)
                    self.update_end = 1

                self.generate_docs(doc_ops="update")
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)

                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets(timeout=3600)

                ###################################################################
                '''
                  STEP - 2
                   -- Space Amplification check after each update iteration.
                   -- Data validation only for last update iteration
                '''
                self.log.info("Step 2, Iteration= {}".format(i+1))
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
                    self.log.info("Update Iteration-{}, Disk Usage at time {} is {}MB \
                    ".format(count+1, time_end - time.time(), _res))
                    if _res < 2.5 * self.disk_usage[self.disk_usage.keys()[0]]:
                        break

                self.assertIs(
                    _res > 2.5 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, msg.format("update", count+1, _res,
                                      self.disk_usage[self.disk_usage.keys()[0]]))

                count += 1
            self.update_itr += self.update_itr

            if i+1 == self.test_itr:
                self.validate_data("update", self.gen_update)
            ###################################################################
            '''
            STEP - 3
              -- Delete half of the docs.
            '''

            self.log.debug("Step 3, Iteration {}".format(i+1))
            self.doc_ops = "delete"

            self.delete_start = 0
            self.delete_end = self.num_items//2
            if self.rev_del:
                self.delete_start = -int(self.num_items//2 - 1)
                self.delete_end = 1

            self.generate_docs(doc_ops="delete")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            ###################################################################
            '''
            STEP - 4
              -- Space Amplification Check after deletion.
            '''
            self.log.debug("Step 4, Iteration {}".format(i+1))
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                 self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Delete Iteration {}, Disk Usage- {}MB\
            ".format(i+1, _res))
            self.assertIs(
                _res > 2.5 * self.disk_usage[
                    self.disk_usage.keys()[0]],
                False, msg.format(
                    "delete", i+1, _res,
                    self.disk_usage[self.disk_usage.keys()[0]]))

            ###################################################################
            '''
            STEP - 5
              -- ReCreation of docs.
            '''
            self.log.debug("Step 5, Iteration= {}".format(i+1))

            self.gen_create = copy.deepcopy(self.gen_delete)
            self.doc_ops = "create"

            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            ###################################################################
            '''
            STEP - 6
              -- Space Amplification Check after Recreation.
            '''
            self.log.debug("Step 6, Iteration= {}".format(i+1))

            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Create Iteration{}, Disk Usage= {}MB \
            ".format(i+1, _res))
            self.assertIs(_res > 2.5 * self.disk_usage[
                self.disk_usage.keys()[0]],
                False, msg.format("Create", _res, i+1,
                                  self.disk_usage[self.disk_usage.keys()[0]]))

        ###################################################################
        '''
        STEP - 7
          -- Validate data
           -- Data validation is only for the creates in last iterations.
        '''
        self.log.debug("Step 7, Iteration= {}".format(i+1))
        self.validate_data("create", self.gen_create)
        self.log.info("====test_multiUpdate_delete ends====")

    def test_update_rev_update(self):
        """
        STEPS:
          -- Update num_items // 2 items.
          -- Reverse update remaining num_items // 2 items.
          -- If next.half is false skip above step
          -- And reverse update items in first point
          -- Check space amplification
          -- Repeat above steps x times
          -- Delete all the items
          -- Check space Amplification
          -- Recreate deleted items
          -- Check Space Amplification
          -- Repeat above steps for n times
          -- After all iterations validate the data
        """
        self.log.info("==== test_update_rev_update starts =====")

        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"
        msg = "{} Iteration= {}, Disk Usage = {}MB\
        exceeds {} times from Actual disk usage = {}MB"

        count = 0
        mutated = 1
        for i in range(self.test_itr):
            self.log.debug("Step 1, Iteration= {}".format(i+1))
            #######################################################################
            '''
            STEP - 1, Update Items, update_itr times
              -- Update n // 2 items
              -- If self.next_half is true
              -- Update remaining n//2 items
              -- Else, again update items in
                reverse order in first point
            '''
            while count < self.update_itr:
                tasks_info = dict()
                self.doc_ops = "update"
                self.gen_update = self.genrate_docs_basic(0, self.num_items //2,
                                                          mutate=mutated)
                tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                                   self.ignore_exceptions,
                                                   _sync=False)
                tasks_info.update(tem_tasks_info.items())
                if self.next_half:
                    start = - (self.num_items - 1)
                    end = - (self.num_items // 2 - 1)
                    self.gen_update = self.genrate_docs_basic(start, end,
                                                              mutate=mutated)
                    tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                                       self.ignore_exceptions,
                                                       _sync=False)
                    tasks_info.update(tem_tasks_info.items())

                for task in tasks_info:
                    self.task_manager.get_task_result(task)
                self.bucket_util.verify_doc_op_task_exceptions(
                    tasks_info, self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks_info)
                mutated += 1

                if not self.next_half:
                    start = - (self.num_items - 1)
                    end = - (self.num_items // 2 - 1)
                    self.gen_update = self.genrate_docs_basic(start, end,
                                                              mutate=mutated)
                    _ = self.loadgen_docs(self.retry_exceptions,
                                                       self.ignore_exceptions,
                                                       _sync=True)
                    mutated += 1

                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
                ###################################################################
                '''
                STEP - 2
                  -- Space Amplification check after each update iteration.
                '''
                self.log.debug("Step 2, Iteration= {}".format(i+1))
                _result = self.check_fragmentation_using_magma_stats(
                    self.buckets[0],
                    self.cluster.nodes_in_cluster)
                self.assertIs(_result, True,
                              msg_stats.format("magma"))

                _r = self.check_fragmentation_using_bucket_stats(
                    self.buckets[0], self.cluster.nodes_in_cluster)
                self.assertIs(_r, True,
                              msg_stats.format("KV"))

                disk_usage = self.get_disk_usage(
                    self.buckets[0], self.cluster.nodes_in_cluster)
                _res = disk_usage[0]
                self.log.info("Update Iteration- {}, Disk Usage- {}MB\
                ".format(count+1, _res))
                self.assertIs(
                    _res > 2.5 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, msg.format("update", count+1, _res, 2.5,
                                      self.disk_usage[self.disk_usage.keys()[0]]))

                count += 1
            self.update_itr += self.update_itr

            ###################################################################
            '''
            STEP - 3
              -- Delete all the items.
            '''
            self.log.debug("Step 3, Iteration {}".format(i+1))
            self.doc_ops = "delete"
            self.delete_start = 0
            self.delete_end = self.num_items
            if self.rev_del:
                self.delete_start = -int(self.num_items - 1)
                self.delete_end = 1

            self.generate_docs(doc_ops="delete")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            ###################################################################
            '''
            STEP - 4
              -- Space Amplification Check after deletion.
            '''
            self.log.debug("Step 4, Iteration {}".format(i+1))
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                 self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Delete Iteration {}, Disk Usage- {}MB\
            ".format(i+1, _res))
            self.assertIs(
                _res > 0.5 * self.disk_usage[
                    self.disk_usage.keys()[0]],
                False, msg.format(
                    "delete", i+1, _res, 0.5,
                    self.disk_usage[self.disk_usage.keys()[0]]))
            ###################################################################
            '''
            STEP - 5
              -- ReCreation of docs.
            '''
            self.log.debug("Step 5, Iteration= {}".format(i+1))
            self.gen_create = copy.deepcopy(self.gen_delete)
            self.doc_ops = "create"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            ###################################################################
            '''
            STEP - 6
              -- Space Amplification Check after Recreation.
            '''
            self.log.debug("Step 6, Iteration= {}".format(i+1))
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Create Iteration{}, Disk Usage= {}MB \
            ".format(i+1, _res))
            self.assertIs(_res > 1.5 * self.disk_usage[
                self.disk_usage.keys()[0]],
                False, msg.format("Create", _res, i+1, 1.5,
                                  self.disk_usage[self.disk_usage.keys()[0]]))

        ###################################################################
        '''
        STEP - 7
          -- Validate data
           -- Data validation is only for the creates in last iterations.
        '''
        self.log.debug("Step 7, Iteration= {}".format(i+1))
        self.validate_data("create", self.gen_create)
        self.log.info("====test_update_rev_update ends====")

    def test_update_single_doc_n_times(self):
        """
        Test Focus: Update single/same doc n times

          Note: MultiThreading is used to update
               single doc, since we are not worried
               about what should be the final mutate
               value of the document semaphores have
               been avoided. MultiThreading also speed up
               the execution of test
        """
        self.log.info("test_update_single_doc_n_times starts")
        self.doc_ops = "update"

        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)

        self.gen_update = self.genrate_docs_basic(start=0, end=1)

        key, val = self.gen_update.next()
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
            self.assertTrue(
                self.bucket_util._wait_warmup_completed(
                    [self.cluster_util.cluster.master],
                    self.bucket_util.buckets[0],
                    wait_time=self.wait_timeout * 10))

        def upsert_doc(start_num, end_num, key_obj, val_obj):
            for i in range(start_num, end_num):
                val_obj.put("mutated", i)
                self.client.upsert(key_obj, val_obj)

        threads = []
        start = 0
        end = 0
        for _ in range(10):
            start = end
            end += 100000
            th = threading.Thread(
                target=upsert_doc, args=[start, end, key, val])
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

        self.bucket_util._wait_for_stats_all_buckets(timeout=3600)

        # Space amplification check
        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"
        _result = self.check_fragmentation_using_magma_stats(
            self.buckets[0],
            self.cluster.nodes_in_cluster)
        self.assertIs(_result, True,
                      msg_stats.format("magma"))

        _r = self.check_fragmentation_using_bucket_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_r, True,
                      msg_stats.format("KV"))

        disk_usage = self.get_disk_usage(
            self.buckets[0],
            self.cluster.nodes_in_cluster)
        self.log.debug("Disk usage after updates {}".format(
            disk_usage))
        _res = disk_usage[0]
        msg = "Disk Usage = {}MB exceeds 2.2 times \
        from Actual disk usage = {}MB"
        self.assertIs(
            _res > 2.2 * self.disk_usage[
                self.disk_usage.keys()[0]],
            False,
            msg.format(_res, self.disk_usage[self.disk_usage.keys()[0]]))
        # Space amplification check ends

        success, fail = self.client.get_multi([key],
                                              self.wait_timeout)

        self.assertIs(key in success, True,
                      msg="key {} doesn't exist\
                      ".format(key))
        actual_val = dict()
        expected_val = Json.loads(val.toString())
        actual_val = Json.loads(success[key][
            'value'].toString())
        self.log.debug("Expected_val= {} and actual_val = {}\
        ".format(expected_val, actual_val))
        self.assertIs(expected_val == actual_val, True,
                      msg="expected_val-{} != Actual_val-{}\
                      ".format(expected_val, actual_val))

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.log.info("====test_update_single_doc_n_times ends====")

    def test_move_val_btwn_key_and_seq_trees(self):
        """
        Test Focus: Update items such that values moves
                    Sequence Tree and Key Trees.
        STEPS:
          -- Update items with new size , so that
             items move from sequence tree to key
             tree or vice versa
          -- Do data validation
          -- Again update items with initial size
          -- Check space amplification
          -- Again validate documents
        """
        self.log.info("test_move_val_btwn_key_and_seq_trees starts")
        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"
        count = 0
        keyTree, seqTree = (self.get_disk_usage(
                        self.buckets[0],
                        self.cluster.nodes_in_cluster)[2:4])
        self.log.debug("Disk usage after pure creates {}".format((
            self.disk_usage, keyTree, seqTree)))
        initial_doc_size = self.doc_size
        upsert_size = 0
        if self.doc_size < 32:
            upsert_size = 2048

        while count < self.test_itr:
            self.log.info("Update Iteration count == {}".format(count))
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [self.cluster_util.cluster.master],
                                self.bucket_util.buckets[0],
                                wait_time=self.wait_timeout * 10))
            #######################################################################
            '''
            STEP - 1, Update items with changed/new size
            '''
            self.log.info("Step 1, Iteration= {}".format(count+1))
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = self.num_items
            if self.rev_update:
                self.update_start = -int(self.num_items - 1)
                self.update_end = 1
            self.doc_size = upsert_size
            self.generate_docs()
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)

            if upsert_size > 32:
                seqTree_update = (self.get_disk_usage(
                        self.buckets[0],
                        self.cluster.nodes_in_cluster)[-1])
                self.log.info("For upsert_size > 32 seqIndex usage-{}\
                ".format(seqTree_update))

            #######################################################################
            '''
            STEP - 2, Validate data after initial upsert
            '''
            self.log.info("Step 2, Iteration= {}".format(count+1))
            self.validate_data("update", self.gen_update)

            #######################################################################
            '''
            STEP - 3, Updating items with changed doc size
                     to move between tress
            '''
            self.log.info("Step 3, Iteration= {}".format(count+1))
            self.doc_size = initial_doc_size
            self.generate_docs()
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)

            #######################################################################
            '''
            STEP - 4, Space Amplification Checks
            '''
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                 self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(
                self.buckets[0], self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("disk usage after upsert count {} is {}MB \
                ".format(count+1, _res))
            if self.doc_size > 32:
                self.assertIs(
                    _res > 1.5 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, "Disk Usage {} After \
                    update count {} exceeds \
                    Actual disk usage {} by 1.5 \
                    times".format(_res, count+1,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
            else:
                self.assertIs(disk_usage[3] > 0.5 * seqTree_update,
                              False, " Current seqTree usage-{} exceeds by'\n'\
                               0.5 times from the earlier '\n' \
                               seqTree usage (after update) -{} \
                              ".format(disk_usage[3], seqTree_update))

            count += 1

            #######################################################################
            '''
            STEP - 5, Data validation
            '''
            self.log.info("Step 5, Iteration= {}".format(count+1))
            self.validate_data("update", self.gen_update)

            #######################################################################
        self.change_swap_space(self.cluster.nodes_in_cluster, disable=False)
        self.log.info("====test_move_docs_btwn_key_and_seq_trees ends====")

    def test_parallel_create_update(self):
        """
        STEPS:
          -- Create new items and update already
             existing items
          -- Check disk_usage after each Iteration
          -- Data validation for last iteration
        """
        self.log.info("test_parallel_create_update starts")
        count = 0
        init_items = self.num_items
        self.doc_ops = "create:update"
        self.update_start = 0
        self.update_end = self.num_items
        while count < self.test_itr:
            self.log.info("Iteration {}".format(count+1))
            self.create_start = self.num_items
            self.create_end = self.num_items+init_items

            if self.rev_write:
                self.create_start = -int(self.num_items+init_items - 1)
                self.create_end = -int(self.num_items - 1)

            self.generate_docs()
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets(timeout=3600)
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            if count == self.test_itr - 1:
                self.validate_data("update", self.gen_update)
            self.update_start = self.num_items
            self.update_end = self.num_items+init_items
            if self.rev_update:
                self.update_start = -int(self.num_items+init_items - 1)
                self.update_end = -int(self.num_items - 1)

            disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            if self.doc_size <= 32:
                self.assertIs(
                    disk_usage[2] >= disk_usage[3], True,
                    "seqIndex usage = {}MB'\n' \
                    after Iteration {}'\n' \
                    exceeds keyIndex usage={}MB'\n' \
                    ".format(disk_usage[3],
                             count+1,
                             disk_usage[2]))
            self.assertIs(
                disk_usage[0] > 2.2 * (2 * self.disk_usage[
                    self.disk_usage.keys()[0]]),
                False, "Disk Usage {}MB After '\n\'\
                Updates exceeds '\n\'\
                Actual disk usage {}MB by '\n'\
                2.2 times".format(disk_usage[0],
                                  (2 * self.disk_usage[
                                      self.disk_usage.keys()[0]])))
            count += 1
        self.change_swap_space(self.cluster.nodes_in_cluster, disable=False)
        self.log.info("====test_parallel_create_update ends====")