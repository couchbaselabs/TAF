import random
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection


class SteadyStateTests(MagmaBaseTest):
    def setUp(self):
        super(SteadyStateTests, self).setUp()
        self.wipe_history = self.input.param("wipe_history", False)
        self.retention_seconds_to_wipe_history = self.input.param("retention_seconds_to_wipe_history", 86400)
        self.retention_bytes_to_wipe_history = self.input.param("retention_bytes_to_wipe_history", 1000000000000)
        self.set_history_in_test = self.input.param("set_history_in_test", False)
        self.meta_purge_interval = self.input.param("meta_purge_interval", 180)

    def tearDown(self):
        super(SteadyStateTests, self).tearDown()

    def test_history_retention_for_n_upsert_iterations(self):
        self.PrintStep("test_history_retention_for_n_upsert_iterations starts")
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.PrintStep("Step 1: Create %s items/collection: %s" % (self.init_items_per_collection,
                                                                   self.key_type))
        self.new_loader(wait=True)
        if not self.set_history_in_test:
            init_history_start_seq = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq after initial creates  {}".format(init_history_start_seq[bucket]))

        count = 1
        while count < self.test_itr+1:
            self.PrintStep("Step 2.%s: Update %s items/collection: %s" % (count, self.init_items_per_collection,
                                                                   self.key_type))
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = self.init_items_per_collection
            self.update_perc = 100
            self.create_perc = 0
            self.new_loader(wait=True)

            if count == 2 and self.set_history_in_test:
                init_history_start_seq = self.get_history_start_seq_for_each_vb()
                for bucket in self.cluster.buckets:
                    self.log.info("history_start_seq after 2nd iteration of upserts  {}".format(init_history_start_seq[bucket]))


            if count == 1 and self.set_history_in_test:
                self.PrintStep("Step 2.%s.0: Setting history params after first upsert iteration"% (count))
                self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400, history_retention_bytes=1000000000000)
                self.sleep(10, "sleep after updating history settings")
                init_history_start_seq = self.get_history_start_seq_for_each_vb()
                for bucket in self.cluster.buckets:
                    self.log.info("history_start_seq after initial creates and upserts  {}".format(init_history_start_seq[bucket]))

            self.PrintStep("Step 2.%s.1: Comparing history start seq number"% (count))
            history_start_seq_stats = dict()
            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq_stats {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "{} vbucket {} has start seq number {}".format(bucket.name, key,
                                                                         history_start_seq_stats[bucket][key]["active"]["history_start_seqno"])
                    self.assertEqual(init_history_start_seq[bucket][key]["active"]["history_start_seqno"],
                                     history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                     msg)
            count += 1
        self.PrintStep("Step 3: Restart CouchBase Server on all Nodes")
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
        self.sleep(60, "sleep before seq number count")

        self.PrintStep("Step 4: Sequence number count check")

        expected_count = ((count) * ((self.num_replicas+1) * self.init_items_per_collection * (self.num_collections-1))) + ((self.num_collections) * (self.vbuckets*(self.num_replicas+1)))
        seq_count = self.get_seqnumber_count()
        self.log.info("expected_count = {}".format(expected_count))
        self.assertEqual(seq_count, expected_count, "Not all sequence numbers are present")

        if self.wipe_history:
            self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=self.retention_seconds_to_wipe_history,
                    history_retention_bytes=self.retention_bytes_to_wipe_history)
            self.sleep(10, "sleep after updating history params")
            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq_stats after wiping history {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "curr_history_start_seq {} for  bucket {} and vbucket {} is non zero ".format(history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                                                                                        bucket.name, key)
                    self.assertNotEqual(0 , history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], msg)
            self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400,
                    history_retention_bytes=96000000000)
            self.update_start = 0
            self.update_end = 2000
            self.new_loader(wait=True)
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.restart_couchbase()
                shell.disconnect()
            self.sleep(60, "Sleep after restarting couchbase server")

            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info(" New history_start_seq_stats {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "init_history_start_seq {} > curr_history_start_seq {} for  bucket {} and vbucket {} ".format(init_history_start_seq[bucket][key]["active"]["history_start_seqno"],
                                                                                                                    history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], 
                                                                                                                    bucket.name, key)
                    self.assertTrue(init_history_start_seq[bucket][key]["active"]["history_start_seqno"] < history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], msg)
            seq_count = self.get_seqnumber_count()

    def test_history_retention_for_n_expiry_iterations(self):
        self.PrintStep("test_history_retention_for_n_expiry_iterations starts")
        self.exp_pager_stime = self.input.param("exp_pager_stime", 10)
        self.check_seq_count = self.input.param("check_seq_count", False)
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.PrintStep("Step 1: Create %s items/collection: %s" % (self.init_items_per_collection,
                                                                   self.key_type))
        self.new_loader(wait=True)
        if not self.set_history_in_test:
            init_history_start_seq = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq after initial creates  {}".format(init_history_start_seq[bucket]))

        count = 1
        while count < self.test_itr+1:
            self.maxttl = random.randint(5, 20)
            self.PrintStep("Step 4.%s: Expire %s items/collection: %s" % (count, self.init_items_per_collection,
                                                                   self.key_type))
            self.bucket_util._expiry_pager(self.cluster, 10000000000)
            self.doc_ops = "expiry"
            self.expiry_start = 0
            self.expiry_end = self.init_items_per_collection
            self.expiry_perc = 100
            self.create_perc = 0
            self.num_items_per_collection -= self.expiry_end - self.expiry_start
            tasks = self.new_loader(wait=False)
            self.sleep(self.maxttl, "Wait for docs to expire")
            self.doc_loading_tm.getAllTaskResult()
            self.printOps.end_task()
            self.retry_failures(tasks, wait_for_stats=False)
            self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
            self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
             to kickoff")
            self.sleep(self.exp_pager_stime*30, "Wait for KV purger to scan expired docs and add \
            tombstones.")

            if count == 2 and self.set_history_in_test:
                init_history_start_seq = self.get_history_start_seq_for_each_vb()
                for bucket in self.cluster.buckets:
                    self.log.info("history_start_seq after 2nd iteration of expiry  {}".format(init_history_start_seq[bucket]))

            if count == 1 and self.set_history_in_test:
                self.PrintStep("Step 2.%s.2: Setting history params after first expiry iteration"% (count))
                self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400, history_retention_bytes=1000000000000)
                self.sleep(90, "sleep after updating history settings")
                init_history_start_seq = self.get_history_start_seq_for_each_vb()
                for bucket in self.cluster.buckets:
                    self.log.info("history_start_seq after initial creates and upserts  {}".format(init_history_start_seq[bucket]))
            history_start_seq_stats = dict()
            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq_stats {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "{} vbucket {} has start seq number {}".format(bucket.name, key,
                                                                         history_start_seq_stats[bucket][key]["active"]["history_start_seqno"])
                    self.assertEqual(init_history_start_seq[bucket][key]["active"]["history_start_seqno"],
                                     history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                     msg)

            count += 1

        self.PrintStep("Step 3: Restart CouchBase Server on all Nodes")
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
        self.sleep(30, "sleep after restarting couchbase-server")
        if self.check_seq_count:
            self.sleep(60, "sleep before seq number count")

            self.PrintStep("Step 4: Sequence number count check")
            expected_count = ((count-1) * ((self.num_replicas+1) * (2 * self.init_items_per_collection) * (self.num_scopes *(self.num_collections-1))))\
            + ((self.num_collections) * (self.vbuckets*(self.num_replicas+1))) \
            + ((self.num_replicas+1) * self.init_items_per_collection * (self.num_scopes * (self.num_collections-1)))

            seq_count = self.get_seqnumber_count()
            self.log.info("expected_count = {}".format(expected_count))
            self.assertEqual(seq_count, expected_count, "Not all sequence numbers are present")

            self.PrintStep("Step 5: Set Meta data purge interval")
            self.meta_purge_interval_in_days = self.meta_purge_interval / 86400.0
            self.set_metadata_purge_interval(
                value=self.meta_purge_interval_in_days, buckets=self.buckets)

            self.sleep(self.meta_purge_interval,
                       "sleeping after setting metadata purge interval using diag/eval")

            self.bucket_util.cbepctl_set_metadata_purge_interval(
                    self.cluster, self.buckets, value=self.meta_purge_interval)

            self.sleep(self.meta_purge_interval*2, "Wait for Metadata Purge Interval to drop \
                tomb-stones from storage")

            self.PrintStep("Step 6: Seq number count check after purge")
            seq_count = self.get_seqnumber_count()
            #self.assertEqual(seq_count, expected_count, "Not all sequence numbers are present after purge")

        if self.wipe_history:
            self.PrintStep("Step 7: Setting history params to wipe history")
            self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=self.retention_seconds_to_wipe_history,
                    history_retention_bytes=self.retention_bytes_to_wipe_history)
            self.sleep(100, "sleep after updating history params")
            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq_stats after wiping history {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "curr_history_start_seq {} for  bucket {} and vbucket {} is non zero ".format(history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                                                                                        bucket.name, key)
                    self.assertNotEqual(0 , history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], msg)
            self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400,
                    history_retention_bytes=96000000000)
            self.expiry_start = 0
            self.expiry_end = 2000
            self.new_loader(wait=False)
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.restart_couchbase()
                shell.disconnect()
            self.sleep(60, "Sleep after restarting couchbase server")

            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info(" New history_start_seq_stats {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "init_history_start_seq {} > curr_history_start_seq {} for  bucket {} and vbucket {} ".format(init_history_start_seq[bucket][key]["active"]["history_start_seqno"],
                                                                                                                    history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], 
                                                                                                                    bucket.name, key)
                    self.assertTrue(init_history_start_seq[bucket][key]["active"]["history_start_seqno"] < history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], msg)
            seq_count = self.get_seqnumber_count()

    def test_history_retention_for_multiple_CRUD_iterations(self):
        self.PrintStep("test_history_retention_for_multiple_CRUD_iterations starts")
        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.PrintStep("Step 1: Create %s items/collection: %s" % (self.init_items_per_collection,
                                                                   self.key_type))
        self.new_loader(wait=True)
        init_history_start_seq = self.get_history_start_seq_for_each_vb()

        count = 0
        while count < self.test_itr:
            self.PrintStep("Step 2.%s: Update %s items/collection: %s" % (count, self.init_items_per_collection,
                                                                   self.key_type))
            self.reset_doc_params(doc_ops="update:read")
            self.update_start = self.read_start = 0
            self.update_end = self.read_end = self.init_items_per_collection
            self.new_loader(wait=True)
            self.reset_doc_params(doc_ops="delete")
            self.delete_start = 0
            self.delete_end = self.init_items_per_collection
            self.num_items_per_collection -= self.delete_end - self.delete_start
            self.new_loader(wait=True)
            self.num_items_per_collection += self.delete_end - self.delete_start

            self.PrintStep("Step 2.%s.1: Comparing history start seq number"% (count+1))
            history_start_seq_stats = dict()
            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq_stats {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "{} vbucket {} has start seq number {}".format(bucket.name, key,
                                                                         history_start_seq_stats[bucket][key]["active"]["history_start_seqno"])
                    self.assertEqual(init_history_start_seq[bucket][key]["active"]["history_start_seqno"],
                                     history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                     msg)
            if count == 0 and self.set_history_in_test:
                self.PrintStep("Step 2.%s.2: Setting history params after first iteration"% (count+1))
                self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400, history_retention_bytes=1000000000000)
                self.sleep(30, "sleep after updating history settings")
                init_history_start_seq = self.get_history_start_seq_for_each_vb()
            count += 1
        self.PrintStep("Step 3: Restart CouchBase Server on all Nodes")

        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shell.restart_couchbase()
            shell.disconnect()
        self.sleep(60, "sleep before seq number count")

        self.PrintStep("Step 4: Sequence number count check")

        expected_count = (( 1+ (count * 2)) * ((self.num_replicas+1) * self.init_items_per_collection * (self.num_collections-1))) + ((self.num_collections) * (self.vbuckets*(self.num_replicas+1)))
        seq_count = self.get_seqnumber_count()
        self.log.info("expected_count = {}".format(expected_count))
        self.assertEqual(seq_count, expected_count, "Not all sequence numbers are present")

        if self.wipe_history:
            self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=self.retention_seconds_to_wipe_history,
                    history_retention_bytes=self.retention_bytes_to_wipe_history)
            self.sleep(10, "sleep after updating history params")
            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info("history_start_seq_stats after wiping history {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "curr_history_start_seq {} for  bucket {} and vbucket {} is non zero ".format(history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                                                                                        bucket.name, key)
                    self.assertEqual(0 , history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], msg)
            self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400,
                    history_retention_bytes=1000000000000)
            self.update_start = 0
            self.update_end = 2000
            self.new_loader(wait=True)
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.restart_couchbase()
                shell.disconnect()
            self.sleep(60, "Sleep after restarting couchbase server")

            history_start_seq_stats = self.get_history_start_seq_for_each_vb()
            for bucket in self.cluster.buckets:
                self.log.info(" New history_start_seq_stats {}".format(history_start_seq_stats[bucket]))
                for key in history_start_seq_stats[bucket].keys():
                    msg = "init_history_start_seq {} > curr_history_start_seq {} for  bucket {} and vbucket {} ".format(init_history_start_seq[bucket][key]["active"]["history_start_seqno"],
                                                                                                                    history_start_seq_stats[bucket][key]["active"]["history_start_seqno"],
                                                                                                                    bucket.name, key)
                    self.assertTrue(init_history_start_seq[bucket][key]["active"]["history_start_seqno"] < history_start_seq_stats[bucket][key]["active"]["history_start_seqno"], msg)
            seq_count = self.get_seqnumber_count()
