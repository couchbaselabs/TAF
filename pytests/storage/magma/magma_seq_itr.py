'''
Created on 22-Jun-2021

@author: riteshagarwal
'''

import copy
import json
import time

from storage.magma.magma_base import MagmaBaseTest
from dcp_new.constants import SUCCESS
from dcp_utils.dcp_ready_functions import DCPUtils
from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer
from remote.remote_util import RemoteMachineShellConnection
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats


class DCPSeqItr(MagmaBaseTest):
    def setUp(self):
        super(DCPSeqItr, self).setUp()

        self.vbuckets = range(1024)
        self.start_seq_no_list = self.input.param("start", [0] * len(self.vbuckets))
        self.end_seq_no = self.input.param("end", 0xffffffffffffffff)
        self.vb_uuid_list = self.input.param("vb_uuid_list", ['0'] * len(self.vbuckets))
        self.vb_retry = self.input.param("retry_limit", 10)
        self.filter_file = self.input.param("filter", None)
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.items = copy.deepcopy(self.init_num_items)
        self.bucket = self.cluster.buckets[0]
        self.dcp_util = DCPUtils(self.cluster.master,
                            self.cluster.buckets[0], self.start_seq_no_list,
                            self.end_seq_no, self.vb_uuid_list)
        self.dcp_util.initialise_cluster_connections()

    def tearDown(self):
        super(MagmaBaseTest, self).tearDown()

    def compute_doc_count(self, start=None, end=None):
        ops_len = len(self.doc_ops.split(":"))
        start = start or self.init_num_items
        end = end or self.init_num_items

        if "create" in self.doc_ops:
            ops_len -= 1
            self.create_start = start
            self.create_end = start + end
            self.items += self.create_end - self.create_start

        if ops_len == 1:
            self.update_start = 0
            self.update_end = end
            self.expiry_start = 0
            self.expiry_end = end
            self.delete_start = 0
            self.delete_end = end
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = end // 2
            self.delete_start = end // 2
            self.delete_end = end

            if "expiry" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = end // 2
                self.expiry_start = end // 2
                self.expiry_end = end
        elif ops_len == 3:
            self.update_start = 0
            self.update_end = end // 3
            self.delete_start = end // 3
            self.delete_end = (2 * end) // 3
            self.expiry_start = (2 * end) // 3
            self.expiry_end = end

        if "delete" in self.doc_ops:
            self.items -= self.delete_end - self.delete_start
        if "expiry" in self.doc_ops:
            self.items -= self.expiry_end - self.expiry_start
        self.log.info("items in coumpute_docs {}".format(self.items))

    def get_collection_id(self, bucket_name, scope_name, collection_name=None):
        uid = None
        _, content = self.bucket_helper_obj.list_collections(bucket_name)
        content = json.loads(content)
        for scope in content["scopes"]:
            if scope["name"] == scope_name:
                uid = scope["uid"]
                if collection_name:
                    collection_list = scope["collections"]
                    for collection in collection_list:
                        if collection["name"] == collection_name:
                            uid = collection["uid"]
                            break
        return uid

    def test_dcp_duplication_in_specific_collection(self):
        '''
        Test Focus :
          --- Create a DCP stream with a random collection
              and validate the item count served by dcp stream
        Steps:
         --- Choose a random collection
         --- Doc ops in random collection
         --- Create DCP stream(collection specific)
         --- Validate the data(item count) of dcp stream
         '''

        self.log.info("test_dcp_duplication_in_specific_collection starts")
        '''
          -- Choose a random collection
        '''
        self.bucket = self.cluster.buckets[0]
        collections = self.bucket_util.get_random_collections(
            [self.cluster.buckets[0]], 1, 1, 1)
        scope_dict = collections[self.cluster.buckets[0].name]["scopes"]
        scope_name = scope_dict.keys()[0]
        collection_name = scope_dict[scope_name]["collections"].keys()[0]

        '''
         --- Doc ops in given collection(random collection)
        '''
        self.compute_doc_count()
        self.generate_docs(doc_ops=self.doc_ops)

        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True,
                              doc_ops=self.doc_ops,
                              scope=scope_name,
                              collection=collection_name)

        self.sleep(30, "sleep for 30 seconds")

        '''
          --- Get the UID of collection
          --- Create DCP stream for specific collection
        '''
        # get the uid and stream dcp data for that collection
        cid = self.get_collection_id(self.bucket.name, scope_name, collection_name)

        self.filter_file = {"collections": [cid]}
        self.filter_file = json.dumps(self.filter_file)
        output_string = self.dcp_util.get_dcp_event(self.filter_file)

        '''
          --- Item Count verification
        '''
        actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
        self.log.info("actual item count is {}".format(actual_item_count))

        msg = "item count mismatch, expected {} actual {}"
        self.assertIs(actual_item_count == self.items, True,
                          msg.format(self.items, actual_item_count))

    def test_stream_entire_bucket(self):
        self.log.info("test_stream_entire_bucket starts")
        self.drop_scopes = self.input.param("drop_scopes", False)
        self.drop_collections = self.input.param("drop_collections",False)
        num_drop_scopes = self.input.param("num_drop_scopes", 0)
        num_drop_collections = self.input.param("num_drop_collections", 0)
        self.crud_ops = self.input.param("crud_ops", True)
        self.collections.remove(CbServer.default_collection)
        self.scopes.remove(CbServer.default_scope)
        collections = copy.deepcopy(self.collections)
        scopes = copy.deepcopy(self.scopes)

        if self.drop_scopes:
            expected_item_count =   ((self.init_items_per_collection * (self.num_collections-1)) + self.init_items_per_collection) * ((len(self.scopes) - num_drop_scopes))
            self.log.info("expected item count is {}".format(expected_item_count))
            for scope_name in scopes[0:num_drop_scopes]:
                self.bucket_util.drop_scope(self.cluster.master, self.bucket, scope_name)
                self.scopes.remove(scope_name)
            scopes.append(CbServer.default_scope)
        elif self.drop_collections:
            if num_drop_collections == self.num_collections:
                num_drop_collections -= 1
            expected_item_count = self.init_items_per_collection * ((self.num_scopes * ((self.num_collections-1) - num_drop_collections)) + 1)
            self.log.info("expected item count is {}".format(expected_item_count))
            for scope_name in self.scopes:
                self.log.info("scope name {}".format(scope_name))
                for collection_name in collections[0:num_drop_collections]:
                    self.bucket_util.drop_collection(self.cluster.master,
                                         self.bucket,
                                         scope_name,
                                         collection_name)
            for collection_name in collections[0:num_drop_collections]:
                self.collections.remove(collection_name)
        self.log.info("collection is {}".format(collections))
        scopes.append(CbServer.default_scope)
        if self.crud_ops:
            self.compute_doc_count()
            self.generate_docs(doc_ops=self.doc_ops)
            for scope in scopes:
                if scope == CbServer.default_scope:
                    self.collections.append(CbServer.default_collection)
                if len(self.collections) == 0:
                    continue
                for collection in self.collections:
                    _ = self.loadgen_docs(self.retry_exceptions,
                                          self.ignore_exceptions,
                                          _sync=True,
                                          doc_ops=self.doc_ops,
                                          scope=scope,
                                          collection=collection)
                    if collection == CbServer.default_collection:
                        self.collections.remove(CbServer.default_collection)
            expected_item_count = self.items * ((len(scopes) * ((self.num_collections-1) - num_drop_collections)) + 1)
            self.log.info("expected item count after cruds is  {}".format(expected_item_count))
        # stream dcp events and verify events
        output_string = self.dcp_util.get_dcp_event()
        actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
        self.log.info("actual_item_count is {}".format(actual_item_count))
        msg = "item count mismatch, expected {} actual {}"
        self.assertIs(actual_item_count == expected_item_count, True,
                          msg.format(expected_item_count, actual_item_count))

    def test_stream_during_rollback(self):
        '''
         -- Ensure creation of at least a single state file
         -- Stop persistence on master node
         -- Start load on master node(say Node A) for a given duration(self.duration * 60 seconds)
         -- Above step ensures creation of new state files (# equal to self.duration)
         -- Kill MemCached on master node(Node A)
         -- Trigger roll back on other/replica nodes
         -- START STREAMING DATA USING DCP
         -- ReStart persistence on master node
         -- Start doc loading on all the nodes(ensure creation of state file)
         -- Above two steps ensure, roll back to new snapshot
         -- Repeat all the above steps for num_rollback times
         --
        '''
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        ops_len = len(self.doc_ops.split(":"))
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 3)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)

        #######################################################################
        '''
        STEP - 1,  Stop persistence on master node
        '''
        master_itr = 0
        for i in range(1, self.num_rollbacks+1):
            start = items
            self.log.info("Roll back Iteration == {}".format(i))

            mem_item_count = 0

            # Stopping persistence on NodeA
            self.log.debug("Iteration == {}, stopping persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

            ###################################################################
            '''
            STEP - 2
              -- Doc ops on master node for  self.duration * 60 seconds
              -- This step ensures new state files (number equal to self.duration)
            '''
            self.log.info("Just before compute docs, iteration {}".format(i))
            self.compute_docs(start, mem_only_items)
            self.gen_create = None
            self.gen_update = None
            self.gen_delete = None
            self.gen_expiry = None
            time_end = time.time() + 60 * self.duration
            while time.time() < time_end:
                master_itr += 1
                time_start = time.time()
                mem_item_count += mem_only_items * ops_len
                self.generate_docs(doc_ops=self.doc_ops,
                                   target_vbucket=self.target_vbucket)
                self.loadgen_docs(_sync=True,
                                  retry_exceptions=self.retry_exceptions)
                if self.gen_create is not None:
                    self.create_start = self.gen_create.key_counter
                if self.gen_update is not None:
                    self.update_start = self.gen_update.key_counter
                if self.gen_delete is not None:
                    self.delete_start = self.gen_delete.key_counter
                if self.gen_expiry is not None:
                    self.expiry_start = self.gen_expiry.key_counter

                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(),
                               "master_itr == {}, Sleep to ensure creation of state files for roll back,"
                               .format(master_itr))
                self.log.info("master_itr == {}, state files== {}".
                              format(master_itr,
                                     self.get_state_files(self.buckets[0])))

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_item_count}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map, timeout=300)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    cbstat_cmd="all",
                    stat_name="vb_replica_queue_size", timeout=300)

            # replica vBuckets
            for bucket in self.cluster.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
            ###################################################################
            '''
            STEP - 3
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/other nodes
              -- Start streaming data (through DCP)
            '''

            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster.master],
                self.cluster.buckets[0],
                wait_time=self.wait_timeout * 10))
            output_string = self.dcp_util.get_dcp_event()
            actual_item_count = len(list(filter(lambda x: 'CMD_MUTATION' in x, output_string)))
            self.log.info("actual_item_count is {}".format(actual_item_count))
            msg = "item count mismatch, expected {} actual {}"
            self.assertIs(actual_item_count == self.num_items, True,
                          msg.format(self.num_items, actual_item_count))

            ###################################################################
            '''
            STEP -4
              -- Restarting persistence on master node(Node A)
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")
            self.sleep(5, "Iteration=={}, sleep after restarting persistence".format(i))
            ###################################################################
            '''
            STEP - 5
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            if i != self.num_rollbacks:
                self.create_start = items
                self.create_end = items + 50000
                self.generate_docs(doc_ops="create", target_vbucket=None)
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True,
                                      doc_ops="create")
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                items = items + 50000
                self.log.debug("Iteration == {}, Total num_items {}".format(i, items))

        shell.disconnect()


