'''
Created on Jan 4, 2018

@author: riteshagarwal
'''
from cbas_base import CBASBaseTest
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
import time
from cbas_utils.cbas_utils import CbasUtil
from TestInput import TestInputSingleton


class PartialRollback_CBAS(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})

        super(PartialRollback_CBAS, self).setUp()

        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes(and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes(and tests wants only 1 cbas node)
        3. There can be only 1 node running KV,CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas. For that service check on nodes is needed.
        '''
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cluster.cbas_nodes) > 0:
            self.otpNodes.extend(self.add_all_nodes_then_rebalance(self.cluster.cbas_nodes))

        '''Create default bucket'''
        self.bucket_util.create_default_bucket(storage=self.bucket_storage)
        self.cbas_util.createConn("default")

        self.merge_policy = self.input.param('merge_policy', None)
        self.max_mergable_component_size = self.input.param('max_mergable_component_size', 16384)
        self.max_tolerance_component_count = self.input.param('max_tolerance_component_count', 2)
        self.create_index = self.input.param('create_index', False)
        self.where_field = self.input.param('where_field',None)
        self.where_value = self.input.param('where_value',None)
        self.CC = self.input.param('CC',False)

    def setup_for_test(self, skip_data_loading=False):

        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets("create", 0,
                                                   self.num_items, batch_size=1000)

        # Create dataset on the CBAS bucket
        if self.merge_policy == None:
            self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                    where_field=self.where_field, where_value=self.where_value,
                                                    cbas_dataset_name=self.cbas_dataset_name)
        else:
            self.cbas_util.create_dataset_on_bucket_merge_policy(cbas_bucket_name=self.cb_bucket_name,
                                                                 where_field=self.where_field, where_value=self.where_value,
                                                                 cbas_dataset_name=self.cbas_dataset_name,merge_policy=self.merge_policy,
                                                                 max_mergable_component_size=self.max_mergable_component_size,
                                                                 max_tolerance_component_count=self.max_tolerance_component_count)
        if self.create_index:
            create_idx_statement = "create index {0} on {1}({2});".format(
                self.index_name, self.cbas_dataset_name, "profession:string")
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                create_idx_statement)
        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        if not skip_data_loading:
            result = RestConnection(self.cluster.master).query_tool("CREATE INDEX {0} ON {1}({2})".format(self.index_name, self.cb_bucket_name, "profession"))
            self.sleep(20, "wait for index creation.")
            self.assertTrue(result['status'] == "success")
            if self.where_field and self.where_value:
                items = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
            else:
                items = self.num_items
            # Validate no. of items in CBAS dataset
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cbas_dataset_name,
                    items):
                self.fail(
                    "No. of items in CBAS dataset do not match that in the CB bucket")

    def tearDown(self):
        super(PartialRollback_CBAS, self).tearDown()

    def test_ingestion_after_kv_rollback_create_ops(self):
        self.setup_for_test()
        items_before_persistence_stop = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)[0]
        self.log.info("Items in CBAS before persistence stop: %s"%items_before_persistence_stop)
        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.cluster.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets("create", self.num_items,
                                               self.num_items*3/2)

        kv_nodes = self.get_kv_nodes(self.servers, self.cluster.master)
        items_in_cb_bucket = 0
        if self.where_field and self.where_value:
            items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
        else:
            for node in kv_nodes:
                items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)
        # Validate no. of items in CBAS dataset
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, items_in_cb_bucket, 0),
                        "No. of items in CBAS dataset do not match that in the CB bucket")

        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)

        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.kill_memcached()
        self.sleep(2,"Wait for 2 secs for DCP rollback sent to CBAS.")
        curr = time.time()
        while items_in_cbas_bucket != 0 and items_in_cbas_bucket > items_before_persistence_stop:
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break
        self.assertTrue(items_in_cbas_bucket<=items_before_persistence_stop, "Roll-back did not happen.")
        self.log.info("#######BINGO########\nROLLBACK HAPPENED")

        items_in_cb_bucket = 0
        curr = time.time()
        while items_in_cb_bucket != items_in_cbas_bucket:
            items_in_cb_bucket = 0
            items_in_cbas_bucket = 0
            if self.where_field and self.where_value:
                try:
                    items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
                except:
                    self.log.info("Indexer in rollback state. Query failed. Pass and move ahead.")
                    pass
            else:
                for node in kv_nodes:
                    items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)

            self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break

        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")

    def test_ingestion_after_kv_rollback_create_ops_MB29860(self):
        self.setup_for_test()
        items_before_persistence_stop = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)[0]
        self.log.info("Items in CBAS before persistence stop: %s"%items_before_persistence_stop)
        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.cluster.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets("create", self.num_items,
                                               self.num_items*3/2)

        kv_nodes = self.get_kv_nodes(self.servers, self.cluster.master)
        items_in_cb_bucket = 0
        if self.where_field and self.where_value:
            items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
        else:
            for node in kv_nodes:
                items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)
        # Validate no. of items in CBAS dataset
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, items_in_cb_bucket, 0),
                        "No. of items in CBAS dataset do not match that in the CB bucket")

        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)

        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.kill_memcached()
        if self.input.param('kill_cbas', False):
            shell = RemoteMachineShellConnection(self.cbas_node)
            shell.kill_process("/opt/couchbase/lib/cbas/runtime/bin/java", "java")
            shell.kill_process("/opt/couchbase/bin/cbas", "cbas")

        tries = 60
        result = False
        while tries >0 and not result:
            try:
                result = self.cbas_util.connect_to_bucket(self.cbas_bucket_name)
                tries -= 1
            except:
                pass
            self.sleep(2)
        self.assertTrue(result, "CBAS connect bucket failed after memcached killed on KV node.")

        self.sleep(2,"Wait for 2 secs for DCP rollback sent to CBAS.")
        curr = time.time()
        while items_in_cbas_bucket != 0 and items_in_cbas_bucket > items_before_persistence_stop:
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break
        self.assertTrue(items_in_cbas_bucket<=items_before_persistence_stop, "Roll-back did not happen.")
        self.log.info("#######BINGO########\nROLLBACK HAPPENED")

        items_in_cb_bucket = 0
        curr = time.time()
        while items_in_cb_bucket != items_in_cbas_bucket:
            items_in_cb_bucket = 0
            items_in_cbas_bucket = 0
            if self.where_field and self.where_value:
                try:
                    items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
                except:
                    self.log.info("Indexer in rollback state. Query failed. Pass and move ahead.")
                    pass
            else:
                for node in kv_nodes:
                    items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)

            self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break

        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")

    def test_ingestion_after_kv_rollback_delete_ops(self):
        self.setup_for_test()
        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.cluster.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets("delete", 0,
                                               self.num_items / 2)

        kv_nodes = self.get_kv_nodes(self.servers, self.cluster.master)
        items_in_cb_bucket = 0
        if self.where_field and self.where_value:
            items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
        else:
            for node in kv_nodes:
                items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)
        # Validate no. of items in CBAS dataset
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, items_in_cb_bucket, 0),
                        "No. of items in CBAS dataset do not match that in the CB bucket")

        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        items_before_rollback = items_in_cbas_bucket
        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.kill_memcached()
        self.sleep(2,"Wait for 2 secs for DCP rollback sent to CBAS.")
        curr = time.time()
        while items_in_cbas_bucket != 0 and items_in_cbas_bucket <= items_before_rollback:
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break
        self.assertTrue(items_in_cbas_bucket>items_before_rollback, "Roll-back did not happen.")
        self.log.info("#######BINGO########\nROLLBACK HAPPENED")

        items_in_cb_bucket = 0
        curr = time.time()
        while items_in_cb_bucket != items_in_cbas_bucket:
            items_in_cb_bucket = 0
            items_in_cbas_bucket = 0
            if self.where_field and self.where_value:
                try:
                    items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
                except:
                    self.log.info("Indexer in rollback state. Query failed. Pass and move ahead.")
                    pass
            else:
                for node in kv_nodes:
                    items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)

            self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break

        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")

    def test_ingestion_after_kv_rollback_cbas_disconnected(self):
        self.setup_for_test()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.cluster.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets("delete", 0,
                                               self.num_items / 2)

        # Count no. of items in CB & CBAS Buckets
        kv_nodes = self.get_kv_nodes(self.servers, self.cluster.master)
        items_in_cb_bucket = 0
        for node in kv_nodes:
            items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)

        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        items_before_rollback = items_in_cbas_bucket
        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)
        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.kill_memcached()
#         self.sleep(10,"Wait for 10 secs for memcached restarts.")
        if self.input.param('kill_cbas', False):
            shell = RemoteMachineShellConnection(self.cbas_node)
            shell.kill_process("/opt/couchbase/lib/cbas/runtime/bin/java", "java")
            shell.kill_process("/opt/couchbase/bin/cbas", "cbas")

        tries = 60
        result = False
        while tries >0 and not result:
            try:
                result = self.cbas_util.connect_to_bucket(self.cbas_bucket_name)
                tries -= 1
            except:
                pass
            self.sleep(2)
        self.assertTrue(result, "CBAS connect bucket failed after memcached killed on KV node.")
        curr = time.time()
        while items_in_cbas_bucket != 0 and items_in_cbas_bucket <= items_before_rollback:
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break
        self.assertTrue(items_in_cbas_bucket>items_before_rollback, "Roll-back did not happen.")
        self.log.info("#######BINGO########\nROLLBACK HAPPENED")

        curr = time.time()
        while items_in_cb_bucket != items_in_cbas_bucket:
            items_in_cb_bucket = 0
            items_in_cbas_bucket = 0
            if self.where_field and self.where_value:
                try:
                    items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
                except:
                    self.log.info("Indexer in rollback state. Query failed. Pass and move ahead.")
                    pass
            else:
                for node in kv_nodes:
                    items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)

            self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            if curr+120 < time.time():
                break

        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)

        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")

    def test_rebalance_kv_rollback_create_ops(self):
        self.setup_for_test()
        items_before_persistence_stop = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)[0]
        self.log.info("Items in CBAS before persistence stop: %s"%items_before_persistence_stop)
        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA")
        mem_client = MemcachedClientHelper.direct_client(self.cluster.master,
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets("create", self.num_items,
                                               self.num_items*3/2)

        kv_nodes = self.get_kv_nodes(self.servers, self.cluster.master)
        items_in_cb_bucket = 0
        if self.where_field and self.where_value:
            items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
        else:
            for node in kv_nodes:
                items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)
        # Validate no. of items in CBAS dataset
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, items_in_cb_bucket, 0),
                        "No. of items in CBAS dataset do not match that in the CB bucket")

        # Count no. of items in CB & CBAS Buckets
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)

        self.log.info("Before Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "Before Rollback : # Items in CBAS bucket does not match that in the CB bucket")

        if self.CC:
            self.cluster_util.remove_node([self.otpNodes[0]],wait_for_rebalance=False)
            self.cbas_util.closeConn()
            self.cbas_util = CbasUtil(self.cluster.master, self.cluster.cbas_nodes[0])
            self.cbas_util.createConn("default")
        else:
            self.cluster_util.remove_node([self.otpNodes[1]],wait_for_rebalance=False)

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.kill_memcached()
        self.sleep(2,"Wait for 2 secs for DCP rollback sent to CBAS.")
        curr = time.time()
        while items_in_cbas_bucket == -1 or (items_in_cbas_bucket != 0 and items_in_cbas_bucket > items_before_persistence_stop):
            try:
                if curr+120 < time.time():
                    break
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                self.log.info("Items in CBAS: %s"%items_in_cbas_bucket)
            except:
                self.log.info("Probably rebalance is in progress and the reason for queries being failing.")
                pass
        self.assertTrue(items_in_cbas_bucket<=items_before_persistence_stop, "Roll-back did not happen.")
        self.log.info("#######BINGO########\nROLLBACK HAPPENED")

        items_in_cb_bucket = 0
        curr = time.time()
        while items_in_cb_bucket != items_in_cbas_bucket or items_in_cb_bucket == 0:
            items_in_cb_bucket = 0
            items_in_cbas_bucket = 0
            if self.where_field and self.where_value:
                try:
                    items_in_cb_bucket = RestConnection(self.cluster.master).query_tool('select count(*) from %s where %s = "%s"'%(self.cb_bucket_name,self.where_field,self.where_value))['results'][0]['$1']
                except:
                    self.log.info("Indexer in rollback state. Query failed. Pass and move ahead.")
                    pass
            else:
                for node in kv_nodes:
                    items_in_cb_bucket += self.get_item_count_mc(node,self.cb_bucket_name)

            self.log.info("Items in CB bucket after rollback: %s"%items_in_cb_bucket)
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            if curr+120 < time.time():
                break
        str_time = time.time()
        while self.rest._rebalance_progress_status() == "running" and time.time()<str_time+300:
            self.sleep(1)
            self.log.info("Waiting for rebalance to complete")

        self.log.info("After Rollback --- # docs in CB bucket : %s, # docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        self.assertTrue(items_in_cb_bucket == items_in_cbas_bucket,
                        "After Rollback : # Items in CBAS bucket does not match that in the CB bucket")
    