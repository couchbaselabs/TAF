import json
import time

from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from cbas_base import CBASBaseTest
from couchbase_helper.documentgenerator import doc_generator
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException


class CBASBucketOperations(CBASBaseTest):
    def setUp(self):
        super(CBASBucketOperations, self).setUp()

        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes
           (and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes
           (and tests wants only 1 cbas node)
        3. There can be only 1 node running KV,CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas.
              For that service check on nodes is needed.
        '''

        if self.bucket_time_sync:
            self.bucket_util._set_time_sync_on_buckets(["default"])

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

    def tearDown(self):
        self.cleanup_cbas()
        super(CBASBucketOperations, self).tearDown()

    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first
            self.perform_doc_ops_in_all_cb_buckets(
                "create",
                0,
                self.num_items,
                durability=self.durability_level)
            self.bucket_util.verify_stats_all_buckets(self.num_items)

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write aborts before dataset creation")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name,
                    "replica")
                load_gen = doc_generator("test_abort_key", 0, self.num_items,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write aborts after dataset creation")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name,
                    "replica")
                load_gen = doc_generator("test_abort_key", 0, self.num_items,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        # Create indexes on the CBAS bucket
        self.create_secondary_indexes = \
            self.input.param("create_secondary_indexes", True)
        if self.create_secondary_indexes:
            self.index_fields = "profession:string,number:bigint"
            create_idx_statement = "create index {0} on {1}({2});".format(
                self.index_name, self.cbas_dataset_name, self.index_fields)
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(
                    create_idx_statement)

            self.assertTrue(status == "success", "Create Index query failed")

            self.assertTrue(
                self.cbas_util.verify_index_created(
                    self.index_name,
                    self.index_fields.split(","),
                    self.cbas_dataset_name)[0])

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write aborts after dataset connect")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name,
                    "replica")
                load_gen = doc_generator("test_abort_key", 0, self.num_items,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        if not skip_data_loading:
            # Validate no. of items in CBAS dataset
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cbas_dataset_name,
                    self.num_items):
                self.fail("No. of items in CBAS dataset do not match "
                          "that in the CB bucket")

    def load_docs_in_cb_bucket_before_cbas_connect(self):
        self.setup_for_test()

    def load_docs_in_cb_bucket_before_and_after_cbas_connect(self):
        self.setup_for_test()

        # Load more docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(
            "create",
            self.num_items,
            self.num_items * 2)
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)

        if self.test_abort_snapshot:
            self.log.info("Creating sync_write aborts after dataset connect")
            for server in self.cluster_util.get_kv_nodes():
                ssh_shell = RemoteMachineShellConnection(server)
                cbstats = Cbstats(ssh_shell)
                replica_vbs = cbstats.vbucket_list(
                    self.bucket_util.buckets[0].name,
                    "replica")
                load_gen = doc_generator("test_abort_key",
                                         self.num_items,
                                         self.num_items,
                                         target_vbucket=replica_vbs)
                success = self.bucket_util.load_durable_aborts(
                    ssh_shell, [load_gen],
                    self.bucket_util.buckets[0],
                    self.durability_level,
                    "update", "all_aborts")
                if not success:
                    self.log_failure("Simulating aborts failed")
                ssh_shell.disconnect()

            self.validate_test_failure()

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items * 2):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def load_docs_in_cb_bucket_after_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Load Couchbase bucket first.
        self.perform_doc_ops_in_all_cb_buckets(
            "create",
            0,
            self.num_items)
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def delete_some_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Delete some docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(
            "delete",
            0,
            self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items / 2):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def delete_all_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Delete all docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(
            "delete",
            0,
            self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def update_some_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Update some docs in Couchbase bucket
        self.perform_doc_ops_in_all_cb_buckets(
            "update",
            0,
            self.num_items / 10,
            mutation_num=1)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items,
                self.num_items / 10):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def update_all_docs_in_cb_bucket(self):
        self.setup_for_test()

        # Update all docs in Couchbase bucket
        self.perform_doc_ops_in_all_cb_buckets(
            "update",
            0,
            self.num_items, mutation_num=1)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items,
                self.num_items):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def create_update_delete_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Perform Create, Update, Delete ops in the CB bucket
        self.perform_doc_ops_in_all_cb_buckets(
            "create",
            self.num_items,
            self.num_items * 2)
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)
        self.perform_doc_ops_in_all_cb_buckets(
            "update",
            0,
            self.num_items,
            mutation_num=1)
        self.perform_doc_ops_in_all_cb_buckets(
            "delete",
            0,
            self.num_items / 2)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items * 3 / 2,
                self.num_items / 2):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def create_update_delete_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Perform Create, Update, Delete ops in the CB bucket
        self.perform_doc_ops_in_all_cb_buckets(
            "create",
            self.num_items,
            self.num_items * 2)
        self.perform_doc_ops_in_all_cb_buckets(
            "update",
            0,
            self.num_items,
            mutation_num=1)
        self.perform_doc_ops_in_all_cb_buckets(
            "delete",
            0,
            self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items * 3 / 2,
                self.num_items / 2):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def flush_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Flush the CB bucket
        BucketHelper(self.cluster.master).flush_bucket(self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def flush_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Flush the CB bucket
        BucketHelper(self.cluster.master).flush_bucket(self.cb_bucket_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def delete_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Delete the CB bucket
        self.cluster.bucket_delete(server=self.cluster.master,
                                   bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def delete_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Delete the CB bucket
        self.cluster.bucket_delete(server=self.cluster.master,
                                   bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    """
    cbas.cbas_bucket_operations.CBASBucketOperations:
      delete_kv_bucket_then_drop_dataset_without_disconnecting_link,
      cb_bucket_name=default,cbas_bucket_name=default_cbas,
      cbas_dataset_name=ds,num_items=10000
    """
    def delete_kv_bucket_then_drop_dataset_without_disconnecting_link(self):
        # setup test
        self.setup_for_test()

        # Delete the KV bucket
        deleted = BucketHelper(self.cluster.master).delete_bucket()
        self.assertTrue(deleted, "Deletion of KV bucket failed")

        # Check Bucket state
        start_time = time.time()
        while start_time + 120 > time.time():
            status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
            self.assertTrue(status, msg="Fetch bucket state failed")
            content = json.loads(content)
            self.log.info(content)
            if content['buckets'][0]['state'] == "disconnected":
                break
            self.sleep(1)

        # Drop dataset with out disconnecting the Link
        self.sleep(2, message="Sleeping 2 seconds after bucket disconnect")
        self.assertTrue(self.cbas_util.drop_dataset(self.cbas_dataset_name),
                        msg="Failed to drop dataset")

    def compact_cb_bucket_with_cbas_connected(self):
        self.setup_for_test()

        # Compact the CB bucket
        BucketHelper(self.cluster.master).flush_bucket(self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    def compact_cb_bucket_then_cbas_connect(self):
        self.setup_for_test()

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        # Compact the CB bucket
        BucketHelper(self.cluster.master).compact_bucket(self.cb_bucket_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items):
            self.fail("No. of items in CBAS dataset do not match"
                      "that in the CB bucket")

    def test_ingestion_resumes_on_reconnect(self):
        self.setup_for_test()

        self.perform_doc_ops_in_all_cb_buckets(
            "update",
            0,
            self.num_items / 4,
            mutation_num=1)

        self.cbas_util.validate_cbas_dataset_items_count(
            self.cbas_dataset_name,
            self.num_items,
            self.num_items / 4)

        # Disconnect from bucket
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        self.perform_doc_ops_in_all_cb_buckets(
            "update",
            self.num_items / 4,
            self.num_items / 2,
            mutation_num=1)

        # Connect to Bucket and sleep for 2s to allow ingestion to start
        self.cbas_util.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        self.sleep(5)

        # Validate no. of items in CBAS dataset
        count, mutated_count = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)

        if not (self.num_items / 4 < mutated_count):
            self.fail("Count after bucket connect = %s. "
                      "Ingestion has restarted." % mutated_count)
        else:
            self.log.info("Count after bucket connect = %s", mutated_count)

    def test_ingestion_after_kv_rollback(self):
        self.setup_for_test()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on NodeA & NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[0],
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[1],
                                                         self.cb_bucket_name)
        mem_client.stop_persistence()

        # Perform Create, Update, Delete ops in the CB bucket
        self.log.info("Performing Mutations")
        self.perform_doc_ops_in_all_cb_buckets(
            "delete",
            0,
            self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name,
                self.num_items / 2, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

        # Count no. of items in CB & CBAS Buckets
        items_in_cb_bucket = self.bucket_util.get_item_count_mc(
            self.cluster.master,
            self.cb_bucket_name)
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)
        self.log.info("Before Rollback --- # docs in CB bucket: %s, "
                      "# docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        if items_in_cb_bucket != items_in_cbas_bucket:
            self.fail("Before Rollback: # Items in CBAS bucket does not match "
                      "that in the CB bucket")

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        mem_client = MemcachedClientHelper.direct_client(self.input.servers[1],
                                                         self.cb_bucket_name)
        mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(10)
        failover_task = self._cb_cluster.async_failover(
            self.input.servers,
            [self.input.servers[1]])
        failover_task.result()

        # Wait for Failover & CBAS rollback to complete
        self.sleep(120)

        # Count no. of items in CB & CBAS Buckets
        items_in_cb_bucket = self.bucket_util.get_item_count_mc(
            self.cluster.master,
            self.cb_bucket_name)
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)
        self.log.info("After Rollback --- # docs in CB bucket: %s, "
                      "# docs in CBAS bucket : %s",
                      items_in_cb_bucket, items_in_cbas_bucket)

        if items_in_cb_bucket != items_in_cbas_bucket:
            self.fail("After Rollback: # Items in CBAS bucket does not match "
                      "that in the CB bucket")

    '''
    cbas.cbas_bucket_operations.CBASBucketOperations:
      test_bucket_flush_while_index_are_created,cb_bucket_name=default,
      cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,
      items=100000,index_fields=profession:String-first_name:String
    '''
    def test_bucket_flush_while_index_are_created(self):
        self.log.info("Add documents, create CBAS buckets, "
                      "dataset and validate count")
        self.setup_for_test()

        self.log.info('Disconnect CBAS bucket')
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)

        self.log.info('Create secondary index in Async')
        index_fields = self.input.param("index_fields", None)
        index_fields = index_fields.replace('-', ',')
        query = "create index {0} on {1}({2});" \
            .format("sec_idx", self.cbas_dataset_name, index_fields)
        create_index_task = self.task.async_cbas_query_execute(
            self.cluster.master, self.cbas_util, None, query, 'default')

        self.log.info('Flush bucket while index are getting created')
        # Flush the CB bucket
        BucketHelper(self.cluster.master).flush_bucket(self.cb_bucket_name)

        self.log.info('Get result on index creation')
        self.task_manager.get_task_result(create_index_task)

        self.log.info('Connect back cbas bucket')
        self.cbas_util.connect_to_bucket(self.cbas_bucket_name)

        self.log.info('Validate no. of items in CBAS dataset')
        if not self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, 0):
            self.fail("No. of items in CBAS dataset do not match "
                      "that in the CB bucket")

    '''
    cbas.cbas_bucket_operations.CBASBucketOperations:
      test_kill_memcached_impact_on_bucket,num_items=100000,
      bucket_type=ephemeral
    '''
    def test_kill_memcached_impact_on_bucket(self):
        self.log.info('Add documents, create CBAS buckets, '
                      'dataset and validate count')
        self.setup_for_test()

        self.log.info('Kill memcached service in all cluster nodes')
        for node in self.cluster.servers:
            RemoteMachineShellConnection(node).kill_memcached()

        self.log.info('Validate document count')
        _ = self.rest.query_tool(
            'select count(*) from %s'
            % self.cb_bucket_name)['results'][0]['$1']
        self.cbas_util.validate_cbas_dataset_items_count(
            self.cbas_dataset_name, 0)

    '''
    cbas.cbas_bucket_operations.CBASBucketOperations:
      test_restart_kv_server_impact_on_bucket,num_items=100000,
      bucket_type=ephemeral
    '''
    def test_restart_kv_server_impact_on_bucket(self):
        self.log.info('Add documents, create CBAS buckets, '
                      'dataset and validate count')
        self.setup_for_test()

        self.log.info('Restart couchbase')
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.reboot_server_and_wait_for_cb_run(self.cluster_util,
                                                self.cluster.master)

        self.log.info('Validate document count')
        count_n1ql = self.rest.query_tool(
            'select count(*) from %s'
            % self.cb_bucket_name)['results'][0]['$1']
        self.cbas_util.validate_cbas_dataset_items_count(
            self.cbas_dataset_name, count_n1ql)


class CBASEphemeralBucketOperations(CBASBaseTest):
    def setUp(self, add_default_cbas_node=True):
        super(CBASEphemeralBucketOperations, self).setUp(add_default_cbas_node)

        self.log.info("Create Ephemeral bucket")
        self.bucket_ram = self.input.param("bucket_size", 100)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type,
            ram_quota=self.bucket_size,
            replica=self.num_replicas,
            conflict_resolution=self.bucket_conflict_resolution_type,
            replica_index=self.bucket_replica_index,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.log.info("Fetch RAM document load percentage")
        self.document_ram_percentage = \
            self.input.param("document_ram_percentage", 0.90)

    def load_document_until_ram_percentage(self):
        self.start = 0
        doc_batch_size = 5000
        self.end = doc_batch_size
        bucket_helper = BucketHelper(self.cluster.master)
        mem_cap = (self.document_ram_percentage
                   * self.bucket_ram
                   * 1000000)
        while True:
            self.log.info("Add documents to bucket")
            self.perform_doc_ops_in_all_cb_buckets(
                "create",
                self.start,
                self.end,
                durability=self.durability_level)

            self.log.info("Calculate available free memory")
            bucket_json = bucket_helper.get_bucket_json(self.cb_bucket_name)
            mem_used = 0
            for node_stat in bucket_json["nodes"]:
                mem_used += node_stat["interestingStats"]["mem_used"]

            if mem_used < mem_cap:
                self.log.info("Memory used: %s < %s" % (mem_used, mem_cap))
                self.start = self.end
                self.end = self.end + doc_batch_size
                self.num_items = self.end
            else:
                break

    """
    cbas.cbas_bucket_operations.CBASEphemeralBucketOperations:
      test_no_eviction_impact_on_cbas,default_bucket=False,num_items=0,
      bucket_type=ephemeral,eviction_policy=noEviction,
      cb_bucket_name=default,cbas_dataset_name=ds,bucket_ram=100,
      document_ram_percentage=0.85
    """
    def test_no_eviction_impact_on_cbas(self):

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                                self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Add documents until ram percentage")
        self.load_document_until_ram_percentage()

        self.log.info("Fetch current document count")
        target_bucket = None
        self.bucket_util.get_all_buckets()
        for tem_bucket in self.bucket_util.buckets:
            if tem_bucket.name == self.cb_bucket_name:
                target_bucket = tem_bucket
                break
        item_count = target_bucket.stats.itemCount
        self.log.info("Completed base load with %s items" % item_count)

        self.log.info("Load more until we are out of memory")
        client = SDKClient([self.cluster.master], target_bucket)
        i = item_count
        op_result = {"status": True}
        while op_result["status"] is True:
            op_result = client.crud("create",
                                    "key-id" + str(i),
                                    '{"name":"dave"}',
                                    durability=self.durability_level)
            i += 1

        if SDKException.AmbiguousTimeoutException not in op_result["error"] \
                or SDKException.RetryReason.KV_TEMPORARY_FAILURE \
                not in op_result["error"]:
            client.close()
            self.fail("Invalid exception for OOM insert: %s" % op_result)

        self.log.info('Memory is full at {0} items'.format(i))
        self.log.info("As a result added more %s items" % (i - item_count))

        self.log.info("Fetch item count")
        target_bucket = None
        self.bucket_util.get_all_buckets()
        for tem_bucket in self.bucket_util.buckets:
            if tem_bucket.name == self.cb_bucket_name:
                target_bucket = tem_bucket
                break
        item_count_when_oom = target_bucket.stats.itemCount
        mem_when_oom = target_bucket.stats.memUsed
        self.log.info('Item count when OOM {0} and memory used {1}'
                      .format(item_count_when_oom, mem_when_oom))

        self.log.info("Validate document count on CBAS")
        count_n1ql = self.rest.query_tool(
            'select count(*) from %s'
            % self.cb_bucket_name)['results'][0]['$1']
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(
            self.cbas_dataset_name, count_n1ql),
            msg="Count mismatch on CBAS")

    """
    cbas.cbas_bucket_operations.CBASEphemeralBucketOperations:
      test_nru_eviction_impact_on_cbas,default_bucket=False,num_items=0,
      bucket_type=ephemeral,eviction_policy=nruEviction,
      cb_bucket_name=default,cbas_dataset_name=ds,bucket_ram=100,
      document_ram_percentage=0.80
    """
    def test_nru_eviction_impact_on_cbas(self):
        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                                self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Add documents until ram percentage")
        self.load_document_until_ram_percentage()

        self.log.info("Fetch current document count")
        target_bucket = None
        self.bucket_util.get_all_buckets()
        for tem_bucket in self.bucket_util.buckets:
            if tem_bucket.name == self.cb_bucket_name:
                target_bucket = tem_bucket
                break

        item_count = target_bucket.stats.itemCount
        self.log.info("Completed base load with %s items" % item_count)

        self.log.info("Get initial inserted 100 docs, so they aren't removed")
        client = SDKClient([self.cluster.master], target_bucket)
        for doc_index in range(100):
            doc_key = "test_docs-" + str(doc_index)
            client.read(doc_key)

        self.log.info("Add 20% more items to trigger NRU")
        for doc_index in range(item_count, int(item_count * 1.2)):
            doc_key = "key_id-" + str(doc_index)
            op_result = client.crud("create",
                                    doc_key,
                                    '{"name":"dave"}',
                                    durability=self.durability_level)
            if op_result["status"] is False:
                self.log.warning("Insert failed for %s: %s"
                                 % (doc_key, op_result))

        # Disconnect the SDK client
        client.close()

        self.log.info("Validate document count on CBAS")
        count_n1ql = self.rest.query_tool(
            'select count(*) from %s'
            % self.cb_bucket_name)['results'][0]['$1']
        if self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, count_n1ql):
            pass
        else:
            self.log.info("Document count mismatch might be due to ejection "
                          "of documents on KV. Retry again")
            count_n1ql = self.rest.query_tool(
                'select count(*) from %s'
                % self.cb_bucket_name)['results'][0]['$1']
            self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(
                self.cbas_dataset_name, count_n1ql),
                msg="Count mismatch on CBAS")
