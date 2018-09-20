import time
import json

from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.tuq_generators import JsonGenerator


class CBASDCPState(CBASBaseTest):

    def setUp(self):
        super(CBASDCPState, self).setUp()

        self.log.info("Establish remote connection to CBAS node and Empty analytics log")
        self.shell = RemoteMachineShellConnection(self.cbas_node)
        self.shell.execute_command("echo '' > /opt/couchbase/var/lib/couchbase/logs/analytics*.log")
        
        self.log.info("Load documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[1], services=["cbas"], rebalance=True), msg="Failed to add CBAS node")
        
        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Validate count on CBAS")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")
        
        self.log.info("Kill CBAS/JAVA Process on NC node")
        self.shell.kill_multiple_process(['java', 'cbas'])

    """
    test_dcp_state_with_cbas_bucket_connected_kv_bucket_deleted,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_connected_kv_bucket_deleted(self):
        """
        Cover's the scenario: CBAS bucket is connected, KV bucket is deleted
        Expected Behaviour: Rebalance must pass once we receive DCP state API response
        """
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Check DCP state")
        start_time = time.time()
        dcp_state_captured = False
        while time.time() < start_time + 120:
            try:
                status, content, _  = self.cbas_util.fetch_dcp_state_on_cbas(self.cbas_dataset_name)
                if status:
                    dcp_state_captured = True
                    content = json.loads(content)
                    break
            except:
                pass
        
        self.log.info("Check DCP state is inconsistent, and rebalance passes since KV bucket does not exist and we don't care about the state")
        self.assertTrue(dcp_state_captured, msg="DCP state not found. Failing the test")
        self.assertFalse(content["exact"], msg="DCP state is consistent. Failing the test since subsequent rebalance will pass.")
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False), msg="Failed to add CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        rebalance_success = False
        try:
            rebalance_success = self.rebalance()
        except Exception as e:
            pass
        self.assertTrue(rebalance_success, msg="Rebalance in of CBAS node must succeed since DCP state API returned success")

    """
    test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted(self):
        """
        Cover's the scenario: CBAS bucket is disconnected, KV bucket deleted
        Expected Behaviour: Rebalance must succeeds and we must see in logs "Bucket Bucket:Default.cbas doesn't exist in KV anymore... nullifying its DCP state"
        """
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_link()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
        
        self.log.info("Grep Analytics logs for message")
        result, _ = self.shell.execute_command("grep 'exist in KV anymore... nullifying its DCP state' /opt/couchbase/var/lib/couchbase/logs/analytics*.log")
        self.assertTrue("nullifying its DCP state" in result[0], msg="Expected message 'nullifying its DCP state' not found")
    
    """
    test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted_and_recreate,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_kv_bucket_deleted_and_recreate(self):
        """
        Cover's the scenario: CBAS bucket is disconnected, CB bucket is deleted and then recreated
        Expected Behaviour: Rebalance succeeds with message in logs "Bucket Bucket:Default.cbas doesn't exist in KV anymore... nullifying its DCP state, then again after bucket is re-created, data is re-ingested from 0"
        """
        self.log.info("Delete KV bucket")
        self.delete_bucket_or_assert(serverInfo=self.master)
        
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_link()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node failed")
        
        self.log.info("Grep Analytics logs for message")
        result, _ = self.shell.execute_command("grep 'exist in KV anymore... nullifying its DCP state' /opt/couchbase/var/lib/couchbase/logs/analytics*.log")
        self.assertTrue("nullifying its DCP state" in result[0], msg="Expected message 'nullifying its DCP state' not found")
        
        self.log.info("Recreate KV bucket")
        self.create_default_bucket()
        
        self.log.info("Load documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items // 100, "create", 0, self.num_items // 100)

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)
        
        self.log.info("Connect to Local link")
        self.cbas_util.connect_link(with_force=True)
        
        self.log.info("Validate count on CBAS post KV bucket re-created")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items // 100), msg="Count mismatch on CBAS")
          
    """
    test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000,user_action=connect_cbas_bucket
    test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_dcp_state_with_cbas_bucket_disconnected_cb_bucket_exist(self):
        """
        Cover's the scenario: CBAS bucket is disconnected
        Expected Behaviour: Rebalance fails with user action Connect the bucket or drop the dataset"
        """
        self.log.info("Disconnect from CBAS bucket")
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                self.cbas_util.disconnect_link()
                break
            except Exception as e:
                pass
        
        self.log.info("Add a CBAS nodes")
        self.assertTrue(self.add_node(self.servers[3], services=["cbas"], rebalance=False),
                         msg="Failed to add a CBAS node")
        
        self.log.info("Rebalance in CBAS node")
        rebalance_success = False
        try:
            rebalance_success = self.rebalance()
        except Exception as e:
            pass

        if rebalance_success == False:
            self.log.info("Grep Analytics logs for user action as rebalance in Failed")
            result, _ = self.shell.execute_command("grep 'Datasets in different partitions have different DCP states.' /opt/couchbase/var/lib/couchbase/logs/analytics*.log")
            self.assertTrue("User action: Connect the bucket:" in result[0] and "or drop the dataset: Default.ds" in result[0], msg="User action not found.")

            user_action = self.input.param("user_action", "drop_dataset")
            if user_action == "connect_cbas_bucket":
                self.log.info("Connect back Local link")
                self.cbas_util.connect_link()
                self.sleep(15, message="Wait for link to be connected")
            else:
                self.log.info("Dropping the dataset")
                self.cbas_util.drop_dataset(self.cbas_dataset_name)
        
            self.log.info("Rebalance in CBAS node")
            self.assertTrue(self.rebalance(), msg="Rebalance in CBAS node must succeed after user has taken the specified action.")
        else:
            self.log.info("Rebalance was successful as DCP state were consistent")

          
    def tearDown(self):
        super(CBASDCPState, self).tearDown()


class CBASPendingMutations(CBASBaseTest):

    def setUp(self):
        super(CBASPendingMutations, self).setUp()

    """
    cbas.cbas_dcp_state.CBASPendingMutations.test_pending_mutations_idle_kv_system,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=200000
    """
    def test_pending_mutations_idle_kv_system(self):

        self.log.info("Load documents in KV")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items, batch_size=5000)

        self.log.info("Create dataset on the CBAS")
        self.cbas_util.create_dataset_on_bucket(
            self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect link")
        self.cbas_util.connect_link()

        self.log.info("Fetch cluster remaining mutations")
        aggregate_remaining_mutations_list = []
        while True:
            status, content, _ = self.cbas_util.fetch_pending_mutation_on_cbas_cluster()
            self.assertTrue(status, msg="Fetch pending mutations failed")
            content = json.loads(content)
            if content:
                aggregate_remaining_mutations_list.append(content["Default.ds"])
                total_count, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                if total_count == self.num_items:
                    break
                    
        self.log.info("Verify remaining mutation count is reducing as ingestion progress's")
        self.log.info(aggregate_remaining_mutations_list)
        is_remaining_mutation_count_reducing = True
        
        for i in range(len(aggregate_remaining_mutations_list)):
            if aggregate_remaining_mutations_list[i] > self.num_items or aggregate_remaining_mutations_list[i] < 0:
                self.fail("Remaining mutation count must not be greater than total documents and must be non -ve")
                
        for i in range(1, len(aggregate_remaining_mutations_list)):
            if not aggregate_remaining_mutations_list[i-1] >= aggregate_remaining_mutations_list[i]:
                is_remaining_mutation_count_reducing = False
                break
        
        self.log.info("Assert mutation progress API response")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")
        self.assertTrue(len(aggregate_remaining_mutations_list) > 1, msg="Found no remaining mutations during ingestion")
        self.assertTrue(is_remaining_mutation_count_reducing, msg="Remaining mutation count must reduce as ingestion progress's")
        
    """
    cbas.cbas_dcp_state.CBASPendingMutations.test_pending_mutations_busy_kv_system,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=cbas,cbas_dataset_name=ds,items=100000
    """
    def test_pending_mutations_busy_kv_system(self):

        self.log.info("Load documents in KV")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create dataset on the CBAS")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect link")
        self.cbas_util.connect_link()
        
        self.log.info("Perform async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 4, start=self.num_items)
        kv_task = self._async_load_all_buckets(self.master, generators, "create", 0, batch_size=3000)
        
        self.log.info("Fetch cluster remaining mutations")
        aggregate_remaining_mutations_list = []
        while True:
            status, content, _ = self.cbas_util.fetch_pending_mutation_on_cbas_cluster()
            self.assertTrue(status, msg="Fetch pending mutations failed")
            content = json.loads(content)
            if content:
                aggregate_remaining_mutations_list.append(content["Default.ds"])
                total_count, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                if total_count == self.num_items * 4:
                    break
        
        self.log.info("Get KV ops result")
        for task in kv_task:
            task.get_result()

        self.log.info("Verify remaining mutation count is reducing as ingestion progress's")
        self.log.info(aggregate_remaining_mutations_list)
        is_remaining_mutation_count_reducing = True

        for i in range(len(aggregate_remaining_mutations_list)):
            if aggregate_remaining_mutations_list[i] < 0:
                self.fail("Remaining mutation count must be non -ve")

        for i in range(1, len(aggregate_remaining_mutations_list)):
            if not aggregate_remaining_mutations_list[i-1] >= aggregate_remaining_mutations_list[i]:
                is_remaining_mutation_count_reducing = False
                break
        
        self.log.info("Assert mutation progress API response")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 4), msg="Count mismatch on CBAS")
        self.assertTrue(len(aggregate_remaining_mutations_list) > 1, msg="Found no items during ingestion")
        self.assertFalse(is_remaining_mutation_count_reducing, msg="Remaining mutation must increase as ingestion progress's")
        
    def tearDown(self):
        super(CBASPendingMutations, self).tearDown()