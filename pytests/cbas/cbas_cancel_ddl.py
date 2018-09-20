import time
import random
import json

from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection


class CBASCancelDDL(CBASBaseTest):

    def setUp(self):
        super(CBASCancelDDL, self).setUp()
        self.analytics_servers = []
        self.analytics_servers.append(self.cbas_node)

        self.log.info("Add CBAS nodes")
        self.add_node(self.servers[1], services=["cbas"], rebalance=False)
        self.analytics_servers.append(self.servers[1])

        self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True)
        self.analytics_servers.append(self.cbas_servers[0])

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Load documents in KV")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items, batch_size=1000)

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_dataset_create,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """
    def test_cancel_ddl_dataset_create(self):
        """
        Cover's the scenario: Cancel create dataset DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed

        steps:
        1.  Add all CBAS nodes(At least 3)
        2.  Create connection to KV bucket
        3.  Load documents in KV
        4.  Disconnect link Local
        5.  Drop dataset if exist
        6.  Pick a time window to sleep before killing Java service
        7.  Pick a CBAS node to kill
        8.  Pick the Java process id on CBAS node selected in previous step
        9.  Create dataset/sleep/kill java process
        10. Wait for service to be up using ping function
        11. Check if dataset is created
        12. Repeat step 4 - 11 for a period of 10 minutes
        13. Make sure we see at least a few create dataset statements fail(No dataset created in step 11)
        """
        dataset_created = 0
        dataset_not_created = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 600:
            times += 1

            self.log.info("Disconnect link")
            self.assertTrue(self.cbas_util.disconnect_link(), msg="Disconnect link failed. Might be a product bug")

            self.log.info("Drop dataset if exists")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("drop dataset %s if exists" % self.cbas_dataset_name)
            self.assertEquals(status, "success", msg="Drop dataset failed")
            self.log.info(cbas_result)

            self.log.info("Pick a time window between 0 - 50ms for killing of node")
            self.kill_window = random.randint(0, 50) / 1000.0

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.analytics_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Create dataset/Sleep window/Kill Java process in parallel
            self.log.info("Create dataset")
            tasks = self.cbas_util.async_query_execute("create dataset %s on %s" % (self.cbas_dataset_name, self.cb_bucket_name), "immediate", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result()

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            cluster_recover_start_time = time.time()
            while time.time() < cluster_recover_start_time + 180:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping()")
                    if status == "success":
                        break
                except:
                    self.sleep(2, message="Wait for service to up again")

            self.log.info("Check DDL create dataset status")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util(
                'select value count(*) from Metadata.`Dataset` d WHERE d.DataverseName <> "Metadata" and DatasetName = "ds"')
            self.assertEquals(status, "success", msg="CBAS query failed")
            self.log.info(cbas_result)
            if cbas_result[0] == 1:
                dataset_created += 1
                self.assertTrue(self.cbas_util.connect_link(), msg="Connect link Failed")
                self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")
            else:
                dataset_not_created += 1

            # Let's break out as soon as one DDL is cancelled
            if dataset_created != dataset_not_created and dataset_created > 0 and dataset_not_created > 0:
                break

        self.log.info("Test run summary")
        self.log.info("Times ran: %d " % times)
        self.log.info("Dataset %s was created %d times" % (self.cbas_dataset_name, dataset_created))
        self.log.info("Dataset %s was not created %d times" % (self.cbas_dataset_name, dataset_not_created))
        # Uncomment below once when we get the timing right
        # self.assertFalse(times == dataset_created or times == dataset_not_created, msg="Please revisit test and update test such that few request cancel and few get processed")

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_dataset_drop,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """

    def test_cancel_ddl_dataset_drop(self):
        """
        Cover's the scenario: Cancel drop dataset DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed

        steps:
        1.  Add all CBAS nodes(At least 3)
        2.  Create connection to KV bucket
        3.  Load documents in KV
        4.  Disconnect link Local
        5.  Drop dataset if exist
        5.  Create dataset
        6.  Pick a time window to sleep before killing Java service
        7.  Pick a CBAS node to kill
        8.  Pick the Java process id on CBAS node selected in previous step
        9.  Drop dataset/sleep/kill java process
        10. Wait for service to be up using ping function
        11. Check if dataset is dropped
        12. Repeat step 4 - 11 for a period of 10 minutes
        13. Make sure we see at least a few drop dataset statements fail
        """
        dataset_dropped = 0
        dataset_not_dropped = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 600:
            times += 1

            self.log.info("Disconnect link")
            self.assertTrue(self.cbas_util.disconnect_link(), msg="Disconnect link failed. Might be a product bug")

            self.log.info("Drop dataset if exists")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("drop dataset %s if exists" % self.cbas_dataset_name)
            self.assertEquals(status, "success", msg="Drop dataset failed")

            self.log.info("Create dataset")
            self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg="Create dataset failed")

            self.log.info("Pick a time window between 0 - 50ms for killing of node")
            self.kill_window = random.randint(0, 50) / 1000.0

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.analytics_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Drop dataset/Sleep window/Kill Java process in parallel
            self.log.info("Drop dataset")
            tasks = self.cbas_util.async_query_execute("drop dataset %s" % self.cbas_dataset_name, "immediate", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result()

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            cluster_recover_start_time = time.time()
            while time.time() < cluster_recover_start_time + 180:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping();")
                    if status == "success":
                        break
                except:
                    self.sleep(2, message="Wait for service to up again")

            self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util(
                'select value count(*) from Metadata.`Dataset` d WHERE d.DataverseName <> "Metadata" and DatasetName = "ds"')
            self.assertEquals(status, "success", msg="CBAS query failed")
            self.log.info(cbas_result)
            if cbas_result[0] == 0:
                dataset_dropped += 1
                self.assertTrue(self.cbas_util.connect_link(), msg="Connect link Failed")
            else:
                dataset_not_dropped += 1

            # Let's break out as soon as one DDL is cancelled
            if dataset_dropped != dataset_not_dropped and dataset_dropped > 0 and dataset_not_dropped > 0:
                break

        self.log.info("Test run summary")
        self.log.info("Times ran %d" % times)
        self.log.info("Dataset %s was dropped %d times" % (self.cbas_dataset_name, dataset_dropped))
        self.log.info("Dataset %s was not dropped %d times" % (self.cbas_dataset_name, dataset_not_dropped))

        # Uncomment below once when we get the timing right
        # self.assertFalse(times == dataset_dropped or times == dataset_not_dropped, msg="Please revisit test and update such that few request cancel and few get processed")

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_index_create,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=100
    """

    def test_cancel_ddl_index_create(self):
        """
        Cover's the scenario: Cancel create index DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed

        steps:
        1.  Add all CBAS nodes(At least 3)
        2.  Create connection to KV bucket
        3.  Load documents in KV
        4.  Create dataset
        5.  Connect link Local
        5.  Assert on document count
        7.  Disconnect link
        8.  Drop index
        9.  Pick a time window to sleep before killing Java service
        10. Pick a CBAS node to kill
        11. Pick the Java process id on CBAS node selected in previous step
        12. Create sec index/sleep/kill java process
        13. Wait for service to be up using ping function
        14. Check if index was was created
        15. Repeat step 4 - 11 for a period of 10 minutes
        16. Make sure we see at least a few create index statements fail
        """

        self.log.info("Create dataset")
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name), msg="Create dataset failed. Might be a product bug")

        self.log.info("Connect to Local link")
        self.assertTrue(self.cbas_util.connect_link(), msg="Connect link failed. Might be a product bug")

        self.log.info("Assert document count")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

        self.log.info("Disconnect link")
        self.assertTrue(self.cbas_util.disconnect_link(), msg="Disconnect link failed. Might be a product bug")

        sec_idx_created = 0
        sec_idx_not_created = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 600:
            times += 1

            self.log.info("Drop index")
            self.cbas_util.execute_statement_on_cbas_util("drop index %s.%s" % (self.cbas_dataset_name, "sec_idx"))

            self.log.info("Pick a time window between 0 - 200ms for killing of node")
            self.kill_window = random.randint(0, 200) / 1000.0

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.analytics_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Create sec index/Sleep window/Kill Java process in parallel
            self.log.info("Create index")
            tasks = self.cbas_util.async_query_execute("create index sec_idx on ds(age:int)", "immediate", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result()

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            cluster_recover_start_time = time.time()
            while time.time() < cluster_recover_start_time + 180:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping();")
                    if status == "success":
                        break
                except:
                    self.sleep(2, message="Wait for service to up again")

            self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util('select value count(*) from Metadata.`Index` where IndexName = "sec_idx"')
            self.assertEquals(status, "success", msg="CBAS query failed")
            self.log.info(cbas_result)
            if cbas_result[0] == 1:
                sec_idx_created += 1
                status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util("select age from %s" % self.cbas_dataset_name)
                self.assertTrue(status == "success", "Select query failed")
            else:
                sec_idx_not_created += 1
                status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util("select age from %s" % self.cbas_dataset_name)
                self.assertTrue(status == "success", "Select query failed")

            # Let's break out as soon as one DDL is cancelled
            if sec_idx_created != sec_idx_not_created and sec_idx_created > 0 and sec_idx_not_created > 0:
                break

        self.log.info("Test run summary")
        self.log.info("Times ran %d" % times)
        self.log.info("Secondary index %s was created %d times" % (self.cbas_dataset_name, sec_idx_created))
        self.log.info("Secondary index %s was not created %d times" % (self.cbas_dataset_name, sec_idx_not_created))

        # Uncomment below once when we get the timing right
        # self.assertFalse(times == sec_idx_created or times == sec_idx_not_created, msg="Please revisit test and update such that few request cancel and few get processed")

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_index_drop,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=100
    """

    def test_cancel_ddl_index_drop(self):
        """
        Cover's the scenario: Cancel drop index DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed

        steps:
        1.  Add all CBAS nodes(At least 3)
        2.  Create connection to KV bucket
        3.  Load documents in KV
        4.  Create dataset
        5.  Connect link Local
        5.  Assert on document count
        7.  Disconnect link
        8.  Drop index
        9.  Pick a time window to sleep before killing Java service
        10. Pick a CBAS node to kill
        11. Pick the Java process id on CBAS node selected in previous step
        12. Create sec index/sleep/kill java process
        13. Wait for service to be up using ping function
        14. Check if index was was created
        15. Repeat step 4 - 11 for a period of 10 minutes
        16. Make sure we see at least a few drop index statements fail
        """

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        sec_idx_dropped = 0
        sec_idx_not_dropped = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 600:
            times += 1

            self.log.info("Create secondary index")
            self.cbas_util.execute_statement_on_cbas_util("create index idx_age on ds(age:int)")

            self.log.info("Connect to Local link")
            self.assertTrue(self.cbas_util.connect_link(), msg="Connect link failed. Might be a product bug")

            self.log.info("Assert document count")
            self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

            self.log.info("Disconnect link")
            self.assertTrue(self.cbas_util.disconnect_link(), msg="Disconnect link failed. Might be a product bug")

            self.log.info("Pick a time window between 0 - 100ms for killing of node")
            self.kill_window = random.randint(0, 100) / 1000.0

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.analytics_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Drop sec index/Sleep window/Kill Java process in parallel
            self.log.info("Create index")
            tasks = self.cbas_util.async_query_execute("drop index %s.%s" % (self.cbas_dataset_name, "sec_idx"), "immediate", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result()

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            cluster_recover_start_time = time.time()
            while time.time() < cluster_recover_start_time + 180:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping();")
                    if status == "success":
                        break
                except:
                    self.sleep(2, message="Wait for service to up again")

            self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
            status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util('select value count(*) from Metadata.`Index` where IndexName = "sec_idx"')
            self.assertEquals(status, "success", msg="CBAS query failed")
            self.log.info(cbas_result)
            if cbas_result[0] == 0:
                sec_idx_dropped += 1
                status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util("select age from %s" % self.cbas_dataset_name)
                self.assertTrue(status == "success", "Select query failed")
            else:
                sec_idx_not_dropped += 1
                status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util("select age from %s" % self.cbas_dataset_name)
                self.assertTrue(status == "success", "Select query failed")

            # Let's break out as soon as one DDL is cancelled
            if sec_idx_dropped != sec_idx_not_dropped and sec_idx_dropped > 0 and sec_idx_not_dropped > 0:
                break

        self.log.info("Test run summary")
        self.log.info("Times ran %d" % times)
        self.log.info("Secondary index %s was dropped %d times" % (self.cbas_dataset_name, sec_idx_dropped))
        self.log.info("Secondary index %s was not dropped %d times" % (self.cbas_dataset_name, sec_idx_not_dropped))

        # Uncomment below once when we get the timing right
        # self.assertFalse(times == sec_idx_dropped or times == sec_idx_not_dropped, msg="Please revisit test and update test such that few request cancel and few get processed")

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_link_connect,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=10000
    """

    def test_cancel_ddl_link_connect(self):
        """
        Cover's the scenario: Cancel link connect DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed

        steps:
        """
        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.assertTrue(self.cbas_util.connect_link(), msg="Connect link failed. Might be a product bug")

        self.log.info("Assert document count")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

        self.log.info("Disconnect link")
        self.assertTrue(self.cbas_util.disconnect_link(), msg="Disconnect link failed. Might be a product bug")

        link_connected = 0
        link_not_connected = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 600:
            times += 1

            self.log.info("Disconnect link")
            self.assertTrue(self.cbas_util.disconnect_link(), msg="Disconnect link failed. Might be a product bug")

            self.log.info("Pick a time window between 0 - 1000ms for killing of node")
            self.kill_window = random.randint(0, 1000) / 1000.0

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.analytics_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Connect link/Sleep window/Kill Java process in parallel
            self.log.info("Connect Link")
            tasks = self.cbas_util.async_query_execute("connect link Local", "immediate", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result()

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            cluster_recover_start_time = time.time()
            while time.time() < cluster_recover_start_time + 180:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping()")
                    if status == "success":
                        break
                except:
                    self.sleep(2, message="Wait for service to up again")

            self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
            status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
            self.assertTrue(status, msg="Fetch bucket state failed")
            content = json.loads(content)
            self.log.info(content)
            if content['buckets'][0]['state'] == "connected":
                link_connected += 1
                self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")
            else:
                link_not_connected += 1

            # Let's break out as soon as one DDL is cancelled
            if link_connected != link_not_connected and link_connected > 0 and link_not_connected > 0:
                break

        self.log.info("Test run summary")
        self.log.info("Times ran %d" % times)
        self.log.info("link Local was connected %d times" % link_connected)
        self.log.info("link Local was not connected %d times" % link_not_connected)

        # Uncomment below once when we get the timing right
        # self.assertFalse(times == link_connected or times == link_not_connected, msg="Please revisit test and update such that few request cancel and few get processed")

    """
    cbas.cbas_cancel_ddl.CBASCancelDDL.test_cancel_ddl_link_disconnect,default_bucket=True,cb_bucket_name=default,cbas_dataset_name=ds,items=50000
    """

    def test_cancel_ddl_link_disconnect(self):
        """
        Cover's the scenario: Cancel link disconnect DDL statement
        Expected Behaviour: Request sent will now either succeed or fail, or its connection will be abruptly closed

        steps:
        """
        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.assertTrue(self.cbas_util.connect_link(), msg="Connect link failed. Might be a product bug")

        self.log.info("Assert document count")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

        link_disconnected = 0
        link_not_disconnected = 0
        times = 0
        start_time = time.time()
        while time.time() < start_time + 60:
            times += 1

            self.log.info("Connect link")
            self.assertTrue(self.cbas_util.connect_link(), msg="Connect link failed. Might be a product bug")

            self.log.info("Pick a time window between 0 - 500ms for killing of node")
            self.kill_window = random.randint(0, 500) / 1000.0

            self.log.info("Pick the cbas node to kill java process")
            server_to_kill_java = self.analytics_servers[random.randint(0, 2)]
            shell = RemoteMachineShellConnection(server_to_kill_java)

            self.log.info("Pick the java process id to kill")
            java_process_id, _ = shell.execute_command("pgrep java")

            # Run the task Connect link/Sleep window/Kill Java process in parallel
            self.log.info("Disconnect Link")
            tasks = self.cbas_util.async_query_execute("disconnect link Local", "immediate", 1)

            self.log.info("Sleep for the window time")
            self.sleep(self.kill_window)

            self.log.info("kill Java process with id %s" % java_process_id[0])
            shell.execute_command("kill -9 %s" % (java_process_id[0]))

            self.log.info("Fetch task result")
            for task in tasks:
                task.get_result()

            self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
            cluster_recover_start_time = time.time()
            while time.time() < cluster_recover_start_time + 180:
                try:
                    status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping()")
                    if status == "success":
                        break
                except:
                    self.sleep(2, message="Wait for service to up again")

            self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
            status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
            self.assertTrue(status, msg="Fetch bucket state failed")
            content = json.loads(content)
            if content['buckets'][0]['state'] == "disconnected":
                link_disconnected += 1
                status, _, _, _, _ = self.cbas_util.execute_statement_on_cbas_util("select count(*) from %s" % self.cbas_dataset_name)
                self.assertTrue(status == "success", "Select query failed")
            else:
                link_not_disconnected += 1
                self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

            # Let's break out as soon as one DDL is cancelled
            if link_disconnected != link_not_disconnected and link_disconnected > 0 and link_not_disconnected > 0:
                break

        self.log.info("Test run summary")
        self.log.info("Times ran %d" % times)
        self.log.info("link Local was disconnected %d times" % link_disconnected)
        self.log.info("link Local was not disconnected %d times" % link_not_disconnected)

        # Uncomment below once when we get the timing right
        # self.assertFalse(times == link_disconnected or times == link_not_disconnected, msg="Please revisit test and update such that few request cancel and few get processed")

    def tearDown(self):
        super(CBASCancelDDL, self).tearDown()


class CBASCancelDDLWhileRebalance(CBASBaseTest):

    ERROR_SEVERITY = "fatal"
    ERROR_MESSAGE = "Operation cannot be performed during rebalance"
    ERROR_CODE = 23003

    DDL_S = [
        {
            "query": "create dataset ds_1 on default",
            "disconnect_link_before_ddl_query": True
        },
        {
            "query": "connect link Local",
            "disconnect_link_before_ddl_query": False
        },
        {
            "query": "disconnect link Local",
            "disconnect_link_before_ddl_query": False
        },
        {
            "query": "create index idx_age on ds(age:int)",
            "disconnect_link_before_ddl_query": True
        },
        {
            "query": "drop index ds.idx_age",
            "disconnect_link_before_ddl_query": True
        },
        {
            "query": "drop dataset ds1",
            "disconnect_link_before_ddl_query": True
        }
    ]

    def setUp(self):
        super(CBASCancelDDLWhileRebalance, self).setUp()

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Load documents in KV")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Connect to Local link")
        self.cbas_util.connect_link()

        self.log.info("Validate count on CBAS")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Count mismatch on CBAS")

    def assert_cancel_ddl_error_response(self, errors, status):
        self.assertEqual(status, CBASCancelDDLWhileRebalance.ERROR_SEVERITY, msg="Error message mismatch")
        self.assertEqual(errors[0]['msg'], CBASCancelDDLWhileRebalance.ERROR_MESSAGE, msg="Error message mismatch")
        self.assertEqual(errors[0]['code'], CBASCancelDDLWhileRebalance.ERROR_CODE, msg="Error code mismatch")

    """
    cbas.cbas_cancel_ddl.CBASCancelDDLWhileRebalance.test_ddl_are_cancelled_during_rebalance,default_bucket=True,cb_bucket_name=default,items=10000
    """
    def test_ddl_are_cancelled_during_rebalance(self):

        for ddl in CBASCancelDDLWhileRebalance.DDL_S:
            otp_nodes = []
            if ddl['disconnect_link_before_ddl_query']:
                self.log.info("Disconnect local link")
                self.cbas_util.disconnect_link()

            self.log.info("Rebalance in a CBAS node")
            otp_nodes.append(self.add_node(self.cbas_servers[0], rebalance=True, wait_for_rebalance_completion=False))

            self.log.info("Execute '%s' DDL statements, and verify DDL fail while rebalance is in progress" % ddl['query'])
            while True:
                if self.rest._rebalance_status_and_progress()[1] >= 25:
                    break
            status, _, errors, _, _ = self.cbas_util.execute_statement_on_cbas_util(ddl['query'])
            self.assert_cancel_ddl_error_response(errors, status)
            
            while True:
                if self.rest._rebalance_progress_status() != "running":
                    self.sleep(10, message="Sleep for 10 seconds after rebalance")
                    break

            self.log.info("Rebalance out the CBAS node")
            print(self.remove_node(otpnode=otp_nodes, wait_for_rebalance=True))

            self.log.info("Connect back link")
            self.cbas_util.connect_link()
