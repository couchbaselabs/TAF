import time

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from sdk_client3 import SDKClient
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class DurabilityFailureTests(DurabilityTestsBase):
    def setUp(self):
        super(DurabilityFailureTests, self).setUp()
        self.log.info("=== DurabilityFailureTests setup complete ===")

    def tearDown(self):
        super(DurabilityFailureTests, self).tearDown()

    def test_sync_write_in_progress(self):
        """
        Test to simulate sync_write_in_progress error and validate the behavior
        This will validate failure in majority of nodes, where durability will
        surely fail for all CRUDs

        1. Select nodes to simulate the error which will affect the durability
        2. Enable the specified error_scenario on the selected nodes
        3. Perform individual CRUDs and verify sync_write_in_progress errors
        4. Validate the end results
        """

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()

        half_of_num_items = max(int(self.num_items/2), 1)
        target_vbuckets = list()

        # Variable to hold one of the doc_generator object
        gen_loader = None

        # Select nodes to affect and open required shell_connections
        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"] = dict()
            vb_info["init"][node.ip] = cbstat_obj[node.ip].failover_stats(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(shell_conn[node.ip])
            # Fetch affected nodes' vb_num which are of type=replica
            target_vbuckets += cbstat_obj[node.ip].vbucket_list(
                self.bucket.name, vbucket_type="replica")

        # Initialize doc_generators to use for testing
        self.log.info("Creating doc_generators")
        gen_create = doc_generator(
            self.key, self.num_items, self.num_items+self.crud_batch_size,
            vbuckets=self.vbuckets, target_vbucket=target_vbuckets)
        gen_update = doc_generator(
            self.key, 0, self.num_items, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        gen_delete = doc_generator(
            self.key, 0, half_of_num_items, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        self.log.info("Done creating doc_generators")

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader = gen_create
        if self.doc_ops[0] == "update":
            gen_loader = gen_update
        if self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(20, msg="Wait for task_1 ops to reach the server")

        # SDK client for performing individual ops
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket)
        # Perform specified CRUD operation on sync_write docs
        while gen_create.has_next():
            key, value = gen_create.next()
            if self.with_non_sync_writes:
                pass
            else:
                if self.doc_ops[1] == "create":
                    _, fail = client.insert(
                        key, value, durability=self.durability_level,
                        timeout=self.durability_timeout)
                elif self.doc_ops[1] == "update":
                    _, fail = client.upsert(
                        key, value, durability=self.durability_level,
                        timeout=self.durability_timeout)
                elif self.doc_ops[1] == "delete":
                    _, fail = client.delete(
                        key, value, durability=self.durability_level,
                        timeout=self.durability_timeout)

            # Validate the returned error from the SDK
            if "sync_write_in_progress" not in fail["error"]:
                raise("Invalid exception: {0}".format(fail["error"]))

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_sync_write_in_progress_for_persist_active(self):
        """
        This test validates sync_write_in_progress error scenario with
        durability=persistActive

        1. Select a random node from cluster
        2. Get active & replica vbucket numbers from the target_node
        3. Simulate specified error on the target_node
        4. Perform CRUDs such that it affects the target_node as well
        5. Validate the CRUDs have the persist_active durability level
        6. Revert the simulated error_condition from the target_node
        7. Doc_loader_1 should all succeed with success
        """

        target_node = self.get_random_node()
        shell_conn = RemoteMachineShellConnection(target_node)
        cbstat_obj = Cbstats(shell_conn)
        error_sim = CouchbaseError(shell_conn)

        self.durability_level = "persistActive"

        half_of_num_items = max(int(self.num_items/2), 1)
        # Override the crud_batch_size
        self.crud_batch_size = 1000
        # Get active/replica vbucket list from the target_node
        active_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                    vbucket_type="active")
        replica_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                     vbucket_type="replica")

        # Initialize doc_generators to use for testing
        self.log.info("Creating doc_generators")
        gen_create = doc_generator(
            self.key, self.num_items, self.num_items+self.crud_batch_size,
            vbuckets=self.vbuckets)
        gen_update = doc_generator(
            self.key, 0, self.num_items, vbuckets=self.vbuckets)
        gen_delete = doc_generator(
            self.key, 0, half_of_num_items, vbuckets=self.vbuckets)
        self.log.info("Done creating doc_generators")

        # Perform specified action
        error_sim.create(self.simulate_error, bucket_name=self.bucket.name)

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader = gen_create
        if self.doc_ops[0] == "update":
            gen_loader = gen_update
        if self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(20, msg="Wait for task_1 ops to reach the server")

        # SDK client for performing individual ops
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket)
        # Perform specified CRUD operation on sync_write docs
        while gen_create.has_next():
            key, value = gen_create.next()
            if self.with_non_sync_writes:
                pass
            else:
                if self.doc_ops[1] == "create":
                    _, fail = client.insert(
                        key, value, durability=self.durability_level,
                        timeout=self.durability_timeout)
                elif self.doc_ops[1] == "update":
                    _, fail = client.upsert(
                        key, value, durability=self.durability_level,
                        timeout=self.durability_timeout)
                elif self.doc_ops[1] == "delete":
                    _, fail = client.delete(
                        key, value, durability=self.durability_level,
                        timeout=self.durability_timeout)

            # Validate the returned error from the SDK
            vb_num = self.bucket_util.get_vbucket_num_for_key(key,
                                                              self.vbuckets)
            if vb_num in active_vb_numbers or vb_num in replica_vb_numbers:
                self.assertTrue("error" in fail, msg="No failure detected")
                self.assertTrue("sync_write_in_progress" in fail["error"],
                                msg="Invalid exception: {0}"
                                .format(fail["error"]))
            else:
                self.assertTrue(fail["success"] is None,
                                msg="CRUD failed for vbucket {0}"
                                .format(vb_num))

        # Revert the introduced error condition
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_durability_with_persistence(self):
        """
        Test to introduce errors in persistence and perform CRUD operation.
        Make sure we see appropriate errors wrt to the nodes on which the
        errors are induced.

        1. Select a random node to disable the disk operations
        2. Introduce the specified disk scenario on the target_node
        3. Perform CRUD operation such that it affect vbuckets in all nodes
        4. Make sure CRUDs errors are present only for the node in which disk
           scenario was enabled
        5. Revert the scenario and retry the CRUD ops,
           so all CRUDs are successful
        6. Validate the stats to make sure it matches the values at the end
        """

        # Select target_node and create required objects
        target_node = self.get_random_node()
        shell_conn = RemoteMachineShellConnection(target_node)
        cbstat_obj = Cbstats(shell_conn)
        error_sim = CouchbaseError(shell_conn)
        vb_info = dict()
        doc_errors = dict()

        # Override the crud_batch_size
        self.crud_batch_size = 5000

        # doc_index_end = self.num_items + self.crud_batch_size

        # Get active/replica vbucket list from the target_node
        active_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                    vbucket_type="active")
        replica_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                     vbucket_type="replica")

        # Fetch vbuckets details from cbstats
        vb_info["init"] = cbstat_obj.failover_stats(self.bucket.name)

        # Create doc_generators
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_read = doc_generator(self.key, 0,
                                 int(self.num_items/2))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))

        # Induce the specified disk related error on the target_node
        error_sim.create(self.simulate_error, self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        doc_errors["create"] = self.task.jython_task_manager.get_task_result(
            tasks[0])
        doc_errors["update"] = self.task.jython_task_manager.get_task_result(
            tasks[1])
        doc_errors["read"] = self.task.jython_task_manager.get_task_result(
            tasks[2])
        doc_errors["delete"] = self.task.jython_task_manager.get_task_result(
            tasks[3])

        # Fetch the vbuckets stats after performing the CRUDs
        vb_info["withDiskIssue"] = cbstat_obj.failover_stats(self.bucket.name)

        # Verify cbstats for the affected vbuckets are not updated during CRUDs
        for vb in range(self.vbuckets):
            if vb in active_vb_numbers:
                for stat_name in vb_info["withDiskIssue"][vb].keys():
                    stat_before_crud = vb_info["init"][vb][stat_name]
                    stat_after_crud = vb_info["withDiskIssue"][vb][stat_name]
                    msg = "Stat '{0}' mismatch for vbucket '{1}'. {2} != {3}" \
                          .format(stat_name, vb, stat_before_crud,
                                  stat_after_crud)
                    self.assertTrue(stat_before_crud == stat_after_crud,
                                    msg=msg)

        # Local function to validate the returned error types
        def validate_doc_errors(crud_type):
            for doc in doc_errors[crud_type]:
                vb_num = self.bucket_util.get_vbucket_num_for_key(
                    doc["key"], self.vbuckets)
                if vb_num in active_vb_numbers:
                    self.assertTrue("durability_not_possible" in doc["error"],
                                    msg="Invalid exception {0}".format(doc))
                elif vb_num in replica_vb_numbers:
                    if self.num_nodes_affected == 1:
                        self.assertTrue(
                            "durability_not_possible" in doc["error"],
                            msg="Invalid exception {0}".format(doc))
                else:
                    self.assertTrue(doc["error"] is None,
                                    msg="Unexpected exception {0}".format(doc))

        # Verify the returned errors from doc_loader
        # Ideally there should be no errors should in doc reads
        self.assertTrue(len(doc_errors["read"]) == 0, msg="Error in doc reads")
        # For "create" doc_loader validate using function
        validate_doc_errors("create")
        validate_doc_errors("delete")
        validate_doc_errors("update")

        # Revert the induced error on the target_node
        error_sim.revert(self.simulate_error, self.bucket.name)

        # TODO: Logic to retry failed docs

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_bulk_sync_write_in_progress(self):
        """
        Test to simulate sync_write_in_progress error and validate the behavior
        This will validate failure in majority of nodes, where durability will
        surely fail for all CRUDs

        1. Select nodes to simulate the error which will affect the durability
        2. Enable the specified error_scenario on the selected nodes
        3. Perform CRUDs which results in sync_write_in_progress errors
        4. Validate the end results
        """

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()

        half_of_num_items = int(self.num_items/2)
        target_vbuckets = list()

        # Variable to hold one of the doc_generator object
        gen_loader = None

        # Select nodes to affect and open required shell_connections
        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"] = dict()
            vb_info["init"][node.ip] = cbstat_obj[node.ip].failover_stats(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(shell_conn[node.ip])
            # Fetch affected nodes' vb_num which are of type=replica
            target_vbuckets += cbstat_obj[node.ip].vbucket_list(
                self.bucket.name, vbucket_type="replica")

        # Initialize doc_generators to use for testing
        self.log.info("Creating doc_generators")
        gen_create = doc_generator(
            self.key, self.num_items, self.num_items+self.crud_batch_size,
            vbuckets=self.vbuckets, target_vbucket=target_vbuckets)
        gen_update = doc_generator(
            self.key, 0, self.num_items, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        gen_delete = doc_generator(
            self.key, 0, half_of_num_items, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        self.log.info("Done creating doc_generators")

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader = gen_create
        if self.doc_ops[0] == "update":
            gen_loader = gen_update
        if self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(20, msg="Wait for task_1 ops to reach the server")

        tem_durability = self.durability_level
        tem_timeout = self.durability_timeout
        if self.with_non_sync_writes:
            tem_durability = None
            tem_timeout = self.sdk_timeout

        # This will support both sync-write and non-sync-writes
        doc_loader_task_2 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[1], 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=tem_durability, timeout_secs=tem_timeout,
            retries=self.sdk_retries)

        # This task should be done will all sync_write_in_progress errors
        self.task.jython_task_manager.get_task_result(doc_loader_task_2)

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # TODO: Add validation to verify the sync_in_write_errors

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_bulk_sync_write_in_progress_for_persist_active(self):
        """
        This test validates sync_write_in_progress error scenario with
        durability=persistActive

        1. Select a random node from cluster
        2. Get active & replica vbucket numbers from the target_node
        3. Simulate specified error on the target_node
        4. Perform CRUDs such that it affects the target_node as well
        5. Validate the CRUDs have the persist_active durability level
        6. Revert the simulated error_condition from the target_node
        7. Doc_loader_1 should all succeed with success
        """

        target_node = self.get_random_node()
        shell_conn = RemoteMachineShellConnection(target_node)
        cbstat_obj = Cbstats(shell_conn)
        error_sim = CouchbaseError(shell_conn)

        self.durability_level = "persistActive"

        half_of_num_items = max(int(self.num_items/2), 1)
        # Override the crud_batch_size
        self.crud_batch_size = 1000
        # Get active/replica vbucket list from the target_node
        active_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                    vbucket_type="active")
        replica_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                     vbucket_type="replica")

        # Initialize doc_generators to use for testing
        self.log.info("Creating doc_generators")
        gen_create = doc_generator(
            self.key, self.num_items, self.num_items+self.crud_batch_size,
            vbuckets=self.vbuckets)
        gen_update = doc_generator(
            self.key, 0, self.num_items, vbuckets=self.vbuckets)
        gen_delete = doc_generator(
            self.key, 0, half_of_num_items, vbuckets=self.vbuckets)
        self.log.info("Done creating doc_generators")

        # Perform specified action
        error_sim.create(self.simulate_error, bucket_name=self.bucket.name)

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader = gen_create
        if self.doc_ops[0] == "update":
            gen_loader = gen_update
        if self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(30, msg="Wait for task_1 ops to reach the server")

        # Initialize tasks and store the task objects
        doc_loader_task_2 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        # Wait for doc_loader_task_2 to complete
        error_docs = self.task.jython_task_manager.get_task_result(
            doc_loader_task_2)

        for doc in error_docs:
            key = doc["key"]
            fail = doc["fail"]
            # Validate the returned error from the SDK
            vb_num = self.bucket_util.get_vbucket_num_for_key(key,
                                                              self.vbuckets)
            if vb_num in active_vb_numbers or vb_num in replica_vb_numbers:
                self.assertTrue("error" in fail, msg="No failure detected")
                self.assertTrue("sync_write_in_progress" in fail["error"],
                                msg="Invalid exception: {0}"
                                .format(fail["error"]))
            else:
                self.assertTrue(fail["success"] is None,
                                msg="CRUD failed for vbucket {0}"
                                .format(vb_num))

        # Revert the introduced error condition
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)


class TimeoutTests(DurabilityTestsBase):
    def setUp(self):
        super(TimeoutTests, self).setUp()
        self.log.info("=== DurabilityTimeoutTests setup complete ===")

    def tearDown(self):
        super(TimeoutTests, self).tearDown()

    def test_timeout_with_successful_crud(self):
        """
        Test to make sure timeout is handled in durability calls
        and no documents are loaded when durability cannot be met using
        error simulation in server node side.

        This will validate failure in majority of nodes, where durability will
        surely fail for all CRUDs

        1. Select a node from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify no operation succeeds
        4. Revert the error scenario from the cluster to resume durability
        5. Validate all mutations are succeeded after reverting
           the error condition

        Note: self.durability_timeout values is considered as 'seconds'
        """

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()

        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"] = dict()
            vb_info["init"][node.ip] = cbstat_obj[node.ip].failover_stats(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(shell_conn[node.ip])

        curr_time = int(time.time())
        expected_timeout = curr_time + self.durability_timeout
        time_to_wait = expected_timeout - 20

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))
        gen_read = doc_generator(self.key, int(self.num_items/3),
                                 int(self.num_items/2))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        self.sleep(time_to_wait, "Wait less than the durability_timeout value")

        # Fetch latest failover stats and validate the values are not changed
        for node in target_nodes:
            vb_info["withinTimeout"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)
            val = vb_info["init"][node.ip] == vb_info["withinTimeout"][node.ip]
            self.assertTrue(val, msg="Mismatch in failover stats")

        # Revert the specified error scenario
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)
        # Disconnect the shell connection
        shell_conn.disconnect()

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest failover stats and validate the values are updated
        for node in target_nodes:
            vb_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)
            val = vb_info["init"][node.ip] != vb_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="Vbucket seq_no stats not updated")

    def test_timeout_with_successful_crud_for_persist_active(self):
        """
        Test to validate timeouts during CRUDs with persistActive

        1. Select a random node from cluster
        2. Get active & replica vbucket numbers from the target_node
        3. Simulate specified error on the target_node
        4. Perform CRUDs such that it affects the target_node as well
        5. Validate the CRUDs failed on the target_node
        6. Revert the simulated error_condition from the target_node
        7. Make sure all CRUDs are successful
        """

        target_node = self.get_random_node()
        shell_conn = RemoteMachineShellConnection(target_node)
        cbstat_obj = Cbstats(shell_conn)
        error_sim = CouchbaseError(shell_conn)
        vb_info = dict()

        self.durability_level = "persistActive"

        curr_time = int(time.time())
        expected_timeout = curr_time + self.durability_timeout
        time_to_wait = expected_timeout - 20

        vb_info["init"] = cbstat_obj.failover_stats(self.bucket.name)

        # Perform specified action
        error_sim.create(self.simulate_error, bucket_name=self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))
        gen_read = doc_generator(self.key, int(self.num_items/3),
                                 int(self.num_items/2))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        self.sleep(time_to_wait, "Wait less than the durability_timeout value")

        # Fetch latest failover stats and validate the values are not changed
        vb_info["withinTimeout"] = cbstat_obj.failover_stats(self.bucket.name)
        val = vb_info["init"] == vb_info["withinTimeout"]
        self.assertTrue(val, msg="Mismatch in failover stats")

        # Revert the specified error scenario
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)
        # Disconnect the shell connection
        shell_conn.disconnect()

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest failover stats and validate the values are updated
        vb_info["afterCrud"] = cbstat_obj.failover_stats(self.bucket.name)
        val = vb_info["init"] != vb_info["afterCrud"]
        self.assertTrue(val, msg="Vbucket seq_no stats not updated")

    def test_timeout_with_crud_failures(self):
        """
        Test to make sure timeout is handled in durability calls
        and no documents are loaded when durability cannot be met using
        error simulation in server node side

        This will validate failure in majority of nodes, where durability will
        surely fail for all CRUDs

        1. Select a node from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify no operations succeeds
        4. Revert the error scenario from the cluster to resume durability
        5. Validate all mutations are succeeded after reverting
           the error condition

        Note: self.durability_timeout values is considered as 'seconds'
        """

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()

        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"] = dict()
            vb_info["init"][node.ip] = cbstat_obj[node.ip].failover_stats(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(shell_conn[node.ip])

        curr_time = int(time.time())
        expected_timeout = curr_time + self.durability_timeout

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))
        gen_read = doc_generator(self.key, int(self.num_items/3),
                                 int(self.num_items/2))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Check whether the timeout triggered properly
        timed_out_ok = int(time.time()) == expected_timeout \
            or int(time.time()) == expected_timeout + 1
        self.assertTrue(timed_out_ok, msg="Timed-out before expected time")

        # Revert the specified error scenario
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
        # Disconnect the shell connection
        shell_conn.disconnect()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest failover stats and validate the values are not changed
        for node in target_nodes:
            vb_info["post_timeout"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)
            val = vb_info["init"][node.ip] == vb_info["post_timeout"][node.ip]
            self.assertTrue(val, msg="Mismatch in failover stats with timeout")

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        # Retry the same CRUDs after reverting the failure environment
        tasks = []
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest failover stats and validate the values are updated
        for node in target_nodes:
            vb_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)
            val = vb_info["init"][node.ip] != vb_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="Vbucket seq_no stats not updated")

    def test_timeout_with_crud_failures_for_persist_active(self):
        """
        Test to validate timeouts during CRUDs with persistActive

        1. Select a random node from cluster
        2. Get active & replica vbucket numbers from the target_node
        3. Simulate specified error on the target_node
        4. Perform CRUDs such that it affects the target_node as well
        5. Validate the CRUDs failed on the target_node
        6. Revert the simulated error_condition from the target_node
        7. Retry failed CRUDs to make sure the durability is met
        """

        target_node = self.get_random_node()
        shell_conn = RemoteMachineShellConnection(target_node)
        cbstat_obj = Cbstats(shell_conn)
        error_sim = CouchbaseError(shell_conn)
        vb_info = dict()

        self.durability_level = "persistActive"

        # Get active/replica vbucket list from the target_node
        active_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                    vbucket_type="active")
        replica_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                     vbucket_type="replica")

        vb_info["init"] = cbstat_obj.failover_stats(self.bucket.name)
        curr_time = int(time.time())
        expected_timeout = curr_time + self.durability_timeout

        # Perform specified action
        error_sim.create(self.simulate_error, bucket_name=self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))
        gen_read = doc_generator(self.key, int(self.num_items/3),
                                 int(self.num_items/2))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Check whether the timeout triggered properly
        timed_out_ok = int(time.time()) == expected_timeout \
            or int(time.time()) == expected_timeout + 1
        self.assertTrue(timed_out_ok, msg="Timed-out before expected time")

        # Revert the specified error scenario
        error_sim.create(self.simulate_error, bucket_name=self.bucket.name)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest failover stats and validate the values are not changed
        vb_info["post_timeout"] = cbstat_obj.failover_stats(self.bucket.name)
        val = vb_info["init"] == vb_info["post_timeout"]
        self.assertTrue(val, msg="Mismatch in failover stats with timeout")

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        # Retry the same CRUDs after reverting the failure environment
        tasks = []
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Wait for document_loader tasks to complete
        for task in tasks:
            error_docs = self.task.jython_task_manager.get_task_result(task)

            for doc in error_docs:
                key = doc["key"]
                fail = doc["fail"]
                # Validate the returned error from the SDK
                vb_num = self.bucket_util.get_vbucket_num_for_key(
                    key, self.vbuckets)
                if vb_num in active_vb_numbers or vb_num in replica_vb_numbers:
                    self.assertTrue("error" in fail, msg="No failure detected")
                    self.assertTrue("sync_write_in_progress" in fail["error"],
                                    msg="Invalid exception: {0}"
                                    .format(fail["error"]))
                else:
                    self.assertTrue(fail["success"] is None,
                                    msg="CRUD failed for vbucket {0}"
                                    .format(vb_num))

        # Fetch latest failover stats and validate the values are updated
        vb_info["afterCrud"] = cbstat_obj.failover_stats(self.bucket.name)
        val = vb_info["init"] != vb_info["afterCrud"]
        self.assertTrue(val, msg="Vbucket seq_no stats not updated")

        # Revert the specified error scenario
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)
        # Disconnect the shell connection
        shell_conn.disconnect()

        for doc in error_docs:
            doc.retry()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
