import time

from cb_tools.cbstats import Cbstats
from couchbase_helper.durability_helper import DurabilityHelper
from couchbase_helper.documentgenerator import doc_generator
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from remote.remote_util import RemoteMachineShellConnection


class DurabilityFailureTests(DurabilityTestsBase):
    def setUp(self):
        super(DurabilityFailureTests, self).setUp()
        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster), self.durability_level)
        self.log.info("=== DurabilityFailureTests setup complete ===")

    def tearDown(self):
        super(DurabilityFailureTests, self).tearDown()

    def test_crud_failures(self):
        """
        Test to configure the cluster in such a way durability will always fail

        1. Try creating the docs with durability set
        2. Verify create failed with durability_not_possible exception
        3. Create docs using async_writes
        4. Perform update and delete ops with durability
        5. Make sure these ops also fail with durability_not_possible exception
        """

        tasks = list()
        vb_info = dict()
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        cbstat_obj = Cbstats(shell_conn)
        old_num_items = self.num_items
        self.num_items *= 2
        gen_load = doc_generator(self.key, old_num_items, self.num_items)
        err_msg = "Doc mutation succeeded with, "  \
                  "cluster size: {0}, replica: {1}" \
                  .format(len(self.cluster.nodes_in_cluster), self.num_replicas)

        # Fetch vbucket seq_no stats from vb_seqno command for verification
        vb_info["init"] = cbstat_obj.vbucket_seqno(self.bucket.name)

        # Perform durable SET operation
        d_create_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_load, "create",
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(d_create_task)

        # Fetch vbucket seq_no status from vb_seqno command after CREATE task
        vb_info["failure_stat"] = cbstat_obj.vbucket_seqno(self.bucket.name)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(old_num_items)
        self.assertTrue(len(d_create_task.fail.keys()) == old_num_items,
                        msg=err_msg)
        self.assertTrue(vb_info["init"] == vb_info["failure_stat"],
                        msg="Failover stats mismatch. {0} != {1}"
                            .format(vb_info["init"], vb_info["failure_stat"]))

        self.assertTrue(self.durability_helper.validate_durability_exception(
            d_create_task.fail, "durability_not_possible"),
            msg=err_msg)

        # Perform aync_write to create the documents
        async_create_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_load, "create",
            batch_size=10, process_concurrency=8,
            timeout_secs=self.durability_timeout)
        self.task.jython_task_manager.get_task_result(async_create_task)

        # Fetch vbucket seq_no status from vb_seqno command after async CREATEs
        vb_info["create_stat"] = cbstat_obj.vbucket_seqno(self.bucket.name)

        # Verify doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Start durable UPDATE operation
        tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_load, "update",
                batch_size=10, process_concurrency=4,
                durability=self.durability_level,
                timeout_secs=self.durability_timeout,
                retries=self.sdk_retries))
        # Start durable DELETE operation
        tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_load, "delete",
                batch_size=10, process_concurrency=4,
                durability=self.durability_level,
                timeout_secs=self.durability_timeout,
                retries=self.sdk_retries))

        # Wait for all tasks to complete and validate the exception
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

            self.assertTrue(len(task.fail.keys()) == self.num_items,
                            msg="Few keys have not received exceptions: {0}"
                                .format(task.fail))
            self.assertTrue(
                self.durability_helper.validate_durability_exception(
                    task.fail, "durability_not_possible"),
                msg=err_msg)

        # Verify doc count is unchanged due to durability failures
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch vbucket seq_no status from vb_seqno after UPDATE/DELETE task
        vb_info["failure_stat"] = cbstat_obj.vbucket_seqno(self.bucket.name)

        self.assertTrue(vb_info["create_stat"] == vb_info["failure_stat"],
                        msg="Failover stats mismatch. {0} != {1}"
                            .format(vb_info["failure_stat"],
                                    vb_info["create_stat"]))

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

        # Variable to hold one of the doc_generator objects
        gen_loader_1 = None
        gen_loader_2 = None

        # Select nodes to affect and open required shell_connections
        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"] = dict()
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])
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
            gen_loader_1 = gen_create
        elif self.doc_ops[0] == "update":
            gen_loader_1 = gen_update
        elif self.doc_ops[0] == "delete":
            gen_loader_1 = gen_delete
            self.num_items -= half_of_num_items

        if self.doc_ops[1] == "create":
            gen_loader_2 = gen_create
        elif self.doc_ops[1] == "update":
            gen_loader_2 = gen_update
        elif self.doc_ops[1] == "delete":
            gen_loader_2 = gen_delete

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader_1, self.doc_ops[0], 0,
            batch_size=self.crud_batch_size, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(20, message="Wait for task_1 ops to reach the server")

        # SDK client for performing individual ops
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket)
        # Perform specified CRUD operation on sync_write docs
        while gen_loader_2.has_next():
            key, value = gen_loader_2.next()
            if self.with_non_sync_writes:
                fail = client.crud(self.doc_ops[1], key, value=value, exp=0)
            else:
                fail = client.crud(self.doc_ops[1], key, value=value, exp=0,
                                   durability=self.durability_level,
                                   timeout=2, time_unit="seconds")

            # Validate the returned error from the SDK
            if "DurableWriteInProgressException" not in str(fail["error"]):
                self.log_failure("Invalid exception: {0}"
                                 .format(fail["error"]))

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Cannot retry for CREATE/DELETE operation. So doing only for UPDATE
        if self.doc_ops[1] == "update":
            # Retry doc_op after reverting the induced error
            while gen_loader_2.has_next():
                key, value = gen_loader_2.next()
                if self.with_non_sync_writes:
                    fail = client.crud(self.doc_ops[1], key, value=value, exp=0)
                else:
                    fail = client.crud(self.doc_ops[1], key, value=value, exp=0,
                                       durability=self.durability_level,
                                       timeout=self.durability_timeout,
                                       time_unit="seconds")
                if "error" in fail:
                    self.log_failure("CRUD failed without error condition: {0}"
                                     .format(fail))

        # Disconnect the client
        client.close()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

    def test_sync_write_in_progress_for_persist_active(self):
        """
        This test validates sync_write_in_progress error scenario with
        durability=MAJORITY_AND_PERSIST_ON_MASTER

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
        error_sim = CouchbaseError(self.log, shell_conn)

        self.durability_level = "MAJORITY_AND_PERSIST_ON_MASTER"

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
        gen_loader = None
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader = gen_create
        elif self.doc_ops[0] == "update":
            gen_loader = gen_update
        elif self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(20, message="Wait for task_1 ops to reach the server")

        # SDK client for performing individual ops
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket)
        # Perform specified CRUD operation on sync_write docs
        while gen_loader.has_next():
            key, value = gen_loader.next()
            if self.with_non_sync_writes:
                fail = client.crud(self.doc_ops[1], key, value=value, exp=0)
            else:
                fail = client.crud(self.doc_ops[1], key, value=value, exp=0,
                                   durability=self.durability_level,
                                   timeout=self.durability_timeout,
                                   time_unit="seconds")

            # Validate the returned error from the SDK
            vb_num = self.bucket_util.get_vbucket_num_for_key(key,
                                                              self.vbuckets)
            if vb_num in active_vb_numbers or vb_num in replica_vb_numbers:
                if "error" not in fail:
                    self.log_failure("No failure detected for {0}"
                                     .format(self.doc_ops[1]))
                if "DurableWriteInProgressException" not in str(fail["error"]):
                    self.log_failure("Invalid exception: {0}"
                                     .format(fail["error"]))
            else:
                if fail["success"] is not None:
                    self.log_failure("CRUD failed for vbucket {0}"
                                     .format(vb_num))

        # Revert the introduced error condition
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

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
        error_sim = CouchbaseError(self.log, shell_conn)
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
        vb_info["init"] = cbstat_obj.vbucket_seqno(self.bucket.name)

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

        # Wait for all tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_all_result(task)

        # Get the failed docs from the tasks
        doc_errors["create"] = tasks[0].fail
        doc_errors["update"] = tasks[1].fail
        doc_errors["read"] = tasks[2].fail
        doc_errors["delete"] = tasks[3].fail

        # Fetch the vbuckets stats after performing the CRUDs
        vb_info["withDiskIssue"] = cbstat_obj.vbucket_seqno(self.bucket.name)

        # Verify cbstats for the affected vbuckets are not updated during CRUDs
        for vb in range(self.vbuckets):
            if vb in active_vb_numbers:
                for stat_name in vb_info["withDiskIssue"][vb].keys():
                    stat_before_crud = vb_info["init"][vb][stat_name]
                    stat_after_crud = vb_info["withDiskIssue"][vb][stat_name]
                    if stat_before_crud != stat_after_crud:
                        self.log_failure(
                            "Stat '{0}' mismatch for vbucket '{1}'. {2} != {3}"
                            .format(stat_name, vb, stat_before_crud,
                                    stat_after_crud))

        # Local function to validate the returned error types
        def validate_doc_errors(crud_type):
            for doc in doc_errors[crud_type]:
                vb_num = self.bucket_util.get_vbucket_num_for_key(
                    doc["key"], self.vbuckets)
                if vb_num in active_vb_numbers:
                    if "durability_not_possible" not in str(doc["error"]):
                        self.log_failure("Invalid exception {0}".format(doc))
                elif vb_num in replica_vb_numbers:
                    if self.num_nodes_affected == 1 \
                            and "durability_not_possible" not in doc["error"]:
                        self.log_failure("Invalid exception {0}".format(doc))
                else:
                    if doc["error"] is not None:
                        self.log_failure("Unexpected exception {0}"
                                        .format(doc))

        # Verify the returned errors from doc_loader
        # Ideally there should be no errors should in doc reads
        if len(doc_errors["read"]) != 0:
            self.log_failure("Error in doc reads")
        # For "create" doc_loader validate using function
        validate_doc_errors("create")
        validate_doc_errors("delete")
        validate_doc_errors("update")

        # Revert the induced error on the target_node
        error_sim.revert(self.simulate_error, self.bucket.name)

        # SDK client for performing retry operations
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket)
        # Retry failed docs
        create_failed = self.durability_helper.retry_with_no_error(
            client, doc_errors["create"], "create")
        read_failed = self.durability_helper.retry_with_no_error(
            client, doc_errors["read"], "read")
        delete_failed = self.durability_helper.retry_with_no_error(
            client, doc_errors["delete"], "delete")
        update_failed = self.durability_helper.retry_with_no_error(
            client, doc_errors["update"], "update")

        # Close the SDK client
        client.close()

        # Validate the retry operation status
        msg = "Retry failed for '{0}' with no error conditions"
        if create_failed:
            self.log_failure(msg.format("create"))
        if read_failed:
            self.log_failure(msg.format("read"))
        if delete_failed:
            self.log_failure(msg.format("delete"))
        if update_failed:
            self.log_failure(msg.format("update"))

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

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
        expected_failed_doc_num = 0

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
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])
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
            expected_failed_doc_num = self.crud_batch_size
        if self.doc_ops[0] == "update":
            gen_loader = gen_update
            expected_failed_doc_num = self.num_items
        if self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items
            expected_failed_doc_num = half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(20, message="Wait for task_1 ops to reach the server")

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

        # Validation to verify the sync_in_write_errors in doc_loader_task_2
        failed_docs = doc_loader_task_2.fail
        if len(failed_docs.keys()) != expected_failed_doc_num:
            self.log_failure("Exception not seen for some docs: {0}"
                            .format(failed_docs))

        valid_exception = self.durability_helper.validate_durability_exception(
            failed_docs, "DurableWriteInProgressException")

        if not valid_exception:
            self.log_failure("Got invalid exception")

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

    def test_bulk_sync_write_in_progress_for_persist_active(self):
        """
        This test validates sync_write_in_progress error scenario with
        durability=MAJORITY_AND_PERSIST_ON_MASTER

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
        error_sim = CouchbaseError(self.log, shell_conn)

        self.durability_level = "MAJORITY_AND_PERSIST_ON_MASTER"

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
        gen_loader = None
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader = gen_create
        elif self.doc_ops[0] == "update":
            gen_loader = gen_update
        elif self.doc_ops[0] == "delete":
            gen_loader = gen_delete
            self.num_items -= half_of_num_items

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.durability_timeout, retries=self.sdk_retries)

        self.sleep(30, message="Wait for task_1 ops to reach the server")

        # Initialize tasks and store the task objects
        doc_loader_task_2 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[1], 0,
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
                if "error" not in fail:
                    self.log_failure("No failures detected")

                if "DurableWriteInProgressException" not in str(fail["error"]):
                    self.log_failure("Invalid exception: {0}"
                                     .format(fail["error"]))
            else:
                if fail["success"] is not None:
                    self.log_failure("CRUD failed for vbucket {0}"
                                     .format(vb_num))

        # Revert the introduced error condition
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Create SDK Client
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket.name)

        # Retry failed docs
        self.durability_helper.retry_with_no_error(client, error_docs,
                                                   self.doc_ops[1])

        # Close the SDK connection
        client.close()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()


class TimeoutTests(DurabilityTestsBase):
    def setUp(self):
        super(TimeoutTests, self).setUp()
        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster), self.durability_level)
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
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

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

        self.sleep(time_to_wait,
                   message="Wait less than the durability_timeout value")

        # Fetch latest stats and validate the values are not changed
        for node in target_nodes:
            vb_info["withinTimeout"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            if vb_info["init"][node.ip] != vb_info["withinTimeout"][node.ip]:
                self.log_failure("Mismatch in vbucket_seqno. {0} != {1}"
                                 .format(vb_info["init"][node.ip],
                                         vb_info["withinTimeout"][node.ip]))

        # Revert the specified error scenario
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)
            # Disconnect the shell connection
            shell_conn[node].disconnect()

        # Create SDK Client
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket.name)

        # Wait for document_loader tasks to complete and retry failed docs
        op_type = None
        msg = "CRUD '{0}' failed after retry with no error condition"
        for index, task in enumerate(tasks):
            self.task.jython_task_manager.get_task_result(task)

            if index == 0:
                op_type = "create"
            elif index == 1:
                op_type = "update"
            elif index == 2:
                op_type = "read"
            elif index == 3:
                op_type = "delete"

            # Retry failed docs
            retry_failed = self.durability_helper.retry_with_no_error(
                client, task.fail, op_type)
            if retry_failed:
                self.log_failure(msg.format(op_type))

        # Close the SDK connection
        client.close()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are updated
        for node in target_nodes:
            vb_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            if vb_info["init"][node.ip] != vb_info["afterCrud"][node.ip]:
                self.log_failure("Mismatch in vbucket_seqno. {0} != {1}"
                                 .format(vb_info["init"][node.ip],
                                         vb_info["withinTimeout"][node.ip]))
        self.validate_test_failure()

    def test_timeout_with_successful_crud_for_persist_active(self):
        """
        Test to validate timeouts during CRUDs with
        durability=MAJORITY_AND_PERSIST_ON_MASTER

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
        error_sim = CouchbaseError(self.log, shell_conn)
        vb_info = dict()

        self.durability_level = "MAJORITY_AND_PERSIST_ON_MASTER"

        curr_time = int(time.time())
        expected_timeout = curr_time + self.durability_timeout
        time_to_wait = expected_timeout - 20

        vb_info["init"] = cbstat_obj.vbucket_seqno(self.bucket.name)

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

        self.sleep(time_to_wait,
                   message="Wait less than the durability_timeout value")

        # Fetch latest stats and validate the values are not changed
        vb_info["withinTimeout"] = cbstat_obj.vbucket_seqno(self.bucket.name)
        if vb_info["init"] != vb_info["withinTimeout"]:
            self.log_failure("Mismatch in vbucket_seqno stats. {0} != {1}"
                             .format(vb_info["init"],
                                     vb_info["withinTimeout"]))

        # Revert the specified error scenario
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)
        # Disconnect the shell connection
        shell_conn.disconnect()

        # Create SDK Client
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket.name)

        # Wait for document_loader tasks to complete and retry failed docs
        op_type = None
        msg = "CRUD '{0}' failed after retry with no error condition"
        for index, task in enumerate(tasks):
            self.task.jython_task_manager.get_task_result(task)

            if index == 0:
                op_type = "create"
            elif index == 1:
                op_type = "update"
            elif index == 2:
                op_type = "read"
            elif index == 3:
                op_type = "delete"

            # Retry failed docs
            retry_failed = self.durability_helper.retry_with_no_error(
                client, task.fail, op_type)
            if retry_failed:
                self.log_failure(msg.format(op_type))

        # Close the SDK connection
        client.close()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are updated
        vb_info["afterCrud"] = cbstat_obj.vbucket_seqno(self.bucket.name)
        if vb_info["init"] != vb_info["afterCrud"]:
            self.log_failure("Mismatch in vbucket_seqno. {0} != {1}"
                             .format(vb_info["init"],
                                     vb_info["withinTimeout"]))
        self.validate_test_failure()

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
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

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
            # TODO: Verify timeout exceptions for each doc_op

        # Check whether the timeout triggered properly
        timed_out_ok = int(time.time()) == expected_timeout \
            or int(time.time()) == expected_timeout + 1
        self.assertTrue(timed_out_ok, msg="Timed-out before expected time")

        # Revert the specified error scenario
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
            # Disconnect the shell connection
            shell_conn[node].disconnect()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are not changed
        for node in target_nodes:
            vb_info["post_timeout"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            val = vb_info["init"][node.ip] == vb_info["post_timeout"][node.ip]
            self.assertTrue(val, msg="Mismatch in vbucket_seqno with timeout")

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        # Retry the same CRUDs after reverting the failure environment
        tasks = list()
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
            if len(task.fail.keys()) != 0:
                self.log_failure("Failures in retry with no error condition"
                                 .format(task.fail))

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are updated
        for node in target_nodes:
            vb_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            val = vb_info["init"][node.ip] != vb_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="Vbucket seq_no stats not updated")
        self.validate_test_failure()

    def test_timeout_with_crud_failures_for_persist_active(self):
        """
        Test to validate timeouts during CRUDs with
        durability=MAJORITY_AND_PERSIST_ON_MASTER

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
        error_sim = CouchbaseError(self.log, shell_conn)
        vb_info = dict()

        self.durability_level = "MAJORITY_AND_PERSIST_ON_MASTER"

        # Get active/replica vbucket list from the target_node
        active_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                    vbucket_type="active")
        replica_vb_numbers = cbstat_obj.vbucket_list(self.bucket.name,
                                                     vbucket_type="replica")

        vb_info["init"] = cbstat_obj.vbucket_seqno(self.bucket.name)
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
            # TODO: Verify timeout exceptions for each doc_op

        # Check whether the timeout triggered properly
        timed_out_ok = int(time.time()) == expected_timeout \
            or int(time.time()) == expected_timeout + 1
        self.assertTrue(timed_out_ok, msg="Timed-out before expected time")

        # Revert the specified error scenario
        error_sim.create(self.simulate_error, bucket_name=self.bucket.name)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are not changed
        vb_info["post_timeout"] = cbstat_obj.vbucket_seqno(self.bucket.name)
        val = vb_info["init"] == vb_info["post_timeout"]
        self.assertTrue(val, msg="Mismatch in vbucket_seqno with timeout")

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        # Retry the same CRUDs after reverting the failure environment
        tasks = list()
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

            for key, doc_info in task.fail.items():
                fail = doc_info[0]
                # Validate the returned error from the SDK
                vb_num = self.bucket_util.get_vbucket_num_for_key(
                    key, self.vbuckets)
                if vb_num in active_vb_numbers or vb_num in replica_vb_numbers:
                    self.assertTrue("error" in fail, msg="No failure detected")
                    self.assertTrue(
                        "DurableWriteInProgressException" in str(fail["error"]),
                        msg="Invalid exception: {0}".format(fail["error"]))
                else:
                    self.assertTrue(fail["success"] is None,
                                    msg="CRUD failed for vbucket {0}"
                                    .format(vb_num))

        # Fetch latest stats and validate the values are updated
        vb_info["afterCrud"] = cbstat_obj.vbucket_seqno(self.bucket.name)
        val = vb_info["init"] != vb_info["afterCrud"]
        self.assertTrue(val, msg="Vbucket seq_no stats not updated")

        # Revert the specified error scenario
        error_sim.revert(self.simulate_error, bucket_name=self.bucket.name)
        # Disconnect the shell connection
        shell_conn.disconnect()

        # Create SDK client for retry operation
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket.name)

        # Retry failed docs to succeed
        op_type = None
        for index, task in enumerate(tasks):
            if index == 0:
                op_type = "create"
            elif index == 1:
                op_type = "update"
            elif index == 1:
                op_type = "read"
            elif index == 1:
                op_type = "delete"
            self.durability_helper.retry_with_no_error(
                client, task.fail, op_type)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
