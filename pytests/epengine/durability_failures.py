import copy
import time
import json

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView


class DurabilityFailureTests(DurabilityTestsBase):
    def setUp(self):
        super(DurabilityFailureTests, self).setUp()
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
        shell_conn = dict()
        cbstat_obj = dict()
        vb_info["init"] = dict()
        vb_info["failure_stat"] = dict()
        vb_info["create_stat"] = dict()
        nodes_in_cluster = self.cluster_util.get_kv_nodes()
        gen_load = doc_generator(self.key, 0, self.num_items)
        err_msg = "Doc mutation succeeded with, "  \
                  "cluster size: {0}, replica: {1}" \
                  .format(len(self.cluster.nodes_in_cluster),
                          self.num_replicas)
        d_impossible_exception = SDKException.DurabilityImpossibleException

        for node in nodes_in_cluster:
            shell_conn[node.ip] = \
                RemoteMachineShellConnection(self.cluster.master)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])

            # Fetch vbucket seq_no stats from vb_seqno command for verification
            vb_info["init"].update(cbstat_obj[node.ip]
                                   .vbucket_seqno(self.bucket.name))

        # MB-34064 - Try same CREATE twice to validate doc cleanup in server
        for _ in range(2):
            # Perform durable SET operation
            d_create_task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_load, "create",
                batch_size=10, process_concurrency=8,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, skip_read_on_error=True)
            self.task.jython_task_manager.get_task_result(d_create_task)

            # Fetch vbucket seq_no status from cbstats after CREATE task
            for node in nodes_in_cluster:
                vb_info["failure_stat"].update(
                    cbstat_obj[node.ip].vbucket_seqno(self.bucket.name))

            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(0)
            self.assertTrue(len(d_create_task.fail.keys()) == self.num_items,
                            msg=err_msg)
            if vb_info["init"] != vb_info["failure_stat"]:
                self.log_failure(
                    "Failover stats mismatch. {0} != {1}"
                    .format(vb_info["init"], vb_info["failure_stat"]))

            validation_passed = \
                self.durability_helper.validate_durability_exception(
                    d_create_task.fail,
                    d_impossible_exception)
            if not validation_passed:
                self.log_failure("Unexpected exception type")

        # Perform aync_write to create the documents
        async_create_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_load, "create",
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(async_create_task)

        # Verify doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch vbucket seq_no status from vb_seqno command after async CREATEs
        for node in nodes_in_cluster:
            vb_info["create_stat"].update(cbstat_obj[node.ip]
                                          .vbucket_seqno(self.bucket.name))

        # Start durable UPDATE operation
        tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_load, "update",
                batch_size=10, process_concurrency=4,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                skip_read_on_error=True))
        # Start durable DELETE operation
        tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_load, "delete",
                batch_size=10, process_concurrency=4,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                skip_read_on_error=True))

        # Wait for all tasks to complete and validate the exception
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

            if len(task.fail.keys()) != self.num_items:
                self.log_failure("Few keys have not received exceptions: {0}"
                                 .format(task.fail.keys()))
            validation_passed = \
                self.durability_helper.validate_durability_exception(
                    task.fail,
                    d_impossible_exception)
            if not validation_passed:
                self.log_failure("Unexpected exception type")

        # Verify doc count is unchanged due to durability failures
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Reset failure_stat dictionary for reuse
        vb_info["failure_stat"] = dict()

        # Fetch vbucket seq_no status from vb_seqno after UPDATE/DELETE task
        for node in nodes_in_cluster:
            vb_info["failure_stat"].update(cbstat_obj[node.ip]
                                           .vbucket_seqno(self.bucket.name))

        if vb_info["create_stat"] != vb_info["failure_stat"]:
            self.log_failure("Failover stats mismatch. {0} != {1}"
                             .format(vb_info["failure_stat"],
                                     vb_info["create_stat"]))
        self.validate_test_failure()

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

        replica_vbs = dict()

        # Variable to hold one of the doc_generator objects
        gen_loader_1 = None
        gen_loader_2 = None

        # Override the crud_batch_size
        self.crud_batch_size = 4

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
            replica_vbs[node.ip] = cbstat_obj[node.ip].vbucket_list(
                self.bucket.name, vbucket_type="replica")

        target_vbuckets = replica_vbs[target_nodes[0].ip]
        if len(target_nodes) > 1:
            index = 1
            while index < len(target_nodes):
                target_vbuckets = list(
                    set(target_vbuckets).intersection(
                        set(replica_vbs[target_nodes[index].ip])
                    )
                )
                index += 1

        # Initialize doc_generators to use for testing
        self.log.info("Creating doc_generators")
        gen_create = doc_generator(
            self.key, self.num_items, self.crud_batch_size,
            vbuckets=self.vbuckets, target_vbucket=target_vbuckets)
        gen_update = doc_generator(
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets, mutate=1)
        gen_delete = doc_generator(
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        self.log.info("Done creating doc_generators")

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader_1 = gen_create
        elif self.doc_ops[0] in ["update", "replace", "touch"]:
            gen_loader_1 = gen_update
        elif self.doc_ops[0] == "delete":
            gen_loader_1 = gen_delete
            self.num_items -= self.crud_batch_size

        if self.doc_ops[1] == "create":
            gen_loader_2 = gen_create
        elif self.doc_ops[1] in ["update", "replace", "touch"]:
            gen_loader_2 = gen_update
        elif self.doc_ops[1] == "delete":
            gen_loader_2 = gen_delete

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader_1, self.doc_ops[0], 0,
            batch_size=self.crud_batch_size, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            print_ops_rate=False,
            start_task=False)

        # SDK client for performing individual ops
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket)

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
        self.sleep(10, "Wait for error simulation to take effect")

        self.task_manager.add_new_task(doc_loader_task_1)
        self.sleep(10, "Wait for task_1 CRUDs to reach server")

        # Perform specified CRUD operation on sync_write docs
        tem_gen = copy.deepcopy(gen_loader_2)
        while tem_gen.has_next():
            key, value = tem_gen.next()
            for fail_fast in [True, False]:
                if self.with_non_sync_writes:
                    fail = client.crud(self.doc_ops[1], key, value=value,
                                       exp=0, timeout=3, time_unit="seconds",
                                       fail_fast=fail_fast)
                else:
                    fail = client.crud(self.doc_ops[1], key, value=value,
                                       exp=0, durability=self.durability_level,
                                       timeout=3, time_unit="seconds",
                                       fail_fast=fail_fast)

                expected_exception = SDKException.AmbiguousTimeoutException
                retry_reason = \
                    SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS
                if fail_fast:
                    expected_exception = \
                        SDKException.RequestCanceledException
                    retry_reason = \
                        SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES

                # Validate the returned error from the SDK
                if expected_exception not in str(fail["error"]):
                    self.log_failure("Invalid exception for {0}: {1}"
                                     .format(key, fail["error"]))
                if retry_reason not in str(fail["error"]):
                    self.log_failure("Invalid retry reason for {0}: {1}"
                                     .format(key, fail["error"]))

                # Try reading the value in SyncWrite in-progress state
                fail = client.crud("read", key)
                if self.doc_ops[0] == "create":
                    # Expected KeyNotFound in case of CREATE operation
                    if fail["status"] is True:
                        self.log_failure(
                            "%s returned value during SyncWrite in progress %s"
                            % (key, fail))
                else:
                    # Expects prev value in case of other operations
                    if fail["status"] is False:
                        self.log_failure(
                            "Key %s read failed for previous value: %s"
                            % (key, fail))

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loader_task_1)

        # Cannot retry for CREATE/DELETE operation. So doing only for UPDATE
        if self.doc_ops[0] == "update":
            # Retry doc_op after reverting the induced error
            while gen_loader_2.has_next():
                key, value = gen_loader_2.next()
                if self.with_non_sync_writes:
                    fail = client.crud(self.doc_ops[0], key,
                                       value=value, exp=0)
                else:
                    fail = client.crud(self.doc_ops[0], key,
                                       value=value, exp=0,
                                       durability=self.durability_level,
                                       timeout=self.sdk_timeout,
                                       time_unit="seconds")
                if fail["error"] or fail["status"] is False:
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
            timeout_secs=self.sdk_timeout)

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
                                   timeout=self.sdk_timeout,
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
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

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
        replica_vbs = dict()

        # Variable to hold the doc_generator objects as per self.doc_ops
        gen_loader = list()

        # Override crud_batch_size to minimum value for testing
        self.crud_batch_size = 4

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
            replica_vbs[node.ip] = cbstat_obj[node.ip].vbucket_list(
                self.bucket.name, vbucket_type="replica")

        target_vbuckets = replica_vbs[target_nodes[0].ip]
        if len(target_nodes) > 1:
            index = 1
            while index < len(target_nodes):
                target_vbuckets = list(
                    set(target_vbuckets).intersection(
                        set(replica_vbs[target_nodes[index].ip])
                    )
                )
                index += 1

        # Initialize doc_generators to use for testing
        self.log.info("Creating doc_generators")
        gen_create = doc_generator(
            self.key, self.num_items, self.crud_batch_size,
            vbuckets=self.vbuckets, target_vbucket=target_vbuckets)
        gen_update = doc_generator(
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets, mutate=1)
        gen_delete = doc_generator(
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        self.log.info("Done creating doc_generators")

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader.append(gen_create)
        if self.doc_ops[0] == "update":
            gen_loader.append(gen_update)
        if self.doc_ops[0] == "delete":
            gen_loader.append(gen_delete)
            self.num_items -= self.crud_batch_size

        if self.doc_ops[1] == "create":
            gen_loader.append(gen_create)
        if self.doc_ops[1] == "update":
            gen_loader.append(gen_update)
        if self.doc_ops[1] == "delete":
            gen_loader.append(gen_delete)

        expected_failed_doc_num = self.crud_batch_size

        tem_durability = self.durability_level
        if self.with_non_sync_writes:
            tem_durability = "NONE"

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader[0], self.doc_ops[0], 0,
            batch_size=self.crud_batch_size, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            print_ops_rate=False,
            start_task=False)

        # This will support both sync-write and non-sync-writes
        doc_loader_task_2 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader[1], self.doc_ops[1], 0,
            batch_size=self.crud_batch_size, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=tem_durability, timeout_secs=10,
            print_ops_rate=False,
            task_identifier="parallel_task2",
            start_task=False)

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        self.task_manager.add_new_task(doc_loader_task_1)
        self.sleep(10, message="Wait for task_1 ops to reach the server")

        # This task should be done with all sync_write_in_progress errors
        self.task_manager.add_new_task(doc_loader_task_2)
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
            failed_docs,
            SDKException.AmbiguousTimeoutException,
            retry_reason=SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS)

        if not valid_exception:
            self.log_failure("Got invalid exception")

        # Validate docs for update success or not
        if self.doc_ops[0] == "update":
            read_task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_loader[0], "read",
                batch_size=self.crud_batch_size, process_concurrency=1,
                timeout_secs=self.sdk_timeout)
            self.task_manager.get_task_result(read_task)
            for key, doc_info in read_task.success.items():
                if doc_info["cas"] != 0 \
                        and json.loads(str(doc_info["value"]))["mutated"] != 1:
                    self.log_failure("Update failed for key %s: %s"
                                     % (key, doc_info))

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

        # Override the crud_batch_size
        self.crud_batch_size = 5
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
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets)
        gen_delete = doc_generator(
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets)
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
            self.num_items -= self.crud_batch_size

        # Initialize tasks and store the task objects
        doc_loader_task_1 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[0], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)

        self.sleep(30, message="Wait for task_1 ops to reach the server")

        # Initialize tasks and store the task objects
        doc_loader_task_2 = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_loader, self.doc_ops[1], 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)

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

    def test_durability_abort(self):
        """
        Test to validate durability abort is triggered properly with proper
        rollback on active vbucket
        :return:
        """
        crud_batch_size = 50
        replica_vbs = dict()
        verification_dict = dict()
        load_gen = dict()

        self.log.info("Loading docs such that all sync_writes will be aborted")
        for server in self.cluster_util.get_kv_nodes():
            ssh_shell = RemoteMachineShellConnection(server)
            cbstats = Cbstats(ssh_shell)
            replica_vbs[server] = cbstats.vbucket_list(self.bucket.name,
                                                       "replica")
            load_gen[server] = doc_generator(
                self.key,
                self.num_items,
                crud_batch_size,
                target_vbucket=replica_vbs[server])
            success = self.bucket_util.load_durable_aborts(
                ssh_shell, [load_gen[server]],
                self.bucket, self.durability_level,
                "create", "all_aborts")
            if not success:
                self.log_failure("Failures seen during loading aborted docs")
            ssh_shell.disconnect()
        self.validate_test_failure()

        # Validate vbucket stats
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        # Uncomment once MB-37153 is resolved
        # verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        if self.durability_level:
            verification_dict["sync_write_aborted_count"] = crud_batch_size * 2
            verification_dict["sync_write_committed_count"] = self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.vbuckets, expected_val=verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details verification failed "
                             "after aborts")

        # Retry aborted keys with healthy cluster
        for doc_op in ["create", "update", "delete", "create"]:
            for server in self.cluster_util.get_kv_nodes():
                task = self.task.async_load_gen_docs(
                    self.cluster, self.bucket, load_gen[server], doc_op, 0,
                    batch_size=20, process_concurrency=8,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)

                if len(task.fail.keys()) != 0:
                    self.log_failure("Failure seen during doc_op: %s" % doc_op)

                # Update verification dict for validation
                # Remove 'if' statement once MB-37153 is resolved
                if doc_op != "delete":
                    verification_dict["ops_" + doc_op] += crud_batch_size
                if self.durability_level:
                    verification_dict["sync_write_committed_count"] += \
                        crud_batch_size
                failed = self.durability_helper.verify_vbucket_details_stats(
                    self.bucket, self.cluster_util.get_kv_nodes(),
                    vbuckets=self.vbuckets, expected_val=verification_dict)
                if failed:
                    self.log_failure("Cbstat vbucket-details verification "
                                     "failed after doc_op: %s" % doc_op)
        self.validate_test_failure()


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

        Note: self.sdk_timeout values is considered as 'seconds'
        """

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        doc_gen = dict()
        vb_info = dict()
        vb_info["init"] = dict()
        vb_info["afterCrud"] = dict()
        msg = "CRUD '{0}' failed after retry with no error condition"

        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

        # Perform CRUDs with induced error scenario is active
        doc_gen["create"] = doc_generator(self.key, self.num_items,
                                          self.num_items+self.crud_batch_size,
                                          doc_size=self.doc_size)
        doc_gen["delete"] = doc_generator(self.key, 0,
                                          int(self.num_items/3),
                                          doc_size=self.doc_size)
        doc_gen["read"] = doc_generator(self.key, int(self.num_items/3),
                                        int(self.num_items/2),
                                        doc_size=self.doc_size)
        doc_gen["update"] = doc_generator(self.key, int(self.num_items/2),
                                          self.num_items,
                                          doc_size=self.doc_size,
                                          mutate=1)

        # Create SDK Client
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket.name)

        for op_type in ["create", "update", "read", "delete"]:
            self.log.info("Performing '%s' with timeout=%s"
                          % (op_type, self.sdk_timeout))
            doc_load_task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_gen[op_type],
                op_type, self.maxttl,
                batch_size=500, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                start_task=False)

            # Perform specified action
            for node in target_nodes:
                error_sim[node.ip].create(self.simulate_error,
                                          bucket_name=self.bucket.name)

            # Start doc_loading task
            self.task_manager.add_new_task(doc_load_task)

            self.sleep(5, "Wait before reverting the error condition")

            # Revert the specified error scenario
            for node in target_nodes:
                error_sim[node.ip].revert(self.simulate_error,
                                          bucket_name=self.bucket.name)

            self.task_manager.get_task_result(doc_load_task)

            if len(doc_load_task.fail.keys()) != 0:
                if op_type == "read":
                    self.log.warning("Read failed for %d keys: %s"
                                     % (len(doc_load_task.fail.keys()),
                                        doc_load_task.fail.keys()))
                else:
                    self.log_failure("Failures during %s operation: %s"
                                     % (op_type, doc_load_task.fail))

            # Fetch latest stats and validate the values are updated
            for node in target_nodes:
                vb_info["afterCrud"][node.ip] = \
                    cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
                if vb_info["init"][node.ip] == vb_info["afterCrud"][node.ip]:
                    self.log_failure("vbucket_seqno not updated. {0} == {1}"
                                     .format(vb_info["init"][node.ip],
                                             vb_info["afterCrud"][node.ip]))

            # Retry failed docs (if any)
            retry_failed = self.durability_helper.retry_with_no_error(
                client, doc_load_task.fail, op_type)
            if retry_failed:
                self.log_failure(msg.format(op_type))

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        # Disconnect the shell connection
        for node in target_nodes:
            shell_conn[node.ip].disconnect()

        # Close the SDK connection
        client.close()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
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
        expected_timeout = curr_time + self.sdk_timeout
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
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        self.sleep(time_to_wait,
                   message="Wait less than the sdk_timeout value")

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

        Note: self.sdk_timeout values is considered as 'seconds'
        """

        # Local method to validate vb_seqno
        def validate_vb_seqno_stats():
            """
            :return retry_validation: Boolean denoting to retry validation
            """
            retry_validation = False
            vb_info["post_timeout"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            for vb_num in range(self.vbuckets):
                vb_num = str(vb_num)
                if vb_num not in affected_vbs:
                    if vb_num in vb_info["init"][node.ip].keys() \
                            and vb_info["init"][node.ip][vb_num] \
                            != vb_info["post_timeout"][node.ip][vb_num]:
                        self.log_failure(
                            "Unaffected vb-%s stat updated: %s != %s"
                            % (vb_num,
                               vb_info["init"][node.ip][vb_num],
                               vb_info["post_timeout"][node.ip][vb_num]))
                elif int(vb_num) in target_nodes_vbuckets["active"]:
                    if vb_num in vb_info["init"][node.ip].keys() \
                            and vb_info["init"][node.ip][vb_num] \
                            != vb_info["post_timeout"][node.ip][vb_num]:
                        self.log.warning(
                            err_msg
                            % (node.ip,
                               "active",
                               vb_num,
                               vb_info["init"][node.ip][vb_num],
                               vb_info["post_timeout"][node.ip][vb_num]))
                elif int(vb_num) in target_nodes_vbuckets["replica"]:
                    if vb_num in vb_info["init"][node.ip].keys() \
                            and vb_info["init"][node.ip][vb_num] \
                            == vb_info["post_timeout"][node.ip][vb_num]:
                        retry_validation = True
                        self.log.warning(
                            err_msg
                            % (node.ip,
                               "replica",
                               vb_num,
                               vb_info["init"][node.ip][vb_num],
                               vb_info["post_timeout"][node.ip][vb_num]))
            return retry_validation

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        target_nodes_vbuckets = dict()
        vb_info = dict()
        tasks = dict()
        doc_gen = dict()
        affected_vbs = list()

        target_nodes_vbuckets["active"] = []
        target_nodes_vbuckets["replica"] = []
        vb_info["init"] = dict()
        vb_info["post_timeout"] = dict()
        vb_info["afterCrud"] = dict()

        # Override crud_batch_size to minimum value for testing
        self.crud_batch_size = 5

        # Create required doc_generators
        doc_gen["create"] = doc_generator(self.key, self.num_items,
                                          self.num_items+self.crud_batch_size)
        doc_gen["delete"] = doc_generator(self.key, 0,
                                          self.crud_batch_size)
        doc_gen["read"] = doc_generator(
            self.key, int(self.num_items/3),
            int(self.num_items/3) + self.crud_batch_size)
        doc_gen["update"] = doc_generator(
            self.key, int(self.num_items/2),
            int(self.num_items/2) + self.crud_batch_size)

        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            target_nodes_vbuckets["active"] += \
                cbstat_obj[node.ip].vbucket_list(self.bucket.name,
                                                 vbucket_type="active")
            target_nodes_vbuckets["replica"] += \
                cbstat_obj[node.ip].vbucket_list(self.bucket.name,
                                                 vbucket_type="replica")
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

        curr_time = int(time.time())
        expected_timeout = curr_time + self.sdk_timeout

        for op_type in doc_gen.keys():
            tasks[op_type] = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_gen[op_type], op_type, 0,
                batch_size=1, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                skip_read_on_error=True,
                start_task=False)

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
        self.sleep(10, "Wait for error_simulation to take effect")

        # Start the doc_ops tasks
        for op_type in doc_gen.keys():
            self.task_manager.add_new_task(tasks[op_type])

        # Wait for document_loader tasks to complete
        for op_type in doc_gen.keys():
            self.task.jython_task_manager.get_task_result(tasks[op_type])

            # Validate task failures
            if op_type == "read":
                # Validation for read task
                if len(tasks[op_type].fail.keys()) != 0:
                    self.log_failure("Read failed for few docs: %s"
                                     % tasks[op_type].fail.keys())
            else:
                # Validation of CRUDs - Update / Create / Delete
                for doc_id, crud_result in tasks[op_type].fail.items():
                    vb_num = self.bucket_util.get_vbucket_num_for_key(
                        doc_id, self.vbuckets)
                    if SDKException.DurabilityAmbiguousException \
                            not in str(crud_result["error"]):
                        self.log_failure(
                            "Invalid exception for doc %s, vb %s: %s"
                            % (doc_id, vb_num, crud_result))

        # Revert the specified error scenario
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Check whether the timeout triggered properly
        if int(time.time()) < expected_timeout:
            self.log_failure("Timed-out before expected time")

        for op_type in doc_gen.keys():
            if op_type == "read":
                continue
            while doc_gen[op_type].has_next():
                doc_id, _ = doc_gen[op_type].next()
                affected_vbs.append(
                    str(self.bucket_util.get_vbucket_num_for_key(
                        doc_id,
                        self.vbuckets)))

        affected_vbs = list(set(affected_vbs))
        err_msg = "%s - mismatch in %s vb-%s seq_no: %s != %s"
        # Fetch latest stats and validate the seq_nos are not updated
        for node in target_nodes:
            retry_count = 0
            max_retry = 3
            while retry_count < max_retry:
                self.log.info("Trying to validate vbseq_no stats: %d"
                              % (retry_count+1))
                retry_count += 1
                retry_required = validate_vb_seqno_stats()
                if not retry_required:
                    break
                self.sleep(5, "Sleep for vbseq_no stats to update")
            else:
                # This will be exited only if `break` condition is not met
                self.log_failure("validate_vb_seqno_stats verification failed")

        self.validate_test_failure()

        # SDK client for retrying AMBIGUOUS for unexpected keys
        sdk_client = SDKClient(RestConnection(self.cluster.master),
                               self.bucket)

        # Doc error validation
        for op_type in doc_gen.keys():
            task = tasks[op_type]

            if self.nodes_init == 1 \
                    and op_type != "read" \
                    and len(task.fail.keys()) != (doc_gen[op_type].end
                                                  - doc_gen[op_type].start):
                self.log_failure("Failed keys %d are less than expected %d"
                                 % (len(task.fail.keys()),
                                    (doc_gen[op_type].end
                                     - doc_gen[op_type].start)))

            # Create table objects for display
            table_view = TableView(self.log.error)
            ambiguous_table_view = TableView(self.log.error)
            table_view.set_headers(["Key", "Exception"])
            ambiguous_table_view.set_headers(["Key", "vBucket"])

            # Iterate failed keys for validation
            for doc_key, doc_info in task.fail.items():
                vb_for_key = self.bucket_util.get_vbucket_num_for_key(doc_key)

                ambiguous_table_view.add_row([doc_key, str(vb_for_key)])
                retry_success = \
                    self.durability_helper.retry_for_ambiguous_exception(
                        sdk_client, op_type, doc_key, doc_info)
                if not retry_success:
                    self.log_failure("%s failed in retry for %s"
                                     % (op_type, doc_key))

                if SDKException.DurabilityAmbiguousException \
                        not in str(doc_info["error"]):
                    table_view.add_row([doc_key, doc_info["error"]])

            # Display the tables (if any errors)
            table_view.display("Unexpected exception during %s" % op_type)
            ambiguous_table_view.display("Ambiguous exception during %s"
                                         % op_type)

        # Close the SDK connection
        sdk_client.close()

        # Verify doc count after expected CRUD failure
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are updated
        for node in target_nodes:
            vb_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            if vb_info["init"][node.ip] == vb_info["afterCrud"][node.ip]:
                self.log_failure("vBucket seq_no stats not updated")

        # Disconnect the shell connection
        for node in target_nodes:
            shell_conn[node.ip].disconnect()

        self.validate_test_failure()
