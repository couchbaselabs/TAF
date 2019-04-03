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

    def test_sync_in_write_progress(self):
        """
        Test to simulate sync_in_write_progress error and validate the behavior

        1. Select nodes to simulate the error which will affect the durability
        2. Enable the specified error_scenario on the selected nodes
        3. Perform individual CRUDs and verify sync_in_write_progress errors
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
            if "sync_in_write_progress" not in fail["error"]:
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

    def test_bulk_sync_in_write_progress(self):
        """
        Test to simulate sync_in_write_progress error and validate the behavior

        1. Select nodes to simulate the error which will affect the durability
        2. Enable the specified error_scenario on the selected nodes
        3. Perform CRUDs which results in sync_in_write_progress errors
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

        # This task should be done will all sync_in_write_progress errors
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
        error simulation in server node side

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

    def test_timeout_with_crud_failures(self):
        """
        Test to make sure timeout is handled in durability calls
        and no documents are loaded when durability cannot be met using
        error simulation in server node side

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
