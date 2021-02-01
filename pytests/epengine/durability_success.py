from BucketLib.bucket import Bucket
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from error_simulation.disk_error import DiskError
from sdk_client3 import SDKClient
from remote.remote_util import RemoteMachineShellConnection


class DurabilitySuccessTests(DurabilityTestsBase):
    def setUp(self):
        super(DurabilitySuccessTests, self).setUp()
        self.log.info("=== DurabilitySuccessTests setup complete ===")

    def tearDown(self):
        super(DurabilitySuccessTests, self).tearDown()

    def test_with_persistence_issues(self):
        """
        Test to make sure timeout is handled in durability calls
        and document CRUDs are successful even with disk related failures

        1. Select nodes from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify the operation succeeds
        4. Validate all mutations are succeeded

        Note: self.sdk_timeout value is considered as 'seconds'
        """

        if self.durability_level.upper() in [
                Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                Bucket.DurabilityLevel.PERSIST_TO_MAJORITY]:
            self.log.critical("Test not valid for persistence durability")
            return

        error_sim = dict()
        shell_conn = dict()
        cbstat_obj = dict()
        failover_info = dict()
        vb_info_info = dict()
        active_vbs_in_target_nodes = list()
        failover_info["init"] = dict()
        failover_info["afterCrud"] = dict()
        vb_info_info["init"] = dict()
        vb_info_info["afterCrud"] = dict()

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.getTargetNodes()

        self.log.info("Will simulate error condition on %s" % target_nodes)
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            active_vbs_in_target_nodes += cbstat_obj[node.ip].vbucket_list(
                self.bucket.name,
                "active")
            vb_info_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            failover_info["init"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)

        if self.simulate_error \
                in [DiskError.DISK_FULL, DiskError.DISK_FAILURE]:
            error_sim = DiskError(self.log, self.task_manager,
                                  self.cluster.master, target_nodes,
                                  60, 0, False, 120,
                                  disk_location="/data")
            error_sim.create(action=self.simulate_error)
        else:
            for node in target_nodes:
                # Create shell_connections
                shell_conn[node.ip] = RemoteMachineShellConnection(node)

                # Perform specified action
                error_sim[node.ip] = CouchbaseError(self.log,
                                                    shell_conn[node.ip])
                error_sim[node.ip].create(self.simulate_error,
                                          bucket_name=self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        self.log.info("Starting parallel doc_ops - Create/Read/Update/Delete")
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            # Verify there is not failed docs in the task
            if len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed during {0}: {1}"
                                 .format(task.op_type, task.fail))

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))

        # Wait for document_loader tasks to complete
        for task in tasks[2:]:
            self.task.jython_task_manager.get_task_result(task)
            # Verify there is not failed docs in the task
            if len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed during {0}: {1}"
                                 .format(task.op_type, task.fail))

        if self.simulate_error \
                in [DiskError.DISK_FULL, DiskError.DISK_FAILURE]:
            error_sim.revert(self.simulate_error)
        else:
            # Revert the induced error condition
            for node in target_nodes:
                error_sim[node.ip].revert(self.simulate_error,
                                          bucket_name=self.bucket.name)

                # Disconnect the shell connection
                shell_conn[node.ip].disconnect()
            self.sleep(10, "Wait for node recovery to complete")

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Create a SDK client connection to retry operation
        client = SDKClient([self.cluster.master], self.bucket)

        # Retry failed docs (if any)
        for index, task in enumerate(tasks):
            if index == 0:
                op_type = "create"
            elif index == 1:
                op_type = "update"
            elif index == 2:
                op_type = "read"
            elif index == 3:
                op_type = "delete"

            op_failed = self.durability_helper.retry_with_no_error(
                client, task.fail, op_type)
            if op_failed:
                self.log_failure(
                    "CRUD '{0}' failed on retry with no error condition"
                    .format(op_type))

        # Close the SDK connection
        client.close()

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)

            # Failover validation
            val = failover_info["init"][node.ip] \
                  == failover_info["afterCrud"][node.ip]
            error_msg = "Failover stats got updated"
            self.assertTrue(val, msg=error_msg)

            # Seq_no validation (High level)
            val = vb_info_info["init"][node.ip] \
                  != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Verify doc count
        self.log.info("Validating doc count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

    def test_with_process_crash(self):
        """
        Test to make sure durability will succeed even if a node goes down
        due to crash and has enough nodes to satisfy the durability

        1. Select a node from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify the operation succeeds
        4. Validate all mutations are succeeded

        Note: self.sdk_timeout values is considered as 'seconds'
        """
        if self.num_replicas < 2:
            self.assertTrue(False, msg="Required: num_replicas > 1")

        # Override num_of_nodes affected to 1
        self.num_nodes_affected = 1

        error_sim = dict()
        shell_conn = dict()
        cbstat_obj = dict()
        failover_info = dict()
        vb_info_info = dict()
        target_vbuckets = range(0, self.cluster_util.vbuckets)
        active_vbs_in_target_nodes = list()
        failover_info["init"] = dict()
        failover_info["afterCrud"] = dict()
        vb_info_info["init"] = dict()
        vb_info_info["afterCrud"] = dict()

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.getTargetNodes()

        self.log.info("Will simulate error condition on %s" % target_nodes)
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            active_vbs_in_target_nodes += cbstat_obj[node.ip].vbucket_list(
                self.bucket.name,
                "active")
            vb_info_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            failover_info["init"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)

        # Remove active vbuckets from doc_loading to avoid errors
        target_vbuckets = list(set(target_vbuckets)
                               ^ set(active_vbs_in_target_nodes))

        # Create required doc_generator for target_vbucket list
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.crud_batch_size,
                                   target_vbucket=target_vbuckets)
        gen_delete = doc_generator(self.key, 0, 50,
                                   target_vbucket=target_vbuckets)
        gen_update = doc_generator(self.key, self.num_items/2, 50,
                                   target_vbucket=target_vbuckets)

        # Perform CRUDs with induced error scenario is active
        self.log.info("Starting parallel doc_ops - Create/Read/Update/Delete")
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            start_task=False,
            sdk_client_pool=self.sdk_client_pool))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            start_task=False,
            sdk_client_pool=self.sdk_client_pool))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            start_task=False,
            sdk_client_pool=self.sdk_client_pool))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            start_task=False,
            sdk_client_pool=self.sdk_client_pool))

        for task in tasks:
            self.task_manager.add_new_task(task)

        self.sleep(10, "Wait for doc loaders to start loading data")

        for node in target_nodes:
            # Create shell_connections
            shell_conn[node.ip] = RemoteMachineShellConnection(node)

            # Perform specified action
            error_sim[node.ip] = CouchbaseError(self.log,
                                                shell_conn[node.ip])
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            # Verify there is not failed docs in the task
            if len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed during {0}: {1}"
                                 .format(task.op_type, task.fail))

        # Update num_items value accordingly to the CRUD performed
        self.num_items += len(gen_create.doc_keys) - len(gen_delete.doc_keys)

        if self.simulate_error \
                not in [DiskError.DISK_FULL, DiskError.DISK_FAILURE]:
            # Revert the induced error condition
            for node in target_nodes:
                error_sim[node.ip].revert(self.simulate_error,
                                          bucket_name=self.bucket.name)

                # Disconnect the shell connection
                shell_conn[node.ip].disconnect()
            self.sleep(10, "Wait for node recovery to complete")

            # In case of error with Ephemeral bucket, need to rebalance
            # to make sure data is redistributed properly
            if self.bucket_type == Bucket.Type.EPHEMERAL:
                retry_num = 0
                while retry_num != 2:
                    result = self.task.rebalance(
                        self.servers[0:self.nodes_init],
                        [], [])
                    if result:
                        break
                    retry_num += 1
                    self.sleep(10, "Wait before retrying rebalance")

                self.assertTrue(result, "Rebalance failed")

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets(cbstat_cmd="all",
                                                     stat_name="ep_queue_size",
                                                     timeout=60)
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Create a SDK client connection to retry operation
        client = SDKClient([self.cluster.master], self.bucket)

        # Retry failed docs (if any)
        for index, task in enumerate(tasks):
            if index == 0:
                op_type = "create"
            elif index == 1:
                op_type = "update"
            elif index == 2:
                op_type = "read"
            elif index == 3:
                op_type = "delete"

            op_failed = self.durability_helper.retry_with_no_error(
                client, task.fail, op_type)
            if op_failed:
                self.log_failure(
                    "CRUD '{0}' failed on retry with no error condition"
                    .format(op_type))

        # Close the SDK connection
        client.close()

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)

            # Failover stat validation
            if self.simulate_error == CouchbaseError.KILL_MEMCACHED:
                val = failover_info["init"][node.ip] \
                      != failover_info["afterCrud"][node.ip]
            else:
                if self.simulate_error != CouchbaseError.STOP_MEMCACHED \
                        and self.bucket_type == Bucket.Type.EPHEMERAL:
                    val = failover_info["init"][node.ip] \
                          != failover_info["afterCrud"][node.ip]
                else:
                    val = failover_info["init"][node.ip] \
                          == failover_info["afterCrud"][node.ip]
            error_msg = "Failover stats mismatch after error condition:" \
                        " %s != %s" \
                        % (failover_info["init"][node.ip],
                           failover_info["afterCrud"][node.ip])
            self.assertTrue(val, msg=error_msg)

            # Seq_no validation (High level)
            val = vb_info_info["init"][node.ip] \
                  != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Verify doc count
        self.log.info("Validating doc count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

    def test_non_overlapping_similar_crud(self):
        """
        Test to run non-overlapping durability cruds on single bucket
        and make sure all CRUD operation succeeds

        1. Run single task_1 with durability operation
        2. Create parallel task to run either SyncWrite / Non-SyncWrite
           operation based on the config param and run that over the docs
           such that it will not overlap with other tasks
        3. Make sure all CRUDs succeeded without any unexpected exceptions
        """

        doc_ops = self.input.param("doc_ops", "create")
        doc_gen = dict()
        half_of_num_items = int(self.num_items/2)

        # Create required doc_generators for CRUD ops
        read_gen = doc_generator(self.key, 0, self.num_items)
        if doc_ops == "create":
            doc_gen[0] = doc_generator(self.key, self.num_items,
                                       self.num_items * 2)
            doc_gen[1] = doc_generator(self.key, self.num_items * 2,
                                       self.num_items * 3)
            # Update expected self.num_items at the end of this op
            self.num_items *= 3
        elif doc_ops in ["update", "delete"]:
            doc_gen[0] = doc_generator(self.key, 0, half_of_num_items)
            doc_gen[1] = doc_generator(self.key, half_of_num_items,
                                       self.num_items)

            # Update expected self.num_items at the end of "delete" op
            if doc_ops == "delete":
                self.num_items = 0

        tasks = list()
        # Sync_Writes for doc_ops[0]
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen[0], doc_ops, 0,
            batch_size=10, process_concurrency=1,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))

        # Non_SyncWrites for doc_ops[1]
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen[1], doc_ops, 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))

        # Generic reader task
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, read_gen, "read", 0,
            batch_size=10, process_concurrency=1,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool))

        # Wait for all task to complete
        for task in tasks:
            # TODO: Receive failed docs and make sure only expected exceptions
            #       are generated
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_non_overlapping_parallel_cruds(self):
        """
        Test to run non-overlapping durability cruds on single bucket
        and make sure all CRUD operation succeeds

        1. Run single task_1 with durability operation
        2. Create parallel task to run either SyncWrite / Non-SyncWrite
           operation based on the config param and run that over the docs
           such that it will not overlap with the other tasks
        3. Make sure all CRUDs succeeded without any unexpected exceptions
        """

        doc_ops = self.input.param("doc_ops", "create;delete;update;read")
        doc_ops = doc_ops.split(";")
        half_of_num_items = int(self.num_items/2)
        doc_gen = dict()
        tasks = list()

        # Create required doc_generators for CRUD ops
        doc_gen["create"] = doc_generator(self.key, self.num_items,
                                          self.num_items * 2)
        doc_gen["update"] = doc_generator(self.key, half_of_num_items,
                                          self.num_items)
        doc_gen["delete"] = doc_generator(self.key, 0, half_of_num_items)
        doc_gen["read"] = doc_gen["update"]

        for index in range(0, 4):
            op_type = doc_ops[index]
            curr_doc_gen = doc_gen[op_type]

            if index < 2:
                # Durability doc_loader for first two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, self.bucket, curr_doc_gen, op_type, 0,
                    batch_size=10, process_concurrency=1,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool))
            else:
                # Non-SyncWrites for last two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, self.bucket, curr_doc_gen, op_type, 0,
                    batch_size=10, process_concurrency=1,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool))

        # Update num_items according to the CRUD operations
        self.num_items += self.num_items - half_of_num_items

        # Wait for all task to complete
        for task in tasks:
            # TODO: Receive failed docs and make sure only expected exceptions
            #       are generated
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
