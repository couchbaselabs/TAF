import copy
import json
import time

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator,\
    sub_doc_generator_for_edit
from couchbase_helper.durability_helper import DurableExceptions
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from table_view import TableView


class SubDocTimeouts(DurabilityTestsBase):
    def setUp(self):
        super(SubDocTimeouts, self).setUp()
        # Loading SubDocs to loaded documents
        self.log.info("Creating doc_generator..")
        # Load basic docs into bucket
        doc_create = sub_doc_generator(
            self.key, 0, self.num_items/2,
            doc_size=self.sub_doc_size,
            target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        self.log.info("Loading {0} Sub-docs into the bucket: {1}"
                      .format(self.num_items/2, self.bucket))
        task = self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, doc_create, "insert", self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(task)
        self.log.info("==========Finished SubDocFailures base setup========")

    def tearDown(self):
        super(SubDocTimeouts, self).tearDown()

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
        vb_info["withinTimeout"] = dict()

        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

        doc_gen["insert"] = sub_doc_generator(
            self.key, self.num_items/2, self.num_items,
            doc_size=self.sub_doc_size)
        doc_gen["read"] = sub_doc_generator(
            self.key,
            self.num_items/4,
            self.num_items/2)
        doc_gen["upsert"] = sub_doc_generator_for_edit(
            self.key,
            self.num_items/4,
            self.num_items/2,
            template_index=2)
        doc_gen["remove"] = sub_doc_generator_for_edit(
            self.key,
            0,
            self.num_items/4,
            template_index=2)

        for op_type in doc_gen.keys():
            self.log.info("Performing '%s' with timeout=%s"
                          % (op_type, self.sdk_timeout))
            doc_load_task = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, doc_gen[op_type],
                op_type, self.maxttl,
                path_create=True,
                batch_size=500, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)

            # Perform specified action
            for node in target_nodes:
                error_sim[node.ip].create(self.simulate_error,
                                          bucket_name=self.bucket.name)

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
                if op_type == "read":
                    continue
                vb_info["afterCrud"][node.ip] = \
                    cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
                if vb_info["init"][node.ip] == vb_info["afterCrud"][node.ip]:
                    self.log_failure("vbucket_seqno not updated. {0} == {1}"
                                     .format(vb_info["init"][node.ip],
                                             vb_info["afterCrud"][node.ip]))

            # # Retry failed docs (if any)
            # retry_failed = self.durability_helper.retry_with_no_error(
            #     client, doc_load_task.fail, op_type)
            # if retry_failed:
            #     self.log_failure(msg.format(op_type))

        # Disconnect the shell connection
        for node in target_nodes:
            shell_conn[node.ip].disconnect()

        # Read mutation field from all docs for validation
        gen_read = sub_doc_generator_for_edit(self.key, 0, self.num_items)
        gen_read.template = '{{ "mutated": "" }}'
        reader_task = self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, gen_read, "read",
            batch_size=50, process_concurrency=8,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(reader_task)

        len_failed_keys = len(reader_task.fail.keys())
        if len_failed_keys != 0:
            self.log_failure("Failures in read_task (%d): %s"
                             % (len_failed_keys, reader_task.fail.keys()))
        for doc_key, crud_result in reader_task.success.items():
            expected_val = 2
            if int(doc_key.split('-')[1]) >= self.num_items/2:
                expected_val = 1
            if reader_task.success[doc_key]["value"][0] != expected_val:
                self.log_failure("Value mismatch for %s: %s"
                                 % (doc_key, crud_result))

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

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
                    if vb_info["init"][node.ip][vb_num] \
                            != vb_info["post_timeout"][node.ip][vb_num]:
                        self.log_failure(
                            "Unaffected vb-%s stat updated: %s != %s"
                            % (vb_num,
                               vb_info["init"][node.ip][vb_num],
                               vb_info["post_timeout"][node.ip][vb_num]))
                elif int(vb_num) in target_nodes_vbuckets["active"]:
                    if vb_info["init"][node.ip][vb_num] \
                            != vb_info["post_timeout"][node.ip][vb_num]:
                        self.log_failure(
                            err_msg
                            % (node.ip,
                               "active",
                               vb_num,
                               vb_info["init"][node.ip][vb_num],
                               vb_info["post_timeout"][node.ip][vb_num]))
                elif int(vb_num) in target_nodes_vbuckets["replica"]:
                    if vb_info["init"][node.ip][vb_num] \
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
        timeout_err_str = self.durability_helper.EXCEPTIONS["request_timeout"]
        ambiguous_err_str = self.durability_helper.EXCEPTIONS["ambiguous"]

        # Create required doc_generators
        doc_gen["insert"] = sub_doc_generator(
            self.key,
            self.num_items/2,
            self.num_items/2 + self.crud_batch_size)
        doc_gen["remove"] = sub_doc_generator_for_edit(
            self.key,
            0,
            self.crud_batch_size,
            template_index=2)
        doc_gen["read"] = sub_doc_generator_for_edit(
            self.key,
            0,
            self.crud_batch_size,
            template_index=0)
        doc_gen["upsert"] = sub_doc_generator_for_edit(
            self.key,
            int(self.num_items/4),
            int(self.num_items/4) + self.crud_batch_size,
            template_index=1)

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
            tasks[op_type] = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, doc_gen[op_type], op_type, 0,
                path_create=True,
                batch_size=1, process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                start_task=False)

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        for op_type in doc_gen.keys():
            self.task_manager.add_new_task(tasks[op_type])

        # Wait for document_loader tasks to complete
        for op_type in doc_gen.keys():
            self.task.jython_task_manager.get_task_result(tasks[op_type])

            # Validate task failures
            if op_type == "read":
                # Validation for read task
                for doc_id, crud_result in tasks[op_type].success.items():
                    vb_num = self.bucket_util.get_vbucket_num_for_key(
                        doc_id, self.vbuckets)
                    if vb_num in target_nodes_vbuckets["active"]:
                        self.log_failure("Read succeeded for %s present in "
                                         "stopped active vbucket: %s"
                                         % (doc_id, vb_num))
                self.durability_helper.validate_durability_exception(
                    tasks[op_type].fail,
                    self.durability_helper.EXCEPTIONS["request_timeout"])
            else:
                # Validation of CRUDs - Update / Create / Delete
                if len(tasks[op_type].success.keys()) != 0:
                    self.log_failure("Few keys succeeded for %s: %s"
                                     % (op_type,
                                        tasks[op_type].success.keys()))
                for doc_id, crud_result in tasks[op_type].fail.items():
                    vb_num = self.bucket_util.get_vbucket_num_for_key(
                        doc_id, self.vbuckets)
                    if vb_num in target_nodes_vbuckets["active"]:
                        if timeout_err_str not in str(crud_result["error"]):
                            self.log_failure(
                                "Invalid exception for doc %s, vb %s: %s"
                                % (doc_id, vb_num, crud_result))
                    else:
                        if ambiguous_err_str not in str(crud_result["error"]):
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

        # If replicas+1 == total nodes, verify no mutation should have
        # succeeded with durability
        if self.nodes_init == self.num_replicas+1:
            read_gen = doc_generator(self.key, 0, self.num_items)
            read_task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, read_gen, "read", 0,
                batch_size=500, process_concurrency=1,
                timeout_secs=self.sdk_timeout)
            self.task_manager.get_task_result(read_task)

            failed_keys = TableView(self.log.error)
            failed_keys.set_headers(["Key", "Error"])
            for doc_key, doc_info in read_task.success.items():
                mutated = json.loads(str(doc_info["value"]))["mutated"]
                if mutated != 0:
                    failed_keys.add_row([doc_key, doc_info])

            failed_keys.display("Affected mutations:")
            self.log.error(read_task.fail)

        # SDK client for retrying AMBIGUOUS for unexpected keys
        sdk_client = SDKClient(RestConnection(self.cluster.master),
                               self.bucket)

        # Doc error validation
        for op_type in doc_gen.keys():
            task = tasks[op_type]

            if self.nodes_init == 1 \
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

                if vb_for_key in target_nodes_vbuckets["active"]:
                    expected_exception = \
                        self.durability_helper.EXCEPTIONS["request_timeout"]
                elif vb_for_key in target_nodes_vbuckets["replica"]:
                    expected_exception = \
                        self.durability_helper.EXCEPTIONS["ambiguous"]
                else:
                    expected_exception = \
                        self.durability_helper.EXCEPTIONS["ambiguous"]
                    ambiguous_table_view.add_row([doc_key, vb_for_key])
                    retry_success = \
                        self.durability_helper.retry_for_ambiguous_exception(
                            sdk_client, op_type, doc_key, doc_info)
                    if not retry_success:
                        self.log_failure("%s failed in retry for %s"
                                         % (op_type, doc_key))

                if expected_exception not in str(doc_info["error"]):
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

        # Retry the same CRUDs after reverting the failure environment
        tasks = list()
        for op_type in doc_gen.keys():
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_gen[op_type], op_type, 0,
                batch_size=10, process_concurrency=1,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout))

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            if len(task.fail.keys()) != 0:
                self.log_failure("Failures with no error condition: {0}, {1}"
                                 .format(task.fail, task.fail.keys()))

        # Verify initial doc load count
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

        def validate_doc_mutated_value(expected_val):
            reader_task = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, subdoc_reader_gen, "read",
                batch_size=10,
                process_concurrency=8,
                timeout_secs=self.sdk_timeout)
            self.task_manager.get_task_result(reader_task)
            for doc_id, read_result in reader_task.success.items():
                if int(read_result["value"][0]) != int(expected_val):
                    self.log_failure("Key %s - mutated value is %s != %s"
                                     % (doc_id,
                                        read_result["value"],
                                        expected_val))

        tasks = list()
        vb_info = dict()
        shell_conn = dict()
        cbstat_obj = dict()
        vb_info["init"] = dict()
        vb_info["failure_stat"] = dict()
        vb_info["create_stat"] = dict()
        nodes_in_cluster = self.cluster_util.get_kv_nodes()
        gen_load = doc_generator(self.key, 0, self.num_items)
        gen_subdoc_load = sub_doc_generator(self.key, 0, self.num_items)
        subdoc_reader_gen = sub_doc_generator(self.key, 0, self.num_items)
        subdoc_reader_gen.template = '{{ "mutated": "" }}'
        err_msg = "Doc mutation succeeded with, " \
                  "cluster size: {0}, replica: {1}" \
            .format(len(self.cluster.nodes_in_cluster),
                    self.num_replicas)
        d_impossible_exception = \
            self.durability_helper.EXCEPTIONS["durabilility_impossible"]

        # Load basic documents without durability for validating SubDocs
        create_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_load, "create",
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(create_task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

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
            d_create_task = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, gen_subdoc_load, "insert",
                path_create=True,
                batch_size=10,
                process_concurrency=8,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(d_create_task)

            # Fetch vbucket seq_no status from cbstats after CREATE task
            for node in nodes_in_cluster:
                vb_info["failure_stat"].update(
                    cbstat_obj[node.ip].vbucket_seqno(self.bucket.name))

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

        validate_doc_mutated_value(0)
        # Perform aync_write to create the documents
        async_create_task = self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, gen_subdoc_load, "insert",
            path_create=True,
            batch_size=10,
            process_concurrency=8,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(async_create_task)

        if len(async_create_task.fail.keys()) != 0:
            self.log_failure("Few failures during async_create(%d): %s"
                             % (len(async_create_task.fail.keys()),
                                async_create_task.fail.keys()))
        validate_doc_mutated_value(1)

        # Verify doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch vbucket seq_no status from vb_seqno command after async CREATEs
        for node in nodes_in_cluster:
            vb_info["create_stat"].update(cbstat_obj[node.ip]
                                          .vbucket_seqno(self.bucket.name))

        # Start durable UPDATE operation
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, gen_subdoc_load, "upsert",
            path_create=True,
            batch_size=10, process_concurrency=4,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        # Start durable DELETE operation
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, gen_subdoc_load, "remove",
            batch_size=10, process_concurrency=4,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

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
        validate_doc_mutated_value(1)

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
        vb_info["init"] = dict()

        # Variable to hold one of the doc_generator objects
        gen_loader = [None, None]
        doc_loader_task_1 = None
        doc_loader_task_2 = None

        # Override the crud_batch_size
        self.crud_batch_size = 5
        expected_failed_doc_num = self.crud_batch_size

        # Select nodes to affect and open required shell_connections
        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
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
        gen_update_delete = doc_generator(
            self.key, 0, self.crud_batch_size, vbuckets=self.vbuckets,
            target_vbucket=target_vbuckets)
        gen_subdoc = sub_doc_generator(
            self.key, 0, self.crud_batch_size,
            vbuckets=self.vbuckets, target_vbucket=target_vbuckets)
        self.log.info("Done creating doc_generators")

        # Start CRUD operation based on the given 'doc_op' type
        if self.doc_ops[0] == "create":
            self.num_items += self.crud_batch_size
            gen_loader[0] = gen_create
        elif self.doc_ops[0] in "update":
            gen_loader[0] = gen_update_delete
        elif self.doc_ops[0] == "delete":
            gen_loader[0] = gen_update_delete
            self.num_items -= self.crud_batch_size
        elif self.doc_ops[0] in ["insert", "upsert", "remove"]:
            gen_loader[0] = gen_subdoc

        if self.doc_ops[1] == "create":
            gen_loader[1] = gen_create
        elif self.doc_ops[1] in ["update", "delete"]:
            gen_loader[1] = gen_update_delete
        elif self.doc_ops[1] in ["insert", "upsert", "remove"]:
            if self.doc_ops[1] == "insert" and self.doc_ops[0] == "create":
                gen_subdoc = sub_doc_generator(
                    self.key, self.num_items, self.crud_batch_size,
                    vbuckets=self.vbuckets, target_vbucket=target_vbuckets)
                gen_loader[1] = gen_subdoc
            gen_loader[1] = gen_subdoc

        # Load task for further upsert / remove operations
        if (self.doc_ops[0] in ["upsert", "remove"]) or (
                self.doc_ops[1] in ["upsert", "remove"]):
            subdoc_load_task = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, gen_subdoc, "insert",
                path_create=True,
                batch_size=self.crud_batch_size, process_concurrency=8,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task_manager.get_task_result(subdoc_load_task)

        tem_durability = self.durability_level
        if self.with_non_sync_writes:
            tem_durability = "NONE"

        # Initialize tasks and store the task objects
        if self.doc_ops[0] in ["create", "update", "delete"]:
            doc_loader_task_1 = self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_loader[0], self.doc_ops[0], 0,
                batch_size=1,
                process_concurrency=self.crud_batch_size,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                start_task=False)
        elif self.doc_ops[0] in ["insert", "upsert", "remove"]:
            doc_loader_task_1 = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, gen_loader[0], self.doc_ops[0], 0,
                path_create=True,
                batch_size=1,
                process_concurrency=self.crud_batch_size,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                start_task=False)

        # This will support both sync-write and non-sync-writes
        if self.doc_ops[1] in ["create", "update", "delete"]:
            doc_loader_task_2 = self.task.async_load_gen_docs(
                self.cluster, self.bucket, gen_loader[1], self.doc_ops[1], 0,
                batch_size=self.crud_batch_size, process_concurrency=1,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=tem_durability, timeout_secs=self.sdk_timeout,
                task_identifier="parallel_task2",
                print_ops_rate=False,
                start_task=False)
        elif self.doc_ops[1] in ["insert", "upsert", "remove"]:
            doc_loader_task_2 = self.task.async_load_gen_sub_docs(
                self.cluster, self.bucket, gen_loader[1], self.doc_ops[1], 0,
                path_create=True,
                batch_size=self.crud_batch_size, process_concurrency=1,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=tem_durability, timeout_secs=self.sdk_timeout,
                task_identifier="parallel_task2",
                print_ops_rate=False,
                start_task=False)

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
        self.sleep(5, "Wait for error simulation to take effect")

        # Start the loader_task_1
        self.task_manager.add_new_task(doc_loader_task_1)
        self.sleep(10, "Wait for task_1 CRUDs to reach server")

        # Start the loader_task_2
        self.task_manager.add_new_task(doc_loader_task_2)
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
            self.log_failure("Exception not seen for few docs: {0}"
                             .format(failed_docs))

        expected_exception = \
            DurableExceptions.DurableWriteInProgressException
        if self.doc_ops[0] in "create":
            expected_exception = \
                DurableExceptions.KeyNotFoundException
        valid_exception = self.durability_helper.validate_durability_exception(
            failed_docs,
            expected_exception)

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
