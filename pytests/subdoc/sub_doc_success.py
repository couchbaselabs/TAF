import json

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator,\
    sub_doc_generator_for_edit
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView


class BasicOps(DurabilityTestsBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.log.info("==========Finished BasisOps base setup========")

    def tearDown(self):
        super(BasicOps, self).tearDown()

    def enable_error_scenario_and_test_durability(self):
        """
        1. Select nodes from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify the operation succeeds
        4. Validate all mutations met the durability condition
        """

        error_sim = dict()
        shell_conn = dict()
        cbstat_obj = dict()
        failover_info = dict()
        vb_info_info = dict()
        target_vbuckets = range(0, self.vbuckets)
        active_vbs_in_target_nodes = list()
        failover_info["init"] = dict()
        failover_info["afterCrud"] = dict()
        vb_info_info["init"] = dict()
        vb_info_info["afterCrud"] = dict()
        disk_related_errors = ["stop_persistence"]
        def_bucket = self.bucket_util.buckets[0]
        insert_end_index = self.num_items / 3
        upsert_end_index = (self.num_items / 3) * 2

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.getTargetNodes()

        self.log.info("Will simulate error condition on %s" % target_nodes)
        for node in target_nodes:
            # Create shell_connections
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            active_vbs = cbstat_obj[node.ip] .vbucket_list(def_bucket.name,
                                                           "active")
            active_vbs_in_target_nodes += active_vbs
            vb_info_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                def_bucket.name)
            failover_info["init"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

        for node in target_nodes:
            # Perform specified action
            error_sim[node.ip] = CouchbaseError(self.log,
                                                shell_conn[node.ip])
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=def_bucket.name)

        if self.simulate_error not in disk_related_errors:
            # Remove active vbuckets from doc_loading to avoid errors
            target_vbuckets = list(set(target_vbuckets)
                                   ^ set(active_vbs_in_target_nodes))

        # Load sub_docs for upsert/remove mutation to work
        sub_doc_gen = sub_doc_generator(self.key,
                                        start=insert_end_index,
                                        end=self.num_items,
                                        doc_size=self.sub_doc_size,
                                        target_vbucket=target_vbuckets,
                                        vbuckets=self.vbuckets)
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, sub_doc_gen, "insert", self.maxttl,
            path_create=True,
            batch_size=20, process_concurrency=8,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(task)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = sub_doc_generator(self.key, 0,
                                       insert_end_index,
                                       target_vbucket=target_vbuckets)
        gen_read = sub_doc_generator(self.key, 0,
                                     self.num_items,
                                     target_vbucket=target_vbuckets)
        gen_update = sub_doc_generator_for_edit(self.key, insert_end_index,
                                                upsert_end_index,
                                                template_index=0,
                                                target_vbucket=target_vbuckets)
        gen_delete = sub_doc_generator_for_edit(self.key, upsert_end_index,
                                                self.num_items,
                                                template_index=1,
                                                target_vbucket=target_vbuckets)

        self.log.info("Starting parallel doc_ops - insert/Read/upsert/remove")
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_create, "insert", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_update, "upsert", 0,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_delete, "remove", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Wait for document_loader tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            # Verify there is not failed docs in the task
            if len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed for {0}".format(task.fail))

        # Revert the induced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=def_bucket.name)

            # Disconnect the shell connection
            shell_conn[node.ip].disconnect()

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(def_bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

            # Failover validation
            if self.simulate_error in disk_related_errors:
                val = failover_info["init"][node.ip] \
                      == failover_info["afterCrud"][node.ip]
                error_msg = "Failover stats got updated"
            else:
                val = failover_info["init"][node.ip] \
                      != failover_info["afterCrud"][node.ip]
                error_msg = "Failover stats not updated after error condition"
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

    def test_basic_ops(self):
        """
        Basic test for Sub-doc CRUD operations
        """
        doc_op = self.input.param("op_type", None)
        def_bucket = self.bucket_util.buckets[0]
        ignore_exceptions = list()
        retry_exceptions = list()

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        one_less_node = self.nodes_init == self.num_replicas

        if self.durability_level:
            pass
            #ignore_exceptions.append(
            #    "com.couchbase.client.core.error.RequestTimeoutException")

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        self.log.info("Creating doc_generator..")
        # Load basic docs into bucket
        doc_create = sub_doc_generator(
            self.key, 0, self.num_items, doc_size=self.sub_doc_size,
            target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, def_bucket))
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_create, "insert", self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        # Update verification_dict and validate
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = self.num_items+len(task.fail.keys())
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.vbuckets, expected_val=verification_dict,
            one_less_node=one_less_node)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.log.info("Creating doc_generator for doc_op")
        num_item_start_for_crud = int(self.num_items / 2)
        sub_doc_gen = sub_doc_generator_for_edit(
            self.key,
            start=num_item_start_for_crud,
            end=self.num_items,
            template_index=0)

        if doc_op == "upsert":
            self.log.info("Performing 'upsert' mutation over the sub-docs")
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, doc_op, self.maxttl,
                path_create=True,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            verification_dict["ops_update"] += \
                (sub_doc_gen.end - sub_doc_gen.start
                 + len(task.fail.keys()))
            if self.durability_level:
                verification_dict["sync_write_committed_count"] += \
                    (sub_doc_gen.end - sub_doc_gen.start)

            # Read all the values to validate update operation
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "read", 0,
                batch_size=100, process_concurrency=8,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

            op_failed_tbl = TableView(self.log.error)
            op_failed_tbl.set_headers(["Update failed key", "CAS", "Value"])
            for key, value in task.success.items():
                if json.loads(str(value["value"]))["mutated"] != 2:
                    op_failed_tbl.add_row([key, value["cas"], value["value"]])
                elif json.loads(str(value["value"]["name"]["last"])) != "None":
                    op_failed_tbl.add_row([key, value["cas"], value["value"]])
                elif json.loads(str(value["value"]["addr"]["city"])) != "UpdatedCity":
                    op_failed_tbl.add_row([key, value["cas"], value["value"]])
                elif json.loads(str(value["value"]["addr"]["pincode"])) != 0:
                    op_failed_tbl.add_row([key, value["cas"], value["value"]])

            op_failed_tbl.display("Update failed for keys:")
            if len(op_failed_tbl.rows) != 0:
                self.fail("Update failed for few keys")
        elif doc_op == "remove":
            self.log.info("Performing 'remove' mutation over the sub-docs")
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, doc_op, 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

            verification_dict["ops_update"] += \
                (sub_doc_gen.end - sub_doc_gen.start
                 + len(task.fail.keys()))
            if self.durability_level:
                verification_dict["sync_write_committed_count"] += \
                    (sub_doc_gen.end - sub_doc_gen.start)

            # Read all the values to validate update operation
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "read", 0,
                batch_size=100, process_concurrency=8,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)

            op_failed_tbl = TableView(self.log.error)
            op_failed_tbl.set_headers(["Delete failed key", "CAS", "Value"])
            for key, value in task.fail.items():
                op_failed_tbl.add_row([key, value["cas"], value["value"]])

            op_failed_tbl.display("Delete failed for keys:")
            if len(op_failed_tbl.rows) != 0:
                self.fail("Delete failed for few keys")
        else:
            self.log.warning("Unsupported doc_operation")

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        # Validate verification_dict and validate
        if self.durability_level:
            verification_dict["sync_write_committed_count"] += self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.vbuckets, expected_val=verification_dict,
            one_less_node=one_less_node)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        self.log.info("Validating doc_count")
        self.bucket_util.verify_stats_all_buckets(self.num_items)

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

        doc_ops = self.input.param("op_type", "create")
        tasks = list()
        def_bucket = self.bucket_util.buckets[0]
        one_less_node = self.nodes_init == self.num_replicas

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        self.log.info("Loading documents to support further sub_doc ops")
        doc_gen = doc_generator(
            self.key, self.num_items, self.num_items*2, doc_size=self.doc_size,
            target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)

        half_of_num_items = self.num_items
        self.num_items *= 2

        # Update verification_dict and validate
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_committed_count"] = self.num_items
        if self.durability_level:
            verification_dict["sync_write_aborted_count"] = 0
            verification_dict["sync_write_committed_count"] = self.num_items

        self.log.info("Validating doc_count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Create required doc_generators for CRUD ops
        doc_gen = dict()
        read_gen = doc_generator(self.key, 0, self.num_items)
        if doc_ops == "insert":
            doc_gen[0] = sub_doc_generator(self.key, 0,
                                           half_of_num_items,
                                           doc_size=self.sub_doc_size,
                                           target_vbucket=self.target_vbucket,
                                           vbuckets=self.vbuckets)
            doc_gen[1] = sub_doc_generator(self.key, half_of_num_items,
                                           self.num_items,
                                           doc_size=self.sub_doc_size,
                                           target_vbucket=self.target_vbucket,
                                           vbuckets=self.vbuckets)
        elif doc_ops in ["upsert", "remove"]:
            self.log.info("Creating sub_docs before upsert/remove operation")
            sub_doc_gen = sub_doc_generator(self.key, 0,
                                            self.num_items,
                                            doc_size=self.sub_doc_size,
                                            target_vbucket=self.target_vbucket,
                                            vbuckets=self.vbuckets)
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "insert", self.maxttl,
                path_create=True,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)

            doc_gen[0] = sub_doc_generator_for_edit(
                self.key,
                start=0,
                end=half_of_num_items,
                template_index=0,
                target_vbucket=self.target_vbucket,
                vbuckets=self.vbuckets)
            doc_gen[1] = sub_doc_generator_for_edit(
                self.key,
                start=half_of_num_items,
                end=self.num_items,
                template_index=1,
                target_vbucket=self.target_vbucket,
                vbuckets=self.vbuckets)
        else:
            self.fail("Invalid sub_doc operation '%s'" % doc_ops)

        # Sync_Writes for doc_ops[0]
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_gen[0], doc_ops, self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Non_SyncWrites for doc_ops[1]
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_gen[1], doc_ops, self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Generic reader task - reads entire document instead of sub-doc
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, read_gen, "read", 0,
            batch_size=10, process_concurrency=1,
            timeout_secs=self.sdk_timeout))

        verification_dict["ops_update"] = self.num_items
        verification_dict["sync_write_committed_count"] += self.num_items
        if self.durability_level:
            verification_dict["sync_write_aborted_count"] += 0
            verification_dict["sync_write_committed_count"] += self.num_items

        # Wait for all task to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.vbuckets, expected_val=verification_dict,
            one_less_node=one_less_node)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

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

        doc_ops = self.input.param("doc_ops", "insert;upsert;remove;read")
        doc_ops = doc_ops.split(";")
        doc_gen = dict()
        sub_doc_gen = dict()
        tasks = list()
        insert_end_index = self.num_items/3
        upsert_end_index = (self.num_items/3) * 2
        def_bucket = self.bucket_util.buckets[0]
        one_less_node = self.nodes_init == self.num_replicas

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        # Load sub_docs for upsert/remove to work
        curr_doc_gen = sub_doc_generator(self.key,
                                         insert_end_index,
                                         self.num_items)
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, curr_doc_gen, "insert", self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(task)

        # Create required doc_generators for CRUD ops
        doc_gen["create"] = doc_generator(self.key,
                                          self.num_items,
                                          self.num_items * 2,
                                          doc_size=self.doc_size,
                                          target_vbucket=self.target_vbucket,
                                          vbuckets=self.vbuckets)
        doc_gen["read"] = doc_generator(self.key,
                                        0,
                                        self.num_items)

        # Create sub-doc generators for CRUD test
        sub_doc_gen["insert"] = sub_doc_generator(self.key,
                                                  start=0,
                                                  end=insert_end_index,
                                                  doc_size=self.sub_doc_size)
        sub_doc_gen["read"] = sub_doc_generator(self.key,
                                                start=0,
                                                end=self.num_items,
                                                doc_size=self.sub_doc_size)
        sub_doc_gen["upsert"] = sub_doc_generator_for_edit(
                                    self.key,
                                    start=insert_end_index,
                                    end=upsert_end_index,
                                    template_index=0,
                                    target_vbucket=self.target_vbucket,
                                    vbuckets=self.vbuckets)
        sub_doc_gen["remove"] = sub_doc_generator_for_edit(
                                    self.key,
                                    start=upsert_end_index,
                                    end=self.num_items,
                                    template_index=1,
                                    target_vbucket=self.target_vbucket,
                                    vbuckets=self.vbuckets)

        # Start full document mutations before starting sub_doc ops
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen["create"], "create", 0,
            batch_size=10, process_concurrency=1,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen["read"], "read", 0,
            batch_size=10, process_concurrency=1,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Start Sub_document mutations
        for index in range(0, 4):
            op_type = doc_ops[index]
            curr_doc_gen = sub_doc_gen[op_type]

            if index < 2:
                # Durability doc_loader for first two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_sub_docs(
                    self.cluster, def_bucket, curr_doc_gen, op_type, 0,
                    path_create=True,
                    batch_size=10, process_concurrency=1,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout))
            else:
                # Non-SyncWrites for last two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_sub_docs(
                    self.cluster, def_bucket, curr_doc_gen, op_type, 0,
                    path_create=True,
                    batch_size=10, process_concurrency=1,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Update num_items to sync with new docs created
        self.num_items *= 2
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = self.num_items / 2
        verification_dict["sync_write_committed_count"] = self.num_items
        if self.durability_level:
            verification_dict["sync_write_aborted_count"] = 0
            verification_dict["sync_write_committed_count"] = self.num_items

        # Wait for all task to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify vb-details cbstats
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.vbuckets, expected_val=verification_dict,
            one_less_node=one_less_node)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_with_persistence_issues(self):
        # Call the generic method for testing
        self.enable_error_scenario_and_test_durability()

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

        # Call the generic method for testing
        self.enable_error_scenario_and_test_durability()
