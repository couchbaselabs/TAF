from cb_constants import DocLoading
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator,\
    sub_doc_generator_for_edit
from couchbase_helper.durability_helper import DurabilityHelper
from epengine.durability_base import DurabilityTestsBase
from constants.sdk_constants.java_client import SDKConstants
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from table_view import TableView


class BasicOps(DurabilityTestsBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.is_sync_write_enabled = DurabilityHelper.is_sync_write_enabled(
            self.bucket_durability_level, self.durability_level)
        self.log.info("==========Finished BasisOps base setup========")

    def tearDown(self):
        super(BasicOps, self).tearDown()

    def test_basic_ops(self):
        """
        Basic test for Sub-doc CRUD operations

        A test in which `self.num_items` documents are created. Half of the
        documents are updated or deleted depending on the supplied `op_type`.
        """
        doc_op = self.input.param("op_type", None)
        def_bucket = self.cluster.buckets[0]

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] += self.num_items

        # Initial validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        self.log.info("Creating doc_generator..")
        # Insert `self.num_items` documents
        doc_create = sub_doc_generator(
            self.key, 0, self.num_items,
            key_size=self.key_size,
            doc_size=self.sub_doc_size,
            target_vbucket=self.target_vbucket,
            vbuckets=def_bucket.numVBuckets)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, def_bucket))
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_create,
            DocLoading.Bucket.SubDocOps.INSERT, self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # The documents that could not be inserted
        insert_failures = len(task.fail.keys())

        # Update verification_dict and validate
        verification_dict["ops_update"] += self.num_items - insert_failures
        if self.is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] += self.num_items - insert_failures
            verification_dict["sync_write_aborted_count"] += insert_failures

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        self.log.info("Creating doc_generator for doc_op")
        num_item_start_for_crud = int(self.num_items / 2)

        template_index = 0
        if doc_op == DocLoading.Bucket.SubDocOps.REMOVE:
            template_index = 2

        sub_doc_gen = sub_doc_generator_for_edit(
            self.key,
            start=0,
            end=num_item_start_for_crud,
            key_size=self.key_size,
            template_index=template_index)

        if doc_op == DocLoading.Bucket.SubDocOps.UPSERT:
            self.log.info("Performing 'upsert' mutation over the sub-docs")
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, doc_op, self.maxttl,
                path_create=True,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

            # The documents keys for which the update failed
            update_failures = len(task.fail.keys())

            verification_dict["ops_update"] += \
                num_item_start_for_crud - update_failures

            if self.is_sync_write_enabled:
                verification_dict["sync_write_committed_count"] += \
                    num_item_start_for_crud - update_failures

            # Edit doc_gen template to read the mutated value as well
            sub_doc_gen.template = \
                sub_doc_gen.template.replace(" }}", ", \"mutated\": \"\" }}")
            # Read all the values to validate update operation
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "read", 0,
                batch_size=100, process_concurrency=8,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

            # A set of expected values following a read operation
            expected_values = {'StateUpdate', 2, 'LastNameUpdate',
                               'TypeChange', 'CityUpdate', 'FirstNameUpdate'}

            op_failed_tbl = TableView(self.log.error)
            op_failed_tbl.set_headers(["Update failed key", "Value"])

            # If the values of attributes does not match the
            # expected value, append op to list of failed ops.
            for key, value in task.success.items():
                if expected_values != set(value["value"]):
                    op_failed_tbl.add_row([key, value["value"]])

            op_failed_tbl.display("Update failed for keys:")
            # Expect the non-updated values to match the update failures
            self.assertEqual(len(op_failed_tbl.rows), update_failures, "")
        elif doc_op == DocLoading.Bucket.SubDocOps.REMOVE:
            self.log.info("Performing 'remove' mutation over the sub-docs")
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, doc_op, 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

            # The number of documents that could not be removed
            remove_failures = len(task.fail.keys())

            verification_dict["ops_update"] += \
                num_item_start_for_crud - remove_failures

            if self.is_sync_write_enabled:
                verification_dict["sync_write_committed_count"] += \
                    num_item_start_for_crud - remove_failures

            # Edit doc_gen template to read the mutated value as well
            sub_doc_gen.template = sub_doc_gen.template \
                .replace(" }}", ", \"mutated\": \"\" }}")
            # Read all the values to validate update operation
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "read", 0,
                batch_size=100, process_concurrency=8,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

            op_failed_tbl = TableView(self.log.error)
            op_failed_tbl.set_headers(["Delete failed key", "Value"])

            # Collect read operations that failed
            for key, value in task.fail.items():
                op_failed_tbl.add_row([key, value["error"]])

            op_failed_tbl.display("Delete succeeded for keys:")

            # Expect the reads to have failed indicating the sub-documents are
            # no longer accessible.
            self.assertEqual(len(op_failed_tbl.rows),
                             num_item_start_for_crud, "Delete failed for few keys")
        else:
            self.log.warning("Unsupported doc_operation")

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # Validate verification_dict and validate
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        self.log.info("Validating doc_count")
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

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
        def_bucket = self.cluster.buckets[0]

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
            vbuckets=def_bucket.numVBuckets)
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)

        half_of_num_items = self.num_items
        self.num_items *= 2

        # Update verification_dict and validate
        verification_dict["ops_create"] = self.num_items
        if self.is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] = self.num_items

        self.log.info("Validating doc_count")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        # Create required doc_generators for CRUD ops
        doc_gen = dict()
        read_gen = doc_generator(self.key, 0, self.num_items)
        if doc_ops == DocLoading.Bucket.SubDocOps.INSERT:
            doc_gen[0] = sub_doc_generator(self.key, 0,
                                           half_of_num_items,
                                           key_size=self.key_size,
                                           doc_size=self.sub_doc_size,
                                           target_vbucket=self.target_vbucket,
                                           vbuckets=def_bucket.numVBuckets)
            doc_gen[1] = sub_doc_generator(self.key, half_of_num_items,
                                           self.num_items,
                                           key_size=self.key_size,
                                           doc_size=self.sub_doc_size,
                                           target_vbucket=self.target_vbucket,
                                           vbuckets=def_bucket.numVBuckets)
        elif doc_ops in [DocLoading.Bucket.SubDocOps.UPSERT,
                         DocLoading.Bucket.SubDocOps.REMOVE]:
            self.log.info("Creating sub_docs before upsert/remove operation")
            sub_doc_gen = sub_doc_generator(
                self.key, 0,
                self.num_items,
                key_size=self.key_size,
                doc_size=self.sub_doc_size,
                target_vbucket=self.target_vbucket,
                vbuckets=def_bucket.numVBuckets)
            template_index_1 = 0
            template_index_2 = 1
            if doc_ops == DocLoading.Bucket.SubDocOps.REMOVE:
                template_index_1 = 2
                template_index_2 = 2
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen,
                DocLoading.Bucket.SubDocOps.INSERT, self.maxttl,
                path_create=True,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

            verification_dict["ops_update"] += self.num_items
            if self.is_sync_write_enabled:
                verification_dict["sync_write_committed_count"] += \
                    self.num_items

            doc_gen[0] = sub_doc_generator_for_edit(
                self.key,
                start=0,
                end=half_of_num_items,
                key_size=self.key_size,
                template_index=template_index_1,
                target_vbucket=self.target_vbucket,
                vbuckets=def_bucket.numVBuckets)
            doc_gen[1] = sub_doc_generator_for_edit(
                self.key,
                start=half_of_num_items,
                end=self.num_items,
                key_size=self.key_size,
                template_index=template_index_2,
                target_vbucket=self.target_vbucket,
                vbuckets=def_bucket.numVBuckets)
        else:
            self.fail("Invalid sub_doc operation '%s'" % doc_ops)

        # Sync_Writes for doc_ops[0]
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_gen[0], doc_ops, self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))

        # Non_SyncWrites for doc_ops[1]
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_gen[1], doc_ops, self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))

        # Generic reader task - reads entire document instead of sub-doc
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, read_gen, "read", 0,
            batch_size=10, process_concurrency=1,
            timeout_secs=self.sdk_timeout, load_using=self.load_docs_using))

        verification_dict["ops_update"] += self.num_items
        if self.is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] += self.num_items

        # Wait for all task to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

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
        def_bucket = self.cluster.buckets[0]

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
                                         self.num_items,
                                         key_size=self.key_size)
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, curr_doc_gen,
            DocLoading.Bucket.SubDocOps.INSERT, self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)
        verification_dict["ops_update"] += \
            (curr_doc_gen.end - curr_doc_gen.start)
        if self.is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] += \
                (curr_doc_gen.end - curr_doc_gen.start)

        # Create required doc_generators for CRUD ops
        doc_gen["create"] = doc_generator(self.key,
                                          self.num_items,
                                          self.num_items * 2,
                                          doc_size=self.doc_size,
                                          target_vbucket=self.target_vbucket,
                                          vbuckets=def_bucket.numVBuckets)
        doc_gen["read"] = doc_generator(self.key,
                                        0,
                                        self.num_items)

        # Create sub-doc generators for CRUD test
        sub_doc_gen["insert"] = sub_doc_generator(self.key,
                                                  start=0,
                                                  end=insert_end_index,
                                                  key_size=self.key_size,
                                                  doc_size=self.sub_doc_size)
        sub_doc_gen["read"] = sub_doc_generator(self.key,
                                                start=insert_end_index,
                                                end=upsert_end_index,
                                                key_size=self.key_size,
                                                doc_size=self.sub_doc_size)
        sub_doc_gen["upsert"] = sub_doc_generator_for_edit(
                                    self.key,
                                    start=insert_end_index,
                                    end=upsert_end_index,
                                    template_index=0,
                                    key_size=self.key_size,
                                    target_vbucket=self.target_vbucket,
                                    vbuckets=def_bucket.numVBuckets)
        sub_doc_gen["remove"] = sub_doc_generator_for_edit(
                                    self.key,
                                    start=upsert_end_index,
                                    end=self.num_items,
                                    template_index=2,
                                    key_size=self.key_size,
                                    target_vbucket=self.target_vbucket,
                                    vbuckets=def_bucket.numVBuckets)

        # Start full document mutations before starting sub_doc ops
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen["create"], "create", 0,
            batch_size=10, process_concurrency=1,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen["read"], "read", 0,
            batch_size=10, process_concurrency=1,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))

        # Start Sub_document mutations
        for index in range(0, 4):
            op_type = doc_ops[index]
            curr_doc_gen = sub_doc_gen[op_type]
            mutation_count = curr_doc_gen.end - curr_doc_gen.start
            if op_type != "read":
                verification_dict["ops_update"] += mutation_count

            if index < 2:
                # Durability doc_loader for first two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_sub_docs(
                    self.cluster, def_bucket, curr_doc_gen, op_type, 0,
                    path_create=True,
                    batch_size=10, process_concurrency=1,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    load_using=self.load_docs_using))
                if op_type != "read" and self.is_sync_write_enabled:
                    verification_dict["sync_write_committed_count"] += \
                        mutation_count
            else:
                # Non-SyncWrites for last two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_sub_docs(
                    self.cluster, def_bucket, curr_doc_gen, op_type, 0,
                    path_create=True,
                    batch_size=10, process_concurrency=1,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    load_using=self.load_docs_using))

        # Wait for all task to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Update num_items to sync with new docs created
        self.num_items *= 2
        verification_dict["ops_create"] = self.num_items
        if self.is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] += \
                self.num_items

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        # Verify vb-details cbstats
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

    def test_with_persistence_issues(self):
        """
        1. Select nodes from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify the operation succeeds
        4. Validate all mutations met the durability condition
        """

        if self.durability_level.upper() in [
                SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY]:
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
        def_bucket = self.cluster.buckets[0]
        insert_end_index = self.num_items / 3
        upsert_end_index = (self.num_items / 3) * 2

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.getTargetNodes()

        self.log.info("Will simulate error condition on %s" % target_nodes)
        for node in target_nodes:
            # Create shell_connections
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(node)
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
                                                shell_conn[node.ip],
                                                node=node)
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Load sub_docs for upsert/remove mutation to work
        sub_doc_gen = sub_doc_generator(self.key,
                                        start=insert_end_index,
                                        end=self.num_items,
                                        key_size=self.key_size,
                                        doc_size=self.sub_doc_size,
                                        target_vbucket=self.target_vbucket,
                                        vbuckets=def_bucket.numVBuckets)
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, sub_doc_gen,
            DocLoading.Bucket.SubDocOps.INSERT, self.maxttl,
            path_create=True,
            batch_size=20, process_concurrency=8,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(task)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = sub_doc_generator(self.key,
                                       0,
                                       insert_end_index,
                                       key_size=self.key_size,
                                       target_vbucket=self.target_vbucket,
                                       vbuckets=def_bucket.numVBuckets)
        gen_update = sub_doc_generator_for_edit(
            self.key,
            insert_end_index,
            upsert_end_index,
            key_size=self.key_size,
            template_index=0,
            vbuckets=def_bucket.numVBuckets,
            target_vbucket=self.target_vbucket)
        gen_delete = sub_doc_generator_for_edit(
            self.key,
            upsert_end_index,
            self.num_items,
            key_size=self.key_size,
            template_index=2,
            vbuckets=def_bucket.numVBuckets,
            target_vbucket=self.target_vbucket)

        self.log.info("Starting parallel doc_ops - insert/Read/upsert/remove")
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_create,
            DocLoading.Bucket.SubDocOps.INSERT, 0,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_update, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_update,
            DocLoading.Bucket.SubDocOps.UPSERT, 0,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_delete,
            DocLoading.Bucket.SubDocOps.REMOVE, 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using))

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
            shell_conn[node.ip].disconnect()

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(def_bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

            # Disconnect the shell connection
            cbstat_obj[node.ip].disconnect()

            # Failover validation
            val = True
            for vb, stat in failover_info["init"][node.ip].items():
                if len(failover_info["init"][node.ip].keys()) \
                        != len(failover_info["afterCrud"][node.ip].keys()):
                    val = False
                    self.log.error("Some fo-vb stats are missing after crud")
                    break
                stat_2 = failover_info["afterCrud"][node.ip][vb]
                if stat != stat_2:
                    val = False
                    self.log.error("Mismatch in failover stats, vb::%s,\n  "
                                   "%s\n  %s" % (vb, stat, stat_2))
            self.assertTrue(val, msg="Failover stats got updated")

            # Seq_no validation (High level)
            val = vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Verify doc count
        self.log.info("Validating doc count")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
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
        target_vbuckets = range(0, self.cluster.vbuckets)
        active_vbs_in_target_nodes = list()
        failover_info["init"] = dict()
        failover_info["afterCrud"] = dict()
        vb_info_info["init"] = dict()
        vb_info_info["afterCrud"] = dict()
        def_bucket = self.cluster.buckets[0]

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.getTargetNodes()

        self.log.info("Will simulate error condition on %s" % target_nodes)
        for node in target_nodes:
            # Create shell_connections
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(node)
            active_vbs = cbstat_obj[node.ip] .vbucket_list(def_bucket.name,
                                                           "active")
            active_vbs_in_target_nodes += active_vbs
            vb_info_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                def_bucket.name)
            failover_info["init"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

        # Load sub_docs for upsert/remove mutation to work
        sub_doc_gen = sub_doc_generator(self.key,
                                        start=0,
                                        end=self.num_items/2,
                                        key_size=self.key_size,
                                        doc_size=self.sub_doc_size)
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, sub_doc_gen,
            DocLoading.Bucket.SubDocOps.INSERT, self.maxttl,
            path_create=True,
            batch_size=20, process_concurrency=8,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(task)

        for node in target_nodes:
            # Perform specified action
            error_sim[node.ip] = CouchbaseError(self.log,
                                                shell_conn[node.ip],
                                                node=node)
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Remove active vbuckets from doc_loading to avoid errors
        target_vbuckets = list(set(target_vbuckets)
                               ^ set(active_vbs_in_target_nodes))

        # Perform CRUDs with induced error scenario is active
        tasks = dict()
        gen = dict()
        gen["insert"] = sub_doc_generator(
            self.key,
            self.num_items/2,
            self.crud_batch_size,
            key_size=self.key_size,
            vbuckets=def_bucket.numVBuckets,
            target_vbucket=target_vbuckets)
        gen["read"] = sub_doc_generator_for_edit(
            self.key,
            self.num_items/4,
            50,
            key_size=self.key_size,
            template_index=0,
            vbuckets=def_bucket.numVBuckets,
            target_vbucket=target_vbuckets)
        gen["upsert"] = sub_doc_generator_for_edit(
            self.key,
            self.num_items/4,
            50,
            key_size=self.key_size,
            template_index=0,
            vbuckets=def_bucket.numVBuckets,
            target_vbucket=target_vbuckets)
        gen["remove"] = sub_doc_generator_for_edit(
            self.key,
            0,
            50,
            key_size=self.key_size,
            template_index=2,
            vbuckets=def_bucket.numVBuckets,
            target_vbucket=target_vbuckets)

        self.log.info("Starting parallel doc_ops - insert/Read/upsert/remove")
        tasks["insert"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["insert"],
            DocLoading.Bucket.SubDocOps.INSERT, 0,
            path_create=True,
            batch_size=1, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        tasks["read"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["read"], "read", 0,
            batch_size=1, process_concurrency=1,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        tasks["upsert"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["upsert"],
            DocLoading.Bucket.SubDocOps.UPSERT, 0,
            path_create=True,
            batch_size=1, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        tasks["remove"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["remove"],
            DocLoading.Bucket.SubDocOps.REMOVE, 0,
            batch_size=1, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)

        # Wait for document_loader tasks to complete
        for _, task in tasks.items():
            self.task_manager.get_task_result(task)

        # Revert the induced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Read mutation field from all docs for validation
        gen_read = sub_doc_generator_for_edit(self.key, 0, self.num_items, 0,
                                              key_size=self.key_size)
        gen_read.template = '{{ "mutated": "" }}'
        reader_task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_read, "read",
            key_size=self.key_size,
            batch_size=50, process_concurrency=8,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(reader_task)

        # Validation for each CRUD task
        for op_type, task in tasks.items():
            if len(task.success.keys()) != len(gen[op_type].pre_generated_keys):
                self.log_failure("Failure during %s operation" % op_type)
            elif len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed during %s: %s"
                                 % (op_type, task.fail))

            for doc_key, crud_result in task.success.items():
                if crud_result["cas"] == 0:
                    self.log_failure("%s failed for %s: %s"
                                     % (op_type, doc_key, crud_result))
                if op_type == DocLoading.Bucket.SubDocOps.INSERT:
                    if reader_task.success[doc_key]["value"][0] != 1:
                        self.log_failure("%s value mismatch for %s: %s"
                                         % (op_type, doc_key, crud_result))
                elif op_type in [DocLoading.Bucket.SubDocOps.UPSERT,
                                 DocLoading.Bucket.SubDocOps.REMOVE]:
                    if reader_task.success[doc_key]["value"][0] != 2:
                        self.log_failure("%s value mismatch for %s: %s"
                                         % (op_type, doc_key, crud_result))
            # Verify there is not failed docs in the task

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(def_bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

            # Failover validation
            val = failover_info["init"][node.ip] \
                == failover_info["afterCrud"][node.ip]
            error_msg = "Failover stats not updated after error condition"
            self.assertTrue(val, msg=error_msg)

            # Seq_no validation (High level)
            val = vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Disconnect the shell connection
        for node in target_nodes:
            cbstat_obj[node.ip].disconnect()
            shell_conn[node.ip].disconnect()

        # Verify doc count
        self.log.info("Validating doc count")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        self.validate_test_failure()

    def test_expired_sys_xattr_consumed_by_dcp(self):
        """
        1. Create a empty doc with exp=5sec
        2. Insert xattr to the same doc (preserve_expiry=True)
        3. Wait for ep_queue_size to become zero
        4. Insert few more docs to the bucket
        5. Create a new secondary index to start DCP streaming from memory
        6. Expect no crash after step#5
        """

        doc_ttl = 10
        key = "test_xattr_doc-1"
        index_retry = 10
        index_created = False
        index_name = "test_index"
        bucket = self.cluster.buckets[0]
        vb_for_key = self.bucket_util.get_vbucket_num_for_key(
            key, bucket.numVBuckets)

        # Open SDK client
        client = SDKClient(self.cluster, bucket)

        # Create Sync_write doc with xattr + doc_ttl=10s
        client.crud(DocLoading.Bucket.DocOps.CREATE, key, {},
                    durability=self.durability_level)
        client.crud("subdoc_insert", key, ["_sdkey", "abc123"],
                    xattr=True, durability=self.durability_level)
        client.crud(DocLoading.Bucket.DocOps.UPDATE, key, {},
                    durability=self.durability_level, exp=doc_ttl)

        # Wait for all items to get persist
        self.log.info("Waiting for ep_queue_size to become zero")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.sleep(doc_ttl, "Wait for doc to expire")

        self.log.info("Loading more docs into the targeted vb: %s"
                      % vb_for_key)
        doc_gen = doc_generator(self.key, self.num_items, 1000,
                                vbuckets=bucket.numVBuckets,
                                target_vbucket=[vb_for_key])
        doc_load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.UPDATE,
            durability=self.durability_level, load_using=self.load_docs_using)
        self.task_manager.get_task_result(doc_load_task)

        rest = RestConnection(self.cluster.master)
        rest.set_indexer_storage_mode()
        self.log.info("Creating 2i on the bucket")
        client.cluster.query("CREATE PRIMARY INDEX %s ON %s"
                             % (index_name, bucket.name))
        self.sleep(2, "Wait for primary index to be created")
        while not index_created and index_retry != 0:
            state = client.cluster \
                .query("SELECT state FROM system:indexes "
                       "WHERE name='%s'" % index_name) \
                .rowsAsObject()[0].get("state")
            if state == "online":
                index_created = True
            else:
                index_retry -= 1
                self.sleep(1, "Retrying.. Index not yet online")

        client.close()
