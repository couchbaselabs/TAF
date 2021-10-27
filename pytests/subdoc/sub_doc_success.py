import json

from Cb_constants import DocLoading
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator,\
    sub_doc_generator_for_edit
from couchbase_helper.durability_helper import DurabilityHelper
from epengine.durability_base import DurabilityTestsBase
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from table_view import TableView


class BasicOps(DurabilityTestsBase):
    def setUp(self):
        super(BasicOps, self).setUp()
        self.log.info("==========Finished BasisOps base setup========")

    def tearDown(self):
        super(BasicOps, self).tearDown()

    def test_basic_ops(self):
        """
        Basic test for Sub-doc CRUD operations
        """
        doc_op = self.input.param("op_type", None)
        def_bucket = self.bucket_util.buckets[0]

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.durability_level in DurabilityHelper.SupportedDurability:
            verification_dict["sync_write_committed_count"] += self.num_items

        # Initial validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets, expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        self.log.info("Creating doc_generator..")
        # Load basic docs into bucket
        doc_create = sub_doc_generator(
            self.key, 0, self.num_items, doc_size=self.sub_doc_size,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, def_bucket))
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, doc_create, "insert", self.maxttl,
            path_create=True,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(task)

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        # Update verification_dict and validate
        verification_dict["ops_update"] += self.num_items
        if self.durability_level in DurabilityHelper.SupportedDurability:
            verification_dict["sync_write_committed_count"] += self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets, expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.log.info("Creating doc_generator for doc_op")
        num_item_start_for_crud = int(self.num_items / 2)

        template_index = 0
        if doc_op == "remove":
            template_index = 2

        sub_doc_gen = sub_doc_generator_for_edit(
            self.key,
            start=0,
            end=num_item_start_for_crud,
            template_index=template_index)

        if doc_op == "upsert":
            self.log.info("Performing 'upsert' mutation over the sub-docs")
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, doc_op, self.maxttl,
                path_create=True,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)
            verification_dict["ops_update"] += \
                (sub_doc_gen.end - sub_doc_gen.start
                 + len(task.fail.keys()))
            if self.durability_level in DurabilityHelper.SupportedDurability:
                verification_dict["sync_write_committed_count"] += \
                    num_item_start_for_crud

            # Edit doc_gen template to read the mutated value as well
            sub_doc_gen.template = \
                sub_doc_gen.template.replace(" }}", ", \"mutated\": \"\" }}")
            # Read all the values to validate update operation
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "read", 0,
                batch_size=100, process_concurrency=8,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

            op_failed_tbl = TableView(self.log.error)
            op_failed_tbl.set_headers(["Update failed key", "Value"])
            for key, value in task.success.items():
                doc_value = value["value"]
                failed_row = [key, doc_value]
                if doc_value[0] != 2:
                    op_failed_tbl.add_row(failed_row)
                elif doc_value[1] != "LastNameUpdate":
                    op_failed_tbl.add_row(failed_row)
                elif doc_value[2] != "TypeChange":
                    op_failed_tbl.add_row(failed_row)
                elif doc_value[3] != "CityUpdate":
                    op_failed_tbl.add_row(failed_row)
                elif json.loads(str(doc_value[4])) != ["get", "up"]:
                    op_failed_tbl.add_row(failed_row)

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
            if self.durability_level in DurabilityHelper.SupportedDurability:
                verification_dict["sync_write_committed_count"] += \
                    num_item_start_for_crud

            # Edit doc_gen template to read the mutated value as well
            sub_doc_gen.template = sub_doc_gen.template \
                .replace(" }}", ", \"mutated\": \"\" }}")
            # Read all the values to validate update operation
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "read", 0,
                batch_size=100, process_concurrency=8,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

            op_failed_tbl = TableView(self.log.error)
            op_failed_tbl.set_headers(["Delete failed key", "Value"])

            for key, value in task.success.items():
                doc_value = value["value"]
                failed_row = [key, doc_value]
                if doc_value[0] != 2:
                    op_failed_tbl.add_row(failed_row)
                for index in range(1, len(doc_value)):
                    if doc_value[index] != "PATH_NOT_FOUND":
                        op_failed_tbl.add_row(failed_row)

            for key, value in task.fail.items():
                op_failed_tbl.add_row([key, value["value"]])

            op_failed_tbl.display("Delete failed for keys:")
            if len(op_failed_tbl.rows) != 0:
                self.fail("Delete failed for few keys")
        else:
            self.log.warning("Unsupported doc_operation")

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        # Validate verification_dict and validate
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets, expected_val=verification_dict)
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
            vbuckets=self.cluster_util.vbuckets)
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_gen, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(task)

        half_of_num_items = self.num_items
        self.num_items *= 2

        # Update verification_dict and validate
        verification_dict["ops_create"] = self.num_items
        if self.durability_level:
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
                                           vbuckets=self.cluster_util.vbuckets)
            doc_gen[1] = sub_doc_generator(self.key, half_of_num_items,
                                           self.num_items,
                                           doc_size=self.sub_doc_size,
                                           target_vbucket=self.target_vbucket,
                                           vbuckets=self.cluster_util.vbuckets)
        elif doc_ops in ["upsert", "remove"]:
            self.log.info("Creating sub_docs before upsert/remove operation")
            sub_doc_gen = sub_doc_generator(self.key, 0,
                                            self.num_items,
                                            doc_size=self.sub_doc_size,
                                            target_vbucket=self.target_vbucket,
                                            vbuckets=self.cluster_util.vbuckets)
            template_index_1 = 0
            template_index_2 = 1
            if doc_ops == "remove":
                template_index_1 = 2
                template_index_2 = 2
            task = self.task.async_load_gen_sub_docs(
                self.cluster, def_bucket, sub_doc_gen, "insert", self.maxttl,
                path_create=True,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

            verification_dict["ops_update"] += self.num_items
            if self.durability_level:
                verification_dict["sync_write_committed_count"] += \
                    self.num_items

            doc_gen[0] = sub_doc_generator_for_edit(
                self.key,
                start=0,
                end=half_of_num_items,
                template_index=template_index_1,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets)
            doc_gen[1] = sub_doc_generator_for_edit(
                self.key,
                start=half_of_num_items,
                end=self.num_items,
                template_index=template_index_2,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets)
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

        verification_dict["ops_update"] += self.num_items
        if self.durability_level:
            verification_dict["sync_write_committed_count"] += self.num_items

        # Wait for all task to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets, expected_val=verification_dict)
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
        def_bucket = self.bucket_util.buckets[0]

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
        verification_dict["ops_update"] += \
            (curr_doc_gen.end - curr_doc_gen.start)
        if self.durability_level:
            verification_dict["sync_write_committed_count"] += \
                (curr_doc_gen.end - curr_doc_gen.start)

        # Create required doc_generators for CRUD ops
        doc_gen["create"] = doc_generator(self.key,
                                          self.num_items,
                                          self.num_items * 2,
                                          doc_size=self.doc_size,
                                          target_vbucket=self.target_vbucket,
                                          vbuckets=self.cluster_util.vbuckets)
        doc_gen["read"] = doc_generator(self.key,
                                        0,
                                        self.num_items)

        # Create sub-doc generators for CRUD test
        sub_doc_gen["insert"] = sub_doc_generator(self.key,
                                                  start=0,
                                                  end=insert_end_index,
                                                  doc_size=self.sub_doc_size)
        sub_doc_gen["read"] = sub_doc_generator(self.key,
                                                start=insert_end_index,
                                                end=upsert_end_index,
                                                doc_size=self.sub_doc_size)
        sub_doc_gen["upsert"] = sub_doc_generator_for_edit(
                                    self.key,
                                    start=insert_end_index,
                                    end=upsert_end_index,
                                    template_index=0,
                                    target_vbucket=self.target_vbucket,
                                    vbuckets=self.cluster_util.vbuckets)
        sub_doc_gen["remove"] = sub_doc_generator_for_edit(
                                    self.key,
                                    start=upsert_end_index,
                                    end=self.num_items,
                                    template_index=2,
                                    target_vbucket=self.target_vbucket,
                                    vbuckets=self.cluster_util.vbuckets)

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
                    timeout_secs=self.sdk_timeout))
                if op_type != "read" and self.durability_level:
                    verification_dict["sync_write_committed_count"] += \
                        mutation_count
            else:
                # Non-SyncWrites for last two ops specified in doc_ops
                tasks.append(self.task.async_load_gen_sub_docs(
                    self.cluster, def_bucket, curr_doc_gen, op_type, 0,
                    path_create=True,
                    batch_size=10, process_concurrency=1,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout))

        # Wait for all task to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Update num_items to sync with new docs created
        self.num_items *= 2
        verification_dict["ops_create"] = self.num_items
        if self.durability_level:
            verification_dict["sync_write_committed_count"] += \
                self.num_items

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Verify vb-details cbstats
        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets, expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

    def test_with_persistence_issues(self):
        """
        1. Select nodes from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify the operation succeeds
        4. Validate all mutations met the durability condition
        """

        if self.durability_level.upper() in ["MAJORITY_AND_PERSIST_ON_MASTER",
                                             "PERSIST_TO_MAJORITY"]:
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

        # Load sub_docs for upsert/remove mutation to work
        sub_doc_gen = sub_doc_generator(self.key,
                                        start=insert_end_index,
                                        end=self.num_items,
                                        doc_size=self.sub_doc_size,
                                        target_vbucket=self.target_vbucket,
                                        vbuckets=self.cluster_util.vbuckets)
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
        gen_create = sub_doc_generator(self.key,
                                       0,
                                       insert_end_index,
                                       target_vbucket=self.target_vbucket,
                                       vbuckets=self.cluster_util.vbuckets)
        gen_update = sub_doc_generator_for_edit(
            self.key,
            insert_end_index,
            upsert_end_index,
            template_index=0,
            target_vbucket=self.target_vbucket)
        gen_delete = sub_doc_generator_for_edit(
            self.key,
            upsert_end_index,
            self.num_items,
            template_index=2,
            target_vbucket=self.target_vbucket)

        self.log.info("Starting parallel doc_ops - insert/Read/upsert/remove")
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_create, "insert", 0,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_update, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_update, "upsert", 0,
            path_create=True,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_delete, "remove", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

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
            val = failover_info["init"][node.ip] \
                != failover_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="Failover stats got updated")

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
        def_bucket = self.bucket_util.buckets[0]

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

        # Load sub_docs for upsert/remove mutation to work
        sub_doc_gen = sub_doc_generator(self.key,
                                        start=0,
                                        end=self.num_items/2,
                                        doc_size=self.sub_doc_size)
        task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, sub_doc_gen, "insert", self.maxttl,
            path_create=True,
            batch_size=20, process_concurrency=8,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(task)

        for node in target_nodes:
            # Perform specified action
            error_sim[node.ip] = CouchbaseError(self.log,
                                                shell_conn[node.ip])
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
            target_vbucket=target_vbuckets)
        gen["read"] = sub_doc_generator_for_edit(
            self.key,
            self.num_items/4,
            50,
            template_index=0,
            target_vbucket=target_vbuckets)
        gen["upsert"] = sub_doc_generator_for_edit(
            self.key,
            self.num_items/4,
            50,
            template_index=0,
            target_vbucket=target_vbuckets)
        gen["remove"] = sub_doc_generator_for_edit(
            self.key,
            0,
            50,
            template_index=2,
            target_vbucket=target_vbuckets)

        self.log.info("Starting parallel doc_ops - insert/Read/upsert/remove")
        tasks["insert"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["insert"], "insert", 0,
            path_create=True,
            batch_size=1, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout)
        tasks["read"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["read"], "read", 0,
            batch_size=1, process_concurrency=1,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout)
        tasks["upsert"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["upsert"], "upsert", 0,
            path_create=True,
            batch_size=1, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout)
        tasks["remove"] = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen["remove"], "remove", 0,
            batch_size=1, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            print_ops_rate=False,
            timeout_secs=self.sdk_timeout)

        # Wait for document_loader tasks to complete
        for _, task in tasks.items():
            self.task_manager.get_task_result(task)

        # Revert the induced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Read mutation field from all docs for validation
        gen_read = sub_doc_generator_for_edit(self.key, 0, self.num_items, 0)
        gen_read.template = '{{ "mutated": "" }}'
        reader_task = self.task.async_load_gen_sub_docs(
            self.cluster, def_bucket, gen_read, "read",
            batch_size=50, process_concurrency=8,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(reader_task)

        # Validation for each CRUD task
        for op_type, task in tasks.items():
            if len(task.success.keys()) != len(gen[op_type].doc_keys):
                self.log_failure("Failure during %s operation" % op_type)
            elif len(task.fail.keys()) != 0:
                self.log_failure("Some CRUD failed during %s: %s"
                                 % (op_type, task.fail))

            for doc_key, crud_result in task.success.items():
                if crud_result["cas"] == 0:
                    self.log_failure("%s failed for %s: %s"
                                     % (op_type, doc_key, crud_result))
                if op_type == "insert":
                    if reader_task.success[doc_key]["value"][0] != 1:
                        self.log_failure("%s value mismatch for %s: %s"
                                         % (op_type, doc_key, crud_result))
                elif op_type in ["upsert", "remove"]:
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
            shell_conn[node.ip].disconnect()

        # Verify doc count
        self.log.info("Validating doc count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()

    def test_doc_expiry_before_commit(self):
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
        bucket = self.bucket_util.buckets[0]
        vb_for_key = self.bucket_util.get_vbucket_num_for_key(key)

        # Open SDK client
        client = SDKClient([self.cluster.master], bucket)

        # Create Sync_write doc with xattr + doc_ttl=10s
        client.crud(DocLoading.Bucket.DocOps.CREATE, key, {},
                    durability=self.durability_level)
        client.crud("subdoc_insert", key, ["_sdkey", "abc123"],
                    xattr=True, durability=self.durability_level)
        client.crud(DocLoading.Bucket.DocOps.UPDATE, key, {},
                    durability=self.durability_level, exp=doc_ttl)

        # Wait for all items to get persist
        self.log.info("Waiting for ep_queue_size to become zero")
        self.bucket_util._wait_for_stats_all_buckets()

        self.sleep(doc_ttl, "Wait for doc to expire")

        self.log.info("Loading more docs into the targeted vb: %s"
                      % vb_for_key)
        doc_gen = doc_generator(self.key, self.num_items, 1000,
                                target_vbucket=[vb_for_key])
        doc_load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.UPDATE,
            durability=self.durability_level)
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
