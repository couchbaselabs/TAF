from eventing.eventing_constants import HANDLER_CODE
from eventing.eventing_base import EventingBaseTest
from membase.helper.cluster_helper import ClusterOperationHelper
from BucketLib.bucket import Bucket
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class EventingSanity(EventingBaseTest):
    def setUp(self):
        super(EventingSanity, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.bucket_size = 200
            self.log.info(self.bucket_size)
            bucket_params_src = Bucket({"name": self.src_bucket_name, "replicaNumber": self.num_replicas})
            src_bucket = self.bucket_util.create_bucket(bucket_params_src)
            self.src_bucket = self.bucket_util.get_all_buckets(self.cluster.master)[0]
            bucket_params_dst = Bucket({"name": self.dst_bucket_name, "replicaNumber": self.num_replicas})
            bucket_params_meta = Bucket({"name": self.metadata_bucket_name, "replicaNumber": self.num_replicas})
            bucket_dst = self.bucket_util.create_bucket(bucket_params_dst)
            bucket_meta = self.bucket_util.create_bucket(bucket_params_meta)
            self.buckets = self.bucket_util.get_all_buckets(self.cluster.master)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3

    def tearDown(self):
        super(EventingSanity, self).tearDown()

    def test_create_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_delete_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE, worker_count=3)
        self.deploy_function(body)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True)
        self.undeploy_and_delete_function(body)

    def test_expiry_mutation_for_dcp_stream_boundary_from_beginning(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_DELETE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, on_delete=True)
        self.undeploy_and_delete_function(body)

    def test_update_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE,
                                              dcp_stream_boundary="from_now", sock_batch_size=1, worker_count=4,
                                              cpp_worker_thread_count=4)
        self.deploy_function(body)
        # update all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="update")
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_n1ql_query_execution_from_handler_code(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE, worker_count=3)
        # Enable this after MB-26527 is fixed
        # sock_batch_size=10, worker_count=4, cpp_worker_thread_count=4)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code_with_n1ql(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_cron_timer_events_from_handler_code_with_n1ql(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE_WITH_CRON_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_doc_timer_events_from_handler_code_with_bucket_ops(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_cron_timer_events_from_handler_code_with_bucket_ops(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_delete_bucket_operation_from_handler_code(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_timers_without_context(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMER_WITHOUT_CONTEXT,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_cancel_timers_with_timers_being_overwritten(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_TIMER_OVERWRITTEN,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016,deletes=True,timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_source_doc_mutations_with_timers(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION_WITH_TIMERS,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # self.verify_source_bucket_mutation(self.docs_per_day * 2016,deletes=True,timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 4032, skip_stats_validation=True)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_source_bucket_mutations_with_timers(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS,
                                              worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        #self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        #self.verify_source_bucket_mutation(self.docs_per_day * 2016)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 4032, skip_stats_validation=True)
        # delete all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_pause_resume_execution(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        self.pause_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.gens_load = self.generate_docs(self.docs_per_day*2)
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


    def test_source_bucket_mutation_for_dcp_stream_boundary_from_now(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name,HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION ,
                                              dcp_stream_boundary="from_now", sock_batch_size=1, worker_count=4,
                                              cpp_worker_thread_count=4)
        self.deploy_function(body)
        # update all documents
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="update")
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2)
        self.undeploy_and_delete_function(body)

    def test_compress_handler(self):
        self.load(load_gen=self.gens_load, bucket=self.src_bucket)
        body = self.create_save_function_body(self.function_name,"handler_code/compress.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.load(load_gen=self.gens_load, bucket=self.src_bucket, operation="delete")
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_with_aborts(self):
        """
        1. Create index (2i/view) on default bucket
        2. Load multiple docs such that all sync_writes will be aborted
        3. Verify nothing went into indexing
        4. Load sync_write docs such that they are successful
        5. Validate the mutated docs are taken into indexing
        :return:
        """
        self.key = "test_query_doc"
        self.sync_write_abort_pattern = self.input.param("sync_write_abort_pattern", "all_aborts")
        self.create_eventing_during = self.input.param("create_eventing_during", "before_doc_ops")
        crud_batch_size = 50
        def_bucket = self.src_bucket
        kv_nodes = self.cluster_util.get_kv_nodes()
        replica_vbs = dict()
        verification_dict = dict()
        load_gen = dict()
        load_gen["ADD"] = dict()
        load_gen["SET"] = dict()
        partial_aborts = ["initial_aborts", "aborts_at_end"]

        durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)

        if self.create_eventing_during == "before_doc_ops":
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
            self.deploy_function(body)

        curr_items = self.bucket_util.get_bucket_current_item_count(
            self.cluster, def_bucket)
        if self.sync_write_abort_pattern in ["all_aborts", "initial_aborts"]:
            self.bucket_util.flush_bucket(kv_nodes[0], def_bucket)
            self.num_items = 0
        else:
            self.num_items = curr_items

        self.log.info("Disabling auto_failover to avoid node failures")
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Validate vbucket stats
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        self.log.info("Loading docs such that all sync_writes will be aborted")
        for server in kv_nodes:
            ssh_shell = RemoteMachineShellConnection(server)
            cbstats = Cbstats(ssh_shell)
            replica_vbs[server] = cbstats.vbucket_list(def_bucket.name,
                                                       "replica")
            load_gen["ADD"][server] = list()
            load_gen["ADD"][server].append(doc_generator(
                self.key, 0, crud_batch_size,
                target_vbucket=replica_vbs[server],
                mutation_type="ADD"))
            if self.sync_write_abort_pattern in partial_aborts:
                load_gen["ADD"][server].append(doc_generator(
                    self.key, 10000, crud_batch_size,
                    target_vbucket=replica_vbs[server],
                    mutation_type="ADD"))
                verification_dict["ops_create"] += crud_batch_size
                verification_dict["sync_write_committed_count"] += \
                    crud_batch_size

            task_success = self.bucket_util.load_durable_aborts(
                ssh_shell, load_gen["ADD"][server], def_bucket,
                self.durability_level,
                "create", self.sync_write_abort_pattern)
            if not task_success:
                self.log_failure("Failure during load_abort task")

            verification_dict["sync_write_aborted_count"] += \
                crud_batch_size
            if self.create_eventing_during == "before_doc_ops":
                self.verify_eventing_results(self.function_name, verification_dict["ops_create"],
                                             skip_stats_validation=True)

            load_gen["SET"][server] = list()
            load_gen["SET"][server].append(doc_generator(
                self.key, 0, crud_batch_size,
                target_vbucket=replica_vbs[server],
                mutation_type="SET"))
            if self.sync_write_abort_pattern in partial_aborts:
                load_gen["SET"][server].append(doc_generator(
                    self.key, 10000, crud_batch_size,
                    target_vbucket=replica_vbs[server],
                    mutation_type="SET"))
                verification_dict["ops_update"] += crud_batch_size
                verification_dict["sync_write_committed_count"] += \
                    crud_batch_size

            verification_dict["sync_write_aborted_count"] += \
                crud_batch_size
            task_success = self.bucket_util.load_durable_aborts(
                ssh_shell, load_gen["SET"][server], def_bucket,
                self.durability_level,
                "update", self.sync_write_abort_pattern)
            if not task_success:
                self.log_failure("Failure during load_abort task")
            ssh_shell.disconnect()
            if self.create_eventing_during == "before_doc_ops":
                self.verify_eventing_results(self.function_name, verification_dict["ops_create"],
                                             skip_stats_validation=True)

        failed = durability_helper.verify_vbucket_details_stats(
            def_bucket, kv_nodes,
            vbuckets=self.vbuckets, expected_val=verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details verification failed")
        self.validate_test_failure()

        if self.create_eventing_during == "after_doc_ops":
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
            self.deploy_function(body)
            self.verify_eventing_results(self.function_name, verification_dict["ops_create"],
                                         skip_stats_validation=True)
        self.log.info("Verify aborts are not consumed by eventing")
        self.verify_eventing_results(self.function_name, verification_dict["ops_create"],
                                     skip_stats_validation=True)

        for server in kv_nodes:
            if self.sync_write_abort_pattern == "initial_aborts":
                load_gen["ADD"][server] = load_gen["ADD"][server][:1]
                load_gen["SET"][server] = load_gen["SET"][server][:1]
            elif self.sync_write_abort_pattern == "aborts_at_end":
                load_gen["ADD"][server] = load_gen["ADD"][server][-1:]
                load_gen["SET"][server] = load_gen["SET"][server][-1:]

        self.log.info("Load sync_write docs such that they are successful")
        for server in kv_nodes:
            for gen_load in load_gen["ADD"][server]:
                task = self.task.async_load_gen_docs(
                    self.cluster, def_bucket, gen_load, "create", 0,
                    batch_size=50, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)
                verification_dict["ops_create"] += crud_batch_size
                if len(task.fail.keys()) != 0:
                    self.log_failure("Some failures seen during doc_ops")
                self.verify_eventing_results(self.function_name, verification_dict["ops_create"],
                                             skip_stats_validation=True)

            for gen_load in load_gen["SET"][server]:
                task = self.task.async_load_gen_docs(
                    self.cluster, def_bucket, gen_load, "update", 0,
                    batch_size=50, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)
                verification_dict["ops_update"] += crud_batch_size
                if len(task.fail.keys()) != 0:
                    self.log_failure("Some failures seen during doc_ops")
                self.verify_eventing_results(self.function_name, verification_dict["ops_update"],
                                             skip_stats_validation=True)

        self.log.info("Validate the mutated docs are taken into eventing")
        self.verify_eventing_results(self.function_name, verification_dict["ops_create"],
                                     skip_stats_validation=True)
        self.validate_test_failure()

    def test_fts_index_with_aborts(self):
        """
        1. Create index (2i/view) on default bucket
        2. Load multiple docs such that all sync_writes will be aborted
        3. Verify nothing went into indexing
        4. Load sync_write docs such that they are successful
        5. Validate the mutated docs are taken into indexing
        :return:
        """
        self.key = "test_query_doc"
        self.index_name = "fts_test_index"
        self.sync_write_abort_pattern = self.input.param("sync_write_abort_pattern", "all_aborts")
        self.create_index_during = self.input.param("create_index_during", "before_doc_ops")
        self.restServer = self.cluster_util.get_nodes_from_services_map(service_type="fts")
        self.rest = RestConnection(self.restServer)
        crud_batch_size = 1000
        def_bucket = self.bucket_util.buckets[0]
        kv_nodes = self.cluster_util.get_kv_nodes()
        replica_vbs = dict()
        verification_dict = dict()
        index_item_count = dict()
        expected_num_indexed = dict()
        load_gen = dict()
        load_gen["ADD"] = dict()
        load_gen["SET"] = dict()
        partial_aborts = ["initial_aborts", "aborts_at_end"]

        durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)

        if self.create_index_during == "before_doc_ops":
            self.create_fts_indexes(def_bucket.name, self.index_name)

        curr_items = self.bucket_util.get_bucket_current_item_count(
            self.cluster, def_bucket)
        if self.sync_write_abort_pattern in ["all_aborts", "initial_aborts"]:
            self.bucket_util.flush_bucket(kv_nodes[0], def_bucket)
            self.num_items = 0
        else:
            self.num_items = curr_items

        self.log.info("Disabling auto_failover to avoid node failures")
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Validate vbucket stats
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        # verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.create_index_during == "before_doc_ops":
            self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])

        self.log.info("Loading docs such that all sync_writes will be aborted")
        for server in kv_nodes:
            ssh_shell = RemoteMachineShellConnection(server)
            cbstats = Cbstats(ssh_shell)
            replica_vbs[server] = cbstats.vbucket_list(def_bucket.name,
                                                       "replica")
            load_gen["ADD"][server] = list()
            load_gen["ADD"][server].append(doc_generator(
                self.key, 0, crud_batch_size,
                target_vbucket=replica_vbs[server],
                mutation_type="ADD"))
            if self.sync_write_abort_pattern in partial_aborts:
                load_gen["ADD"][server].append(doc_generator(
                    self.key, 10000, crud_batch_size,
                    target_vbucket=replica_vbs[server],
                    mutation_type="ADD"))
                verification_dict["ops_create"] += crud_batch_size
                verification_dict["sync_write_committed_count"] += \
                    crud_batch_size


            task_success = self.bucket_util.load_durable_aborts(
                ssh_shell, load_gen["ADD"][server], def_bucket,
                self.durability_level,
                "create", self.sync_write_abort_pattern)
            if not task_success:
                self.log_failure("Failure during load_abort task")

            verification_dict["sync_write_aborted_count"] += \
                crud_batch_size
            if self.create_index_during == "before_doc_ops":
                self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])

            load_gen["SET"][server] = list()
            load_gen["SET"][server].append(doc_generator(
                self.key, 0, crud_batch_size,
                target_vbucket=replica_vbs[server],
                mutation_type="SET"))
            if self.sync_write_abort_pattern in partial_aborts:
                load_gen["SET"][server].append(doc_generator(
                    self.key, 10000, crud_batch_size,
                    target_vbucket=replica_vbs[server],
                    mutation_type="SET"))
                verification_dict["ops_update"] += crud_batch_size
                verification_dict["sync_write_committed_count"] += \
                    crud_batch_size

            verification_dict["sync_write_aborted_count"] += \
                crud_batch_size
            task_success = self.bucket_util.load_durable_aborts(
                ssh_shell, load_gen["SET"][server], def_bucket,
                self.durability_level,
                "update", self.sync_write_abort_pattern)
            if not task_success:
                self.log_failure("Failure during load_abort task")

            ssh_shell.disconnect()

            if self.create_index_during == "before_doc_ops":
                self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])
        failed = durability_helper.verify_vbucket_details_stats(
            def_bucket, kv_nodes,
            vbuckets=self.vbuckets, expected_val=verification_dict)
        # if failed:
        #     self.sleep(6000)
        #     self.log_failure("Cbstat vbucket-details verification failed")
        self.validate_test_failure()

        if self.create_index_during == "after_doc_ops":
            self.create_fts_indexes(def_bucket.name, self.index_name)
            self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])

        self.log.info("Verify aborts are not indexed")
        self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])

        for server in kv_nodes:
            if self.sync_write_abort_pattern == "initial_aborts":
                load_gen["ADD"][server] = load_gen["ADD"][server][:1]
                load_gen["SET"][server] = load_gen["SET"][server][:1]
            elif self.sync_write_abort_pattern == "aborts_at_end":
                load_gen["ADD"][server] = load_gen["ADD"][server][-1:]
                load_gen["SET"][server] = load_gen["SET"][server][-1:]

        self.log.info("Load sync_write docs such that they are successful")
        for server in kv_nodes:
            for gen_load in load_gen["ADD"][server]:
                task = self.task.async_load_gen_docs(
                    self.cluster, def_bucket, gen_load, "create", 0,
                    batch_size=50, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)
                if len(task.fail.keys()) != 0:
                    self.log_failure("Some failures seen during doc_ops")
                verification_dict["ops_create"] += crud_batch_size
                self.validate_indexed_doc_count(self.index_name, verification_dict["ops_create"])

            for gen_load in load_gen["SET"][server]:
                task = self.task.async_load_gen_docs(
                    self.cluster, def_bucket, gen_load, "update", 0,
                    batch_size=50, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)
                if len(task.fail.keys()) != 0:
                    self.log_failure("Some failures seen during doc_ops")
                verification_dict["ops_update"] += crud_batch_size
                self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])

        self.log.info("Validate the mutated docs are taken into indexing")
        self.validate_indexed_doc_count(self.index_name , verification_dict["ops_create"])
        self.validate_test_failure()

    def _create_fts_index_params(self, bucket_name, bucket_uuid, index_name):
        body = {}
        body['type'] = "fulltext-index"
        body['name'] = index_name
        body['sourceType'] = "couchbase"
        body['sourceName'] = bucket_name
        body['sourceUUID'] = bucket_uuid
        body['planParams'] = {}
        body['planParams']['maxPartitionsPerPIndex'] = 171
        body['planParams']['indexPartitions'] = 6
        body['params'] = {}
        body['params']['doc_config'] = {}
        body['params']['mapping'] = {}
        body['params']['store'] = {}
        body['params']['doc_config']['docid_prefix_delim'] = ""
        body['params']['doc_config']['docid_regexp'] = ""
        body['params']['doc_config']['mode'] = "type_field"
        body['params']['doc_config']['type_field'] = "type"
        body['params']['mapping']['analysis'] = {}
        body['params']['mapping']['default_analyzer'] = "standard"
        body['params']['mapping']['default_datetime_parser'] = "dateTimeOptional"
        body['params']['mapping']['default_field'] = "_all"
        body['params']['mapping']['default_mapping'] = {}
        body['params']['mapping']['default_mapping']['dynamic'] = True
        body['params']['mapping']['default_mapping']['enabled'] = True
        body['params']['mapping']['default_type'] = "_default"
        body['params']['mapping']['docvalues_dynamic'] = True
        body['params']['mapping']['index_dynamic'] = True
        body['params']['mapping']['store_dynamic'] = False
        body['params']['mapping']['type_field'] = "_type"
        body['params']['store']['indexType'] = "scorch"
        body['sourceParams'] = {}
        return body

    def create_fts_indexes(self, bucket, index_name):
        bucket_stats = self.bucket_helper.get_bucket_json(bucket)
        bucket_uuid = bucket_stats["uuid"]
        params = self._create_fts_index_params(bucket, bucket_uuid, index_name)
        self.rest.create_fts_index(index_name, params)

    def validate_indexed_doc_count(self, index, expected_index_item_count):
        actual_item_count = self.rest.get_fts_index_doc_count(index)
        print("actual_item_count : {0} expected_index_item_count : {1}".format(actual_item_count, expected_index_item_count))
        if expected_index_item_count != actual_item_count:
            raise Exception("data mismatch in fts index")
