from copy import deepcopy

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading
from bucket_collections.collections_base import CollectionBase
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from error_simulation.disk_error import DiskError
from remote.remote_util import RemoteMachineShellConnection


class CollectionsSuccessTests(CollectionBase):
    def setUp(self):
        super(CollectionsSuccessTests, self).setUp()
        self.bucket = self.cluster.buckets[0]

    def tearDown(self):
        super(CollectionsSuccessTests, self).tearDown()

    def __perform_collection_crud(self, mutation_num=1,
                                  verification_dict=None):
        collection_crud_spec = dict()
        collection_crud_spec["doc_crud"] = dict()
        collection_crud_spec[
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 2
        collection_crud_spec[
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 5
        collection_crud_spec[
            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 10
        collection_crud_spec["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 100
        collection_crud_spec["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"

        collection_crud_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                collection_crud_spec,
                mutation_num=mutation_num)
        if collection_crud_task.result is False:
            self.log_failure("Collection CRUD failed")

        if verification_dict is not None:
            self.update_verification_dict_from_collection_task(
                verification_dict,
                collection_crud_task)

    def test_basic_ops(self):
        """
        Basic tests for document CRUD operations using JSON docs
        """
        load_spec = dict()
        verification_dict = dict()

        # Stat validation reference variables
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        for _, scope in self.bucket.scopes.items():
            for _, collection in scope.collections.items():
                verification_dict["ops_create"] += collection.num_items
                if self.durability_level in self.supported_d_levels:
                    verification_dict["sync_write_committed_count"] \
                        += collection.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # load_spec["target_vbuckets"] = []
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][MetaCrudParams.DocCrud.COMMON_DOC_KEY] \
            = "test_collections"

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=2,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud(verification_dict=verification_dict)

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")
        self.validate_test_failure()

        self.log.info("Validating doc_count in buckets")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Validate vbucket stats
        self.update_verification_dict_from_collection_task(verification_dict,
                                                           doc_loading_task)

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        self.validate_cruds_from_collection_mutation(doc_loading_task)

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

        if self.durability_level in [
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
        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)

        self.log.info("Simulate error condition on %s" % target_nodes)
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
        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud(mutation_num=2)

        # Wait for doc_loading to complete and validate the doc ops
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed with persistence issue")

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

        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(self.bucket.name)

            # Failover validation
            val = \
                failover_info["init"][node.ip] \
                == failover_info["afterCrud"][node.ip]
            error_msg = "Failover stats got updated"
            self.assertTrue(val, msg=error_msg)

            # Seq_no validation (High level)
            val = \
                vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        self.validate_test_failure()

        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

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

        # Override num_of_nodes affected to 1 (Positive case)
        self.num_nodes_affected = 1

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
        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)

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
        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"
        load_spec["target_vbuckets"] = list(set(range(0, 1024))
                                            ^ set(active_vbs_in_target_nodes))

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        self.sleep(5, "Wait for doc loaders to start loading data")

        for node in target_nodes:
            # Create shell_connections
            shell_conn[node.ip] = RemoteMachineShellConnection(node)

            # Perform specified action
            error_sim[node.ip] = CouchbaseError(self.log,
                                                shell_conn[node.ip])
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud()

        # Wait for document_loader tasks to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed with process crash")

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
                result = None
                while retry_num != 2:
                    result = self.task.rebalance(
                        self.servers[0:self.nodes_init],
                        [], [])
                    if result:
                        break
                    retry_num += 1
                    self.sleep(10, "Wait before retrying rebalance")

                self.assertTrue(result, "Rebalance failed")

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
            val = \
                vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Doc count validation
        self.validate_test_failure()
        self.bucket_util.validate_docs_per_collections_all_buckets()

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

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        for _, scope in self.bucket.scopes.items():
            for _, collection in scope.collections.items():
                verification_dict["ops_create"] += collection.num_items
                if self.durability_level in self.supported_d_levels:
                    verification_dict["sync_write_committed_count"] \
                        += collection.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        doc_ops = self.input.param("doc_ops", "create")
        # Reset initial doc_loading params to NO_OPS
        doc_load_template = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        doc_load_template[MetaCrudParams.DURABILITY_LEVEL] = ""
        doc_load_template[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 3
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"

        # Create required doc_generators for CRUD ops
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 25
        if DocLoading.Bucket.DocOps.CREATE in doc_ops:
            doc_load_template["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        elif DocLoading.Bucket.DocOps.UPDATE in doc_ops:
            doc_load_template["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 50
        elif DocLoading.Bucket.DocOps.DELETE in doc_ops:
            doc_load_template["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 50

        async_write_crud_spec = deepcopy(doc_load_template)
        sync_write_crud_spec = deepcopy(doc_load_template)

        sync_write_crud_spec[MetaCrudParams.DURABILITY_LEVEL] = \
            self.durability_level

        async_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                async_write_crud_spec,
                mutation_num=1,
                async_load=True)
        sync_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                sync_write_crud_spec,
                mutation_num=2,
                async_load=True)

        # Wait for all task to complete
        self.task.jython_task_manager.get_task_result(async_write_loading_task)
        self.task.jython_task_manager.get_task_result(sync_write_loading_task)

        # Validate CRUD loading results
        self.bucket_util.validate_doc_loading_results(async_write_loading_task)
        self.bucket_util.validate_doc_loading_results(sync_write_loading_task)

        if async_write_loading_task.result is False:
            self.log_failure("Doc_ops failed in async_write_task")
        if sync_write_loading_task.result is False:
            self.log_failure("Doc_ops failed in sync_write_task")

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_crud_with_transaction(self):
        num_items_in_transaction = self.num_items
        transaction_time = self.input.param("transaction_start_time",
                                            "before_collection_crud")
        gen_create = doc_generator("atomicity_doc",
                                   0, num_items_in_transaction)
        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"

        # Create transaction task
        atomicity_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets, gen_create,
            DocLoading.Bucket.DocOps.CREATE, 0, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            transaction_timeout=self.transaction_timeout,
            commit=self.transaction_commit,
            durability=self.durability_level,
            sync=True, defer=self.defer,
            start_task=False)

        if transaction_time == "before_collection_crud":
            self.log.info("Starting transaction creates")
            self.task_manager.add_new_task(atomicity_task)

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        if transaction_time == "after_collection_crud":
            self.log.info("Starting transaction creates")
            self.task_manager.add_new_task(atomicity_task)

        # Wait for document_loader and transaction tasks to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.task_manager.get_task_result(atomicity_task)

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.validate_cruds_from_collection_mutation(doc_loading_task)

    def test_sub_doc_basic_ops(self):
        """
        Basic test for Sub-doc CRUD operations
        """
        load_spec = dict()

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        for _, scope in self.bucket.scopes.items():
            for _, collection in scope.collections.items():
                verification_dict["ops_create"] += collection.num_items
                if self.durability_level in self.supported_d_levels:
                    verification_dict["sync_write_committed_count"] \
                        += collection.num_items

        # Initial validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        # load_spec["target_vbuckets"] = []
        load_spec["doc_crud"] = dict()
        load_spec["subdoc_crud"] = dict()
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 100
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 0
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 0
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"
        load_spec[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = "all"
        load_spec[MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD] = "all"
        load_spec[MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD] = "all"
        load_spec[MetaCrudParams.DURABILITY_LEVEL] = self.durability_level

        self.log.info("Perform initial 'insert' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")
        self.validate_test_failure()

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Validate vbucket stats
        self.update_verification_dict_from_collection_task(verification_dict,
                                                           doc_loading_task)
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 0
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 30
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 30

        self.log.info("Perform 'upsert' & 'remove' mutations")
        doc_loading_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=2,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud(mutation_num=3,
                                       verification_dict=verification_dict)

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")
        self.update_verification_dict_from_collection_task(verification_dict,
                                                           doc_loading_task)
        self.validate_test_failure()

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Validate verification_dict and validate
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            # MB-39963 - Not failing due to known behavior
            self.log.warning("Cbstat vbucket-details verification failed")
        # self.validate_cruds_from_collection_mutation(doc_loading_task)

    def test_sub_doc_non_overlapping_similar_crud(self):
        """
        Test to run non-overlapping durability cruds on single bucket
        and make sure all CRUD operation succeeds

        1. Run single task_1 with durability operation
        2. Create parallel task to run either SyncWrite / Non-SyncWrite
           operation based on the config param and run that over the docs
           such that it will not overlap with other tasks
        3. Make sure all CRUDs succeeded without any unexpected exceptions
        """
        doc_ops = self.input.param("op_type",
                                   DocLoading.Bucket.SubDocOps.INSERT)

        # Create new docs for sub-doc operations to run
        self.load_data_for_sub_doc_ops()

        # Create required doc_generators for CRUD ops
        doc_load_template = dict()
        doc_load_template["doc_crud"] = dict()
        doc_load_template["subdoc_crud"] = dict()
        doc_load_template[MetaCrudParams.DURABILITY_LEVEL] = ""
        doc_load_template[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 3

        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 50

        if DocLoading.Bucket.SubDocOps.INSERT in doc_ops:
            doc_load_template["subdoc_crud"][
                MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 20
        elif DocLoading.Bucket.SubDocOps.UPSERT in doc_ops:
            doc_load_template["subdoc_crud"][
                MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 10
        elif DocLoading.Bucket.SubDocOps.REMOVE in doc_ops:
            doc_load_template["subdoc_crud"][
                MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 10

        async_write_crud_spec = deepcopy(doc_load_template)
        sync_write_crud_spec = deepcopy(doc_load_template)

        sync_write_crud_spec[MetaCrudParams.DURABILITY_LEVEL] = \
            self.durability_level

        async_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                async_write_crud_spec,
                mutation_num=1,
                async_load=True)
        sync_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                sync_write_crud_spec,
                mutation_num=1,
                async_load=True)

        # Wait for all task to complete
        self.task.jython_task_manager.get_task_result(async_write_loading_task)
        self.task.jython_task_manager.get_task_result(sync_write_loading_task)

        # Validate CRUD loading results
        self.bucket_util.validate_doc_loading_results(async_write_loading_task)
        self.bucket_util.validate_doc_loading_results(sync_write_loading_task)

        if async_write_loading_task.result is False:
            self.log_failure("Doc_ops failed in async_write_task")
        if sync_write_loading_task.result is False:
            self.log_failure("Doc_ops failed in sync_write_task")

        # Verify doc count and other stats
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_sub_doc_with_persistence_issues(self):
        """
        1. Select nodes from the cluster to simulate the specified error
        2. Perform CRUD on the target bucket with given timeout
        3. Using cbstats to verify the operation succeeds
        4. Validate all mutations met the durability condition
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
        def_bucket = self.cluster.buckets[0]

        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["subdoc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.COMMON_DOC_KEY] = "test_collections"
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 50
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 20
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 10
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 10

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)

        # Create new docs for sub-doc operations to run
        self.load_data_for_sub_doc_ops()

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

        # Perform CRUDs with induced error scenario is active
        self.log.info("Perform 'insert', 'upsert', 'remove' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=0,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud(mutation_num=1)

        # Wait for doc_loading to complete and validate the doc ops
        self.task_manager.get_task_result(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed with persistence issue")

        # Revert the induced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(def_bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

            # Failover validation
            val = \
                failover_info["init"][node.ip] \
                == failover_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="Failover stats not updated")

            # Seq_no validation (High level)
            val = \
                vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Disconnect the shell connection
        for node in target_nodes:
            shell_conn[node.ip].disconnect()

        self.validate_test_failure()
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_sub_doc_with_process_crash(self):
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
        active_vbs_in_target_nodes = list()
        failover_info["init"] = dict()
        failover_info["afterCrud"] = dict()
        vb_info_info["init"] = dict()
        vb_info_info["afterCrud"] = dict()
        def_bucket = self.cluster.buckets[0]

        self.load_data_for_sub_doc_ops()

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)

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

            # Remove active vbuckets from doc_loading to avoid errors

        load_spec = dict()
        # load_spec["target_vbuckets"] = list(set(target_vbuckets)
        #                                    ^ set(active_vbs_in_target_nodes))
        load_spec["doc_crud"] = dict()
        load_spec["subdoc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 10
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 50
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 25
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 25

        self.log.info("Perform 'create', 'update', 'delete' mutations")

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        self.sleep(5, "Wait for doc loaders to start loading data")

        for node in target_nodes:
            # Perform specified action
            error_sim[node.ip] = CouchbaseError(self.log,
                                                shell_conn[node.ip])
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud(mutation_num=2)

        # Wait for document_loader tasks to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Sub_doc CRUDs failed with process crash")

        # Revert the induced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=def_bucket.name)

        # Fetch latest failover stats and validate the values are updated
        self.log.info("Validating failover and seqno cbstats")
        for node in target_nodes:
            vb_info_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(def_bucket.name)
            failover_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].failover_stats(def_bucket.name)

            # Failover validation
            val = \
                failover_info["init"][node.ip] \
                == failover_info["afterCrud"][node.ip]
            error_msg = "Failover stats not updated after error condition"
            self.assertTrue(val, msg=error_msg)

            # Seq_no validation (High level)
            val = \
                vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        # Disconnect the shell connection
        for node in target_nodes:
            shell_conn[node.ip].disconnect()

        self.validate_test_failure()
        # Doc count validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()
