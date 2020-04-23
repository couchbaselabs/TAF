from copy import deepcopy
from random import randint

from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from bucket_collections.collections_base import CollectionBase
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from error_simulation.cb_error import CouchbaseError
from error_simulation.disk_error import DiskError
from remote.remote_util import RemoteMachineShellConnection


class CollectionsSuccessTests(CollectionBase):
    def setUp(self):
        super(CollectionsSuccessTests, self).setUp()
        self.bucket = self.bucket_util.buckets[0]

    def tearDown(self):
        super(CollectionsSuccessTests, self).tearDown()

    def __getTargetNodes(self):
        def select_randam_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != self.num_nodes_affected:
                select_randam_node(node_list)
        else:
            node_list.append(self.cluster.master)
        return node_list

    def __load_data_for_sub_doc_ops(self):
        new_data_load_template = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        new_data_load_template[MetaCrudParams.DURABILITY_LEVEL] = \
            self.durability_level
        new_data_load_template["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        new_data_load_template["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 50
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                new_data_load_template,
                mutation_num=0)
        if doc_loading_task.result is False:
            self.fail("Extra doc loading task failed")


    def __perform_collection_crud(self):
        collection_crud_spec = dict()
        collection_crud_spec[
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 2
        collection_crud_spec[
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 5
        collection_crud_spec[
            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 10
        collection_crud_spec["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 100

        collection_crud_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                collection_crud_spec)
        if collection_crud_task.result is False:
            self.log_failure("Collection CRUD failed")

    def test_basic_ops(self):
        """
        Basic tests for document CRUD operations using JSON docs
        """
        load_spec = dict()
        supported_d_levels = self.bucket_util.get_supported_durability_levels()

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        docs_mutated = self.num_items / 2
        # TODO: Add support for target vb testing
        # load_spec["target_vbuckets"] = []
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 25

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud()

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")

        self.validate_test_failure()

        # Update ref values
        verification_dict["ops_create"] += self.num_items
        verification_dict["ops_update"] += docs_mutated
        verification_dict["ops_delete"] += docs_mutated
        if self.durability_level in supported_d_levels:
            # For create op
            verification_dict["sync_write_committed_count"] += self.num_items
            # For update op
            verification_dict["sync_write_committed_count"] += docs_mutated
            # For delete op
            verification_dict["sync_write_committed_count"] += docs_mutated

        # Validate vbucket stats
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Read all the values to validate delete operation
        task = self.task.async_validate_docs(
            self.cluster, self.bucket,
            None, "delete", 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            sdk_client_pool=self.sdk_client_pool)
        self.task.jython_task_manager.get_task_result(task)

        self.log.info("Validating doc_count in buckets")
        self.bucket_util.validate_docs_per_collections_all_buckets()

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
        target_nodes = self.__getTargetNodes()

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

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                load_spec,
                mutation_num=0,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud()

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

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

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

        # Doc count validation
        self.validate_test_failure()
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
        target_nodes = self.__getTargetNodes()

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
        # TODO: Add support for target vb testing
        # load_spec["target_vbuckets"] = list(set(target_vbuckets)
        #                                ^ set(active_vbs_in_target_nodes))
        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 25

        self.log.info("Perform 'create', 'update', 'delete' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
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

        doc_ops = self.input.param("doc_ops", "create")

        # Reset initial doc_loading params to NO_OPS
        doc_load_template = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        doc_load_template[MetaCrudParams.DURABILITY_LEVEL] = ""
        doc_load_template[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 3
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0

        # Create required doc_generators for CRUD ops
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 25
        if "create" in doc_ops:
            doc_load_template["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        elif "update" in doc_ops:
            doc_load_template["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 50
        elif "delete" in doc_ops:
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
                self.bucket_util.buckets,
                async_write_crud_spec,
                mutation_num=1,
                async_load=True)
        sync_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
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

        # Create transaction task
        atomicity_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.bucket_util.buckets, gen_create,
            "create", 0, batch_size=self.batch_size,
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
                self.bucket_util.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        if transaction_time == "after_collection_crud":
            self.log.info("Starting transaction creates")
            self.task_manager.add_new_task(atomicity_task)

        # Wait for document_loader tasks to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        self.task_manager.get_task_result(atomicity_task)

        # Update default collection's num_items for transactional creates
        self.bucket.scopes[
            CbServer.default_scope].collections[
            CbServer.default_collection].num_items += self.num_items

        # Verify doc count and other stats
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_sub_doc_basic_ops(self):
        """
        Basic test for Sub-doc CRUD operations
        """
        load_spec = dict()
        supported_d_levels = self.bucket_util.get_supported_durability_levels()

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += self.num_items

        # Initial validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        docs_mutated = self.num_items / 2
        # TODO: Add support for target vb testing
        # load_spec["target_vbuckets"] = []
        load_spec["subdoc_crud"] = dict()
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 100
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 0
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 0

        self.log.info("Perform initial 'insert' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                load_spec,
                mutation_num=1,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud()

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")

        self.validate_test_failure()

        # Update verification_dict and validate
        verification_dict["ops_update"] += self.num_items
        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.log.info("Perform 'insert', 'upsert', 'remove' mutations")
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 0
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 30
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 30

        self.log.info("Perform 'upsert' & 'remove' mutations")
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                load_spec,
                mutation_num=2,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud()

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")

        self.validate_test_failure()

        verification_dict["ops_update"] += \
            (sub_doc_gen.end - sub_doc_gen.start
             + len(task.fail.keys()))
        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += \
                num_item_start_for_crud

        # Edit doc_gen template to read the mutated value as well
        sub_doc_gen.template = \
            sub_doc_gen.template.replace(" }}", ", \"mutated\": \"\" }}")
        # Read all the values to validate update operation
        task = self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, sub_doc_gen, "read", 0,
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

        verification_dict["ops_update"] += \
            (sub_doc_gen.end - sub_doc_gen.start
             + len(task.fail.keys()))
        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += \
                num_item_start_for_crud

        # Edit doc_gen template to read the mutated value as well
        sub_doc_gen.template = sub_doc_gen.template \
            .replace(" }}", ", \"mutated\": \"\" }}")
        # Read all the values to validate update operation
        task = self.task.async_load_gen_sub_docs(
            self.cluster, self.bucket, sub_doc_gen, "read", 0,
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

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        # Validate verification_dict and validate
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify doc count and other stats
        self.bucket_util.validate_docs_per_collections_all_buckets()

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
        doc_ops = self.input.param("op_type", "create")

        # Reset initial doc_loading params to NO_OPS
        doc_load_template = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        doc_load_template[MetaCrudParams.DURABILITY_LEVEL] = ""
        doc_load_template[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 3
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0

        # Create new docs for sub-doc operations to run
        self.__load_data_for_sub_doc_ops()

        # Create required doc_generators for CRUD ops
        doc_load_template["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 30
        if "create" in doc_ops:
            doc_load_template["subdoc_crud"][
                MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 50
        elif "update" in doc_ops:
            doc_load_template["subdoc_crud"][
                MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 25
        elif "delete" in doc_ops:
            doc_load_template["subdoc_crud"][
                MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 25

        async_write_crud_spec = deepcopy(doc_load_template)
        sync_write_crud_spec = deepcopy(doc_load_template)

        sync_write_crud_spec[MetaCrudParams.DURABILITY_LEVEL] = \
            self.durability_level

        async_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                async_write_crud_spec,
                mutation_num=1,
                async_load=True)
        sync_write_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
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
        def_bucket = self.bucket_util.buckets[0]

        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["subdoc_crud"] = dict()
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 10
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 50
        load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 25
        load_spec["doc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 25

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.__getTargetNodes()

        # Create new docs for sub-doc operations to run
        self.__load_data_for_sub_doc_ops()

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
                self.bucket_util.buckets,
                load_spec,
                mutation_num=0,
                async_load=True)

        # Perform new scope/collection creation during doc ops in parallel
        self.__perform_collection_crud()

        # Wait for doc_loading to complete and validate the doc ops
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed with persistence issue")
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
            val = \
                failover_info["init"][node.ip] \
                != failover_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="Failover stats got updated")

            # Seq_no validation (High level)
            val = \
                vb_info_info["init"][node.ip] \
                != vb_info_info["afterCrud"][node.ip]
            self.assertTrue(val, msg="vbucket seq_no not updated after CRUDs")

        self.validate_test_failure()
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
        def_bucket = self.bucket_util.buckets[0]

        self.__load_data_for_sub_doc_ops()

        self.log.info("Selecting nodes to simulate error condition")
        target_nodes = self.__getTargetNodes()

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

        # TODO: Add support for target vb testing
        # load_spec["target_vbuckets"] = list(set(target_vbuckets)
        #                                ^ set(active_vbs_in_target_nodes))
        load_spec = dict()
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
                self.bucket_util.buckets,
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
        self.__perform_collection_crud()

        # Wait for document_loader tasks to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Sub_doc CRUDs failed with process crash")

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
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(reader_task)

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

        # Doc count validation
        self.validate_test_failure()
        self.bucket_util.validate_docs_per_collections_all_buckets()
