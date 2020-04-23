import json

from bucket_collections.collections_base import CollectionBase
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException


class CollectionDurabilityTests(CollectionBase):
    def setUp(self):
        super(CollectionDurabilityTests, self).setUp()
        self.bucket = self.bucket_util.buckets[0]
        self.with_non_sync_writes = self.input.param("with_non_sync_writes",
                                                     False)
        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

    def tearDown(self):
        super(CollectionDurabilityTests, self).tearDown()

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

    def test_crud_failures(self):
        """
        Test to configure the cluster in such a way durability will always fail

        1. Try creating the docs with durability set
        2. Verify create failed with durability_not_possible exception
        3. Create docs using async_writes
        4. Perform update and delete ops with durability
        5. Make sure these ops also fail with durability_not_possible exception
        """

        vb_info = dict()
        shell_conn = dict()
        cbstat_obj = dict()
        vb_info["init"] = dict()
        vb_info["failure_stat"] = dict()
        vb_info["create_stat"] = dict()
        nodes_in_cluster = self.cluster_util.get_kv_nodes()
        sub_doc_test = self.input.param("sub_doc_test", False)

        doc_load_spec = dict()
        doc_load_spec["doc_crud"] = dict()
        doc_load_spec["subdoc_crud"] = dict()
        doc_load_spec[MetaCrudParams.IGNORE_EXCEPTIONS] = [
            SDKException.DurabilityImpossibleException]
        doc_load_spec[MetaCrudParams.SKIP_READ_ON_ERROR] = True

        if not sub_doc_test:
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 10
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 10
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 10
        else:
            self.__load_data_for_sub_doc_ops()
            doc_load_spec["subdoc_crud"][
                MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 10
            doc_load_spec["subdoc_crud"][
                MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 10
            doc_load_spec["subdoc_crud"][
                MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 10

        err_msg = "Mutation succeeded with cluster size: %s, replica: %s" \
                  % (len(self.cluster.nodes_in_cluster),
                     self.num_replicas)
        num_items_before_d_load = \
            self.bucket_util.get_expected_total_num_items(self.bucket)

        for node in nodes_in_cluster:
            shell_conn[node.ip] = \
                RemoteMachineShellConnection(self.cluster.master)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])

            # Fetch vbucket seq_no stats from vb_seqno command for verification
            vb_info["init"].update(cbstat_obj[node.ip]
                                   .vbucket_seqno(self.bucket.name))

        # MB-34064 - Try same CREATE twice to validate doc cleanup in server
        for _ in range(2):
            collection_crud_task = \
                self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    self.bucket_util.buckets,
                    doc_load_spec)
            if collection_crud_task.result is True:
                self.log_failure(err_msg)

            # Fetch vbucket seq_no status from cbstats after CREATE task
            for node in nodes_in_cluster:
                vb_info["failure_stat"].update(
                    cbstat_obj[node.ip].vbucket_seqno(self.bucket.name))

            # Verify initial doc load count
            self.bucket_util.validate_docs_per_collections_all_buckets()
            curr_num_items = \
                self.bucket_util.get_expected_total_num_items(self.bucket)
            if curr_num_items != num_items_before_d_load:
                self.log_failure("Few mutation went in."
                                 "Docs expected: %s, actual: %s"
                                 % (num_items_before_d_load, curr_num_items))

            if vb_info["init"] != vb_info["failure_stat"]:
                self.log_failure(
                    "Failure stats mismatch. {0} != {1}"
                    .format(vb_info["init"], vb_info["failure_stat"]))

        # Perform aync_write to create the documents
        doc_load_spec[MetaCrudParams.DURABILITY_LEVEL] = ""
        collection_crud_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_load_spec)
        if collection_crud_task.result is False:
            self.log_failure("Mutation failed with async_writes")

        # Verify initial doc load count
        self.bucket_util.validate_docs_per_collections_all_buckets()
        curr_num_items = \
            self.bucket_util.get_expected_total_num_items(self.bucket)
        if curr_num_items == num_items_before_d_load:
            self.log_failure("Mutation failed with async_writes")

        # Fetch vbucket seq_no status from vb_seqno command after async creates
        for node in nodes_in_cluster:
            vb_info["create_stat"].update(cbstat_obj[node.ip]
                                          .vbucket_seqno(self.bucket.name))

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
        # Close all ssh sessions
        for node in nodes_in_cluster:
            shell_conn[node.ip].disconnect()
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
        load_task = dict()

        self.log.info("Loading docs such that all sync_writes will be aborted")
        kv_nodes = self.cluster_util.get_kv_nodes()
        for server in kv_nodes:
            ssh_shell = RemoteMachineShellConnection(server)
            cbstats = Cbstats(ssh_shell)
            cb_err = CouchbaseError(self.log, ssh_shell)
            replica_vbs[server] = cbstats.vbucket_list(self.bucket.name,
                                                       "replica")
            doc_load_spec = dict()
            doc_load_spec["doc_crud"] = dict()
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 5
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 5
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 5
            doc_load_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                self.durability_level
            doc_load_spec[MetaCrudParams.IGNORE_EXCEPTIONS] = \
                SDKException.DurabilityAmbiguousException
            doc_load_spec[MetaCrudParams.SDK_TIMEOUT] = 2
            doc_load_spec[MetaCrudParams.SKIP_READ_ON_ERROR] = True

            cb_err.create(CouchbaseError.STOP_MEMCACHED)
            load_task[server] = \
                self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    self.bucket_util.buckets,
                    doc_load_spec,
                    batch_size=1,
                    validate_task=False)
            cb_err.revert(CouchbaseError.STOP_MEMCACHED)
            ssh_shell.disconnect()
        self.validate_test_failure()

        # Validate vbucket stats
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        if self.durability_level:
            verification_dict["sync_write_aborted_count"] = \
                crud_batch_size * len(kv_nodes)
            verification_dict["sync_write_committed_count"] = self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket, kv_nodes,
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details verification failed "
                             "after aborts")
        self.validate_test_failure()

        # Retry aborted keys with healthy cluster
        self.log.info("Performing CRUDs on healthy cluster")
        for server in kv_nodes:
            self.bucket_util.validate_doc_loading_results(
                load_task[server])
            if load_task[server].result is False:
                self.log_failure("Doc retry task failed on %s" % server.ip)

            # TODO: Update verification dict for validation
            failed = self.durability_helper.verify_vbucket_details_stats(
                self.bucket, self.cluster_util.get_kv_nodes(),
                vbuckets=self.cluster_util.vbuckets,
                expected_val=verification_dict)
            if failed:
                self.log_failure("Cbstat vbucket-details verification "
                                 "failed after ops on server: %s" % server.ip)
        self.validate_test_failure()

    def test_sync_write_in_progress(self):
        doc_ops = self.input.param("doc_ops", "create;create").split(';')
        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()

        replica_vbs = dict()

        # Acquire SDK client from the pool for performing doc_ops locally
        client = self.sdk_client_pool.get_client_for_bucket(self.bucket)

        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)
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

        doc_load_spec = dict()
        doc_load_spec["doc_crud"] = dict()
        # TODO: Target VB support for doc_loading task
        # doc_load_spec[MetaCrudParams.TARGET_VB] = target_vbuckets
        doc_load_spec[MetaCrudParams.DURABILITY_LEVEL] = self.durability_level
        doc_load_spec[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 5
        doc_load_spec[MetaCrudParams.SDK_TIMEOUT] = 60

        if doc_ops[0] == "create":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "update":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "replace":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "delete":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 1

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_load_spec,
                async_load=True)

        self.sleep(5, "Wait for doc ops to reach server")

        for bucket, s_dict in doc_loading_task.loader_spec.items():
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, c_meta in c_dict["collections"].items():
                    for op_type in c_meta:
                        key, value = c_meta[op_type]["doc_gen"].next()
                        for fail_fast in [True, False]:
                            if self.with_non_sync_writes:
                                fail = client.crud(
                                    doc_ops[1], key, value,
                                    exp=0, timeout=2, time_unit="seconds",
                                    fail_fast=fail_fast)
                            else:
                                fail = client.crud(
                                    doc_ops[1], key, value,
                                    exp=0,
                                    durability=self.durability_level,
                                    timeout=2, time_unit="seconds",
                                    fail_fast=fail_fast)

                            expected_exception = \
                                SDKException.AmbiguousTimeoutException
                            retry_reason = \
                                SDKException \
                                .RetryReason \
                                .KV_SYNC_WRITE_IN_PROGRESS
                            if fail_fast:
                                expected_exception = \
                                    SDKException.RequestCanceledException
                                retry_reason = \
                                    SDKException \
                                    .RetryReason \
                                    .KV_SYNC_WRITE_IN_PROGRESS_NO_MORE_RETRIES
                            if doc_ops[0] == "create" \
                                    and doc_ops[1] in ["delete",
                                                            "replace"]:
                                expected_exception = \
                                    SDKException.DocumentNotFoundException
                                retry_reason = None

                            # Validate the returned error from the SDK
                            if expected_exception not in str(fail["error"]):
                                self.log_failure("Invalid exception for %s: %s"
                                                 % (key, fail["error"]))
                            if retry_reason \
                                    and retry_reason not in str(fail["error"]):
                                self.log_failure(
                                    "Invalid retry reason for %s: %s"
                                    % (key, fail["error"]))

                            # Try reading the value in SyncWrite state
                            fail = client.crud("read", key)
                            if doc_ops[0] == "create":
                                # Expected KeyNotFound in case of CREATE op
                                if fail["status"] is True:
                                    self.log_failure(
                                        "%s returned value during SyncWrite %s"
                                        % (key, fail))
                            else:
                                # Expects prev val in case of other operations
                                if fail["status"] is False:
                                    self.log_failure(
                                        "Key %s read failed for prev value: %s"
                                        % (key, fail))

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")

        # Release the acquired SDK client
        self.sdk_client_pool.release_client(client)

    def test_bulk_sync_write_in_progress(self):
        doc_ops = self.input.param("doc_ops").split(';')
        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()

        replica_vbs = dict()

        # Acquire SDK client from the pool for performing doc_ops locally
        client = self.sdk_client_pool.get_client_for_bucket(self.bucket)

        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)
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

        doc_load_spec = dict()
        doc_load_spec["doc_crud"] = dict()
        # TODO: Target VB support for doc_loading task
        # doc_load_spec[MetaCrudParams.TARGET_VB] = target_vbuckets
        doc_load_spec[MetaCrudParams.DURABILITY_LEVEL] = self.durability_level
        doc_load_spec[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 5
        doc_load_spec[MetaCrudParams.SDK_TIMEOUT] = 60

        if doc_ops[0] == "create":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "update":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "replace":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "delete":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 1

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_load_spec,
                async_load=True)

        self.sleep(5, "Wait for doc ops to reach server")

        tem_durability = self.durability_level
        if self.with_non_sync_writes:
            tem_durability = "NONE"

        for bucket, s_dict in doc_loading_task.loader_spec.items():
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, c_meta in c_dict["collections"].items():
                    for op_type in c_meta:
                        # This will support both sync-write and non-sync-writes
                        doc_loader_task_2 = self.task.async_load_gen_docs(
                            self.cluster, self.bucket,
                            c_meta[op_type]["doc_gen"], doc_ops[1], 0,
                            scope=s_name, collection=c_name,
                            sdk_client_pool=self.sdk_client_pool,
                            batch_size=self.crud_batch_size,
                            process_concurrency=1,
                            replicate_to=self.replicate_to,
                            persist_to=self.persist_to,
                            durability=tem_durability, timeout_secs=3,
                            print_ops_rate=False,
                            skip_read_on_error=True,
                            task_identifier="parallel_task2")
                        self.task.jython_task_manager.get_task_result(
                            doc_loader_task_2)

                        # Validation to verify the sync_in_write_errors
                        # in doc_loader_task_2
                        failed_docs = doc_loader_task_2.fail
                        if len(failed_docs.keys()) != 1:
                            self.log_failure("Exception not seen for docs: {0}"
                                             .format(failed_docs))

                        valid_exception = \
                            self.durability_helper.validate_durability_exception(
                                failed_docs,
                                SDKException.AmbiguousTimeoutException,
                                retry_reason=SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS)

                        if not valid_exception:
                            self.log_failure("Got invalid exception")

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loading to complete
        self.task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")

        # Validate docs for update success or not
        if doc_ops[0] == "update":
            for bucket, s_dict in doc_loading_task.loader_spec.items():
                for s_name, c_dict in s_dict["scopes"].items():
                    for c_name, c_meta in c_dict["collections"].items():
                        for op_type in c_meta:
                            read_task = self.task.async_load_gen_docs(
                                self.cluster, self.bucket,
                                c_meta[op_type]["doc_gen"], "read",
                                batch_size=self.crud_batch_size,
                                process_concurrency=1,
                                timeout_secs=self.sdk_timeout)
                            self.task_manager.get_task_result(read_task)
                            for key, doc_info in read_task.success.items():
                                if doc_info["cas"] != 0 \
                                        and json.loads(str(doc_info["value"]))["mutated"] != 1:
                                    self.log_failure(
                                        "Update failed for key %s: %s"
                                        % (key, doc_info))

        # Release the acquired SDK client
        self.sdk_client_pool.release_client(client)

        # Validate doc_count per collection
        self.validate_test_failure()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_sub_doc_sync_write_in_progress(self):
        """
        Test to simulate sync_write_in_progress error and validate the behavior
        This will validate failure in majority of nodes, where durability will
        surely fail for all CRUDs

        1. Select nodes to simulate the error which will affect the durability
        2. Enable the specified error_scenario on the selected nodes
        3. Perform individual CRUDs and verify sync_write_in_progress errors
        4. Validate the end results
        """

        doc_ops = self.input.param("doc_ops", "insert;insert").split(";")

        shell_conn = dict()
        cbstat_obj = dict()
        error_sim = dict()
        vb_info = dict()
        replica_vbs = dict()
        vb_info["init"] = dict()
        doc_load_spec = dict()

        amb_timeout_exception = SDKException.AmbiguousTimeoutException
        kv_sync_write_in_progress = \
            SDKException.RetryReason.KV_SYNC_WRITE_IN_PROGRESS
        doc_not_found_exception = SDKException.DocumentNotFoundException

        # Override the crud_batch_size
        self.crud_batch_size = 5
        expected_failed_doc_num = self.crud_batch_size

        self.__load_data_for_sub_doc_ops()

        # Acquire SDK client from the pool for performing doc_ops locally
        client = self.sdk_client_pool.get_client_for_bucket(self.bucket)

        # Select nodes to affect and open required shell_connections
        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)
        doc_load_spec["doc_crud"] = dict()
        doc_load_spec["subdoc_crud"] = dict()
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

        # Update mutation spec based on the required doc_operation
        if doc_ops[0] == "create":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] in "update":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "delete":
            doc_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 1
        elif doc_ops[0] == "insert":
            doc_load_spec["subdoc_crud"][
                MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 1
        elif doc_ops[0] == "upsert":
            doc_load_spec["subdoc_crud"][
                MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 1
        elif doc_ops[0] == "remove":
            doc_load_spec["subdoc_crud"][
                MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 1

        # This is to support both sync-write and non-sync-writes
        tem_durability = self.durability_level
        if self.with_non_sync_writes:
            tem_durability = "NONE"

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
        self.sleep(5, "Wait for error simulation to take effect")

        # Initialize tasks and store the task objects
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_load_spec,
                mutation_num=2,
                batch_size=1,
                async_load=True)

        # Start the doc_loader_task
        self.sleep(10, "Wait for task_1 CRUDs to reach server")

        for bucket, s_dict in doc_loading_task.loader_spec.items():
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, c_meta in c_dict["collections"].items():
                    for op_type in c_meta:
                        while c_meta[op_type]["doc_gen"].has_next():
                            key, val = c_meta[op_type]["doc_gen"].next()
                            expected_exception = amb_timeout_exception
                            retry_reason = kv_sync_write_in_progress
                            if doc_ops[0] in "create":
                                expected_exception = doc_not_found_exception
                                retry_reason = None

                            result = client.crud(
                                op_type, key, val,
                                durability=tem_durability,
                                timeout=3)

                            if result["status"] is True:
                                self.log_failure("Doc crud succeeded for %s"
                                                 % op_type)
                            elif expected_exception not in result["error"]:
                                self.log_failure(
                                    "Invalid exception for key %s: %s"
                                    % (key, result["error"]))
                            elif retry_reason is not None \
                                    and retry_reason not in result["error"]:
                                self.log_failure(
                                    "Retry reason missing for key %s: %s"
                                    % (key, result["error"]))

        # Revert the introduced error condition
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Wait for doc_loader_task_1 to complete
        self.task.jython_task_manager.get_task_result(doc_loading_task)
        self.bucket_util.validate_doc_loading_results(doc_loading_task)
        if doc_loading_task.result is False:
            self.log_failure("Doc CRUDs failed")

        # Validate docs for update success or not
        if doc_ops[0] == "update":
            for bucket, s_dict in doc_loading_task.loader_spec.items():
                for s_name, c_dict in s_dict["scopes"].items():
                    for c_name, c_meta in c_dict["collections"].items():
                        for op_type in c_meta:
                            c_meta[op_type]["doc_gen"].reset()
                            read_task = self.task.async_load_gen_docs(
                                self.cluster, self.bucket,
                                c_meta[op_type]["doc_gen"], "read",
                                batch_size=self.crud_batch_size,
                                process_concurrency=1,
                                timeout_secs=self.sdk_timeout)
                            self.task_manager.get_task_result(read_task)
                            for key, doc_info in read_task.success.items():
                                if doc_info["cas"] != 0 and \
                                        json.loads(str(doc_info["value"]))["mutated"] != 2:
                                    self.log_failure(
                                        "Update failed for key %s: %s"
                                        % (key, doc_info))

        # Release the acquired SDK client
        self.sdk_client_pool.release_client(client)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.validate_test_failure()
