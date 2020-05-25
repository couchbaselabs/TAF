from copy import deepcopy
from random import sample

import time

from Cb_constants import CbServer
from Cb_constants.DocLoading import Bucket
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator, sub_doc_generator_for_edit
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView


class SDKExceptionTests(CollectionBase):
    def setUp(self):
        super(SDKExceptionTests, self).setUp()
        self.bucket = self.bucket_util.buckets[0]
        self.subdoc_test = self.input.param("subdoc_test", False)
        self.log_setup_status("TimeoutTests", "complete")

        if self.subdoc_test:
            self.load_data_for_sub_doc_ops()

        self.log_setup_status("TimeoutTests", "complete")

    def tearDown(self):
        super(SDKExceptionTests, self).tearDown()

    @staticmethod
    def __get_random_doc_ttl_and_durability_level():
        # Max doc_ttl value=2147483648. Reference:
        # docs.couchbase.com/server/6.5/learn/buckets-memory-and-storage/expiration.html
        doc_ttl = sample([0, 30000, 2147483648], 1)[0]
        durability_level = sample(
            BucketUtils.get_supported_durability_levels() + [""], 1)[0]
        return doc_ttl, durability_level

    def create_scope_collection(self, create_scope=True):
        if create_scope and self.scope_name != CbServer.default_scope:
            self.bucket_util.create_scope(self.cluster.master, self.bucket,
                                          {"name", self.scope_name})
        if self.collection_name != CbServer.default_collection:
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               self.scope_name,
                                               {"name": self.collection_name})

    def test_collection_not_exists(self):
        """
        1. Load docs into required collection
        2. Validate docs based on the targeted collection
        3. Create non-default scope/collection for CRUDs to happen
        4. Perform doc_ops again and perform CRUDs
        5. Drop the target collection and validate the CollectionNotExists
           exception from client side
        6. Recreate non-default collection and re-create the docs and validate
        """
        def validate_vb_detail_stats():
            failed = durability_helper.verify_vbucket_details_stats(
                self.bucket, self.cluster_util.get_kv_nodes(),
                vbuckets=self.cluster_util.vbuckets,
                expected_val=verification_dict)
            if failed:
                self.log_failure("vBucket_details validation failed")
            self.bucket_util.validate_docs_per_collections_all_buckets()

        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        durability_helper = DurabilityHelper(self.log,
                                             len(self.cluster.kv_nodes),
                                             durability=self.durability_level)

        drop_scope = self.input.param("drop_scope", False)

        # Doc generator used for mutations
        doc_gen = doc_generator("test_col_not_exists", 0, 10)

        # Acquire SDK client for mutations
        client = self.sdk_client_pool.get_client_for_bucket(
            self.bucket,
            self.scope_name,
            self.collection_name)

        doc_ttl, _ = \
            SDKExceptionTests.__get_random_doc_ttl_and_durability_level()
        self.log.info("Creating docs with doc_ttl %s into %s:%s:%s"
                      % (doc_ttl,
                         self.bucket.name,
                         self.scope_name,
                         self.collection_name))

        while doc_gen.has_next():
            key, value = doc_gen.next()
            result = client.crud("create", key, value,
                                 exp=doc_ttl,
                                 durability=self.durability_level)
            if self.collection_name == CbServer.default_collection:
                if result["status"] is False:
                    self.log_failure("Create doc failed for key: %s" % key)
                else:
                    verification_dict["ops_create"] += 1
                    if self.durability_level:
                        verification_dict["sync_write_committed_count"] += 1
                    self.bucket.scopes[
                        self.scope_name].collections[
                        self.collection_name].num_items += 1

            elif result["status"] is True:
                self.log_failure("Create didn't fail as expected for key: %s"
                                 % key)
            elif SDKException.CollectionNotFoundException \
                    not in str(result["error"]):
                self.log_failure("Invalid exception for key %s: %s"
                                 % (key, result["error"]))

        validate_vb_detail_stats()

        # Create required scope/collection for successful CRUD operation
        if self.scope_name != CbServer.default_scope:
            self.scope_name = self.bucket_util.get_random_name()
        if self.collection_name != CbServer.default_collection:
            self.collection_name = self.bucket_util.get_random_name()
        self.create_scope_collection()

        # Reset doc_gen itr value for retry purpose
        doc_gen.reset()
        doc_ttl, _ = \
            SDKExceptionTests.__get_random_doc_ttl_and_durability_level()
        self.log.info("Creating docs with doc_ttl %s into %s:%s:%s"
                      % (doc_ttl,
                         self.bucket.name,
                         self.scope_name,
                         self.collection_name))
        op_type = "create"
        if self.collection_name == CbServer.default_collection:
            op_type = "update"

        while doc_gen.has_next():
            key, value = doc_gen.next()
            result = client.crud(op_type, key, value,
                                 exp=doc_ttl,
                                 durability=self.durability_level)
            if result["status"] is False:
                self.log_failure("Create fail for key %s: %s"
                                 % (key, result))
            else:
                if op_type == "create":
                    verification_dict["ops_create"] += 1
                    self.bucket.scopes[
                        self.scope_name].collections[
                        self.collection_name].num_items += 1
                else:
                    verification_dict["ops_update"] += 1

                if self.durability_level:
                    verification_dict["sync_write_committed_count"] += 1
        validate_vb_detail_stats()

        if drop_scope:
            self.log.info("Dropping scope %s" % self.scope_name)
            self.bucket_util.drop_scope(self.cluster.master,
                                        self.bucket,
                                        self.scope_name)
        else:
            self.log.info("Dropping collection %s:%s" % (self.scope_name,
                                                         self.collection_name))
            self.bucket_util.drop_collection(self.cluster.master,
                                             self.bucket,
                                             self.scope_name,
                                             self.collection_name)
        validate_vb_detail_stats()

        # Reset doc_gen itr value for retry purpose
        doc_gen.reset()
        while doc_gen.has_next():
            key, value = doc_gen.next()
            result = client.crud("create", key, value,
                                 exp=doc_ttl,
                                 durability=self.durability_level)
            if result["status"] is True:
                self.log_failure("Create doc succeeded for dropped collection")
        validate_vb_detail_stats()

        self.create_scope_collection(create_scope=drop_scope)
        if self.collection_name != CbServer.default_collection:
            doc_gen.reset()
            while doc_gen.has_next():
                key, value = doc_gen.next()
                result = client.crud("create", key, value,
                                     exp=doc_ttl,
                                     durability=self.durability_level)
                if result["status"] is False:
                    self.log_failure("Create failed after collection recreate "
                                     "for key %s: %s" % (key, result["error"]))
                else:
                    verification_dict["ops_create"] += 1
                    if self.durability_level:
                        verification_dict["sync_write_committed_count"] += 1
                    self.bucket.scopes[
                        self.scope_name].collections[
                        self.collection_name].num_items += 1
            validate_vb_detail_stats()

        # Release the acquired client
        self.sdk_client_pool.release_client(client)
        self.validate_test_failure()

    def test_collections_not_available(self):
        """
        Perform different collection dependent operations
        and validate we get CollectionNotAvailable exception for all the ops

        1. Perform scope create/delete from SDK
        2. Perform collection create/delete from SDK
        3. Perform crud to target collection and validate
        """
        # Acquire SDK client for mutations
        client = self.sdk_client_pool.get_client_for_bucket(self.bucket)

        scope_name = self.bucket_util.get_random_name()
        col_name = self.bucket_util.get_random_name()

        try:
            client.create_scope(scope_name)
            self.log_failure("Create scope succeeded")
        except Exception as e:
            self.log.info("Create scope failed with exception: %s" % e)
            if SDKException.CollectionsNotAvailableException not in str(e):
                self.log_failure("Invalid expection during create scope")

        try:
            client.create_collection(col_name, scope=CbServer.default_scope)
            self.log_failure("Create collection succeeded")
        except Exception as e:
            self.log.info("Create scope failed with exception: %s" % e)
            if SDKException.CollectionsNotAvailableException not in str(e):
                self.log_failure("Invalid expection during create collection")

        try:
            client.create_collection(col_name, scope=scope_name)
            self.log_failure("Create collection succeeded")
        except Exception as e:
            self.log.info("Create scope failed with exception: %s" % e)
            if SDKException.CollectionsNotAvailableException not in str(e):
                self.log_failure("Invalid expection during create collection")

        client.select_collection(scope_name, col_name)
        result = client.crud("create", "key", "value")
        if result["status"] is True:
            self.log_failure("Collection create successful")
        elif SDKException.CollectionsNotAvailableException \
                not in str(result["error"]):
            self.log_failure("Invalid expection during doc create")

        # Release the acquired client
        self.sdk_client_pool.release_client(client)
        self.validate_test_failure()

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
        vb_info = dict()
        vb_info["init"] = dict()
        vb_info["afterCrud"] = dict()

        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

        doc_load_spec = dict()
        doc_load_spec["doc_crud"] = dict()
        doc_load_spec["subdoc_crud"] = dict()
        doc_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 0

        doc_load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 0
        doc_load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 0
        doc_load_spec["subdoc_crud"][
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 0

        for op_type in ["create", "update", "read",
                        "insert", "upsert", "remove",
                        "delete"]:
            self.log.info("Performing '%s' with timeout=%s"
                          % (op_type, self.sdk_timeout))
            curr_spec = deepcopy(doc_load_spec)
            if op_type == "create":
                doc_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] \
                    = 5
            elif op_type == "update":
                doc_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] \
                    = 5
            elif op_type == "delete":
                doc_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] \
                    = 5
            elif op_type == "read":
                doc_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 5
                doc_load_spec[MetaCrudParams.RETRY_EXCEPTIONS] = [
                    SDKException.TimeoutException]
            elif op_type == "insert":
                doc_load_spec["subdoc_crud"][
                    MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 5
            elif op_type == "upsert":
                doc_load_spec["subdoc_crud"][
                    MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION] = 5
            elif op_type == "remove":
                doc_load_spec["subdoc_crud"][
                    MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION] = 5

            doc_loading_task = \
                self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    self.bucket_util.buckets,
                    curr_spec,
                    mutation_num=1,
                    async_load=True,
                    validate_task=False)

            # Perform specified action
            for node in target_nodes:
                error_sim[node.ip].create(self.simulate_error,
                                          bucket_name=self.bucket.name)

            self.sleep(10, "Wait before reverting the error condition")

            # Revert the specified error scenario
            for node in target_nodes:
                error_sim[node.ip].revert(self.simulate_error,
                                          bucket_name=self.bucket.name)

            self.task_manager.get_task_result(doc_loading_task)
            self.bucket_util.validate_doc_loading_results(doc_loading_task)
            if doc_loading_task.result is False:
                self.fail("Initial doc_loading failed")

            # Fetch latest stats and validate the values are updated
            for node in target_nodes:
                vb_info["afterCrud"][node.ip] = \
                    cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
                if vb_info["init"][node.ip] == vb_info["afterCrud"][node.ip]:
                    self.log_failure("vbucket_seqno not updated. {0} == {1}"
                                     .format(vb_info["init"][node.ip],
                                             vb_info["afterCrud"][node.ip]))

        # Disconnect the shell connection
        for node in target_nodes:
            shell_conn[node.ip].disconnect()

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
            for tem_vb_num in range(self.cluster_util.vbuckets):
                tem_vb_num = str(tem_vb_num)
                if tem_vb_num not in affected_vbs:
                    if tem_vb_num in vb_info["init"][node.ip].keys() \
                            and vb_info["init"][node.ip][tem_vb_num] \
                            != vb_info["post_timeout"][node.ip][tem_vb_num]:
                        self.log_failure(
                            "Unaffected vb-%s stat updated: %s != %s"
                            % (tem_vb_num,
                               vb_info["init"][node.ip][tem_vb_num],
                               vb_info["post_timeout"][node.ip][tem_vb_num]))
                elif int(tem_vb_num) in target_nodes_vbuckets["active"]:
                    if tem_vb_num in vb_info["init"][node.ip].keys() \
                            and vb_info["init"][node.ip][tem_vb_num] \
                            != vb_info["post_timeout"][node.ip][tem_vb_num]:
                        self.log.warning(
                            err_msg
                            % (node.ip,
                               "active",
                               tem_vb_num,
                               vb_info["init"][node.ip][tem_vb_num],
                               vb_info["post_timeout"][node.ip][tem_vb_num]))
                elif int(tem_vb_num) in target_nodes_vbuckets["replica"]:
                    if tem_vb_num in vb_info["init"][node.ip].keys() \
                            and vb_info["init"][node.ip][tem_vb_num] \
                            == vb_info["post_timeout"][node.ip][tem_vb_num]:
                        retry_validation = True
                        self.log.warning(
                            err_msg
                            % (node.ip,
                               "replica",
                               tem_vb_num,
                               vb_info["init"][node.ip][tem_vb_num],
                               vb_info["post_timeout"][node.ip][tem_vb_num]))
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

        # Create required scope/collection for successful CRUD operation
        if self.scope_name != CbServer.default_scope:
            self.scope_name = self.bucket_util.get_random_name()
        if self.collection_name != CbServer.default_collection:
            self.collection_name = self.bucket_util.get_random_name()
        self.create_scope_collection()

        # Create required doc_generators
        doc_gen["create"] = doc_generator(self.key, self.num_items,
                                          self.num_items+self.crud_batch_size)
        doc_gen["delete"] = doc_generator(self.key, 0,
                                          self.crud_batch_size)
        doc_gen["read"] = doc_generator(
            self.key, int(self.num_items/3),
            int(self.num_items/3) + self.crud_batch_size)
        doc_gen["update"] = doc_generator(
            self.key, int(self.num_items/2),
            int(self.num_items/2) + self.crud_batch_size)

        # Create required subdoc generators
        doc_gen["insert"] = sub_doc_generator(
            self.key, int(self.num_items/2),
            int(self.num_items/2) + self.crud_batch_size)
        doc_gen["upsert"] = sub_doc_generator_for_edit(
            self.key, int(self.num_items/2),
            int(self.num_items/2) + self.crud_batch_size,
            template_index=1)
        doc_gen["remove"] = sub_doc_generator_for_edit(
            self.key, int(self.num_items/2),
            int(self.num_items/2) + self.crud_batch_size,
            template_index=1)

        target_nodes = DurabilityHelper.getTargetNodes(self.cluster,
                                                       self.nodes_init,
                                                       self.num_nodes_affected)
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

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)
        self.sleep(10, "Wait for error_simulation to take effect")

        self.log.info("Starting doc ops")
        for op_type in doc_gen.keys():
            if doc_gen in Bucket.DOC_OPS:
                tasks[op_type] = self.task.async_load_gen_docs(
                    self.cluster, self.bucket, doc_gen[op_type], op_type, 0,
                    scope=self.scope_name,
                    collection=self.collection_name,
                    sdk_client_pool=self.sdk_client_pool,
                    batch_size=1, process_concurrency=8,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    print_ops_rate=False,
                    skip_read_on_error=True)
            else:
                tasks[op_type] = self.task.async_load_gen_sub_docs(
                    self.cluster, self.bucket, doc_gen[op_type], op_type, 0,
                    scope=self.scope_name,
                    collection=self.collection_name,
                    batch_size=1, process_concurrency=8,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    print_ops_rate=False)

        # Start the doc_ops tasks
        for op_type in doc_gen.keys():
            self.task_manager.add_new_task(tasks[op_type])

        # Wait for document_loader tasks to complete
        for op_type in doc_gen.keys():
            self.task.jython_task_manager.get_task_result(tasks[op_type])

            # Validate task failures
            if op_type == "read":
                # Validation for read task
                if len(tasks[op_type].fail.keys()) != 0:
                    self.log_failure("Read failed for few docs: %s"
                                     % tasks[op_type].fail.keys())
            else:
                # Validation of CRUDs - Update / Create / Delete
                for doc_id, crud_result in tasks[op_type].fail.items():
                    vb_num = self.bucket_util.get_vbucket_num_for_key(
                        doc_id, self.cluster_util.vbuckets)
                    if SDKException.DurabilityAmbiguousException \
                            not in str(crud_result["error"]):
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
                        self.cluster_util.vbuckets)))

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

        # Get SDK Client from client_pool
        sdk_client = self.sdk_client_pool.get_client_for_bucket(
            self.bucket,
            self.scope_name,
            self.collection_name)

        # Doc error validation
        for op_type in doc_gen.keys():
            task = tasks[op_type]

            if self.nodes_init == 1 \
                    and op_type != "read" \
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

                ambiguous_table_view.add_row([doc_key, str(vb_for_key)])
                retry_success = \
                    self.durability_helper.retry_for_ambiguous_exception(
                        sdk_client, op_type, doc_key, doc_info)
                if not retry_success:
                    self.log_failure("%s failed in retry for %s"
                                     % (op_type, doc_key))

                if SDKException.DurabilityAmbiguousException \
                        not in str(doc_info["error"]):
                    table_view.add_row([doc_key, doc_info["error"]])

            # Display the tables (if any errors)
            table_view.display("Unexpected exception during %s" % op_type)
            ambiguous_table_view.display("Ambiguous exception during %s"
                                         % op_type)

        # Release the acquired client
        self.sdk_client_pool.release_client(sdk_client)

        # Verify doc count after expected CRUD failure
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
