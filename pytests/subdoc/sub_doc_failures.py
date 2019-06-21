import json

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


class SubDocFailures(DurabilityTestsBase):
    def setUp(self):
        super(SubDocFailures, self).setUp()
        self.key = 'test_sub_docs'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode,
            bucket_type=self.bucket_type)
        self.bucket_util.add_rbac_user()

        self.src_bucket = self.bucket_util.get_all_buckets()
        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)

        self.log.info("Creating doc_generator..")
        # Load basic docs into bucket
        doc_create = doc_generator(
            self.key, 0, self.num_items, doc_size=self.doc_size,
            target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, self.bucket_util.buckets[0]))
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket_util.buckets[0],
            doc_create, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)

        self.log.info("Validating doc_count")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.log.info("==========Finished BasisOps base setup========")

    def tearDown(self):
        super(SubDocFailures, self).tearDown()

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

        target_nodes = self.getTargetNodes()
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstat_obj[node.ip] = Cbstats(shell_conn[node.ip])
            vb_info["init"] = dict()
            vb_info["init"][node.ip] = cbstat_obj[node.ip].vbucket_seqno(
                self.bucket.name)
            error_sim[node.ip] = CouchbaseError(self.log, shell_conn[node.ip])

        curr_time = int(time.time())
        expected_timeout = curr_time + self.sdk_timeout
        time_to_wait = expected_timeout - 20

        # Perform specified action
        for node in target_nodes:
            error_sim[node.ip].create(self.simulate_error,
                                      bucket_name=self.bucket.name)

        # Perform CRUDs with induced error scenario is active
        tasks = list()
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+self.crud_batch_size)
        gen_delete = doc_generator(self.key, 0,
                                   int(self.num_items/3))
        gen_read = doc_generator(self.key, int(self.num_items/3),
                                 int(self.num_items/2))
        gen_update = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_read, "read", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_delete, "delete", 0,
            batch_size=10, process_concurrency=1,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries))

        # Update num_items value accordingly to the CRUD performed
        self.num_items += self.crud_batch_size - int(self.num_items/3)

        self.sleep(time_to_wait,
                   message="Wait less than the sdk_timeout value")

        # Fetch latest stats and validate the values are not changed
        for node in target_nodes:
            vb_info["withinTimeout"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            if vb_info["init"][node.ip] != vb_info["withinTimeout"][node.ip]:
                self.log_failure("Mismatch in vbucket_seqno. {0} != {1}"
                                 .format(vb_info["init"][node.ip],
                                         vb_info["withinTimeout"][node.ip]))

        # Revert the specified error scenario
        for node in target_nodes:
            error_sim[node.ip].revert(self.simulate_error,
                                      bucket_name=self.bucket.name)
            # Disconnect the shell connection
            shell_conn[node].disconnect()

        # Create SDK Client
        client = SDKClient(RestConnection(self.cluster.master),
                           self.bucket.name)

        # Wait for document_loader tasks to complete and retry failed docs
        op_type = None
        msg = "CRUD '{0}' failed after retry with no error condition"
        for index, task in enumerate(tasks):
            self.task.jython_task_manager.get_task_result(task)

            if index == 0:
                op_type = "create"
            elif index == 1:
                op_type = "update"
            elif index == 2:
                op_type = "read"
            elif index == 3:
                op_type = "delete"

            # Retry failed docs
            retry_failed = self.durability_helper.retry_with_no_error(
                client, task.fail, op_type)
            if retry_failed:
                self.log_failure(msg.format(op_type))

        # Close the SDK connection
        client.close()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Fetch latest stats and validate the values are updated
        for node in target_nodes:
            vb_info["afterCrud"][node.ip] = \
                cbstat_obj[node.ip].vbucket_seqno(self.bucket.name)
            if vb_info["init"][node.ip] != vb_info["afterCrud"][node.ip]:
                self.log_failure("Mismatch in vbucket_seqno. {0} != {1}"
                                 .format(vb_info["init"][node.ip],
                                         vb_info["withinTimeout"][node.ip]))
        self.validate_test_failure()
