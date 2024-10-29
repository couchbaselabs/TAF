from json import loads as json_loads
from random import randint

from math import floor

from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from error_simulation.cb_error import CouchbaseError
from constants.sdk_constants.java_client import SDKConstants
from sdk_exceptions import SDKException


class DurabilityHelper:
    EQUAL = '=='
    GREATER_THAN_EQ = '>='

    def __init__(self, logger, cluster_len,
                 durability=SDKConstants.DurabilityLevel.MAJORITY,
                 replicate_to=0, persist_to=0):
        """
        :param logger:       Logger object to log the errors/warnings
        :param cluster_len:  Length of the cluster used (int)
        :param durability:   Durability_level    Default="MAJORITY"
        :param replicate_to: Replicate_to value  Default=0
        :param persist_to:   Persist_to value    Default=0
        """
        # Logger object and cluster length
        self.log = logger
        self.cluster_len = cluster_len

        # Durability related values
        self.replicate_to = replicate_to
        self.persist_to = persist_to
        self.durability = durability

        # These are induced error_types with which durability=MAJORITY
        # should not be affected.
        self.disk_error_types = ["disk_failure", "disk_full"]

    @staticmethod
    def __compare(lhs_val, rhs_val, comparison):
        """
        :param lhs_val:
        :param rhs_val:
        :param comparison:
        :return: Bool denoting comparison result
        """
        if comparison == DurabilityHelper.EQUAL:
            return lhs_val == rhs_val
        elif comparison == DurabilityHelper.GREATER_THAN_EQ:
            return lhs_val >= rhs_val
        return False

    @staticmethod
    def getTargetNodes(cluster, nodes_init, num_nodes_affected):
        def select_random_node(nodes):
            rand_node_index = randint(1, nodes_init-1)
            if cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != num_nodes_affected:
                select_random_node(node_list)
        else:
            node_list.append(cluster.master)
        return node_list

    @staticmethod
    def is_sync_write_enabled(bucket_durability, doc_durability):
        if (bucket_durability != "none") \
                or (doc_durability != "" and doc_durability.lower() != "none"):
            return True
        return False

    def durability_succeeds(self, bucket_name, master,
                            induced_error=None, failed_nodes=[]):
        """
        Determines whether the durability will fail/work based on
        the type of error_induced during the test and number of nodes the
        error is induced on.

        :param bucket_name:   Name of the bucket used for fetching
                              the replica value (str)
        :param master:        Master node from the cluster
        :param induced_error: Error induced during the test execution (str)
        :param failed_nodes:  No of nodes failed due to the induced_error (int)

        :return durability_succeeds: Durability status for the bucket (bool)
        """
        durability_succeeds = True
        bucket = BucketHelper(master).get_buckets_json(bucket_name)
        if bucket["replicaNumber"] > 2:
            # Durability is not supported with replica > 2 (MB-34453)
            return False

        min_nodes_req = bucket["replicaNumber"] + 1
        majority_value = floor(min_nodes_req/2) + 1

        if induced_error is None:
            if (self.cluster_len-failed_nodes) < majority_value:
                durability_succeeds = False
        else:
            if (self.durability == SDKConstants.DurabilityLevel.MAJORITY
                    and induced_error in self.disk_error_types):
                durability_succeeds = True
            elif (self.cluster_len-len(failed_nodes)) < majority_value:
                durability_succeeds = False

        return durability_succeeds

    def validate_durability_exception(self, failed_docs, expected_exception,
                                      retry_reason=None):
        """
        Iterates all failed docs and validates the type of exception
        falls within the list of expected_exceptions passed by the testcase.

        :param failed_docs:         All failed docs (dict)
        :param expected_exception:  Expected exceptions (list of str)
        :param retry_reason: Exception str to enable retry of failed docs

        :return validation_passed: Validation result of doc's exceptions (bool)
        """
        validation_passed = True
        for key, failed_doc in failed_docs.items():
            if not SDKException.check_if_exception_exists(expected_exception,
                                                          failed_doc["error"]):
                validation_passed = False
                self.log.error("Unexpected exception '{0}' for key '{1}'"
                               .format(failed_doc["error"], key))
            if retry_reason and retry_reason not in str(failed_doc["error"]):
                validation_passed = False
                self.log.error("Retry reason missing in '{0}' for key '{1}'"
                               .format(failed_doc["error"], key))
        return validation_passed

    def retry_with_no_error(self, client, doc_list, op_type, timeout=5,
                            load_using="default_loader"):
        """
        Retry all failed docs in singular CRUD manner and
        expects no errors from all operations.
        If as exception is seen, it marks 'op_failed' variable to true.

        :param client:    SDK client used for CRUD operations (sdk client)
        :param doc_list:  List of failed docs which needs to be retried (dict)
        :param op_type:   Type of CRUD(create/update/delete) (str)
        :param timeout:   Timeout used for SDK operations (int)

        :return op_failed: Success status of all CRUDs (bool)
        """
        op_failed = False
        if load_using == "sirius_go_sdk":
            pass
        else:
            for key, doc_info in doc_list.items():
                # If doc expiry is not set, use exp=0
                if "exp" not in doc_info:
                    doc_info["exp"] = 0

                result = client.crud(
                    op_type, key, value=doc_info["value"], exp=doc_info["exp"],
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability, timeout=timeout)
                if result["status"] is False:
                    op_failed = True
                    self.log.error("Exception: '{0}' for '{1}' during '{2}' "
                                   "with durability={3}, timeout={4}"
                                   .format(result["error"], key, op_type,
                                           self.durability, timeout))
        return op_failed

    def retry_for_ambiguous_exception(self, sdk_client, op_type, doc_key,
                                      doc_info):
        """
        Based on the op_type fetch the document and validate the
        status of the document and retry if the mutation failed in
        previous attempt.

        :param sdk_client: Sdk_Client object for reading/upsert the doc
        :param op_type: CRUD type insert/delete/update/read
        :param doc_key: Document key to validate
        :param doc_info: Dictionary of the result for the previous
                         CRUD operation
        :return retry_success: Saying the retry succeeded or not
        """
        retry_success = True
        retry_op = False
        read_result = sdk_client.crud("read", doc_key)
        if op_type == "create":
            # Previous create failed
            if read_result["value"] != doc_info["value"]:
                retry_op = True
        elif op_type == "delete":
            # Previous delete failed
            if read_result["cas"] != 0:
                retry_op = True
        elif op_type == "read":
            retry_success = False
            self.log.error("Cannot get AMBIGUOUS for %s during READ !"
                           % doc_key)
        elif op_type == "update":
            expected_mutation = doc_info["value"]["mutated"]
            curr_mutation = read_result["value"]["mutated"]
            if expected_mutation != curr_mutation:
                retry_op = True
        else:
            retry_success = False
            self.log.error("Operation %s not supported!" % op_type)

        if retry_op:
            result = sdk_client.crud(op_type, doc_key,
                                     value=doc_info["value"],
                                     replicate_to=self.replicate_to,
                                     persist_to=self.persist_to,
                                     durability=self.durability,
                                     timeout=10, time_unit="seconds")
            if result["status"] is False:
                retry_success = False
                self.log.error("%s failed during retry for %s (durability=%s)"
                               % (op_type, doc_key, self.durability))
        return retry_success

    def verify_vbucket_details_stats(self, bucket, kv_servers,
                                     vbuckets=1024,
                                     expected_val=dict()):
        """

        :param bucket: Bucket object
        :param kv_servers: List of kv_nodes currently present in the cluster
        :param vbuckets: Total vbucket count for the bucket. Default 1024
        :param expected_val: dict() containing expected key,value pairs
        :return verification_failed: Bool value denoting verification
                                     failed or not
        """
        verification_failed = False
        vb_details_stats = dict()
        ops_val = dict()
        ops_val["ops_create"] = 0
        ops_val["ops_delete"] = 0
        ops_val["ops_update"] = 0
        ops_val["ops_reject"] = 0
        ops_val["ops_get"] = 0
        ops_val["rollback_item_count"] = 0
        ops_val["sync_write_aborted_count"] = 0
        ops_val["sync_write_committed_count"] = 0
        ops_val["pending_writes"] = 0

        # Fetch stats for all available vbuckets into 'vb_details_stats'
        for server in kv_servers:
            cbstat_obj = Cbstats(server)
            vb_details_stats.update(cbstat_obj.vbucket_details(bucket.name))
            cbstat_obj.disconnect()

        for vb_num in range(0, vbuckets):
            vb_num = str(vb_num)
            for op_type in list(ops_val.keys()):
                ops_val[op_type] += int(vb_details_stats[vb_num][op_type])

        # Verification block
        for op_type in list(ops_val.keys()):
            self.log.debug("%s for %s: %s" % (op_type, bucket.name,
                                              ops_val[op_type]))

            if op_type in expected_val \
                    and not DurabilityHelper.__compare(ops_val[op_type],
                                                       expected_val[op_type],
                                                       DurabilityHelper.EQUAL):
                verification_failed = True
                self.log.error("Mismatch in %s stats. %s != %s"
                               % (op_type,
                                  ops_val[op_type],
                                  expected_val[op_type]))
        return verification_failed

    @staticmethod
    def get_vb_and_error_type(d_level):
        # Select target_vb type for testing with CRUDs
        target_vb_type = "replica"
        simulate_error = CouchbaseError.STOP_MEMCACHED

        if d_level == SDKConstants.DurabilityLevel.MAJORITY:
            target_vb_type = "replica"
            simulate_error = CouchbaseError.STOP_MEMCACHED
        elif d_level == \
                SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            target_vb_type = "active"
            simulate_error = CouchbaseError.STOP_PERSISTENCE
        elif d_level == SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY:
            target_vb_type = "replica"
            simulate_error = CouchbaseError.STOP_PERSISTENCE
        return target_vb_type, simulate_error
