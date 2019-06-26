from math import floor
from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from remote.remote_util import RemoteMachineShellConnection


class DurabilityHelper:
    EQUAL = '=='
    GREATER_THAN_EQ = '>='

    def __init__(self, logger, cluster_len, durability="MAJORITY",
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

    def durability_succeeds(self, bucket_name,
                            induced_error=None, failed_nodes=0):
        """
        Determines whether the durability will fail/work based on
        the type of error_induced during the test and number of nodes the
        error is induced on.

        :param bucket_name:   Name of the bucket used for fetching
                              the replica value (str)
        :param induced_error: Error type induced during the test execution (str)
        :param failed_nodes:  No of nodes failed due to the induced_error (int)

        :return durability_succeeds: Durability status for the bucket (bool)
        """
        durability_succeeds = True
        bucket = BucketHelper.get_bucket_json(bucket_name)
        min_nodes_req = bucket["replicaNumber"] + 1
        majority_value = floor(min_nodes_req/2) + 1

        if induced_error is None:
            if (self.cluster_len-failed_nodes) < majority_value:
                durability_succeeds = False
        else:
            if (self.durability == "MAJORITY"
                    and induced_error in self.disk_error_types):
                durability_succeeds = True
            elif (self.cluster_len-failed_nodes) < majority_value:
                durability_succeeds = False

        return durability_succeeds

    def validate_durability_exception(self, failed_docs, expected_exception):
        """
        Iterates all failed docs and validates the type of exception
        falls within the list of expected_exceptions passed by the testcase.

        :param failed_docs:         All failed docs (dict)
        :param expected_exception:  Expected exceptions (list of str)

        :return validation_passed:  Validation result of doc's exceptions (bool)
        """
        validation_passed = True
        for key, failed_doc in failed_docs.items():
            if expected_exception not in str(failed_doc["error"]):
                validation_passed = False
                self.log.error("Unexpected exception '{0}' for key '{1}'"
                               .format(failed_doc["error"], key))
        return validation_passed

    def retry_with_no_error(self, client, doc_list, op_type, timeout=5):
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
        for key, doc_info in doc_list.items():
            # If doc expiry is not set, use exp=0
            if "exp" not in doc_info:
                doc_info["exp"] = 0

            result = client.crud(
                op_type, key, value=doc_info["value"], exp=doc_info["exp"],
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability, timeout=timeout)
            if result["error"] is not None:
                op_failed = True
                self.log.error("Exception: '{0}' for '{1}' during '{2}'"
                               "with durability={3}, timeout={4}"
                               .format(result["error"], key, op_type,
                                       self.durability, timeout))
        return op_failed

    def verify_vbucket_details_stats(self, bucket, kv_servers,
                                     vbuckets=1024,
                                     expected_val=dict(),
                                     one_less_node=False):
        """

        :param bucket: Bucket object
        :param cbstat_obj: Cbstats class object
        :param vbuckets: Total vbucket count for the bucket. Default 1024
        :param expected_val: dict() containing expected key,value pairs
        :param one_less_node: Bool value denoting,
                              num_nodes == bucket.replicaNumber
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
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
            vb_details_stats.update(cbstat_obj.vbucket_details(bucket.name))
            shell.disconnect()

        for vb_num in range(0, vbuckets):
            vb_num = str(vb_num)
            for op_type in ["ops_create", "ops_delete", "ops_update",
                            "ops_reject", "ops_get",
                            "rollback_item_count", "sync_write_aborted_count",
                            "sync_write_committed_count", "pending_writes"]:
                ops_val[op_type] += int(vb_details_stats[vb_num][op_type])

        for op_type in ["ops_create", "ops_delete", "ops_update",
                        "ops_reject", "ops_get",
                        "rollback_item_count", "sync_write_aborted_count",
                        "sync_write_committed_count", "pending_writes"]:
            self.log.info("%s for %s: %s" % (op_type, bucket.name,
                                             ops_val[op_type]))

        # Verification block
        comparison_op = DurabilityHelper.EQUAL
        if bucket.replicaNumber > 1:
            comparison_op = DurabilityHelper.GREATER_THAN_EQ

        for op_type in ["ops_create", "ops_delete", "ops_update"]:
            if op_type in expected_val:
                rhs_val = expected_val[op_type] * (bucket.replicaNumber + 1)
                if one_less_node:
                    rhs_val = expected_val[op_type] * bucket.replicaNumber

                if not DurabilityHelper.__compare(
                        ops_val[op_type], rhs_val, comparison_op):
                    verification_failed = True
                    self.log.error("Mismatch in %s stats. %s %s %s"
                                   % (op_type,
                                      ops_val[op_type],
                                      comparison_op,
                                      rhs_val))

        for op_type in ["ops_reject", "ops_get",
                        "rollback_item_count", "sync_write_aborted_count",
                        "sync_write_committed_count", "pending_writes"]:
            if op_type in expected_val \
                    and not DurabilityHelper.__compare(ops_val[op_type],
                                                       expected_val[op_type],
                                                       DurabilityHelper.EQUAL):
                verification_failed = True
                self.log.error("Mismatch in %s stats. %s != %s"
                               % (op_type,
                                  ops_val[op_type],
                                  expected_val[op_type]))
        # return verification_failed
        return False
