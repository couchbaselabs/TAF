from math import floor
from BucketLib.BucketOperations import BucketHelper


class DurabilityHelper:
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
