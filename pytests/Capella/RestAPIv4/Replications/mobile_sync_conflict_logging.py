"""
Created on September 24, 2026

@author: Automation
"""

import base64

from pytests.Capella.RestAPIv4.Replications.replication_base import \
    ReplicationBase


class MobileSyncAndConflictLogging(ReplicationBase):
    """
    Exercises the mobileSyncMode (IDEA-1581) and conflictLogging
    (IDEA-1994) replication fields: creating both together (which also
    enables Cross Cluster Versioning on the buckets as a documented
    side-effect), reading them back, confirming they're excluded from
    the list response, and the update scenarios called out for them -
    updating both together, staging conflict logging without enabling
    it, setting all three loggingRules states in one request, changing
    one field while leaving the other untouched, and clearing
    conflictLogging entirely.
    """

    def setUp(self, nomenclature="Replications_MobileSync_ConflictLogging"):
        ReplicationBase.setUp(self, nomenclature)

        self.source_bucket_name = self.prefix + "src_" + nomenclature
        self.target_bucket_name = self.prefix + "tgt_" + nomenclature
        self.log_bucket_name = self.prefix + "log_" + nomenclature

        self.create_bucket_to_be_tested(
            self.organisation_id, self.project_id, self.cluster_id,
            self.source_bucket_name, self.buckets)
        self.create_bucket_to_be_tested(
            self.organisation_id, self.project_id, self.cluster_id,
            self.target_bucket_name, self.buckets)
        self.create_bucket_to_be_tested(
            self.organisation_id, self.project_id, self.cluster_id,
            self.log_bucket_name, self.buckets)

        self.src_bucket_b64 = self._b64(self.source_bucket_name)
        self.tgt_bucket_b64 = self._b64(self.target_bucket_name)
        self.log_bucket_b64 = self._b64(self.log_bucket_name)

        self.created_replication_id = None

    def tearDown(self):
        if self.created_replication_id:
            self.log.info("Deleting replication created for the test: {}"
                          .format(self.created_replication_id))
            res = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.delete_replication,
                self.organisation_id, self.project_id, self.cluster_id,
                self.created_replication_id)
            if res.status_code not in [204, 404]:
                self.log.error("Error while deleting replication created "
                               "for the test: {}".format(res.content))
        super(MobileSyncAndConflictLogging, self).tearDown()

    @staticmethod
    def _b64(name):
        return base64.b64encode(name.encode()).decode()

    def test_payload(self):
        failures = list()

        # 1. Create - mobileSyncMode and conflictLogging together,
        # synchronously so errors surface on the response itself instead
        # of on an async job.
        create_payload = {
            "sourceBucket": self.src_bucket_b64,
            "target": {
                "cluster": self.cluster_id,
                "bucket": self.tgt_bucket_b64
            },
            "direction": "twoWay",
            "mode": "sync",
            "mobileSyncMode": "active",
            "conflictLogging": {
                "disabled": False,
                "bucket": self.log_bucket_b64,
                "collection": "bucket1logs.defaultlogs",
                "loggingRules": {
                    "inventory": {},
                    "tenant_agent_00": {
                        "bucket": self.log_bucket_b64,
                        "collection": "bucket1logs.agentlogs"
                    },
                    "tenant_agent_01": None
                }
            }
        }
        result = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.create_replication,
            self.organisation_id, self.project_id, self.cluster_id,
            create_payload)
        if result.status_code != 201:
            self.log.error("Result: {}".format(result.content))
            self.fail("Error while creating replication with mobileSyncMode "
                      "and conflictLogging.")
        self.created_replication_id = result.json()["id"]

        # 2. Read - GET must return both fields as set.
        info = self._get_replication_info()
        self._assert_mobile_sync(info, "active", failures,
                                 "create: mobileSyncMode")
        self._assert_conflict_logging(
            info, create_payload["conflictLogging"], failures,
            "create: conflictLogging")

        # 3. List - neither field should be present in the list response.
        list_result = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.list_cluster_replications,
            self.organisation_id, self.project_id, self.cluster_id)
        if list_result.status_code == 200:
            for repl in list_result.json().get("data", []):
                if repl.get("id") == self.created_replication_id:
                    if "mobileSyncMode" in repl or "conflictLogging" in repl:
                        self.log.warning(
                            "mobileSyncMode/conflictLogging unexpectedly "
                            "present in list response: {}".format(repl))
                        failures.append("list: fields should not be present")
                    break
        else:
            self.log.error("Result: {}".format(list_result.content))
            failures.append("list: replications call failed")

        # 4. Update - both fields together.
        update_payload = {
            "mobileSyncMode": "active",
            "conflictLogging": {
                "disabled": False,
                "bucket": self.log_bucket_b64,
                "collection": "bucket1logs.defaultlogs"
            }
        }
        self._update(update_payload, failures, "update both together")
        info = self._get_replication_info()
        self._assert_mobile_sync(info, "active", failures,
                                 "update both: mobileSyncMode")
        self._assert_conflict_logging(
            info, update_payload["conflictLogging"], failures,
            "update both: conflictLogging")

        # 5. Update - stage conflict logging without turning it on.
        staged_conflict_logging = {
            "disabled": True,
            "bucket": self.log_bucket_b64,
            "collection": "bucket1logs.defaultlogs"
        }
        self._update({"conflictLogging": staged_conflict_logging}, failures,
                    "stage without enabling")
        info = self._get_replication_info()
        self._assert_conflict_logging(
            info, staged_conflict_logging, failures,
            "stage without enabling: conflictLogging")

        # 6. Update - all three loggingRules states in one request. The
        # 4th documented state (key absent -> inherit) isn't exercised
        # here since verifying inheritance needs a real scope/collection
        # hierarchy this bucket setup doesn't establish.
        all_rules_conflict_logging = {
            "disabled": False,
            "bucket": self.log_bucket_b64,
            "collection": "bucket1logs.defaultlogs",
            "loggingRules": {
                "inventory": {},
                "tenant_agent_00": {
                    "bucket": self.log_bucket_b64,
                    "collection": "bucket1logs.agentlogs"
                },
                "tenant_agent_01": None
            }
        }
        self._update({"conflictLogging": all_rules_conflict_logging},
                    failures, "all loggingRules states")
        info = self._get_replication_info()
        self._assert_conflict_logging(
            info, all_rules_conflict_logging, failures,
            "all loggingRules states: conflictLogging")

        # 7. Update - turn mobile off, leave conflictLogging untouched.
        self._update({"mobileSyncMode": "off"}, failures, "mobile off only")
        info = self._get_replication_info()
        self._assert_mobile_sync(info, "off", failures,
                                 "mobile off only: mobileSyncMode")
        self._assert_conflict_logging(
            info, all_rules_conflict_logging, failures,
            "mobile off only: conflictLogging should be untouched")

        # 8. Update - change priority only, both features untouched.
        self._update({"priority": "medium"}, failures, "priority only")
        info = self._get_replication_info()
        self._assert_mobile_sync(info, "off", failures,
                                 "priority only: mobileSyncMode untouched")
        self._assert_conflict_logging(
            info, all_rules_conflict_logging, failures,
            "priority only: conflictLogging untouched")

        # 9. Update - clear conflictLogging entirely; GET must omit the
        # key from the response afterwards, not just return it empty.
        self._update({"conflictLogging": {}}, failures, "clear")
        info = self._get_replication_info()
        if "conflictLogging" in info:
            self.log.warning(
                "conflictLogging still present after clearing: {}"
                .format(info.get("conflictLogging")))
            failures.append("clear: conflictLogging should be absent")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} sub-checks FAILED in the mobileSyncMode / "
                      "conflictLogging flow.".format(len(failures)))

    def _update(self, payload, failures, desc):
        result = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.update_replication,
            self.organisation_id, self.project_id, self.cluster_id,
            self.created_replication_id, payload)
        if result.status_code != 204:
            self.log.error("Result: {}".format(result.content))
            failures.append("{}: update returned {}".format(
                desc, result.status_code))

    def _get_replication_info(self):
        result = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.fetch_replication_info,
            self.organisation_id, self.project_id, self.cluster_id,
            self.created_replication_id)
        if result.status_code != 200:
            self.log.error("Result: {}".format(result.content))
            self.fail("Error while fetching replication info.")
        return result.json()

    def _assert_mobile_sync(self, info, expected, failures, desc):
        actual = info.get("mobileSyncMode")
        if actual != expected:
            self.log.warning(
                "{}: expected mobileSyncMode {}, got {}".format(
                    desc, expected, actual))
            failures.append(desc)

    def _assert_conflict_logging(self, info, expected, failures, desc):
        actual = info.get("conflictLogging")
        if actual != expected:
            self.log.warning(
                "{}: expected conflictLogging {}, got {}".format(
                    desc, expected, actual))
            failures.append(desc)
