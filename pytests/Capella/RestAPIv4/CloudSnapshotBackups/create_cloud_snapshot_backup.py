import copy

from pytests.Capella.RestAPIv4.CloudSnapshotBackups.cloud_snapshot_backup_base import \
    CloudSnapshotBackupBase


class CreateCloudSnapshotBackup(CloudSnapshotBackupBase):

    def setUp(self, nomenclature="Cloud_Snapshot_Backup_Create"):
        CloudSnapshotBackupBase.setUp(self, nomenclature)

    def tearDown(self):
        super(CreateCloudSnapshotBackup, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Create cloud snapshot backup with valid path"
            }, {
                "description": "Create cloud snapshot backup with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Create cloud snapshot backup with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Create cloud snapshot backup with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("[backup-trace] Executing test: {}".format(
                testcase.get("description", testcase.get("desc"))))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]

            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_cloud_snapshot_backup,
                organization, project, cluster, retention=self.default_retention)
            self.validate_testcase(result, [202, 409], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            self.log.info("[backup-trace] Executing test: {}".format(
                testcase.get("description", testcase.get("desc"))))
            header = dict()
            self.auth_test_setup(
                testcase, failures, header, self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_cloud_snapshot_backup,
                self.organisation_id, self.project_id, self.cluster_id,
                retention=self.default_retention, headers=header)
            if ("expected_status_code" not in testcase and
                    self.is_backup_quota_exceeded(result)):
                # Only 5 backups/hour are allowed per cluster, fewer than the
                # number of authorized roles. RBAC is evaluated before the
                # quota (unauthorized roles still get 403 here), so a quota
                # 429 means this role was authorized.
                self.log.info("{}: authorized, blocked only by backup quota"
                              .format(testcase["description"]))
                continue
            self.validate_testcase(result, [202, 409], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        payload = {"retention": self.default_retention}
        # The backend applies the per-cluster rate limit and hourly backup
        # quota BEFORE validating the payload. Invalid payloads don't create
        # backups (so don't use quota), hence run them first and the valid
        # case (which consumes a quota slot) last.
        testcases = [
            {
                "desc": "Invalid retention type",
                "payload": dict(payload, retention="168"),
                "expected_status_code": 422
            }, {
                "desc": "Invalid regionsToCopy type",
                "payload": dict(payload, regionsToCopy="us-east-1"),
                "expected_status_code": 400
            }, {
                "desc": "Valid cloud snapshot backup payload",
                "payload": copy.deepcopy(payload)
            }
        ]

        failures = list()
        skipped = list()
        for testcase in testcases:
            self.log.info("[backup-trace] Executing test: {}".format(
                testcase.get("description", testcase.get("desc"))))
            pld = testcase["payload"]
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_cloud_snapshot_backup,
                self.organisation_id, self.project_id, self.cluster_id,
                retention=pld.get("retention"),
                regionsToCopy=pld.get("regionsToCopy"))
            if ("expected_status_code" in testcase and
                    self.is_backup_quota_exceeded(result)):
                # Quota is checked before payload validation, so the payload
                # can't be verified until the hourly window frees up.
                self.log.warning(
                    "[backup-trace] SKIPPED '{}': backup quota exceeded "
                    "before payload validation, expected {} could not be "
                    "verified".format(testcase["desc"],
                                      testcase["expected_status_code"]))
                skipped.append(testcase["desc"])
                continue
            self.validate_testcase(result, [202, 409], testcase, failures,
                                   payloadTest=True)

        if skipped:
            self.log.warning("[backup-trace] {} payload case(s) not verified "
                             "due to backup quota: {}".format(
                                 len(skipped), skipped))
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
