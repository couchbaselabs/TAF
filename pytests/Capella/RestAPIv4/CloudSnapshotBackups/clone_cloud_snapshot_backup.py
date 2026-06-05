import copy

from pytests.Capella.RestAPIv4.CloudSnapshotBackups.cloud_snapshot_backup_base import \
    CloudSnapshotBackupBase


class CloneCloudSnapshotBackup(CloudSnapshotBackupBase):

    def setUp(self, nomenclature="Cloud_Snapshot_Backup_Clone"):
        CloudSnapshotBackupBase.setUp(self, nomenclature)
        self.backup_id = self.ensure_backup(wait_for_complete=True)
        self.clone_data = self.clone_payload()

    def tearDown(self):
        super(CloneCloudSnapshotBackup, self).tearDown()

    def clone_call(self, organization, project, backup, payload, headers=None):
        return self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.clone_cloud_snapshot_backup,
            organization, project, backup, payload["name"],
            payload["cloudProvider"], payload["availability"],
            payload["support"], payload.get("description"),
            payload.get("zones"), headers=headers)

    def test_api_path(self):
        testcases = [
            # {"description": "Clone cloud snapshot backup with valid path"},
            {
                "description": "Clone cloud snapshot backup with non-hex organizationID",
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
                "description": "Clone cloud snapshot backup with non-hex projectID",
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
                "description": "Clone cloud snapshot backup with non-hex backupID",
                "invalid_backupID": self.replace_last_character(
                    self.backup_id, non_hex=True),
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
            organization = self.organisation_id
            project = self.project_id
            backup = self.backup_id
            payload = copy.deepcopy(self.clone_data)
            payload["name"] = self.generate_random_string(special_characters=False)

            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_backupID" in testcase:
                backup = testcase["invalid_backupID"]

            result = self.clone_call(organization, project, backup, payload)
            self.validate_testcase(result, [202, 409], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReader", "projectDataReaderWriter"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            payload = copy.deepcopy(self.clone_data)
            payload["name"] = self.generate_random_string(special_characters=False)
            result = self.clone_call(
                self.organisation_id, self.project_id, self.backup_id, payload,
                headers=header)
            self.validate_testcase(result, [202, 409], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Valid clone payload",
                "payload": copy.deepcopy(self.clone_data)
            }, {
                "desc": "Missing clone name",
                "payload": dict(copy.deepcopy(self.clone_data), name=""),
                "expected_status_code": 422
            }, {
                "desc": "Invalid cloudProvider type",
                "payload": dict(copy.deepcopy(self.clone_data), cloudProvider="aws"),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            payload = testcase["payload"]
            if payload.get("name"):
                payload["name"] = self.generate_random_string(special_characters=False)
            result = self.clone_call(
                self.organisation_id, self.project_id, self.backup_id, payload)
            self.validate_testcase(result, [202, 409], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
