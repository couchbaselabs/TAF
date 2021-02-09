import json

from BucketLib.bucket import TravelSample, BeerSample
from rbac_utils.Rbac_ready_functions import RbacUtils
from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
from remote.remote_util import RemoteMachineShellConnection


class CBASRBACTests(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        super(CBASRBACTests, self).setUp()
        self.rbac_util = RbacUtils(self.cluster.master)

    def tearDown(self):
        super(CBASRBACTests, self).tearDown()

    def test_cbas_rbac(self):
        self.bucket_util.load_sample_bucket(self.sample_bucket)

        users = [{"username": "analytics_manager1",
                  "roles": "bucket_full_access[travel-sample]:analytics_manager[travel-sample]"},
                 {"username": "analytics_manager2",
                  "roles": "bucket_admin[travel-sample]:analytics_manager[travel-sample]"},
                 {"username": "analytics_manager3",
                  "roles": "analytics_manager[travel-sample]"},
                 {"username": "analytics_manager4",
                  "roles": "data_reader[travel-sample]:analytics_manager[travel-sample]"},
                 {"username": "analytics_reader1",
                  "roles": "bucket_admin[travel-sample]:analytics_reader"},
                 {"username": "analytics_reader2", "roles": "analytics_reader"},
                 {"username": "analytics_reader3",
                  "roles": "bucket_full_access[travel-sample]:analytics_reader"},
                 {"username": "analytics_reader4",
                  "roles": "data_reader[travel-sample]:analytics_reader"},
                 {"username": "ro_admin", "roles": "ro_admin"},
                 {"username": "cluster_admin", "roles": "cluster_admin"},
                 {"username": "admin", "roles": "admin"},
                 {"username": "analytics_reader", "roles": "analytics_reader"},
                 {"username": "analytics_manager", "roles": "analytics_manager[*]"}  
                ]

        operation_map = [
            {"operation": "drop_dataset",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager4",
                                       "admin"
                                       ],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_manager2",
                                           "analytics_reader1", 
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4",
                                           "cluster_admin",
                                           ]},
            {"operation": "create_index",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager4",
                                       "admin"
                                       ],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_manager2",
                                           "analytics_reader1", 
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4",
                                           "cluster_admin",
                                           ]},
            {"operation": "drop_index",
             "should_work_for_users": ["analytics_manager1",
                                       "analytics_manager4",
                                       "admin"
                                       ],
             "should_not_work_for_users": ["analytics_manager3",
                                           "analytics_manager2",
                                           "analytics_reader1", 
                                           "analytics_reader2",
                                           "analytics_reader3",
                                           "analytics_reader4",
                                           "cluster_admin",
                                           ]},
            {"operation": "execute_query",
             "should_work_for_users": ["analytics_manager3",
                                       "analytics_reader2",
                                       "cluster_admin",
                                       "admin"
                                       ]},
            {"operation": "execute_metadata_query",
             "should_work_for_users": ["analytics_manager3",
                                       "analytics_reader2",
                                       "cluster_admin",
                                       "admin"
                                       ]},
            {
                "operation": "create_dataverse",
                "should_work_for_users": [
                                          "cluster_admin",
                                          "admin"
                                          ],
                "should_not_work_for_users": [
                                              "analytics_reader",
                                              "analytics_manager",
                                              "ro_admin"
                                              ]
            },
            {
                "operation": "drop_dataverse",
                "should_work_for_users": [
                                          "cluster_admin",
                                          "admin"
                                          ],
                "should_not_work_for_users": [
                                              "analytics_reader",
                                              "analytics_manager",
                                              "ro_admin"
                                              ]
            }
            ]

        for user in users:
            self.log.info("Creating user %s", user["username"])
            self.rbac_util._create_user_and_grant_role(user["username"], user["roles"])
            self.sleep(2)

        status = True

        for operation in operation_map:
            self.log.info(
                "============ Running tests for operation %s ============",
                operation["operation"])
            for user in operation["should_work_for_users"]:
                result = self._run_operation(operation["operation"], user)
                if not result:
                    self.log.info(
                        "=== Operation {0} failed for user {1} while it should have worked".format(
                            operation["operation"], user))
                    status = False
                else:
                    self.log.info(
                        "Operation : {0}, User : {1} = Works as expected".format(
                            operation["operation"], user))
            if "should_not_work_for_users" in operation:
                for user in operation["should_not_work_for_users"]:
                    result = self._run_operation(operation["operation"], user)
                    if result:
                        self.log.info(
                            "=== Operation {0} worked for user {1} while it should not have worked".format(
                                operation["operation"], user))
                        status = False
                    else:
                        self.log.info(
                            "Operation : {0}, User : {1} = Works as expected".format(
                                operation["operation"], user))

        self.assertTrue(status,
                        "=== Some operations have failed for some users. Pls check the log above.")

    def _run_operation(self, operation, username):
        if username:
            try:
                self.cbas_util.createConn(self.cb_bucket_name,username=username)
            except:
                self.cbas_util.closeConn()
                return False
        if operation:
            if operation == "create_dataset":
                status = self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                                       self.cbas_dataset_name,
                                                       username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "connect_bucket":
                self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                              self.cbas_dataset_name)
                status = self.cbas_util.connect_to_bucket(self.cbas_bucket_name,
                                                username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "disconnect_bucket":
                self.cbas_util.create_dataset_on_bucket(self.cbas_bucket_name,
                                              self.cbas_dataset_name)
                self.cbas_util.connect_to_bucket(self.cbas_bucket_name)
                status = self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name,
                                                     username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "drop_dataset":
                self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                              self.cbas_dataset_name)
                status = self.cbas_util.drop_dataset(self.cbas_dataset_name,
                                           username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "drop_bucket":
                status = self.cbas_util.drop_cbas_bucket(self.cbas_bucket_name,
                                               username=username)
                self.log.info(
                    "^^^^^^^^^^^^^^ Status of drop bucket for user {0}: {1}".format(
                        username, status))

                # Cleanup
                self.cleanup_cbas()

            elif operation == "create_index":
                self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                              self.cbas_dataset_name)
                create_idx_statement = "create index idx1 on {0}(city:String);".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    create_idx_statement, username=username)
                status = False if status != "success" else True

                # Cleanup
                drop_idx_statement = "drop index {0}.idx1;".format(
                    self.cbas_dataset_name)
                self.cbas_util.execute_statement_on_cbas_util(drop_idx_statement)
                self.cleanup_cbas()

            elif operation == "drop_index":
                self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                              self.cbas_dataset_name)
                create_idx_statement = "create index idx1 on {0}(city:String);".format(
                    self.cbas_dataset_name)
                self.cbas_util.execute_statement_on_cbas_util(create_idx_statement)
                self.sleep(10)
                drop_idx_statement = "drop index {0}.idx1;".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    drop_idx_statement, username=username)
                status = False if status != "success" else True

                # Cleanup
                drop_idx_statement = "drop index {0}.idx1;".format(
                    self.cbas_dataset_name)
                self.cbas_util.execute_statement_on_cbas_util(drop_idx_statement)
                self.cleanup_cbas()

            elif operation == "execute_query":
                self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name,
                                              self.cbas_dataset_name)
                self.cbas_util.connect_to_bucket(self.cbas_bucket_name)
                query_statement = "select count(*) from {0};".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    query_statement, username=username)

                # Cleanup
                self.cleanup_cbas()

            elif operation == "execute_metadata_query":
                query_statement = "select Name from Metadata.`Bucket`;".format(
                    self.cbas_dataset_name)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    query_statement, username=username)
                self.cleanup_cbas()
            
            elif operation == "create_dataverse":
                status = self.cbas_util.create_dataverse_on_cbas(dataverse_name="Custom", username=username)
                self.cleanup_cbas()
            
            elif operation == "drop_dataverse":
                self.cbas_util.create_dataverse_on_cbas(dataverse_name="Custom")
                status = self.cbas_util.drop_dataverse_on_cbas(dataverse_name="Custom", username=username)
                self.cleanup_cbas()
                
        self.cbas_util.closeConn()
        return status

    def test_rest_api_authorization_version_api_no_authentication(self):
        api_url = "http://{0}:8095/analytics/version".format(self.cbas_node.ip)
        shell = RemoteMachineShellConnection(self.cluster.master)

        roles = ["analytics_manager[*]", "analytics_reader", "ro_admin",
                 "cluster_admin", "admin"]

        for role in roles:
            self.rbac_util._create_user_and_grant_role("testuser", role)

            output, error = shell.execute_command(
                """curl -i {0} 2>/dev/null | head -n 1 | cut -d$' ' -f2""".format(
                    api_url))
            response = ""
            for line in output:
                response = response + line
            response = json.loads(response)
            self.log.info(response)

            self.assertEqual(response, 200)
        shell.disconnect()

    def test_rest_api_authorization_cbas_cluster_info_api(self):
        validation_failed = False

        self.bucket_util.load_sample_bucket(TravelSample())

        self.bucket_util.load_sample_bucket(BeerSample())

        api_authentication = [{
            "api_url": "http://{0}:8095/analytics/cluster".format(
                self.cbas_node.ip),
            "roles": [{"role": "ro_admin",
                       "expected_status": 200},
                      {"role": "cluster_admin",
                       "expected_status": 200},
                      {"role": "admin",
                       "expected_status": 200},
                      {"role": "analytics_manager[*]",
                       "expected_status": 401},
                      {"role": "analytics_reader",
                       "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/cluster/cc".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 200},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/diagnostics".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 200},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/node/diagnostics".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 200},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]},
            {
                "api_url": "http://{0}:8095/analytics/cc/config".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 401},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]
            },
            {
                "api_url": "http://{0}:8095/analytics/node/config".format(
                    self.cbas_node.ip),
                "roles": [{"role": "ro_admin",
                           "expected_status": 401},
                          {"role": "cluster_admin",
                           "expected_status": 200},
                          {"role": "admin",
                           "expected_status": 200},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}]
            },
            {
                "api_url": "http://{0}:9110/analytics/node/agg/stats/remaining".format(self.cbas_node.ip),
                "roles": [
                    {"role": "analytics_manager[*]", "expected_status": 200},
                    {"role": "analytics_reader", "expected_status": 200}],
            },
            {
                "api_url": "http://{0}:8095/analytics/backup?bucket=travel-sample".format(self.cbas_node.ip),
                "roles": [
                    {"role": "admin", "expected_status": 200},
                    {"role": "data_backup[*],analytics_reader", "expected_status": 200},
                    {"role": "data_backup[*], analytics_manager[*]", "expected_status": 200},
                    {"role": "data_backup[travel-sample], analytics_reader", "expected_status": 200},
                    {"role": "data_backup[travel-sample], analytics_manager[travel-sample]", "expected_status": 200},
                    {"role": "ro_admin", "expected_status": 401},
                    {"role": "analytics_reader", "expected_status": 401},
                    {"role": "analytics_manager[*]", "expected_status": 401},
                    {"role": "data_backup[beer-sample], analytics_reader", "expected_status": 401},
                    {"role": "data_backup[beer-sample], analytics_manager[*]", "expected_status": 401},
                    {"role": "data_backup[beer-sample], analytics_manager[beer-sample]", "expected_status": 401},
                ],
            },
            {
                "api_url": "http://{0}:8095/analytics/cluster/restart".format(self.cbas_node.ip),
                "roles": [
                          {"role": "cluster_admin",
                           "expected_status": 202},
                          {"role": "admin",
                           "expected_status": 202},
                          {"role": "analytics_manager[*]",
                           "expected_status": 401},
                          {"role": "analytics_reader",
                           "expected_status": 401}],
                "method": "POST"
            },
        ]

        shell = RemoteMachineShellConnection(self.cluster.master)

        for api in api_authentication:
            for role in api["roles"]:
                self.rbac_util._create_user_and_grant_role("testuser", role["role"])
                self.sleep(5)

                if "method" in api:
                    output, error = shell.execute_command(
                        """curl -i {0} -X {1} -u {2}:{3} 2>/dev/null | head -n 1 | cut -d$' ' -f2""".format(
                            api["api_url"], api["method"], "testuser",
                            "password"))
                    self.sleep(10)
                else:
                    output, error = shell.execute_command(
                        """curl -i {0} -u {1}:{2} 2>/dev/null | head -n 1 | cut -d$' ' -f2""".format(
                            api["api_url"], "testuser", "password"))
                response = ""
                for line in output:
                    response = response + line
                response = json.loads(str(response))
                if response != role["expected_status"]:
                    self.log.info(
                        "Error accessing {0} as user with {1} role. Response = {2}".format(
                            api["api_url"], role["role"], response))
                    validation_failed = True
                else:
                    self.log.info(
                        "Accessing {0} as user with {1} role worked as expected".format(
                        api["api_url"], role["role"]))

                self.rbac_util._drop_user("testuser")

        shell.disconnect()

        self.assertFalse(validation_failed,
                         "Authentication errors with some APIs. Check the test log above.")
