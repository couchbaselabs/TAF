from rbac_utils.Rbac_ready_functions import RbacUtils
from cbas.cbas_base import CBASBaseTest


class CBASConcurrentQueryMgtTests(CBASBaseTest):
    def setUp(self):
        super(CBASConcurrentQueryMgtTests, self).setUp()
        self.validate_error = False
        if self.expected_error:
            self.validate_error = True
        self.handles = []
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.expected_count = self.num_items
        self.cb_bucket_name = "default"
        self.cbas_bucket_name = "default_bucket"
        self.cbas_dataset_name = "default_ds"
        self.validate_item_count = True
        self.statement = "select sleep(count(*),500) from {0} where mutated=0;".format(
            self.cbas_dataset_name)

    def tearDown(self):
        super(CBASConcurrentQueryMgtTests, self).tearDown()

    def _setupForTest(self):
        # Create bucket on CBAS
        self.cbas_util.createConn(self.cb_bucket_name)
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Load CB bucket
        self.perform_doc_ops_in_all_cb_buckets("create", 0,
                                               self.num_items)

        # Wait while ingestion is completed
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name],self.num_items)
        if self.mode is not None:
            self.cbas_util.closeConn()

    def test_concurrent_query_mgmt(self):
        self._setupForTest()
        self.cbas_util._run_concurrent_queries(self.statement, self.mode, self.num_concurrent_queries,batch_size=self.concurrent_batch_size)

    def test_resource_intensive_queries_queue_mgmt(self):
        self._setupForTest()
        compiler_param_statement = "SET `{0}` \"{1}\";".format(
            self.compiler_param, self.compiler_param_val)
        group_by_query_statement = "select first_name, sleep(count(*),500) from {0} GROUP BY first_name;".format(
            self.cbas_dataset_name)
        order_by_query_statement = "select first_name from {0} ORDER BY first_name;".format(
            self.cbas_dataset_name)
        default_query_statement = "select sleep(count(*),500) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        join_query_statement = "select a.firstname as fname, b.firstname as firstName from {0} a, {0} b where a.number=b.number;".format(
            self.cbas_dataset_name)

        if self.compiler_param == "compiler.groupmemory":
            self.statement = compiler_param_statement + group_by_query_statement
        elif self.compiler_param == "compiler.joinmemory":
            self.statement = compiler_param_statement + join_query_statement
        elif self.compiler_param == "compiler.sortmemory":
            self.statement = compiler_param_statement + order_by_query_statement
        elif self.compiler_param == "compiler.parallelism":
            self.statement = compiler_param_statement + default_query_statement

        self.validate_item_count = False

        self.cbas_util._run_concurrent_queries(self.statement, self.mode, self.num_concurrent_queries,batch_size=self.concurrent_batch_size)

        if self.expect_reject:
            if self.cbas_util.rejected_count < self.num_concurrent_queries:
                self.fail("Not all queries rejected. Rejected Count: %s, Query Count: %s"%(self.rejected_count, self.num_concurrent_queries))
        else:
            if self.cbas_util.rejected_count:
                self.fail("Some queries rejected. Rejected Count: %s"%self.cbas_util.rejected_count)

    def test_cancel_ongoing_request(self):
        self._setupForTest()

        client_context_id = "abcd1234"
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="async", client_context_id=client_context_id)

        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "200":
            self.fail ("Status is not 200")

    def test_cancel_cancelled_request(self):
        self._setupForTest()

        client_context_id = "abcd1234"
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="async", client_context_id=client_context_id)

        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "200":
            self.fail("Status is not 200")

#         status = self.cbas_util.delete_request(client_context_id)
#         if str(status) != "404":
#             self.fail("Status is not 404")

    def test_cancel_invalid_context(self):
        client_context_id = "abcd1235"
        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "404":
            self.fail("Status is not 404")

    def test_cancel_completed_request(self):
        self._setupForTest()

        client_context_id = "abcd1234"

        statement = "select count(*) from {0};".format(self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="immediate", client_context_id=client_context_id)

        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "404":
            self.fail("Status is not 404")

    def test_cancel_request_in_queue(self):
        client_context_id = "query_thread_{0}".format(int(self.num_concurrent_queries)-1)
        self._setupForTest()
        self.statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(self.statement, self.mode, self.num_concurrent_queries,batch_size=self.concurrent_batch_size)
        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "200":
            self.fail ("Status is not 200")

    def test_cancel_ongoing_request_null_contextid(self):
        self._setupForTest()

        client_context_id = None
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="async", client_context_id=client_context_id)

        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "404":
            self.fail("Status is not 404")

    def test_cancel_ongoing_request_empty_contextid(self):
        self._setupForTest()

        client_context_id = ""
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="async", client_context_id=client_context_id)

        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "200":
            self.fail("Status is not 200")

    def test_cancel_multiple_ongoing_request_same_contextid(self):
        self._setupForTest()

        client_context_id = "abcd1234"
        statement = "select sleep(count(*),10000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="async", client_context_id=client_context_id)
        status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
            statement, mode="async", client_context_id=client_context_id)

        status = self.cbas_util.delete_request(client_context_id)
        if str(status) != "200":
            self.fail("Status is not 200")

    def test_rest_api_authorization_cancel_request(self):
        validation_failed = False

        self._setupForTest()

        roles = [
#                 {"role": "ro_admin",
#                   "expected_status": 401},
                 {"role": "cluster_admin",
                  "expected_status": 200},
                 {"role": "admin",
                  "expected_status": 200},
                 {"role": "analytics_manager[*]",
                  "expected_status": 200},
                 {"role": "analytics_reader",
                  "expected_status": 200}]

        for role in roles:
            RbacUtils(self.cluster.master)._create_user_and_grant_role("testuser", role["role"])
            self.sleep(5)

            client_context_id = "abcd1234"
            statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
                self.cbas_dataset_name)
            status, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(
                statement, mode="async", client_context_id=client_context_id)

            status = self.cbas_util.delete_request(client_context_id, username="testuser")
            if str(status) != str(role["expected_status"]):
                self.log.info(
                    "Error cancelling request as user with {0} role. Response = {1}".format(
                        role["role"], status))
                validation_failed = True
            else:
                self.log.info(
                    "Cancelling request as user with {0} role worked as expected".format(
                        role["role"]))

            RbacUtils(self.cluster.master)._drop_user("testuser")

        self.assertFalse(validation_failed,
                         "Authentication errors with some APIs. Check the test log above.")
