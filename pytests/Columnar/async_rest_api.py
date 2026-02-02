"""
Created on 12-Dec-2025

@author: himanshu.jain@couchbase.com
"""
import os
import random
import time
from queue import Queue

from concurrent.futures import ThreadPoolExecutor
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from custom_exceptions.exception import RebalanceFailedException
from cbas_utils.cbas_utils_columnar import CBOUtil
from membase.api.rest_client import RestConnection
from awsLib.s3_data_helper import perform_S3_operation
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil

runtype = TestInputSingleton.input.param("runtype", "default").lower()


class AsyncRestApi(ColumnarBaseTest):
    def setUp(self):
        test_method_name = self._testMethodName
        if test_method_name in ['test_query_rebalance', 'test_rebalance_query']:
            TestInputSingleton.input.test_params['nodes_init'] = '1|2'

        super(AsyncRestApi, self).setUp()

        self.initial_doc_count = self.input.param("initial_doc_count", 1000)
        self.file_format = self.input.param("file_format", "json")

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        # Initialize Analytics REST API client for async operations
        self.analytics_api = AnalyticsRestAPI(self.analytics_cluster.master)

        # Initialize rebalance utility for onprem tests
        if runtype == "onprem-columnar":
            self.rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task, False,
                self.cbas_util)

        # Setup AWS credentials for tests that use S3/external links
        if not hasattr(self, 'aws_access_key') or not self.aws_access_key:
            self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", None)
        if not hasattr(self, 'aws_secret_key') or not self.aws_secret_key:
            self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
        if not hasattr(self, 'aws_region') or not self.aws_region:
            self.aws_region = self.input.param("aws_region", "")

        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__
        )

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if hasattr(self, "columnar_spec"):
            if not self.cbas_util.delete_cbas_infra_created_from_spec(
                    self.columnar_cluster):
                self.fail("Error while deleting cbas entities")

        try:
            super(AsyncRestApi, self).tearDown()
        except RebalanceFailedException as e:
            pass
        except Exception as e:
            pass

        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def setup_remote_collection(self):
        self.log.info(
            f"Remote cluster: {self.remote_cluster.master.ip if self.remote_cluster else 'None'}")
        self.log.info(
            f"Columnar cluster: {self.columnar_cluster.master.ip if self.columnar_cluster else 'None'}")
        self.log.info(f"Initial doc count: {self.initial_doc_count}")

        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        # create remote link and remote collection in columnar
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        self.log.info("Started Doc loading on remote cluster")
        self.load_remote_collections(self.remote_cluster,
                                     create_start_index=0, create_end_index=self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        for dataset in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset.full_name,
                    dataset.num_of_items):
                self.fail("Doc count mismatch.")

        self.log.info(
            f"{self.initial_doc_count} docs loaded into remote cluster")

    def test_request_success(self):
        # Submit async request
        statement = 'SELECT sleep("v", 10000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Assertion failed: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}, has_handle={'handle' in result}"
                        )  # CHANGE TO 202

        # Wait for completion
        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")
        status, _, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=60, poll_interval=1
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(status and result.get("status") == "success" and
                        response.status_code == 200 and result.get(
                            "resultCount") > 0,
                        f"Assertion failed: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}, result_count={result.get('resultCount')}"
                        )

        # Get results
        status, result, response = self.analytics_api.get_request_result(
            request_id, handle)
        self.assertTrue(status and ("results" in result) and ("metrics" in result) and
                        response.status_code == 200 and len(
                            result.get("results")) > 0,
                        f"Assertion failed: status={status}, has_results={'results' in result}, has_metrics={'metrics' in result}, "
                        f"response_code={response.status_code}, result_count={len(result.get('results'))}, metrics={result.get('metrics')}"
                        )
        self.log.info(f"Results: {result.get('results')}")

    def test_request_fatal(self):
        # Submit async request
        statement = 'SELECT SLECT;'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(
            status and result.get("status") == "failed" and
            response.status_code == 200 and ("errors" in result),
            f"Assertion failed: status={status}, result_status={result.get('status')}, "
            f"response_code={response.status_code}, has_errors={'errors' in result}"
        )  # CHANGE TO 400 and fatal
        self.log.info(f"Errors: {result.get('errors')}")

    def test_status_timeout(self):
        # Submit async request with timeout
        statement = 'SELECT sleep("v", 60*60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async",
            timeout="30s"
        )
        self.assertTrue(
            status and result.get("status") == "queued" and
            response.status_code == 200 and "handle" in result,
            f"Assertion failed: status={status}, result_status={result.get('status')}, "
            f"response_code={response.status_code}, has_handle={'handle' in result}"
        )

        # Wait for completion
        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")
        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=60, poll_interval=1
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(
            status and result.get("status") == "timeout" and
            response.status_code == 200 and ("errors" in result) and result.get(
                "errors")[0].get("code") == 21002 and ("metrics" in result),
            f"Assertion failed: status={status}, result_status={result.get('status')}, "
            f"response_code={response.status_code}, has_errors={'errors' in result}, has_metrics={'metrics' in result}, error_code={result.get('errors')[0].get('code')}"
        )
        self.log.info(f"Errors: {result.get('errors')}")

    def test_status_queued(self):
        # Submit multiple async requests
        count = 0
        while count < 100:
            statement = 'SELECT sleep("v", 60*60*1000);'
            status, result, response = self.analytics_api.submit_service_request(
                statement=statement,
                mode="async"
            )
            self.assertTrue(status, "Failed to submit async request")

            request_id = result["requestID"]
            handle = result["handle"].split("/")[-1]
            count += 1

        self.log.info(
            f"Last submitted request: {request_id}, handle: {handle}")

        # Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(
            status and result.get("status") == "queued" and
            response.status_code == 200 and ("metrics" in result),
            f"Assertion failed: status={status}, result_status={result.get('status')}, "
            f"response_code={response.status_code}, has_metrics={'metrics' in result}"
        )
        self.log.info(f"Request in queue verified")

    def test_status_running(self):
        # Submit async request
        statement = 'SELECT sleep("v", 60*60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status, "Failed to submit async request")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")

        # Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(
            status and result.get("status") == "running" and
            response.status_code == 200 and ("metrics" in result),
            f"Assertion failed: status={status}, result_status={result.get('status')}, "
            f"response_code={response.status_code}, has_metrics={'metrics' in result}"
        )
        self.log.info(f"Request in running state verified")

    def test_status_runtime_fatal(self):
        # Submit async request
        statement = f"get_day(\"invalid\");"
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and response.status_code == 200,
                        f"Assertion failed: status={status}, response_code={response.status_code}"
                        )

        # Wait for completion
        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=60, poll_interval=1
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        errors = result.get("errors")
        error_code = errors[0].get(
            "code") if errors and len(errors) > 0 else None
        self.assertTrue(status and result.get("status") == "failed" and
                        response.status_code == 200 and error_code == 24011,
                        f"Assertion failed: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}, error_code={error_code}"
                        )
        self.log.info(f"Errors: {result.get('errors')}")

    def test_restart_analytics_request(self):
        # Restart analytics cluster
        status, result, response = self.analytics_api.restart_analytics_service()
        self.assertTrue(status, "Failed to restart analytics service")
        self.log.info(f"Analytics service restarted")

        # Submit async request
        statement = f'SELECT 1;'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(response.status_code == 503 and result.get("errors")[0].get("code") == 23000 and ("metrics" in result),
                        f"Assertion failed: status={status}, response_code={response.status_code}, \
                            error_code={result.get('errors')[0].get('code')}, has_metrics={'metrics' in result}"
                        )
        self.log.info(f"Errors: {result.get('errors')}")

    def test_invalid_mode(self):
        # Submit async request
        statement = f'SELECT sleep("v", 60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="foobar"
        )
        self.assertTrue(response.status_code == 400 and result.get("status") == "fatal" and result.get(
            "errors")[0].get("code") == 21008 and ("metrics" in result),
            f"Assertion failed: response_code={response.status_code}, result_status={result.get('status')}, \
            error_code={result.get('errors')[0].get('code')}, has_metrics={'metrics' in result}"
        )
        self.log.info(f"Errors: {result.get('errors')}")

    def test_unauthorized_request(self):
        # Submit async request
        statement = f'SELECT sleep("v", 60*60*1000);'
        self.analytics_api.username = "random"
        self.log.info(
            f"Executing statement: {statement} in async mode with username: {self.analytics_api.username}")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(response.status_code == 401 and result.get(
            "errors")[0].get("code") == 20000,
            f"Assertion failed: response_code={response.status_code}, \
            error_code={result.get('errors')[0].get('code')}"
        )
        self.log.info(f"Errors: {result.get('errors')}")

    def test_different_user_status(self):
        # Create 2 users with analytics_admin role using direct REST API
        user1 = "user1"
        user2 = "user2"
        password = "password"

        rest = RestConnection(self.analytics_cluster.master)

        # Create 2 users with analytics_admin role using direct REST API
        self.log.info(f"Creating user {user1} with analytics_admin role")
        payload1 = f"password={password}&roles=analytics_admin"
        rest.add_set_builtin_user(user1, payload1)
        self.log.info(f"Successfully created user {user1}")

        self.log.info(f"Creating user {user2} with analytics_admin role")
        payload2 = f"password={password}&roles=analytics_admin"
        rest.add_set_builtin_user(user2, payload2)
        self.log.info(f"Successfully created user {user2}")

        # Submit async request as user1
        statement = 'SELECT sleep("v", 60*60*1000);'
        self.analytics_api.username = user1
        self.analytics_api.password = password
        self.log.info(
            f"Executing statement: {statement} in async mode as {user1}")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Failed to submit async request: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}, has_handle={'handle' in result}")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")

        # Get status as user1 (should work)
        self.analytics_api.username = user1
        self.analytics_api.password = password
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(status and response.status_code == 200,
                        f"User1 should be able to get status: status={status}, response_code={response.status_code}")
        self.log.info(f"User1 status check successful: {result.get('status')}")

        # Get status as user2 (should fail with 404)
        self.analytics_api.username = user2
        self.analytics_api.password = password
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(response is not None and response.status_code == 404,
                        f"User2 should not be able to get status of user1's request: "
                        f"status={status}, response_code={response.status_code if response else None}")
        self.log.info(f"User2 status check correctly returned 404 as expected")

        # Clean up users
        self.log.info(f"Cleaning up users {user1} and {user2}")
        try:
            rest.delete_builtin_user(user1)
        except Exception as e:
            self.log.warning(f"Failed to delete user {user1}: {e}")
        try:
            rest.delete_builtin_user(user2)
        except Exception as e:
            self.log.warning(f"Failed to delete user {user2}: {e}")

    def test_discard_results(self):
        # Submit async request
        statement = f'SELECT 1;'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status, "Failed to submit async request")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")
        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=60, poll_interval=1
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Discard results
        status, result, response = self.analytics_api.discard_request_result(
            request_id, handle)
        self.assertTrue(status and response.status_code == 202,
                        f"Assertion failed: status={status}, response_code={response.status_code}")

        # Fetch results
        status, result, response = self.analytics_api.get_request_result(
            request_id, handle)
        self.assertTrue(response.status_code == 404,
                        f"Assertion failed: response_code={response.status_code}"
                        )
        self.log.info(
            f"Discard result validated, request result not found for handle")

    def test_cancel_queued_request(self):
        # Submit multiple async requests
        count = 0
        while count < 100:
            statement = 'SELECT sleep("v", 60*60*1000);'
            status, result, response = self.analytics_api.submit_service_request(
                statement=statement,
                mode="async"
            )
            self.assertTrue(status, "Failed to submit async request")

            request_id = result["requestID"]
            handle = result["handle"].split("/")[-1]
            count += 1

        self.log.info(
            f"Last submitted request: {request_id}, handle: {handle}")

        # Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(
            status and result.get("status") == "queued" and
            response.status_code == 200,
            f"Assertion failed: status={status}, result_status={result.get('status')}, "
            f"response_code={response.status_code}"
        )  # and ("metrics" in result)

        # Cancel request
        status, result, response = self.analytics_api.cancel_request(
            request_id)
        self.assertTrue(status and response.status_code == 200,
                        f"Assertion failed: status={status}, response_code={response.status_code}")
        self.log.info(
            f"Cancelling queued request validated, request cancelled")

    def test_cancel_running_request(self):
        # Submit async request
        statement = f'SELECT sleep("v", 60*60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status, "Failed to submit async request")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")

        # Cancel request
        status, result, response = self.analytics_api.cancel_request(
            request_id)
        self.assertTrue(status and response.status_code == 200,
                        f"Assertion failed: status={status}, response_code={response.status_code}")

        # Fetch results
        status, result, response = self.analytics_api.get_request_result(
            request_id, handle)
        self.assertTrue(response.status_code == 404,
                        f"Assertion failed: response_code={response.status_code}")
        self.log.info(
            f"Cancelling running request validated, request result not found for handle")

    def test_invalid_handle(self):
        # Get request result with invalid handle
        status, result, response = self.analytics_api.get_request_result(
            "invalidUUID", "30-0"
        )
        self.assertTrue(response.status_code == 404,
                        f"Assertion failed: response_code={response.status_code}")
        self.log.info(f"Request result not found for invalid handle")

    def test_set_result_ttl(self):
        # Set resultTtl to 1000ms
        self.log.info("Setting resultTtl to 1000ms")
        status, result, response = self.analytics_api.update_service_config(
            {"resultTtl": 1000}
        )
        self.assertTrue(status and response.status_code == 200,
                        f"Failed to set resultTtl: status={status}, response_code={response.status_code}")
        self.log.info(f"resultTtl set successfully")

        # Restart analytics cluster
        self.log.info("Restarting analytics service")
        status, result, response = self.analytics_api.restart_analytics_service()
        self.assertTrue(status, "Failed to restart analytics service")
        self.log.info("Analytics service restarted")

        # Wait for service to be ready after restart
        self.log.info(
            "Waiting for analytics service to be ready after restart")
        time.sleep(60)

        # Submit async request
        statement = 'SELECT sleep("v", 60*60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and ("handle" in result),
                        f"Failed to submit async request: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}, has_handle={'handle' in result}")

        # Sleep for 1000ms (1 second) to let TTL expire
        self.log.info(
            "Sleeping for 10 sec (1sec + buffer) to let result TTL expire")
        time.sleep(10)

        # Fetch results - should get 404 since TTL expired
        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")
        status, result, response = self.analytics_api.get_request_result(
            request_id, handle)
        self.assertTrue(
            response.status_code == 404,
            f"Expected 404 after TTL expiry, but got: response_code={response.status_code}"
        )
        self.log.info(
            f"Results correctly returned {response.status_code} after TTL expiry")

    def test_parallel_request_execution(self):
        # Submit async requests
        statement = f'SELECT sleep("v", 60*60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status, "Failed to submit async request")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(f"Submitted request: {request_id}, handle: {handle}")

        # parallelly fetch status and cancel request
        def fetch_status():
            """Fetch request status"""
            return self.analytics_api.get_request_status(request_id, handle)

        def cancel_request():
            """Cancel the request"""
            return self.analytics_api.cancel_request(request_id)

        # Execute both operations in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            status_future = executor.submit(fetch_status)
            cancel_future = executor.submit(cancel_request)

            # Wait for both to complete
            status_result1 = status_future.result()
            cancel_result = cancel_future.result()

        # Fetch status
        status, result, response = status_result1
        self.log.info(
            f"Status = {status} ; Response Code: {response.status_code} ; Result status: {result.get('status')}")
        self.assertTrue(status and response.status_code == 200 and result.get('status') == "running",
                        f"Failed to fetch status: status={status}, response_code={response.status_code}, result_status={result.get('status')}")

        # Verify cancel result
        status, result, response = cancel_result
        self.log.info(
            f"Request cancelled: Response Code: {response.status_code}")
        self.assertTrue(status and response.status_code == 200,
                        f"Failed to cancel request: status={status}, response_code={response.status_code}")

        # Fetch status after cancellation
        time.sleep(30)
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle)
        self.log.info(
            f"Status = {status} ; Response Code:{response.status_code} ; Result status: {result.get('status')}")
        self.assertTrue(status and response.status_code == 200 and result.get('status') == "failed",
                        f"Failed to fetch status: status={status}, response_code={response.status_code}, result_status={result.get('status')}")

    def test_fetch_results_from_partition(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")
        for dataset in datasets:
            # Submit async request
            statement = f'SELECT * from {dataset.full_name}'
            self.log.info(f"Executing statement: {statement} in async mode")
            status, result, response = self.analytics_api.submit_service_request(
                statement=statement,
                mode="async"
            )
            self.assertTrue(status, "Failed to submit async request")

            # Wait for completion
            request_id = result["requestID"]
            handle = result["handle"].split("/")[-1]
            self.log.info(f"Submitted request: {request_id}, handle: {handle}")
            status, result, _ = self.analytics_api.wait_for_request_completion(
                request_id, handle, timeout=60, poll_interval=1
            )
            self.assertTrue(status, "Failed to wait for request completion")

            # Fetch results
            status, result, _ = self.analytics_api.get_request_status(
                request_id, handle)
            self.assertTrue(status, "Failed to fetch results")

            # Fetch partition results
            partitions = result.get("partitions")
            self.log.info(f"Number of partitions: {len(partitions)}")
            for i in range(len(partitions)):
                partition_handle = handle+f"/{i}"
                self.log.info(
                    f"Fetching results for partition: {request_id}/{partition_handle}")
                status, partition_result, _ = self.analytics_api.get_request_result(
                    request_id, partition_handle)
                self.assertTrue(status and len(partition_result.get("results")) > 0,
                                f"Assertion failed: status={status}, partition_result_count={len(partition_result.get('results'))}")

    def test_partition_count_match(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")
        for dataset in datasets:
            # Submit async request
            statement = f'SELECT * from {dataset.full_name}'
            self.log.info(f"Executing statement: {statement} in async mode")
            status, result, response = self.analytics_api.submit_service_request(
                statement=statement,
                mode="async"
            )
            self.assertTrue(status, "Failed to submit async request")

            # Wait for completion
            request_id = result["requestID"]
            handle = result["handle"].split("/")[-1]
            self.log.info(f"Submitted request: {request_id}, handle: {handle}")
            status, result, _ = self.analytics_api.wait_for_request_completion(
                request_id, handle, timeout=60, poll_interval=1
            )
            self.assertTrue(status, "Failed to wait for request completion")

            # Fetch results
            status, result, _ = self.analytics_api.get_request_status(
                request_id, handle)
            self.assertTrue(status, "Failed to fetch results")

            # Fetch partition resultCount
            partitions = result.get("partitions")
            self.log.info(f"Number of partitions: {len(partitions)}")
            totalExpectedResultCount = self.initial_doc_count
            totalActualResultCount = 0
            for i, partition in enumerate(partitions):
                resultCount = partition.get("resultCount")
                self.log.info(f"Result count for partition {i}: {resultCount}")
                totalActualResultCount += resultCount
            self.log.info(
                f"resultCount: {totalExpectedResultCount}, sum of resultCount for all partitions: {totalActualResultCount}")
            self.assertEqual(totalExpectedResultCount, totalActualResultCount,
                             f"resultCount: {totalExpectedResultCount}, sum of resultCount for all partitions: {totalActualResultCount}")

    def test_results_ordered(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")
        for dataset in datasets:
            # run ANALYZE COLLECTION
            cboutil = CBOUtil()
            cboutil.create_sample_for_analytics_collections(
                self.columnar_cluster, dataset.name, sample_size="high", analytics=False)

            statement = f"SET `compiler.sort.parallel` \"true\"; select price from {dataset.full_name} order by price;"
            self.log.info(f"Executing statement: {statement} in async mode")
            status, result, response = self.analytics_api.submit_service_request(
                statement=statement,
                mode="async"
            )
            self.assertTrue(status and result.get("status") == "queued" and
                            response.status_code == 200 and (
                                "handle" in result),
                            f"Assertion failed: status={status}, response_code={response.status_code}, has_handle={'handle' in result}"
                            )

            # Wait for completion
            request_id = result["requestID"]
            handle = result["handle"].split("/")[-1]
            self.log.info(f"Submitted request: {request_id}, handle: {handle}")
            status, result, _ = self.analytics_api.wait_for_request_completion(
                request_id, handle
            )
            self.assertTrue(status, "Failed to wait for request completion")

            # Get request result
            status, result, response = self.analytics_api.get_request_status(
                request_id, handle)
            self.assertTrue(status and response.status_code == 200 and result.get("status") == "success" and
                            result.get("resultCount") > 0 and result.get(
                                "resultSetOrdered") == True,
                            f"Assertion failed: status={status}, response_code={response.status_code}, result_status={result.get('status')}, \
                                result_count={result.get('resultCount')}, result_set_ordered={result.get('resultSetOrdered')}"
                            )
            self.log.info(
                f"Test status: {result.get('status')} ; Result set ordered: {result.get('resultSetOrdered')}")

    def test_async_copy_to_blob_storage(self):
        """
        1. create destination s3bucket
        2. create standalone collection and s3link
        3. load data into standalone collection
        4. copy data to blob storage using async COPY TO statement
        5. wait for completion
        6. fetch status
        """
        # Step 1: Create destination S3 bucket
        self.sink_blob_bucket_name = "copy-to-blob-async-" + \
            str(random.randint(1, 100000))

        self.log.info("Creating S3 bucket: {} at region {}".format(
            self.sink_blob_bucket_name, self.aws_region))

        self.sink_bucket_created = perform_S3_operation(
            aws_access_key=self.aws_access_key,
            aws_secret_key=self.aws_secret_key,
            create_bucket=True, bucket_name=self.sink_blob_bucket_name,
            region=self.aws_region)

        if not self.sink_bucket_created:
            self.fail("Failed to create S3 bucket")

        # Step 2: Create standalone collection and S3 link
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=None,
            external_collection_file_formats=[self.file_format])

        # Set primary key for standalone dataset
        if self.file_format == "parquet":
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"`name=id`": "string"}]
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"id": "string"}]

        # Create entities on columnar cluster
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[])
        if not result:
            self.fail(str(msg))

        # Get standalone datasets
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        if not datasets:
            self.fail("No standalone datasets created")

        # Step 3: Load data into standalone collection
        self.log.info(
            "Adding {} documents in standalone dataset".format(self.initial_doc_count))

        # Set sdk_clients_per_user if not already set
        if not hasattr(self, 'sdk_clients_per_user'):
            self.sdk_clients_per_user = self.input.param(
                "sdk_clients_per_user", 1)

        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Some documents were not inserted")

        # Get S3 link
        blob_storage_link = self.cbas_util.get_all_link_objs("s3")
        if not blob_storage_link:
            self.fail(
                "No S3 link found. Ensure external_link is configured in the spec.")
        blob_storage_link = blob_storage_link[0]

        # Step 4: Copy data to blob storage using async COPY TO statement
        dataset = datasets[0]
        path = "copy_dataset_async_0"

        # Generate COPY TO command
        copy_to_cmd = self.cbas_util.generate_copy_to_s3_cmd(
            collection_name=dataset.name,
            dataverse_name=dataset.dataverse_name,
            database_name=dataset.database_name,
            destination_bucket=self.sink_blob_bucket_name,
            destination_link_name=blob_storage_link.full_name,
            path=path,
            file_format=self.file_format
        )

        self.log.info(
            "Executing COPY TO statement in async mode: {}".format(copy_to_cmd))

        # Submit async request
        status, result, response = self.analytics_api.submit_service_request(
            statement=copy_to_cmd,
            mode="async"
        )

        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Failed to submit async COPY TO request: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}, "
                        f"has_handle={'handle' in result}")

        # Step 5: Wait for completion
        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info("Submitted async COPY TO request: request_id={}, handle={}".format(
            request_id, handle))

        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=3600, poll_interval=5
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Step 6: Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(status and result.get("status") == "success" and
                        response.status_code == 200,
                        f"COPY TO request failed: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}")

        self.log.info("Async COPY TO completed successfully. Status: {}".format(
            result.get("status")))

        # Cleanup: Delete S3 bucket
        try:
            if hasattr(self, 'sink_blob_bucket_name') and self.sink_blob_bucket_name:
                self.log.info("Emptying S3 bucket: {}".format(
                    self.sink_blob_bucket_name))
                perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    empty_bucket=True,
                    bucket_name=self.sink_blob_bucket_name,
                    region=self.aws_region,
                    endpoint_url=self.aws_endpoint)

                self.log.info("Deleting S3 bucket: {}".format(
                    self.sink_blob_bucket_name))
                perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    delete_bucket=True,
                    bucket_name=self.sink_blob_bucket_name,
                    region=self.aws_region,
                    endpoint_url=self.aws_endpoint)
        except Exception as e:
            self.log.warning("Failed to cleanup S3 bucket: {}".format(str(e)))

    def test_async_copy_into_standalone_collection(self):
        """
        1. create standalone collection and s3link
        2. copy into standalone collection from s3
        3. wait for completion with timeout
        4. fetch status
        """
        # Step 1: Create standalone collection and S3 link
        # Get file format parameter
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=None,
            external_collection_file_formats=[self.file_format])

        # Set primary key for standalone dataset
        if self.file_format == "parquet":
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"`name=id`": "string"}]
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"id": "string"}]

        # Copy standalone collection properties from external dataset
        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        # Create entities on columnar cluster
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[])
        if not result:
            self.fail(str(msg))

        # Get standalone dataset
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        if not datasets:
            self.fail("No standalone datasets created")

        # Get S3 link
        blob_storage_link = self.cbas_util.get_all_link_objs("s3")
        if not blob_storage_link:
            self.fail(
                "No S3 link found. Ensure external_link is configured in the spec.")
        blob_storage_link = blob_storage_link[0]

        # Step 2: Generate COPY INTO command and submit as async request
        dataset = datasets[0]
        s3_bucket_name = self.input.param("s3_source_bucket", "")
        if not s3_bucket_name and hasattr(self, 's3_source_bucket'):
            s3_bucket_name = self.s3_source_bucket
        if not s3_bucket_name:
            self.fail(
                "S3 source bucket name is required. Set s3_source_bucket parameter.")

        # Get dataset properties for COPY INTO parameters
        dataset_properties = dataset.dataset_properties if hasattr(
            dataset, 'dataset_properties') else {}
        link_name = dataset.link_name if hasattr(
            dataset, 'link_name') else blob_storage_link.name

        # Extract COPY INTO parameters with defaults
        copy_params = {
            "files_to_include": dataset_properties.get("include", f"*.{self.file_format}"),
            "file_format": dataset_properties.get("file_format", self.file_format),
            "type_parsing_info": dataset_properties.get("object_construction_def", ""),
            "path_on_aws_bucket": dataset_properties.get("path_on_external_container", ""),
            "header": dataset_properties.get("header", None),
            "null_string": dataset_properties.get("null_string", None),
            "files_to_exclude": dataset_properties.get("exclude", []),
            "parse_json_string": dataset_properties.get("parse_json_string", 0),
            "convert_decimal_to_double": dataset_properties.get("convert_decimal_to_double", 0),
            "timezone": dataset_properties.get("timezone", "")
        }

        # Generate COPY INTO command
        copy_into_cmd = self.cbas_util.generate_copy_from_cmd(
            collection_name=dataset.name,
            aws_bucket_name=s3_bucket_name,
            external_link_name=link_name,
            dataverse_name=dataset.dataverse_name,
            database_name=dataset.database_name,
            **copy_params
        )

        self.log.info(
            "Executing COPY INTO statement in async mode: {}".format(copy_into_cmd))

        # Submit async request
        status, result, response = self.analytics_api.submit_service_request(
            statement=copy_into_cmd,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Failed to submit async COPY INTO request: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}, "
                        f"has_handle={'handle' in result}")

        # Step 4: Wait for completion with timeout
        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        timeout = self.input.param("timeout", 300)
        poll_interval = self.input.param("poll_interval", 5)
        self.log.info("Submitted async COPY INTO request: request_id={}, handle={}, timeout={}".format(
            request_id, handle, timeout))

        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=timeout, poll_interval=poll_interval
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Step 5: Fetch status
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(status and result.get("status") == "success" and
                        response.status_code == 200,
                        f"COPY INTO request failed: status={status}, result_status={result.get('status')}, "
                        f"response_code={response.status_code}")

        self.log.info("Async COPY INTO completed successfully. Status: {}".format(
            result.get("status")))

        # Validate document count
        validate_doc_count = self.input.param("validate_doc_count", True)
        if validate_doc_count:
            expected_doc_count = 1001
            actual_doc_count = self.cbas_util.get_num_items_in_cbas_dataset(
                self.columnar_cluster, dataset.full_name, timeout=3600, analytics_timeout=3600)
            if not actual_doc_count == expected_doc_count:
                self.fail("Expected doc count {0}. Actual doc "
                          "count {1}".format(expected_doc_count, actual_doc_count))
            self.log.info(
                "Document count validation passed: {}".format(expected_doc_count))

    def test_query_rebalance(self):
        """
        1. create 2 node cluster
        2. trigger async request
        3. add another node and rebalance
        4. wait for rebalance to complete
        5. wait for query to complete
        6. verify status
        """
        # Step 1: Verify we have a 2-node cluster (should be configured via nodes_init parameter)
        if not hasattr(self, 'analytics_cluster') or not self.analytics_cluster:
            self.fail("Analytics cluster not initialized")

        initial_node_count = len(self.analytics_cluster.nodes_in_cluster)
        self.log.info(
            f"Starting with {initial_node_count} nodes in analytics cluster")

        # Verify that we have exactly 2 nodes as expected
        self.assertEqual(initial_node_count, 2,
                         f"Expected 2-node cluster but found {initial_node_count} nodes. "
                         f"Check that nodes_init=2 is set in the conf file and not being overridden.")

        # Step 2: Trigger async request (use a long-running query that will complete after rebalance)
        # 5 minute query - long enough for rebalance, but will complete
        statement = 'SELECT sleep("v", 5*60*1000);'
        self.log.info(f"Executing statement: {statement} in async mode")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Failed to submit async request: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}, "
                        f"has_handle={'handle' in result}")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(
            f"Submitted async request: request_id={request_id}, handle={handle}")

        # Step 3: Add another node and rebalance (only for onprem-columnar)
        if runtype == "onprem-columnar" and hasattr(self, 'rebalance_util'):
            self.log.info(
                "Adding a Columnar node to the analytics cluster and rebalancing")
            rebalance_task, self.analytics_cluster.available_servers = self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_in=1,
                available_servers=self.analytics_cluster.available_servers,
                in_node_services="kv,cbas", wait_for_complete=True)

            if not rebalance_task.result:
                self.fail(
                    "Error while rebalance-In Columnar node in analytics cluster")

            final_node_count = len(self.analytics_cluster.nodes_in_cluster)
            self.log.info(
                f"Rebalance completed. Cluster now has {final_node_count} nodes")
        else:
            self.log.warning(
                "Skipping rebalance step - not onprem-columnar or rebalance_util not available")

        # Step 5: Wait for query to complete
        self.log.info("Waiting for async query to complete")
        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=3600, poll_interval=5
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Step 6: Verify status
        self.log.info("Fetching final request status")
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(status and result.get("status") == "success" and
                        response.status_code == 200,
                        f"Query failed after rebalance: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}")

        self.log.info(
            f"Query completed successfully after rebalance. Status: {result.get('status')}")

    def test_rebalance_query(self):
        """
        1. create 2 node cluster
        2. add another node and rebalance
        3. when rebalance is running, trigger async request
        4. wait for rebalance to complete
        5. wait for query to complete
        6. verify status
        """
        # Step 1: Verify we have a 2-node cluster (should be configured via nodes_init parameter)
        if not hasattr(self, 'analytics_cluster') or not self.analytics_cluster:
            self.fail("Analytics cluster not initialized")

        initial_node_count = len(self.analytics_cluster.nodes_in_cluster)
        self.log.info(
            f"Starting with {initial_node_count} nodes in analytics cluster")

        # Verify that we have exactly 2 nodes as expected
        self.assertEqual(initial_node_count, 2,
                         f"Expected 2-node cluster but found {initial_node_count} nodes. "
                         f"Check that nodes_init=2 is set in the conf file and not being overridden.")

        # Required for rebalance to run for 5 minutes
        # 3 minute query so that rebalance can run for 3 minutes
        statement = 'SELECT sleep("v", 3*60*1000);'
        self.log.info(
            f"Executing statement: {statement} in async mode so that rebalance can run for 3 minutes")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Failed to submit async request: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}, "
                        f"has_handle={'handle' in result}")

        # Step 2: Add another node and start rebalance (without waiting for completion)
        rebalance_task = None
        if runtype == "onprem-columnar" and hasattr(self, 'rebalance_util'):
            self.log.info(
                "Adding a Columnar node to the analytics cluster and starting rebalance")
            rebalance_task, self.analytics_cluster.available_servers = self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_in=1,
                available_servers=self.analytics_cluster.available_servers,
                in_node_services="kv,cbas",
                wait_for_complete=False)  # Start rebalance but don't wait

            self.log.info(
                "Rebalance started, waiting a moment for it to begin")
        else:
            self.log.warning(
                "Skipping rebalance step - not onprem-columnar or rebalance_util not available")

        self.log.info(
            "Sleeping for 90 seconds to give rebalance a moment to start")
        time.sleep(90)
        # Step 3: Trigger async request while rebalance is running
        # 10 minute query - long enough for rebalance, but will complete
        statement = 'SELECT sleep("v", 2*60*1000);'
        self.log.info(
            f"Executing statement: {statement} in async mode (while rebalance is running)")
        status, result, response = self.analytics_api.submit_service_request(
            statement=statement,
            mode="async"
        )
        self.assertTrue(status and result.get("status") == "queued" and
                        response.status_code == 200 and "handle" in result,
                        f"Failed to submit async request: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}, "
                        f"has_handle={'handle' in result}")

        request_id = result["requestID"]
        handle = result["handle"].split("/")[-1]
        self.log.info(
            f"Submitted async request: request_id={request_id}, handle={handle}")

        # Step 5: Wait for query to complete
        self.log.info("Waiting for async query to complete")
        status, result, _ = self.analytics_api.wait_for_request_completion(
            request_id, handle, timeout=3600, poll_interval=5
        )
        self.assertTrue(status, "Failed to wait for request completion")

        # Step 6: Verify status
        self.log.info("Fetching final request status")
        status, result, response = self.analytics_api.get_request_status(
            request_id, handle
        )
        self.assertTrue(status and result.get("status") == "success" and
                        response.status_code == 200,
                        f"Query failed after rebalance: status={status}, "
                        f"result_status={result.get('status')}, response_code={response.status_code}")

        self.log.info(
            f"Query completed successfully after rebalance. Status: {result.get('status')}")
