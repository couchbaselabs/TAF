import random
import string
from queue import Queue

from table_view import TableView
from security_utils.x509main import x509main
from cbas_utils.cbas_utils_columnar import RBAC_Util as ColumnarRBACUtil
from awsLib.s3_data_helper import perform_S3_operation
from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from cb_server_rest_util.connection import CBRestConnection


class ColumnarRolesOnPrem(ColumnarOnPremBase):
    def setUp(self):
        super(ColumnarRolesOnPrem, self).setUp()
        self.columnar_rbac_util = ColumnarRBACUtil(
            self.task, self.use_sdk_for_cbas)
        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)

        self.columnar_spec_name = self.input.param(
            "columnar_spec_name", "regressions.copy_to_s3")
        self.columnar_spec = self.columnar_cbas_utils.get_columnar_spec(
            self.columnar_spec_name)
        self.rest_connection = CBRestConnection()

        for cluster_name, cluster in self.cb_clusters.items():
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.remote_cluster = cluster

        self.ACCESS_DENIED_ERR = "Insufficient permissions or the requested object does not exist"

        self.aws_region = self.input.param("aws_bucket_region", self.input.param("aws_region", "us-east-1"))
        self.s3_source_bucket = "columnar-functional-sanity-test-data"
        self.sink_s3_bucket_name = None

        self.rest_api_port = "8095"
        self.admin_rest_api_port = "8091"
        self.base_url = "http://{}:".format(self.analytics_cluster.master.ip)
        self.get_active_request_api = self.base_url + self.rest_api_port + "/api/v1/active_requests"
        self.get_completed_request_api = self.base_url + self.rest_api_port + "/api/v1/completed_requests"
        self.get_service_status_api = self.base_url + self.rest_api_port + "/api/v1/status/service"
        self.get_ingestion_status_api = self.base_url + self.rest_api_port + "/api/v1/status/ingestion"
        self.restart_service_api = self.base_url + self.rest_api_port + "/api/v1/service/restart"
        self.restart_node_api = self.base_url + self.rest_api_port + "/api/v1/node/restart"
        self.get_health_check_api = self.base_url + self.rest_api_port + "/api/v1/health"
        self.service_level_parameter = self.base_url + self.rest_api_port + "/api/v1/config/service"
        self.node_level_parameter = self.base_url + self.rest_api_port + "/api/v1/config/node"
        self.link_api = self.base_url + self.rest_api_port + "/api/v1/link/"
        self.multiple_links_api = self.base_url + self.rest_api_port + "/api/v1/link"
        self.request_api = self.base_url + self.rest_api_port + "/api/v1/request"
        self.settings_api = self.base_url + self.admin_rest_api_port + "/settings/analytics"

    def create_user_for_server(self, username="temp_user", password="password", role=""):
        self.log.info("Creating user: {}".format(username))
        result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                     username,
                                                     username,
                                                     password,
                                                     roles=role)
        if not result:
            self.fail("Failed to create user - {}".format(username))

        return result

    def generate_random_password(self, length=12):
        """Generate a random password."""
        password_characters = string.ascii_letters + string.digits
        password = ''.join(random.choice(password_characters) for i in range(length))
        return password

    def generate_random_entity_name(self, length=5, type="database"):
        """Generate random database name."""
        base_name = "TAF-" + type
        entity_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
        entity_name = base_name + "-" + entity_id
        return entity_name

    def test_get_active_requests(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info("Making request to url - GET {}, Role - {}".format(self.get_active_request_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.get_active_request_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Failed to get the active request for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info("Making request to url - GET {}, Role - {}".format(self.get_active_request_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.get_active_request_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Able to get the active request for user - {}, Reason - {}".format(username, response.content))

    def test_get_completed_requests(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info("Making request to url - GET {}, Role - {}".format(self.get_completed_request_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.get_completed_request_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Failed to get the completed request for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.get_completed_request_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.get_completed_request_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Failed to get the completed request for user - {}, Reason - {}".format(username, response.content))

    def test_get_service_status(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info("Making request to url - GET {}, Role - {}".format(self.get_service_status_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.get_service_status_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Failed to get the service status for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.get_service_status_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.get_service_status_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail("Able to get the service status for user - {}, Reason - {}".format(username, response.content))

    def test_get_ingestion_status(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.get_ingestion_status_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.get_ingestion_status_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Failed to get the ingestion status for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.get_ingestion_status_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.get_ingestion_status_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Able to get the ingestion status for user - {}, Reason - {}".format(username, response.content))

    def test_health_check(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.get_health_check_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.get_health_check_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 204:
            self.fail("Failed to get the health status for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.get_health_check_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.get_health_check_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 204:
            self.fail("Not able to get the health status for user - {}, Reason - {}".format(username, response.content))

    def test_post_service_restart(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.restart_service_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.restart_service_api, "POST", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail("Able to restart the service for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.restart_service_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.restart_service_api, "POST", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 202:
            self.fail("Failed to restart the service for user - {}, Reason - {}".format(username, response.content))

    def test_post_node_restart(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.restart_node_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.restart_node_api, "POST", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 202:
            self.fail("Failed to restart node for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.restart_node_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.restart_node_api, "POST", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail("Able to restart node for user - {}, Reason - {}".format(username, response.content))

    def test_get_service_level_parameter(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.service_level_parameter, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.service_level_parameter, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail("Failed to get service level parameter for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.service_level_parameter, "analytics_access"))
        status, content, response = self.rest_connection.request(self.service_level_parameter, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail("Able to get service level parameter for user - {}, Reason - {}".format(username, response.content))

    def test_put_service_level_parameter(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - PUT {}, Role - {}".format(self.service_level_parameter, "analytics_admin"))
        params = {
                "analyticsHttpRequestQueueSize": 256,
                "analyticsHttpThreadCount": 16,
                "cloudAccessPreemptiveRefreshIntervalSeconds": 15,
                "cloudAccessRefreshHaltTimeoutSeconds": 120,
                "cloudAccessTtlSeconds": 15,
                "cloudMaxReadRequestsPerSecond": 1500,
                "cloudMaxWriteRequestsPerSecond": 250,
                "cloudRequestsHttpConnectionAcquireTimeout": 120,
                "cloudRequestsMaxHttpConnections": 1000,
                "cloudRequestsMaxPendingHttpConnections": 10000,
                "cloudStorageAllocationPercentage": 0.8,
                "cloudStorageDiskMonitorInterval": 120,
                "cloudStorageIndexInactiveDurationThreshold": 360,
                "cloudStorageSweepThresholdPercentage": 0.9,
                "cloudWriteBufferSize": 8388608,
                "compilerCbo": True,
                "compilerColumnFilter": True,
                "compilerCopyToWriteBufferSize": 8388608,
                "compilerExternalFieldPushdown": True,
                "compilerFramesize": 32768,
                "compilerGroupmemory": 33554432,
                "compilerJoinmemory": 33554432,
                "compilerRuntimeMemoryOverhead": 5,
                "compilerSortmemory": 33554432,
                "compilerWindowmemory": 33554432,
                "copyToKvBucketWaitUntilReadyTimeout": 30,
                "coresMultiplier": 3,
                "dcpChannelConnectAttemptTimeout": 120,
                "dcpChannelConnectTotalTimeout": 480,
                "dcpConnectionBufferSize": 1048576,
                "jobQueueCapacity": 4096,
                "jvmArgs": None,
                "kafkaMaxFetchBytes": 4194304,
                "maxRedirectsRemoteLink": 10,
                "maxWebRequestSize": 209715200,
                "rebalanceEjectDelaySeconds": 0,
                "remoteLinkConnectTimeoutSeconds": 30,
                "remoteLinkRefreshAuthSeconds": 0,
                "remoteLinkSocketTimeoutSeconds": 60,
                "remoteLinkValidationMaxRetries": 0,
                "remoteStorageSizeMetricTtlMillis": 300000,
                "requestsArchiveSize": 1000,
                "resultTtl": 86400000,
                "storageFormat": "column",
                "storageMaxConcurrentFlushesPerPartition": 1,
                "storageMaxConcurrentMergesPerPartition": 1,
                "storageMaxScheduledMergesPerPartition": 8,
                "storageMemorycomponentMaxScheduledFlushes": 0,
                "txnDatasetCheckpointInterval": 3600
        }
        status, content, response = self.rest_connection.request(self.service_level_parameter, "PUT", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to modify service level parameter for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - PUT {}, Role - {}".format(self.service_level_parameter, "analytics_access"))
        status, content, response = self.rest_connection.request(self.service_level_parameter, "PUT", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail("Able to modify service level parameter for user - {}, Reason - {}".format(username, response.content))

    def test_get_node_level_parameter(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.node_level_parameter, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.node_level_parameter, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to get node level parameter for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.node_level_parameter, "analytics_access"))
        status, content, response = self.rest_connection.request(self.node_level_parameter, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to get node level parameter for user - {}, Reason - {}".format(username, response.content))

    def test_put_node_level_parameter(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - PUT {}, Role - {}".format(self.node_level_parameter, "analytics_admin"))
        params = {
              "jvmArgs": None,
              "storageBuffercacheSize": 0,
              "storageMemorycomponentGlobalbudget": 0
            }
        status, content, response = self.rest_connection.request(self.node_level_parameter, "PUT", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to modify node level parameter for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - PUT {}, Role - {}".format(self. node_level_parameter, "analytics_access"))
        status, content, response = self.rest_connection.request(self.node_level_parameter, "PUT", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to modify node level parameter for user - {}, Reason - {}".format(username, response.content))

    def test_create_new_link(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        link_name = "test_link"

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        params = {
            "name": "test_link",
            "type": "s3",
            "encryption": "none",
            "accessKeyId": "asdsadasdas",
            "secretAccessKey": "asdasdsad",
            "region": "us-east-1"
        }

        self.link_api = self.link_api + link_name
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to create new link for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.link_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.link_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to create new link for user - {}, Reason - {}".format(username, response.content))

    def test_get_single_link(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        link_name = "test_link"

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        params = {
            "name": "test_link",
            "type": "s3",
            "encryption": "none",
            "accessKeyId": "asdsadasdas",
            "secretAccessKey": "asdasdsad",
            "region": "us-east-1"
        }

        self.link_api = self.link_api + link_name
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to get link details for user - {}, Reason - {}".format(username, response.content))

        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to get link details for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.link_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.link_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to get link details for user - {}, Reason - {}".format(username, response.content))

    def test_put_single_link(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        link_name = "test_link"
        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        params = {
            "name": "test_link",
            "type": "s3",
            "encryption": "none",
            "accessKeyId": "asdsadasdas",
            "secretAccessKey": "asdasdsad",
            "region": "us-east-1"
        }

        self.link_api = self.link_api + link_name
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to create single link for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        params = {
            "name": "test_link",
            "type": "s3",
            "encryption": "none",
            "accessKeyId": "adasdadaddd",
            "secretAccessKey": "adadadddddd",
            "region": "us-east-1"
        }
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - PUT {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "PUT", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to modify single link for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - PUT {}, Role - {}".format(self.link_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.link_api, "PUT", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to modify single link for user - {}, Reason - {}".format(username, response.content))

    def test_delete_single_link(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        link_name = "test_link"
        self.link_api = self.link_api + link_name

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        params = {
            "name": "test_link",
            "type": "s3",
            "encryption": "none",
            "accessKeyId": "adasdadaddd",
            "secretAccessKey": "adadadddddd",
            "region": "us-east-1"
        }
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "POST", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to create link for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - DELETE {}, Role - {}".format(self.link_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.link_api, "DELETE", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 500:
            self.fail(
                "Able to delete link for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - DELETE {}, Role - {}".format(self.restart_node_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "DELETE", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to delete single link for user - {}, Reason - {}".format(username, response.content))

    def test_get_multiple_links(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        link_name = "test_link"

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        params = {
            "name": "test_link",
            "type": "s3",
            "encryption": "none",
            "accessKeyId": "asdsadasdas",
            "secretAccessKey": "asdasdsad",
            "region": "us-east-1"
        }

        self.link_api = self.link_api + link_name
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.link_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.link_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to get multiple links for user - {}, Reason - {}".format(username, response.content))

        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.multiple_links_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.multiple_links_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to get multiple links for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")
        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.multiple_links_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.multiple_links_api, "GET", headers=headers)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Not able to get multiple links for user - {}, Reason - {}".format(username, response.content))

    def test_request_api(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        params = {
            "statement": "SELECT * FROM `Metadata`.`AssignedRole`;",
            "scan_consistency": "not_bounded",
            "client_context_id": "bb32faa4-f387-47da-8c08-bf58f6a415f9",
            "optimized-logical-plan": True,
            "plan-format": "json",
            "max-warnings": 10,
            "query_context": "default:`Default`.`Default`",
            "source": "query_editor"
        }

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.request_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.request_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to send request for user - {}, Reason - {}".format(username, response.content))

        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.request_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.request_api, "GET", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Failed to get request for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.request_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.request_api, "POST", headers=headers, params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Not able to send request for user - {}, Reason - {}".format(username, response.content))

        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.request_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.request_api, "GET", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 200:
            self.fail(
                "Not able to get request for user - {}, Reason - {}".format(username, response.content))

    def test_setting_analytics(self):
        self.log.info("Testing ns server roles for Columnar Rest APIs")

        params = {
            "blobStorageScheme": "s3",
            "blobStorageBucket": "test-bucket",
            "blobStorageRegion": "us-east-1",
            "blobStoragePrefix": "analytics-data/",
            "blobStorageAnonymousAuth": True,
            "blobStorageForcePathStyle": True,
            "numStoragePartitions": 128
        }

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_admin")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.settings_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.settings_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Failed to modify analytics settings for user - {}, Reason - {}".format(username, response.content))

        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.settings_api, "analytics_admin"))
        status, content, response = self.rest_connection.request(self.settings_api, "GET", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Failed to get analytics settings for user - {}, Reason - {}".format(username, response.content))

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        self.create_user_for_server(username=username, password=password, role="analytics_access")

        headers = self.rest_connection.create_headers(username, password)
        self.log.info(
            "Making request to url - POST {}, Role - {}".format(self.settings_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.settings_api, "POST", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to get service level parameter for user - {}, Reason - {}".format(username, response.content))

        self.log.info(
            "Making request to url - GET {}, Role - {}".format(self.settings_api, "analytics_access"))
        status, content, response = self.rest_connection.request(self.settings_api, "GET", headers=headers,
                                                                 params=params)
        self.log.info("The response is - {}, {}, {}".format(status, content, response))
        if response.status_code != 403:
            self.fail(
                "Able to get service level parameter for user - {}, Reason - {}".format(username, response.content))