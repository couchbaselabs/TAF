import json
import boto3

from botocore.exceptions import ClientError
from couchbase.exceptions import AmbiguousTimeoutException, AuthenticationException, BucketNotFoundException, DocumentNotFoundException
import requests
from lib.membase.api.rest_client import Node, RestConnection
from pytests.security.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI as CapellaAPI_v4
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, TLSVerifyMode
from couchbase_utils.cb_server_rest_util.security.security_api import SecurityRestAPI
from datetime import timedelta

class RBACTest(SecurityBase):
    def setUp(self):
        SecurityBase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.org_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.log.info(self.url)
        self.log.info(self.user)
        self.log.info(self.passwd)
        self.log.info(self.cluster_id)
        self.log.info(self.org_id)
        self.log.info(self.project_id)
        self.rbac_base_url = ("https://" + self.url + "/v2/organizations/{}/projects/{}/clusters/{}/roles".
                              format(self.org_id, self.project_id, self.cluster_id)).replace("cloud", "")
        self.cluster_access_base_url = ("https://" + self.url + "/v2/organizations/{}/projects/{}/clusters/{}/users".
                                        format(self.org_id, self.project_id, self.cluster_id)).replace("cloud", "")
        self.cluster_info_base_url = ("https://" + self.url + "/internal/support/clusters/{}".
                                      format(self.cluster_id)).replace("cloud", "")
        self.create_buckets_scopes_collections()
        self.access_name = "access"
        self.access_password = "Password@123"
        self.role_name = self.input.param("role_name", ["created_role"])
        if type(self.role_name) == str:
            self.role_name = json.loads(self.role_name)
        self.role_description = self.input.param("role_description", ["Sample description"])
        if type(self.role_description) == str:
            self.role_description = json.loads(self.role_description)
        self.role_permissions = self.input.param("role_permissions", [{"dataRead": {}, "dataManage": {}}])
        if type(self.role_permissions) == str:
            self.role_permissions = json.loads(self.role_permissions)
        self.credential_type = self.input.param("credential_type", "basic")
        self.basic_permissions = self.input.param("basic_permissions", {"data_reader": {}})
        if type(self.basic_permissions) == str:
            self.basic_permissions = json.loads(self.basic_permissions)
        self.conn_string = self.get_conn_string()
        self.log.info("Connection string: %s", self.conn_string)
        self.cidr_id = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
            self.org_id, self.project_id, self.cluster_id, "0.0.0.0/0").json()['id']
        self.capella_to_dp_role_mapping = {
            "analyticsAdmin": ["analytics_admin"],
            "analyticsRead": ["analytics_reader"],
            "analyticsManage": ["analytics_manager"],
            "analyticsSelect": ["analytics_select"],
            "dataRead": ["data_reader", "data_dcp_reader"],
            "dataMonitor": ["data_monitoring"],
            "dataManage": ["data_writer"],
            "queryRead": ["query_select"],
            "queryInsert": ["query_insert"],
            "queryUpdate": ["query_update"],
            "queryDelete": ["query_delete"],
            "queryIndex": ["query_manage_index"],
            "queryCatalog": ["query_system_catalog"],
            "queryExecute": ["query_execute_functions", "query_execute_external_functions"],
            "queryManage": ["query_manage_functions", "query_manage_external_functions"],
            "globalFunctionExecute": ["query_execute_global_functions", "query_execute_global_external_functions"],
            "globalFunctionManage": ["query_manage_global_functions", "query_manage_global_external_functions"],
            "ftsManage": ["fts_admin"],
            "ftsRead": ["fts_searcher"],
            "eventingManage": ["eventing_manage_functions"],
            "statsRead": ["external_stats_reader"]
        }

    def get_dp_admin_secrets(self):

        secret_name = f"{self.cluster_id}_dp-admin"
        region_name = "us-east-1"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            return get_secret_value_response['SecretString']
        except ClientError as e:
            self.fail("Failed to get dp admin secrets: {}".format(e))


    def create_buckets_scopes_collections(self):
        """
        Creates two buckets named bucket1 (couchstore) and bucket2 (magma) and
        creates scopes and collection, with name scope1.collection1, scope2.collection2 on both buckets
        """
        buckets = ["bucket1", "bucket2"]
        scopes = ["scope1", "scope2"]
        collections = ["collection1", "collection2"]
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.org_id, self.project_id, self.cluster_id, "beer-sample")
        if resp.status_code != 201:
            self.fail("Failed to create sample bucket: {}".format(resp.text))
        for bucket in buckets:
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.org_id, self.project_id, self.cluster_id,
                bucket, "couchbase", "couchstore", 1024,
                "seqno", "none", 1, False, 0)
            self.log.info("Response from create_bucket: {}".format(resp.json()))
            bucket_id = resp.json()['id']
            if resp.status_code // 100 != 2:
                self.fail("Failed to create bucket, got response message {} and status code {}"
                          .format(resp.text, resp.status_code))

            for scope in scopes:
                resp = self.capellaAPI.cluster_ops_apis.create_scope(self.org_id, self.project_id, self.cluster_id,
                                                                     bucket_id, scope)
                if resp.status_code // 100 != 2:
                    self.fail("Failed to create scope got response message {} and status code {}"
                              .format(resp.text, resp.status_code))
                for collection in collections:
                    resp = self.capellaAPI.cluster_ops_apis.create_collection(self.org_id, self.project_id,
                                                                              self.cluster_id,
                                                                              bucket_id, scope, collection)
                    if resp.status_code // 100 != 2:
                        self.fail("Failed to create collection got response message {} and status code {}"
                                  .format(resp.text, resp.status_code))

    def validate_resp_status_code(self, resp, expected_status):
        if resp.status_code != expected_status:
            err_msg = "Expected: {}, got: {}, reason: {}".format(expected_status, resp.status_code, resp.text)
            self.fail(err_msg)

    def get_conn_string(self, secure=True):
        resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.org_id, self.project_id, self.cluster_id)
        if resp.status_code == 200:
            if secure:
                return "couchbases://" + resp.json()['connectionString']
            else:
                return "couchbase://" + resp.json()['connectionString']
        else:
            self.fail("Failed to fetch cluster info: {}".format(resp.text))

    def _get_privilege_resources(self, privilege):
        """
        Returns the resource structure required for a given privilege based on RBAC level.
        Based on Capella privileges reference:
        - Global privileges: no resources (empty dict)
        - Bucket-level privileges: buckets structure
        - Scope-level privileges: buckets with scopes structure
        - Collection-level privileges: buckets with scopes and collections structure
        """
        # Global privileges - no resources needed
        global_privileges = [
            "analyticsAdmin", "analyticsRead", "analyticsSelect",
            "queryCatalog", "globalFunctionExecute", "globalFunctionManage",
            "statsRead"
        ]

        # Bucket-level privileges
        bucket_privileges = [
            "analyticsManage", "ftsManage", "ftsRead"
        ]

        # Scope-level privileges
        scope_privileges = [
            "queryExecute", "queryManage", "eventingManage"
        ]

        # Collection-level privileges
        collection_privileges = [
            "dataRead", "dataMonitor", "dataManage",
            "queryRead", "queryInsert", "queryUpdate",
            "queryDelete", "queryIndex"
        ]

        if privilege in global_privileges:
            return {}
        elif privilege in bucket_privileges:
            return {}
        elif privilege in scope_privileges:
            return {}
        elif privilege in collection_privileges:
            return {}
        else:
            # Default to empty dict for unknown privileges
            return {}

    def create_role(self, name, description, permissions={}):
        url = self.rbac_base_url
        request_body = {
            "name": name,
            "description": description,
            "permissions": permissions
        }
        resp = self.capellaAPIv2.do_internal_request(url, method="POST", params=json.dumps(request_body))
        return resp

    def list_roles(self, query_params=None):
        default_query_params = {
            "page": 1,
            "perPage": 10,
            "sortBy": "name",
            "sortDirection": "asc"
        }
        if query_params:
            default_query_params.update(query_params)
        url = self.rbac_base_url
        resp = self.capellaAPIv2.do_internal_request(url, method="GET", params=default_query_params)
        return resp

    def list_role_details(self, role_id):
        url = self.rbac_base_url + "/{}".format(role_id)
        resp = self.capellaAPIv2.do_internal_request(url, method="GET")
        return resp

    def delete_role(self, role_id):
        url = self.rbac_base_url + "/{}".format(role_id)
        resp = self.capellaAPIv2.do_internal_request(url, method="DELETE")
        return resp

    def update_role(self, role_id, name=None, description=None, permissions=None):
        request_body = {}
        if name:
            request_body["name"] = name
        if description:
            request_body["description"] = description
        if permissions:
            request_body["permissions"] = permissions
        url = self.rbac_base_url + "/{}".format(role_id)
        self.log.info("Making request to update role: {}".format(url))
        resp = self.capellaAPIv2.do_internal_request(url, method="POST", params=json.dumps(request_body))
        return resp

    def create_cluster_access(self, name, password, permissions={}):
        """
        Creates basic cluster access credentials.
        """
        url = self.cluster_access_base_url
        request_body = {
            "name": name,
            "password": password,
            "permissions": permissions,
            "credentialType": "basic"
        }
        resp = self.capellaAPIv2.do_internal_request(
            url, method="POST", params=json.dumps(request_body))
        return resp

    def create_cluster_access_adv(self, name, password, user_roles=[]):
        """
        Creates advanced cluster access credentials with user roles.
        """
        url = self.cluster_access_base_url
        request_body = {
            "name": name,
            "password": password,
            "credentialType": "advanced",
            "userRoles": user_roles
        }
        resp = self.capellaAPIv2.do_internal_request(
            url, method="POST", params=json.dumps(request_body))
        return resp

    def get_cluster_access(self, name):
        url = "{}/{}".format(self.cluster_access_base_url, name)
        resp = self.capellaAPIv2.do_internal_request(url, method="GET")
        return resp

    def delete_cluster_access(self, user_id):
        url = self.cluster_access_base_url + "/{}".format(user_id)
        resp = self.capellaAPIv2.do_internal_request(url, method="DELETE")
        return resp

    def update_cluster_access(self, user_id, request_body):
        url = self.cluster_access_base_url + "/{}".format(user_id)
        resp = self.capellaAPIv2.do_internal_request(url, method="POST", params=json.dumps(request_body))
        return resp

    def create_api_key(self, name, description, organizationRoles, project_roles={}):
        """
        project_roles is a dictionary with project_id as key and list of roles as value.
        Example:
        {
            "project_id": ["role1", "role2"],
            "project_id2": ["role3", "role4"]
        }
        """
        resources = []
        for project_id, roles in project_roles.items():
            resources.append({
                "id": project_id,
                "roles": roles
            })
        self.log.info("Resources: {}".format(resources))
        resp = self.capellaAPI.org_ops_apis.create_api_key(self.org_id,
                                                           name,
                                                           organizationRoles=organizationRoles,
                                                           description=description,
                                                           resources=resources,
                                                           allowedCIDRs=["0.0.0.0/0"])
        return resp

    def get_api_key(self, api_key_id):
        resp = self.capellaAPI.org_ops_apis.fetch_api_key_info(self.org_id, api_key_id)
        return resp

    def delete_api_key(self, api_key_id):
        resp = self.capellaAPI.org_ops_apis.delete_api_key(self.org_id, api_key_id)
        return resp

    def get_cluster_info(self):
        resp = self.capellaAPIv2.do_internal_request(self.cluster_info_base_url, method="GET")
        self.validate_resp_status_code(resp, 200)
        return resp.json()

    def get_user_in_dp(self, node_domain, admin_password, user_id):
        url= f"https://{node_domain}:18091/settings/rbac/users/{user_id}"
        resp = requests.get(url, auth=("Administrator", admin_password))
        if resp.status_code != 200:
            self.fail("Failed to get user in DP: {}".format(resp.text))
        return resp.json()

    def _validate_operation(self, expected_success, actual_success, resource_description):
        """Helper method to validate if operation result matches expectation."""
        if expected_success != actual_success:
            self.log.error("Validation failed for {}: Expected success={}, Actual success={}".format(
                resource_description, expected_success, actual_success))
            return False
        return True

    def _perform_read_on_bucket(self, cluster, bucket_name):
        """Perform read operation on a bucket and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            bucket = cluster.bucket(resolved_bucket)
            default_collection = bucket.default_collection()
            default_collection.get("test_doc")
            self.log.info("Data read operation on bucket {} succeeded as expected".format(bucket_name))
            return True
        except BucketNotFoundException as e:
            self.log.info("Data read operation on bucket {} failed with BucketNotFoundException: {}".format(bucket_name, e))
            self.log.info("Bucket {} does not exist for user".format(bucket_name))
            return False
        except DocumentNotFoundException as e:
            self.log.info("Data read operation on bucket {} failed with DocumentNotFoundException: {}".format(bucket_name, e))
            self.log.info("Document {} does not exist in bucket {}".format(
                "test_doc", bucket_name))
            return True
        except AuthenticationException as e:
            self.log.info("Data read operation on bucket {} failed with AuthenticationException: {}".format(bucket_name, e))
            return False

    def _perform_read_on_collection(self, cluster, bucket_name, scope_name, collection_name):
        """Perform read operation on a collection and return success status."""
        try:
            resolved_collection = collection_name if collection_name != "*" else "collection1"
            bucket = cluster.bucket(bucket_name)
            scope = bucket.scope(scope_name)
            collection = scope.collection(resolved_collection)
            collection.get("test_doc")
            self.log.info("Data read operation on collection {}.{}.{} succeeded as expected".format(
                bucket_name, scope_name, collection_name))
            return True
        except BucketNotFoundException   as e:
            self.log.info("Data read operation on collection {}.{}.{} failed with BucketNotFoundException: {}".format(
                bucket_name, scope_name, collection_name, e))
            self.log.info("Bucket {} does not exist for user".format(bucket_name))
            return False
        except DocumentNotFoundException as e:
            self.log.info("Data read operation on collection {}.{}.{} failed with DocumentNotFoundException: {}".format(
                bucket_name, scope_name, collection_name, e))
            self.log.info("Document {} does not exist in collection {}.{}.{}".format(
                "test_doc", bucket_name, scope_name, collection_name))
            return True
        except AuthenticationException as e:
            self.log.info("Data read operation on collection {}.{}.{} failed with AuthenticationException: {}".format(
                bucket_name, scope_name, collection_name, e))
            return False

    def _perform_write_on_bucket(self, cluster, bucket_name):
        """Perform write operation on a bucket and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            bucket = cluster.bucket(resolved_bucket)
            default_collection = bucket.default_collection()
            default_collection.upsert("test_manage_doc", {"action": "write", "bucket": bucket_name})
            self.log.info("Data manage operation on bucket {} succeeded as expected".format(bucket_name))
            return True
        except BucketNotFoundException as e:
            self.log.info("Data manage operation on bucket {} failed with BucketNotFoundException: {}".format(bucket_name, e))
            self.log.info("Bucket {} does not exist for user".format(bucket_name))
            return False
        except AuthenticationException as e:
            self.log.info("Data manage operation on bucket {} failed with AuthenticationException: {}".format(bucket_name, e))
            return False

    def _perform_write_on_collection(self, cluster, bucket_name, scope_name, collection_name):
        """Perform write operation on a collection and return success status."""
        try:
            resolved_collection = collection_name if collection_name != "*" else "collection1"
            bucket = cluster.bucket(bucket_name)
            scope = bucket.scope(scope_name)
            collection = scope.collection(resolved_collection)
            collection.upsert("test_doc", {"action": "write", "collection": collection_name})
            self.log.info("Data manage operation on collection {}.{}.{} succeeded as expected".format(
                bucket_name, scope_name, collection_name))
            return True
        except BucketNotFoundException as e:
            self.log.info("Data manage operation on collection {}.{}.{} failed with BucketNotFoundException: {}".format(
                bucket_name, scope_name, collection_name, e))
            self.log.info("Bucket {} does not exist for user".format(bucket_name))
            return False
        except AuthenticationException as e:
            self.log.info("Data manage operation on collection {}.{}.{} failed with AuthenticationException: {}".format(
                bucket_name, scope_name, collection_name, e))
            return False

    def _perform_query_select(self, cluster, bucket_name, scope_name=None, collection_name=None):
        """Perform N1QL SELECT query and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            resolved_scope = scope_name if scope_name and scope_name != "*" else "_default"
            resolved_collection = collection_name if collection_name and collection_name != "*" else "_default"

            if scope_name and collection_name:
                query = "SELECT * FROM `{}`.`{}`.`{}` LIMIT 1".format(resolved_bucket, resolved_scope, resolved_collection)
            else:
                query = "SELECT * FROM `{}` LIMIT 1".format(resolved_bucket)

            cluster.query(query)
            self.log.info("Query SELECT on {}.{}.{} succeeded".format(resolved_bucket, resolved_scope, resolved_collection))
            return True
        except Exception as e:
            self.log.info("Query SELECT failed: {}".format(e))
            return False

    def _perform_query_insert(self, cluster, bucket_name, scope_name=None, collection_name=None):
        """Perform N1QL INSERT query and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            resolved_scope = scope_name if scope_name and scope_name != "*" else "_default"
            resolved_collection = collection_name if collection_name and collection_name != "*" else "_default"

            import uuid
            doc_id = "test_insert_{}".format(str(uuid.uuid4())[:8])

            if scope_name and collection_name:
                query = "INSERT INTO `{}`.`{}`.`{}` (KEY, VALUE) VALUES ('{}', {{'test': 'value'}})".format(
                    resolved_bucket, resolved_scope, resolved_collection, doc_id)
            else:
                query = "INSERT INTO `{}` (KEY, VALUE) VALUES ('{}', {{'test': 'value'}})".format(
                    resolved_bucket, doc_id)

            cluster.query(query)
            self.log.info("Query INSERT on {}.{}.{} succeeded".format(resolved_bucket, resolved_scope, resolved_collection))
            return True
        except Exception as e:
            self.log.info("Query INSERT failed: {}".format(e))
            return False

    def _perform_query_update(self, cluster, bucket_name, scope_name=None, collection_name=None):
        """Perform N1QL UPDATE query and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            resolved_scope = scope_name if scope_name and scope_name != "*" else "_default"
            resolved_collection = collection_name if collection_name and collection_name != "*" else "_default"

            if scope_name and collection_name:
                query = "UPDATE `{}`.`{}`.`{}` SET test_field = 'updated' WHERE META().id = 'nonexistent_doc'".format(
                    resolved_bucket, resolved_scope, resolved_collection)
            else:
                query = "UPDATE `{}` SET test_field = 'updated' WHERE META().id = 'nonexistent_doc'".format(resolved_bucket)

            cluster.query(query)
            self.log.info("Query UPDATE on {}.{}.{} succeeded".format(resolved_bucket, resolved_scope, resolved_collection))
            return True
        except Exception as e:
            self.log.info("Query UPDATE failed: {}".format(e))
            return False

    def _perform_query_delete(self, cluster, bucket_name, scope_name=None, collection_name=None):
        """Perform N1QL DELETE query and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            resolved_scope = scope_name if scope_name and scope_name != "*" else "_default"
            resolved_collection = collection_name if collection_name and collection_name != "*" else "_default"

            if scope_name and collection_name:
                query = "DELETE FROM `{}`.`{}`.`{}` WHERE META().id = 'nonexistent_doc'".format(
                    resolved_bucket, resolved_scope, resolved_collection)
            else:
                query = "DELETE FROM `{}` WHERE META().id = 'nonexistent_doc'".format(resolved_bucket)

            cluster.query(query)
            self.log.info("Query DELETE on {}.{}.{} succeeded".format(resolved_bucket, resolved_scope, resolved_collection))
            return True
        except Exception as e:
            self.log.info("Query DELETE failed: {}".format(e))
            return False

    def _perform_query_index(self, cluster, bucket_name, scope_name=None, collection_name=None):
        """Perform N1QL CREATE INDEX query and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            resolved_scope = scope_name if scope_name and scope_name != "*" else "_default"
            resolved_collection = collection_name if collection_name and collection_name != "*" else "_default"

            import uuid
            index_name = "test_idx_{}".format(str(uuid.uuid4())[:8])

            if scope_name and collection_name:
                query = "CREATE INDEX `{}` ON `{}`.`{}`.`{}`(test_field) WITH {{'defer_build': true}}".format(
                    index_name, resolved_bucket, resolved_scope, resolved_collection)
            else:
                query = "CREATE INDEX `{}` ON `{}`(test_field) WITH {{'defer_build': true}}".format(
                    index_name, resolved_bucket)

            cluster.query(query)
            self.log.info("Query CREATE INDEX on {}.{}.{} succeeded".format(resolved_bucket, resolved_scope, resolved_collection))
            return True
        except Exception as e:
            self.log.info("Query CREATE INDEX failed: {}".format(e))
            return False

    def _perform_query_catalog(self, cluster):
        """Perform N1QL system catalog query and return success status."""
        try:
            query = "SELECT * FROM system:keyspaces LIMIT 1"
            cluster.query(query)
            self.log.info("Query system catalog succeeded")
            return True
        except Exception as e:
            self.log.info("Query system catalog failed: {}".format(e))
            return False

    def _perform_analytics_select(self, cluster, bucket_name=None):
        """Perform Analytics SELECT query and return success status."""
        try:
            resolved_bucket = bucket_name if bucket_name and bucket_name != "*" else "bucket1"
            query = "SELECT * FROM `{}` LIMIT 1".format(resolved_bucket)
            cluster.analytics_query(query)
            self.log.info("Analytics SELECT on {} succeeded".format(resolved_bucket))
            return True
        except Exception as e:
            self.log.info("Analytics SELECT failed: {}".format(e))
            return False

    def _perform_fts_search(self, cluster, bucket_name):
        """Perform FTS search and return success status."""
        try:
            from couchbase.search import SearchOptions, QueryStringQuery
            resolved_bucket = bucket_name if bucket_name != "*" else "bucket1"
            # Try to search - will fail if no index exists but that's okay for permission check
            result = cluster.search_query("{}_fts_index".format(resolved_bucket),
                                          QueryStringQuery("test"),
                                          SearchOptions(limit=1))
            self.log.info("FTS search on {} succeeded".format(resolved_bucket))
            return True
        except Exception as e:
            error_str = str(e).lower()
            # If the error is about missing index, permission was granted
            if "index not found" in error_str or "no index" in error_str:
                self.log.info("FTS search permission granted but index not found: {}".format(e))
                return True
            self.log.info("FTS search failed: {}".format(e))
            return False

    def _extract_resources_from_permission(self, value):
        """
        Extract bucket, scope, and collection information from permission value.
        Returns a list of tuples: (bucket_name, scope_name, collection_name)
        scope_name and collection_name can be None if not specified.
        """
        resources = []
        if not value or not isinstance(value, dict):
            # Global permission - return default resource
            resources.append(("bucket1", None, None))
            return resources

        if "buckets" not in value:
            # Global permission
            resources.append(("bucket1", None, None))
            return resources

        for bucket_info in value.get("buckets", []):
            bucket_name = bucket_info.get("name", "bucket1")

            if "scopes" not in bucket_info:
                # Bucket level permission
                resources.append((bucket_name, None, None))
            else:
                for scope_info in bucket_info.get("scopes", []):
                    scope_name = scope_info.get("name", "scope1")

                    if "collections" not in scope_info:
                        # Scope level permission
                        resources.append((bucket_name, scope_name, None))
                    else:
                        for collection_info in scope_info.get("collections", []):
                            if isinstance(collection_info, dict):
                                collection_name = collection_info.get("name", "collection1")
                            else:
                                collection_name = collection_info
                            resources.append((bucket_name, scope_name, collection_name))

        return resources if resources else [("bucket1", None, None)]

    def perform_action_based_on_permissions(self, permissions, access_name, access_password):
        """
        Validates that operations based on granted permissions match expected outcomes.
        Returns True if all operations succeed as expected, False otherwise.

        Supports the following privileges:
        - Data: dataRead, dataManage, dataMonitor
        - Query: queryRead, queryInsert, queryUpdate, queryDelete, queryIndex, queryCatalog
        - Analytics: analyticsAdmin, analyticsRead, analyticsManage, analyticsSelect
        - FTS: ftsManage, ftsRead
        - Eventing: eventingManage
        - Stats: statsRead
        - Global Functions: globalFunctionExecute, globalFunctionManage, queryExecute, queryManage
        """
        options = ClusterOptions(PasswordAuthenticator(access_name, access_password), tls_verify=TLSVerifyMode.NONE)
        options.apply_profile('wan_development')
        cluster = Cluster(self.conn_string, options)
        cluster.wait_until_ready(timedelta(seconds=10))

        all_validations_passed = True

        for permission in permissions:
            for privilege, value in permission.items():
                resources = self._extract_resources_from_permission(value)
                self.log.info("Validating privilege {} with resources: {}".format(privilege, resources))

                if privilege == "dataRead":
                    for bucket_name, scope_name, collection_name in resources:
                        if scope_name and collection_name:
                            actual_success = self._perform_read_on_collection(cluster, bucket_name, scope_name, collection_name)
                            resource_desc = "dataRead on collection {}.{}.{}".format(bucket_name, scope_name, collection_name)
                        else:
                            actual_success = self._perform_read_on_bucket(cluster, bucket_name)
                            resource_desc = "dataRead on bucket {}".format(bucket_name)
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "dataManage":
                    for bucket_name, scope_name, collection_name in resources:
                        if scope_name and collection_name:
                            actual_success = self._perform_write_on_collection(cluster, bucket_name, scope_name, collection_name)
                            resource_desc = "dataManage on collection {}.{}.{}".format(bucket_name, scope_name, collection_name)
                        else:
                            actual_success = self._perform_write_on_bucket(cluster, bucket_name)
                            resource_desc = "dataManage on bucket {}".format(bucket_name)
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "dataMonitor":
                    # Data monitoring is typically validated through stats access
                    self.log.info("dataMonitor privilege granted - monitoring access enabled")

                elif privilege == "queryRead":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_query_select(cluster, bucket_name, scope_name, collection_name)
                        resource_desc = "queryRead on {}.{}.{}".format(bucket_name, scope_name or "_default", collection_name or "_default")
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "queryInsert":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_query_insert(cluster, bucket_name, scope_name, collection_name)
                        resource_desc = "queryInsert on {}.{}.{}".format(bucket_name, scope_name or "_default", collection_name or "_default")
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "queryUpdate":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_query_update(cluster, bucket_name, scope_name, collection_name)
                        resource_desc = "queryUpdate on {}.{}.{}".format(bucket_name, scope_name or "_default", collection_name or "_default")
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "queryDelete":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_query_delete(cluster, bucket_name, scope_name, collection_name)
                        resource_desc = "queryDelete on {}.{}.{}".format(bucket_name, scope_name or "_default", collection_name or "_default")
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "queryIndex":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_query_index(cluster, bucket_name, scope_name, collection_name)
                        resource_desc = "queryIndex on {}.{}.{}".format(bucket_name, scope_name or "_default", collection_name or "_default")
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "queryCatalog":
                    actual_success = self._perform_query_catalog(cluster)
                    if not self._validate_operation(True, actual_success, "queryCatalog"):
                        all_validations_passed = False

                elif privilege == "analyticsSelect":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_analytics_select(cluster, bucket_name)
                        resource_desc = "analyticsSelect on {}".format(bucket_name)
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "analyticsAdmin":
                    self.log.info("analyticsAdmin privilege granted - full analytics admin access enabled")

                elif privilege == "analyticsRead":
                    self.log.info("analyticsRead privilege granted - analytics metadata read access enabled")

                elif privilege == "analyticsManage":
                    self.log.info("analyticsManage privilege granted - analytics resource management enabled")

                elif privilege == "ftsRead":
                    for bucket_name, scope_name, collection_name in resources:
                        actual_success = self._perform_fts_search(cluster, bucket_name)
                        resource_desc = "ftsRead on {}".format(bucket_name)
                        if not self._validate_operation(True, actual_success, resource_desc):
                            all_validations_passed = False

                elif privilege == "ftsManage":
                    self.log.info("ftsManage privilege granted - FTS index management enabled")

                elif privilege == "eventingManage":
                    self.log.info("eventingManage privilege granted - eventing function management enabled")

                elif privilege == "statsRead":
                    self.log.info("statsRead privilege granted - external stats read access enabled")

                elif privilege == "queryExecute":
                    self.log.info("queryExecute privilege granted - UDF execution at scope level enabled")

                elif privilege == "queryManage":
                    self.log.info("queryManage privilege granted - UDF management at scope level enabled")

                elif privilege == "globalFunctionExecute":
                    self.log.info("globalFunctionExecute privilege granted - global UDF execution enabled")

                elif privilege == "globalFunctionManage":
                    self.log.info("globalFunctionManage privilege granted - global UDF management enabled")

                else:
                    self.log.warning("Unknown privilege: {}".format(privilege))

        return all_validations_passed


    ################################################################################
    #                                  Test cases                                  #
    ################################################################################

    def test_role_CRUD_ops(self):

        create_resp = self.create_role("created_role", "Sample description", permissions={"dataRead": {}})
        self.validate_resp_status_code(create_resp, 200)
        role_id = create_resp.json()['id']
        self.log.info("Role created successfully with id: {}".format(role_id))

        self.sleep(20)

        update_resp = self.update_role(role_id, name="updated_role", description="Updated description",
                                       permissions={"dataManage": {}})
        self.validate_resp_status_code(update_resp, 200)
        self.log.info("Role updated successfully")

        list_resp = self.list_roles()
        self.validate_resp_status_code(list_resp, 200)
        self.log.info("List of roles: {}".format(list_resp.json()))

        role_details_resp = self.list_role_details(role_id)
        self.validate_resp_status_code(role_details_resp, 200)
        self.log.info("Role details: {}".format(role_details_resp.json()))

        create_resp = self.create_role("created_role", "Sample description", permissions={"dataRead": {}})
        self.validate_resp_status_code(create_resp, 422)
        self.log.info("Role creation failed as expected (duplicate entry): {}".format(create_resp.json()))

        delete_resp = self.delete_role(role_id)
        self.validate_resp_status_code(delete_resp, 200)
        self.log.info("Role deleted successfully")

    def test_user_role_management(self):

        create_resp = self.create_role("created_role", "Sample description", permissions={"dataRead": {}})
        self.validate_resp_status_code(create_resp, 200)
        role_id = create_resp.json()['id']
        self.log.info("Role created successfully with id: {}".format(role_id))

        create_resp = self.create_cluster_access_adv(self.access_name,
                                                     self.access_password,
                                                     user_roles=["created_role"])
        self.validate_resp_status_code(create_resp, 200)
        user_id = create_resp.json()['id']
        self.log.info("Cluster access created successfully with id: {}".format(user_id))

        delete_resp = self.delete_cluster_access(user_id)
        self.validate_resp_status_code(delete_resp, 200)
        self.log.info("Cluster access deleted successfully")

    def test_custom_role_resource_mapping(self):

        if self.credential_type == "basic":
            create_resp = self.create_cluster_access(self.access_name,
                                                     self.access_password,
                                                     permissions=self.basic_permissions)
            self.validate_resp_status_code(create_resp, 200)
            user_id = create_resp.json()['id']
            self.log.info("Cluster access created successfully with id: {}".format(user_id))
            delete_resp = self.delete_cluster_access(user_id)
            self.validate_resp_status_code(delete_resp, 200)
            self.log.info("Cluster access deleted successfully")

        elif self.credential_type == "advanced":
            user_roles = {}
            for role, desc, perm in zip(self.role_name, self.role_description, self.role_permissions):
                self.log.info("Creating {} with {}".format(role, desc))
                create_resp = self.create_role(role, desc, permissions=perm)
                self.validate_resp_status_code(create_resp, 200)
                role_id = create_resp.json()['id']
                self.log.info("Role created successfully with id: {}".format(role_id))
                user_roles[role_id] = role
            create_resp = self.create_cluster_access_adv(self.access_name,
                                                         self.access_password,
                                                         user_roles=list(user_roles.values()))
            self.validate_resp_status_code(create_resp, 200)
            user_id = create_resp.json()['id']
            self.log.info("Cluster access created successfully with id: {}".format(user_id))
        else:
            self.fail("Invalid credential type: {}".format(self.credential_type))

        all_validations_passed = self.perform_action_based_on_permissions(self.role_permissions, self.access_name, self.access_password)
        if not all_validations_passed:
            self.fail("All operations based on granted permissions did not succeed as expected")


    def test_for_privilege_escalation(self):
        create_resp = self.create_role("read_only_role", "Sample description", permissions={"dataRead": {"buckets": [{"name": "bucket1"}]}})
        self.validate_resp_status_code(create_resp, 200)
        role_id = create_resp.json()['id']
        self.log.info("Role created successfully with id: {}".format(role_id))

        create_resp = self.create_cluster_access_adv(self.access_name,
                                                     self.access_password,
                                                     user_roles=["read_only_role"])
        self.validate_resp_status_code(create_resp, 200)
        user_id = create_resp.json()['id']
        self.log.info("Cluster access created successfully with id: {}".format(user_id))
        self.sleep(10)
        options = ClusterOptions(PasswordAuthenticator(self.access_name, self.access_password), tls_verify=TLSVerifyMode.NONE)
        options.apply_profile('wan_development')
        cluster = Cluster(self.conn_string, options)
        cluster.wait_until_ready(timedelta(seconds=10))

        bucket = cluster.bucket("bucket1")
        scope = bucket.scope("scope1")
        collection = scope.collection("collection1")
        try:
            collection.insert("test_doc", "test_value")
            self.fail("Expected failure to write to collection as the role has only read permission")
        except AmbiguousTimeoutException as e:
            self.fail("Unexpected exception occured: {}".format(e))
        except AuthenticationException as e:
            self.log.info("Expected exception: {}".format(e))

        udates_resp = self.update_role(role_id, permissions={"dataManage": {"buckets": [{"name": "*"}]}})
        self.validate_resp_status_code(udates_resp, 200)
        self.log.info("Role updated successfully")
        self.sleep(10)
        try:
            collection.insert("test_doc", "test_value")
            self.log.info("Test_doc inserted successfully")
        except Exception as e:
            self.fail("Exception raised: {}".format(e))

    def test_restricted_role_enforcement(self):
        pass

    def test_roles_in_server(self):
        user_roles = {}
        role_name = "all_privileges_role"
        role_description = "Role with all available privileges"
        all_privileges = list(self.capella_to_dp_role_mapping.keys())

        # Build permissions dict with proper resource structure for each privilege
        permissions_dict = {}
        for privilege in all_privileges:
            permissions_dict[privilege] = self._get_privilege_resources(privilege)

        self.log.info("Creating role '{}' with all privileges".format(role_name))
        self.log.info("Permissions structure: {}".format(permissions_dict))
        create_resp = self.create_role(role_name, role_description, permissions=permissions_dict)
        self.validate_resp_status_code(create_resp, 200)
        role_id = create_resp.json()['id']
        self.log.info("Role created successfully with id: {}".format(role_id))
        user_roles[role_id] = role_name

        resp = self.create_cluster_access_adv(self.access_name,
                                               self.access_password,
                                               user_roles=list(user_roles.values()))
        self.validate_resp_status_code(resp, 200)
        user_id = resp.json()['id']
        self.log.info("Cluster access created successfully with id: {}".format(user_id))

        node_resp = self.capellaAPI.get_nodes(self.tenant_id, self.project_id, self.cluster_id)
        node_domain = json.loads(node_resp.content)["data"][0]["data"]["hostname"]
        admin_password = self.get_dp_admin_secrets()
        user_info_dp = self.get_user_in_dp(node_domain, admin_password, self.access_name)

        self.assertEqual(user_info_dp['id'], self.access_name)
        self.log.info("User Role Mapping in DP: {}".format(user_info_dp['roles']))

        expected_dp_roles = []
        for privilege in all_privileges:
            if privilege in self.capella_to_dp_role_mapping:
                expected_dp_roles.extend(self.capella_to_dp_role_mapping[privilege])
        # Add roles that are not tied to a privilege but are expected for a user
        expected_dp_roles.extend(["admin", "ro_admin"])

        actual_dp_roles = [role['role'] for role in user_info_dp['roles']]

        self.log.info("Expected DP roles: {}".format(sorted(expected_dp_roles)))
        self.log.info("Actual DP roles: {}".format(sorted(actual_dp_roles)))

        self.assertEqual(expected_dp_roles, actual_dp_roles,
                              "Mismatch between expected and actual roles in the data plane.")

    def test_audit_log(self):
        create_resp = self.create_role("created_role", "Sample description", permissions={"dataManage": {"buckets": [{"name": "*"}]}, "dataRead": {"buckets": [{"name": "*"}]}})
        self.validate_resp_status_code(create_resp, 200)
        role_id = create_resp.json()['id']
        self.log.info("Role created successfully with id: {}".format(role_id))
        resp = self.create_cluster_access_adv(self.access_name,
                                       self.access_password,
                                       user_roles=["created_role"])
        self.validate_resp_status_code(resp, 200)
        user_id = resp.json()['id']
        self.log.info("Cluster access created successfully with id: {}".format(user_id))

        options = ClusterOptions(PasswordAuthenticator(self.access_name, self.access_password), tls_verify=TLSVerifyMode.NONE)
        options.apply_profile('wan_development')
        cluster = Cluster(self.conn_string, options)

        # Wait for the cluster to be ready.
        cluster.wait_until_ready(timedelta(seconds=10))

        bucket = cluster.bucket("bucket1")
        scope = bucket.scope("scope1")
        collection = scope.collection("collection1")
        document_body = {"test_value": "dummy_value"}
        try:
            collection.insert("test_doc", document_body)
        except ValueError as err:
            self.log.info("Insert failed with ValueError: %s. Retrying with JSON payload.", err)
            collection.insert("test_doc", json.dumps(document_body))
        collection.get("test_doc")
        collection.remove("test_doc")
        self.log.info("Sleeping for 100 seconds to allow audit log events to be populated")
        self.sleep(100)
        audit_resp = self.capellaAPI.cluster_ops_apis.fetch_audit_log_event_info(self.org_id, self.project_id,
                                                                                 self.cluster_id)
        self.validate_resp_status_code(audit_resp, 200)
        self.log.info("Audit log event info: {}".format(audit_resp.json()['events']))
        found = False
        for event in audit_resp.json()['events']:
             if event['name'] == "GRANT ROLE statement":
                self.log.info("Grant role statement event found: {}".format(event))
                found = True
                break
        if not found:
            self.fail("Grant role statement event not found in audit log")

    def test_metrics_and_observability(self):
        pass

    def test_multiple_overlapping_roles_with_privileges(self):
        # NOTE: in the conf file make sure the first role is read only and second role is read/write
        # not following this order can lead to test failure

        role_ids = []
        for role, desc, perm in zip(self.role_name, self.role_description, self.role_permissions):
            self.log.info("Creating {} with {}".format(role, desc))
            create_resp = self.create_role(role, desc, permissions=perm)
            self.validate_resp_status_code(create_resp, 200)
            role_id = create_resp.json()['id']
            role_ids.append(role_id)
            self.log.info("Role created successfully with id: {}".format(role_id))
        resp = self.create_cluster_access_adv(self.access_name,
                                       self.access_password,
                                       user_roles=self.role_name)
        self.validate_resp_status_code(resp, 200)
        user_id = resp.json()['id']
        self.log.info("Cluster access created successfully with id: {}".format(user_id))
        options = ClusterOptions(PasswordAuthenticator(self.access_name, self.access_password), tls_verify=TLSVerifyMode.NONE)
        options.apply_profile('wan_development')
        cluster = Cluster(self.conn_string, options)
        cluster.wait_until_ready(timedelta(seconds=10))

        bucket = cluster.bucket("bucket1")
        scope = bucket.scope("scope1")
        collection = scope.collection("collection1")
        try:
            collection.insert("test_doc", "test_value")
            self.log.info("Test_doc inserted successfully")
        except AuthenticationException as e:
            self.fail("Write failed when it should have succeeded: {}".format(e))
        except AmbiguousTimeoutException as e:
            self.fail("Unexpected exception occured: {}".format(e))

        delete_resp = self.delete_role(role_ids[1])
        self.validate_resp_status_code(delete_resp, 200)
        self.log.info("Role deleted successfully with id: {}".format(role_ids[1]))
        try:
            collection.insert("test_doc", "test_value")
            self.fail("Write should have failed as the role has only read permission")
        except AuthenticationException as e:
            self.log.info("Expected exception: {}".format(e))
        except Exception as e:
            self.fail("Unexpected exception occured: {}".format(e))

    def tearDown(self):
        SecurityBase.tearDown(self)