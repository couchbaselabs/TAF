"""
Created on 1-March-2024

@author: Umang Agrawal

This utility is for performing actions on Capella Columnar only.
"""
import json
import math
import urllib.parse
import time
import string
import random
import importlib
import copy
import requests
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, current_thread

from Columnar.templates.crudTemplate.docgen_template import Hotel
from global_vars import logger
from SecurityLib.rbac import RbacUtil
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity_columnar import (
    Database, Dataverse, Remote_Link, External_Link, Kafka_Link,
    Remote_Dataset, External_Dataset, Standalone_Dataset, Synonym,
    CBAS_Index, DatabaseUser, ColumnarRole)
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from common_lib import sleep
from queue import Queue
from StatsLib.StatsOperations import StatsHelper
from connections.Rest_Connection import RestConnection
from py_constants import CbServer
from sirius_client_framework.multiple_database_config import ColumnarLoader, CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_constants import SiriusCodes
from Jython_tasks.sirius_task import WorkLoadTask
from Jython_tasks.task_manager import TaskManager


class BaseUtil(object):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        self.log = logger.get("test")
        self.task = server_task
        self.run_query_using_sdk = run_query_using_sdk

    def get_cbas_helper_object(self, cluster):
        if self.run_query_using_sdk:
            from CbasLib.CBASOperations_PythonSDK import CBASHelper as SDK_CBASHelper
            return SDK_CBASHelper(cluster)
        else:
            return CBASHelper(cluster.master)

    def get_all_active_requests(self, cluster, username=None, password=None,
                                timeout=300, analytics_timeout=300):
        cmd = "select value active_requests()"
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if status != 'success':
            return False
        return results[0]

    def execute_statement_on_cbas_util(
            self, cluster, statement, mode=None, timeout=300,
            client_context_id=None, username=None, password=None,
            analytics_timeout=300, time_out_unit="s",
            scan_consistency=None, scan_wait=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        :param statement str, statement to execute on analytics workbench
        :param mode str, cli, rest, jdk
        :param timeout int, timeout is second for REST API request
        :param client_context_id str
        :param username str
        :param password str
        :param analytics_timeout int, timeout for analytics workbench
        :param time_out_unit i,
        :param scan_consistency str
        :param scan_wait str
        """
        cbas_helper = self.get_cbas_helper_object(cluster)
        pretty = "true"
        try:
            if "insert into" not in statement.lower():
                self.log.debug("Running query on cbas: %s" % statement)
            else:
                self.log.debug("Running insert query on cbas")
            response = cbas_helper.execute_statement_on_cbas(
                statement, mode, pretty, timeout, client_context_id,
                username, password,
                analytics_timeout=analytics_timeout,
                time_out_unit=time_out_unit,
                scan_consistency=scan_consistency, scan_wait=scan_wait)
            if type(response) in [str, bytes]:
                response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
                if type(errors) == str:
                    errors = json.loads(errors)
            else:
                errors = None

            if "results" in response:
                results = response["results"]
                if type(results) == str:
                    results = json.loads(results)
            else:
                results = None

            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None

            if "metrics" in response:
                metrics = response["metrics"]
                if type(metrics) == str:
                    metrics = json.loads(metrics)
            else:
                metrics = None

            return response["status"], metrics, errors, results, handle

        except Exception as e:
            self.log.error("Query: {} failed due to: {}".format(statement, str(e)))
            raise Exception(str(e))

    def execute_parameter_statement_on_cbas_util(
            self, cluster, statement, mode=None, timeout=300,
            client_context_id=None, username=None, password=None,
            analytics_timeout=300, parameters={}):
        """
        Executes a statement on CBAS using the REST API using REST Client
        :param statement str, statement to execute on analytics workbench
        :param mode str, cli, rest, jdk
        :param timeout int, timeout is second for REST API request
        :param client_context_id str
        :param username str
        :param password str
        :param analytics_timeout int, timeout for analytics workbench
        :param parameters dict,
        """
        cbas_helper = self.get_cbas_helper_object(cluster)
        pretty = "true"
        try:
            self.log.debug("Running query on cbas: %s" % statement)
            response = cbas_helper.execute_parameter_statement_on_cbas(
                statement, mode, pretty, timeout, client_context_id,
                username, password,
                analytics_timeout=analytics_timeout, parameters=parameters)
            if type(response) == str:
                response = json.loads(response)
            if "errors" in response:
                errors = response["errors"]
                if type(errors) == str:
                    errors = json.loads(errors)
            else:
                errors = None

            if "results" in response:
                results = response["results"]
                if type(results) == str:
                    results = json.loads(results, parse_int=int)
            else:
                results = None

            if "handle" in response:
                handle = response["handle"]
            else:
                handle = None

            if "metrics" in response:
                metrics = response["metrics"]
                if type(metrics) == str:
                    metrics = json.loads(metrics)
            else:
                metrics = None

            return response["status"], metrics, errors, results, handle

        except Exception as e:
            raise Exception(str(e))

    def validate_error_in_response(self, status, errors, expected_error=None,
                                   expected_error_code=None):
        """
        Validates the error response against the expected one.
        :param status str, status returned by REST API call
        :param errors list, errors returned by REST API call
        :param expected_error str, error to match
        :param expected_error_code str, error code to match
        """
        if status != "success":
            if isinstance(errors, list):
                actual_error = (errors[0]["msg"]).replace("`", "")
                expected_error = expected_error.replace("`", "")

            else:
                actual_error = errors["msg"].replace("`", "")
                expected_error = expected_error.replace("`", "")

            if expected_error not in actual_error:
                self.log.error("Error message mismatch. Expected: %s, got: %s"
                               % (expected_error, actual_error))
                return False
            self.log.error("Error message matched. Expected: %s, got: %s"
                           % (expected_error, actual_error))
            if expected_error_code is not None:
                if isinstance(errors, list):
                    error_code = errors[0]["code"]
                else:
                    error_code = errors["code"]

                if expected_error_code != error_code:
                    self.log.error("Error code mismatch. Expected: %s, got: %s"
                                   % (expected_error_code, error_code))
                    return False
                self.log.info("Error code matched. Expected: %s, got: %s"
                              % (expected_error_code, error_code))
            return True
        return False

    @staticmethod
    def generate_name(name_cardinality=1, max_length=30, fixed_length=False,
                      name_key=None, seed=0):
        """
        Creates a name based on name_cadinality.
        :param name_cardinality: int, accepted values are -
            0 - Creates string 'Default'
            1 - Creates a string of random letters and digits of format
            "exampleString"
            2 - Creates a string of random letters and digits of format
            "exampleString.exampleString"
            3 - Creates a string of random letters and digits of format
            "exampleString.exampleString.exampleString"
            4 - Creates string 'Metadata'
        :param max_length: int, number of characters in the name.
        :param fixed_length: bool, If True, creates a name with length equals to no_of_char
        specified, otherwise creates a name with length upto no_of_char specified.
        :param name_key str, if specified, it will generate name with the name_key
        :param seed int seed for generating random string.
        """
        if not seed:
            sleep(0.1, "Sleeping before generating name")
            random.seed(round(time.time() * 1000))
        else:
            random.seed(seed)
        if 0 < name_cardinality < 3:
            if name_key:
                generated_name = ".".join(name_key for i in range(
                    name_cardinality))
            else:
                max_name_len = max_length / name_cardinality
                if fixed_length:
                    generated_name = random.choice(
                        string.ascii_letters) + ('.'.join(''.join(random.choice(
                        string.ascii_letters + string.digits)
                                                                  for _ in range(max_name_len))
                                                          for _ in range(name_cardinality)))
                else:
                    generated_name = random.choice(
                        string.ascii_letters) + '.'.join(''.join(random.choice(
                        string.ascii_letters + string.digits)
                                                                 for _ in range(
                        random.randint(1, max_name_len)))
                                                         for _ in range(name_cardinality))
            reserved_words = {"at", "in", "for", "by", "which", "select",
                              "from", "like", "or", "and", "to", "if", "else",
                              "as", "with", "on", "where", "is", "all", "end",
                              "div", "into", "let", "asc", "desc", "key",
                              "any", "run", "set", "use", "type", "False", "True",
                              "false", "true", "copy"}
            if len(set(generated_name.lower().split(".")) & reserved_words) > 0:
                return BaseUtil.generate_name(
                    name_cardinality, max_length, fixed_length, name_key, seed)
            else:
                return generated_name
        else:
            return None

    @staticmethod
    def get_cbas_spec(module_name):
        """
        Fetches the cbas_specs from spec file mentioned.
        :param module_name str, name of the module from where the specs have to be fetched
        """
        spec_package = importlib.import_module(
            'pytests.cbas.cbas_templates.' +
            module_name)
        return copy.deepcopy(spec_package.spec)

    @staticmethod
    def get_columnar_spec(module_name):
        """
        Fetches the columnar_specs from spec file mentioned.
        :param module_name str, name of the module from where the specs have to be fetched
        """
        spec_package = importlib.import_module(
            'pytests.Columnar.templates.' + module_name)
        return copy.deepcopy(spec_package.spec)

    @staticmethod
    def update_cbas_spec(cbas_spec, updated_specs):
        """
        Updates new dataverse spec in the overall cbas spec.
        :param cbas_spec dict, original cbas spec dict
        :param updated_specs dict, keys or sub dicts to be updated
        """
        if isinstance(cbas_spec, dict) and isinstance(updated_specs, dict):
            for key in cbas_spec.keys():
                if key in updated_specs:
                    cbas_spec[key] = updated_specs[key]
                else:
                    BaseUtil.update_cbas_spec(cbas_spec[key], updated_specs)
        elif isinstance(cbas_spec, list) and isinstance(updated_specs, list):
            for i in range(min(len(cbas_spec), len(updated_specs))):
                BaseUtil.update_cbas_spec(cbas_spec[i], updated_specs[i])

    def run_jobs_in_parallel(self, jobs, results, thread_count,
                             async_run=False, wait_for_job=[]):

        def consume_from_queue(jobs, results):
            while not jobs.empty():
                job = jobs.get()
                try:
                    results.append(job[0](**job[1]))
                except Exception as e:
                    self.log.error(str(e))
                    results.append(False)
                finally:
                    jobs.task_done()
                if len(wait_for_job) > 0 and wait_for_job[0] is True and jobs.empty():
                    time.sleep(5)

        # start worker threads
        if jobs.qsize() < thread_count:
            thread_count = jobs.qsize()
        for tc in range(1, thread_count + 1):
            worker = Thread(
                target=consume_from_queue,
                name="cbas_worker_{0}".format(str(tc)),
                args=(jobs, results,))
            worker.start()
        if not async_run:
            jobs.join()

    @staticmethod
    def get_kv_entity(cluster, bucket_util, include_buckets=[],
                      exclude_buckets=[], include_scopes=[], exclude_scopes=[],
                      include_collections=[], exclude_collections=[]):
        bucket = None
        while not bucket:
            bucket = random.choice(cluster.buckets)
            if include_buckets and bucket.name not in include_buckets:
                bucket = None
            if exclude_buckets and bucket.name in exclude_buckets:
                bucket = None

        scope = None
        while not scope:
            scope = random.choice(bucket_util.get_active_scopes(bucket))
            if include_scopes and scope.name not in include_scopes:
                scope = None
            if exclude_scopes and scope.name in exclude_scopes:
                scope = None

        collection = None
        while not collection:
            collection = random.choice(
                bucket_util.get_active_collections(bucket, scope.name))
            if include_collections and collection.name not in include_collections:
                collection = None
            if exclude_collections and collection.name in exclude_collections:
                collection = None
        return bucket, scope, collection

    @staticmethod
    def format_name(*args):
        return CBASHelper.format_name(*args)

    @staticmethod
    def unformat_name(*args):
        return CBASHelper.unformat_name(*args)

    @staticmethod
    def metadata_format(name):
        return CBASHelper.metadata_format(name)


class RBAC_Util(BaseUtil):
    def __init__(self, server_task=None, run_query_using_sdk=False):
        super(RBAC_Util, self).__init__(server_task, run_query_using_sdk)
        self.database_users = dict()
        self.columnar_roles = dict()

    def create_user(self, cluster, id, username, password):
        """
            Function to create server user.
        """
        user = DatabaseUser(id, username, password)
        result = True
        try:
            RbacUtil().create_user_source([user.to_dict()], "builtin",
                                          cluster.master)
        except Exception as e:
            self.log.error("Exception while creating new user: {}".format(e))
            result = False

        if result:
            self.database_users[user.id] = user

        return result

    def get_user_obj(self, user_id):
        """
            Get user object by user id.
        """
        if user_id in self.database_users:
            return self.database_users[user_id]

        return None

    def get_all_user_obj(self):
        """
            Get all user objects.
        """
        db_users = list()
        db_users.extend(list(self.database_users.values()))

        return db_users

    def create_columnar_role(
            self, cluster, role_name, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, timeout=300,
            analytics_timeout=300):
        """
            Creates a new analytics role.
        """

        role_name = self.format_name(role_name)
        create_role_cmd = "CREATE ROLE " + role_name
        create_role_cmd += ";"

        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, create_role_cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to create role {0}: {1}".format(
                    role_name, str(errors)))
                return False
            else:
                if not self.get_columnar_role_object(role_name):
                    role = ColumnarRole(role_name)
                    self.columnar_roles[role.role_name] = role
                return True

    def drop_columnar_role(
            self, cluster, role_name, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, timeout=300,
            analytics_timeout=300):
        """
            Creates a new analytics role.
        """

        role_name = self.format_name(role_name)
        create_role_cmd = "DROP ROLE " + role_name
        create_role_cmd += ";"

        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, create_role_cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to drop role {0}: {1}".format(
                    role_name, str(errors)))
                return False
            else:
                if self.get_columnar_role_object(role_name):
                    del self.columnar_roles[role_name]
                return True

    def get_columnar_role_object(self, role_name):
        """
        Get columnar role object by role name.
        """
        role_name = self.format_name(role_name)
        if role_name in self.columnar_roles:
            return self.columnar_roles[role_name]

        return None

    def get_all_columnar_role_objs(self):
        """
        Get all columnar role objects.
        """
        columnar_roles = list()
        columnar_roles.extend(self.columnar_roles.values())

        return columnar_roles

    def find_resource_type(self, resource):
        name_cardinality = len(resource.split('.'))

        if name_cardinality == 3:
            return "COLLECTION"
        elif name_cardinality == 2:
            return "SCOPE"
        elif name_cardinality == 1:
            return "DATABASE"

    def grant_privileges_to_subjects(
            self, cluster, privileges=[], subjects=[], resources=[],
            privilege_resource_type="database", use_any=False,
            use_subject_type=False, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, timeout=300,
            analytics_timeout=300):
        """
        Grant privileges to users or roles on resources.

        :param privileges: List of privileges to grant to users/roles.
        :param roles: List of roles to assign to users.
        :param subjects: List of users/roles to grant the privileges or assign roles.
        :param resources: List of resources on which the privileges should be granted.
        :param privilege_resource_type: The type of resource for which privilege is to be granted.
        :param use_any: Boolean flag to determine whether to use the 'any' syntax when granting privileges.
        :param subject_type: Boolean flag to determine wheter to use subject type(USER or ROLE)
                             while granting privileges
        """
        grant_cmd = None
        if privilege_resource_type == "database":
            grant_cmd = "GRANT {0} DATABASE "
        elif privilege_resource_type == "scope":
            grant_cmd = "GRANT {0} SCOPE "
            if not use_any:
                grant_cmd += "IN DATABASE {1} "
        elif privilege_resource_type == "collection_ddl":
            grant_cmd = "GRANT {0} COLLECTION "
            if not use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "IN " + resource_type + " {1} "
        elif privilege_resource_type == "collection_dml":
            grant_cmd = "GRANT {0} ON "
            if use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "ANY COLLECTION IN " + resource_type + " {1} "
            else:
                grant_cmd += "COLLECTION {1} "
        elif privilege_resource_type == "link_ddl":
            grant_cmd = "GRANT {0} LINK "
        elif privilege_resource_type == "link_dml" or \
                privilege_resource_type == "link_connection":
            grant_cmd = "GRANT {0} ON "
            if use_any:
                grant_cmd += "ANY LINK "
            else:
                grant_cmd += "LINK {1} "
        elif privilege_resource_type == "role":
            grant_cmd = "GRANT {0} ROLE "
        elif privilege_resource_type == "synonym":
            grant_cmd = "GRANT {0} SYNONYM "
            if not use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "IN " + resource_type + " {1} "
        elif privilege_resource_type == "index":
            grant_cmd = "GRANT {0} INDEX ON "
            if use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "ANY COLLECTION IN " + resource_type + " {1} "
            else:
                grant_cmd += "COLLECTION {1} "
        elif privilege_resource_type == "udf_ddl":
            grant_cmd = "GRANT {0} FUNCTION "
            if not use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "IN " + resource_type + " {1} "
        elif privilege_resource_type == "udf_execute":
            grant_cmd = "GRANT {0} ON FUNCTION {1} "
            if use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "ANY FUNCTION IN " + resource_type + " {1} "
        elif privilege_resource_type == "view_ddl":
            grant_cmd = "GRANT {0} VIEW "
            if not use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "IN " + resource_type + " {1} "
        elif privilege_resource_type == "view_select":
            grant_cmd = "GRANT {0} ON "
            if use_any:
                resource_type = self.find_resource_type(resources[0])
                grant_cmd += "ANY VIEW IN " + resource_type + " {1} "
            else:
                grant_cmd += "VIEW {1} "

        if use_subject_type:
            users_roles = ["USER {}".format(self.format_name(str(sub))) if isinstance(sub, DatabaseUser)
                           else "ROLE {}".format(self.format_name(str(sub)))
                           for sub in subjects]
            users_roles = ','.join(users_roles)
        else:
            users_roles = ','.join([self.format_name(str(sub)) for sub in subjects])

        grant_cmd += "TO {2}"
        grant_cmd = grant_cmd.format(','.join(privileges),
                                     ','.join([self.format_name(res) for res in resources]),
                                     users_roles)
        grant_cmd += ";"

        self.log.info("Running GRANT statement: {}".format(grant_cmd))
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, grant_cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to grant privlege: {}".format(
                    str(errors)))
                return False
            else:
                return True

    def assign_role_to_user(self, cluster, roles=[], users=[],
                            username=None, password=None,
                            validate_error_msg=False, expected_error=None,
                            expected_error_code=None, timeout=300,
                            analytics_timeout=300):

        columnar_roles = ','.join([self.format_name(str(role)) for role in roles])
        database_users = ','.join([self.format_name(str(user)) for user in users])

        grant_cmd = "GRANT ROLE " + columnar_roles + " TO " + database_users + ";"

        self.log.info("Running GRANT statement: {}".format(grant_cmd))
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, grant_cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to grant role to user: {}".format(
                    str(errors)))
                return False
            else:
                return True


class Database_Util(BaseUtil):
    """
    Database level utility
    """

    def __init__(self, server_task=None, run_query_using_sdk=False):
        super(Database_Util, self).__init__(server_task, run_query_using_sdk)
        self.databases = dict()
        default_database_obj = Database()
        self.databases[default_database_obj.name] = default_database_obj
        default_dataverse_obj = Dataverse()
        default_database_obj.dataverses[
            default_dataverse_obj.name] = default_dataverse_obj

    def get_database_obj(self, database_name):
        """
        Return Database object if database with the required name already
        exists.
        :param database_name str, fully qualified database_name
        """
        return self.databases.get(database_name, None)

    def create_database(
            self, cluster, database_name, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_not_exists=False, timeout=300,
            analytics_timeout=300):
        """
        Creates database.
        :param cluster: object, cluster object
        :param database_name: str, Name of the database which has to be created
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate
        error raised with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_not_exists bool, use IfNotExists flag to check database
        exists before creation, if a database exists the query return
        successfully without creating any database.
        :param timeout int, connection timeout
        :param analytics_timeout int, analytics query timeout
        """
        database_name = self.format_name(database_name)
        cmd = "create database {}".format(database_name)
        if if_not_exists:
            cmd += " if not exists"
        cmd = cmd + ";"
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to create database {0}: {1}".format(
                    database_name, str(errors)))
                return False
            else:
                if not self.get_database_obj(database_name):
                    obj = Database(database_name)
                    self.databases[obj.name] = obj
                return True

    @staticmethod
    def get_database_spec(cbas_spec):
        """
        Fetches database specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictionary.
        """
        return cbas_spec.get("database", {})

    def create_database_from_spec(self, cluster, cbas_spec):
        """
        Creates database from spec
        :param cluster: cluster object
        :param cbas_spec: cbas spec object
        """
        self.log.info("Creating database based on CBAS Spec")

        database_spec = self.get_database_spec(cbas_spec)
        results = [True]
        no_of_databases = database_spec.get("no_of_databases", 1)
        for i in range(1, no_of_databases):
            if database_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name()
            else:
                name = database_spec.get("name_key") + "_{0}".format(
                    str(i))
            results.append(self.create_database(
                cluster, self.format_name(name), if_not_exists=True,
                timeout=cbas_spec.get("api_timeout", 300),
                analytics_timeout=cbas_spec.get("cbas_timeout", 300)
            ))
        return all(results)

    def validate_database_in_metadata(
            self, cluster, database_name, username=None, password=None,
            timeout=300, analytics_timeout=300):
        """
        Validates whether a database is present in Metadata.Database
        :param cluster: cluster object
        :param database_name: str, Name of the database which has to be
        validated.
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        self.log.info("Validating database entry in Metadata")
        cmd = ("select value db from Metadata.`Database` as db where "
               "db.DatabaseName = \"{0}\";".format(
            self.unformat_name(database_name)))
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if status == "success":
            if results:
                return True
            else:
                return False
        else:
            return False

    def drop_database(self, cluster, database_name, username=None,
                      password=None, validate_error_msg=False,
                      expected_error=None, expected_error_code=None,
                      if_exists=False, timeout=300, analytics_timeout=300):
        """
        Drop database.
        :param cluster: object, cluster object
        :param database_name: str, Name of the database which has to be dropped
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate
        error raised with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use If Exists flag to check database
        exists before deletion, if a database exists the query return
        successfully without deleting any database.
        :param timeout int, connection timeout
        :param analytics_timeout int, analytics query timeout
        """
        database_name = self.format_name(database_name)
        cmd = "drop database {}".format(database_name)
        if if_exists:
            cmd += " if exists"
        cmd += ";"
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                obj = self.get_database_obj(database_name)
                if obj:
                    del self.databases[obj.name]
                return True

    def get_all_databases_from_metadata(self, cluster):
        database_query = ("select value db.DatabaseName from Metadata."
                          "`Database` as db where db.SystemDatabase = false;")
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, database_query, mode="immediate", timeout=300,
            analytics_timeout=300)
        if status == 'success':
            return results
        return []


class Dataverse_Util(Database_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Dataverse_Util, self).__init__(server_task, run_query_using_sdk)

    def validate_dataverse_in_metadata(
            self, cluster, dataverse_name, database_name=None, username=None,
            password=None, timeout=300, analytics_timeout=300):
        """
        Validates whether a dataverse is present in Metadata.Dataverse
        :param cluster: object, cluster object
        :param dataverse_name: str, Name of the dataverse which has to be validated.
        :param database_name: str, Name of the database under which the
        dataverse is present.
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        self.log.info("Validating dataverse entry in Metadata")
        cmd = "select value dv from Metadata.`Dataverse` as dv where\
         dv.DataverseName = \"{0}\"".format(
            self.unformat_name(dataverse_name))
        if database_name:
            cmd += " and dv.DatabaseName = \"{0}\"".format(
                self.unformat_name(database_name))
        cmd += ";"
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if status == "success":
            if results:
                return True
            else:
                return False
        else:
            return False

    def create_dataverse(
            self, cluster, dataverse_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_not_exists=False,
            analytics_scope=False, scope=False,
            timeout=300, analytics_timeout=300):
        """
        Creates dataverse.
        :param cluster: cluster, cluster object
        :param dataverse_name: str, Name of the dataverse which has to be created.
        :param database_name: str, Name of the database in which dataverse exists
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_not_exists bool, use IfNotExists flag to check dataverse exists before creation,
        if a dataverse exists the query return successfully without creating any dataverse.
        :param analytics_scope bool, If True, will use create analytics scope syntax
        :param scope bool, If True, will use create scope syntax (cuurently
        only supported on columnar)
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        dataverse_name = self.format_name(dataverse_name)

        if analytics_scope:
            cmd = "create analytics scope "
        elif scope:
            cmd = "create scope "
        else:
            cmd = "create dataverse "
        if database_name:
            cmd += "{0}.{1}".format(
                self.format_name(database_name), dataverse_name)
        else:
            cmd += "{0}".format(dataverse_name)

        if if_not_exists:
            cmd += " if not exists"

        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to create dataverse {0}: {1}".format(
                    dataverse_name, str(errors)))
                return False
            else:
                if not self.get_dataverse_obj(dataverse_name, database_name):
                    if database_name:
                        database_obj = self.get_database_obj(database_name)
                        obj = Dataverse(dataverse_name, database_name)
                    else:
                        database_obj = self.get_database_obj("Default")
                        obj = Dataverse(dataverse_name)
                    database_obj.dataverses[obj.name] = obj
                return True

    def drop_dataverse(
            self, cluster, dataverse_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_exists=False, analytics_scope=False,
            scope=False, timeout=300, analytics_timeout=300):
        """
        Drops the dataverse.
        :param cluster: cluster, cluster object
        :param dataverse_name: str, Name of the dataverse which has to be dropped.
        :param database_name: str, Name of the database in which the
        dataverse is present.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use IfExists flag to check dataverse exists before deletion,
        if a dataverse does not exists the query return successfully without deleting any dataverse.
        :param analytics_scope bool, If True, will use create analytics scope syntax
        :param scope bool, If True, will use drop scope syntax (cuurently
        only supported on columnar)
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        dataverse_name = self.format_name(dataverse_name)

        if analytics_scope:
            cmd = "drop analytics scope "
        elif scope:
            cmd = "drop scope "
        else:
            cmd = "drop dataverse "

        if database_name:
            cmd += "{0}.{1}".format(
                self.format_name(database_name), dataverse_name)
        else:
            cmd += "{0}".format(dataverse_name)

        if if_exists:
            cmd += " if exists"
        cmd += ";"

        status, metrics, errors, results, _ = (
            self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout))
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                obj = self.get_dataverse_obj(dataverse_name, database_name)
                if obj:
                    del self.databases[obj.database_name].dataverses[obj.name]
                return True

    def create_analytics_scope(
            self, cluster, cbas_scope_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_not_exists=False, timeout=300,
            analytics_timeout=300):
        """
        Creates analytics scope. This method is synthetic sugar for creating
        dataverse in analytics.
        :param cbas_scope_name: str, Name of the cbas_scope which has to be created.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_not_exists bool, use IfNotExists flag to check dataverse exists before creation,
        if a dataverse exists the query return successfully without creating any dataverse.
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        return self.create_dataverse(
            cluster, cbas_scope_name, database_name, username, password,
            validate_error_msg, expected_error, expected_error_code,
            if_not_exists, True, False, timeout, analytics_timeout)

    def drop_analytics_scope(
            self, cluster, cbas_scope_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_exists=False, timeout=300,
            analytics_timeout=300):
        """
        Drops the dataverse.
        :param cbas_scope_name: str, Name of the cbas_scope which has to be dropped.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use IfExists flag to check dataverse exists before deletion,
        if a dataverse does not exists the query return successfully without deleting any dataverse.
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        return self.drop_dataverse(
            cluster, cbas_scope_name, database_name, username, password,
            validate_error_msg, expected_error, expected_error_code, if_exists,
            True, False, timeout, analytics_timeout)

    def create_scope(
            self, cluster, cbas_scope_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_not_exists=False, timeout=300,
            analytics_timeout=300):
        """
        Creates scope. This method is synthetic sugar for creating
        dataverse in analytics.
        :param cbas_scope_name: str, Name of the cbas_scope which has to be created.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_not_exists bool, use IfNotExists flag to check dataverse exists before creation,
        if a dataverse exists the query return successfully without creating any dataverse.
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        return self.create_dataverse(
            cluster, cbas_scope_name, database_name, username, password,
            validate_error_msg, expected_error, expected_error_code,
            if_not_exists, False, True, timeout, analytics_timeout)

    def drop_scope(
            self, cluster, cbas_scope_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_exists=False, timeout=300,
            analytics_timeout=300):
        """
        Drops the dataverse.
        :param cbas_scope_name: str, Name of the cbas_scope which has to be dropped.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use IfExists flag to check dataverse exists before deletion,
        if a dataverse does not exists the query return successfully without deleting any dataverse.
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        return self.drop_dataverse(
            cluster, cbas_scope_name, database_name, username, password,
            validate_error_msg, expected_error, expected_error_code, if_exists,
            False, True, timeout, analytics_timeout)

    def get_dataverse_obj(self, dataverse_name, database_name=None):
        """
        Return Dataverse/CBAS_scope object if dataverse with the required
        name already exists.
        :param dataverse_name str, fully qualified dataverse_name
        :param database_name str, fully qualifies database_name
        """
        if not database_name:
            database_obj = self.get_database_obj("Default")
        else:
            database_obj = self.get_database_obj(database_name)

        if database_obj and (dataverse_name in database_obj.dataverses):
            return database_obj.dataverses[dataverse_name]
        return None

    def get_crud_dataverse_obj(self, dataverse_name, database_name=None):
        if not database_name:
            database_obj = self.get_database_obj("Default")
        else:
            database_obj = self.get_database_obj(database_name)
        if database_obj and (dataverse_name in database_obj.crud_dataverses):
            return database_obj.crud_dataverses[dataverse_name]
        return None

    @staticmethod
    def get_dataverse_spec(cbas_spec):
        """
        Fetches dataverse specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictionary.
        """
        return cbas_spec.get("dataverse", {})

    def create_crud_dataverses(self, cluster, no_of_dataverses, timeout=300):
        """
        Create dataverses to perform create crud on collections
        """
        for i in range(0, no_of_dataverses):
            name = self.generate_name(name_cardinality=1)
            database = random.choice(list(self.databases.values()))
            dataverse = Dataverse(name)
            creation_method = random.choice(["dataverse", "analytics_scope",
                                             "scope"])
            if creation_method == "dataverse":
                status = self.create_dataverse(
                    cluster, dataverse.name, database.name, if_not_exists=True,
                    analytics_scope=False, scope=False, timeout=timeout,
                    analytics_timeout=300)
            elif creation_method == "analytics_scope":
                status = self.create_dataverse(
                    cluster, dataverse.name, database.name, if_not_exists=True,
                    analytics_scope=True, scope=False, timeout=timeout,
                    analytics_timeout=300)
            else:
                status = self.create_dataverse(
                    cluster, dataverse.name, database.name, if_not_exists=True,
                    analytics_scope=False, scope=True, timeout=timeout,
                    analytics_timeout=300)

            if not status:
                self.log.error("Failed to create dataverse: {}".format(name))
            else:
                database_obj = self.get_database_obj("Default")
                del database_obj.dataverses[dataverse.name]
                if not self.get_crud_dataverse_obj(dataverse.name):
                    database_obj.crud_dataverses[dataverse.name] = dataverse

    def create_dataverse_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating dataverses based on CBAS Spec")

        dataverse_spec = self.get_dataverse_spec(cbas_spec)

        # Create a dataverse in non-default database, since a non-default
        # database is created without a default dataverse. This logic is
        # required because if we randomly pick a database in any of the
        # create_from_spec methods and if the database does not have a
        # dataverse then it will throw index out of range exception.
        no_of_dataverses = dataverse_spec.get("no_of_dataverses", 1)
        for db in list(self.databases.values()):
            if db.name != "Default":
                name = self.generate_name(name_cardinality=1)
                if self.create_dataverse(
                        cluster=cluster, database_name=db.name,
                        dataverse_name=self.format_name(name),
                        if_not_exists=True, analytics_scope=False, scope=True,
                        timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                    no_of_dataverses -= 1
                else:
                    self.log.debug("Failed to create dataverse {0} in "
                                   "database {1}".format(name, db.name))
                    return False

        results = list()
        if no_of_dataverses > 1:
            for i in range(1, dataverse_spec.get("no_of_dataverses")):
                while True:
                    database = random.choice(list(self.databases.values()))
                    if (dataverse_spec.get(
                            "include_databases", []) and
                            self.unformat_name(database.name) not in
                            dataverse_spec.get("include_databases")):
                        database = None
                    if (dataverse_spec.get(
                            "exclude_databases", []) and
                            self.unformat_name(database.name) in
                            dataverse_spec.get("exclude_databases")):
                        database = None
                    if database:
                        break

                if dataverse_spec.get("name_key",
                                      "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = dataverse_spec.get("name_key") + "_{0}".format(
                        str(i))
                creation_method = dataverse_spec.get(
                    "creation_method", "all").lower()

                if creation_method == "all":
                    creation_method = random.choice(
                        ["dataverse", "analytics_scope", "scope"])

                if creation_method == "dataverse":
                    analytics_scope = False
                    scope = False
                elif creation_method == "analytics_scope":
                    analytics_scope = True
                    scope = False
                else:
                    analytics_scope = False
                    scope = True

                if self.create_dataverse(
                        cluster, self.format_name(name), database.name,
                        if_not_exists=True, analytics_scope=analytics_scope,
                        scope=scope, timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                    results.append(True)
                else:
                    results.append(False)
            return all(results)
        return True

    def get_all_dataverses_from_metadata(self, cluster):
        dataverse_query = "select value dv.DatabaseName || \".\" || " \
                          "dv.DataverseName from Metadata.`Dataverse` as dv " \
                          "where dv.DataverseName <> \"Metadata\";"
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, dataverse_query, mode="immediate", timeout=300,
            analytics_timeout=300)
        if status == 'success':
            return results
        return []

    def get_all_dataverse_obj(self, database=None):
        """
        Get all dataverse objects from every or the specfied database.
        """
        dataverses = list()
        if database:
            databases = [self.get_database_obj(database)]
        else:
            databases = list(self.databases.values())
        for database in databases:
            dataverses.extend(list(database.dataverses.values()))
        return dataverses


class Link_Util(Dataverse_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Link_Util, self).__init__(server_task, run_query_using_sdk)
        self.remote_links = dict()
        self.external_links = dict()
        self.kafka_links = dict()

    def validate_link_in_metadata(
            self, cluster, link_name, link_type="Local", username=None,
            password=None, is_active=False, timeout=300,
            analytics_timeout=300):
        """
        Validates whether a link is present in Metadata.Link
        :param cluster: cluster, cluster object
        :param link_name: str, Name of the link to be validated.
        :param link_type: str, type of link, valid values are Local, s3 or couchbase
        :param username : str
        :param password : str
        :param is_active : bool, verifies whether the link is active or not, valid only for
        Local and couchbase links
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        self.log.info("Validating link entry in Metadata")
        cmd = ("select value lnk from Metadata.`Link` as lnk where "
               "lnk.Name = \"{0}\" ").format(self.unformat_name(link_name))

        if link_type != "Local":
            cmd += " and lnk.`Type` = \"{0}\"".format(link_type.upper())
        cmd += ";"

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if status == "success":
            if results:
                if is_active:
                    if (link_type.lower() != "s3" or link_type.lower() != "azureblob") and results[0]["IsActive"]:
                        return True
                    else:
                        return False
                else:
                    return True
            else:
                return False
        else:
            return False

    def is_link_active(
            self, cluster, link_name, link_type="Local", username=None,
            password=None, timeout=300, analytics_timeout=300):
        """
        Validates whether a link is active or not. Valid only for
        Local and couchbase links
        :param cluster: cluster, cluster obj
        :param link_name: str, Name of the link whose status has to be checked.
        :param dataverse_name: str, dataverse name where the link is present
        :param database_name: str, database name where the link is present
        :param link_type: str, type of link, valid values are Local, s3 or couchbase
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        return self.validate_link_in_metadata(
            cluster, link_name, link_type,
            username, password, True, timeout, analytics_timeout)

    def create_link(
            self, cluster, link_properties, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, create_if_not_exists=False, timeout=300):
        """
        Creates remote/external Link.
        :param link_properties: dict, parameters required for creating link.
        Common for both AWS and couchbase link.
        Note - please use the exact key names as provided above in link properties dict.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param create_if_not_exists bool, check link exists before creation
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        cbas_helper = self.get_cbas_helper_object(cluster)

        exists = False
        if create_if_not_exists:
            exists = self.validate_link_in_metadata(
                cluster, link_properties["name"], link_properties["type"],
                username, password)

        if not exists:
            link_prop = copy.deepcopy(link_properties)

            if self.run_query_using_sdk:
                status, content, errors = cbas_helper.create_link(link_prop)
            else:
                params = dict()
                uri = ""

                if "name" in link_prop:
                    uri += "/{0}".format(urllib.parse.quote_plus(
                        self.unformat_name(link_prop["name"]), safe=""))
                    del link_prop["name"]

                for key, value in link_prop.items():
                    if value:
                        if isinstance(value, str):
                            params[key] = str(value)
                        else:
                            params[key] = value
                params = urllib.parse.urlencode(params)
                status, status_code, content, errors = cbas_helper.analytics_link_operations(
                    method="POST", uri=uri, params=params, timeout=timeout,
                    username=username, password=password)
            if validate_error_msg:
                return self.validate_error_in_response(
                    status, errors, expected_error, expected_error_code)
            if errors:
                self.log.error(str(errors))
            return status
        else:
            return exists

    def drop_link(self, cluster, link_name, username=None, password=None,
                  validate_error_msg=False, expected_error=None,
                  expected_error_code=None, if_exists=False,
                  timeout=300, analytics_timeout=300):
        """
        Drop Link.
        :param link_name: str, Name of the link to be dropped.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, check link exists before creation
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        cmd = "drop link {}".format(self.format_name(link_name))

        if if_exists:
            cmd += " if exists"
        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def get_link_info(
            self, cluster, link_name=None, link_type=None, username=None,
            password=None, timeout=300, validate_error_msg=False,
            expected_error=None, expected_error_code=None):
        """
        Fetch the list of links based on parameters passed.
        :param link_name (optional) The name of the link to be retrieved instead of
        retrieving all links. If this parameter is specified, the dataverse name
        has to be specified as well.
        :param link_type (optional) Link type (e.g. S3) to be retrieved,
        if not specified, links of all types will be retrieved (excluding the Local link)
        :param username : used for authentication while calling API.
        :param password : used for authentication while calling API.
        :param timeout : timeout for API response
        """
        cbas_helper = self.get_cbas_helper_object(cluster)
        if self.run_query_using_sdk:
            status, content, errors = cbas_helper.get_link_info(
                None, self.unformat_name(link_name), link_type)
        else:
            params = dict()
            uri = ""
            if link_name:
                uri += "/{0}".format(urllib.parse.quote_plus(
                    self.unformat_name(link_name), safe=""))
            if link_type:
                params["type"] = link_type
            params = urllib.parse.urlencode(params)
            status, status_code, content, errors = cbas_helper.analytics_link_operations(
                method="GET", uri=uri, params=params, timeout=timeout,
                username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        if status:
            return content

    def alter_link_properties(
            self, cluster, link_properties, username=None, password=None,
            timeout=300, validate_error_msg=False, expected_error=None,
            expected_error_code=None):
        """
        Alter all the link properties with the new values.
        :param link_properties: dict, parameters required for creating link.
        :param username : used for authentication while calling API.
        :param password : used for authentication while calling API.
        :param timeout : timeout for API response
        :param validate_error_msg : boolean, If set to true, it will compare the error raised while creating the link
        with the error msg or error code passed.
        :param expected_error : str, expected error string
        :param expected_error_code: str, expected error code
        """
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        link_prop = copy.deepcopy(link_properties)
        params = dict()
        uri = ""

        if "name" in link_prop:
            uri += "/{0}".format(urllib.parse.quote(
                self.unformat_name(link_prop["name"]), safe=""))
            del link_prop["name"]

        for key, value in link_prop.items():
            if value:
                if isinstance(value, str):
                    params[key] = str(value)
                else:
                    params[key] = value
        params = urllib.parse.urlencode(params)
        status, status_code, content, errors = cbas_helper.analytics_link_operations(
            method="PUT", uri=uri, params=params, timeout=timeout,
            username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        return status

    def connect_link(self, cluster, link_name, validate_error_msg=False,
                     with_force=True, username=None, password=None,
                     expected_error=None, expected_error_code=None,
                     timeout=300, analytics_timeout=300):
        """
        Connects a Link
        :param link_name: str, Name of the link to be connected.
        :param with_force: bool, use force flag while connecting a link
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate
        error raised with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        cmd_connect_link = "connect link {}".format(self.format_name(
            link_name))

        if not with_force:
            cmd_connect_link += " with {'force':false}"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd_connect_link, username=username,
            password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def disconnect_link(self, cluster, link_name, validate_error_msg=False,
                        username=None, password=None, expected_error=None,
                        expected_error_code=None, timeout=300,
                        analytics_timeout=300):
        """
        Disconnects a link
        :param link_name: str, Name of the link to be disconnected.
        :param disconnect_if_connected: bool, use force flag while connecting a link
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        cmd_disconnect_link = 'disconnect link {};'.format(self.format_name(
            link_name))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd_disconnect_link, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def get_link_obj(self, link_name):
        """
        Return Link object if the link with the required name already exists.
        If dataverse_name or link_type is not mentioned then it will return first
        link found with the link_name.
        :param link_name str, name of the link whose object has to returned.
        """
        all_link_objs = (self.remote_links + self.external_links +
                         self.kafka_links)
        return all_link_objs[self.format_name(link_name)]

    def get_all_link_objs(self, link_type=None):
        """
        Returns list of all link objects.
        :param link_type <str> s3, azureblob, gcp, kafka or couchbase, if None
        returns all link types
        """
        link_objs = list()
        if not link_type:
            link_objs.extend(self.remote_links.values())
            link_objs.extend(self.external_links.values())
            link_objs.extend(self.kafka_links.values())
        else:
            if link_type == "kafka":
                link_objs.extend(self.kafka_links.values())
            elif link_type == "couchbase":
                link_objs.extend(self.remote_links.values())
            else:
                for link in self.external_links.values():
                    if link.link_type == link_type.lower():
                        link_objs.append(link)
        return link_objs

    def validate_get_link_info_response(
            self, cluster, link_properties, username=None, password=None,
            timeout=300):
        response = self.get_link_info(
            cluster, link_properties["name"], link_properties["type"],
            username=username, password=password, timeout=timeout)
        if "secretAccessKey" in link_properties:
            link_properties["secretAccessKey"] = "<redacted sensitive entry>"
        entry_present = False
        for r in response:
            while not entry_present:
                if link_properties.viewitems() <= r.viewitems():
                    entry_present = True
            if entry_present:
                break
        return entry_present

    def get_all_links_from_metadata(self, cluster, link_type=None):
        links_query = "select value lnk.Name from Metadata.`Link` as lnk " \
                      "where lnk.Name <> \"Local\""
        if link_type:
            links_query += " and lnk.`Type` = \"{0}\"".format(link_type.upper())
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, links_query, mode="immediate",
            timeout=300, analytics_timeout=300)
        if status == 'success':
            return results
        return []


class RemoteLink_Util(Link_Util):
    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(RemoteLink_Util, self).__init__(
            server_task, run_query_using_sdk)

    def create_remote_link(
            self, cluster, link_properties, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, create_if_not_exists=False,
            timeout=300):
        """
        Creates a Link to remote couchbase cluster.
        :param link_properties: dict, parameters required for creating link.
        <Required> name : name of the link to be created.
        <Required> scope : name of the dataverse under which the link has to be created.
        <Required> type : couchbase
        <Required> hostname : The hostname of the link
        <Optional> username : The username for host authentication. Required if encryption is set to
        "none" or "half. Optional if encryption is set to "full".
        <Optional> password : The password for host authentication. Required if encryption is set to
        "none" or "half. Optional if encryption is set to "full".
        <Required> encryption : The link secure connection type ('none', 'full' or 'half')
        <Optional> certificate : The root certificate of target cluster for authentication.
        Required only if encryption is set to "full"
        <Optional> clientCertificate : The user certificate for authentication.
        Required only if encryption is set to "full" and username and password is not used.
        <Optional> clientKey : The client key for user authentication.
        Required only if encryption is set to "full" and username and password is not used.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param create_if_not_exists bool, check link exists before creation
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        self.log.info("Creating remote link {0} to remote cluster {1}".format(
            link_properties["name"], link_properties["hostname"]))
        return self.create_link(
            cluster, link_properties, username, password, validate_error_msg,
            expected_error, expected_error_code, create_if_not_exists,
            timeout)

    @staticmethod
    def get_remote_link_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("remote_link", {})

    def create_remote_link_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating Remote Links based on CBAS Spec")

        link_spec = self.get_remote_link_spec(cbas_spec)
        results = list()
        num_of_remote_links = link_spec.get("no_of_remote_links", 0)
        properties = link_spec.get("properties", [{}])
        properties_size = len(properties)

        for i in range(1, num_of_remote_links + 1):
            if link_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name()
            else:
                name = link_spec.get("name_key") + "_{0}".format(str(i))

            link = Remote_Link(
                name=name, properties=properties[i % properties_size])

            if not self.create_remote_link(
                    cluster, link.properties, create_if_not_exists=True,
                    timeout=cbas_spec.get("api_timeout", 300)):
                results.append(False)
            else:
                self.remote_links[link.name] = link
                results.append(True)
        return all(results)

    def create_remote_link_obj(
            self, cluster, hostname=None, username=None, password=None,
            encryption=None, certificate=None, clientCertificate=None,
            clientKey=None, no_of_objs=1, name_length=30, fixed_length=False):
        """
        Generates Remote Link objects.
        """
        count = 0
        while count < no_of_objs:
            link = Remote_Link(
                name=self.generate_name(
                    max_length=name_length, fixed_length=fixed_length),
                properties={
                    "type": "couchbase", "hostname": hostname,
                    "username": username, "password": password,
                    "encryption": encryption, "certificate": certificate,
                    "clientCertificate": clientCertificate,
                    "clientKey": clientKey})
            self.remote_links[link.name] = link
            count += 1

    def doc_operations_remote_collection_sirius(self, task_manager, collection_name, bucket_name, scope_name,
                                                connection_string, start, end, sdk_batch_size=25, doc_size=1024,
                                                template="product", username=None, password=None,
                                                sirius_url="http://127.0.0.1:4000", action="create"):
        database_information = CouchbaseLoader(username=username, password=password,
                                               connection_string=connection_string,
                                               bucket=bucket_name, scope=scope_name, collection=collection_name,
                                               sdk_batch_size=sdk_batch_size)
        operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template=template, doc_size=doc_size)
        op_type = SiriusCodes.DocOps.CREATE
        if action == "delete":
            op_type = SiriusCodes.DocOps.DELETE
        if action == "update":
            op_type = SiriusCodes.DocOps.UPDATE
        task = WorkLoadTask(bucket=bucket_name, task_manager=self.task, op_type=op_type,
                            database_information=database_information, operation_config=operation_config,
                            default_sirius_base_url=sirius_url)
        task_manager.add_new_task(task)
        task_manager.get_task_result(task)
        return task


class ExternalLink_Util(RemoteLink_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(ExternalLink_Util, self).__init__(
            server_task, run_query_using_sdk)

    def create_external_link(
            self, cluster, link_properties, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, create_if_not_exists=False,
            timeout=300):
        """
        Creates a Link to external data source.
        :param link_properties: dict, parameters required for creating link.
        <Required> name : name of the link to be created.
        <Required> scope : name of the dataverse under which the link has to be created.
        <Required> type : s3
        <Required> accessKeyId : The access key of the link
        <Required> secretAccessKey : The secret key of the link
        <Required> region : The region of the link
        <Optional> serviceEndpoint : The service endpoint of the link.
        Note - please use the exact key names as provided above in link properties dict.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param create_if_not_exists bool, check link exists before creation
        :param timeout int, REST API timeout
        """
        self.log.info(
            "Creating link - {0} in region {1}".format(
                link_properties["name"], link_properties["region"]))
        return self.create_link(
            cluster, link_properties, username, password, validate_error_msg,
            expected_error, expected_error_code, create_if_not_exists,
            timeout)

    @staticmethod
    def get_external_link_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("external_link", {})

    def create_external_link_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating External Links based on CBAS Spec")

        link_spec = self.get_external_link_spec(cbas_spec)
        results = list()
        num_of_external_links = link_spec.get("no_of_external_links", 0)
        properties = link_spec.get("properties", [{}])
        properties_size = len(properties)
        for i in range(0, num_of_external_links):
            if link_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = link_spec.get("name_key") + "_{0}".format(str(i))

            link = External_Link(
                name=name, properties=properties[i % properties_size])

            if not self.create_external_link(
                    cluster, link.properties, create_if_not_exists=True,
                    timeout=cbas_spec.get("api_timeout", 300)):
                results.append(False)
            else:
                self.external_links[link.name] = link
                results.append(True)
        return all(results)

    def create_external_link_obj(
            self, cluster, accessKeyId=None, secretAccessKey=None, regions=[],
            serviceEndpoint=None, link_type="s3", no_of_objs=1, name_length=30,
            fixed_length=False, link_perm=False):
        """
        Generates External Link objects.
        """
        external_link_list = []
        count = 0
        while count < no_of_objs:

            if link_type.lower() == "s3":
                link = External_Link(
                    name=self.generate_name(
                        name_cardinality=1, max_length=name_length,
                        fixed_length=fixed_length),
                    properties={"type": "s3", "accessKeyId": accessKeyId,
                                "secretAccessKey": secretAccessKey,
                                "region": random.choice(regions),
                                "serviceEndpoint": serviceEndpoint})
            elif link_type.lower() == "azureblob":
                if not link_perm:
                    link = External_Link(
                        name=self.generate_name(
                            name_cardinality=1, max_length=name_length,
                            fixed_length=fixed_length),
                        properties={"type": "azureblob",
                                    "endpoint": serviceEndpoint,
                                    "accountName": accessKeyId,
                                    "accountKey": secretAccessKey,
                                    })
                else:
                    link = External_Link(
                        name=self.generate_name(
                            name_cardinality=1, max_length=name_length,
                            fixed_length=fixed_length),
                        properties={"type": "azureblob",
                                    "endpoint": serviceEndpoint,
                                    })
            self.external_links[link.name] = link
            external_link_list.append(link)
            count += 1
        return external_link_list


class KafkaLink_Util(ExternalLink_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(KafkaLink_Util, self).__init__(server_task, run_query_using_sdk)

    def create_kafka_link(
            self, cluster, link_name, kafka_cluster_details,
            schema_registry_details=None, validate_error_msg=False,
            username=None, password=None, expected_error=None, timeout=300,
            analytics_timeout=300):
        """
        Creates a dataset/analytics collection on a KV bucket.
        :param link_name str, fully qualified dataset name.
        :param kafka_cluster_details dict, details of the kafka cluster
        that will be used to stream data from external database.
        :param schema_registry_details dict, details of the schema registry.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """

        cmd = "CREATE LINK {0} TYPE `kafka-sink` WITH ".format(
            CBASHelper.format_name(link_name))

        source_details = {"kafkaClusterDetails": kafka_cluster_details}
        if schema_registry_details:
            source_details["schemaRegistryDetails"] = schema_registry_details
        cmd += json.dumps(source_details)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def alter_kafka_link(self):
        pass

    @staticmethod
    def get_kafka_link_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("kafka_link", {})

    def create_kafka_link_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating Kafka Links based on CBAS Spec")

        link_spec = self.get_kafka_link_spec(cbas_spec)
        results = list()
        num_of_kafka_links = link_spec.get("no_of_kafka_links", 0)
        vendors = link_spec.get("vendors", [])
        if not vendors:
            vendors = ["confluent", "aws_msk"]

        for i in range(0, num_of_kafka_links):
            if link_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = link_spec.get("name_key") + "_{0}".format(str(i))

            vendor = vendors[i % len(vendors)]
            kafka_cluster_details = link_spec.get("kafka_cluster_details")[
                vendor]
            kafka_cluster_detail = kafka_cluster_details[
                i % len(kafka_cluster_details)]

            schema_registry_details = link_spec.get("schema_registry_details")[
                vendor]
            if schema_registry_details:
                schema_registry_detail = schema_registry_details[
                    i % len(schema_registry_details)]
            else:
                schema_registry_detail = None

            link = Kafka_Link(
                name=name, kafka_type=vendor,
                kafka_cluster_details=kafka_cluster_detail,
                schema_registry_details=schema_registry_detail)

            if not self.create_kafka_link(
                    cluster, name, kafka_cluster_detail,
                    schema_registry_detail,
                    timeout=cbas_spec.get("api_timeout", 300),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                results.append(False)
            else:
                self.kafka_links[link.name] = link
                results.append(True)
        return all(results)

    def create_kafka_link_obj(
            self, vendors, kafka_cluster_details,
            schema_registry_details={}, no_of_objs=1, name_length=30,
            fixed_length=False):
        """
        Generates Kafka Link objects.
        vendors <list> List of kafka vendors. Accepted values are confluent
        and aws_msk
        kafka_cluster_details <dict> Dict of List of kakfa cluster detail objects
        schema_registry_details <dict> Dict of List of schema registry detail objects
        """
        links = list()
        for i in range(0, no_of_objs):
            vendor = vendors[i % len(vendors)]
            kafka_cluster_detail = kafka_cluster_details[vendor][
                i % len(kafka_cluster_details[vendor])]
            schema_registry_detail = schema_registry_details[vendor][
                i % len(schema_registry_details[vendor])]

            link = Kafka_Link(
                name=self.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=fixed_length),
                kafka_type=vendor, kafka_cluster_details=kafka_cluster_detail,
                schema_registry_details=schema_registry_detail)
            links.append(link)
        return links


class Dataset_Util(KafkaLink_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Dataset_Util, self).__init__(
            server_task, run_query_using_sdk)

    def validate_dataset_in_metadata(
            self, cluster, dataset_name, dataverse_name=None,
            database_name=None, username=None, password=None, timeout=300,
            analytics_timeout=300, **kwargs):
        """
        validates metadata information about a dataset with entry in
        Metadata.Dataset collection.
        :param dataset_name str, name of the dataset to be validated.
        :param dataverse_name str, name of the dataverse where the dataset
        is present.
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param **kwargs dict, other Metadata attributes that needs to be
        validated.
        """
        self.log.debug("Validating dataset entry in Metadata")
        cmd = 'SELECT value MD FROM Metadata.`Dataset` as MD WHERE ' \
              'DatasetName="{0}"'.format(
            CBASHelper.unformat_name(dataset_name))
        if dataverse_name:
            cmd += ' and DataverseName = "{0}"'.format(
                CBASHelper.metadata_format(dataverse_name))
        if database_name:
            cmd += ' and DatabaseName = "{0}"'.format(
                CBASHelper.metadata_format(database_name))
        cmd += ";"
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if status == "success" and results:
            for result in results:
                if result["DatasetType"] == "INTERNAL":
                    for key, value in kwargs.items():
                        if value != result.get(key, ''):
                            self.log.error(
                                "Data Mismatch. Expected - {0} /t Actual - {1}".format(
                                    result.get(key, ''), value))
                            return False
                elif result["DatasetType"] == "EXTERNAL":
                    for prop in results["ExternalDetails"]["Properties"]:
                        if prop["Name"] == "container":
                            actual_link_name = prop["Value"]
                        if prop["Name"] == "name":
                            actual_bucket_name = prop["Value"]
                    if actual_link_name != kwargs["link_name"]:
                        self.log.error(
                            "Link name mismatch. Expected - {0} /t Actual - {1}".format(
                                kwargs["link_name"], actual_link_name))
                        return False
                    if actual_bucket_name != kwargs["bucket_name"]:
                        self.log.error(
                            "Bucket name mismatch. Expected - {0} /t Actual - {1}".format(
                                kwargs["bucket_name"], actual_bucket_name))
                        return False
            return True
        else:
            return False

    def generate_create_dataset_cmd(self, dataset_name, kv_entity, dataverse_name=None,
                                    database_name=None, if_not_exists=False, compress_dataset=False,
                                    with_clause=None, link_name=None, where_clause=None,
                                    analytics_collection=False, storage_format=None):

        if analytics_collection:
            cmd = "create analytics collection"
        else:
            cmd = "create dataset"

        if if_not_exists:
            cmd += " if not exists"

        if dataverse_name and database_name:
            cmd += " {0}.{1}.{2}".format(database_name, dataverse_name, dataset_name)
        elif dataverse_name:
            cmd += " {0}.{1}".format(dataverse_name, dataset_name)
        else:
            cmd += " {0}".format(dataset_name)

        if with_clause or compress_dataset or storage_format:
            with_params = dict()

            if compress_dataset:
                with_params["storage-block-compression"] = {'scheme': 'snappy'}

            if storage_format:
                with_params["storage-format"] = {"format": storage_format}

            cmd += " with " + json.dumps(with_params) + " "

        cmd += " on {0}".format(kv_entity)

        if link_name:
            cmd += " at {0}".format(link_name)

        if where_clause:
            cmd += " where " + where_clause

        cmd += ";"

        return cmd

    def create_dataset(
            self, cluster, dataset_name, kv_entity, dataverse_name=None,
            database_name=None, if_not_exists=False, compress_dataset=False,
            with_clause=None, link_name=None, where_clause=None,
            validate_error_msg=False, username=None, password=None,
            expected_error=None, timeout=300, analytics_timeout=300,
            analytics_collection=False, storage_format=None):
        """
        Creates a dataset/analytics collection on a KV bucket.
        :param dataset_name str, fully qualified dataset name.
        :param kv_entity str, fully qualified KV entity name, can be only bucket name or
        bucket.scope.collection.
        :param dataverse_name str, Dataverse where dataset is to be created.
        :param if_not_exists bool, if this flag is set then, if a dataset with same name is present
        in the same dataverse, then the create statement will pass without creating a new dataset.
        :param compress_dataset bool, use to set compression policy for dataset.
        :param with_clause str, use to set other conditions apart from compress dataset, can be of format
        { "merge-policy": {"name": <>, "parameters": {"max-mergable-component-size": <>,"max-tolerance-component-count": <>}}}
        :param link_name str, used only while creating dataset on remote cluster, specifies link to be used to ingest data.
        :param where_clause str, used to filter content while ingesting docs from KV, format - `field_name`="field_value"
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param analytics_collection bool, If True, will use create analytics collection syntax
        :param storage_format string, whether to use row or column storage for analytics datasets. Valid values are
        row and column
        """

        cmd = self.generate_create_dataset_cmd(dataset_name, kv_entity, dataverse_name,
                                               database_name, if_not_exists, compress_dataset,
                                               with_clause, link_name, where_clause,
                                               analytics_collection, storage_format)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_analytics_collection(
            self, cluster, dataset_name, kv_entity, dataverse_name=None,
            database_name=None, if_not_exists=False, compress_dataset=False,
            with_clause=None, link_name=None, where_clause=None,
            validate_error_msg=False, username=None, password=None,
            expected_error=None, timeout=300, analytics_timeout=300,
            storage_format=None):
        """
        Creates a analytics collection which is syntactic sugar for creating datasets.
        :param dataset_name str, fully qualified dataset name.
        :param kv_entity str, fully qualified KV entity name, can be only bucket name or
        bucket.scope.collection.
        :param dataverse_name str, Dataverse where dataset is to be created.
        :param if_not_exists bool, if this flag is set then, if a dataset with same name is present
        in the same dataverse, then the create statement will pass without creating a new dataset.
        :param compress_dataset bool, use to set compression policy for dataset.
        :param with_clause str, use to set other conditions apart from compress dataset, can be of format
        { "merge-policy": {"name": <>, "parameters": {"max-mergable-component-size": <>,"max-tolerance-component-count": <>}}}
        :param link_name str, used only while creating dataset on remote cluster, specifies link to be used to ingest data.
        :param where_clause str, used to filter content while ingesting docs from KV, format - `field_name`="field_value"
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param storage_format string, whether to use row or column storage for analytics datasets. Valid values are
        row and column
        """
        return self.create_dataset(
            cluster, dataset_name, kv_entity, dataverse_name, database_name,
            if_not_exists, compress_dataset, with_clause, link_name, where_clause,
            validate_error_msg, username, password, expected_error,
            timeout, analytics_timeout, True, storage_format)

    def drop_dataset(
            self, cluster, dataset_name, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_exists=False,
            analytics_collection=False, timeout=300, analytics_timeout=300):
        """
        Drops the dataset.
        :param dataset_name: str, dataset to be droppped
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use IfExists flag to check dataverse exists before deletion,
        if a dataverse does not exists the query return successfully without deleting any dataverse.
        :param analytics_collection bool, If True, will use create analytics collection syntax
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """

        if analytics_collection:
            cmd = "drop analytics collection %s" % dataset_name
        else:
            cmd = "drop dataset %s" % dataset_name

        if if_exists:
            cmd += " if exists"
        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def drop_analytics_collection(
            self, cluster, dataset_name, username=None, password=None,
            validate_error_msg=False, expected_error=None, expected_error_code=None,
            if_exists=False, timeout=300, analytics_timeout=300):
        """
        Drop a analytics collection which is syntactic sugar for dropping datasets.
        :param dataset_name: str, dataset to be droppped
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use IfExists flag to check dataverse exists before deletion,
        if a dataverse does not exists the query return successfully without deleting any dataverse.
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        return self.drop_dataset(
            cluster, dataset_name, username, password, validate_error_msg,
            expected_error, expected_error_code, if_exists, True, timeout,
            analytics_timeout)

    def get_num_items_in_cbas_dataset(
            self, cluster, dataset_name, timeout=300, analytics_timeout=300):
        """
        Gets the count of docs in the cbas dataset
        """
        total_items = -1
        cmd_get_num_items = "select count(*) from %s;" % dataset_name

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(
                cluster, cmd_get_num_items, timeout=timeout,
                analytics_timeout=analytics_timeout)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.debug("No. of items in CBAS dataset {0} : {1}"
                           .format(dataset_name, results[0]['$1']))
            total_items = results[0]['$1']

        return total_items

    def wait_for_ingestion_complete(
            self, cluster, dataset_name, num_items, timeout=300):

        start_time = time.time()
        end_time = start_time + timeout
        while end_time > time.time():
            self.log.debug("Total items in KV Bucket to be ingested "
                           "in CBAS datasets %s" % num_items)
            actual_doc_count = self.get_num_items_in_cbas_dataset(cluster, dataset_name)
            if num_items == actual_doc_count:
                self.log.debug("Data ingestion completed in %s seconds." %
                               (time.time() - start_time))
                return True
            else:
                sleep(2)

        self.log.error(
            "Dataset: {0} Expected Doc Count: {1} Actual Doc Count: "
            "{2}".format(dataset_name, num_items, actual_doc_count))

        return False

    def wait_for_ingestion_all_datasets(self, cluster, timeout=600):

        jobs = Queue()
        results = []
        datasets = self.get_all_dataset_objs()
        if len(datasets) > 0:
            self.log.info("Waiting for data to be ingested into datasets")
            for dataset in datasets:
                jobs.put((
                    self.wait_for_ingestion_complete,
                    {"cluster": cluster, "dataset_name": dataset.full_name,
                     "num_items": dataset.num_of_items, "timeout": timeout}))
        self.run_jobs_in_parallel(jobs, results, 15, async_run=False)
        return all(results)

    def validate_cbas_dataset_items_count(
            self, cluster, dataset_name, expected_count,
            num_tries=12, timeout=300, analytics_timeout=300):
        """
        Compares the count of CBAS dataset total items with the expected
        values.
        """
        count = self.get_num_items_in_cbas_dataset(
            cluster, dataset_name, timeout=timeout,
            analytics_timeout=analytics_timeout)
        tries = num_tries

        while count != expected_count and tries > 0:
            sleep(10)
            count = self.get_num_items_in_cbas_dataset(
                cluster, dataset_name, timeout=timeout,
                analytics_timeout=analytics_timeout)
            tries -= 1

        self.log.debug("Expected Count: %s, Actual Count: %s"
                       % (expected_count, count))

        if count == expected_count:
            return True
        elif count != expected_count:
            return False

    def get_dataset_compression_type(self, cluster, dataset_name):
        query = ("select raw BlockLevelStorageCompression."
                 "DatasetCompressionScheme from Metadata.`Dataset` "
                 "where DatasetName='{0}';").format(
            CBASHelper.unformat_name(dataset_name))
        _, _, _, ds_compression_type, _ = self.execute_statement_on_cbas_util(
            cluster, query)
        ds_compression_type = ds_compression_type[0]
        if ds_compression_type is not None:
            ds_compression_type = ds_compression_type.encode('ascii', 'ignore')
        self.log.info(
            "Compression Type for Dataset {0} is {1}".format(dataset_name,
                                                             ds_compression_type))

        return ds_compression_type

    def get_all_dataset_objs(self, dataset_source=None):
        """
        :param dataset_source <str> Accepted values are dataset, remote,
        external, standalone
        Returns list of all Dataset/CBAS_Collection objects across all the dataverses.
        """
        dataset_objs = list()
        for database in list(self.databases.values()):
            for dataverse in database.dataverses.values():
                if not dataset_source:
                    dataset_objs.extend(dataverse.datasets.values())
                    dataset_objs.extend(dataverse.remote_datasets.values())
                    dataset_objs.extend(dataverse.external_datasets.values())
                    dataset_objs.extend(dataverse.standalone_datasets.values())
                else:
                    if dataset_source == "dataset":
                        dataset_objs.extend(dataverse.datasets.values())
                    elif dataset_source == "remote":
                        dataset_objs.extend(dataverse.remote_datasets.values())
                    elif dataset_source == "external":
                        dataset_objs.extend(dataverse.external_datasets.values())
                    elif dataset_source == "standalone":
                        dataset_objs.extend(dataverse.standalone_datasets.values())
        return dataset_objs

    def validate_docs_in_all_datasets(
            self, cluster, timeout=600):
        datasets = self.get_all_dataset_objs()
        jobs = Queue()
        results = list()

        for dataset in datasets:
            jobs.put((self.wait_for_ingestion_complete,
                      {"cluster": cluster, "dataset_name": dataset.full_name,
                       "num_items": dataset.num_of_items, "timeout": timeout}))

        self.run_jobs_in_parallel(jobs, results, 50, async_run=False)
        return all(results)

    def refresh_remote_dataset_item_count(self, bucket_util):
        """
        Method to refresh dataset doc count based on kv collection doc
        count, if TAF doc loader is used to load data into remote collection
        """
        datasets = self.get_all_dataset_objs("remote")
        for dataset in datasets:
            if dataset.kv_collection:
                dataset.num_of_items = dataset.kv_collection.num_items
            else:
                dataset.num_of_items = bucket_util.get_collection_obj(
                    bucket_util.get_scope_obj(dataset.kv_bucket,
                                              "_default"),
                    "_default").num_items

    def get_all_datasets_from_metadata(self, cluster, fields=[]):
        datasets_created = []
        datasets_query = "select value ds.DatabaseName || \".\" || " \
                         "ds.DataverseName || \".\" || ds.DatasetName from " \
                         "Metadata.`Dataset` as ds where ds.DataverseName  " \
                         "<> \"Metadata\";"
        if fields:
            datasets_query = 'SELECT * ' \
                             'FROM Metadata.`Dataset` d ' \
                             'WHERE d.DataverseName <> "Metadata"'
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, datasets_query, mode="immediate", timeout=300,
            analytics_timeout=300)
        if status == 'success':
            if fields:
                results = CBASHelper.get_json(json_data=results)
                for result in results:
                    ds = result['d']
                    datasets_created.append(
                        {field: ds[field] for field in fields})
            else:
                return results
        return datasets_created

    def create_datasets_for_tpch(self, cluster):
        result = True
        for bucket in cluster.buckets:
            result = result and self.create_dataset(cluster, bucket.name, bucket.name)
        return result


class Remote_Dataset_Util(Dataset_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Remote_Dataset_Util, self).__init__(
            server_task, run_query_using_sdk)

    def create_remote_dataset(
            self, cluster, dataset_name, kv_entity, link_name,
            dataverse_name=None, database_name=None, if_not_exists=False,
            compress_dataset=False, with_clause=None, where_clause=None,
            storage_format=None, analytics_collection=False,
            validate_error_msg=False, username=None, password=None,
            expected_error=None, timeout=300, analytics_timeout=300):
        """
        Creates remote datasets.
        :param dataset_name str, fully qualified dataset name.
        :param kv_entity str, fully qualified KV entity name, can be only bucket name or
        bucket.scope.collection.
        :param dataverse_name str, Dataverse where dataset is to be created.
        :param if_not_exists bool, if this flag is set then, if a dataset with same name is present
        in the same dataverse, then the create statement will pass without creating a new dataset.
        :param compress_dataset bool, use to set compression policy for dataset.
        :param with_clause str, use to set other conditions apart from compress dataset, can be of format
        { "merge-policy": {"name": <>, "parameters": {"max-mergable-component-size": <>,"max-tolerance-component-count": <>}}}
        :param link_name str, used only while creating dataset on remote cluster, specifies link to be used to ingest data.
        :param where_clause str, used to filter content while ingesting docs from KV, format - `field_name`="field_value"
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param analytics_collection bool, If True, will use create analytics collection syntax
        :param storage_format string, whether to use row or column storage for analytics datasets. Valid values are
        row and column
        """

        return self.create_dataset(
            cluster, dataset_name, kv_entity, dataverse_name, database_name,
            if_not_exists, compress_dataset, with_clause,
            link_name, where_clause, validate_error_msg,
            username, password, expected_error, timeout,
            analytics_timeout, analytics_collection, storage_format)

    @staticmethod
    def get_remote_dataset_spec(cbas_spec):
        """
        Fetches remote dataset specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("remote_dataset", {})

    def create_remote_dataset_obj(
            self, cluster, bucket, scope, collection, link=None,
            bucket_util=None, use_only_existing_db=False,
            use_only_existing_dv=False, storage_format=None, name_length=30,
            fixed_length=False, no_of_objs=1, capella_as_source=False):
        """
        Generates remote dataset objects.
        """

        if storage_format == "mixed":
            storage_format = random.choice(["row", "column"])

        def get_database_dataverse_link(link):
            if not link:
                link = random.choice(self.get_all_link_objs("couchbase"))

            if use_only_existing_db:
                database_obj = random.choice(list(self.databases.values()))
            else:
                database_name = self.generate_name()
                if not self.create_database(cluster, database_name):
                    self.log.error(
                        "Error while creating database {0}".format(
                            database_name))
                database_obj = self.get_database_obj(self.format_name(
                    database_name))
                dataverse_name = self.generate_name()
                if not self.create_dataverse(
                        cluster, dataverse_name, database_obj.name):
                    self.log.error(
                        "Error while creating dataverse {0} in "
                        "database {1}".format(
                            dataverse_name, database_obj.name))

            if use_only_existing_dv:
                dataverse_obj = random.choice(self.get_all_dataverse_obj(
                    database_obj.name))
            else:
                dataverse_name = self.generate_name()
                if not self.create_dataverse(
                        cluster, dataverse_name, database_obj.name):
                    self.log.error(
                        "Error while creating dataverse {0} in "
                        "database {1}".format(
                            dataverse_name, database_obj.name))
                dataverse_obj = self.get_dataverse_obj(
                    self.format_name(dataverse_name),
                    database_obj.name)

            return database_obj, dataverse_obj, link

        def data_source_capella(dataset_name, bucket, scope, collection, link):
            if not (bucket or scope or collection):
                self.log.error("Please provide bucket, scope and collection")
                return False

            database, dataverse, link = get_database_dataverse_link(link)

            dataset_obj = Remote_Dataset(
                name=dataset_name, link_name=link.name,
                dataverse_name=dataverse.name, database_name=database.name,
                bucket=bucket, scope=scope, collection=collection,
                storage_format=storage_format, capella_as_source=True)
            self.log.info("Adding Remote Dataset object for - {0}".format(
                dataset_obj.full_name))
            dataverse.remote_datasets[dataset_obj.name] = dataset_obj
            return dataset_obj

        def data_source_on_prem(dataset_name, bucket, scope, collection, link):

            if not scope:
                scope = bucket_util.get_scope_obj(bucket, "_default")
            if not collection:
                collection = bucket_util.get_collection_obj(scope, "_default")

            database, dataverse, link = get_database_dataverse_link(link)

            dataset_obj = Remote_Dataset(
                name=dataset_name, link_name=link.name,
                dataverse_name=dataverse.name, database_name=database.name,
                bucket=bucket, scope=scope, collection=collection,
                num_of_items=collection.num_items,
                storage_format=storage_format, capella_as_source=False)

            self.log.info("Adding Remote Dataset object for - {0}".format(
                dataset_obj.full_name))
            dataverse.remote_datasets[dataset_obj.name] = dataset_obj
            return dataset_obj

        dataset_objs = list()
        for _ in range(no_of_objs):
            dataset_name = self.generate_name(
                name_cardinality=1, max_length=name_length,
                fixed_length=fixed_length)
            if capella_as_source:
                dataset_objs.append((data_source_capella(
                    dataset_name, bucket, scope, collection, link)))
            else:
                dataset_objs.append((data_source_on_prem(
                    dataset_name, bucket, scope, collection, link)))
        return dataset_objs

    def create_remote_dataset_from_spec(
            self, cluster, remote_clusters, cbas_spec, bucket_util):
        """
        Creates remote datasets based on remote dataset specs.
        """
        self.log.info("Creating Remote Datasets based on CBAS Spec")

        dataset_spec = self.get_remote_dataset_spec(cbas_spec)
        results = list()

        # get all remote link objects
        remote_link_objs = set(self.get_all_link_objs("couchbase"))
        if dataset_spec.get("include_links", []):
            for link_name in dataset_spec["include_links"]:
                remote_link_objs |= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name), "couchbase")])

        if dataset_spec.get("exclude_links", []):
            for link_name in dataset_spec["exclude_links"]:
                remote_link_objs -= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name), "couchbase")])

        remote_link_objs = list(remote_link_objs)

        num_of_remote_datasets = dataset_spec.get("num_of_remote_datasets", 0)
        if num_of_remote_datasets == 0:
            return True

        kv_collection_used = {}
        # create a map of cluster and all their KV collections
        all_kv_collection_remote_cluster_map = {}
        for remote_cluster in remote_clusters:
            kv_collection_used[remote_cluster.name] = []
            all_kv_collection_remote_cluster_map[remote_cluster.name] = []
            for bucket in remote_cluster.buckets:
                if bucket.name in dataset_spec.get("exclude_buckets", []):
                    continue
                else:
                    for scope in bucket_util.get_active_scopes(bucket):
                        if scope.name in dataset_spec.get(
                                "exclude_scopes", []):
                            continue
                        else:
                            for collection in bucket_util.get_active_collections(
                                    bucket, scope.name):
                                if collection.name in dataset_spec.get(
                                        "exclude_collections", []):
                                    continue
                                else:
                                    all_kv_collection_remote_cluster_map[
                                        remote_cluster.name].append(
                                        (bucket, scope, collection))

        for i in range(0, num_of_remote_datasets):

            database = None
            while not database:
                database = random.choice(list(self.databases.values()))
                if dataset_spec.get(
                        "include_databases", []) and CBASHelper.unformat_name(
                    database.name) not in dataset_spec.get(
                    "include_databases"):
                    database = None
                if dataset_spec.get(
                        "exclude_databases", []) and CBASHelper.unformat_name(
                    database.name) in dataset_spec.get("exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))
                if dataset_spec.get("include_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) not in dataset_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if dataset_spec.get("exclude_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) in dataset_spec.get(
                    "exclude_dataverses"):
                    dataverse = None

            if dataset_spec.get(
                    "name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = dataset_spec["name_key"] + "_{0}".format(
                    str(i))

            if not dataset_spec.get("creation_methods", []):
                dataset_spec["creation_methods"] = [
                    "cbas_collection", "cbas_dataset"]
            creation_method = random.choice(
                dataset_spec["creation_methods"])

            link = remote_link_objs[i % len(remote_link_objs)]
            remote_cluster = None
            for tmp_cluster in remote_clusters:
                if (tmp_cluster.master.ip == link.properties["hostname"]
                        or tmp_cluster.srv == link.properties["hostname"]):
                    remote_cluster = tmp_cluster
                    break

            # Logic to ensure remote dataset is made on all collections of a
            # bucket.
            while True:
                kv_entity = random.choice(all_kv_collection_remote_cluster_map[
                                              remote_cluster.name])
                if kv_entity not in kv_collection_used[remote_cluster.name]:
                    kv_collection_used[remote_cluster.name].append(kv_entity)
                    break
                else:
                    if len(kv_collection_used[remote_cluster.name]) == len(
                            all_kv_collection_remote_cluster_map[
                                remote_cluster.name]):
                        kv_collection_used[remote_cluster.name] = [kv_entity]
                        break

            bucket, scope, collection = kv_entity
            num_of_items = collection.num_items

            if dataset_spec["storage_format"] == "mixed":
                storage_format = random.choice(["row", "column"])
            else:
                storage_format = dataset_spec["storage_format"]

            dataset_obj = Remote_Dataset(
                name=name, link_name=link.name, dataverse_name=dataverse.name,
                database_name=dataverse.database_name, bucket=bucket,
                scope=scope, collection=collection, num_of_items=num_of_items,
                storage_format=storage_format)

            dataverse_name = dataset_obj.dataverse_name
            if dataverse_name == "Default":
                dataverse_name = None

            if creation_method == "cbas_collection":
                analytics_collection = True
            else:
                analytics_collection = False
            results.append(
                self.create_remote_dataset(
                    cluster, dataset_obj.name,
                    dataset_obj.full_kv_entity_name, link.name,
                    dataverse_name, dataset_obj.database_name, False, False,
                    None, None, storage_format, analytics_collection,
                    False, None, None, None,
                    timeout=cbas_spec.get("api_timeout",
                                          300),
                    analytics_timeout=cbas_spec.get(
                        "cbas_timeout", 300)))
            if results[-1]:
                dataverse.remote_datasets[
                    dataset_obj.name] = dataset_obj
        return all(results)


class External_Dataset_Util(Remote_Dataset_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(External_Dataset_Util, self).__init__(
            server_task, run_query_using_sdk)

    @staticmethod
    def get_external_dataset_spec(cbas_spec):
        """
        Fetches external dataset specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("external_dataset", {})

    def generate_create_external_dataset_cmd(self, dataset_name, external_container_name,
                                             link_name, if_not_exists=False, dataverse_name=None,
                                             database_name=None, object_construction_def=None,
                                             path_on_external_container=None, file_format="json",
                                             redact_warning=None, header=None, null_string=None, include=None,
                                             exclude=None, parse_json_string=0, convert_decimal_to_double=0,
                                             timezone="", embed_filter_values=None):

        cmd = "CREATE EXTERNAL DATASET"

        if if_not_exists:
            cmd += " if not exists"

        if database_name and dataverse_name:
            cmd += " {0}.{1}.{2}".format(
                database_name, dataverse_name, dataset_name)
        elif dataverse_name:
            cmd += " {0}.{1}".format(dataverse_name, dataset_name)
        else:
            cmd += " {0}".format(dataset_name)

        if object_construction_def:
            cmd += "({0})".format(object_construction_def)

        cmd += " ON `{0}` AT {1}".format(external_container_name, link_name)

        if path_on_external_container is not None:
            cmd += " USING \"{0}\"".format(path_on_external_container)

        with_parameters = dict()
        with_parameters["format"] = file_format

        if redact_warning is not None:
            with_parameters["redact-warnings"] = redact_warning

        if header is not None:
            with_parameters["header"] = header

        if null_string:
            with_parameters["null"] = null_string

        if include is not None:
            with_parameters["include"] = include

        if exclude is not None:
            with_parameters["exclude"] = exclude

        if with_parameters["format"] == "parquet":
            if timezone:
                with_parameters["timezone"] = timezone.upper()
            if parse_json_string > 0:
                if parse_json_string == 1:
                    with_parameters["parse-json-string"] = True
                else:
                    with_parameters["parse-json-string"] = False
            if convert_decimal_to_double > 0:
                if convert_decimal_to_double == 1:
                    with_parameters["decimal-to-double"] = True
                else:
                    with_parameters["decimal-to-double"] = False
        if embed_filter_values:
            with_parameters["embed-filter-values"] = embed_filter_values

        cmd += " WITH {0};".format(json.dumps(with_parameters))

        return cmd

    def create_dataset_on_external_resource(
            self, cluster, dataset_name, external_container_name,
            link_name, if_not_exists=False, dataverse_name=None,
            database_name=None, object_construction_def=None,
            path_on_external_container=None, file_format="json",
            redact_warning=None, header=None, null_string=None, include=None,
            exclude=None, parse_json_string=0, convert_decimal_to_double=0,
            timezone="", validate_error_msg=False, username=None,
            password=None, expected_error=None, expected_error_code=None,
            timeout=300, analytics_timeout=300, embed_filter_values=None):
        """
        Creates a dataset for an external resource like AWS S3 bucket /
        Azure container / GCP buckets.
        Note - No shadow dataset is created for this type of external datasets.
        :param dataset_name (str) : Name for the dataset to be created.
        :param external_container_name (str): AWS S3 bucket / Azure
        container/ GCP bukcet name from where the data will be read
        into the dataset. S3 bucket should be in the same region as the
        link, that is used to create this dataset.
        :param dataverse_name str, Dataverse where dataset is to be created.
        :param if_not_exists bool, if this flag is set then, if a dataset with same name is present
        in the same dataverse, then the create statement will pass without creating a new dataset.
        :param link_name (str): external link to AWS S3
        :param object_construction_def (str): It defines how the data read will be parsed.
        Required only for csv and tsv formats.
        :param path_on_external_container (str): Relative path in external
        container on AWS/Azure/GCP, from where the files will be read.
        :param file_format (str): Type of files to read. Valid values -
        json, csv, tsv and parquet
        :param redact_warning (bool): internal information like e.g. filenames are redacted from warning messages.
        :param header (bool): True means every csv, tsv file has a header record at the top and
        the expected behaviour is that the first record (the header) is skipped.
        False means every csv, tsv file does not have a header.
        :param null_string (str): a string that represents the NULL value if the field accepts NULLs.
        :param include str, include filter
        :param exclude str, exclude filter
        :param validate_error_msg (bool): validate errors that occur while creating dataset.
        :param username (str):
        :param password (str):
        :param expected_error (str):
        :param expected_error_code (str):
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param parse_json_string int, used in case of datasets created
        on parquet files, if the flag is set then, fields marked as JSON
        will be converted to JSON object, otherwise it will be kept as plain text.
        0 - Use default value , default is True
        1 - Explicitly Set to True.
        2 - Set to False
        :param convert_decimal_to_double int, Will tell the compiler to
        convert any encountered decimal value to double value. If this is
        not enabled, and we encounter a decimal, Analytics will fail.
        0 - Use default value , default is False
        1 - Set to True.
        2 - Explicitly Set to False
        :param timezone str, Timezone values likes GMT, PST, IST etc (in
        upper case only)
        :return True/False
        """

        cmd = self.generate_create_external_dataset_cmd(dataset_name, external_container_name,
                                                        link_name, if_not_exists, dataverse_name,
                                                        database_name, object_construction_def,
                                                        path_on_external_container, file_format,
                                                        redact_warning, header, null_string, include,
                                                        exclude, parse_json_string, convert_decimal_to_double,
                                                        timezone, embed_filter_values)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to create external collection {0}: "
                               "{1}".format(dataset_name, str(errors)))
                self.log.error(str(cmd))
                return False
            else:
                return True

    def create_external_dataset_obj(
            self, cluster, external_container_names, link_type="s3",
            use_only_existing_db=False, use_only_existing_dv=False,
            object_construction_def=None, paths_on_external_container=[],
            file_format="json", redact_warning=None, header=None,
            null_string=None, include=None, exclude=None, name_length=30,
            fixed_length=False, no_of_objs=1, parse_json_string=0,
            convert_decimal_to_double=0, timezone="",
            embed_filter_values=True, dataverse_name=None):
        """
        Creates a Dataset object for external datasets.
        :param external_container_names: <dict> {"external_container_name":"region"}
        :param link_type <str> s3,azureblob,gcp
        """
        external_datasets = []
        all_links = self.get_all_link_objs(link_type)
        for _ in range(no_of_objs):
            external_container = random.choice(list(external_container_names.keys()))
            link = random.choice(all_links)
            while link.properties["region"] != external_container_names[
                external_container]:
                link = random.choice(all_links)

            if use_only_existing_db:
                database_obj = random.choice(list(self.databases.values()))
            else:
                database_name = self.generate_name()
                if not self.create_database(cluster, database_name):
                    self.log.error(
                        "Error while creating database {0}".format(
                            database_name))
                database_obj = self.get_database_obj(self.format_name(
                    database_name))
                dataverse_name = self.generate_name()
                if not self.create_dataverse(
                        cluster, dataverse_name, database_obj.name):
                    self.log.error(
                        "Error while creating dataverse {0} in "
                        "database {1}".format(
                            dataverse_name, database_obj.name))

            if use_only_existing_dv:
                dataverse_obj = self.get_all_dataverse_obj(database_obj.name)[0]
            else:
                dataverse_name = self.generate_name()
                if not self.create_dataverse(
                        cluster, dataverse_name, database_obj.name):
                    self.log.error(
                        "Error while creating dataverse {0} in "
                        "database {1}".format(
                            dataverse_name, database_obj.name))
                dataverse_obj = self.get_dataverse_obj(
                    self.format_name(dataverse_name),
                    database_obj.name)

            dataset = External_Dataset(
                name=self.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=fixed_length),
                dataverse_name=dataverse_obj.name,
                database_name=database_obj.name, link_name=link.name,
                dataset_properties={})

            dataset.dataset_properties["external_container_name"] = external_container
            dataset.dataset_properties["object_construction_def"] = object_construction_def
            if paths_on_external_container is not None:
                dataset.dataset_properties["path_on_external_container"] = random.choice(paths_on_external_container)
            dataset.dataset_properties["file_format"] = file_format
            dataset.dataset_properties["redact_warning"] = redact_warning
            dataset.dataset_properties["header"] = header
            dataset.dataset_properties["null_string"] = null_string
            dataset.dataset_properties["include"] = include
            dataset.dataset_properties["exclude"] = exclude
            dataset.dataset_properties["parse_json_string"] = parse_json_string
            dataset.dataset_properties["convert_decimal_to_double"] = convert_decimal_to_double
            dataset.dataset_properties["timezone"] = timezone
            dataset.dataset_properties["embed_filter_values"] = embed_filter_values

            dataverse_obj.external_datasets[dataset.name] = dataset
            external_datasets.append(dataset)
        return external_datasets

    def create_external_dataset_from_spec(self, cluster, cbas_spec):
        """
        Creates external datasets based on external dataset specs.
        """
        self.log.info("Creating External Datasets based on CBAS Spec")

        dataset_spec = self.get_external_dataset_spec(cbas_spec)
        results = list()

        # get all external link objects
        link_types = dataset_spec.get("include_link_types", [])
        if not link_types:
            link_types = ["s3", "azure", "gcp"]

        if set(link_types) == set(
                dataset_spec.get("exclude_link_types", [])):
            self.log.error("Include and exclude link type cannot be same")
        else:
            for link_type in set(
                    dataset_spec.get("exclude_link_types", [])):
                link_types.pop(link_types.index(link_type))

        external_link_objs = list()
        for link_type in link_types:
            external_link_objs.extend(self.get_all_link_objs(link_type))

        if dataset_spec.get("include_links", []):
            for link_name in dataset_spec["include_links"]:
                external_link_objs |= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))])

        if dataset_spec.get("exclude_links", []):
            for link_name in dataset_spec["exclude_links"]:
                external_link_objs -= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))])

        num_of_external_datasets = dataset_spec.get(
            "num_of_external_datasets", 0)

        for i in range(0, num_of_external_datasets):

            database = None
            while not database:
                database = random.choice(list(self.databases.values()))
                if dataset_spec.get(
                        "include_databases", []) and CBASHelper.unformat_name(
                    database.name) not in dataset_spec.get(
                    "include_databases"):
                    database = None
                if dataset_spec.get(
                        "exclude_databases", []) and CBASHelper.unformat_name(
                    database.name) in dataset_spec.get("exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))
                if dataset_spec.get("include_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) not in dataset_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if dataset_spec.get("exclude_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) in dataset_spec.get(
                    "exclude_dataverses"):
                    dataverse = None

            if dataset_spec.get(
                    "name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = dataset_spec["name_key"] + "_{0}".format(
                    str(i))

            if len(external_link_objs) == 0:
                return False
            link = external_link_objs[i % len(external_link_objs)]
            while True:
                dataset_properties = dataset_spec[
                    "external_dataset_properties"][i % len(dataset_spec[
                                                               "external_dataset_properties"])]
                if link.properties["region"] == dataset_properties.get(
                        "region", None):
                    break

            dataset_obj = External_Dataset(
                name=name, dataverse_name=dataverse.name,
                database_name=dataverse.database_name,
                link_name=link.name,
                dataset_properties=dataset_properties)

            results.append(
                self.create_dataset_on_external_resource(
                    cluster, dataset_obj.name,
                    dataset_obj.dataset_properties[
                        "external_container_name"],
                    dataset_obj.link_name, False,
                    dataset_obj.dataverse_name, dataset_obj.database_name,
                    dataset_obj.dataset_properties[
                        "object_construction_def"],
                    dataset_obj.dataset_properties[
                        "path_on_external_container"],
                    dataset_obj.dataset_properties[
                        "file_format"],
                    dataset_obj.dataset_properties[
                        "redact_warning"],
                    dataset_obj.dataset_properties[
                        "header"],
                    dataset_obj.dataset_properties[
                        "null_string"],
                    dataset_obj.dataset_properties[
                        "include"],
                    dataset_obj.dataset_properties[
                        "exclude"],
                    dataset_obj.dataset_properties[
                        "parse_json_string"],
                    dataset_obj.dataset_properties[
                        "convert_decimal_to_double"],
                    dataset_obj.dataset_properties[
                        "timezone"],
                    False, None, None, None, None,
                    timeout=cbas_spec.get(
                        "api_timeout", 300),
                    analytics_timeout=cbas_spec.get(
                        "cbas_timeout", 300)))
            if results[-1]:
                dataverse.external_datasets[
                    dataset_obj.name] = dataset_obj
        return all(results)


class StandaloneCollectionLoader(External_Dataset_Util):

    def __init__(self, server_task=None, run_query_using_sdk=True):
        """
        :param server_task task object
        """
        super(StandaloneCollectionLoader, self).__init__(
            server_task, run_query_using_sdk)

    def convert_unicode_to_string(self, data):
        """
        Convert tnicode to string
        """
        if isinstance(data, dict):
            return {self.convert_unicode_to_string(key): self.convert_unicode_to_string(value) for key, value in
                    data.items()}
        elif isinstance(data, list):
            return [self.convert_unicode_to_string(element) for element in data]
        elif isinstance(data, str):
            return data
        elif isinstance(data, str):
            return data.encode('utf-8')
        else:
            return data

    def generate_docs(self, document_size=256000, country_type="string", include_country=True):
        """
        Generate docs of specific size
        """
        doc = None
        try:
            hotel = Hotel()
            hotel.generate_document(document_size, country_type, include_country)
            doc = json.loads(json.dumps(hotel.__dict__, default=lambda o: o.__dict__, ensure_ascii=False))
            del hotel
            doc = self.convert_unicode_to_string(doc)
        except Exception as err:
            self.log.error(str(err))
        return doc

    class GenerateDocsCallable:
        def __init__(self, instance, document_size, country_type, include_country):
            self.instance = instance
            self.document_size = document_size
            self.country_type = country_type
            self.include_country = include_country

        def call(self):
            return self.instance.generate_docs(self.document_size, self.country_type, self.include_country)

    def doc_operations_standalone_collection_sirius(self, task_manager, collection_name, dataverse_name, database_name,
                                                    connection_string, start, end, sdk_batch_size=25, doc_size=1024,
                                                    template="product", username=None, password=None,
                                                    sirius_url="http://127.0.0.1:4000", action="create"):
        sdk_batch_size = min(sdk_batch_size, end - start)
        database_information = ColumnarLoader(username=username, password=password, connection_string=connection_string,
                                              bucket=database_name, scope=dataverse_name, collection=collection_name,
                                              sdk_batch_size=int(sdk_batch_size))
        operation_config = WorkloadOperationConfig(start=int(start), end=int(end), template=template, doc_size=doc_size)
        op_type = SiriusCodes.DocOps.CREATE
        if action == "delete":
            op_type = SiriusCodes.DocOps.DELETE
        if action == "upsert":
            op_type = SiriusCodes.DocOps.UPDATE
        task = WorkLoadTask(task_manager=self.task, op_type=op_type,
                            database_information=database_information, operation_config=operation_config,
                            default_sirius_base_url=sirius_url)
        task_manager.add_new_task(task)
        task_manager.get_task_result(task)
        return task

    def load_doc_to_standalone_collection(
            self, cluster, collection_name, dataverse_name, database_name,
            no_of_docs, document_size=1024, batch_size=500,
            max_concurrent_batches=10, country_type="string",
            include_country=True, analytics_timeout=1800, timeout=1800):
        """
        Load documents to a standalone collection.
        """
        start = time.time()
        executor = ThreadPoolExecutor(max_workers=max_concurrent_batches)
        try:
            for i in range(0, no_of_docs, batch_size):
                batch_start = i
                batch_end = min(i + batch_size, no_of_docs)
                tasks = []

                for j in range(batch_start, batch_end):
                    tasks.append(self.GenerateDocsCallable(
                        self, document_size, country_type, include_country))

                batch_docs = []
                futures = executor.map(lambda task: task.call(), tasks)
                for future in futures:
                    batch_docs.append(future)

                retry_count = 0
                while retry_count < 3:
                    result = self.insert_into_standalone_collection(
                        cluster, collection_name,
                        batch_docs, dataverse_name, database_name,
                        analytics_timeout=analytics_timeout,
                        timeout=timeout)
                    if result:
                        break
                    elif retry_count == 2:
                        self.log.error("Error while inserting docs in collection {}".format(
                            CBASHelper.format_name(dataverse_name, collection_name)))
                        return False
                    else:
                        retry_count += 1
        finally:
            executor.shutdown()

        end = time.time()
        time_spent = end - start
        self.log.info("Took {} seconds to insert {} docs".format(
            time_spent, no_of_docs))
        return True

    def generate_insert_into_cmd(self, document, collection_name, database_name=None,
                                 dataverse_name=None):
        doc_to_insert = []
        doc_to_insert.extend(document)
        cmd = "INSERT INTO "
        if database_name:
            cmd += "{}.".format(database_name)
        if dataverse_name:
            cmd += "{0}.{1} ".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            cmd += "{0} ".format(CBASHelper.format_name(collection_name))
        cmd += "({0});".format(doc_to_insert)
        return cmd

    def insert_into_standalone_collection(self, cluster, collection_name, document, dataverse_name=None,
                                          database_name=None, username=None, validate_error_msg=None,
                                          expected_error=None, expected_error_code=None, password=None,
                                          analytics_timeout=300, timeout=300):
        """
        Query to insert into standalone collection
        """
        cmd = self.generate_insert_into_cmd(document, collection_name, database_name,
                                            dataverse_name)
        self.log.info("Inserting into: {0}.{1}".format(CBASHelper.format_name(dataverse_name),
                                                       CBASHelper.format_name(collection_name)))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        elif status != "success":
            self.log.error(str(errors))
            return False
        else:
            return True

    def generate_upsert_into_cmd(self, collection_name, new_item, dataverse_name=None,
                                 database_name=None):
        cmd = "UPSERT INTO "
        if database_name:
            cmd += "{}.".format(database_name)
        if dataverse_name:
            cmd += "{0}.{1} ".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            cmd += "{0} ".format(CBASHelper.format_name(collection_name))
        cmd += "({0});".format(new_item)

        return cmd

    def upsert_into_standalone_collection(self, cluster, collection_name, new_item, dataverse_name=None,
                                          database_name=None, username=None, validate_error_msg=None,
                                          expected_error=None, expected_error_code=None, password=None,
                                          analytics_timeout=300, timeout=300):
        """
        Upsert into standalone collection
        """
        cmd = self.generate_upsert_into_cmd(collection_name, new_item, dataverse_name,
                                            database_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        elif status != "success":
            self.log.error(str(errors))
            return False
        else:
            return True

    def generate_delete_from_cmd(self, collection_name, dataverse_name=None, database_name=None,
                                 where_clause=None, use_alias=False):
        cmd = "DELETE FROM "
        if database_name:
            cmd += "{0}.".format(database_name)
        if dataverse_name:
            cmd += "{0}.{1} ".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            cmd += "{0} ".format(CBASHelper.format_name(collection_name))

        if use_alias:
            cmd += "as alias "

        if where_clause:
            cmd += "WHERE {}".format(where_clause)

        return cmd

    def delete_from_standalone_collection(
            self, cluster, collection_name, dataverse_name=None, database_name=None,
            where_clause=None, use_alias=False, username=None, validate_error_msg=None,
            expected_error=None, expected_error_code=None, password=None,
            analytics_timeout=300, timeout=300):
        """
        Query to delete from standalone collection
        """
        cmd = self.generate_delete_from_cmd(collection_name, dataverse_name,
                                            database_name, where_clause, use_alias)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        elif status != "success":
            self.log.error(str(errors))
            return False
        else:
            return True

    def get_random_doc(self, cluster, collection_name, dataverse_name, database_name, username=None, password=None,
                       analytics_timeout=300, timeout=300):
        """
        Get a doc from the standalone collection
        """
        cmd = "SELECT * from "
        if database_name:
            cmd += "{}.".format(database_name)
        if dataverse_name:
            cmd += "{0}.{1} ".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            cmd += "{0} ".format(CBASHelper.format_name(collection_name))
            cmd += "LIMIT 1"
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password, timeout=timeout,
                analytics_timeout=analytics_timeout)
            if status != "success":
                self.log.error(str(errors))
                return random.randint(1, 10000)
            else:
                cbas_helper = CBASHelper(cluster.cbas_cc_node)
                results = cbas_helper.get_json(json_data=results)
                return random.choice(results)["id"]

    def crud_on_standalone_collection(
            self, cluster, collection_name, dataverse_name=None, database_name=None,
            target_num_docs=100000, time_for_crud_in_mins=1,
            insert_percentage=10, upsert_percentage=80,
            delete_percentage=10, where_clause_for_delete_op=None,
            doc_size=256000, use_alias=False):
        """
        Perform crud on standlaone collection
        """
        end_time = time.time() + time_for_crud_in_mins * 60

        if database_name and dataverse_name and collection_name:
            collection_full_name = "{0}.{1}.{2}".format(
                CBASHelper.format_name(database_name),
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        elif dataverse_name and collection_name:
            collection_full_name = "{0}.{1}".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            collection_full_name = CBASHelper.format_name(collection_name)

        current_docs_count = self.get_num_items_in_cbas_dataset(
            cluster, collection_full_name)

        # First insert target number of docs/
        while current_docs_count < target_num_docs:
            if not self.load_doc_to_standalone_collection(
                    cluster, collection_name, dataverse_name, database_name,
                    target_num_docs, doc_size):
                self.log.error("Error while inserting docs in "
                               "collection {}".format(collection_full_name))
                return False
            current_docs_count = self.get_num_items_in_cbas_dataset(
                cluster, collection_full_name)

        # Perform CRUD
        while time.time() < end_time:

            operation = None
            weights = [insert_percentage, upsert_percentage, delete_percentage]
            total_weight = sum(weights)
            random_num = random.uniform(0, total_weight)
            current_weight = 0

            for op, weight in zip(["insert", "upsert", "delete"], weights):
                current_weight += weight
                if current_weight >= random_num:
                    operation = op
                    break

            if operation == "delete":
                self.log.info("Deleting documents in collection: {0}".format(
                    collection_full_name))
                if not self.delete_from_standalone_collection(
                        cluster, collection_name, dataverse_name, database_name,
                        where_clause_for_delete_op, use_alias):
                    self.log.error(
                        "Error while deleting docs in collection {"
                        "}".format(collection_full_name))
                    return False

            elif operation == "insert":
                self.log.info("Inserting documents in collection: {0}".format(
                    collection_full_name))
                if not self.insert_into_standalone_collection(
                        cluster, collection_name,
                        [self.generate_docs(doc_size)], dataverse_name, database_name):
                    self.log.error(
                        "Error while inserting docs in collection {"
                        "}".format(collection_full_name))
                    return False

            elif operation == "upsert":
                self.log.info("Upserting documents in collection: {0}".format(
                    collection_full_name))
                if not self.upsert_into_standalone_collection(
                        cluster, collection_name,
                        [self.generate_docs(doc_size)], dataverse_name, database_name):
                    self.log.error(
                        "Error while upserting docs in collection {"
                        "}".format(collection_full_name))
                    return False

        current_docs_count = self.get_num_items_in_cbas_dataset(
            cluster, collection_full_name)

        # Make sure final number of docs are equal to target_num_docs
        while current_docs_count > target_num_docs:
            num_docs_to_delete = current_docs_count - target_num_docs
            where_clause_for_delete_op = (
                f"alias.id in (SELECT VALUE x.id FROM {collection_full_name} "
                f"as x limit {num_docs_to_delete})")
            if not self.delete_from_standalone_collection(
                    cluster, collection_name, dataverse_name,
                    where_clause_for_delete_op, use_alias):
                self.log.error(
                    "Error while deleting docs in collection {"
                    "}".format(collection_full_name))
                return False
            current_docs_count -= num_docs_to_delete

        if current_docs_count < target_num_docs:
            num_docs_to_insert = target_num_docs - current_docs_count
            if not self.load_doc_to_standalone_collection(
                    cluster, collection_name, dataverse_name, database_name,
                    num_docs_to_insert, doc_size):
                self.log.error("Error while inserting docs in "
                               "collection {}".format(collection_full_name))
                return False
        return True


class StandAlone_Collection_Util(StandaloneCollectionLoader):

    def __init__(self, server_task=None, run_query_using_sdk=True):
        """
        :param server_task task object
        """
        super(StandAlone_Collection_Util, self).__init__(
            server_task, run_query_using_sdk)

    def generate_copy_from_cmd(self, collection_name, aws_bucket_name,
                               external_link_name, dataverse_name=None, database_name=None,
                               files_to_include=[], file_format="json", type_parsing_info="",
                               path_on_aws_bucket="", header=None, null_string=None,
                               files_to_exclude=[], parse_json_string=0,
                               convert_decimal_to_double=0, timezone=""):
        cmd = "COPY INTO "
        if database_name:
            cmd += "{}.".format(CBASHelper.format_name(database_name))
        if dataverse_name:
            cmd += "{0}.{1} ".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            cmd += "{0} ".format(CBASHelper.format_name(collection_name))

        if type_parsing_info:
            cmd += "AS ({0}) ".format(type_parsing_info)

        cmd += "FROM `{0}` AT {1} PATH \"{2}\" ".format(
            aws_bucket_name, CBASHelper.format_name(external_link_name),
            path_on_aws_bucket)

        with_parameters = dict()
        if file_format:
            with_parameters["format"] = file_format

        if header is not None:
            with_parameters["header"] = header

        if null_string:
            with_parameters["null"] = null_string

        if files_to_include:
            with_parameters["include"] = files_to_include

        if files_to_exclude:
            with_parameters["exclude"] = files_to_exclude

        if "format" in with_parameters and with_parameters["format"] == "parquet":
            if timezone:
                with_parameters["timezone"] = timezone.upper()
            if parse_json_string > 0:
                if parse_json_string == 1:
                    with_parameters["parse-json-string"] = True
                else:
                    with_parameters["parse-json-string"] = False
            if convert_decimal_to_double > 0:
                if convert_decimal_to_double == 1:
                    with_parameters["decimal-to-double"] = True
                else:
                    with_parameters["decimal-to-double"] = False
        if bool(with_parameters):
            cmd += "WITH {0};".format(json.dumps(with_parameters))

        return cmd

    def copy_from_external_resource_into_standalone_collection(
            self, cluster, collection_name, aws_bucket_name,
            external_link_name, dataverse_name=None, database_name=None,
            files_to_include=[], file_format="json", type_parsing_info="",
            path_on_aws_bucket="", header=None, null_string=None,
            files_to_exclude=[], parse_json_string=0,
            convert_decimal_to_double=0, timezone="", validate_error_msg=False,
            username=None, password=None, expected_error=None,
            expected_error_code=None, timeout=7200, analytics_timeout=7200):
        """
        Copy data into standalone collection from external S3 bucket using
        external links
        :param collection_name (str) : Name of the standalone collection
        where data is to be copied.
        :param aws_bucket_name (str): AWS S3 bucket from which the data is
        to be copied.
        :param external_link_name (str): external link to AWS S3
        :param dataverse_name str, Dataverse where dataset is present.
        :param type_parsing_info (str): It defines how the data read
        will be parsed. Required only for csv and tsv formats.
        :param path_on_aws_bucket (str): Relative path in S3 bucket,
        from where the files will be copied.
        :param file_format (str): Type of files to read. Valid values -
        json, csv, tsv and parquet
        :param header (bool): True means every csv, tsv file has a
        header record at the top and the expected behaviour is that the
        first record (the header) is skipped.
        False means every csv, tsv file does not have a header.
        :param null_string (str): a string that represents the NULL
        value if the field accepts NULLs.
        :param files_to_include list, include file filter
        :param files_to_exclude list, exclude file filter
        :param parse_json_string int, used in case of datasets created
        on parquet files, if the flag is set then, fields marked as JSON
        will be converted to JSON object, otherwise it will be kept as plain text.
        0 - Use default value , default is True
        1 - Explicitly Set to True.
        2 - Set to False
        :param convert_decimal_to_double int, Will tell the compiler to
        convert any encountered decimal value to double value. If this is
        not enabled, and we encounter a decimal, Analytics will fail.
        0 - Use default value , default is False
        1 - Set to True.
        2 - Explicitly Set to False
        :param timezone str, Timezone values likes GMT, PST, IST etc (in
        upper case only)
        :param validate_error_msg (bool): validate errors that occur while creating dataset.
        :param username (str):
        :param password (str):
        :param expected_error (str):
        :param expected_error_code (str):
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return True/False
        """
        cmd = self.generate_copy_from_cmd(collection_name, aws_bucket_name,
                                          external_link_name, dataverse_name,
                                          database_name, files_to_include, file_format,
                                          type_parsing_info, path_on_aws_bucket, header,
                                          null_string, files_to_exclude, parse_json_string,
                                          convert_decimal_to_double, timezone)
        self.log.info("Copying into {0} using external link {1} from "
                      "S3 bucket path {2}/{3}".format(
            collection_name, external_link_name, aws_bucket_name, path_on_aws_bucket))
        self.log.debug(cmd)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def generate_standalone_create_DDL(
            self, collection_name, dataverse_name=None, database_name=None,
            if_not_exists=True, primary_key={}, subquery="", link_name=None,
            external_collection=None, ddl_format="random",
            compress_dataset=False, storage_format=None,
            kafka_connector_details=None):
        """
        :param collection_name <str> Name of the collection to be created.
        :param dataverse_name <str> Name of the dataverse under which the
        collection has to be created.
        :param if_not_exists <bool> if this flag is set then, if a
        dataset with same name is present in the same dataverse, then
        the create statement will pass without creating a new dataset.
        :param primary_key <dict> Contains field name and field type to be
        used as primary key. If empty then auto generated UUID will be used
        as primary key.
        :param subquery <str> Subquery to filter out data to be stored in
        standalone collection.
        :param link_name <str> Fully qualified name of the kafka link.
        :param external_collection <str> Fully qualified name of the
        collection on external databases like mongo, dynamo, cassandra etc
        :param ddl_format <str> DDL format to be used. Accepted values are
        DATASET, ANALYTICS COLLECTION, COLLECTION or random
        :param compress_dataset <bool> use to set compression policy for
        dataset.
        :param with_clause <str> use to set other conditions apart from
        compress dataset, can be of format { "merge-policy": {"name": <>,
        "parameters": {"max-mergable-component-size": <>,
        "max-tolerance-component-count": <>}}}
        """
        cmd = "CREATE {0} ".format(random.choice(
            ["DATASET", "ANALYTICS COLLECTION", "COLLECTION"]) if
                                   ddl_format.lower() == "random" else ddl_format)
        if database_name:
            cmd += "{}.".format(CBASHelper.format_name(database_name))
        if dataverse_name:
            cmd += "{0}.{1}".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
        else:
            cmd += "{0}".format(CBASHelper.format_name(collection_name))

        if if_not_exists:
            cmd += " IF NOT EXISTS"

        if primary_key:
            pk = ""
            for field_name, field_type in primary_key.items():
                pk += "{0}: {1}, ".format(field_name, field_type)
            pk = pk.rstrip(pk[-1:-3:-1])
        else:
            pk = "`id` : UUID"

        cmd += " PRIMARY KEY({0})".format(pk)
        if not primary_key:
            cmd += " AUTOGENERATED"

        if subquery:
            cmd += " AS {0}".format(subquery)

        if link_name and external_collection:
            if kafka_connector_details:
                cmd += " ON `{0}` AT {1}".format(external_collection, link_name)
            else:
                cmd += " ON {0} AT {1}".format(
                    CBASHelper.format_name(external_collection), link_name)

        if compress_dataset or storage_format or kafka_connector_details:
            with_params = dict()

            if compress_dataset:
                with_params["storage-block-compression"] = {'scheme': 'snappy'}

            if storage_format:
                with_params["storage-format"] = {"format": storage_format}

            if kafka_connector_details:
                with_params.update(kafka_connector_details)

            cmd += " with " + json.dumps(with_params) + " "

        cmd += ";"
        return cmd

    def create_standalone_collection(
            self, cluster, collection_name, ddl_format="random",
            if_not_exists=True, dataverse_name=None, database_name=None,
            primary_key={}, subquery="", compress_dataset=False,
            storage_format=None, kafka_connector_details=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, username=None, password=None,
            timeout=300, analytics_timeout=300):
        """
        Creates a standalone collection.
        :param collection_name str Name of the collection to be created.
        :param ddl_format str DDL format to be used. Accepted values are
        DATASET, ANALYTICS COLLECTION, COLLECTION or random
        :param if_not_exists bool, if this flag is set then, if a
        dataset with same name is present in the same dataverse, then
        the create statement will pass without creating a new dataset.
        :param dataverse_name str Name of the dataverse under which the
        collection has to be created.
        :param primary_key dict Contains field name and field type to be
        used as primary key. If empty then auto generated UUID will be used
        as primary key.
        :param subquery str Subquery to filter out data to be stored in
        standalone collection.
        :param compress_dataset bool, use to set compression policy for
        dataset.
        :param with_clause str, use to set other conditions apart from
        compress dataset, can be of format { "merge-policy": {"name": <>,
        "parameters": {"max-mergable-component-size": <>,
        "max-tolerance-component-count": <>}}}
        :param validate_error_msg (bool): validate errors that occur
        while creating dataset.
        :param username (str):
        :param password (str):
        :param expected_error (str):
        :param expected_error_code (str):
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return True/False
        """
        cmd = self.generate_standalone_create_DDL(
            collection_name, dataverse_name, database_name, if_not_exists,
            primary_key, subquery, None, None, ddl_format, compress_dataset,
            storage_format, kafka_connector_details)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def create_standalone_collection_using_links(
            self, cluster, collection_name, ddl_format="random",
            if_not_exists=True, dataverse_name=None, database_name=None,
            primary_key={}, link_name=None, external_collection=None,
            compress_dataset=False, storage_format=None,
            kafka_connector_details=None, validate_error_msg=False,
            expected_error=None, expected_error_code=None, username=None,
            password=None, timeout=300, analytics_timeout=300):
        """
        Creates a standalone collection.
        :param collection_name str Name of the collection to be created.
        :param ddl_format str DDL format to be used. Accepted values are
        DATASET, ANALYTICS COLLECTION, COLLECTION or random
        :param if_not_exists bool, if this flag is set then, if a
        dataset with same name is present in the same dataverse, then
        the create statement will pass without creating a new dataset.
        :param dataverse_name str Name of the dataverse under which the
        collection has to be created.
        :param primary_key dict Contains field name and field type to be
        used as primary key. If empty then auto generated UUID will be used
        as primary key.
        :param link_name <str> Fully qualified name of the kafka or remote
        link.
        :param external_collection <str> Fully qualified name of the
        collection on external databases like mongo, dynamo, cassandra etc
        :param compress_dataset bool, use to set compression policy for
        dataset.
        :param with_clause str, use to set other conditions apart from
        compress dataset, can be of format { "merge-policy": {"name": <>,
        "parameters": {"max-mergable-component-size": <>,
        "max-tolerance-component-count": <>}}}
        :param validate_error_msg (bool): validate errors that occur
        while creating dataset.
        :param username (str):
        :param password (str):
        :param expected_error (str):
        :param expected_error_code (str):
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return True/False
        """
        cmd = self.generate_standalone_create_DDL(
            collection_name, dataverse_name, database_name, if_not_exists,
            primary_key, "", link_name, external_collection, ddl_format,
            compress_dataset, storage_format, kafka_connector_details)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password,
            timeout=timeout, analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def create_standalone_dataset_obj(
            self, cluster, no_of_objs=1, datasource=None, primary_key={},
            link=None, same_db_same_dv_for_link_and_dataset=False,
            same_db_diff_dv_for_link_and_dataset=False,
            external_collection_name=None, database_name=None,
            dataverse_name=None, storage_format=None, name_length=30,
            fixed_length=False):
        """
        Generates standalone dataset objects.
        :param no_of_objs <int> number of standalone dataset objects to be
        created.
        :param datasource <str> Source from where data will be ingested
        into dataset. Accepted Values - mongo, dynamo, rds, cassandra, s3, gcp,
        azure, shadow_dataset, crud
        :param primary_key <dict> dict of field_name:field_type to be used
        as primary key.
        :param link <link obj> Object of the link to be used in standalone
        collection.
        :param same_dv_for_link_and_dataset <bool>
        :param external_collection_name <str> Fully qualified name of the
        collection on external databases like mongo, dynamo, cassandra etc
        """

        if storage_format == "mixed":
            storage_format = random.choice(["row", "column"])

        collection_objs = list()
        for i in range(no_of_objs):
            if not link:
                if datasource in ["s3", "azure", "gcp"]:
                    link = random.choice(self.get_all_link_objs(
                        link_type=datasource.lower()))
                    dataset_properties = link.properties
                else:
                    if datasource in ["mongo", "dynamo", "cassandra"]:
                        link = random.choice(self.get_all_link_objs(
                            link_type="kafka"))
                    dataset_properties = {}

            if (same_db_same_dv_for_link_and_dataset or
                    same_db_diff_dv_for_link_and_dataset):
                database_obj = self.get_database_obj(link.database_name)
                link_dataverse_obj = self.get_dataverse_obj(
                    link.dataverse_name, link.database_name)
                if same_db_diff_dv_for_link_and_dataset:
                    all_dataverses_in_link_db = self.get_all_dataverse_obj()
                    if len(all_dataverses_in_link_db) > 1:
                        while True:
                            dataverse = random.choice(
                                all_dataverses_in_link_db)
                            if link_dataverse_obj != dataverse:
                                break
                        dataverse_obj = dataverse
                    else:
                        dataverse_name = self.generate_name()
                        if not self.create_dataverse(
                                cluster, dataverse_name, database_obj.name):
                            self.log.error(
                                "Error while creating dataverse {0} in "
                                "database {1}".format(
                                    dataverse_name, database_obj.name))
                        database_obj = self.get_dataverse_obj(
                            database_obj.name,
                            self.format_name(dataverse_name))
                else:
                    dataverse_obj = link_dataverse_obj
            else:
                if not database_name:
                    database_name = self.generate_name()
                if not dataverse_name:
                    dataverse_name = self.generate_name()
                database_obj = self.get_database_obj(
                    self.format_name(database_name))
                if database_obj is None:
                    if not self.create_database(cluster, database_name):
                        self.log.error("Error while creating database {0}".format(
                            dataverse_name, database_name))
                    database_obj = self.get_database_obj(
                        self.format_name(database_name))
                dataverse_obj = self.get_dataverse_obj(
                    self.format_name(dataverse_name), database_obj.name)
                if dataverse_obj is None:
                    if not self.create_dataverse(
                            cluster, dataverse_name, database_name, if_not_exists=True):
                        self.log.error("Error while creating dataverse {0} in "
                                       "database {1}".format(
                            dataverse_name, database_name))
                    dataverse_obj = self.get_dataverse_obj(
                        self.format_name(dataverse_name), database_obj.name)

            dataset_name = self.generate_name(
                name_cardinality=1, max_length=name_length,
                fixed_length=fixed_length)

            link_name = link.name if link else None

            dataset_obj = Standalone_Dataset(
                dataset_name, datasource, primary_key, dataverse_obj.name,
                database_obj.name, link_name, external_collection_name,
                dataset_properties, 0, storage_format)

            collection_objs.append(dataset_obj)
            dataverse_obj.standalone_datasets[dataset_obj.name] = dataset_obj
        return collection_objs

    @staticmethod
    def get_standalone_dataset_spec(cbas_spec, spec_name="standalone_dataset"):
        """
        Fetches dataset specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get(spec_name, {})

    def create_standalone_dataset_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating Standalone Datasets based on CBAS Spec")

        dataset_spec = self.get_standalone_dataset_spec(cbas_spec)
        results = list()

        num_of_standalone_coll = dataset_spec.get("num_of_standalone_coll", 0)

        for i in range(0, num_of_standalone_coll):
            database = None
            while not database:
                database = random.choice(list(self.databases.values()))
                if dataset_spec.get(
                        "include_databases", []) and CBASHelper.unformat_name(
                    database.name) not in dataset_spec.get(
                    "include_databases"):
                    database = None
                if dataset_spec.get(
                        "exclude_databases", []) and CBASHelper.unformat_name(
                    database.name) in dataset_spec.get("exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))
                if dataset_spec.get("include_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) not in dataset_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if dataset_spec.get("exclude_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) in dataset_spec.get(
                    "exclude_dataverses"):
                    dataverse = None
            if dataset_spec.get(
                    "name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = dataset_spec["name_key"] + "_{0}".format(
                    str(i))

            if not dataset_spec.get("creation_methods", []):
                dataset_spec["creation_methods"] = [
                    "DATASET", "COLLECTION",
                    "ANALYTICS COLLECTION"]
            creation_method = random.choice(
                dataset_spec["creation_methods"])

            if dataset_spec["storage_format"] == "mixed":
                storage_format = random.choice(
                    ["row", "column"])
            else:
                storage_format = dataset_spec[
                    "storage_format"]

            # Here datasources can be S3, GCP, AZURE or None
            data_sources = dataset_spec.get("data_source", [])
            # Link means external links only.
            link = None
            if len(data_sources) > 0:
                data_source = data_sources[i % len(data_sources)]
                if data_source is not None:
                    links = self.get_all_link_objs(link_type=data_source)
                    if dataset_spec.get("include_links", []):
                        for link_name in dataset_spec.get("include_links"):
                            link_obj = self.get_link_obj(cluster, link_name)
                            if link_obj not in links:
                                links.append(link_obj)
                    if dataset_spec.get("exclude_links", []):
                        for link_name in dataset_spec.get("exclude_links"):
                            link_obj = self.get_link_obj(cluster, link_name)
                            if link_obj in links:
                                links.remove(link_obj)
                    link = random.choice(links)
            else:
                data_source = None

            link_name = link.name if link else None
            dataset_obj = Standalone_Dataset(
                name, data_source, dataset_spec["primary_key"][i % len(dataset_spec["primary_key"])],
                dataverse.name, database.name, link_name, None,
                dataset_spec["standalone_collection_properties"][
                    i % len(dataset_spec["standalone_collection_properties"])],
                0, storage_format)

            results.append(
                self.create_standalone_collection(
                    cluster=cluster, collection_name=name,
                    ddl_format=creation_method, if_not_exists=False,
                    dataverse_name=dataset_obj.dataverse_name,
                    database_name=dataset_obj.database_name,
                    primary_key=dataset_obj.primary_key,
                    storage_format=storage_format))
            if results[-1]:
                dataverse.standalone_datasets[
                    dataset_obj.name] = dataset_obj
        return all(results)

    """
    Note - Number of external collection should be same as number of
    standalone collections to be created.
    """

    def create_standalone_dataset_for_external_db_from_spec(
            self, cluster, cbas_spec):
        self.log.info("Creating Standalone Datasets for external database "
                      "based on CBAS Spec")

        dataset_spec = self.get_standalone_dataset_spec(
            cbas_spec, spec_name="kafka_dataset")
        results = list()

        kafka_link_objs = set(self.get_all_link_objs("kafka"))
        if dataset_spec.get("include_links", []):
            for link_name in dataset_spec["include_links"]:
                kafka_link_objs |= {self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))}

        if dataset_spec.get("exclude_links", []):
            for link_name in dataset_spec["exclude_links"]:
                kafka_link_objs -= {self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))}

        num_of_ds_on_external_db = dataset_spec.get("num_of_ds_on_external_db", 0)
        link_source_db_pairs = []
        for i in range(0, num_of_ds_on_external_db):
            while not database:
                database = random.choice(list(self.databases.values()))
                if dataset_spec.get(
                        "include_databases", []) and CBASHelper.unformat_name(
                    database.name) not in dataset_spec.get(
                    "include_databases"):
                    database = None
                if dataset_spec.get(
                        "exclude_databases", []) and CBASHelper.unformat_name(
                    database.name) in dataset_spec.get("exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))
                if dataset_spec.get("include_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) not in dataset_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if dataset_spec.get("exclude_dataverses",
                                    []) and CBASHelper.unformat_name(
                    dataverse.name) in dataset_spec.get(
                    "exclude_dataverses"):
                    dataverse = None
            if dataset_spec.get(
                    "name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = dataset_spec["name_key"] + "_{0}".format(
                    str(i))

            if not dataset_spec.get("creation_methods", []):
                dataset_spec["creation_methods"] = [
                    "DATASET", "COLLECTION",
                    "ANALYTICS COLLECTION"]
                creation_method = random.choice(
                    dataset_spec["creation_methods"])

            if dataset_spec["storage_format"] == "mixed":
                storage_format = random.choice(
                    ["row", "column"])
            else:
                storage_format = dataset_spec[
                    "storage_format"]

            if not dataset_spec["data_source"]:
                datasource = random.choice(["mongo", "dynamo", "rds"])
            else:
                datasource = dataset_spec["data_source"][i % len(dataset_spec["data_source"])]

            if (dataset_spec["include_kafka_topics"].get(
                    datasource, {}) and dataset_spec[
                "exclude_kafka_topics"].get(datasource, {})
                    and (set(dataset_spec["include_kafka_topics"][
                                 datasource]) == set(
                        dataset_spec["exclude_kafka_topics"][
                            datasource]))):
                self.log.error("Both include and exclude "
                               "kafka topics cannot be same")
                return False
            elif dataset_spec["exclude_kafka_topics"].get(datasource, {}):
                dataset_spec["include_kafka_topics"][datasource] = (
                        set(dataset_spec["include_kafka_topics"][
                                datasource]) - set(
                    dataset_spec["exclude_kafka_topics"][datasource]))

            eligible_links = [link for link in kafka_link_objs if
                              link.db_type == datasource]

            """
            This is to make sure that we don't create 2 collection with same
            source collection and link
            include_kafka_topics should be of format -
            { "datasource" : [("region","collection_name")]}
            for mongo and MySQL, region should be empty string
            This is to support Dynamo tables multiple regions.
            """
            retry = 0
            while retry < 100:
                link = random.choice(eligible_links)

                if dataset_spec["include_kafka_topics"][datasource]:
                    external_collection_name = random.choice(
                        dataset_spec["include_kafka_topics"][
                            datasource])

                    # this will be True for Dynamo only
                    if external_collection_name[0]:
                        while (external_collection_name[0] !=
                               link.external_database_details[
                                   "connectionFields"]["region"]):
                            external_collection_name = random.choice(
                                dataset_spec["include_kafka_topics"][
                                    datasource])
                    external_collection_name = external_collection_name[1]
                else:
                    return False
                link_source_db_pair = (link.name, external_collection_name)
                if link_source_db_pair not in link_source_db_pairs:
                    link_source_db_pairs.append(link_source_db_pair)
                    break
                else:
                    retry += 1

            dataset_obj = Standalone_Dataset(
                name=name, data_source=datasource, primary_key=random.choice(
                    dataset_spec["primary_key"]),
                dataverse_name=dataverse.name, database_name=database.name,
                link_name=link.name,
                external_collection_name=external_collection_name,
                storage_format=storage_format)

            if not self.create_standalone_collection_using_links(
                    cluster=cluster, collection_name=name,
                    ddl_format=creation_method, if_not_exists=False,
                    dataverse_name=dataverse.name, database_name=database.name,
                    primary_key=dataset_obj.primary_key,
                    link_name=link.name,
                    external_collection=external_collection_name,
                    storage_format=storage_format):
                self.log.error("Failed to create dataset {}".format(name))
                results.append(False)
            else:
                dataverse.standalone_datasets[
                    dataset_obj.name] = dataset_obj
                results.append(True)
        return all(results)


class Synonym_Util(StandAlone_Collection_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Synonym_Util, self).__init__(
            server_task, run_query_using_sdk)

    def validate_synonym_in_metadata(
            self, cluster, synonym_name, synonym_dataverse_name,
            synonym_database_name, cbas_entity_name,
            cbas_entity_dataverse_name, cbas_entity_database_name,
            username=None, password=None):
        """
        Validates whether an entry for Synonym is present in Metadata.Synonym.
        :param synonym_name : str, Synonym which has to be validated.
        :param synonym_dataverse_name : str, dataverse where the synonym is
        present
        :param cbas_entity_name : str, name of the cbas object on which the
        synonym is based.
        :param cbas_entity_dataverse_name : str, name of the dataverse where
        the cbas object on
        which the synonym is based.
        :param username : str
        :param password : str
        :return boolean
        """
        self.log.debug("Validating Synonym entry in Metadata")
        cmd = ("select value sy from Metadata.`Synonym` as sy where "
               "sy.SynonymName = \"{0}\" and sy.DataverseName = \"{1}\" and "
               "sy.DatabaseName = \"{2}\";".format(
            self.unformat_name(synonym_name),
            self.metadata_format(synonym_dataverse_name),
            self.metadata_format(synonym_database_name)))

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password)
        if status == "success":
            for result in results:
                if (result['ObjectDataverseName'] == self.metadata_format(
                        cbas_entity_dataverse_name) and result[
                    'ObjectName'] == self.unformat_name(cbas_entity_name)
                        and result['ObjectDatabaseName'
                        ] == self.metadata_format(cbas_entity_database_name)):
                    return True
            return False
        else:
            return False

    def generate_create_analytics_synonym_cmd(self, synonym_full_name,
                                              cbas_entity_full_name, if_not_exists=False):
        cmd = f"create analytics synonym {synonym_full_name}"
        cmd += " If Not Exists" if if_not_exists \
            else f" for {cbas_entity_full_name};"
        return cmd

    def create_analytics_synonym(
            self, cluster, synonym_full_name, cbas_entity_full_name,
            if_not_exists=False, validate_error_msg=False, expected_error=None,
            username=None, password=None, timeout=300, analytics_timeout=300):
        """
        Creates an analytics synonym on a dataset.
        :param synonym_full_name : str, Fully qualified Synonym which has to be created.
        :param cbas_entity_full_name : str, Fully qualified name of the cbas object for
        which the synonym is to be created.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        """
        cmd = self.generate_create_analytics_synonym_cmd(synonym_full_name,
                                                         cbas_entity_full_name, if_not_exists)

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def generate_drop_analytics_synonym_cmd(self, synonym_full_name,
                                            if_exists=False):
        cmd = "drop analytics synonym {0}".format(synonym_full_name)
        cmd += " if exists;" if if_exists else ";"
        return cmd

    def drop_analytics_synonym(
            self, cluster, synonym_full_name, if_exists=False,
            validate_error_msg=False, expected_error=None, username=None,
            password=None, timeout=300, analytics_timeout=300):
        """
        Drop an analytics synonym.
        :param synonym_full_name : str, Fully qualified Synonym which has to be dropped.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        """

        cmd = self.generate_drop_analytics_synonym_cmd(synonym_full_name, if_exists)

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def validate_synonym_doc_count(
            self, cluster, synonym_full_name, cbas_entity_full_name,
            validate_error_msg=False, expected_error=None,
            timeout=300, analytics_timeout=300):
        """
        Validate that doc count in synonym is same as the dataset on which it was created.
        :param synonym_full_name : str, Fully qualified Synonym name whose count needs to be validated.
        :param cbas_entity_full_name : str, Fully qualified name of the cbas object for
        which the synonym was created.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param timeout : int
        :param analytics_timeout : int
        """

        cmd_get_num_items = "select count(*) from %s;"

        synonym_status, metrics, errors, synonym_results, _ = \
            self.execute_statement_on_cbas_util(
                cluster, cmd_get_num_items % synonym_full_name,
                timeout=timeout,
                analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(synonym_status, errors,
                                                   expected_error)
        else:
            if synonym_status != "success":
                self.log.error("Querying synonym failed")
                synonym_results = 0
            else:
                self.log.debug("No. of items in synonym {0} : {1}"
                               .format(synonym_full_name,
                                       synonym_results[0]['$1']))
                synonym_results = synonym_results[0]['$1']

            dataset_status, metrics, errors, dataset_results, _ = \
                self.execute_statement_on_cbas_util(
                    cluster, cmd_get_num_items % cbas_entity_full_name,
                    timeout=timeout,
                    analytics_timeout=analytics_timeout)
            if dataset_status != "success":
                self.log.error("Querying dataset failed")
                dataset_results = 0
            else:
                self.log.debug("No. of items in dataset {0} : {1}"
                               .format(cbas_entity_full_name,
                                       dataset_results[0]['$1']))
                dataset_results = dataset_results[0]['$1']

            if dataset_results != synonym_results:
                return False
            return True

    def get_synonym_obj(self, cluster, synonym_name, synonym_dataverse=None,
                        synonym_database=None, cbas_entity_name=None,
                        cbas_entity_dataverse=None, cbas_entity_database=None):
        """
        Return Synonym object if the synonym with the required name already exists.
        If synonym_dataverse or cbas_entity_name or cbas_entity_dataverse is not mentioned
        then it will return first synonym found with the synonym_name.
        :param synonym_name str, name of the synonym whose object has to returned.
        :param synonym_dataverse str, name of the dataverse where the synonym is present
        :param cbas_entity_name str, name of the cbas_entity on which the synonym was created.
        :param cbas_entity_dataverse str, name of the dataverse where the cbas_entity is present
        """
        if not synonym_database or not synonym_dataverse:
            cmd = "select value sy from Metadata.`Synonym` as sy where \
            sy.SynonymName = \"{0}\"".format(
                CBASHelper.unformat_name(synonym_name))
            if cbas_entity_name:
                cmd += " and sy.ObjectName = \"{0}\"".format(
                    CBASHelper.unformat_name(cbas_entity_name))
            if cbas_entity_dataverse:
                cmd += " and sy.ObjectDataverseName = \"{0}\"".format(
                    CBASHelper.unformat_name(cbas_entity_dataverse))
            if cbas_entity_database:
                cmd += " and sy.ObjectDatabaseName = \"{0}\"".format(
                    CBASHelper.unformat_name(cbas_entity_database))
            cmd += ";"
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd)
            if status == "success" and results:
                synonym_database = CBASHelper.format_name(
                    results[0]["DatabaseName"])
                synonym_dataverse = CBASHelper.format_name(
                    results[0]["DataverseName"])
            else:
                return None

        dataverse_obj = self.get_dataverse_obj(synonym_dataverse, synonym_database)
        return dataverse_obj.synonyms.get(synonym_name, None)

    def get_all_synonym_objs(self):
        """
        Returns list of all Synonym objects across all the dataverses.
        """
        synonym_objs = list()
        for database in list(self.databases.values()):
            for dataverse in database.dataverses.values():
                synonym_objs.extend(dataverse.synonyms.values())
        return synonym_objs

    @staticmethod
    def get_synonym_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("synonym", {})

    def create_synonym_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating Synonyms based on CBAS Spec")

        synonym_spec = self.get_synonym_spec(cbas_spec)

        if synonym_spec.get("no_of_synonyms", 0) > 0:

            results = list()
            all_cbas_entities = (self.get_all_dataset_objs() +
                                 self.get_all_synonym_objs())
            for i in range(1, synonym_spec["no_of_synonyms"] + 1):
                if synonym_spec.get("name_key", "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = synonym_spec["name_key"] + "_{0}".format(str(i))

                database = None
                while not database:
                    database = random.choice(list(self.databases.values()))
                    if synonym_spec.get("include_databases",
                                        []) and CBASHelper.unformat_name(
                        database.name) not in synonym_spec.get(
                        "include_databases"):
                        database = None
                    if synonym_spec.get("exclude_databases",
                                        []) and CBASHelper.unformat_name(
                        database.name) in synonym_spec.get(
                        "exclude_databases"):
                        database = None

                dataverse = None
                while not dataverse:
                    dataverse = random.choice(self.get_all_dataverse_obj(
                        database.name))
                    if synonym_spec[
                        "include_dataverses"] and CBASHelper.unformat_name(
                        dataverse.name) not in synonym_spec[
                        "include_dataverses"]:
                        dataverse = None
                    if synonym_spec[
                        "exclude_dataverses"] and CBASHelper.unformat_name(
                        dataverse.name) in synonym_spec[
                        "exclude_dataverses"]:
                        dataverse = None

                cbas_entity = None
                while not cbas_entity:
                    cbas_entity = random.choice(all_cbas_entities)
                    if synonym_spec["include_datasets"] or synonym_spec[
                        "include_synonyms"]:
                        if CBASHelper.unformat_name(cbas_entity.name) not in \
                                synonym_spec["include_datasets"] + \
                                synonym_spec["include_synonyms"]:
                            cbas_entity = None
                    if synonym_spec["exclude_datasets"] or synonym_spec[
                        "exclude_synonyms"]:
                        if CBASHelper.unformat_name(cbas_entity.name) in \
                                synonym_spec["exclude_datasets"] + \
                                synonym_spec["exclude_synonyms"]:
                            cbas_entity = None

                if isinstance(cbas_entity, Synonym):
                    synonym_on_synonym = True
                else:
                    synonym_on_synonym = False
                synonym = Synonym(
                    name=name, cbas_entity_name=cbas_entity.name,
                    cbas_entity_dataverse=cbas_entity.dataverse_name,
                    cbas_entity_database=cbas_entity.database_name,
                    dataverse_name=dataverse.name,
                    database_name=database.name,
                    synonym_on_synonym=synonym_on_synonym)

                if not self.create_analytics_synonym(
                        cluster=cluster, synonym_full_name=synonym.full_name,
                        cbas_entity_full_name=synonym.cbas_entity_full_name,
                        if_not_exists=False,
                        timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                    results.append(False)
                else:
                    dataverse.synonyms[synonym.name] = synonym
                    results.append(True)
            return all(results)
        return True

    def get_all_synonyms_from_metadata(self, cluster):
        synonyms_query = "select value syn.DatabaseName || \".\" || " \
                         "syn.DataverseName || \".\" || syn.SynonymName from " \
                         "Metadata.`Synonym` as syn where syn.DataverseName " \
                         "<> \"Metadata\";"
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, synonyms_query, mode="immediate", timeout=300,
            analytics_timeout=300)
        if status == 'success':
            return results
        return []


class View_Util(Synonym_Util):
    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(View_Util, self).__init__(
            server_task, run_query_using_sdk)

    def create_analytics_view(
            self, cluster, view_full_name, view_defn,
            if_not_exists=False, validate_error_msg=False, expected_error=None,
            username=None, password=None, timeout=300, analytics_timeout=300):

        cmd = "create analytics view {0}".format(view_full_name)

        if if_not_exists:
            cmd += " If Not Exists"

        cmd += " as {0};".format(view_defn)

        self.log.info("Executing cmd - \n{0}\n".format(cmd))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_analytics_view(
            self, cluster, view_full_name, if_exists=False,
            validate_error_msg=False, expected_error=None, username=None,
            password=None, timeout=300, analytics_timeout=300):

        cmd = "drop analytics view {0}".format(view_full_name)

        if if_exists:
            cmd += " if exists;"
        else:
            cmd += ";"

        self.log.info("Executing cmd - \n{0}\n".format(cmd))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def get_all_views_from_metadata(self, cluster):
        views_created = []
        views_query = "select value ds.DatabaseName || \".\" || " \
                      "ds.DataverseName || \".\" || ds.DatasetName from " \
                      "Metadata.`Dataset` as ds where ds.DataverseName " \
                      "<> \"Metadata\" and ds.DatasetType = \"VIEW\";"
        while not views_created:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, views_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            if status.encode('utf-8') == 'success':
                results = list(
                    map(lambda result: result.encode('utf-8').split("."),
                        results))
                views_created = list(
                    map(lambda result: CBASHelper.format_name(*result),
                        results))
                break
        return views_created


class Index_Util(View_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Index_Util, self).__init__(
            server_task, run_query_using_sdk)

    def verify_index_created(
            self, cluster, index_name, dataset_name, indexed_fields):
        """
        Validate in cbas index entry in Metadata.Index.
        :param index_name str, name of the index to be validated.
        :param dataset_name str, name of the dataset on which the index was created.
        :param indexed_fields list, list of indexed fields.
        """
        result = True
        statement = "select * from Metadata.`Index` where DatasetName='{0}' " \
                    "and IsPrimary=False".format(
            CBASHelper.unformat_name(dataset_name))
        status, metrics, errors, content, _ = \
            self.execute_statement_on_cbas_util(cluster, statement)
        if status != "success":
            result = False
            self.log.debug("Index not created. Metadata query status = %s",
                           status)
        else:
            index_found = False
            for idx in content:
                if idx["Index"]["IndexName"] == CBASHelper.unformat_name(
                        index_name):
                    index_found = True
                    field_names = []
                    for index_field in indexed_fields:
                        if "meta()".lower() in index_field:
                            field_names.append(index_field.split(".")[1])
                        else:
                            field_names.append(str(index_field.split(":")[0]))
                            field_names.sort()
                    self.log.debug(field_names)

                    actual_field_names = []
                    for index_field in idx["Index"]["SearchKey"]:
                        if type(index_field) is list:
                            index_field = ".".join(index_field)
                        actual_field_names.append(str(index_field))
                        actual_field_names.sort()
                    self.log.debug(actual_field_names)

                    if field_names != actual_field_names:
                        result = False
                        self.log.debug("Index fields not correct")
                    break
            result &= index_found
        return result, content

    def verify_index_used(
            self, cluster, statement, index_used=False, index_name=None):
        """
        Validate whether an index was used while executing the query.
        :param statement str, query to be executed.
        :param index_used bool, verify whether index was used or not.
        :param index_name str, name of the index that should have been used while
        executing the query.
        """
        statement = 'EXPLAIN %s' % statement
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, statement)
        if status == 'success':
            if not errors:
                if index_used:
                    if ("index-search" in str(results)) and (
                            "data-scan" not in str(results)):
                        self.log.info(
                            "INDEX-SEARCH is found in EXPLAIN hence indexed "
                            "data will be scanned to serve %s" % statement)
                        if index_name:
                            if CBASHelper.unformat_name(index_name) in str(
                                    results):
                                return True
                            else:
                                return False
                        return True
                    else:
                        return False
                else:
                    if ("index-search" not in str(results)) and (
                            "data-scan" in str(results)):
                        self.log.info(
                            "DATA-SCAN is found in EXPLAIN hence index is "
                            "not used to serve %s" % statement)
                        return True
                    else:
                        return False
            else:
                return False
        else:
            return False

    def generate_create_index_cmd(self, index_name, indexed_fields, dataset_name,
                                  analytics_index=False, if_not_exists=False):
        index_fields = ""
        for index_field in indexed_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]

        if analytics_index:
            create_idx_statement = "create analytics index {0}".format(
                index_name)
        else:
            create_idx_statement = "create index {0}".format(index_name)
        if if_not_exists:
            create_idx_statement += " IF NOT EXISTS"
        create_idx_statement += " on {0}({1});".format(
            dataset_name, index_fields)

        return create_idx_statement

    def create_cbas_index(
            self, cluster, index_name, indexed_fields, dataset_name,
            analytics_index=False, validate_error_msg=False,
            expected_error=None, username=None, password=None,
            timeout=300, analytics_timeout=300, if_not_exists=False):
        """
        Create index on dataset.
        :param index_name str, name of the index to be created.
        :param dataset_name str, name of the dataset on which the index was created.
        :param indexed_fields list, list of indexed fields.
        :param analytics_index bool, If True, then use create analytics index syntax
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        :param if_not_exists : bool, checks if index doesn't exist before issuing create command
        """
        create_idx_statement = self.generate_create_index_cmd(index_name, indexed_fields,
                                                              dataset_name, analytics_index,
                                                              if_not_exists)

        self.log.info("Executing cmd - \n{0}\n".format(create_idx_statement))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, create_idx_statement, username=username,
            password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def generate_drop_index_cmd(self, index_name, dataset_name,
                                analytics_index=False, if_exists=False):

        dataset_name = self.format_name(dataset_name)
        index_name = self.format_name(index_name)
        if analytics_index:
            drop_idx_statement = "drop analytics index {0}.{1}".format(
                dataset_name, index_name)
        else:
            drop_idx_statement = "drop index {0}.{1}".format(
                dataset_name, index_name)
        if if_exists:
            drop_idx_statement += " IF EXISTS;"
        else:
            drop_idx_statement += ";"

        return drop_idx_statement

    def drop_cbas_index(self, cluster, index_name, dataset_name,
                        analytics_index=False, validate_error_msg=False,
                        expected_error=None, username=None, password=None,
                        timeout=300, analytics_timeout=300, if_exists=False):
        """
        Drop index on dataset.
        :param index_name str, name of the index to be created.
        :param dataset_name str, name of the dataset on which the index was created.
        :param analytics_index bool, If True, then use create analytics index syntax
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        :param if_exists : bool, checks if index even exists before issuing drop command
        """
        dataset_name = self.format_name(dataset_name)
        index_name = self.format_name(index_name)
        if analytics_index:
            drop_idx_statement = "drop analytics index {0}.{1}".format(
                dataset_name, index_name)
        else:
            drop_idx_statement = "drop index {0}.{1}".format(
                dataset_name, index_name)
        if if_exists:
            drop_idx_statement += " IF EXISTS;"
        else:
            drop_idx_statement += ";"

        self.log.info("Executing cmd - \n{0}\n".format(drop_idx_statement))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, drop_idx_statement, username=username,
            password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def get_all_index_objs(self):
        """
        Returns list of all Synonym objects across all the dataverses.
        """
        index_objs = list()
        for dataset in self.get_all_dataset_objs():
            index_objs.extend(dataset.indexes.values())
        return index_objs

    @staticmethod
    def get_index_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("index", {})

    def create_index_from_spec(self, cluster, cbas_spec):
        self.log.info(
            "Creating Secondary indexes on datasets based on CBAS spec")

        index_spec = self.get_index_spec(cbas_spec)

        results = list()
        for i in range(1, index_spec.get("no_of_indexes", 0) + 1):
            if index_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = index_spec["name_key"] + "_{0}".format(str(i))

            datasets = list(set(self.get_all_dataset_objs()) - set(
                self.get_all_dataset_objs("external")))
            dataset = None
            while not dataset:
                dataset = random.choice(datasets)

                if index_spec.get("include_databases", []) and CBASHelper.unformat_name(
                        dataset.database_name) not in index_spec["include_databases"]:
                    dataset = None
                if index_spec.get("exclude_databases", []) and CBASHelper.unformat_name(
                        dataset.database_name) in index_spec["exclude_databases"]:
                    dataset = None

                if index_spec.get("include_dataverses",
                                  []) and CBASHelper.unformat_name(
                    dataset.dataverse_name) not in index_spec[
                    "include_dataverses"]:
                    dataset = None
                if index_spec.get("exclude_dataverses",
                                  []) and CBASHelper.unformat_name(
                    dataset.dataverse_name) in index_spec["exclude_dataverses"]:
                    dataset = None

                if index_spec.get("include_datasets",
                                  []) and CBASHelper.unformat_name(
                    dataset.name) not in index_spec["include_datasets"]:
                    dataset = None
                if index_spec.get("exclude_datasets",
                                  []) and CBASHelper.unformat_name(
                    dataset.name) in index_spec["exclude_datasets"]:
                    dataset = None

            index = CBAS_Index(
                name=name, dataset_name=dataset.name,
                dataverse_name=dataset.dataverse_name,
                database_name=dataset.database_name,
                indexed_fields=index_spec["indexed_fields"][i % len(
                    index_spec["indexed_fields"])])

            if index_spec.get("creation_method", "all") == "all":
                creation_method = random.choice(["cbas_index", "index"])
            else:
                creation_method = index_spec["creation_method"].lower()
            if creation_method == "cbas_index":
                index.analytics_index = True
            else:
                index.analytics_index = False

            if not self.create_cbas_index(
                    cluster, index_name=index.name,
                    indexed_fields=index.indexed_fields,
                    dataset_name=index.full_dataset_name,
                    analytics_index=index.analytics_index,
                    timeout=cbas_spec.get("api_timeout", 300),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                results.append(False)
            else:
                dataset.indexes[index.name] = index
                results.append(True)

        return all(results)

    def get_all_indexes_from_metadata(self, cluster):
        indexes_query = "select value idx.DatabaseName || \".\" || " \
                        "idx.DataverseName || \".\" || idx.DatasetName || " \
                        "\".\" || idx.IndexName from Metadata.`Index` as " \
                        "idx where idx.DataverseName <> \"Metadata\" and " \
                        "idx.IsPrimary <> true"
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, indexes_query, mode="immediate", timeout=300,
            analytics_timeout=300)
        if status == 'success':
            return results
        return []


class UDFUtil(Index_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(UDFUtil, self).__init__(server_task, run_query_using_sdk)

    def generate_create_udf_cmd(self, name, dataverse=None, database=None, or_replace=False,
                                parameters=[], body=None, if_not_exists=False,
                                query_context=False, use_statement=False):

        create_udf_statement = ""
        if use_statement:
            create_udf_statement += 'use ' + dataverse + ';\n'
        create_udf_statement += "create"
        if or_replace:
            create_udf_statement += " or replace"
        create_udf_statement += " analytics function "
        if database and dataverse and not query_context and not use_statement:
            create_udf_statement += "{0}.{1}.".format(database, dataverse)
        elif dataverse and not query_context and not use_statement:
            create_udf_statement += "{0}.".format(dataverse)
        create_udf_statement += name
        # The below "if" is for a negative test scenario where we do not put
        # function parenthesis.
        if parameters is not None:
            param_string = ",".join(parameters)
            create_udf_statement += "({0})".format(param_string)
        if if_not_exists:
            create_udf_statement += " if not exists "
        if body:
            create_udf_statement += "{" + body + "}"

        return create_udf_statement

    def create_udf(self, cluster, name, dataverse=None, database=None,
                   or_replace=False, parameters=[], body=None,
                   if_not_exists=False, query_context=False,
                   use_statement=False, validate_error_msg=False,
                   expected_error=None, username=None, password=None,
                   timeout=300, analytics_timeout=300):
        """
        Create CBAS User Defined Functions
        :param name str, name of the UDF to be created.
        :param dataverse str, name of the dataverse in which the UDF will be created.
        :param or_replace Bool, Whether to use 'or replace' flag or not.
        :param parameters list/None, if None then the create UDF is passed
        without function parenthesis.
        :param body str/None, if None then no function body braces are passed.
        :param if_not_exists bool, whether to use "if not exists" flag
        :param query_context bool, whether to use query_context while
        executing query using REST API calls
        :param use_statement bool, Whether to use 'Use' statement while
        executing query.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : int
        :param analytics_timeout : int
        """
        param = {}
        create_udf_statement = self.generate_create_udf_cmd(name, dataverse, database, or_replace,
                                                            parameters, body, if_not_exists,
                                                            query_context, use_statement)

        self.log.info("Executing cmd - \n{0}\n".format(create_udf_statement))

        status, metrics, errors, results, \
            _ = self.execute_parameter_statement_on_cbas_util(
            cluster, create_udf_statement, timeout=timeout, username=username,
            password=password, analytics_timeout=analytics_timeout,
            parameters=param)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def generate_drop_udf_cmd(self, name, dataverse, database=None, parameters=[],
                              if_exists=False, use_statement=False, query_context=False):
        drop_udf_statement = ""
        if use_statement:
            drop_udf_statement += 'use ' + dataverse + ';\n'
        drop_udf_statement += "drop analytics function "
        if database and dataverse and not query_context and not use_statement:
            drop_udf_statement += "{0}.{1}.".format(database, dataverse)
        if dataverse and not query_context and not use_statement:
            drop_udf_statement += "{0}.".format(dataverse)
        drop_udf_statement += name
        # The below "if" is for a negative test scenario where we do not put
        # function parenthesis.
        if parameters is not None:
            param_string = ",".join(parameters)
            drop_udf_statement += "({0})".format(param_string)
        if if_exists:
            drop_udf_statement += " if exists"

        return drop_udf_statement

    def drop_udf(self, cluster, name, dataverse, database, parameters=[],
                 if_exists=False, use_statement=False, query_context=False,
                 validate_error_msg=False, expected_error=None,
                 username=None, password=None, timeout=300,
                 analytics_timeout=300):
        """
        Drop UDF.
        :param name str, name of the UDF to be created.
        :param dataverse str, name of the dataverse in which the UDF will be created.
        :param parameters list/None, if None then the create UDF is passed
        without function parenthesis.
        :param if_exists bool, whether to use "if exists" flag
        :param query_context bool, whether to use query_context while
        executing query using REST API calls
        :param use_statement bool, Whether to use 'Use' statement while
        executing query.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        """
        param = {}
        drop_udf_statement = self.generate_drop_udf_cmd(name, dataverse, database,
                                                        parameters, if_exists, use_statement,
                                                        query_context)

        if query_context:
            if dataverse:
                param["query_context"] = "default:{0}".format(dataverse)
            else:
                param["query_context"] = "default:Default"

        self.log.info("Executing cmd - \n{0}\n".format(drop_udf_statement))

        status, metrics, errors, results, \
            _ = self.execute_parameter_statement_on_cbas_util(
            cluster, drop_udf_statement, timeout=timeout, username=username,
            password=password, analytics_timeout=analytics_timeout,
            parameters=param)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def get_udf_obj(self, cluster, name, dataverse_name=None,
                    database_name=None, parameters=[]):
        """
        Return CBAS_UDF object if the UDF with the required name already
        exists.
        If dataverse_name is not mentioned then it will return first
        CBAS_UDF found with the name.
        If parameters is None, then it will return UDF with name and dataverse.
        :param name str, name of the UDF whose object has to returned.
        :param dataverse_name str, name of the dataverse under which UDF was created.
        :param parameters List/None, parameters passed during creating
        function.
        """
        cmd = "select value func from Metadata.`Function` as func where " \
              "func.Name=`{0}`".format(CBASHelper.unformat_name(name))
        if database_name:
            cmd += " and func.Databasename = \"{0}\"".format(
                CBASHelper.unformat_name(database_name))
        if dataverse_name:
            cmd += " and func.DataverseName = \"{0}\"".format(
                CBASHelper.unformat_name(dataverse_name))
        if parameters is not None:
            if "..." in parameters:
                parameters.remove("...")
                parameters.append("args")
            cmd += " and Params = {0}".format(json.dumps(parameters))
        cmd += ";"

        status, metrics, errors, results, \
            _ = self.execute_statement_on_cbas_util(cluster, cmd)

        if status == "success" and results:
            dataverse_name = CBASHelper.format_name(
                results[0]["DataverseName"])
            function_name = CBASHelper.format_name(results[0]["Name"])
        else:
            return None

        dataverse_obj = self.get_dataverse_obj(dataverse_name)
        return dataverse_obj.udfs[function_name]

    def get_all_udf_objs(self):
        """
        Returns list of all CBAS_UDF objects across all the dataverses.
        """
        udfs = list()
        for database in list(self.databases.values()):
            for dataverse in database.dataverses.values():
                udfs.extend(dataverse.udfs.values())

        return udfs

    def validate_udf_in_metadata(
            self, cluster, udf_name, udf_dataverse_name, udf_database_name,
            parameters, body, dataset_dependencies=[], udf_dependencies=[],
            synonym_dependencies=[]):
        """
        Validates whether an entry for UDF is present in Metadata.Function.
        :param udf_name : str, UDF which has to be validated.
        :param udf_dataverse_name : str, dataverse where the UDF is present
        :param parameters : list, list of parameters used while creating UDF
        :param body : str, body of the UDF
        :param dataset_dependencies : list, list of lists with each inner
        list of format [dataset's_dataverse_name, dataset_name]
        :param udf_dependencies : list, list of lists with each inner
        list of format [udf_dataverse_name, udf_name, udf_arity]
        :param synonym_dependencies : list, list of lists with each inner
        list of format [synonym_dataverse_name, synonym_name]
        :return boolean
        """
        self.log.info("Validating UDF entry in Metadata")
        cmd = "select value func from Metadata.`Function` as func where " \
              "Name=\"{0}\" and DataverseName=\"{1}\" and DatabaseName=\"{2}\"".format(
            CBASHelper.unformat_name(udf_name),
            CBASHelper.metadata_format(udf_dataverse_name),
            CBASHelper.metadata_format(udf_database_name))

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, \
            _ = self.execute_statement_on_cbas_util(cluster, cmd)

        if parameters and parameters[0] == "...":
            arity = -1
            parameters[0] = "args"
        else:
            arity = len(parameters)
            parameters = [CBASHelper.unformat_name(param) for param in parameters]

        if status == "success":
            response = []
            for result in results:
                if int(result["Arity"]) != arity:
                    self.log.error("Expected Value : {0}\tActual Value : {1}"
                                   .format(arity, result["Arity"]))
                    response.append(False)
                    continue
                if result["Definition"] != body:
                    self.log.error("Expected Value : {0}\tActual Value : {1}"
                                   .format(body, result["Definition"]))
                    response.append(False)
                    continue
                if result["Params"] != parameters:
                    self.log.error("Expected Value : {0}\tActual Value : {1}"
                                   .format(parameters, result["Params"]))
                    response.append(False)
                    continue

                for dataset_dependency in dataset_dependencies:
                    if dataset_dependency not in result["Dependencies"][0]:
                        self.log.error(
                            "Expected Value : {0}\tActual Value : {1}".format(
                                dataset_dependencies, result[
                                    "Dependencies"][0]))
                        response.append(False)
                        continue
                for synonym_dependency in synonym_dependencies:
                    if synonym_dependency not in result["Dependencies"][3]:
                        self.log.error(
                            "Expected Value : {0}\tActual Value : {1}".format(
                                synonym_dependencies, result[
                                    "Dependencies"][0]))
                        response.append(False)
                        continue
                for udf_dependency in udf_dependencies:
                    if udf_dependency not in result["Dependencies"][1]:
                        self.log.error(
                            "Expected Value : {0}\tActual Value : {1}".format(
                                udf_dependencies, result["Dependencies"][1]))
                        response.append(False)
                        continue
                response.append(True)
            if response:
                return any(response)
            else:
                return False
        else:
            return False

    def verify_function_execution_result(
            self, cluster, func_name, func_parameters, expected_result=None,
            validate_error_msg=False, expected_error=None, username=None,
            password=None, timeout=300, analytics_timeout=300):
        cmd = "{0}({1})".format(func_name, ",".join([str(param) for param in func_parameters]))
        self.log.info("Executing function {0}".format(cmd))
        status, metrics, errors, results, \
            _ = self.execute_statement_on_cbas_util(
            cluster, cmd, timeout=timeout, username=username, password=password,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                if results[0] != expected_result:
                    self.log.error(
                        "Expected Result : {0}\tActual Result : {1}".format(
                            expected_result, results[0]
                        ))
                    return False
                else:
                    return True

    def get_all_udfs_from_metadata(self, cluster):
        udfs_created = []
        udf_query = "select value(fn) from Metadata.`Function` as fn"
        status, _, _, results, _ = self.execute_statement_on_cbas_util(
            cluster, udf_query, mode="immediate",
            timeout=300, analytics_timeout=300)
        if status == 'success':
            for r in results:
                udf_full_name = ".".join(
                    [r["DatabaseName"], r["DataverseName"], r["Name"]])
                if int(r["Arity"]) == -1:
                    param = ["..."]
                else:
                    param = r["Params"]
                udfs_created.append([udf_full_name, param])
        return udfs_created


class CBOUtil(UDFUtil):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(CBOUtil, self).__init__(server_task, run_query_using_sdk)

    """
    Method creates samples on collection specified.
    collection_name str fully qualified name of the collection on which sample is to be created.
    sample_size str accepted values are low, medium and high
    sample_sed int can be a positive or negative integer
    """

    def create_sample_for_analytics_collections(
            self, cluster, collection_name, sample_size=None, sample_seed=None):

        cmd = "ANALYZE ANALYTICS COLLECTION %s" % collection_name
        params = dict()
        if sample_seed is not None:
            params["sample-seed"] = sample_seed
        if sample_size is not None:
            params["sample"] = sample_size
        if params:
            params = json.dumps(params)
            cmd += " WITH {0}".format(params)
        cmd += ";"

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cluster, cmd)
        if status != "success":
            return False
        else:
            return True

    """
    Method drops any existing sample created on collection specified.
    """

    def drop_sample_for_analytics_collections(self, cluster, collection_name):
        cmd = "ANALYZE ANALYTICS COLLECTION %s DROP STATISTICS;" % collection_name

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cluster, cmd)
        if status != "success":
            return False
        else:
            return True

    def verify_sample_present_in_Metadata(self, cluster, dataset_name, dataverse_name=None):
        query = "select count(*) from Metadata.`Index` where IsPrimary=false and IndexStructure=\"SAMPLE\" and DatasetName= \"%s\"" % dataset_name
        if dataverse_name:
            query += " and DataverseName=\"%s\"" % CBASHelper.metadata_format(dataverse_name)
        query += ";"
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cluster, query)
        if status == "success" and results[0]["$1"]:
            return True
        else:
            return False


class CbasUtil(CBOUtil):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(CbasUtil, self).__init__(server_task, run_query_using_sdk)

        self.travel_sample_inventory_views = {
            "airline_view": 149,
            "airport_view": 1709,
            "hotel_endorsement_view": 4004,
            "hotel_review_view": 4104,
            "hotel_view": 917,
            "landmark_view": 4495,
            "route_schedule_view": 505300,
            "route_view": 24024
        }
        self.travel_sample_inventory_collections = {
            "airline": 187,
            "airport": 1968,
            "hotel": 917,
            "landmark": 4495,
            "route": 24024
        }

        self.travel_sample_inventory_indexes = {
            "airline": [
                {
                    "index_name": "al_type_idx_airline",
                    "indexed_field": ["`type`:string"]
                },
                {
                    "index_name": "al_name_idx_airline",
                    "indexed_field": ["name:string"]
                },
                {
                    "index_name": "al_country_idx_airline",
                    "indexed_field": ["country:string"]
                },
                {
                    "index_name": "al_callsign_idx_airline",
                    "indexed_field": ["callsign:string"]
                },
                {
                    "index_name": "al_id_idx_airline",
                    "indexed_field": ["id:bigint"]
                }
            ],
            "airport": [
                {
                    "index_name": "ar_airportname_idx_airport",
                    "indexed_field": ["airportname:string"]
                },
                {
                    "index_name": "ar_city_idx_airport",
                    "indexed_field": ["city:string"]
                },
                {
                    "index_name": "ar_country_idx_airport",
                    "indexed_field": ["country:string"]
                },
                {
                    "index_name": "ar_geo_idx_airport",
                    "indexed_field": ["geo.lat:double", "geo.lon:DOUBLE", "geo.alt:bigint"]
                },
                {
                    "index_name": "ar_id_idx_airport",
                    "indexed_field": ["id:bigint"]
                }
            ],
            "hotel": [
                {
                    "index_name": "h_country_idx_hotel",
                    "indexed_field": ["country:string"]
                },
                {
                    "index_name": "h_city_idx_hotel",
                    "indexed_field": ["city:string"]
                },
                {
                    "index_name": "h_state_idx_hotel",
                    "indexed_field": ["state:string"]
                },
                {
                    "index_name": "h_geo_idx_hotel",
                    "indexed_field": ["geo.lat:double", "geo.lon:DOUBLE", "geo.accuracy:string"]
                }
            ],
            "landmark": [
                {
                    "index_name": "l_country_idx_landmark",
                    "indexed_field": ["country:string"]
                },
                {
                    "index_name": "l_city_idx_landmark",
                    "indexed_field": ["city:string"]
                },
                {
                    "index_name": "l_activity_idx_landmark",
                    "indexed_field": ["activity:string"]
                },
                {
                    "index_name": "l_geo_idx_landmark",
                    "indexed_field": ["geo.lat:double", "geo.lon:DOUBLE", "geo.accuracy:string"]
                }
            ],
            "route": [
                {
                    "index_name": "r_airline_idx_route",
                    "indexed_field": ["airline:string"]
                },
                {
                    "index_name": "r_sourceairport_idx_route",
                    "indexed_field": ["sourceairport:string"]
                },
                {
                    "index_name": "r_destinationairport_idx_route",
                    "indexed_field": ["destinationairport:string"]
                },
                {
                    "index_name": "r_stops_idx_route",
                    "indexed_field": ["stops:bigint"]
                },
                {
                    "index_name": "r_distance_idx_route",
                    "indexed_field": ["distance:double"]
                }
            ]
        }

    def get_ingestion_status(self, cluster, username=None, password=None):
        """
        Retreives ingestion matrics for all collections
        """
        cbas_helper = CBASHelper(cluster.master)
        try:
            resp = cbas_helper.get_ingestion_status(username, password)
            return resp.json()
        except Exception as e:
            raise Exception(str(e))

    def wait_for_data_ingestion_in_the_collections(self, cluster, timeout=100000):
        completed = False
        start_time = time.time()
        while not completed and time.time() < start_time + timeout:
            resp = self.get_ingestion_status(cluster)
            if resp == {}:
                return True
            for link in resp["links"]:
                if not isinstance(link["state"], list):
                    completed = True
                else:
                    for state in link["state"]:
                        progress = state["progress"]
                        collections = list()
                        for scope in state["scopes"]:
                            scope_name = scope["name"].replace("/", ".")
                            for collection in scope["collections"]:
                                collections.append(
                                    f"{scope_name}.{collection['name']}")
                        self.log.debug(
                            "For collections {0} on link {1} ingestion "
                            "progress : {2}%".format(
                                str(collections), link["name"], progress * 100
                            ))
                        if math.isclose(progress, 1.0):
                            completed = True
                        else:
                            completed = False
            time.sleep(5)
        if completed:
            self.log.info("Ingestion Complete")
            return True
        else:
            self.log.error("Ingestion not completed even after {} sec".format(timeout))
            return False

    def delete_request(
            self, cluster, client_context_id, username=None, password=None):
        """
        Deletes a request from CBAS
        """
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        try:
            if client_context_id is None:
                payload = "client_context_id=None"
            else:
                payload = "client_context_id=" + client_context_id

            status = cbas_helper.delete_active_request_on_cbas(
                payload, username, password)
            self.log.info(status)
            return status
        except Exception as e:
            raise Exception(str(e))

    def retrieve_request_status_using_handle(
            self, cluster, server, handle, shell=None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(server)

        """
        This check is requires as the mode param being used while running
        analytical query is not a released feature. Currently handle
        returned by the query is in incorrect format, where http protocol is
        being returned with ssl port
        """
        if "18095" in handle:
            handle = handle.replace("http", "https")

        output, error = shell.execute_command(
            "curl --tlsv1.2 -g -v {0} -u {1}:{2} -k".format(
                handle, cluster.cbas_cc_node.rest_username,
                cluster.cbas_cc_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)

        if not shell:
            shell.disconnect()

        status = ""
        handle = ""
        if 'status' in response:
            status = response['status']
        if 'handle' in response:
            handle = response['handle']

        self.log.debug("status=%s, handle=%s" % (status, handle))
        return status, handle

    def _run_concurrent_queries(
            self, cluster, query, mode, num_queries, rest=None, batch_size=100,
            timeout=300, analytics_timeout=300, wait_for_execution=True):
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.timeout_count = 0
        self.handles = []
        self.concurrent_batch_size = batch_size
        # Run queries concurrently
        self.log.debug("Running queries concurrently now...")
        threads = []
        for i in range(0, num_queries):
            threads.append(Thread(
                target=self._run_query, name="query_thread_{0}".format(i),
                args=(cluster, query, mode, rest, False, 0, timeout, analytics_timeout)))
        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                self.log.debug("Submitted {0} queries".format(i))
                sleep(5)
            thread.start()
        sleep(3)
        if wait_for_execution:
            for thread in threads:
                thread.join()

            self.log.debug("%s queries submitted, %s failed, %s passed, "
                           "%s rejected, %s cancelled, %s timeout"
                           % (
                               num_queries, self.failed_count,
                               self.success_count,
                               self.rejected_count, self.cancel_count,
                               self.timeout_count))
            if self.failed_count + self.error_count != 0:
                raise Exception("Queries Failed:%s , Queries Error Out:%s"
                                % (self.failed_count, self.error_count))
        return self.handles

    def _run_query(self, cluster, query, mode, rest=None,
                   validate_item_count=False,
                   expected_count=0, timeout=300, analytics_timeout=300):
        # Execute query (with sleep induced)
        name = current_thread().name
        client_context_id = name
        try:
            status, metrics, errors, results, handle = \
                self.execute_statement_on_cbas_util(
                    cluster, query, mode=mode, timeout=timeout,
                    client_context_id=client_context_id,
                    analytics_timeout=analytics_timeout)
            # Validate if the status of the request is success,
            # and if the count matches num_items
            if mode == "immediate":
                if status == "success":
                    if validate_item_count:
                        if results[0]['$1'] != expected_count:
                            self.log.warn("Query result: %s", results[0]['$1'])
                            self.log.error("*** Thread %s: failure ***", name)
                            self.failed_count += 1
                        else:
                            self.log.debug("--- Thread %s: success ---", name)
                            self.success_count += 1
                    else:
                        self.log.debug("--- Thread %s: success ---", name)
                        self.success_count += 1
                else:
                    self.log.warn("Status = %s", status)
                    self.log.error("*** Thread %s: failure ***", name)
                    self.failed_count += 1
            elif mode == "async":
                if status == "running" and handle:
                    self.log.debug("--- Thread %s: success ---", name)
                    self.handles.append(handle)
                    self.success_count += 1
                else:
                    self.log.warn("Status = %s", status)
                    self.log.error("*** Thread %s: failure ***", name)
                    self.failed_count += 1
            elif mode == "deferred":
                if status == "success" and handle:
                    self.log.debug("--- Thread %s: success ---", name)
                    self.handles.append(handle)
                    self.success_count += 1
                else:
                    self.log.warn("Status = %s", status)
                    self.log.error("*** Thread %s: failure ***", name)
                    self.failed_count += 1
            elif mode is None:
                if status == "success":
                    self.log.debug("--- Thread %s: success ---", name)
                    self.success_count += 1
                else:
                    self.log.warn("Status = %s", status)
                    self.log.error("*** Thread %s: failure ***", name)
                    self.failed_count += 1

        except Exception as e:
            if str(e) == "Request Rejected":
                self.log.debug("Error 503 : Request Rejected")
                self.rejected_count += 1
            elif str(e) == "Request TimeoutException":
                self.log.debug("Request TimeoutException")
                self.timeout_count += 1
            elif str(e) == "Request RuntimeException":
                self.log.debug("Request RuntimeException")
                self.timeout_count += 1
            elif str(e) == "Request RequestCancelledException":
                self.log.debug("Request RequestCancelledException")
                self.cancel_count += 1
            elif str(e) == "CouchbaseException":
                self.log.debug("General CouchbaseException")
                self.rejected_count += 1
            elif str(e) == "Capacity cannot meet job requirement":
                self.log.debug(
                    "Error 500 : Capacity cannot meet job requirement")
                self.rejected_count += 1
            else:
                self.error_count += 1
                self.log.error(str(e))

    def retrieve_cc_ip_from_master(self, cluster, timeout=300):
        end_time = time.time() + timeout
        response = None
        counter = 1
        while time.time() < end_time:
            try:
                response = self.fetch_analytics_cluster_response(cluster)
                if response:
                    break
            except Exception:
                pass
            finally:
                time.sleep(min(10, 2 * counter))
                counter += 1
        if response:
            cc_node_id = ""
            cc_node_ip = ""
            if 'ccNodeId' in response:
                cc_node_id = response['ccNodeId']
            if 'nodes' in response:
                nodes = response['nodes']
                for node in nodes:
                    if node["nodeId"] == cc_node_id:
                        cc_node_ip = node['nodeName'].split(":")[0]
                        break
            self.log.debug("CC IP retrieved from master is {0}".format(cc_node_ip))
            return cc_node_ip
        else:
            self.log.debug(
                "Following response was received from master node - {"
                "0}".format(response))
            return response

    def fetch_analytics_cluster_response(self, cluster):
        """
        Retrieves response from /analytics/cluster endpoint
        """
        rest = RestConnection(cluster.master)
        url = "http://{0}:{1}/_p/cbas/analytics/cluster".format(
            cluster.master.ip, CbServer.port)
        if CbServer.use_https:
            url = "https://{0}:{1}/_p/cbas/analytics/cluster".format(
                cluster.master.ip, CbServer.ssl_port)

        headers = rest._create_capi_headers(
            cluster.master.rest_username, cluster.master.rest_password)

        status, content, header = rest._http_request(
            url, 'GET', headers=headers)

        if status:
            return json.loads(content)
        else:
            for node in cluster.cbas_nodes:
                try:
                    rest = RestConnection(node, 30)
                    url = "http://{0}:{1}/analytics/cluster".format(
                        node.ip, CbServer.cbas_port)
                    if CbServer.use_https:
                        url = "https://{0}:{1}/analytics/cluster".format(
                            node.ip, CbServer.ssl_cbas_port)

                    headers = rest._create_capi_headers(
                        node.rest_username, node.rest_password)

                    status, content, header = rest._http_request(
                        url, 'GET', headers=headers)

                    if status:
                        return json.loads(content)
                    else:
                        self.log.error("/analytics/cluster status:{0}, content:{1}"
                                       .format(status, content))
                except Exception:
                    self.log.error("Failed to connect to node {0}".format(
                        node.ip))
            return None

    def is_analytics_running(self, cluster, timeout=600):
        end_time = time.time() + timeout
        self.log.info("Waiting for analytics service to come up")
        counter = 1
        while end_time > time.time():
            try:
                response = self.fetch_analytics_cluster_response(cluster)
                if response and response["state"] == "ACTIVE":
                    return True
            except Exception:
                pass
            finally:
                time.sleep(min(10, 2 * counter))
                counter += 1
        return False

    def get_replicas_info(self, cluster, timeout=300):
        end_time = time.time() + timeout
        response = None
        counter = 1
        while time.time() < end_time:
            try:
                response = self.fetch_analytics_cluster_response(cluster)
                if response:
                    break
            except Exception:
                pass
            finally:
                time.sleep(min(10, 2 * counter))
                counter += 1
        replica_info = dict()
        if response:
            for partition in response["partitionsTopology"]["partitions"]:
                if partition["id"] in replica_info:
                    replica_info[partition["id"]].extend(partition["replicas"])
                else:
                    replica_info[partition["id"]] = partition["replicas"]
        return replica_info

    def set_log_level_on_cbas(self, cluster, log_level_dict, timeout=300,
                              username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        payload = ""
        for component, level in log_level_dict.items():
            payload += '{ "level": "' + level + '", "name": "' + component + '" },'
        payload = payload.rstrip(",")
        params = '{ "loggers": [ ' + payload + ' ] }'
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="PUT", params=params, logger_name=None,
                timeout=timeout, username=username, password=password)
        return status, content, response

    def get_log_level_on_cbas(self, cluster, timeout=300, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="GET", params="", logger_name=None, timeout=timeout,
                username=username, password=password)
        if content:
            content = json.loads(content)
        return status, content, response

    def fetch_cbas_stats(self, cluster, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = cbas_helper.fetch_cbas_stats(
            username=username, password=password)
        return status, content, response

    def log_concurrent_query_outcome(self, cluster, node_in_test, handles):
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell = RemoteMachineShellConnection(node_in_test)
        for handle in handles:
            status, handle = self.retrieve_request_status_using_handle(
                cluster, node_in_test, handle, shell)
            if status == "running":
                run_count += 1
                self.log.debug("query with handle %s is running." % handle)
            elif status == "failed":
                fail_count += 1
                self.log.debug("query with handle %s is failed." % handle)
            elif status == "success":
                success_count += 1
                self.log.debug("query with handle %s is successful." % handle)
            else:
                aborted_count += 1
                self.log.debug("Queued job is deleted: %s" % status)

        self.log.debug("%s queued jobs are Running." % run_count)
        self.log.debug("%s queued jobs are Failed." % fail_count)
        self.log.debug("%s queued jobs are Successful." % success_count)
        self.log.debug("%s queued jobs are Aborted." % aborted_count)

    def update_service_parameter_configuration_on_cbas(
            self, cluster, config_map=None, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if config_map:
            params = json.dumps(config_map)
        else:
            raise ValueError("Missing config map")
        status, content, response = \
            cbas_helper.operation_service_parameters_configuration_cbas(
                method="PUT", params=params, username=username, password=password)
        return status, content, response

    def fetch_service_parameter_configuration_on_cbas(
            self, cluster, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_service_parameters_configuration_cbas(
                method="GET", username=username, password=password)
        return status, json.loads(content), response

    def update_node_parameter_configuration_on_cbas(
            self, cluster, config_map=None, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if config_map:
            params = json.dumps(config_map)
        else:
            raise ValueError("Missing config map")
        status, content, response = \
            cbas_helper.operation_node_parameters_configuration_cbas(
                method="PUT", params=params, username=username, password=password)
        return status, content, response

    def fetch_node_parameter_configuration_on_cbas(
            self, cluster, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_node_parameters_configuration_cbas(
                method="GET", username=username, password=password)
        return status, content, response

    def restart_analytics_cluster_uri(self, cluster, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.restart_analytics_cluster_uri(username=username,
                                                      password=password)
        return status, content, response

    def restart_analytics_node_uri(self, cluster, node_id, port=8095,
                                   username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.restart_analytics_node_uri(
                node_id, port, username=username, password=password)
        return status, content, response

    def fetch_bucket_state_on_cbas(self, cluster):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.fetch_bucket_state_on_cbas(
                method="GET", username=None, password=None)
        return status, content, response

    def fetch_pending_mutation_on_cbas_node(self, cluster, node_ip, port=9110,
                                            username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.fetch_pending_mutation_on_cbas_node(
                node_ip, port, method="GET",
                username=username, password=password)
        return status, content, response

    def fetch_pending_mutation_on_cbas_cluster(self, cluster, port=9110,
                                               username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.fetch_pending_mutation_on_cbas_cluster(
                port, method="GET", username=username, password=password)
        return status, content, response

    def fetch_dcp_state_on_cbas(self, cluster, data_set, data_verse="Default",
                                username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if not data_set:
            raise ValueError("dataset is required field")
        status, content, response = cbas_helper.fetch_dcp_state_on_cbas(
            data_set, method="GET", dataverse=data_verse,
            username=username, password=password)
        return status, content, response

    def get_analytics_diagnostics(self, cluster, timeout=300):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        response = cbas_helper.get_analytics_diagnostics(timeout=timeout)
        return response

    def set_global_compression_type(self, cluster, compression_type="snappy",
                                    username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        return cbas_helper.set_global_compression_type(compression_type,
                                                       username, password)

    def wait_for_cbas_to_recover(self, cluster, timeout=180):
        """
        Returns True if analytics service is recovered/available.
        False if service is unavailable despite waiting for specified "timeout" period.
        """
        analytics_recovered = False
        cluster_recover_start_time = time.time()
        counter = 1
        while time.time() < cluster_recover_start_time + timeout:
            try:
                status, _, _, _, _ = self.execute_statement_on_cbas_util(
                    cluster, "set `import-private-functions` `true`;ping()")
                if status == "success":
                    analytics_recovered = True
                    break
                else:
                    self.log.info("Service unavailable. Will retry..")
                    time.sleep(min(10, 2 * counter))
                    counter += 1
            except:
                self.log.info("Service unavailable. Will retry..")
                time.sleep(min(10, 2 * counter))
                counter += 1
        return analytics_recovered

    # Backup Analytics metadata
    def backup_cbas_metadata(
            self, cluster, bucket_name='default', username=None,
            password=None, include="", exclude=""):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        response = cbas_helper.backup_cbas_metadata(
            bucket_name, username=username, password=password, include=include,
            exclude=exclude)
        return response.json()

    # Restore Analytics metadata
    def restore_cbas_metadata(self, cluster, metadata, bucket_name='default',
                              username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if metadata is None:
            raise ValueError("Missing metadata")
        response = cbas_helper.restore_cbas_metadata(
            metadata, bucket_name, username=username, password=password)
        return response.json()

    def connect_links(self, cluster, cbas_spec):
        """
        Connect all links present
        """
        # Connect link only when remote links or kafka links are present,
        # Local link is connected by default and external links are not
        # required to be connected.
        results = list()
        links = (self.get_all_link_objs("couchbase") +
                 self.get_all_link_objs("kafka"))
        if len(links) > 0:
            self.log.info("Connecting all remote and kafka Links")
            for link in links:
                if not self.connect_link(
                        cluster, link.name,
                        timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout",
                                                        300)):
                    self.log.error("Failed to connect link {0}".format(
                        link.name))
                else:
                    self.log.info("Successfully connected link {0}".format(
                        link.name))

    def disconnect_links(self, cluster, cbas_spec):
        """
                Connect all links present
                """
        # Connect link only when remote links or kafka links are present,
        # Local link is connected by default and external links are not
        # required to be connected.
        results = list()
        links = (self.get_all_link_objs("couchbase") +
                 self.get_all_link_objs("kafka"))
        if len(links) > 0:
            self.log.info("Connecting all remote and kafka Links")
            for link in links:
                if not self.disconnect_link(
                        cluster, link.name,
                        timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout",
                                                        300)):
                    self.log.error("Failed to connect link {0}".format(
                        link.name))
                else:
                    self.log.info("Successfully connected link {0}".format(
                        link.name))

    def create_cbas_infra_from_spec(
            self, cluster, cbas_spec, bucket_util,
            wait_for_ingestion=True, remote_clusters=None,
            connect_link_before_creating_ds=False):
        """
        Method creates CBAS infra based on the spec data.
        :param cluster
        :param cbas_spec dict, spec to create CBAS infra.
        :param bucket_util bucket_util_obj, bucket util object of local cluster.
        :param remote_clusters bucket_util_obj, bucket util object of remote cluster.
        :param wait_for_ingestion bool
        """

        def connect_links():
            # Connect link only when remote links or kafka links are present,
            # Local link is connected by default and external links are not
            # required to be connected.
            results = list()
            links = (self.get_all_link_objs("couchbase") +
                     self.get_all_link_objs("kafka"))
            if len(links) > 0:
                self.log.info("Connecting all remote and kafka Links")
                for link in links:
                    if not self.connect_link(
                            cluster, link.name,
                            timeout=cbas_spec.get("api_timeout", 300),
                            analytics_timeout=cbas_spec.get("cbas_timeout",
                                                            300)):
                        results.append(False)
                    else:
                        results.append(True)

            if not all(results):
                return False, "Failed at connect_link"

            return True, ""

        if not self.create_database_from_spec(cluster, cbas_spec):
            return False, "Failed at create database"

        if not self.create_dataverse_from_spec(cluster, cbas_spec):
            return False, "Failed at create dataverse"

        if not self.create_remote_link_from_spec(cluster, cbas_spec):
            return False, "Failed at create remote link from spec"

        if not self.create_external_link_from_spec(cluster, cbas_spec):
            return False, "Failed at create external link from spec"

        if not self.create_kafka_link_from_spec(cluster, cbas_spec):
            return False, "Failed at create kafka link from spec"

        if connect_link_before_creating_ds:
            status, error = connect_links()
            if not status:
                return status, error

        if not self.create_remote_dataset_from_spec(
                cluster, remote_clusters, cbas_spec, bucket_util):
            return False, "Failed at create remote dataset from spec"
        if not self.create_external_dataset_from_spec(cluster, cbas_spec):
            return False, "Failed at create external dataset from spec"
        if not self.create_standalone_dataset_from_spec(cluster, cbas_spec):
            return False, "Failed at create standalone collection from spec"
        if not self.create_standalone_dataset_for_external_db_from_spec(
                cluster, cbas_spec):
            return False, "Failed at create standalone collection from spec"

        if not connect_link_before_creating_ds:
            status, error = connect_links()
            if not status:
                return status, error

        jobs = Queue()
        results = []

        if wait_for_ingestion:
            # Wait for data ingestion only for datasets based on either local KV source or remote KV source,
            internal_datasets = self.get_all_dataset_objs(
                dataset_source="dataset") + self.get_all_dataset_objs(
                dataset_source="remote")
            if len(internal_datasets) > 0:
                self.log.info("Waiting for data to be ingested into datasets")
                for dataset in internal_datasets:
                    jobs.put((
                        self.wait_for_ingestion_complete,
                        {"cluster": cluster,
                         "dataset_name": dataset.full_name,
                         "num_items": dataset.num_of_items,
                         "timeout": cbas_spec.get("api_timeout", 300)}))
            self.run_jobs_in_parallel(jobs, results,
                                      cbas_spec["max_thread_count"],
                                      async_run=False)
            if not all(results):
                return False, "Failed at wait for ingestion"

        self.log.info("Creating Synonyms based on CBAS Spec")
        if not self.create_synonym_from_spec(cluster, cbas_spec):
            return False, "Could not create synonym"

        self.log.info("Creating Indexes based on CBAS Spec")
        if not self.create_index_from_spec(cluster, cbas_spec):
            return False, "Failed at create index from spec"

        return True, "Success"

    def delete_cbas_infra_created_from_spec(
            self, cluster, cbas_spec={}, expected_index_drop_fail=True,
            expected_synonym_drop_fail=True, expected_dataset_drop_fail=True,
            expected_link_drop_fail=True, expected_dataverse_drop_fail=True,
            delete_dataverse_object=True, delete_database_object=True,
            retry_link_disconnect_fail=True):

        results = list()

        def retry_func(obj, func_name, func_params):
            retry = 0
            result = False
            while retry < 3 and not result:
                result = func_name(**func_params)
                retry += 1
            if not result:
                if hasattr(obj, "full_name"):
                    results.append(obj.full_name)
                else:
                    results.append(obj.name)

        def print_failures(name, list_of_failures):
            self.log.error("Failed to drop following {0} -".format(name))
            self.log.error(str(list_of_failures))

        self.log.info("Dropping all the indexes")
        for index in self.get_all_index_objs():
            retry_func(
                index, self.drop_cbas_index,
                {"cluster": cluster, "index_name": index.name,
                 "dataset_name": index.full_dataset_name,
                 "analytics_index": index.analytics_index,
                 "timeout": cbas_spec.get("api_timeout", 300),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 300)}
            )
        if any(results):
            if expected_index_drop_fail:
                print_failures("Secondary Indexes", results)
                results = []
            else:
                return False

        self.log.info("Dropping all the synonyms")
        for synonym in self.get_all_synonym_objs():
            retry_func(
                synonym, self.drop_analytics_synonym,
                {"cluster": cluster, "synonym_full_name": synonym.full_name,
                 "if_exists": True, "timeout": cbas_spec.get("api_timeout", 300),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 300)})
        if any(results):
            if expected_synonym_drop_fail:
                print_failures("Synonyms", results)
                results = []
            else:
                return False

        self.log.info("Disconnecting all the Links")
        for link in self.get_all_link_objs():
            if link.link_type != "s3":
                link_info = self.get_link_info(
                    cluster, link.name, link.link_type)
                if ((type(link_info) is not None) and len(link_info) > 0 and
                        "linkState" in link_info[0] and link_info[0][
                            "linkState"] == "DISCONNECTED"):
                    pass
                else:
                    retry_func(
                        link, self.disconnect_link,
                        {"cluster": cluster, "link_name": link.name,
                         "timeout": cbas_spec.get("api_timeout", 300),
                         "analytics_timeout": cbas_spec.get("cbas_timeout", 300)})
            if any(results):
                if retry_link_disconnect_fail:
                    retry_func(
                        link, self.disconnect_link,
                        {"cluster": cluster, "link_name": link.name,
                         "timeout": cbas_spec.get("api_timeout", 300),
                         "analytics_timeout": cbas_spec.get("cbas_timeout",
                                                            300)})
                    if any(results):
                        return False
                else:
                    print_failures("Disconnect Links", results)

        self.log.info("Dropping all the Datasets")
        for dataset in self.get_all_dataset_objs():
            dataset_name = dataset.full_name

            if dataset.dataverse_name == "Default":
                dataset_name = dataset.name

            retry_func(
                dataset, self.drop_dataset,
                {"cluster": cluster, "dataset_name": dataset_name,
                 "if_exists": True, "timeout": cbas_spec.get("api_timeout", 300),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 300),
                 "analytics_collection": random.choice([True, False])})
        if any(results):
            if expected_dataset_drop_fail:
                print_failures("Datasets", results)
                results = []
            else:
                return False

        self.log.info("Dropping all the Links")
        for link in self.get_all_link_objs():
            retry_func(
                link, self.drop_link,
                {"cluster": cluster, "link_name": link.name,
                 "if_exists": True, "timeout": cbas_spec.get("api_timeout", 300),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 300)})

        if any(results):
            if expected_link_drop_fail:
                print_failures("Links", results)
                results = []
            else:
                return False

        self.log.info("Dropping all the Dataverses")
        for dataverse in self.get_all_dataverse_obj():
            if dataverse.name != "Default":
                retry_func(
                    dataverse, self.drop_dataverse,
                    {"cluster": cluster, "dataverse_name": dataverse.name,
                     "if_exists": True,
                     "database_name": dataverse.database_name,
                     "analytics_scope": random.choice([True, False]),
                     "timeout": cbas_spec.get("api_timeout", 300),
                     "analytics_timeout": cbas_spec.get("cbas_timeout",
                                                        300)})

        self.log.info("Dropping all the Databases")
        databases = list(self.databases.values())
        for database in databases:
            if database.name != "Default":
                retry_func(
                    database, self.drop_database,
                    {"cluster": cluster, "database_name": database.name,
                     "if_exists": True}
                )

        if any(results):
            if expected_dataverse_drop_fail:
                print_failures("Dataverses", results)
            else:
                return False
        return True

    def kill_cbas_process(self, cluster, cbas_nodes=[]):
        results = {}
        if not cbas_nodes:
            cbas_nodes = [cluster.cbas_cc_node]
        for cbas_node in cbas_nodes:
            cbas_shell = RemoteMachineShellConnection(cbas_node)
            output, error = cbas_shell.kill_cbas()
            results[cbas_node.ip] = (output, error)
            cbas_shell.disconnect()
        return results

    def start_kill_processes_task(self, cluster, cluster_util, cbas_kill_count=0,
                                  memcached_kill_count=0, interval=5):
        if not cbas_kill_count and not memcached_kill_count:
            return None
        process_kill_task = KillProcessesInLoopTask(
            cluster, self, cluster_util, cbas_kill_count, memcached_kill_count,
            interval)
        self.task.jython_task_manager.add_new_task(process_kill_task)
        return process_kill_task

    def wait_for_processes(self, servers, processes=[], timeout=600):
        results = {}
        for server in servers:
            shell_helper = RemoteMachineHelper(RemoteMachineShellConnection(
                server))
            results[server.ip] = []
            for process in processes:
                process_running = shell_helper.is_process_running(process)
                process_timeout = time.time() + timeout
                while process_running is None and \
                        time.time() <= process_timeout:
                    time.sleep(1)
                    process_running = shell_helper.is_process_running(process)
                results[server.ip].append(process_running)
            time.sleep(20)
        return results

    def start_connect_disconnect_links_task(self, cluster, links,
                                            run_infinitely=False, interval=5):
        if not links:
            return None
        links_task = DisconnectConnectLinksTask(
            cluster, self, links, run_infinitely, interval)
        self.task.jython_task_manager.add_new_task(links_task)
        return links_task

    def cleanup_cbas(self, cluster):
        """
        This method will delete all the analytics entities on the specified
        cluster
        """
        try:
            # Drop all UDFs
            for udf in self.get_all_udfs_from_metadata(cluster):
                if not self.drop_udf(
                        cluster, CBASHelper.format_name(udf[0]), None, None, udf[1]):
                    self.log.error("Unable to drop UDF {0}".format(udf[0]))

            # Drop all indexes
            for idx in self.get_all_indexes_from_metadata(cluster):
                idx_split = idx.split(".")
                if not self.drop_cbas_index(cluster, idx_split[-1],
                                            ".".join(idx_split[:-1])):
                    self.log.error(
                        "Unable to drop Index {0}".format(idx))

            # Drop all Synonyms
            for syn in self.get_all_synonyms_from_metadata(cluster):
                if not self.drop_analytics_synonym(cluster, syn):
                    self.log.error("Unable to drop Synonym {0}".format(syn))

            # Disconnect all remote links
            remote_links = self.get_all_links_from_metadata(cluster, "couchbase")
            for remote_link in remote_links:
                if not self.disconnect_link(cluster, remote_link):
                    self.log.error(
                        "Unable to disconnect Link {0}".format(remote_link))

            # Disconnect all Kafka links
            kafka_links = self.get_all_links_from_metadata(cluster, "kafka")
            for kafka_link in kafka_links:
                if not self.disconnect_link(cluster, kafka_link):
                    self.log.error(
                        "Unable to disconnect Link {0}".format(kafka_link))

            # Drop all datasets
            for ds in self.get_all_datasets_from_metadata(cluster):
                if not self.drop_dataset(cluster, ds):
                    self.log.error("Unable to drop Dataset {0}".format(ds))

            for lnk in self.get_all_links_from_metadata(cluster):
                if not self.drop_link(cluster, lnk):
                    self.log.error("Unable to drop Link {0}".format(lnk))

            for dv in self.get_all_dataverses_from_metadata(cluster):
                if dv != "Default.Default":
                    if not self.drop_dataverse(cluster, dv):
                        self.log.error("Unable to drop Dataverse {0}".format(dv))

            for db in self.get_all_databases_from_metadata(cluster):
                if db != "Default":
                    if not self.drop_database(cluster, db):
                        self.log.error("Unable to drop Database {0}".format(db))
        except Exception as e:
            self.log.info(str(e))

    def get_replica_number_from_settings(
            self, node, method="GET", param="", username=None, password=None,
            validate_error_msg=False, expected_error="",
            expected_error_code=None):
        """
        This method get the current replica number that is set for analytics
        from cluster settings page.
        """
        status, status_code, content, errors = CBASHelper.analytics_replica_settings(
            self.log, node, method=method, params=param, timeout=300,
            username=username, password=password)
        if validate_error_msg:
            if self.validate_error_in_response(
                    status, errors, expected_error, expected_error_code):
                return 4
            else:
                return -1
        if status:
            return content["numReplicas"]
        else:
            return -1

    def set_replica_number_from_settings(
            self, node, replica_num=0, username=None, password=None,
            validate_error_msg=False, expected_error="",
            expected_error_code=None):
        """
        This method sets the replica number for analytics on cluster
        settings page.
        """
        params = {"numReplicas": replica_num}
        params = urllib.parse.urlencode(params)
        return self.get_replica_number_from_settings(
            node, "POST", params, username, password, validate_error_msg,
            expected_error, expected_error_code)

    def wait_for_replication_to_finish(self, cluster, timeout=600):
        """
        This method waits for replication to finish.
        """
        self.log.info("Waiting for replication to finish")
        stats_helper = StatsHelper(cluster.master)
        ingestion_complete = False
        end_time = time.time() + timeout
        while (not ingestion_complete) and time.time() < end_time:
            result = stats_helper.get_range_api_metrics(
                "cbas_pending_replicate_ops", function=None,
                label_values={"nodesAggregation": "sum"}, optional_params=None)
            if result:
                for data in result["data"]:
                    data["values"].sort(key=lambda x: x[0])
                    if int(data["values"][-1][1]) == 0:
                        ingestion_complete = True
            time.sleep(3)
        return ingestion_complete

    def get_actual_number_of_replicas(self, cluster):
        """
        Fetches number of replicas that are currently created in CBAS
        """
        response = self.fetch_analytics_cluster_response(cluster)
        if response:
            return response["partitionsTopology"]["numReplicas"]
        else:
            return -1

    def verify_actual_number_of_replicas(self, cluster, expected_num):
        """
        Verifies actual number of replicas created for each partition.
        """
        response = self.fetch_analytics_cluster_response(cluster)
        if response:
            replica_num_matched = True
            if response["partitionsTopology"]["numReplicas"] == expected_num:
                replica_num_matched = replica_num_matched and True
            else:
                self.log.error("Expected number of replicas - {0}, Actual "
                               "number of replicas - {1}".format(
                    expected_num, response["partitionsTopology"]["numReplicas"]))
                replica_num_matched = replica_num_matched and False

            partition_ids = list()
            for partition in response["partitions"]:
                partition_ids.append(partition["partitionId"])

            for partition in response["partitionsTopology"]["partitions"]:
                if partition["id"] in partition_ids:
                    if len(partition["replicas"]) == expected_num:
                        replica_num_matched = replica_num_matched and True
                    else:
                        self.log.error(
                            "Actual number of replicas for partition {0} is {1}".format(
                                expected_num, len(partition["replicas"])))
                        replica_num_matched = replica_num_matched and False
            return replica_num_matched
        else:
            return False

    def force_flush_cbas_data_to_disk(self, cbas_node, dataverse_name,
                                      dataset_name, username=None, password=None):
        """
        This method force flushes the data to the disk for the dataset
        specified.
        """
        url = "http://{0}:8095/analytics/connector?".format(
            cbas_node.ip)
        for dv_part in dataverse_name.split("."):
            url += "dataverseName={0}&".format(urllib.parse.quote_plus(
                CBASHelper.unformat_name(dv_part), safe=""))
        url += "datasetName={0}".format(urllib.parse.quote_plus(
            CBASHelper.unformat_name(dataset_name), safe=""))

        if not username:
            username = cbas_node.rest_username
        if not password:
            password = cbas_node.rest_password
        response = requests.get(url, auth=(username, password))
        if response.status_code in [200, 201, 204]:
            return True
        else:
            return False

    def get_partition_storage_paths(self, cluster):
        """
        This method returns a dict containing info on which node is the
        partition located and it's storage path on that node.
        """
        nodes_info = dict()
        storage_info = dict()

        response = self.fetch_analytics_cluster_response(cluster)

        if response:
            for node in response["nodes"]:
                if node["nodeId"] not in nodes_info:
                    nodes_info[node["nodeId"]] = node["nodeName"].split(":")[0]
            for partition in response["partitions"]:
                if partition["partitionId"] not in storage_info:
                    storage_info["partitionId"] = {
                        "path": partition["path"],
                        "node": nodes_info[partition["nodeId"]]
                    }
        return storage_info

    def get_btree_file_paths(self, cluster):
        partition_paths = self.get_partition_storage_paths(cluster)
        paths = list()
        for partition_id in partition_paths:
            for node in cluster.servers:
                if node.ip == partition_paths[partition_id]["node"]:
                    break
            shell = RemoteMachineShellConnection(node)
            shell.list_files(partition_paths[partition_id]["path"])

    def connect_disconnect_all_local_links(self, cluster, disconnect=False):
        result = True
        for dv in self.dataverses.values():
            if disconnect:
                if not self.disconnect_link(cluster, link_name=dv.name + ".Local"):
                    self.log.error("Failed to disconnect link - {0}".format(
                        dv.name + ".Local"))
                    result = result and False
            else:
                if not self.connect_link(cluster, link_name=dv.name + ".Local"):
                    self.log.error("Failed to connect link - {0}".format(
                        dv.name + ".Local"))
                    result = result and False
        return result

    def get_cbas_nodes(self, cluster, cluster_util, servers=None):
        if servers is None:
            servers = cluster.servers
        cbas_servers = cluster_util.get_nodes_from_services_map(
            cluster, service_type=CbServer.Services.CBAS,
            get_all_nodes=True, servers=servers)
        new_servers = []
        for server in servers:
            for cbas_server in cbas_servers:
                if cbas_server.ip == server.ip and \
                        cbas_server.port == server.port and \
                        server not in new_servers:
                    new_servers.append(server)
        return new_servers

    '''
    This method verifies whether all the datasets and views for travel-sample.inventory get's loaded
    automatically on analytics workbench.
    '''

    def verify_datasets_and_views_are_loaded_for_travel_sample(self, cluster):
        views_list = self.travel_sample_inventory_views.keys()
        dataset_list = self.travel_sample_inventory_collections.keys()
        end_time = time.time() + 300
        while views_list != [] and time.time() < end_time:
            for view in views_list:
                result = self.validate_dataset_in_metadata(cluster, view, "travel-sample.inventory")
                if result:
                    views_list.remove(view)
            time.sleep(5)
        while dataset_list != [] and time.time() < end_time:
            for dataset in dataset_list:
                result = self.validate_dataset_in_metadata(cluster, dataset, "travel-sample.inventory")
                if result:
                    dataset_list.remove(dataset)
            time.sleep(5)
        if views_list != [] or dataset_list != []:
            return False
        else:
            return True

    def generate_copy_to_s3_cmd(self, collection_name=None, dataverse_name=None,
                                database_name=None, source_definition_query=None,
                                alias_identifier=None, destination_bucket=None,
                                destination_link_name=None, path=None, partition_by=None,
                                partition_alias=None, compression=None, order_by=None,
                                max_object_per_file=None, file_format=None):
        cmd = "COPY "
        if source_definition_query:
            cmd = cmd + "( {0} ) ".format(source_definition_query)
        elif database_name and dataverse_name and collection_name:
            cmd = cmd + "{0}.{1}.{2} ".format(database_name, dataverse_name, collection_name)
        elif dataverse_name is not None and dataverse_name != "Default" and collection_name:
            cmd = cmd + "{0}.{1} ".format(dataverse_name, collection_name)
        else:
            cmd = cmd + "{0} ".format(collection_name)

        if alias_identifier:
            cmd += "AS {0} ".format(alias_identifier)

        destination_bucket = CBASHelper.format_name(destination_bucket)
        cmd += "TO {0} AT {1} ".format(destination_bucket, destination_link_name)
        if path and partition_alias:
            if isinstance(partition_alias, list):
                cmd += "PATH(\"{0}\"".format(path)
                for part_alias in partition_alias:
                    cmd += ", {0}".format(part_alias)
                cmd += ") "
            else:
                cmd += "PATH(\"{0}\", {1}) ".format(path, partition_alias)
        else:
            cmd += "PATH(\"{0}\") ".format(path)
        over = dict()
        if isinstance(partition_by, list) and isinstance(partition_alias, list):
            build = ""
            for i in range(len(partition_by)):
                if i == 0:
                    build += "{0} as {1}".format(partition_by[i], partition_alias[i])
                else:
                    build += ", {0} as {1}".format(partition_by[i], partition_alias[i])
            over["PARTITION BY"] = build

        elif partition_by and partition_alias:
            over["PARTITION BY"] = "{0} AS {1}".format(partition_by, partition_alias)
        if order_by:
            over["ORDER BY"] = order_by
        with_dict = dict()
        if compression:
            with_dict["compression"] = compression
        if max_object_per_file:
            with_dict["max-objects-per-file"] = str(max_object_per_file)
        if file_format:
            with_dict["format"] = file_format

        if len(over) > 0:
            cmd += "OVER ("
            for key in over:
                cmd += "{0} {1} ".format(key, over[key])
            cmd += " ) "
        if len(with_dict) > 0:
            cmd += "WITH " + json.dumps(with_dict)

        return cmd

    def copy_to_s3(
            self, cluster, collection_name=None, dataverse_name=None,
            database_name=None, source_definition_query=None,
            alias_identifier=None, destination_bucket=None,
            destination_link_name=None, path=None, partition_by=None,
            partition_alias=None, compression=None, order_by=None,
            max_object_per_file=None, file_format=None, username=None,
            password=None, timeout=300, analytics_timeout=300,
            validate_error_msg=None, expected_error=None,
            expected_error_code=None):

        cmd = self.generate_copy_to_s3_cmd(collection_name, dataverse_name,
                                           database_name, source_definition_query,
                                           alias_identifier, destination_bucket,
                                           destination_link_name, path, partition_by,
                                           partition_alias, compression, order_by,
                                           max_object_per_file, file_format)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True

    def generate_copy_to_kv_cmd(self, collection_name=None, database_name=None,
                                dataverse_name=None, source_definition=None,
                                dest_bucket=None, link_name=None, primary_key=None, function=None):

        cmd = "COPY "
        if source_definition:
            cmd = cmd + "( {0} ) ".format(source_definition)
        elif database_name and dataverse_name and collection_name:
            cmd = cmd + "{0}.{1}.{2} ".format(
                database_name, dataverse_name, collection_name)
        elif (dataverse_name is not None and dataverse_name != "Default"
              and collection_name):
            cmd = cmd + "{0}.{1} ".format(dataverse_name, collection_name)
        else:
            cmd = cmd + "{0} ".format(collection_name)

        cmd += "as c "
        cmd += "TO "
        if dest_bucket:
            cmd += dest_bucket
        cmd += " AT "
        if link_name:
            cmd += link_name
        cmd += " KEY "
        if primary_key:
            primary_key = 'c.' + primary_key
        else:
            primary_key = "AUTOGENERATED"

        if function:
            primary_key = function.format(primary_key)

        cmd += primary_key

        return cmd

    def copy_to_kv(self, cluster, collection_name=None, database_name=None,
                   dataverse_name=None, source_definition=None,
                   dest_bucket=None, link_name=None, primary_key=None, function=None,
                   username=None, password=None, timeout=300,
                   analytics_timeout=300, validate_error_msg=None,
                   expected_error=None, expected_error_code=None):
        """
        Method to copy query results to a KV collection using a remote link.
        """
        # with clause yet to be decided.
        cmd = self.generate_copy_to_kv_cmd(collection_name, database_name,
                                           dataverse_name, source_definition,
                                           dest_bucket, link_name, primary_key, function)

        for i in range(5):
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout)
            if status != "success":
                if isinstance(errors, list):
                    error_code = errors[0]["code"]
                else:
                    error_code = errors["code"]

                if isinstance(errors, list):
                    actual_error = (errors[0]["msg"]).replace("`", "")

                else:
                    actual_error = errors["msg"].replace("`", "")

                if error_code == 24230 and "WaitUntilReady" in actual_error:
                    self.log.info("Sleeping 30 seconds before executing again")
                    time.sleep(30)
                else:
                    break
            else:
                break
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error(str(errors))
                return False
            else:
                return True


class FlushToDiskTask(object):
    pass


class DisconnectConnectLinksTask(object):
    pass


class KillProcessesInLoopTask(object):
    pass


class CBASRebalanceUtil(object):
    pass


class BackupUtils(object):
    pass


class ColumnarStats(object):
    def cpu_utalization_rate(self, cluster):
        uri = "https://" + cluster.srv + ":18091/pools/default"
        username = cluster.servers[0].rest_username
        password = cluster.servers[0].rest_password
        utilization = 0
        resp = requests.get(uri, auth=(username, password), verify=False)
        if resp.status_code != 200:
            return -1
        resp = resp.json()
        for nodes in resp["nodes"]:
            utilization += nodes["systemStats"]["cpu_utilization_rate"]
        cpu_node_average = utilization / len(resp["nodes"])
        return cpu_node_average
