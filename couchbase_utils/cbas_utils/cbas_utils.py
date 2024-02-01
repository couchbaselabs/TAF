"""
Created on 08-Dec-2020

@author: Umang

Very Important Note - All the CBAS DDLs currently run sequentially,
thus the code for executing DDLs is hard coded to run on a single thread.
If this changes in future, we can just remove this hard coded value.
"""
import concurrent
import concurrent.futures
import json
import urllib
import time
from threading import Thread
import threading
import string
import random
import importlib
import copy
import requests

from Goldfish.templates.crudTemplate.docgen_template import Hotel
from couchbase_helper.tuq_helper import N1QLHelper
from global_vars import logger
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import (
    Database, Dataverse, Remote_Link, External_Link, Kafka_Link, Dataset,
    Remote_Dataset, External_Dataset, Standalone_Dataset, Synonym,
    CBAS_Index)
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from common_lib import sleep
from Queue import Queue
from sdk_exceptions import SDKException
from collections_helper.collections_spec_constants import MetaCrudParams
from Jython_tasks.task import Task, RunQueriesTask
from StatsLib.StatsOperations import StatsHelper
from connections.Rest_Connection import RestConnection
from Cb_constants import CbServer
from java.lang import System
from java.util.concurrent import Executors, Callable, TimeUnit, CompletableFuture


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
            from CbasLib.CBASOperations_JavaSDK import CBASHelper as SDK_CBASHelper
            return SDK_CBASHelper(cluster)
        else:
            return CBASHelper(cluster.cbas_cc_node)

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
            self.log.debug("Running query on cbas: %s" % statement)
            response = cbas_helper.execute_statement_on_cbas(
                statement, mode, pretty, timeout, client_context_id,
                username, password,
                analytics_timeout=analytics_timeout,
                time_out_unit=time_out_unit,
                scan_consistency=scan_consistency, scan_wait=scan_wait)
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
                self.log.debug("Error message mismatch. Expected: %s, got: %s"
                               % (expected_error, actual_error))
                return False
            self.log.debug("Error message matched. Expected: %s, got: %s"
                           % (expected_error, actual_error))
            if expected_error_code is not None:
                if isinstance(errors, list):
                    error_code = errors[0]["code"]
                else:
                    error_code = errors["code"]

                if expected_error_code != error_code:
                    self.log.debug("Error code mismatch. Expected: %s, got: %s"
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
                    generated_name = '.'.join(''.join(random.choice(
                        string.ascii_letters + string.digits)
                                                      for _ in range(max_name_len))
                                              for _ in range(name_cardinality))
                else:
                    generated_name = '.'.join(''.join(random.choice(
                        string.ascii_letters + string.digits)
                                                      for _ in range(
                        random.randint(1, max_name_len)))
                                              for _ in range(name_cardinality))
            reserved_words = {"at", "in", "for", "by", "which", "select",
                              "from", "like", "or", "and", "to", "if", "else",
                              "as", "with", "on", "where", "is", "all", "end",
                              "div", "into", "let", "asc", "desc", "key",
                              "any", "run", "set", "use", "type"}
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
    def get_goldfish_spec(module_name):
        """
        Fetches the goldfish_specs from spec file mentioned.
        :param module_name str, name of the module from where the specs have to be fetched
        """
        spec_package = importlib.import_module(
            'pytests.Goldfish.templates.' + module_name)
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
                             async_run=False):

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
    def get_kv_entity(cluster, bucket_util, bucket_cardinality=1,
                      include_buckets=[], exclude_buckets=[],
                      include_scopes=[], exclude_scopes=[],
                      include_collections=[], exclude_collections=[]):
        bucket = None
        while not bucket:
            bucket = random.choice(cluster.buckets)
            if include_buckets and bucket.name not in include_buckets:
                bucket = None
            if exclude_buckets and bucket.name in exclude_buckets:
                bucket = None
        if bucket_cardinality > 1:
            scope = None
            while not scope:
                scope = random.choice(bucket_util.get_active_scopes(bucket))
                if include_scopes and scope.name not in include_scopes:
                    scope = None
                if exclude_scopes and scope.name in exclude_scopes:
                    scope = None
            if bucket_cardinality > 2:
                collection = None
                while not collection:
                    collection = random.choice(
                        bucket_util.get_active_collections(bucket, scope.name))
                    if include_collections and collection.name not in include_collections:
                        collection = None
                    if exclude_collections and collection.name in exclude_collections:
                        collection = None
                return bucket, scope, collection
            else:
                return bucket, scope, None
        else:
            return bucket, None, None

    @staticmethod
    def format_name(*args):
        return CBASHelper.format_name(*args)

    @staticmethod
    def unformat_name(*args):
        return CBASHelper.unformat_name(*args)

    @staticmethod
    def metadata_format(name):
        return CBASHelper.metadata_format(name)


class Database_Util(BaseUtil):
    """
    Database level utility
    """

    def __init__(self, server_task=None, run_query_using_sdk=False):
        super(Database_Util, self).__init__(server_task, run_query_using_sdk)
        self.databases = dict()
        default_database_obj = Database()
        self.databases[default_database_obj.name] = default_database_obj

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
        self.log.debug("Validating database entry in Metadata")
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
                      if_exists=False, timeout=300, analytics_timeout=300,
                      delete_database_obj=True):
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
                return False
            else:
                if delete_database_obj:
                    obj = self.get_database_obj(database_name)
                    if obj:
                        del self.databases[obj.name]
                return True

    def get_databases(self, cluster, retries=10):
        databases_created = []
        database_query = ("select value db.DatabaseName from Metadata."
                          "`Database` as db where db.SystemDatabase = false;")
        while not databases_created and retries:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, database_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            if status.encode('utf-8') == 'success' and results:
                databases_created = list(
                    map(lambda dv: dv.encode('utf-8'), results))
                break
            sleep(12, "Wait for atleast one dataverse to be created")
            retries -= 1
        return databases_created


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
        self.log.debug("Validating dataverse entry in Metadata")
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
        only supported on goldfish)
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        database_name = self.format_name(database_name)
        dataverse_name = self.format_name(dataverse_name)
        if database_name and database_name != "Default" and not self.create_database(
                cluster, database_name, if_not_exists=True,
                timeout=timeout, analytics_timeout=analytics_timeout):
            return False
        if analytics_scope:
            cmd = "create analytics scope "
        elif scope:
            cmd = "create scope "
        else:
            cmd = "create dataverse "
        if database_name:
            cmd += "{0}.{1}".format(database_name, dataverse_name)
        else:
            cmd += "{0}".format(dataverse_name)

        if if_not_exists:
            cmd += " if not exists"

        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to create dataverse {0}: {1}".format(dataverse_name, str(errors)))
                return False
            else:
                if not self.get_dataverse_obj(dataverse_name, database_name):
                    database_obj = self.get_database_obj(database_name)
                    if database_name:
                        obj = Dataverse(dataverse_name, database_name)
                    else:
                        obj = Dataverse(dataverse_name)
                    database_obj.dataverses[obj.name] = obj
                return True

    def drop_dataverse(
            self, cluster, dataverse_name, database_name=None, username=None,
            password=None, validate_error_msg=False, expected_error=None,
            expected_error_code=None, if_exists=False, analytics_scope=False,
            scope=False, timeout=300, analytics_timeout=300,
            delete_dataverse_obj=True, disconnect_local_link=False):
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
        only supported on goldfish)
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param delete_dataverse_obj bool, deletes dropped dataverse's object from dataverse list
        """
        database_name = self.format_name(database_name)
        dataverse_name = self.format_name(dataverse_name)
        if disconnect_local_link:
            if database_name:
                link_cmd = "disconnect link {0}.{1}.Local".format(
                    database_name, dataverse_name)
            else:
                link_cmd = "disconnect link {0}.Local".format(dataverse_name)

            status, metrics, errors, results, _ = (
                self.execute_statement_on_cbas_util(
                    cluster, link_cmd, username=username, password=password,
                    timeout=timeout, analytics_timeout=analytics_timeout))
            if status != "success":
                return False

        if analytics_scope:
            cmd = "drop analytics scope "
        elif scope:
            cmd = "drop scope "
        else:
            cmd = "drop dataverse "

        if database_name:
            cmd += "{0}.{1}".format(database_name, dataverse_name)
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
                return False
            else:
                if delete_dataverse_obj:
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
            database = random.choice(self.databases.values())
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
        results = list()
        if dataverse_spec.get("no_of_dataverses", 1) > 1:
            for i in range(1, dataverse_spec.get("no_of_dataverses", 1)):
                while True:
                    database = random.choice(self.databases.values())
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
                creation_method = dataverse_spec.get("creation_method").lower()

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

    def get_dataverses(self, cluster, retries=10):
        dataverses_created = []
        dataverse_query = "select value dv.DatabaseName || \".\" || " \
                          "dv.DataverseName from Metadata.`Dataverse` as dv " \
                          "where dv.DataverseName <> \"Metadata\";"
        while not dataverses_created and retries:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, dataverse_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            if status.encode('utf-8') == 'success' and results:
                dataverses_created = list(
                    map(lambda dv: dv.encode('utf-8'), results))
                break
            sleep(12, "Wait for atleast one dataverse to be created")
            retries -= 1
        return dataverses_created

    def get_all_dataverse_obj(self, database=None):
        """
        Get names of all dataverses in all t
        """
        dataverses = list()
        if database:
            databases = [self.get_database_obj(database)]
        else:
            databases = self.databases.values()
        for database in databases:
            dataverses.extend(list(database.dataverses.values()))
        return dataverses


class Link_Util(Dataverse_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Link_Util, self).__init__(server_task, run_query_using_sdk)

    def validate_link_in_metadata(
            self, cluster, link_name, dataverse_name, database_name="Default",
            link_type="Local", username=None, password=None,
            is_active=False, timeout=300, analytics_timeout=300):
        """
        Validates whether a link is present in Metadata.Link
        :param cluster: cluster, cluster object
        :param link_name: str, Name of the link to be validated.
        :param dataverse_name: str, dataverse name where the link is present
        :param database_name: str, database name where the dataverse is present
        :param link_type: str, type of link, valid values are Local, s3 or couchbase
        :param username : str
        :param password : str
        :param is_active : bool, verifies whether the link is active or not, valid only for
        Local and couchbase links
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        self.log.debug("Validating link entry in Metadata")
        cmd = ("select value lnk from Metadata.`Link` as lnk where "
               "lnk.DataverseName = \"{0}\" and lnk.Name = \"{1}\" and "
               "lnk.DatabaseName = \"{2}\"").format(
            self.format_name(dataverse_name), self.unformat_name(link_name),
            self.format_name(database_name))

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
            self, cluster, link_name, dataverse_name, database_name="Default",
            link_type="Local", username=None, password=None, timeout=300,
            analytics_timeout=300):
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
            cluster, link_name, dataverse_name, database_name, link_type,
            username, password, True, timeout, analytics_timeout)

    def create_link(
            self, cluster, link_properties, username=None, password=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, create_if_not_exists=False, timeout=300,
            analytics_timeout=300, create_dataverse=True):
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
                cluster, link_properties["name"], link_properties["dataverse"],
                link_properties["database"], link_properties["type"],
                username, password)

        if not exists:
            # If dataverse does not exits
            if (create_dataverse and link_properties["dataverse"] !=
                    "Default" and not self.create_dataverse(
                        cluster, link_properties["dataverse"],
                        link_properties["database"], if_not_exists=True,
                        timeout=timeout, analytics_timeout=analytics_timeout)):
                return False

            if self.run_query_using_sdk:
                status, content, errors = cbas_helper.create_link(link_properties)
            else:
                link_prop = copy.deepcopy(link_properties)
                params = dict()
                uri = ""
                if "database" in link_prop and link_prop["database"]:
                    dataverse = "{0}.{1}".format(link_prop["database"], link_prop["dataverse"])
                    del link_prop["database"]
                elif "dataverse" in link_prop:
                    dataverse = link_prop["dataverse"]
                    del link_prop["dataverse"]
                if dataverse:
                    uri += "/{0}".format(urllib.quote_plus(
                        self.metadata_format(dataverse), safe=""))

                if "name" in link_prop:
                    uri += "/{0}".format(urllib.quote_plus(
                        self.unformat_name(link_prop["name"]), safe=""))
                    del link_prop["name"]

                for key, value in link_prop.iteritems():
                    if value:
                        if isinstance(value, unicode):
                            params[key] = str(value)
                        else:
                            params[key] = value
                params = urllib.urlencode(params)
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
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def get_link_info(self, cluster, dataverse=None, database=None,
                      link_name=None, link_type=None, username=None,
                      password=None, timeout=300, validate_error_msg=False,
                      expected_error=None, expected_error_code=None):
        """
        Fetch the list of links based on parameters passed.
        :param dataverse (optional) Dataverse name where the links reside.
        If not specified, all the links from all the dataverses will be retrieved.
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
            if database == "Default" or database is None:
                status, content, errors = cbas_helper.get_link_info(
                    "{}".format(self.unformat_name(dataverse)),
                    self.unformat_name(link_name), link_type)
            else:
                status, content, errors = cbas_helper.get_link_info(
                    "{}".format(self.unformat_name(dataverse, database)),
                    self.unformat_name(link_name), link_type)
        else:
            params = dict()
            uri = ""
            if database and dataverse:
                dataverse = "{}".format(self.unformat_name(
                    database, dataverse))

            if dataverse:
                uri += "/{0}".format(urllib.quote_plus(
                    self.unformat_name(dataverse), safe=""))
            if link_name:
                uri += "/{0}".format(urllib.quote_plus(
                    self.unformat_name(link_name), safe=""))
            if link_type:
                params["type"] = link_type
            params = urllib.urlencode(params)
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
        if "database" in link_prop and link_prop["database"]:
            dataverse = "{0}.{1}".format(link_prop["database"], link_prop["dataverse"])
            del link_prop["database"]
        elif "dataverse" in link_prop:
            dataverse = link_prop["dataverse"]
            del link_prop["dataverse"]
        if dataverse:
            uri += "/{0}".format(urllib.quote_plus(self.metadata_format(
                dataverse), safe=""))

        if "name" in link_prop:
            uri += "/{0}".format(urllib.quote(
                self.unformat_name(link_prop["name"]), safe=""))
            del link_prop["name"]

        for key, value in link_prop.iteritems():
            if value:
                if isinstance(value, unicode):
                    params[key] = str(value)
                else:
                    params[key] = value
        params = urllib.urlencode(params)
        status, status_code, content, errors = cbas_helper.analytics_link_operations(
            method="PUT", uri=uri, params=params, timeout=timeout, username=username,
            password=password)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        return status

    def connect_link(self, cluster, link_name, validate_error_msg=False,
                     with_force=True, username=None, password=None,
                     expected_error=None, expected_error_code=None, timeout=300,
                     analytics_timeout=300):
        """
        Connects a Link
        :param link_name: str, Name of the link to be connected.
        :param with_force: bool, use force flag while connecting a link
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        cmd_connect_link = "connect link {}".format(self.format_name(
            link_name))

        if not with_force:
            cmd_connect_link += " with {'force':false}"

        retry_attempt = 5
        connect_link_failed = True
        while connect_link_failed and retry_attempt > 0:
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd_connect_link, username=username,
                password=password, timeout=timeout,
                analytics_timeout=analytics_timeout)

            if errors:
                # Below errors are to be fixed in Alice, until they are fixed retry is only option
                actual_error = errors[0]["msg"]
                if "Failover response The vbucket belongs to another server" in actual_error or "Bucket configuration doesn't contain a vbucket map" in actual_error:
                    retry_attempt -= 1
                    self.log.debug("Retrying connecting of bucket")
                    sleep(10)
                else:
                    self.log.debug("Not a vbucket error, so don't retry")
                    connect_link_failed = False
            else:
                connect_link_failed = False
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
                        expected_error_code=None, timeout=300, analytics_timeout=300):
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
                return False
            else:
                return True

    def get_link_obj(self, cluster, link_name, link_type=None,
                     dataverse_name=None, database_name=None):
        """
        Return Link object if the link with the required name already exists.
        If dataverse_name or link_type is not mentioned then it will return first
        link found with the link_name.
        :param link_name str, name of the link whose object has to returned.
        :param link_type str, s3 or couchbase
        :param dataverse_name str, name of the dataverse where the link is present
        """
        if not dataverse_name or not database_name:
            cmd = "select value lnk from Metadata.`Link` as lnk where " \
                  "lnk.Name = \"{0}\"".format(self.unformat_name(link_name))

            if link_type:
                cmd += " and lnk.`Type` = \"{0}\"".format(link_type.upper())

            if database_name:
                cmd += " and lnk.DatabaseName = \"{0}\"".format(
                    self.unformat_name(database_name))
            if dataverse_name:
                cmd += " and lnk.DataverseName = \"{0}\"".format(
                    self.unformat_name(dataverse_name))
            cmd += ";"

            self.log.debug("Executing cmd - \n{0}\n".format(cmd))
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd)
            if status == "success":
                if results:
                    if not database_name:
                        database_name = self.format_name(results[0]["DatabaseName"])
                    if not dataverse_name:
                        dataverse_name = self.format_name(
                            results[0]["DataverseName"])
                    link_type = results[0]["Type"]
                else:
                    return None
            else:
                return None
        else:
            database_name = self.format_name(database_name)
            dataverse_name = self.format_name(dataverse_name)
        dataverse_obj = self.get_dataverse_obj(dataverse_name, database_name)
        all_link_objs = (dataverse_obj.remote_links +
                         dataverse_obj.external_links +
                         dataverse_obj.kafka_links)
        link = all_link_objs[self.format_name(link_name)]
        if link and link_type:
            if link_type.lower() == link.link_type:
                return link
            else:
                return None
        return link

    def list_all_link_objs(self, link_type=None):
        """
        Returns list of all link objects across all the dataverses.
        :param link_type <str> s3, azureblob, gcp, kafka or couchbase, if None
        returns all
        link
        types
        """
        link_objs = list()
        for databases in self.databases.values():
            for dataverse in databases.dataverses.values():
                if not link_type:
                    link_objs.extend(dataverse.remote_links.values())
                    link_objs.extend(dataverse.external_links.values())
                    link_objs.extend(dataverse.kafka_links.values())
                else:
                    if link_type == "kafka":
                        link_objs.extend(dataverse.kafka_links.values())
                    elif link_type == "couchbase":
                        link_objs.extend(dataverse.remote_links.values())
                    else:
                        for link in dataverse.external_links.values():
                            if link.link_type == link_type.lower():
                                link_objs.append(link)
        return link_objs

    def validate_get_link_info_response(
            self, cluster, link_properties, username=None, password=None,
            timeout=300):
        response = self.get_link_info(
            cluster, link_properties["dataverse"], link_properties["database"], link_properties["name"],
            link_properties["type"], username=username, password=password,
            timeout=timeout)
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
            timeout=300, analytics_timeout=300, create_dataverse=True):
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
        if "database" in link_properties and link_properties["database"]:
            dataverse = "{0}.{1}".format(link_properties["database"], link_properties["dataverse"])
        elif "dataverse" in link_properties and link_properties["dataverse"]:
            dataverse = link_properties["dataverse"]
        self.log.info("Creating remote link {0}.{1} to remote cluster "
                      "{2}".format(dataverse,
                                   link_properties["name"],
                                   link_properties["hostname"]))
        return self.create_link(cluster, link_properties, username, password,
                                validate_error_msg, expected_error,
                                expected_error_code, create_if_not_exists, timeout,
                                analytics_timeout, create_dataverse)

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

        for i in range(1, num_of_remote_links + 1):
            if link_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name()
            else:
                name = link_spec.get("name_key") + "_{0}".format(str(i))

            database = None
            while not database:
                database = random.choice(self.databases.values())
                if link_spec.get("include_databases",
                                 []) and CBASHelper.unformat_name(
                    database.name) not in link_spec.get(
                    "include_databases"):
                    database = None
                if link_spec.get("exclude_databases",
                                 []) and CBASHelper.unformat_name(
                    database.name) in link_spec.get(
                    "exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))
                if link_spec.get("include_dataverses",
                                 []) and CBASHelper.unformat_name(
                    dataverse.name) not in link_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if link_spec.get("exclude_dataverses",
                                 []) and CBASHelper.unformat_name(
                    dataverse.name) in link_spec.get(
                    "exclude_dataverses"):
                    dataverse = None

            link = Remote_Link(
                name=name, dataverse_name=dataverse.name, database_name=dataverse.database_name,
                properties=random.choice(
                    link_spec.get("properties", [{}])))

            if not self.create_remote_link(
                    cluster, link.properties, create_if_not_exists=True,
                    timeout=cbas_spec.get("api_timeout", 300),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                results.append(False)
            else:
                dataverse.remote_links[link.name] = link
                results.append(True)
        return all(results)

    def create_remote_link_obj(
            self, cluster, dataverse=None, database=None, link_cardinality=1,
            hostname=None, username=None, password=None, encryption=None,
            certificate=None, clientCertificate=None, clientKey=None,
            no_of_objs=1, name_length=30, fixed_length=False):
        """
        Generates Remote Link objects.
        """
        count = 0
        while count < no_of_objs:
            if link_cardinality > 2:
                if not database:
                    database_name = CBASHelper.format_name(self.generate_name(
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_database(
                            cluster, database_name,
                            if_not_exists=True):
                        raise Exception("Error while creating database {"
                                        "}".format(database_name))
                else:
                    database_name = CBASHelper.format_name(database)
            elif link_cardinality == 2:
                if not dataverse:
                    dataverse_name = CBASHelper.format_name(self.generate_name(
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_dataverse(
                            cluster, dataverse_name, database_name,
                            if_not_exists=True):
                        raise Exception("Error while creating dataverse {"
                                        "}".format(dataverse_name))
                    dataverse = self.get_dataverse_obj(dataverse_name, database_name)
            else:
                dataverse = self.get_dataverse_obj("Default")

            link = Remote_Link(
                name=self.generate_name(max_length=name_length, fixed_length=fixed_length),
                dataverse_name=dataverse.name,
                database_name=dataverse.database_name,
                properties={
                    "type": "couchbase", "hostname": hostname,
                    "username": username, "password": password,
                    "encryption": encryption, "certificate": certificate,
                    "clientCertificate": clientCertificate,
                    "clientKey": clientKey})
            dataverse.remote_links[link.name] = link
            count += 1


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
            timeout=300, analytics_timeout=300, create_dataverse=True):
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
        :param analytics_timeout int, analytics query timeout
        """
        if "database" in link_properties and link_properties["database"]:
            dataverse = "{0}.{1}".format(link_properties["database"], link_properties["dataverse"])
        else:
            dataverse = link_properties["dataverse"]
        self.log.info(
            "Creating link - {0}.{1} in region {2}".format(
                dataverse, link_properties["name"],
                link_properties["region"]))
        return self.create_link(
            cluster, link_properties, username, password, validate_error_msg,
            expected_error, expected_error_code, create_if_not_exists,
            timeout, analytics_timeout, create_dataverse)

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

            database = None
            while not database:
                database = random.choice(self.databases.values())
                if link_spec.get("include_databases",
                                 []) and CBASHelper.unformat_name(
                    database.name) not in link_spec.get(
                    "include_databases"):
                    database = None
                if link_spec.get("exclude_databases",
                                 []) and CBASHelper.unformat_name(
                    database.name) in link_spec.get(
                    "exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))

                if link_spec.get("include_dataverses",
                                 []) and CBASHelper.unformat_name(
                    dataverse.name) not in link_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if link_spec.get("exclude_dataverses",
                                 []) and CBASHelper.unformat_name(
                    dataverse.name) in link_spec.get(
                    "exclude_dataverses"):
                    dataverse = None
            link = External_Link(
                name=name, dataverse_name=dataverse.name, database_name=dataverse.database_name,
                properties=properties[i % properties_size])

            if not self.create_external_link(
                    cluster, link.properties, create_if_not_exists=True,
                    timeout=cbas_spec.get("api_timeout", 300),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                results.append(False)
            else:
                dataverse.external_links[link.name] = link
                results.append(True)
        return all(results)

    def create_external_link_obj(
            self, cluster, dataverse=None, database=None, link_cardinality=1,
            accessKeyId=None, secretAccessKey=None, regions=[],
            serviceEndpoint=None, link_type="s3", no_of_objs=1, name_length=30,
            fixed_length=False, link_perm=False):
        """
        Generates External Link objects.
        """
        external_link_list = []
        count = 0
        while count < no_of_objs:
            if link_cardinality > 2:
                if not database:
                    database_name = CBASHelper.format_name(self.generate_name(
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_database(
                            cluster, database_name,
                            if_not_exists=True):
                        raise Exception("Error while creating database {"
                                        "}".format(database_name))
                else:
                    database_name = CBASHelper.format_name(database)
            elif link_cardinality == 2:
                if not dataverse:
                    dataverse_name = CBASHelper.format_name(self.generate_name(
                        name_cardinality=link_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_dataverse(
                            cluster, dataverse_name, database_name,
                            if_not_exists=True):
                        raise Exception("Error while creating dataverse")
                    dataverse = self.get_dataverse_obj(dataverse_name)
                else:
                    dataverse = self.get_dataverse_obj("Default")

            if link_type.lower() == "s3":
                link = External_Link(
                    name=self.generate_name(
                        name_cardinality=1, max_length=name_length,
                        fixed_length=fixed_length),
                    dataverse_name=dataverse.name,
                    database_name=dataverse.database_name,
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
                        dataverse_name=dataverse.name,
                        database_name=dataverse.database_name,
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
                        dataverse_name=dataverse.name,
                        database_name=dataverse.database_name,
                        properties={"type": "azureblob",
                                    "endpoint": serviceEndpoint,
                                    })
            dataverse.external_links[link.name] = link
            external_link_list.append(link)
            count += 1
        return external_link_list


class KafkaLink_Util(ExternalLink_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(KafkaLink_Util, self).__init__(server_task, run_query_using_sdk)

    def wait_for_kafka_links(self, cluster, state="CONNECTED", timeout=1200):
        kafka_links = set(self.list_all_link_objs("kafka"))
        end_time = time.time() + timeout
        while len(kafka_links) > 0 and time.time() < end_time:
            self.log.info("Waiting for KAFKA links to be {}".format(state))
            links_in_desired_state = []
            for link in kafka_links:
                link_info = self.get_link_info(
                    cluster, link.dataverse_name,
                    link.database_name,link.name, link.link_type)
                if ((type(link_info) is not None) and len(link_info) > 0 and
                        "linkState" in link_info[0] and link_info[0][
                            "linkState"] == state):
                    links_in_desired_state.append(link)
            while links_in_desired_state:
                kafka_links.remove(links_in_desired_state.pop())
            if kafka_links:
                time.sleep(60)

        if kafka_links:
            for link in kafka_links:
                self.log.error("Link {} was not in {} state even after {} "
                               "seconds".format(link.full_name, state, timeout))
            return False
        return True

    def create_kafka_link(
            self, cluster, link_name, external_db_details, dataverse_name=None,
            database_name=None, validate_error_msg=False, username=None,
            password=None, expected_error=None, timeout=300,
            analytics_timeout=300):
        """
        Creates a dataset/analytics collection on a KV bucket.
        :param link_name str, fully qualified dataset name.
        :param external_db_details <Kafka object> details of the kafka cluster
        that will be used to stream data from external database.
        :param external_db_details <ExternalDB object> details of the source
        database cluster from data is to be fetched.
        :param dataverse_name str, Dataverse where link is to be created.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        if dataverse_name and dataverse_name != "Default" and not \
                self.create_dataverse(
                cluster, dataverse_name=dataverse_name,
                database_name=database_name, if_not_exists=True,
                timeout=timeout, analytics_timeout=analytics_timeout):
            return False

        if dataverse_name:
            cmd = "CREATE LINK {0}.{1}.{2} TYPE KAFKA WITH ".format(
                CBASHelper.format_name(database_name),
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(link_name))
        else:
            cmd = "CREATE LINK {0} TYPE KAFKA WITH ".format(
                CBASHelper.format_name(link_name))

        source_details = {"sourceDetails": external_db_details}
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
        num_of_external_links = link_spec.get("no_of_kafka_links", 0)

        for i in range(0, num_of_external_links):
            if link_spec.get("name_key", "random").lower() == "random":
                name = self.generate_name(name_cardinality=1)
            else:
                name = link_spec.get("name_key") + "_{0}".format(str(i))

            database = None
            while not database:
                database = random.choice(self.databases.values())
                if link_spec.get("include_databases",
                                 []) and CBASHelper.unformat_name(
                    database.name) not in link_spec.get(
                    "include_databases"):
                    database = None
                if link_spec.get("exclude_databases",
                                 []) and CBASHelper.unformat_name(
                    database.name) in link_spec.get(
                    "exclude_databases"):
                    database = None

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj(
                    database.name))

                if link_spec.get("include_dataverses",
                                 []) and CBASHelper.unformat_name(
                    dataverse.name) not in link_spec.get(
                    "include_dataverses"):
                    dataverse = None
                if link_spec.get("exclude_dataverses",
                                 []) and CBASHelper.unformat_name(
                    dataverse.name) in link_spec.get(
                    "exclude_dataverses"):
                    dataverse = None

            if len(link_spec.get("database_type")) > 0:
                db_type = link_spec["database_type"][i % len(link_spec["database_type"])]
            else:
                db_type = random.choice(["mongo", "dynamo", "rds"])

            external_db_details = random.choice(link_spec[
                                                    "external_database_details"][db_type])

            link = Kafka_Link(
                name=name, dataverse_name=dataverse.name, database_name=dataverse.database_name,
                db_type=db_type, external_database_details=external_db_details)

            if not self.create_kafka_link(
                    cluster, name, external_db_details,
                    dataverse.name, dataverse.database_name, timeout=cbas_spec.get("api_timeout", 300),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                results.append(False)
            else:
                dataverse.kafka_links[link.name] = link
                results.append(True)
        return all(results)

    def create_kafka_link_obj(
            self, cluster, dataverse=None, database=None, link_cardinality=1,
            db_type="", external_db_details={}, no_of_objs=1,
            name_length=30, fixed_length=False):
        """
        Generates Kafka Link objects.
        """
        links = list()
        while len(links) < no_of_objs:
            if link_cardinality > 2:
                if not database:
                    database_name = CBASHelper.format_name(self.generate_name(
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_database(
                            cluster, database_name,
                            if_not_exists=True):
                        raise Exception("Error while creating database {"
                                        "}".format(database_name))
                else:
                    database_name = CBASHelper.format_name(database)
            elif link_cardinality == 2:
                if not dataverse:
                    dataverse_name = CBASHelper.format_name(self.generate_name(
                        name_cardinality=link_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_dataverse(
                            cluster, dataverse_name,
                            database_name, if_not_exists=True):
                        raise Exception("Error while creating dataverse")
                    dataverse = self.get_dataverse_obj(dataverse_name)
            else:
                dataverse = self.get_dataverse_obj("Default")

            link = Kafka_Link(
                name=self.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=fixed_length), dataverse_name=dataverse.name,
                database_name=dataverse.database_name, db_type=db_type,
                external_database_details=external_db_details)
            links.append(link)
        return links


class Dataset_Util(KafkaLink_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Dataset_Util, self).__init__(
            server_task, run_query_using_sdk)

    def copy_to_s3(self, cluster, collection_name=None, dataverse_name=None, database_name=None,
                   source_definition_query=None,
                   alias_identifier=None, destination_bucket=None, destination_link_name=None,
                   path=None, partition_by=None, partition_alias=None, compression=None, order_by=None,
                   max_object_per_file=None, file_format=None, username=None, password=None, timeout=300,
                   analytics_timeout=300, validate_error_msg=None, expected_error=None,
                   expected_error_code=None):
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

    def validate_dataset_in_metadata(
            self, cluster, dataset_name, dataverse_name=None, database_name=None, username=None,
            password=None, timeout=300, analytics_timeout=300, **kwargs):
        """
        validates metadata information about a dataset with entry in Metadata.Dataset collection.
        :param dataset_name str, name of the dataset to be validated.
        :param dataverse_name str, name of the dataverse where the dataset is present.
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param **kwargs dict, other Metadata attributes that needs to be validated.
        """
        if not database_name:
            database_name = "Default"
        self.log.debug("Validating dataset entry in Metadata")
        cmd = 'SELECT value MD FROM Metadata.`Dataset` as MD WHERE DatasetName="{0}" and DatabaseName="{1}"'.format(
            CBASHelper.unformat_name(dataset_name), CBASHelper.unformat_name(database_name))
        if dataverse_name:
            cmd += ' and DataverseName = "{0}"'.format(
                CBASHelper.metadata_format(dataverse_name))
        cmd += ";"
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
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

    def create_dataset(
            self, cluster, dataset_name, kv_entity, dataverse_name=None, database_name=None,
            if_not_exists=False, compress_dataset=False, with_clause=None,
            link_name=None, where_clause=None, validate_error_msg=False,
            username=None, password=None, expected_error=None, timeout=300,
            analytics_timeout=300, analytics_collection=False, storage_format=None):
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
        if dataverse_name and not self.create_dataverse(
                cluster, dataverse_name=dataverse_name, database_name=database_name, if_not_exists=True,
                timeout=timeout, analytics_timeout=analytics_timeout):
            return False

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

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_analytics_collection(
            self, cluster, dataset_name, kv_entity, dataverse_name=None, database_name=None,
            if_not_exists=False, compress_dataset=False, with_clause=None,
            link_name=None, where_clause=None, validate_error_msg=False,
            username=None, password=None, expected_error=None,
            timeout=300, analytics_timeout=300, storage_format=None):
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
            cluster, dataset_name, kv_entity, dataverse_name, database_name, if_not_exists,
            compress_dataset, with_clause, link_name, where_clause,
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
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
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

    def enable_analytics_from_KV(
            self, cluster, kv_entity_name, compress_dataset=False,
            validate_error_msg=False, expected_error=None, username=None,
            password=None, timeout=300, analytics_timeout=300,
            storage_format=None):
        """
        Enables analytics directly from KV.
        :param kv_entity_name: string, can be fully quantified collection name
        (bucket.scope.collection) or a bucket name.
        :param compress_cbas_collection: boolean, to enable compression on created dataset.
        :param validate_error_msg: boolean,
        :param expected_error: string,
        :param username: string,
        :param password: string,
        :param timeout: int,
        :param analytics_timeout: int,
        :param storage_format string, whether to use row or column storage
        for analytics datasets. Valid values are row and column
        """
        cmd = "Alter collection {0} enable analytics ".format(kv_entity_name)

        if compress_dataset or storage_format:
            with_params = dict()

            if compress_dataset:
                with_params["storage-block-compression"] = {'scheme': 'snappy'}

            if storage_format:
                with_params["storage-format"] = {"format": storage_format}

            cmd += " with " + json.dumps(with_params) + " "
        cmd = cmd + ";"

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def disable_analytics_from_KV(
            self, cluster, kv_entity_name, validate_error_msg=False,
            expected_error=None, username=None, password=None,
            timeout=300, analytics_timeout=300):
        """
        Disables analytics directly from KV.
        :param kv_entity_name: string, can be fully quantified collection name
        (bucket.scope.collection) or a bucket name.
        :param validate_error_msg: boolean,
        :param expected_error: string,
        :param username: string,
        :param password: string,
        :param timeout: int,
        :param analytics_timeout: int,
        """
        cmd = "Alter collection {0} disable analytics;".format(kv_entity_name)

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def get_num_items_in_cbas_dataset(
            self, cluster, dataset_name, timeout=300, analytics_timeout=300):
        """
        Gets the count of docs in the cbas dataset
        """
        total_items = -1
        mutated_items = -1
        cmd_get_num_items = "select count(*) from %s;" % dataset_name
        cmd_get_num_mutated_items = "select count(*) from %s where mutated>0;" \
                                    % dataset_name

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

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(
                cluster, cmd_get_num_mutated_items,
                timeout=timeout, analytics_timeout=analytics_timeout)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.debug("No. of items mutated in CBAS dataset {0}: {1}"
                           .format(dataset_name, results[0]['$1']))
            mutated_items = results[0]['$1']

        return total_items, mutated_items

    def wait_for_ingestion_complete(
            self, cluster, dataset_name, num_items, timeout=300):

        counter = 0
        while timeout > counter:
            self.log.debug("Total items in KV Bucket to be ingested "
                           "in CBAS datasets %s" % num_items)
            if num_items == self.get_num_items_in_cbas_dataset(cluster, dataset_name)[0]:
                self.log.debug("Data ingestion completed in %s seconds." %
                               counter)
                return True
            else:
                sleep(2)
                counter += 2

        self.log.error("Dataset: {0} kv-items: {1} ds-items: {2}".format(
            dataset_name, num_items,
            self.get_num_items_in_cbas_dataset(cluster, dataset_name)[0]))

        return False

    def wait_for_ingestion_all_datasets(self, cluster, bucket_util, timeout=600):

        self.refresh_dataset_item_count(bucket_util)
        jobs = Queue()
        results = []
        datasets = self.list_all_dataset_objs()
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
            self, cluster, dataset_name, expected_count, expected_mutated_count=0,
            num_tries=12, timeout=300, analytics_timeout=300):
        """
        Compares the count of CBAS dataset total
        and mutated items with the expected values.
        """
        count, mutated_count = self.get_num_items_in_cbas_dataset(
            cluster, dataset_name, timeout=timeout,
            analytics_timeout=analytics_timeout)
        tries = num_tries
        if expected_mutated_count:
            while (count != expected_count
                   or mutated_count != expected_mutated_count) and tries > 0:
                sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(
                    cluster, dataset_name, timeout=timeout,
                    analytics_timeout=analytics_timeout)
                tries -= 1
        else:
            while count != expected_count and tries > 0:
                sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(
                    cluster, dataset_name, timeout=timeout,
                    analytics_timeout=analytics_timeout)
                tries -= 1

        self.log.debug("Expected Count: %s, Actual Count: %s"
                       % (expected_count, count))
        self.log.debug("Expected Mutated Count: %s, Actual Mutated Count: %s"
                       % (expected_mutated_count, mutated_count))
        if count == expected_count and mutated_count == expected_mutated_count:
            return True
        elif count != expected_count:
            return False
        elif mutated_count == expected_mutated_count:
            return True
        else:
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

    def get_dataset_obj(self, cluster, dataset_name, dataverse_name=None, database_name=None):
        """
        Return Dataset/CBAS_collection object if the dataset with the required name already exists.
        If dataverse_name is not mentioned then it will return first dataset found with the dataset_name.
        :param dataset_name str, name of the dataset whose object has to returned.
        :param dataverse_name str, name of the dataverse where the dataset is present
        """
        if not dataverse_name:
            cmd = 'SELECT value MD FROM Metadata.`Dataset` as MD WHERE DatasetName="{0}";'.format(
                CBASHelper.unformat_name(dataset_name))
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd)
            if status == "success" and results:
                dataverse_name = CBASHelper.format_name(
                    results[0]["DataverseName"])
            else:
                return None

        dataverse_obj = self.get_dataverse_obj(dataverse_name, database_name)
        all_dataset_objs = (dataverse_obj.datasets +
                            dataverse_obj.remote_datasets +
                            dataverse_obj.external_datasets +
                            dataverse_obj.standalone_datasets)

        return all_dataset_objs.get(dataset_name, None)

    def list_all_crud_dataset_objs(self, dataset_source=None):
        """
        List all the datasets created during collection crud operation
        """
        dataset_objs = list()

        for database in self.databases.values():
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

    def list_all_dataset_objs(self, dataset_source=None):
        """
        :param dataset_source <str> Accepted values are dataset, remote,
        external, standalone
        Returns list of all Dataset/CBAS_Collection objects across all the dataverses.
        """
        dataset_objs = list()
        for database in self.databases.values():
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

    @staticmethod
    def get_dataset_spec(cbas_spec):
        """
        Fetches dataset specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("dataset", {})

    def create_dataset_from_spec(self, cluster, cbas_spec, bucket_util):
        self.log.info("Creating Datasets based on CBAS Spec")

        dataset_spec = self.get_dataset_spec(cbas_spec)
        results = list()

        num_datasets = dataset_spec.get("num_of_datasets", 0)

        for i in range(1, num_datasets + 1):

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj())
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
                    "cbas_collection", "cbas_dataset",
                    "enable_cbas_from_kv"]
            creation_method = random.choice(
                dataset_spec["creation_methods"])

            if dataset_spec.get("bucket_cardinality", 0) == 0:
                bucket_cardinality = random.choice([1, 3])
            else:
                bucket_cardinality = dataset_spec[
                    "bucket_cardinality"]

            bucket, scope, collection = self.get_kv_entity(
                cluster, bucket_util, bucket_cardinality,
                dataset_spec.get("include_buckets", []),
                dataset_spec.get("exclude_buckets", []),
                dataset_spec.get("include_scopes", []),
                dataset_spec.get("exclude_scopes", []),
                dataset_spec.get("include_collections", []),
                dataset_spec.get("exclude_collections", []))

            enabled_from_KV = False
            if creation_method == "enable_cbas_from_kv":
                enabled_from_KV = True
                temp_scope = scope
                temp_collection = collection
                if not scope:
                    temp_scope = bucket_util.get_scope_obj(
                        bucket, "_default")
                    temp_collection = bucket_util.get_collection_obj(
                        temp_scope, "_default")
                    # Check Synonym with name bucket name is present in Default dataverse or not
                    cmd = "select value sy from Metadata.`Synonym` as sy where \
                        sy.SynonymName = \"{0}\" and sy.DataverseName = \"{1}\";".format(
                        bucket.name, "Default")

                    status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                        cluster, cmd)
                    if status == "success":
                        if results:
                            enabled_from_KV = False
                    else:
                        enabled_from_KV = False

                temp_dataverse_name = CBASHelper.format_name(
                    bucket.name, temp_scope.name)
                temp_dataverse_obj = self.get_dataverse_obj(
                    temp_dataverse_name)
                if temp_dataverse_obj:
                    if temp_dataverse_obj.datasets.get(
                            CBASHelper.format_name(
                                temp_collection.name),
                            None):
                        enabled_from_KV = False
                if enabled_from_KV:
                    if not scope:
                        synonym_obj = Synonym(
                            bucket.name, temp_collection.name,
                            temp_dataverse_name,
                            dataverse_name="Default")
                        self.dataverses[
                            "Default"].synonyms[
                            synonym_obj.name] = synonym_obj
                    scope = temp_scope
                    collection = temp_collection
                    name = collection.name
                    dataverse = Dataverse(
                        temp_dataverse_name)
                    self.dataverses[
                        temp_dataverse_name] = dataverse
                else:
                    creation_method = random.choice(
                        ["cbas_collection",
                         "cbas_dataset"])

            if collection:
                num_of_items = collection.num_items
            else:
                num_of_items = bucket_util.get_collection_obj(
                    bucket_util.get_scope_obj(bucket, "_default"),
                    "_default").num_items

            if dataset_spec["storage_format"] == "mixed":
                storage_format = random.choice(
                    ["row", "column"])
            else:
                storage_format = dataset_spec[
                    "storage_format"]

            dataset_obj = Dataset(
                name=name, dataverse_name=dataverse.name, database_name=dataverse.database_name,
                bucket=bucket, scope=scope,
                collection=collection,
                enabled_from_KV=enabled_from_KV,
                num_of_items=num_of_items,
                storage_format=storage_format
            )

            dataverse_name = dataset_obj.dataverse_name
            if dataverse_name == "Default":
                dataverse_name = None

            if enabled_from_KV:
                results.append(
                    self.enable_analytics_from_KV(
                        cluster,
                        dataset_obj.full_kv_entity_name,
                        False, False, None, None, None,
                        timeout=cbas_spec.get(
                            "api_timeout", 300),
                        analytics_timeout=cbas_spec.get(
                            "cbas_timeout", 300),
                        storage_format=storage_format
                    ))
            else:
                if creation_method == "cbas_collection":
                    analytics_collection = True
                else:
                    analytics_collection = False
                results.append(
                    self.create_dataset(
                        cluster, dataset_obj.name,
                        dataset_obj.full_kv_entity_name,
                        dataverse_name, False, False, None,
                        None, None, False,
                        None, None, None,
                        timeout=cbas_spec.get("api_timeout",
                                              300),
                        analytics_timeout=cbas_spec.get(
                            "cbas_timeout", 300),
                        analytics_collection=analytics_collection,
                        storage_format=storage_format))
            if results[-1]:
                dataverse.datasets[
                    dataset_obj.name] = dataset_obj
        return all(results)

    def create_datasets_on_all_collections(
            self, cluster, bucket_util, cbas_name_cardinality=1,
            kv_name_cardinality=1, creation_methods=None,
            storage_format=None):
        """
        Create datasets on every collection across all the buckets and scopes.
        :param bucket_util obj, bucket_util obj to perform operations on KV bucket.
        :param cbas_name_cardinality int, no of parts in dataset name. valid value 1,2,3
        :param kv_name_cardinality int, no of parts in KV entity name. Valid values 1 or 3.
        :param remote_datasets bool, if True create remote datasets using remote links.
        :param creation_methods list, support values are "cbas_collection","cbas_dataset","enable_cbas_from_kv"
        :param storage_format string, whether to use row or column storage for analytics datasets. Valid values are
        row, column, None and mixed.
        """
        self.log.info("Creating Datasets on all KV collections")
        results = list()

        if not creation_methods:
            creation_methods = ["cbas_collection", "cbas_dataset",
                                "enable_cbas_from_kv"]

        def dataset_creation(bucket, scope, collection, storage_format):
            creation_method = random.choice(creation_methods)

            name = self.generate_name(name_cardinality=1)

            if creation_method == "enable_cbas_from_kv":
                enabled_from_KV = True
                dataverse = Dataverse(bucket.name + "." + scope.name)
                self.dataverses[dataverse.name] = dataverse
                name = CBASHelper.format_name(collection.name)
            else:
                enabled_from_KV = False
                if cbas_name_cardinality > 1:
                    dataverse = Dataverse(
                        self.generate_name(cbas_name_cardinality - 1))
                    self.dataverses[dataverse.name] = dataverse
                else:
                    dataverse = self.get_dataverse_obj("Default")

            num_of_items = collection.num_items
            if storage_format == "mixed":
                storage_format = random.choice(["row", "column"])

            dataset_obj = Dataset(
                name=name, dataverse_name=dataverse.name,
                bucket=bucket, scope=scope, collection=collection,
                enabled_from_KV=enabled_from_KV, num_of_items=num_of_items,
                storage_format=storage_format
            )

            dataverse_name = dataverse.name

            if dataverse_name == "Default":
                dataverse_name = None

            if dataset_obj.enabled_from_KV:

                if kv_name_cardinality > 1:
                    results.append(
                        self.enable_analytics_from_KV(
                            cluster, dataset_obj.full_kv_entity_name, False,
                            False, None, None, None, 300, 300, storage_format))
                else:
                    results.append(
                        self.enable_analytics_from_KV(
                            cluster, dataset_obj.get_fully_qualified_kv_entity_name(1),
                            False, False, None, None, None, 300, 300, storage_format))
            else:

                if creation_method == "cbas_collection":
                    analytics_collection = True
                else:
                    analytics_collection = False

                if kv_name_cardinality > 1 and cbas_name_cardinality > 1:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.full_kv_entity_name,
                            dataverse_name, False, False, None,
                            dataset_obj.link_name, None, False, None, None,
                            None, 300, 300, analytics_collection, storage_format))
                elif kv_name_cardinality > 1 and cbas_name_cardinality == 1:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.full_kv_entity_name,
                            None, False, False, None, dataset_obj.link_name,
                            None, False, None, None, None, 300, 300,
                            analytics_collection, storage_format))
                elif kv_name_cardinality == 1 and cbas_name_cardinality > 1:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.get_fully_qualified_kv_entity_name(1),
                            dataverse_name, False, False, None,
                            dataset_obj.link_name, None, False, None, None,
                            None, 300, 300, analytics_collection, storage_format))
                else:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.get_fully_qualified_kv_entity_name(1),
                            None, False, False, None, dataset_obj.link_name,
                            None, False, None, None,
                            None, 300, 300, analytics_collection, storage_format))

            if results[-1]:
                dataverse.datasets[dataset_obj.name] = dataset_obj

        for bucket in cluster.buckets:
            if kv_name_cardinality > 1:
                for scope in bucket_util.get_active_scopes(bucket):
                    for collection in bucket_util.get_active_collections(
                            bucket,
                            scope.name):
                        dataset_creation(bucket, scope, collection, storage_format)
            else:
                scope = bucket_util.get_scope_obj(bucket, "_default")
                dataset_creation(bucket, scope, bucket_util.get_collection_obj(
                    scope, "_default"), storage_format)

        return all(results)

    def create_dataset_obj(
            self, cluster, bucket_util, dataset_cardinality=1,
            bucket_cardinality=1, enabled_from_KV=False,
            for_all_kv_entities=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1, storage_format=None):
        """
        Generates dataset objects.
        """

        if storage_format == "mixed":
            storage_format = random.choice(["row", "column"])

        def create_object(
                bucket, scope, collection, enabled_from_KV=enabled_from_KV,
                dataset_cardinality=dataset_cardinality, name_length=name_length,
                fixed_length=fixed_length):
            if not scope:
                scope = bucket_util.get_scope_obj(bucket, "_default")
            if not collection:
                collection = bucket_util.get_collection_obj(scope, "_default")
            if enabled_from_KV:
                dataverse_name = bucket.name + "." + scope.name
                # To cover the scenario where the default collection is not
                # present.
                if not collection:
                    # Assign a dummy collection object
                    active_collections = bucket_util.get_active_collections(
                        bucket, scope.name, only_names=False)
                    collection = random.choice(active_collections)
                    collection.name = "_default"
                dataset_name = collection.name
            else:
                dataverse_name = None

                if dataset_cardinality > 1:
                    dataverse_name = self.generate_name(
                        name_cardinality=dataset_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length)
                dataset_name = self.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=fixed_length)

            if dataverse_name:
                dataverse_name = CBASHelper.unformat_name(dataverse_name)
                dataverse_obj = self.get_dataverse_obj(dataverse_name)
                if not dataverse_obj:
                    dataverse_obj = Dataverse(dataverse_name)
                    self.dataverses[dataverse_name] = dataverse_obj
            else:
                dataverse_obj = self.get_dataverse_obj("Default")

            if collection:
                num_of_items = collection.num_items
            else:
                num_of_items = 0

            dataset_obj = Dataset(
                name=dataset_name, dataverse_name=dataverse_name,
                bucket=bucket, scope=scope, collection=collection,
                enabled_from_KV=enabled_from_KV, num_of_items=num_of_items,
                storage_format=storage_format)

            self.log.info("Adding Dataset object for - {0}".format(
                dataset_obj.full_name))

            dataverse_obj.datasets[dataset_obj.name] = dataset_obj

            if enabled_from_KV:
                if collection and collection.name == "_default":
                    dataverse_obj.synonyms[bucket.name] = Synonym(
                        bucket.name, collection.name, dataverse_name,
                        dataverse_name="Default",
                        synonym_on_synonym=False)
            return dataset_obj

        dataset_objs = list()
        if for_all_kv_entities:
            for bucket in cluster.buckets:

                if bucket.name in exclude_bucket:
                    continue

                if bucket_cardinality == 1:
                    dataset_objs.append(create_object(bucket, None, None))
                else:
                    active_scopes = bucket_util.get_active_scopes(bucket)
                    for scope in active_scopes:
                        if scope.is_dropped or scope.name in exclude_scope:
                            continue

                        active_collections = bucket_util.get_active_collections(
                            bucket, scope.name, only_names=False)
                        for collection in active_collections:
                            if collection.is_dropped or collection.name in exclude_collection:
                                continue
                            dataset_objs.append(create_object(
                                bucket, scope, collection))
        else:
            for _ in range(no_of_objs):
                bucket = random.choice(cluster.buckets)
                while bucket.name in exclude_bucket:
                    bucket = random.choice(cluster.buckets)

                if bucket_cardinality == 1:
                    dataset_objs.append(create_object(bucket, None, None))
                else:
                    active_scopes = bucket_util.get_active_scopes(bucket)
                    scope = random.choice(active_scopes)
                    while scope.is_dropped or (scope.name in exclude_scope):
                        scope = random.choice(active_scopes)

                    active_collections = bucket_util.get_active_collections(
                        bucket, scope.name, only_names=False)
                    collection = random.choice(active_collections)
                    while collection.is_dropped or (
                            collection.name in exclude_collection):
                        collection = random.choice(active_collections)

                    dataset_objs.append(create_object(bucket, scope, collection))
        return dataset_objs

    def validate_docs_in_all_datasets(
            self, cluster, bucket_util, timeout=600):
        self.refresh_dataset_item_count(bucket_util)
        datasets = self.list_all_dataset_objs()
        jobs = Queue()
        results = list()

        for dataset in datasets:
            jobs.put((self.wait_for_ingestion_complete,
                      {"cluster": cluster, "dataset_name": dataset.full_name,
                       "num_items": dataset.num_of_items, "timeout": timeout}))

        self.run_jobs_in_parallel(jobs, results, 50, async_run=False)
        return all(results)

    def refresh_dataset_item_count(self, bucket_util):
        datasets = self.list_all_dataset_objs()
        for dataset in datasets:
            if dataset.kv_collection:
                dataset.num_of_items = dataset.kv_collection.num_items
            else:
                if dataset.link_name:
                    dataset.num_of_items = bucket_util.get_collection_obj(
                        bucket_util.get_scope_obj(dataset.kv_bucket, "_default"),
                        "_default").num_items
                else:
                    dataset.num_of_items = bucket_util.get_collection_obj(
                        bucket_util.get_scope_obj(dataset.kv_bucket, "_default"),
                        "_default").num_items

    def get_datasets(self, cluster, retries=10, fields=[]):
        datasets_created = []
        datasets_query = "select value regexp_replace(ds.DataverseName,\"/\",\".\") || " \
                         "\".\" || ds.DatasetName from Metadata.`Dataset` as ds " \
                         "where ds.DataverseName  <> \"Metadata\";"
        if fields:
            datasets_query = 'SELECT * ' \
                             'FROM Metadata.`Dataset` d ' \
                             'WHERE d.DataverseName <> "Metadata"'
        while not datasets_created and retries:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, datasets_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            if status.encode('utf-8') == 'success' and results:
                if fields:
                    results = CBASHelper.get_json(json_data=results)
                    for result in results:
                        ds = result['d']
                        datasets_created.append(
                            {field: ds[field] for field in fields})
                else:
                    datasets_created = list(
                        map(lambda dv: dv.encode('utf-8'), results))
                break
            sleep(12, "Wait for atleast one dataset to be created")
            retries -= 1
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
            self, remote_cluster, bucket_util, dataset_cardinality=1,
            bucket_cardinality=1, for_all_kv_entities=False,
            link=None, same_dv_for_link_and_dataset=False,
            storage_format=None, name_length=30, fixed_length=False,
            exclude_bucket=[], exclude_scope=[], exclude_collection=[],
            no_of_objs=1):
        """
        Generates remote dataset objects.
        """

        if storage_format == "mixed":
            storage_format = random.choice(["row", "column"])

        def create_object(bucket, scope, collection, link):

            if not scope:
                scope = bucket_util.get_scope_obj(bucket, "_default")
            if not collection:
                collection = bucket_util.get_collection_obj(scope, "_default")

            dataverse_name = None

            if not link:
                link = random.choice(self.list_all_link_objs(
                    link_type="couchbase"))

            if same_dv_for_link_and_dataset and link:
                dataverse_name = link.dataverse_name
            else:
                if dataset_cardinality > 1:
                    dataverse_name = self.generate_name(
                        name_cardinality=dataset_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length)
            dataset_name = self.generate_name(
                name_cardinality=1, max_length=name_length,
                fixed_length=fixed_length)

            if dataverse_name:
                dataverse_name = CBASHelper.unformat_name(dataverse_name)
                dataverse_obj = self.get_dataverse_obj(dataverse_name)
                if not dataverse_obj:
                    dataverse_obj = Dataverse(dataverse_name)
                    self.dataverses[dataverse_name] = dataverse_obj
            else:
                dataverse_obj = self.get_dataverse_obj("Default")

            if collection:
                num_of_items = collection.num_items
            else:
                num_of_items = 0

            if link:
                link_name = link.full_name
            else:
                link_name = None

            dataset_obj = Remote_Dataset(
                name=dataset_name, dataverse_name=dataverse_name,
                link_name=link_name, bucket=bucket, scope=scope,
                collection=collection, num_of_items=num_of_items,
                storage_format=storage_format)

            self.log.info("Adding Remote Dataset object for - {0}".format(
                dataset_obj.full_name))

            dataverse_obj.remote_datasets[dataset_obj.name] = dataset_obj

            return dataset_obj

        dataset_objs = list()
        if for_all_kv_entities:
            for bucket in remote_cluster.buckets:

                if bucket.name in exclude_bucket:
                    continue

                if bucket_cardinality == 1:
                    dataset_objs.append(create_object(bucket, None, None, link))
                else:
                    active_scopes = bucket_util.get_active_scopes(bucket)
                    for scope in active_scopes:
                        if scope.is_dropped or scope.name in exclude_scope:
                            continue

                        active_collections = bucket_util.get_active_collections(
                            bucket, scope.name, only_names=False)
                        for collection in active_collections:
                            if collection.is_dropped or collection.name in exclude_collection:
                                continue
                            dataset_objs.append(create_object(
                                bucket, scope, collection, link))
        else:
            for _ in range(no_of_objs):
                bucket = random.choice(remote_cluster.buckets)
                while bucket.name in exclude_bucket:
                    bucket = random.choice(remote_cluster.buckets)

                if bucket_cardinality == 1:
                    dataset_objs.append(create_object(bucket, None, None, link))
                else:
                    active_scopes = bucket_util.get_active_scopes(bucket)
                    scope = random.choice(active_scopes)
                    while scope.is_dropped or (scope.name in exclude_scope):
                        scope = random.choice(active_scopes)

                    active_collections = bucket_util.get_active_collections(
                        bucket, scope.name, only_names=False)
                    collection = random.choice(active_collections)
                    while collection.is_dropped or (
                            collection.name in exclude_collection):
                        collection = random.choice(active_collections)

                    dataset_objs.append(create_object(bucket, scope, collection, link))
        return dataset_objs

    def create_remote_dataset_from_spec(self, cluster, remote_clusters,
                                        cbas_spec, bucket_util, include_collection=None):
        """
        Creates remote datasets based on remote dataset specs.
        """
        self.log.info("Creating Remote Datasets based on CBAS Spec")

        dataset_spec = self.get_remote_dataset_spec(cbas_spec)
        results = list()

        # get all remote link objects
        remote_link_objs = set(self.list_all_link_objs("couchbase"))
        if dataset_spec.get("include_links", []):
            for link_name in dataset_spec["include_links"]:
                remote_link_objs |= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))])

        if dataset_spec.get("exclude_links", []):
            for link_name in dataset_spec["exclude_links"]:
                remote_link_objs -= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))])

        remote_link_objs = list(remote_link_objs)

        num_of_remote_datasets = dataset_spec.get("num_of_remote_datasets", 0)

        for i in range(0, num_of_remote_datasets):
            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj())
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

            if dataset_spec.get("bucket_cardinality", 0) == 0:
                bucket_cardinality = random.choice([1, 3])
            else:
                bucket_cardinality = dataset_spec["bucket_cardinality"]

            link = remote_link_objs[i % len(remote_link_objs)]
            remote_cluster = None
            if remote_clusters is None:
                my_collection = random.choice(dataset_spec.get("include_collections", [])).split('.')
                bucket = my_collection[0]
                scope = my_collection[1]
                collection = my_collection[2]

            else:

                for tmp_cluster in remote_clusters:
                    if tmp_cluster.master.ip == link.properties["hostname"]:
                        remote_cluster = tmp_cluster

                bucket, scope, collection = self.get_kv_entity(
                    remote_cluster, bucket_util, bucket_cardinality,
                    dataset_spec.get("include_buckets", []),
                    dataset_spec.get("exclude_buckets", []),
                    dataset_spec.get("include_scopes", []),
                    dataset_spec.get("exclude_scopes", []),
                    dataset_spec.get("include_collections", []),
                    dataset_spec.get("exclude_collections", []))

            if collection:
                if isinstance(collection, dict):
                    num_of_items = collection.num_items
                else:
                    num_of_items = None
            else:
                num_of_items = bucket_util.get_collection_obj(
                    bucket_util.get_scope_obj(bucket, "_default"),
                    "_default").num_items

            if dataset_spec["storage_format"] == "mixed":
                storage_format = random.choice(
                    ["row", "column"])
            else:
                storage_format = dataset_spec[
                    "storage_format"]

            dataset_obj = Remote_Dataset(
                name=name, link_name=link.full_name,
                dataverse_name=dataverse.name, database_name=dataverse.database_name, bucket=bucket,
                scope=scope, collection=collection,
                num_of_items=num_of_items,
                storage_format=storage_format
            )

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
                    dataset_obj.full_kv_entity_name, link.full_name,
                    dataverse_name, dataverse.database_name, False, False, None,
                    None, storage_format, analytics_collection,
                    False, None, None, None,
                    timeout=cbas_spec.get("api_timeout",
                                          300),
                    analytics_timeout=cbas_spec.get(
                        "cbas_timeout", 300)))
            if results[-1]:
                dataverse.remote_datasets[
                    dataset_obj.name] = dataset_obj
        return all(results)

    def create_remote_datasets_on_all_collections(
            self, cluster, remote_cluster, bucket_util,
            dataset_cardinality=1, kv_name_cardinality=1,
            creation_methods=None, storage_format=None):
        """
        Create datasets on every collection across all the buckets and scopes.
        :param bucket_util obj, bucket_util obj to perform operations on KV bucket.
        :param dataset_cardinality int, no of parts in dataset name. valid value 1,2,3
        :param kv_name_cardinality int, no of parts in KV entity name. Valid values 1 or 3.
        :param remote_datasets bool, if True create remote datasets using remote links.
        :param creation_methods list, support values are "cbas_collection","cbas_dataset"
        :param storage_format string, whether to use row or column storage for analytics datasets. Valid values are
        row, column, None and mixed.
        """
        self.log.info("Creating Remote Datasets on all KV collections")
        results = list()

        if not creation_methods:
            creation_methods = ["cbas_collection", "cbas_dataset"]

        dataset_objs = self.create_remote_dataset_obj(
            remote_cluster, bucket_util, dataset_cardinality,
            kv_name_cardinality, True, None, storage_format)

        for dataset_obj in dataset_objs:
            creation_method = random.choice(creation_methods)
            dataverse_name = dataset_obj.dataverse_name

            if dataverse_name == "Default":
                dataverse_name = None

            if creation_method == "cbas_collection":
                analytics_collection = True
            else:
                analytics_collection = False

            if kv_name_cardinality > 1 and dataset_cardinality > 1:
                results.append(
                    self.create_remote_dataset(
                        cluster, dataset_obj.name,
                        dataset_obj.full_kv_entity_name,
                        dataset_obj.link_name,
                        dataverse_name, False, False, None, None,
                        storage_format, analytics_collection,
                        False, None, None, None,
                        timeout=300, analytics_timeout=300))
            elif kv_name_cardinality > 1 and dataset_cardinality == 1:
                results.append(
                    self.create_remote_dataset(
                        cluster, dataset_obj.name,
                        dataset_obj.full_kv_entity_name,
                        dataset_obj.link_name,
                        None, False, False, None, None,
                        storage_format, analytics_collection,
                        False, None, None, None,
                        timeout=300, analytics_timeout=300))
            elif kv_name_cardinality == 1 and dataset_cardinality > 1:
                results.append(
                    self.create_remote_dataset(
                        cluster, dataset_obj.name,
                        dataset_obj.get_fully_qualified_kv_entity_name(1),
                        dataset_obj.link_name,
                        dataverse_name, False, False, None, None,
                        storage_format, analytics_collection,
                        False, None, None, None,
                        timeout=300, analytics_timeout=300))
            else:
                results.append(
                    self.create_remote_dataset(
                        cluster, dataset_obj.name,
                        dataset_obj.get_fully_qualified_kv_entity_name(1),
                        dataset_obj.link_name,
                        None, False, False, None, None,
                        storage_format, analytics_collection,
                        False, None, None, None,
                        timeout=300, analytics_timeout=300))

            if results[-1]:
                dataverse_obj = self.get_dataverse_obj(
                    dataset_obj.dataverse_name)
                dataverse_obj.remote_datasets[dataset_obj.name] = dataset_obj

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

    def create_dataset_on_external_resource(
            self, cluster, dataset_name, external_container_name,
            link_name, if_not_exists=False, dataverse_name=None, database_name=None,
            object_construction_def=None, path_on_external_container=None,
            file_format="json", redact_warning=None, header=None,
            null_string=None, include=None, exclude=None,
            parse_json_string=0, convert_decimal_to_double=0, timezone="",
            create_dv=True, validate_error_msg=False, username=None,
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
        if create_dv and dataverse_name and not self.create_dataverse(
                cluster, dataverse_name=dataverse_name, database_name=database_name, username=username,
                password=password, if_not_exists=True):
            return False

        cmd = "CREATE EXTERNAL DATASET"

        if if_not_exists:
            cmd += " if not exists"

        if database_name and dataverse_name:
            cmd += " {0}.{1}.{2}".format(database_name, dataverse_name, dataset_name)
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

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                self.log.error("Failed to create external collection {0}: {1}".format(dataset_name, str(errors)))
                self.log.error(str(cmd))
                return False
            else:
                return True

    def create_external_dataset_obj(
            self, cluster, external_container_names, link_type="s3",
            same_dv_for_link_and_dataset=False,
            dataset_cardinality=1, object_construction_def=None,
            paths_on_external_container=[], file_format="json",
            redact_warning=None, header=None, null_string=None,
            include=None, exclude=None, name_length=30, fixed_length=False,
            no_of_objs=1, parse_json_string=0, convert_decimal_to_double=0,
            timezone="", embed_filter_values=True, dataverse_name=None):
        """
        Creates a Dataset object for external datasets.
        :param external_container_names: <dict> {"external_container_name":"region"}
        :param link_type <str> s3,azureblob,gcp
        """
        external_datasets = []
        for _ in range(no_of_objs):
            external_container = random.choice(external_container_names.keys())

            all_links = self.list_all_link_objs(link_type)
            link = random.choice(all_links)
            while link.properties["region"] != external_container_names[external_container]:
                link = random.choice(all_links)

            if same_dv_for_link_and_dataset:
                dataverse = self.get_dataverse_obj(link.dataverse_name, link.database_name)
            else:
                if dataverse_name:
                    dataverse = self.get_dataverse_obj(dataverse_name)
                    if dataverse is None:
                        if not self.create_dataverse(cluster, dataverse_name, if_not_exists=True):
                            raise Exception("Error while creating dataverse")
                        dataverse = self.get_dataverse_obj(dataverse_name)
                else:
                    dataverse = random.choice(self.get_all_dataverse_obj())

            dataset = External_Dataset(
                name=self.generate_name(
                    name_cardinality=1, max_length=name_length,
                    fixed_length=fixed_length),
                dataverse_name=dataverse.name, database_name=dataverse.database_name, link_name=link.full_name,
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

            dataverse.external_datasets[dataset.name] = dataset
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
            external_link_objs.extend(self.list_all_link_objs(link_type))

        if dataset_spec.get("include_links", []):
            for link_name in dataset_spec["include_links"]:
                external_link_objs |= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))])

        if dataset_spec.get("exclude_links", []):
            for link_name in dataset_spec["exclude_links"]:
                external_link_objs -= set([self.get_link_obj(
                    cluster, CBASHelper.format_name(link_name))])

        num_of_external_datasets = dataset_spec.get("num_of_external_datasets", 0)

        for i in range(0, num_of_external_datasets):

            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj())
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
                dataset_properties = random.choice(
                    dataset_spec.get(
                        "external_dataset_properties", [{}]))
                if link.properties["region"] == dataset_properties.get(
                        "region", None):
                    break
            dataset_obj = External_Dataset(
                name=name, dataverse_name=dataverse.name, database_name=dataverse.database_name,
                link_name=link.full_name,
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
                    False, False, None, None, None, None,
                    timeout=cbas_spec.get(
                        "api_timeout", 300),
                    analytics_timeout=cbas_spec.get(
                        "cbas_timeout", 300)))
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
        elif isinstance(data, unicode):
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
            doc = json.loads(json.dumps(hotel, default=lambda o: o.__dict__, ensure_ascii=False))
            del hotel
            doc = self.convert_unicode_to_string(doc)
        except Exception as err:
            self.log.error(str(err))
        return doc

    class GenerateDocsCallable(Callable):
        def __init__(self, instance, document_size, country_type, include_country):
            self.instance = instance
            self.document_size = document_size
            self.country_type = country_type
            self.include_country = include_country

        def call(self):
            return self.instance.generate_docs(self.document_size, self.country_type, self.include_country)

    def load_doc_to_standalone_collection(
            self, cluster, collection_name, dataverse_name, database_name, no_of_docs,
            document_size=1024, batch_size=500, max_concurrent_batches=10, country_type="string", include_country=True):
        """
        Load documents to a standalone collection.
        """
        start = System.currentTimeMillis()
        executor = Executors.newFixedThreadPool(max_concurrent_batches)
        try:
            for i in range(0, no_of_docs, batch_size):
                batch_start = i
                batch_end = min(i + batch_size, no_of_docs)
                tasks = []
                
                for j in range(batch_start, batch_end):
                    tasks.append(self.GenerateDocsCallable(self, document_size, country_type, include_country))
                
                batch_docs = []
                futures = executor.invokeAll(tasks)
                for future in futures:
                    batch_docs.append(future.get())

                retry_count = 0
                while retry_count < 3:
                    result = self.insert_into_standalone_collection(cluster, collection_name,
                                                                    batch_docs, dataverse_name, database_name)
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

        end = System.currentTimeMillis()
        time_spent = end - start
        self.log.info("Took {} seconds to insert {} docs".format(
            time_spent / 1000.0, no_of_docs))
        return True

    def insert_into_standalone_collection(self, cluster, collection_name, document, dataverse_name=None,
                                          database_name=None, username=None,
                                          password=None, analytics_timeout=300, timeout=300):
        """
        Query to insert into standalone collection
        """
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
        self.log.info("Inserting into: {0}.{1}".format(CBASHelper.format_name(dataverse_name),
                                                       CBASHelper.format_name(collection_name)))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if status != "success":
            self.log.error(str(errors))
            return False
        else:
            return True

    def upsert_into_standalone_collection(self, cluster, collection_name, new_item, dataverse_name=None,
                                          database_name=None, username=None, password=None, analytics_timeout=300,
                                          timeout=300):
        """
        Upsert into standalone collection
        """
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

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if status != "success":
            self.log.error(str(errors))
            return False
        else:
            return True

    def delete_from_standalone_collection(
            self, cluster, collection_name, dataverse_name=None, database_name=None,
            where_clause=None, use_alias=False, username=None, password=None,
            analytics_timeout=300, timeout=300):
        """
        Query to delete from standalone collection
        """
        cmd = "DELETE FROM "
        if database_name:
            cmd += "{}.".format(database_name)
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

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)

        if status != "success":
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

        current_docs_count, _ = self.get_num_items_in_cbas_dataset(
            cluster, collection_full_name)

        # First insert target number of docs/
        while current_docs_count < target_num_docs:
            if not self.load_doc_to_standalone_collection(
                    cluster, collection_name, dataverse_name,
                    target_num_docs, doc_size):
                self.log.error("Error while inserting docs in "
                               "collection {}".format(collection_full_name))
                return False
            current_docs_count, _ = self.get_num_items_in_cbas_dataset(
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
                        cluster, collection_name, dataverse_name,
                        where_clause_for_delete_op.format(
                            collection_full_name, 5), use_alias):
                    self.log.error(
                        "Error while deleting docs in collection {"
                        "}".format(collection_full_name))
                    return False

            elif operation == "insert":
                self.log.info("Inserting documents in collection: {0}".format(
                    collection_full_name))
                if not self.insert_into_standalone_collection(
                        cluster, collection_name,
                        [self.generate_docs(doc_size)], dataverse_name):
                    self.log.error(
                        "Error while inserting docs in collection {"
                        "}".format(collection_full_name))
                    return False

            elif operation == "upsert":
                self.log.info("Upserting documents in collection: {0}".format(
                    collection_full_name))
                if not self.upsert_into_standalone_collection(
                        cluster, collection_name,
                        [self.generate_docs(doc_size)], dataverse_name):
                    self.log.error(
                        "Error while upserting docs in collection {"
                        "}".format(collection_full_name))
                    return False

        current_docs_count, _ = self.get_num_items_in_cbas_dataset(
            cluster, collection_full_name)

        # Make sure final number of docs are equal to target_num_docs
        while current_docs_count > target_num_docs:
            num_docs_to_delete = current_docs_count - target_num_docs
            if not self.delete_from_standalone_collection(
                    cluster, collection_name, dataverse_name,
                    where_clause_for_delete_op.format(
                        collection_full_name, num_docs_to_delete), use_alias):
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

    def copy_from_external_resource_into_standalone_collection(
            self, cluster, collection_name, aws_bucket_name,
            external_link_name, dataverse_name=None, database_name=None, files_to_include=[],
            file_format="json", type_parsing_info="", path_on_aws_bucket="",
            header=None, null_string=None, files_to_exclude=[],
            parse_json_string=0, convert_decimal_to_double=0,
            timezone="", validate_error_msg=False, username=None,
            password=None, expected_error=None, expected_error_code=None,
            timeout=7200, analytics_timeout=7200):
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

        cmd += "FROM `{0}` AT {1} ".format(aws_bucket_name,
                                           CBASHelper.format_name(
                                               external_link_name))

        if path_on_aws_bucket:
            cmd += "PATH \"{0}\" ".format(path_on_aws_bucket)

        with_parameters = dict()
        with_parameters["format"] = file_format

        if header is not None:
            with_parameters["header"] = header

        if null_string:
            with_parameters["null"] = null_string

        if files_to_include:
            with_parameters["include"] = files_to_include

        if files_to_exclude:
            with_parameters["exclude"] = files_to_exclude

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

        cmd += "WITH {0};".format(json.dumps(with_parameters))
        self.log.info("Coping into {0} from {1}".format(collection_name, external_link_name))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
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
            self, collection_name, dataverse_name=None, database_name=None, if_not_exists=True,
            primary_key={}, subquery="", link_name=None,
            external_collection=None, ddl_format="random",
            compress_dataset=False, storage_format=None):
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
            for field_name, field_type in primary_key.iteritems():
                pk += "{0}: {1}, ".format(field_name, field_type)
            pk = pk.rstrip(pk[-1:-3:-1])
        else:
            pk = "`id` : UUID"

        cmd += " PRIMARY KEY({0})".format(pk)
        if not primary_key:
            cmd += " AUTOGENERATED"

        if subquery:
            cmd += " AS {0}".format(subquery)

        if compress_dataset or storage_format:
            with_params = dict()

            if compress_dataset:
                with_params["storage-block-compression"] = {'scheme': 'snappy'}

            if storage_format:
                with_params["storage-format"] = {"format": storage_format}

            cmd += " with " + json.dumps(with_params) + " "

        if link_name and external_collection:
            cmd += " ON {0} AT {1}".format(
                CBASHelper.format_name(external_collection), link_name)

        cmd += ";"
        return cmd

    def create_standalone_collection(
            self, cluster, collection_name,
            ddl_format="random", if_not_exists=True, dataverse_name=None, database_name=None,
            primary_key={}, subquery="",
            compress_dataset=False, storage_format=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, username=None,
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

        if dataverse_name and not self.create_dataverse(
                cluster, dataverse_name=dataverse_name, database_name=database_name, username=username,
                password=password, if_not_exists=True, timeout=timeout,
                analytics_timeout=analytics_timeout):
            return False
        cmd = self.generate_standalone_create_DDL(
            collection_name, dataverse_name, database_name, if_not_exists, primary_key,
            subquery, None, None, ddl_format, compress_dataset, storage_format)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
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
            self, cluster, collection_name,
            ddl_format="random", if_not_exists=True, dataverse_name=None, database_name=None,
            primary_key={}, link_name=None, external_collection=None,
            compress_dataset=False, storage_format=None,
            validate_error_msg=False, expected_error=None,
            expected_error_code=None, username=None,
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

        if dataverse_name and not self.create_dataverse(
                cluster, dataverse_name=dataverse_name, database_name=database_name, if_not_exists=True,
                timeout=timeout, analytics_timeout=analytics_timeout):
            return False
        cmd = self.generate_standalone_create_DDL(
            collection_name, dataverse_name, database_name, if_not_exists, primary_key,
            "", link_name, external_collection, ddl_format,
            compress_dataset, storage_format)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
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
            self, no_of_objs=1, dataset_cardinality=1,
            datasource="shadow_dataset", primary_key={},
            link=None, same_dv_for_link_and_dataset=False,
            external_collection_name=None,
            storage_format=None, name_length=30, fixed_length=False):
        """
        Generates standalone dataset objects.
        :param no_of_objs <int> number of standalone dataset objects to be
        created.
        :param dataset_cardinality <int> Accepted value 1,2,3
        :param datasource <str> Source from where data will be ingested
        into dataset. Accepted Values - mongo, dynamo, cassandra, s3, gcp,
        azure, shadow_dataset
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

            dataverse_name = None
            database_name = None
            if not link:
                if datasource in ["s3", "azure", "gcp"]:
                    link = random.choice(self.list_all_link_objs(
                        link_type=datasource.lower()))
                    dataset_properties = link.properties
                else:
                    if datasource in ["mongo", "dynamo", "cassandra"]:
                        link = random.choice(self.list_all_link_objs(
                            link_type="kafka"))
                    dataset_properties = {}

            if same_dv_for_link_and_dataset and link:
                dataverse_name = link.dataverse_name
                database_name = link.database_name
            else:
                if dataset_cardinality > 1:
                    dataverse_name = self.generate_name(
                        name_cardinality=dataset_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length)
            dataset_name = self.generate_name(
                name_cardinality=1, max_length=name_length,
                fixed_length=fixed_length)

            if dataverse_name:
                dataverse_name = CBASHelper.unformat_name(dataverse_name)
                database_name = CBASHelper.unformat_name(database_name)
                dataverse_obj = self.get_dataverse_obj(dataverse_name, database_name)
                if not dataverse_obj:
                    dataverse_obj = Dataverse(dataverse_name, database_name)
                    self.create_dataverse(dataverse_obj.name, database_name)
            else:
                dataverse_obj = self.get_dataverse_obj("Default")

            if link:
                link_name = link.full_name
            else:
                link_name = None

            dataset_obj = Standalone_Dataset(
                dataset_name, datasource, primary_key, dataverse_name, database_name,
                link_name, external_collection_name, dataset_properties, 0,
                storage_format)

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
            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj())
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

            data_source = dataset_spec.get("data_source", [])
            link = None
            if len(data_source) > 0:
                data_source = data_source[i % len(data_source)]
                if data_source is None:
                    link = None
                else:
                    links = self.list_all_link_objs(link_type=data_source)
                    link = random.choice(links) if len(links) > 0 else None
                    if link is None:
                        data_source = None

            link_name = link.full_name if link else None
            dataset_obj = Standalone_Dataset(
                name, data_source,
                random.choice(dataset_spec["primary_key"]),
                dataverse.name, dataverse.database_name, link_name, None,
                random.choice(
                    dataset_spec.get("standalone_collection_properties", [{}])),
                0, storage_format
            )

            dataverse_name = dataset_obj.dataverse_name
            if dataverse_name == "Default":
                dataverse_name = None

            results.append(
                self.create_standalone_collection(
                    cluster, name, creation_method, False,
                    dataverse_name, dataverse.database_name, dataset_obj.primary_key, "",
                    False, storage_format)
            )
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

        dataset_spec = self.get_standalone_dataset_spec(cbas_spec, spec_name="kafka_dataset")
        results = list()

        kafka_link_objs = set(self.list_all_link_objs("kafka"))
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
            dataverse = None
            while not dataverse:
                dataverse = random.choice(self.get_all_dataverse_obj())
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

            if (dataset_spec["include_external_collections"].get(
                    datasource, {}) and dataset_spec[
                "exclude_external_collections"].get(datasource, {})
                    and (set(dataset_spec["include_external_collections"][
                                 datasource]) == set(
                        dataset_spec["exclude_external_collections"][
                            datasource]))):
                self.log.error("Both include and exclude "
                               "external collections cannot be "
                               "same")
                return False
            elif dataset_spec["exclude_external_collections"].get(datasource,
                                                                  {}):
                dataset_spec["include_external_collections"][datasource] = (
                        set(dataset_spec["include_external_collections"][
                                datasource]) - set(
                    dataset_spec["exclude_external_collections"][datasource]))

            eligible_links = [link for link in kafka_link_objs if
                              link.db_type == datasource]

            """
            This is to make sure that we don't create 2 collection with same 
            source collection and link
            include_external_collections should be of format -
            { "datasource" : [("region","collection_name")]}
            for mongo and MySQL, region should be empty string
            This is to support Dynamo tables multiple regions.
            """
            retry = 0
            while retry < 100:
                link = random.choice(eligible_links)

                if dataset_spec["include_external_collections"][datasource]:
                    external_collection_name = random.choice(
                        dataset_spec["include_external_collections"][
                            datasource])

                    # this will be True for Dynamo only
                    if external_collection_name[0]:
                        while (external_collection_name[0] !=
                               link.external_database_details[
                                   "connectionFields"]["region"]):
                            external_collection_name = random.choice(
                                dataset_spec["include_external_collections"][
                                    datasource])
                    external_collection_name = external_collection_name[1]
                else:
                    return False
                link_source_db_pair = (link.full_name, external_collection_name)
                if link_source_db_pair not in link_source_db_pairs:
                    link_source_db_pairs.append(link_source_db_pair)
                    break
                else:
                    retry += 1

            dataset_obj = Standalone_Dataset(
                name, datasource,
                random.choice(dataset_spec["primary_key"]),
                dataverse.name, dataverse.database_name, link.full_name, external_collection_name,
                {}, 0, storage_format)

            dataverse_name = dataset_obj.dataverse_name
            if dataverse_name == "Default":
                dataverse_name = None

            if not self.create_standalone_collection_using_links(
                    cluster, name, creation_method, False,
                    dataverse_name, dataverse.database_name, dataset_obj.primary_key,
                    link.full_name, external_collection_name,
                    False, storage_format):
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
            self, cluster, synonym_name, synonym_dataverse_name, synonym_database_name, cbas_entity_name,
            cbas_entity_dataverse_name, username=None, password=None):
        """
        Validates whether an entry for Synonym is present in Metadata.Synonym.
        :param synonym_name : str, Synonym which has to be validated.
        :param synonym_dataverse_name : str, dataverse where the synonym is present
        :param cbas_entity_name : str, name of the cbas object on which the synonym is based.
        :param cbas_entity_dataverse_name : str, name of the dataverse where the cbas object on
        which the synonym is based.
        :param username : str
        :param password : str
        :return boolean
        """
        self.log.debug("Validating Synonym entry in Metadata")
        cmd = "select value sy from Metadata.`Synonym` as sy where \
        sy.SynonymName = \"{0}\" and sy.DataverseName = \"{1}\" and sy.DatabaseName = \"{2}\";".format(
            CBASHelper.unformat_name(synonym_name),
            CBASHelper.metadata_format(synonym_dataverse_name),
            CBASHelper.metadata_format(synonym_database_name))

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password)
        if status == "success":
            for result in results:
                if result['ObjectDataverseName'] == CBASHelper.metadata_format(
                        cbas_entity_dataverse_name) and result[
                    'ObjectName'] == CBASHelper.unformat_name(
                    cbas_entity_name):
                    return True
            return False
        else:
            return False

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
        cmd = "create analytics synonym {0}".format(synonym_full_name)

        if if_not_exists:
            cmd += " If Not Exists"

        cmd += " for {0};".format(cbas_entity_full_name)

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

        cmd = "drop analytics synonym {0}".format(synonym_full_name)

        if if_exists:
            cmd += " if exists;"
        else:
            cmd += ";"

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
                        cbas_entity_name=None, cbas_entity_dataverse=None):
        """
        Return Synonym object if the synonym with the required name already exists.
        If synonym_dataverse or cbas_entity_name or cbas_entity_dataverse is not mentioned
        then it will return first synonym found with the synonym_name.
        :param synonym_name str, name of the synonym whose object has to returned.
        :param synonym_dataverse str, name of the dataverse where the synonym is present
        :param cbas_entity_name str, name of the cbas_entity on which the synonym was created.
        :param cbas_entity_dataverse str, name of the dataverse where the cbas_entity is present
        """
        if not synonym_dataverse:
            cmd = "select value sy from Metadata.`Synonym` as sy where \
            sy.SynonymName = \"{0}\"".format(
                CBASHelper.unformat_name(synonym_name))
            if cbas_entity_name:
                cmd += " and sy.ObjectName = \"{0}\"".format(
                    CBASHelper.unformat_name(cbas_entity_name))
            if cbas_entity_dataverse:
                cmd += " and sy.ObjectDataverseName = \"{0}\"".format(
                    CBASHelper.unformat_name(cbas_entity_dataverse))
            cmd += ";"
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd)
            if status == "success" and results:
                synonym_dataverse = CBASHelper.format_name(
                    results[0]["DataverseName"])
            else:
                return None

        dataverse_obj = self.get_dataverse_obj(synonym_dataverse)
        return dataverse_obj.synonyms.get(synonym_name, None)

    def list_all_synonym_objs(self):
        """
        Returns list of all Synonym objects across all the dataverses.
        """
        synonym_objs = list()
        for database in self.databases.values():
            for dataverse in database.dataverses.values():
                synonym_objs.extend(dataverse.synonyms.values())
        return synonym_objs

    @staticmethod
    def get_synonym_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec["synonym"]

    def create_synonym_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating Synonyms based on CBAS Spec")

        synonym_spec = self.get_synonym_spec(cbas_spec)

        if synonym_spec.get("no_of_synonyms", 0) > 0:

            results = list()
            all_cbas_entities = self.list_all_dataset_objs() + self.list_all_synonym_objs()
            for i in range(1, synonym_spec["no_of_synonyms"] + 1):
                if synonym_spec.get("name_key", "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = synonym_spec["name_key"] + "_{0}".format(str(i))

                dataverse = None
                while not dataverse:
                    dataverse = random.choice(self.get_all_dataverse_obj())
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
                synonym = Synonym(name=name, cbas_entity_name=cbas_entity.name,
                                  cbas_entity_dataverse=cbas_entity.dataverse_name,
                                  cbas_entity_database=cbas_entity.database_name,
                                  dataverse_name=dataverse.name, database_name=dataverse.database_name,
                                  synonym_on_synonym=synonym_on_synonym)

                if not self.create_analytics_synonym(
                        cluster, synonym.full_name, synonym.cbas_entity_full_name,
                        if_not_exists=False, validate_error_msg=False,
                        expected_error=None, username=None, password=None,
                        timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                    results.append(False)
                else:
                    dataverse.synonyms[synonym.name] = synonym
                    results.append(True)
            return all(results)
        return True

    def get_synonyms(self, cluster, retries=10):
        synonyms_created = []
        synonyms_query = "select value regexp_replace(syn.DataverseName,\"/\",\".\") " \
                         "|| \".\" || syn.SynonymName from Metadata.`Synonym` as syn " \
                         "where syn.DataverseName <> \"Metadata\";"
        while not synonyms_created and retries:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, synonyms_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            if status.encode('utf-8') == 'success' and results:
                results = list(
                    map(lambda result: result.encode('utf-8').split("."),
                        results))
                synonyms_created = list(
                    map(lambda result: CBASHelper.format_name(*result),
                        results))
                break
            sleep(12, "Wait for atleast one synonym to be created")
            retries -= 1
        return synonyms_created

    def get_dataset_obj_for_synonym(
            self, cluster, synonym_name, synonym_dataverse=None,
            cbas_entity_name=None, cbas_entity_dataverse=None):

        def inner_func(
                synonym_name, synonym_dataverse,
                cbas_entity_name, cbas_entity_dataverse):
            syn_obj = self.get_synonym_obj(
                cluster, synonym_name, synonym_dataverse,
                cbas_entity_name, cbas_entity_dataverse)
            while syn_obj.synonym_on_synonym:
                syn_obj = inner_func(
                    syn_obj.cbas_entity_name, syn_obj.cbas_entity_dataverse,
                    None, None)
            return syn_obj

        final_obj = inner_func(
            synonym_name, synonym_dataverse,
            cbas_entity_name, cbas_entity_dataverse)
        return self.get_dataset_obj(
            cluster, final_obj.cbas_entity_name, final_obj.cbas_entity_dataverse)


class Index_Util(Synonym_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(Index_Util, self).__init__(
            server_task, run_query_using_sdk)

    def verify_index_created(self, cluster, index_name, dataset_name, indexed_fields):
        """
        Validate in cbas index entry in Metadata.Index.
        :param index_name str, name of the index to be validated.
        :param dataset_name str, name of the dataset on which the index was created.
        :param indexed_fields list, list of indexed fields.
        """
        result = True
        statement = "select * from Metadata.`Index` where DatasetName='{0}' and IsPrimary=False" \
            .format(CBASHelper.unformat_name(dataset_name))
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

    def verify_index_used(self, cluster, statement, index_used=False, index_name=None):
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
                            "INDEX-SEARCH is found in EXPLAIN hence indexed data will be scanned to serve %s" % statement)
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
                            "DATA-SCAN is found in EXPLAIN hence index is not used to serve %s" % statement)
                        return True
                    else:
                        return False
            else:
                return False
        else:
            return False

    def create_cbas_index(self, cluster, index_name, indexed_fields, dataset_name,
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
        index_fields = ""
        for index_field in indexed_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]

        if analytics_index:
            create_idx_statement = "create analytics index {0}".format(index_name)
        else:
            create_idx_statement = "create index {0}".format(index_name)
        if if_not_exists:
            create_idx_statement += " IF NOT EXISTS"
        create_idx_statement += " on {0}({1});".format(dataset_name, index_fields)

        self.log.debug("Executing cmd - \n{0}\n".format(create_idx_statement))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, create_idx_statement, username=username, password=password,
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

    def drop_cbas_index(self, cluster, index_name, dataset_name, analytics_index=False,
                        validate_error_msg=False, expected_error=None,
                        username=None, password=None,
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

        self.log.debug("Executing cmd - \n{0}\n".format(drop_idx_statement))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, drop_idx_statement, username=username, password=password,
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

    def get_index_obj(self, cluster, index_name, dataset_name=None,
                      dataset_dataverse=None, dataset_database=None):
        """
        Return CBAS_index object if the index with the required name already exists.
        If dataset_name or dataset_dataverse is not mentioned then it will return
        first index found with the index_name.
        :param index_name str, name of the index whose object has to returned.
        :param dataset_dataverse str, name of the dataverse where the dataset on which
        the index was created is present
        :param dataset_name str, name of the dataset on which the index was created.
        """
        cmd = "select value idx from Metadata.`Index` as idx where idx.IsPrimary=False and idx.IndexName='{0}'".format(
            CBASHelper.unformat_name(index_name))
        if dataset_name:
            cmd += " and idx.DatasetName = \"{0}\"".format(
                CBASHelper.unformat_name(dataset_name))
        if dataset_dataverse:
            cmd += " and idx.DataverseName = \"{0}\"".format(
                CBASHelper.unformat_name(dataset_dataverse))
        if dataset_database:
            cmd += " and idx.DatabaseName = \"{0}\"".format(
                CBASHelper.unformat_name(dataset_database))
        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd)
        if status == "success" and results:
            dataset_dataverse = CBASHelper.format_name(
                results[0]["DataverseName"])
            dataset_name = CBASHelper.format_name(results[0]["DatasetName"])
        else:
            return None

        dataset_obj = self.get_dataset_obj(cluster, dataset_name, dataset_dataverse)
        return dataset_obj.indexes.get(index_name, None)

    def list_all_index_objs(self):
        """
        Returns list of all Synonym objects across all the dataverses.
        """
        index_objs = list()
        for dataset in self.list_all_dataset_objs():
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

            datasets = self.list_all_dataset_objs()
            external_datasets = self.list_all_dataset_objs("external")
            datasets = [x for x in datasets if x not in external_datasets]
            dataset = None
            while not dataset:
                dataset = random.choice(datasets)
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
                dataverse_name=dataset.dataverse_name, database_name=dataset.database_name,
                indexed_fields=random.choice(
                    index_spec.get("indexed_fields", [])))

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
                    validate_error_msg=False, expected_error=None,
                    username=None, password=None,
                    timeout=cbas_spec.get("api_timeout", 300),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 300)):
                results.append(False)
            else:
                dataset.indexes[index.name] = index
                results.append(True)

        return all(results)

    def get_indexes(self, cluster, retries=10):
        indexes_created = []
        indexes_query = "select value regexp_replace(idx.DataverseName,\"/\",\".\") " \
                        "|| \".\" || idx.DatasetName || \".\" || idx.IndexName " \
                        "from Metadata.`Index` as idx where idx.DataverseName <> \"Metadata\""
        while not indexes_created and retries:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, indexes_query, mode="immediate", timeout=300,
                analytics_timeout=300)
            if status.encode('utf-8') == 'success' and results:
                indexes_created = list(
                    map(lambda idx: idx.encode('utf-8'), results))
                break
            sleep(12, "Wait for atleast one index to be created")
            retries -= 1
        return indexes_created


class UDFUtil(Index_Util):

    def __init__(self, server_task=None, run_query_using_sdk=False):
        """
        :param server_task task object
        """
        super(UDFUtil, self).__init__(server_task, run_query_using_sdk)

    def create_udf(self, cluster, name, dataverse=None, database=None, or_replace=False,
                   parameters=[], body=None, if_not_exists=False,
                   query_context=False, use_statement=False,
                   validate_error_msg=False, expected_error=None,
                   username=None, password=None, timeout=300,
                   analytics_timeout=300):
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

        if query_context:
            if dataverse:
                param["query_context"] = "default:{0}".format(dataverse)
            else:
                param["query_context"] = "default:Default"

        self.log.debug("Executing cmd - \n{0}\n".format(create_udf_statement))

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

    def drop_udf(self, cluster, name, dataverse, database, parameters=[], if_exists=False,
                 use_statement=False, query_context=False,
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

        if query_context:
            if dataverse:
                param["query_context"] = "default:{0}".format(dataverse)
            else:
                param["query_context"] = "default:Default"

        self.log.debug("Executing cmd - \n{0}\n".format(drop_udf_statement))

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
                return False
            else:
                return True

    def get_udf_obj(self, cluster, name, dataverse_name=None, database_name=None, parameters=[]):
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
            cmd += " and func.Databasename = \"{0}\"".format(CBASHelper.unformat_name(database_name))
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

    def list_all_udf_objs(self):
        """
        Returns list of all CBAS_UDF objects across all the dataverses.
        """
        udfs = list()
        for database in self.databases.values():
            for dataverse in database.dataverses.values():
                udfs.extend(dataverse.udfs.values())

        return udfs

    def validate_udf_in_metadata(
            self, cluster, udf_name, udf_dataverse_name, udf_database_name, parameters, body,
            dataset_dependencies=[], udf_dependencies=[], synonym_dependencies=[]):
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

    def get_udfs(self, cluster, retries=10):
        udfs_created = []
        udf_query = "select value(fn) from Metadata.`Function` as fn"
        while not udfs_created and retries:
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
                cluster, udf_query, mode="immediate", timeout=300, analytics_timeout=300)
            if status.encode('utf-8') == 'success' and results:
                for r in results:
                    dv_name = ".".join(r["DataverseName"].split("/"))
                    if int(r["Arity"]) == -1:
                        param = ["..."]
                    else:
                        param = r["Params"]
                    udfs_created.append([dv_name, r["Name"], param])
                break
            sleep(10, "Wait for atleast one User defined function to be "
                      "created")
            retries -= 1
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

    def create_sample_for_analytics_collections(self, cluster, collection_name, sample_size=None, sample_seed=None):

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

    def delete_request(self, cluster, client_context_id, username=None, password=None):
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
        except Exception, e:
            raise Exception(str(e))

    def retrieve_request_status_using_handle(self, cluster, server, handle, shell=None):
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

    def retrieve_result_using_handle(self, cluster, server, handle):
        """
        Retrieves result from the /analytics/results endpoint
        """
        shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}""".format(
                handle, cluster.cbas_cc_node.rest_username,
                cluster.cbas_cc_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        shell.disconnect()

        returnval = None
        if 'results' in response:
            returnval = response['results']

        return returnval

    def convert_execution_time_into_ms(self, time):
        """
        Converts the execution time into ms
        """
        import re
        match = re.match(r"([0-9]+.[0-9]+)([a-zA-Z]+)", time, re.I)
        if match:
            items = match.groups()

            if items[1] == "s":
                return float(items[0]) * 1000
            if items[1] == "ms":
                return float(items[0])
            if items[1] == "m":
                return float(items[0]) * 1000 * 60
            if items[1] == "h":
                return float(items[0]) * 1000 * 60 * 60
        else:
            return None

    def async_query_execute(self, cluster, statement, mode, num_queries):
        """
        Asynchronously run queries
        """
        self.log.debug("Executing %s queries concurrently", num_queries)

        cbas_base_url = "http://{0}:8095/analytics/service" \
            .format(cluster.cbas_cc_node.ip)

        pretty = "true"
        tasks = []
        for count in range(0, num_queries):
            tasks.append(self.task.async_cbas_query_execute(
                cluster, self, cbas_base_url, statement))
        return tasks

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

    def _run_query(self, cluster, query, mode, rest=None, validate_item_count=False,
                   expected_count=0, timeout=300, analytics_timeout=300):
        # Execute query (with sleep induced)
        name = threading.currentThread().getName()
        client_context_id = name
        try:
            status, metrics, errors, results, handle = \
                self.execute_statement_on_cbas_util(
                    cluster, query, mode=mode, timeout=timeout,
                    client_context_id=client_context_id,
                    analytics_timeout=analytics_timeout)
            # Validate if the status of the request is success, and if the count matches num_items
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

        except Exception, e:
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

    def get_partitions_info(self, cluster, timeout=300):
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
        partitions = dict()
        if response:
            nodes_info = dict()
            for node in response["nodes"]:
                nodes_info[node["nodeId"]] = node["nodeName"]

            for partition in response["partitionsTopology"]["partitions"]:
                if nodes_info[partition["master"]] in partitions:
                    partitions[nodes_info[partition["master"]]].append(partition)
                else:
                    partitions[nodes_info[partition["master"]]] = [partition]

        return partitions

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

    def set_specific_log_level_on_cbas(
            self, cluster, logger_name, log_level, timeout=300, username=None,
            password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="PUT", params=None, logger_name=logger_name,
                log_level=log_level, timeout=timeout, username=username,
                password=password)
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

    def get_specific_cbas_log_level(self, cluster, logger_name, timeout=300,
                                    username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="GET", params=None, logger_name=logger_name,
                timeout=timeout, username=username, password=password)
        return status, content, response

    def delete_all_loggers_on_cbas(self, cluster, timeout=300, username=None,
                                   password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="DELETE", params="", logger_name=None,
                timeout=timeout, username=username, password=password)
        return status, content, response

    def delete_specific_cbas_log_level(self, cluster, logger_name, timeout=300,
                                       username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="DELETE", params=None, logger_name=logger_name,
                timeout=timeout, username=username, password=password)
        return status, content, response

    def update_config_on_cbas(self, cluster, config_name=None, config_value=None,
                              username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if config_name and config_value:
            params = '{"' + config_name + '":' + str(config_value) + '}'
        else:
            raise ValueError("Missing config name and/or config value")
        status, content, response = cbas_helper.operation_config_on_cbas(
            method="PUT", params=params, username=username, password=password)
        return status, content, response

    def fetch_config_on_cbas(self, cluster, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = cbas_helper.operation_config_on_cbas(
            method="GET", username=username, password=password)
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
        links = (self.list_all_link_objs("couchbase") +
                 self.list_all_link_objs("kafka"))
        if len(links) > 0:
            self.log.info("Connecting all remote and kafka Links")
            for link in links:
                if not self.connect_link(
                        cluster, link.full_name,
                        timeout=cbas_spec.get("api_timeout", 300),
                        analytics_timeout=cbas_spec.get("cbas_timeout",
                                                        300)):
                    self.log.error("Failed to connect link {0}".format(link.full_name))
                else:
                    self.log.info("Successfully connected link {0}".format(link.full_name))

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
            links = (self.list_all_link_objs("couchbase") +
                     self.list_all_link_objs("kafka"))
            if len(links) > 0:
                self.log.info("Connecting all remote and kafka Links")
                for link in links:
                    if not self.connect_link(
                            cluster, link.full_name,
                            timeout=cbas_spec.get("api_timeout", 300),
                            analytics_timeout=cbas_spec.get("cbas_timeout",
                                                            300)):
                        results.append(False)
                    else:
                        results.append(True)

            if not all(results):
                return False, "Failed at connect_link"

            if not self.wait_for_kafka_links(cluster):
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

        if not self.create_dataset_from_spec(cluster, cbas_spec, bucket_util):
            return False, "Failed at create dataset from spec"
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
            internal_datasets = self.list_all_dataset_objs(
                dataset_source="dataset") + self.list_all_dataset_objs(
                dataset_source="remote")
            self.refresh_dataset_item_count(bucket_util)
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
            self, cluster, cbas_spec, expected_index_drop_fail=True,
            expected_synonym_drop_fail=True, expected_dataset_drop_fail=True,
            expected_link_drop_fail=True, expected_dataverse_drop_fail=True,
            delete_dataverse_object=True, delete_database_object=True, retry_link_disconnect_fail=True):

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
        for index in self.list_all_index_objs():
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
        for synonym in self.list_all_synonym_objs():
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
        for link in self.list_all_link_objs():
            if link.link_type != "s3":
                link_info = self.get_link_info(cluster, link.dataverse_name, link.database_name,
                                               link.name, link.link_type)
                if ((type(link_info) is not None) and len(link_info) > 0 and
                        "linkState" in link_info[0] and link_info[0][
                            "linkState"] == "DISCONNECTED"):
                    pass
                else:
                    retry_func(
                        link, self.disconnect_link,
                        {"cluster": cluster, "link_name": link.full_name,
                         "timeout": cbas_spec.get("api_timeout", 300),
                         "analytics_timeout": cbas_spec.get("cbas_timeout", 300)})
            if any(results):
                if retry_link_disconnect_fail:
                    if link.link_type == "kafka":
                        self.log.info("Waiting for kafka link to get "
                                      "connected before disconnecting")
                        if not self.wait_for_kafka_links(cluster, "CONNECTED"):
                            self.log.error("Kafka links did not get connected")
                    retry_func(
                        link, self.disconnect_link,
                        {"cluster": cluster, "link_name": link.full_name,
                         "timeout": cbas_spec.get("api_timeout", 300),
                         "analytics_timeout": cbas_spec.get("cbas_timeout",
                                                            300)})
                    if any(results):
                        return False
                else:
                    print_failures("Disconnect Links", results)
        if not self.wait_for_kafka_links(cluster, "DISCONNECTED"):
            self.log.error("Kafka links did not get disconnected")

        self.log.info("Dropping all the Datasets")
        for dataset in self.list_all_dataset_objs():
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
        for link in self.list_all_link_objs():
            retry_func(
                link, self.drop_link,
                {"cluster": cluster, "link_name": link.full_name,
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
                     "analytics_scope": random.choice([True, False]),
                     "delete_dataverse_obj": delete_dataverse_object,
                     "timeout": cbas_spec.get("api_timeout", 300),
                     "analytics_timeout": cbas_spec.get("cbas_timeout",
                                                        300)})

        self.log.info("Dropping all the Databases")
        for database in self.databases.values():
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

    def cleanup_cbas(self, cluster, retry=10):
        """
        Drops all Dataverses, Datasets, Indexes, UDFs, Synonyms and Links
        """
        try:
            # Drop all indexes
            for idx in self.get_indexes(cluster, retry):
                idx = idx.split(".")
                if not self.drop_cbas_index(cluster, idx[-1], ".".join(idx[:-1])):
                    self.log.error("Unable to drop Index {0}".format(idx))

            # Disconnect all links
            links = [".".join(l["scope"].split("/") + [l["name"]]) for l in self.get_link_info(
                cluster)]
            for lnk in links:
                if not self.disconnect_link(cluster, lnk):
                    self.log.error("Unable to disconnect Link {0}".format(lnk))

            for syn in self.get_synonyms(cluster, retry):
                if not self.drop_analytics_synonym(cluster, syn):
                    self.log.error("Unable to drop Synonym {0}".format(syn))

            for udf in self.get_udfs(cluster, retry):
                if not self.drop_udf(cluster, udf[1], udf[0], udf[2]):
                    self.log.error("Unable to drop UDF {0}".format(udf[1]))

            for ds in self.get_datasets(cluster, retry):
                if not self.drop_dataset(cluster, ds):
                    self.log.error("Unable to drop Dataset {0}".format(ds))

            for lnk in links:
                if not self.drop_link(cluster, lnk):
                    self.log.error("Unable to drop Link {0}".format(lnk))

            for dv in self.get_dataverses(cluster, retry).remove("Default"):
                if not self.drop_dataverse(cluster, dv):
                    self.log.error("Unable to drop Dataverse {0}".format(dv))
        except Exception as e:
            self.log.info(e.message)

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
        params = urllib.urlencode(params)
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
            url += "dataverseName={0}&".format(urllib.quote_plus(
                CBASHelper.unformat_name(dv_part), safe=""))
        url += "datasetName={0}".format(urllib.quote_plus(
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


class FlushToDiskTask(Task):
    def __init__(self, cluster, cbas_util, datasets=[], run_infinitely=False,
                 interval=5):
        super(FlushToDiskTask, self).__init__("FlushToDiskTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        if datasets:
            self.datasets = datasets
        else:
            self.datasets = self.cbas_util.list_all_dataset_objs()
        self.run_infinitely = run_infinitely
        self.results = []
        self.interval = interval

    def call(self):
        self.start_task()
        try:
            while True:
                for dataset in self.datasets:
                    self.results.append(self.cbas_util.force_flush_cbas_data_to_disk(
                        self.cluster.rest, dataset.dataverse_name,
                        dataset.name))
                    self.sleep(self.interval)
                if not self.run_infinitely:
                    break
        except Exception as e:
            self.set_exception(e)
            return
        self.result = all(self.results)
        self.complete_task()


class DisconnectConnectLinksTask(Task):
    def __init__(self, cluster, cbas_util, links, run_infinitely=False, interval=5):
        super(DisconnectConnectLinksTask, self).__init__(
            "DisconnectConnectLinksTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.links = links
        self.run_infinitely = run_infinitely
        self.results = []
        self.interval = interval

    def call(self):
        self.start_task()
        try:
            while True:
                for link in self.links:
                    dv, name = link.split(".")
                    if self.cbas_util.is_link_active(self.cluster, name, dv):
                        disconnect_result = self.cbas_util.disconnect_link(
                            self.cluster, link)
                        self.sleep(self.interval)
                        connect_result = self.cbas_util.connect_link(
                            self.cluster, link, with_force=True)
                    else:
                        connect_result = self.cbas_util.connect_link(
                            self.cluster, link, with_force=True)
                        self.sleep(self.interval)
                        disconnect_result = self.cbas_util.disconnect_link(
                            self.cluster, link)
                    self.sleep(self.interval)
                    self.results.append(connect_result and disconnect_result)
                if not self.run_infinitely:
                    break
        except Exception as e:
            self.set_exception(e)
            return
        self.result = all(self.results)
        self.complete_task()


class KillProcessesInLoopTask(Task):
    def __init__(self, cluster, cbas_util, cluster_util, cbas_kill_count=0,
                 memcached_kill_count=0, interval=5, timeout=600):
        super(KillProcessesInLoopTask, self).__init__(
            "KillProcessesInLoopTask")
        self.cluster = cluster
        self.cbas_util = cbas_util
        self.cluster_util = cluster_util
        self.cbas_kill_count = cbas_kill_count
        self.memcached_kill_count = memcached_kill_count
        self.interval = interval
        self.timeout = timeout

    def call(self):
        self.start_task()
        try:
            for _ in range(self.cbas_kill_count + self.memcached_kill_count):
                if self.cbas_kill_count > 0:
                    output = self.cbas_util.kill_cbas_process(
                        self.cluster, self.cluster.servers)
                    self.cbas_kill_count -= 1
                    self.log.info(str(output))
                if self.memcached_kill_count > 0:
                    self.cluster_util.kill_memcached(self.cluster)
                    self.memcached_kill_count -= 1
                self.sleep(self.interval)
        except Exception as e:
            self.set_exception(e)
            return
        self.complete_task()


class CBASRebalanceUtil(object):

    def __init__(self, cluster_util, bucket_util, task, vbucket_check=True,
                 cbas_util=None):
        """
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        """
        self.log = logger.get("test")
        self.cluster_util = cluster_util
        self.bucket_util = bucket_util
        self.task = task
        self.vbucket_check = vbucket_check
        self.cbas_util = cbas_util

        self.query_threads = list()

    def wait_for_rebalance_task_to_complete(self, task, cluster,
                                            check_cbas_running=False):
        self.task.jython_task_manager.get_task_result(task)
        if hasattr(cluster, "cbas_cc_node"):
            self.reset_cbas_cc_node(cluster)
        if task.result and hasattr(cluster, "cbas_nodes"):
            if check_cbas_running:
                return self.cbas_util.is_analytics_running(cluster)
            return True
        return task.result

    def rebalance(self, cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                  cbas_nodes_out=0, available_servers=[], exclude_nodes=[]):
        if kv_nodes_out > 0:
            cluster_kv_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster, service_type="kv", get_all_nodes=True,
                servers=cluster.nodes_in_cluster)
        else:
            cluster_kv_nodes = []

        if cbas_nodes_out > 0:
            cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster, service_type="cbas", get_all_nodes=True,
                servers=cluster.nodes_in_cluster)
        else:
            cluster_cbas_nodes = []

        for node in exclude_nodes:
            try:
                cluster_kv_nodes.remove(node)
            except:
                pass
            try:
                cluster_cbas_nodes.remove(node)
            except:
                pass

        servs_in = random.sample(available_servers, kv_nodes_in + cbas_nodes_in)
        servs_out = random.sample(cluster_kv_nodes, kv_nodes_out) + random.sample(
            cluster_cbas_nodes, cbas_nodes_out)

        if kv_nodes_in == kv_nodes_out:
            self.vbucket_check = False

        services = list()
        if kv_nodes_in > 0:
            services += ["kv"] * kv_nodes_in
        if cbas_nodes_in > 0:
            services += ["cbas"] * cbas_nodes_in

        rebalance_task = self.task.async_rebalance(
            cluster, servs_in, servs_out,
            check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=200, services=services)

        available_servers = [servs for servs in available_servers if
                             servs not in servs_in]
        available_servers += servs_out

        cluster.nodes_in_cluster.extend(servs_in)
        nodes_in_cluster = [server for server in cluster.nodes_in_cluster if
                            server not in servs_out]
        cluster.nodes_in_cluster = nodes_in_cluster
        cluster.servers = nodes_in_cluster

        return rebalance_task, available_servers

    def start_parallel_queries(self, cluster, run_kv_queries, run_cbas_queries,
                               parallelism=3):
        queries = ["SELECT COUNT (*) FROM {0};"]
        n1ql_query_task = None
        cbas_query_task = None
        if run_kv_queries:
            n1ql_helper = N1QLHelper(
                use_rest=True, buckets=cluster.buckets, log=self.log,
                n1ql_port=cluster.master.n1ql_port, server=cluster.master)
            n1ql_query_task = RunQueriesTask(
                cluster, queries, self.task.jython_task_manager,
                n1ql_helper, query_type="n1ql", parallelism=parallelism,
                run_infinitely=True, is_prepared=False, record_results=True)
            self.task.jython_task_manager.add_new_task(n1ql_query_task)
        if run_cbas_queries:
            cbas_query_task = RunQueriesTask(
                cluster, queries, self.task.jython_task_manager,
                self.cbas_util, query_type="cbas", parallelism=parallelism,
                run_infinitely=True, is_prepared=False, record_results=True)
            self.task.jython_task_manager.add_new_task(cbas_query_task)
        return n1ql_query_task, cbas_query_task

    def stop_parallel_queries(self, n1ql_query_task, cbas_query_task):
        if cbas_query_task:
            self.log.info("Stopping CBAS queries")
            self.task.jython_task_manager.stop_task(cbas_query_task)
        if n1ql_query_task:
            self.log.info("Stopping N1QL queries")
            self.task.jython_task_manager.stop_task(n1ql_query_task)

    def set_retry_exceptions(self, doc_loading_spec, durability_level=None):
        """
        Exceptions for which mutations need to be retried during
        topology changes
        """
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        retry_exceptions.append(SDKException.ServerOutOfMemoryException)
        if durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    @staticmethod
    def set_ignore_exceptions(doc_loading_spec):
        """
        Exceptions to be ignored.
        Ignoring DocumentNotFoundExceptions because there could be race conditons
        eg: reads or deletes before creates
        """
        ignore_exceptions = list()
        ignore_exceptions.append(SDKException.DocumentNotFoundException)
        doc_loading_spec[MetaCrudParams.IGNORE_EXCEPTIONS] = ignore_exceptions

    def wait_for_data_load_to_complete(self, task, skip_validations):
        self.task.jython_task_manager.get_task_result(task)
        if not skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
        return task.result

    def data_load_collection(
            self, cluster, doc_spec_name, skip_validations, async_load=True,
            skip_read_success_results=True,
            create_percentage_per_collection=100,
            delete_percentage_per_collection=0, update_percentage_per_collection=0,
            replace_percentage_per_collection=0, durability_level=None):
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            doc_spec_name)
        self.set_retry_exceptions(doc_loading_spec, durability_level)
        self.set_ignore_exceptions(doc_loading_spec)
        doc_loading_spec[MetaCrudParams.DURABILITY_LEVEL] = durability_level
        doc_loading_spec[
            MetaCrudParams.SKIP_READ_SUCCESS_RESULTS] = skip_read_success_results
        doc_loading_spec[MetaCrudParams.SKIP_READ_SUCCESS_RESULTS] = \
            skip_read_success_results
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = \
            create_percentage_per_collection
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = \
            delete_percentage_per_collection
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = \
            update_percentage_per_collection
        doc_loading_spec["doc_crud"][
            MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION] = \
            replace_percentage_per_collection
        task = self.bucket_util.run_scenario_from_spec(
            self.task, cluster, cluster.buckets,
            doc_loading_spec, mutation_num=0, async_load=async_load)
        if not async_load:
            if not skip_validations:
                self.bucket_util.validate_doc_loading_results(task)
            return task.result
        return task

    def data_validation_collection(
            self, cluster, skip_validations=True, doc_and_collection_ttl=False,
            async_compaction=False):
        retry_count = 0
        while retry_count < 10:
            try:
                self.bucket_util._wait_for_stats_all_buckets(
                    cluster, cluster.buckets)
            except:
                retry_count = retry_count + 1
                self.log.info(
                    "ep-queue hasn't drained yet. Retry count: {0}".format(
                        retry_count))
            else:
                break
        if retry_count == 10:
            self.log.info("Attempting last retry for ep-queue to drain")
            self.bucket_util._wait_for_stats_all_buckets(
                cluster, cluster.buckets)
        if doc_and_collection_ttl:
            self.bucket_util._expiry_pager(cluster, val=5)
            self.bucket_util._compaction_exp_mem_threshold(cluster)
            self.log.info("wait for doc/collection maxttl to finish")
            time.sleep(400)
            compaction_tasks = self.bucket_util._run_compaction(
                cluster, number_of_times=1, async_run=async_compaction)
            if not async_compaction:
                for bucket, task in compaction_tasks.iteritems():
                    if not task.result:
                        raise Exception("Compaction failed on bucket - %s" % bucket)
                time.sleep(60)
                self.bucket_util._wait_for_stats_all_buckets(
                    cluster, cluster.buckets)
            else:
                return compaction_tasks
        else:
            if not skip_validations:
                self.bucket_util.validate_docs_per_collections_all_buckets(cluster)

    def get_failover_count(self, cluster):
        cluster_status = cluster.rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def wait_for_failover_or_assert(self, cluster, expected_failover_count,
                                    timeout=7200):
        # Timeout is kept large for graceful failover
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count(cluster)
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        if actual_failover_count != expected_failover_count:
            self.log.info(cluster.rest.print_UI_logs())

        if actual_failover_count == expected_failover_count:
            self.log.info("{0} nodes failed over as expected in {1} seconds"
                          .format(actual_failover_count,
                                  time_end - time_start))
        else:
            raise Exception(
                "{0} nodes failed over, expected : {1}".format(
                    actual_failover_count, expected_failover_count))

    def failover(self, cluster, kv_nodes=0, cbas_nodes=0, failover_type="Hard",
                 action=None, timeout=7200, available_servers=[],
                 exclude_nodes=[], kv_failover_nodes=None,
                 cbas_failover_nodes=None, all_at_once=False,
                 wait_for_complete=True, reset_cbas_cc=True):
        """
        This fucntion fails over KV or CBAS node/nodes.
        :param cluster <cluster_obj> cluster in which the nodes are present
        :param kv_nodes <int> number of KV nodes to fail over.
        :param cbas_nodes <int> number of KV nodes to fail over.
        :param failover_type <str> Accepted values are Graceful and Hard
        :param action <str> action to be performed on failed over node.
        Accepted values are RebalanceOut, FullRecovery, DeltaRecovery
        :param available_servers <list> list of nodes that are not the part
        of the cluster.
        :param exclude_nodes <list> list of nodes not to be considered for
        fail over.
        """
        self.log.info("Failover Type - {0}, Action after failover {1}".format(
            failover_type, action))

        if kv_nodes:
            cluster_kv_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster, service_type="kv", get_all_nodes=True,
                servers=cluster.nodes_in_cluster)
        else:
            cluster_kv_nodes = []

        if cbas_nodes:
            cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster, service_type="cbas", get_all_nodes=True,
                servers=cluster.nodes_in_cluster)
        else:
            cluster_cbas_nodes = []

        for node in exclude_nodes:
            try:
                cluster_kv_nodes.remove(node)
            except:
                pass
            try:
                cluster_cbas_nodes.remove(node)
            except:
                pass

        failover_count = 0
        if cbas_failover_nodes is None:
            cbas_failover_nodes = []
        if kv_failover_nodes is None:
            kv_failover_nodes = []
        fail_over_status = True

        def pick_node(cluster, target_nodes, how_many=1):
            picked = list()
            node_status = cluster.rest.node_statuses()
            for i in range(how_many):
                for node in node_status:
                    if node.ip == target_nodes[i].ip:
                        picked.append(node)
            self.log.info("Nodes selected are - {0}".format(
                [p.id for p in picked]))
            return picked

        # Mark Node for failover
        if failover_type == "Graceful":
            chosen = pick_node(cluster, cluster_kv_nodes, kv_nodes)
            if all_at_once:
                fail_over_status = fail_over_status and cluster.rest.fail_over(
                    [x.id for x in chosen], graceful=False, all_at_once=True)
            else:
                for node in chosen:
                    fail_over_status = fail_over_status and cluster.rest.fail_over(
                        node.id, graceful=False, all_at_once=False)
            failover_count += kv_nodes
            kv_failover_nodes.extend(chosen)
        else:
            if kv_nodes and cluster_kv_nodes:
                chosen = pick_node(cluster, cluster_kv_nodes, kv_nodes)
                if all_at_once:
                    fail_over_status = fail_over_status and cluster.rest.fail_over(
                        [x.id for x in chosen], graceful=False,
                        all_at_once=True)
                else:
                    for node in chosen:
                        fail_over_status = fail_over_status and cluster.rest.fail_over(
                            node.id, graceful=False, all_at_once=False)
                failover_count += kv_nodes
                kv_failover_nodes.extend(chosen)
            if cbas_nodes and cluster_cbas_nodes:
                chosen = pick_node(cluster, cluster_cbas_nodes, cbas_nodes)
                if all_at_once:
                    fail_over_status = fail_over_status and cluster.rest.fail_over(
                        [x.id for x in chosen], graceful=False, all_at_once=True)
                else:
                    for node in chosen:
                        fail_over_status = fail_over_status and cluster.rest.fail_over(
                            node.id, graceful=False, all_at_once=False)
                failover_count += cbas_nodes
                cbas_failover_nodes.extend(chosen)
                if reset_cbas_cc:
                    self.reset_cbas_cc_node(cluster)
        if kv_nodes or cbas_nodes:
            time.sleep(30)
            self.wait_for_failover_or_assert(cluster, failover_count, timeout)

        kv_failover_nodes = [node for kv_node in kv_failover_nodes for
                             node in (cluster.nodes_in_cluster +
                                      available_servers) if kv_node.ip == node.ip]
        cbas_failover_nodes = [node for cbas_node in cbas_failover_nodes for
                               node in (cluster.nodes_in_cluster +
                                        available_servers) if cbas_node.ip ==
                               node.ip]

        if action and fail_over_status:
            self.perform_action_on_failed_over_nodes(
                cluster, action, available_servers, kv_failover_nodes,
                cbas_failover_nodes, wait_for_complete)
        return available_servers, kv_failover_nodes, cbas_failover_nodes

    def perform_action_on_failed_over_nodes(
            self, cluster, action="FullRecovery", available_servers=[],
            kv_failover_nodes=[], cbas_failover_nodes=[],
            wait_for_complete=True):
        # Perform the action
        if action == "RebalanceOut":
            rebalance_task = self.task.async_rebalance(
                cluster, [], kv_failover_nodes + cbas_failover_nodes,
                check_vbucket_shuffling=self.vbucket_check,
                retry_get_process_num=200)

            if wait_for_complete:
                if not self.wait_for_rebalance_task_to_complete(
                        rebalance_task, cluster):
                    raise Exception(
                        "Rebalance failed while rebalancing nodes after failover")
                time.sleep(10)
                servs_out = [node for node in cluster.nodes_in_cluster for
                             fail_node in (kv_failover_nodes + cbas_failover_nodes)
                             if node.ip == fail_node.ip]
                cluster.server = cluster.nodes_in_cluster
                available_servers += servs_out
                time.sleep(10)
            else:
                return rebalance_task
        else:
            node_ids = dict()
            for node in cluster.rest.get_nodes(inactive_added=True,
                                               inactive_failed=True):
                node_ids[node.ip] = node.id

            if action == "FullRecovery":
                for node in kv_failover_nodes + cbas_failover_nodes:
                    cluster.rest.set_recovery_type(
                        otpNode=node_ids[node.ip], recoveryType="full")
            elif action == "DeltaRecovery":
                for node in kv_failover_nodes:
                    cluster.rest.set_recovery_type(
                        otpNode=node_ids[node.ip], recoveryType="delta")
            rebalance_task = self.task.async_rebalance(
                cluster, [], [], retry_get_process_num=200)
            if wait_for_complete:
                if not self.wait_for_rebalance_task_to_complete(
                        rebalance_task, cluster):
                    raise Exception(
                        "Rebalance failed while doing recovery after failover")
                time.sleep(10)
            else:
                return rebalance_task
        if cbas_failover_nodes:
            self.reset_cbas_cc_node(cluster)
        # After the action has been performed on failed over node,
        # the failed over nodes list should become empty.
        kv_failover_nodes, cbas_failover_nodes = [], []
        return available_servers, kv_failover_nodes, cbas_failover_nodes

    def reset_cbas_cc_node(self, cluster):
        sleep(10, "Waiting for cluster service map to get updated.")
        self.log.info("Reassigning cluster CBAS CC node")
        cluster.cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            cluster, service_type="cbas", get_all_nodes=True,
            servers=cluster.nodes_in_cluster)
        cbas_cc_node_ip = self.cbas_util.retrieve_cc_ip_from_master(cluster)
        for node in cluster.cbas_nodes:
            if node.ip == cbas_cc_node_ip:
                cluster.cbas_cc_node = node
                break
        self.log.info("Reassigned CBAS CC node is {0}".format(cbas_cc_node_ip))


class BackupUtils(object):

    def __init__(self):
        pass

    def _configure_backup(self, cluster, archive, repo, disable_analytics,
                          exclude, include):

        shell = RemoteMachineShellConnection(cluster.cbas_cc_node)
        shell.log.info('Delete previous backups')
        command = 'rm -rf %s' % archive
        o, r = shell.execute_command(command)
        shell.log_command_output(o, r)

        shell.log.info('Configure backup')
        configure_bkup_cmd = '{0}cbbackupmgr config -a {1} -r {2}'.format(
            shell.return_bin_path_based_on_os(shell.return_os_type()),
            archive, repo)
        configure_bkup_cmd = self._build_backup_cmd_with_optional_parameters(
            disable_analytics, exclude, include, [], configure_bkup_cmd)
        o, r = shell.execute_command(configure_bkup_cmd)
        shell.log_command_output(o, r)
        shell.disconnect()

    def cbbackupmgr_backup_cbas(
            self, cluster, archive='/tmp/backups', repo='example',
            disable_analytics=False, exclude=[], include=[],
            username="Administrator", password="password", skip_configure_bkup=False):

        shell = RemoteMachineShellConnection(cluster.cbas_cc_node)
        if not skip_configure_bkup:
            self._configure_backup(
                cluster, archive, repo, disable_analytics, exclude, include)

        if CbServer.use_https:
            bkup_cmd = '{0}cbbackupmgr backup -a {1} -r {2} --cluster couchbases://{3} --username {4} --password {5} --no-ssl-verify'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, cluster.cbas_cc_node.ip, username, password)
        else:
            bkup_cmd = '{0}cbbackupmgr backup -a {1} -r {2} --cluster couchbase://{3} --username {4} --password {5}'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, cluster.cbas_cc_node.ip, username, password)

        o, r = shell.execute_command(bkup_cmd)
        shell.log_command_output(o, r)
        shell.disconnect()
        return o

    def cbbackupmgr_restore_cbas(
            self, cluster, archive='/tmp/backups', repo='example',
            disable_analytics=False, exclude=[], include=[], mappings=[],
            username="Administrator", password="password"):

        shell = RemoteMachineShellConnection(cluster.cbas_cc_node)
        shell.log.info('Restore backup using cbbackupmgr')

        if CbServer.use_https:
            restore_cmd = '{0}cbbackupmgr restore -a {1} -r {2} --cluster couchbases://{3} --username {4} --password {5} --force-updates --no-ssl-verify'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, cluster.cbas_cc_node.ip, username, password)
        else:
            restore_cmd = '{0}cbbackupmgr restore -a {1} -r {2} --cluster couchbase://{3} --username {4} --password {5} --force-updates'.format(
                shell.return_bin_path_based_on_os(shell.return_os_type()),
                archive, repo, cluster.cbas_cc_node.ip, username, password)

        restore_cmd = self._build_backup_cmd_with_optional_parameters(
            disable_analytics, exclude, include, mappings, restore_cmd)
        o, r = shell.execute_command(restore_cmd)
        shell.log_command_output(o, r)
        shell.disconnect()
        return o

    def _build_backup_cmd_with_optional_parameters(
            self, disable_analytics, exclude, include, mappings, command):
        if disable_analytics:
            command = command + ' --disable-analytics'
        if exclude:
            command = command + ' --exclude-data ' + ",".join(exclude)
        if include:
            command = command + ' --include-data ' + ",".join(include)
        if mappings:
            command = command + ' --map-data ' + ",".join(mappings)
        return command

    def rest_backup_cbas(self, cluster, username=None, password=None, bucket="",
                         include="", exclude="", level="cluster"):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = cbas_helper.backup_cbas(
            username=username, password=password, bucket=bucket,
            include=include, exclude=exclude, level=level)
        return status, CBASHelper.get_json(content), response

    def rest_restore_cbas(self, cluster, username=None, password=None, bucket="",
                          include=[], exclude=[], remap="", level="cluster",
                          backup={}):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = cbas_helper.restore_cbas(
            username=username, password=password, bucket=bucket,
            include=include, exclude=exclude, remap=remap,
            level=level, backup=backup)
        return status, CBASHelper.get_json(content), response
