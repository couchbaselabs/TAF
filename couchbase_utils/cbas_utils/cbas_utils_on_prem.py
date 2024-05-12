"""
Created on 08-Dec-2020

@author: Umang Agrawal

This utility is for performing actions on Analytics on On-prem and
Capella clusters.

Very Important Note - All the CBAS DDLs currently run sequentially, thus the code for
executing DDLs is hard coded to run on a single thread. If this changes in future, we can just
remove this hard coded value.
"""
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

from couchbase_helper.tuq_helper import N1QLHelper
from global_vars import logger
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity_on_prem import Dataverse, CBAS_Scope, Link, Dataset, \
    CBAS_Collection, Synonym, CBAS_Index
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from common_lib import sleep
from Queue import Queue
from sdk_exceptions import SDKException
from collections_helper.collections_spec_constants import MetaCrudParams
from Jython_tasks.task import Task, RunQueriesTask
from StatsLib.StatsOperations import StatsHelper
from connections.Rest_Connection import RestConnection
from cb_constants import CbServer


class BaseUtil(object):

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        self.log = logger.get("test")
        self.task = server_task

    def createConn(self, cluster, bucket, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        cbas_helper.createConn(bucket, username, password)

    def closeConn(self, cluster):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        cbas_helper.closeConn()

    def execute_statement_on_cbas_util(self, cluster, statement, mode=None,
                                       timeout=120, client_context_id=None,
                                       username=None, password=None,
                                       analytics_timeout=120,
                                       time_out_unit="s",
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
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
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
            raise Exception(str(e))

    def execute_parameter_statement_on_cbas_util(self, cluster, statement, mode=None,
                                                 timeout=120,
                                                 client_context_id=None,
                                                 username=None, password=None,
                                                 analytics_timeout=120,
                                                 parameters={}):
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
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
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

        except Exception, e:
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
            actual_error = errors[0]["msg"].replace("`", "")
            expected_error = expected_error.replace("`", "")
            if expected_error not in actual_error:
                self.log.debug("Error message mismatch. Expected: %s, got: %s"
                               % (expected_error, actual_error))
                return False
            self.log.debug("Error message matched. Expected: %s, got: %s"
                           % (expected_error, actual_error))
            if expected_error_code is not None:
                if expected_error_code != errors[0]["code"]:
                    self.log.debug("Error code mismatch. Expected: %s, got: %s"
                                   % (expected_error_code, errors[0]["code"]))
                    return False
                self.log.info("Error code matched. Expected: %s, got: %s"
                              % (expected_error_code, errors[0]["code"]))
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
        """
        if not seed:
            sleep(0.1, "Sleeping before generating name")
            random.seed(round(time.time()*1000))
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
                              "any", "run", "set", "use"}
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
    def update_cbas_spec(cbas_spec, updated_specs):
        """
        Updates new dataverse spec in the overall cbas spec.
        :param cbas_spec dict, original cbas spec dict
        :param updated_specs dict, keys or sub dicts to be updated
        """
        for key, value in updated_specs.iteritems():
            if key in cbas_spec:
                if isinstance(value, dict):
                    BaseUtil.update_cbas_spec(cbas_spec[key], value)
                else:
                    cbas_spec[key] = value

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


class Dataverse_Util(BaseUtil):

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(Dataverse_Util, self).__init__(server_task)
        # Initiate dataverses dict with Dataverse object for Default dataverse
        self.dataverses = dict()
        default_dataverse_obj = Dataverse()
        self.dataverses[default_dataverse_obj.name] = default_dataverse_obj

    def validate_dataverse_in_metadata(
            self, cluster, dataverse_name, username=None,
            password=None, timeout=120, analytics_timeout=120):
        """
        Validates whether a dataverse is present in Metadata.Dataverse
        :param dataverse_name: str, Name of the dataverse which has to be validated.
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :return boolean
        """
        self.log.debug("Validating dataverse entry in Metadata")
        cmd = "select value dv from Metadata.`Dataverse` as dv where\
         dv.DataverseName = \"{0}\";".format(
            CBASHelper.metadata_format(dataverse_name))
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if status == "success":
            if results:
                return True
            else:
                return False
        else:
            return False

    def create_dataverse(self, cluster, dataverse_name, username=None,
                         password=None, validate_error_msg=False,
                         expected_error=None, expected_error_code=None,
                         if_not_exists=False, analytics_scope=False,
                         timeout=120, analytics_timeout=120):
        """
        Creates dataverse.
        :param dataverse_name: str, Name of the dataverse which has to be created.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_not_exists bool, use IfNotExists flag to check dataverse exists before creation,
        if a dataverse exists the query return successfully without creating any dataverse.
        :param analytics_scope bool, If True, will use create analytics scope syntax
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        if analytics_scope:
            cmd = "create analytics scope %s" % dataverse_name
            obj = CBAS_Scope(dataverse_name)
        else:
            cmd = "create dataverse %s" % dataverse_name
            obj = Dataverse(dataverse_name)

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
                return False
            else:
                if not self.get_dataverse_obj(dataverse_name):
                    self.dataverses[obj.name] = obj
                return True

    def drop_dataverse(self, cluster, dataverse_name, username=None, password=None,
                       validate_error_msg=False, expected_error=None,
                       expected_error_code=None, if_exists=False,
                       analytics_scope=False, timeout=120,
                       analytics_timeout=120,
                       delete_dataverse_obj=True,
                       disconnect_local_link=False):
        """
        Drops the dataverse.
        :param dataverse_name: str, Name of the dataverse which has to be dropped.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :param if_exists bool, use IfExists flag to check dataverse exists before deletion,
        if a dataverse does not exists the query return successfully without deleting any dataverse.
        :param analytics_scope bool, If True, will use create analytics scope syntax
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        :param delete_dataverse_obj bool, deletes dropped dataverse's object from dataverse list
        """
        if disconnect_local_link:
            link_cmd = "disconnect link {0}.Local".format(dataverse_name)
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, link_cmd, username=username, password=password, timeout=timeout,
                analytics_timeout=analytics_timeout)
            if status != "success":
                return False

        if analytics_scope:
            cmd = "drop analytics scope %s" % dataverse_name
        else:
            cmd = "drop dataverse %s" % dataverse_name

        if if_exists:
            cmd += " if exists"
        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                if delete_dataverse_obj:
                    obj = self.get_dataverse_obj(dataverse_name)
                    if obj:
                        del self.dataverses[obj.name]
                return True

    def create_analytics_scope(self, cluster, cbas_scope_name, username=None,
                               password=None,
                               validate_error_msg=False, expected_error=None,
                               expected_error_code=None, if_not_exists=False,
                               timeout=120, analytics_timeout=120):
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
        return self.create_dataverse(cluster, cbas_scope_name, username, password,
                                     validate_error_msg, expected_error,
                                     expected_error_code, if_not_exists, True,
                                     timeout, analytics_timeout)

    def drop_analytics_scope(self, cluster, cbas_scope_name, username=None,
                             password=None,
                             validate_error_msg=False, expected_error=None,
                             expected_error_code=None, if_exists=False,
                             timeout=120, analytics_timeout=120):
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
        return self.drop_dataverse(cluster, cbas_scope_name, username, password,
                                   validate_error_msg,
                                   expected_error, expected_error_code,
                                   if_exists, True,
                                   timeout, analytics_timeout)

    def get_dataverse_obj(self, dataverse_name):
        """
        Return Dataverse/CBAS_scope object if dataverse with the required
        name already exists.
        :param dataverse_name str, fully qualified dataverse_name
        """
        return self.dataverses.get(dataverse_name, None)

    @staticmethod
    def get_dataverse_spec(cbas_spec):
        """
        Fetches dataverse specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("dataverse", {})

    def create_dataverse_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating dataverses based on CBAS Spec")

        dataverse_spec = self.get_dataverse_spec(cbas_spec)
        results = list()

        if cbas_spec.get("no_of_dataverses", 1) > 1:
            for i in range(1, cbas_spec.get("no_of_dataverses", 1)):
                if dataverse_spec.get("name_key",
                                      "random").lower() == "random":
                    if dataverse_spec.get("cardinality", 0) == 0:
                        name = self.generate_name(
                            name_cardinality=random.choice([1, 2]))
                    elif dataverse_spec.get("cardinality") == 1:
                        name = self.generate_name(name_cardinality=1)
                    elif dataverse_spec.get("cardinality") == 2:
                        name = self.generate_name(name_cardinality=2)
                else:
                    name_key = dataverse_spec.get("name_key") + "_{0}".format(
                        str(i))
                    if dataverse_spec.get("cardinality", 0) == 0:
                        name = self.generate_name(
                            name_cardinality=random.choice([1, 2]),
                            name_key=name_key)
                    elif dataverse_spec.get("cardinality") == 1:
                        name = name_key
                    elif dataverse_spec.get("cardinality") == 2:
                        name = self.generate_name(
                            name_cardinality=2, name_key=name_key)

                if dataverse_spec.get("creation_method",
                                      "all").lower() == "all":
                    dataverse = random.choice([Dataverse, CBAS_Scope])(name)
                elif dataverse_spec.get(
                        "creation_method").lower() == "dataverse":
                    dataverse = Dataverse(name)
                elif dataverse_spec.get(
                        "creation_method").lower() == "analytics_scope":
                    dataverse = CBAS_Scope(name)

                if isinstance(dataverse, CBAS_Scope):
                    analytics_scope = True
                elif isinstance(dataverse, Dataverse):
                    analytics_scope = False
                results.append(
                    self.create_dataverse(
                        cluster, dataverse.name,
                        if_not_exists=True,
                        analytics_scope=analytics_scope,
                        timeout=cbas_spec.get("api_timeout", 120),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 120)
                    )
                )

            return all(results)
        return True

    def get_dataverses(self, cluster, retries=10):
        dataverses_created = []
        dataverse_query = "select value regexp_replace(dv.DataverseName,\"/\"," \
            "\".\") from Metadata.`Dataverse` as dv where dv.DataverseName <> \"Metadata\";"
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


class Link_Util(Dataverse_Util):

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(Link_Util, self).__init__(server_task)

    def validate_link_in_metadata(self, cluster, link_name, dataverse_name,
                                  link_type="Local", username=None, password=None,
                                  is_active=False, timeout=120, analytics_timeout=120):
        """
        Validates whether a link is present in Metadata.Link
        :param link_name: str, Name of the link to be validated.
        :param dataverse_name: str, dataverse name where the link is present
        :param link_type: str, type of link, valid values are Local, s3 or couchbase
        :param username : str
        :param password : str
        :param is_active : bool, verifies whether the link is active or not, valid only for
        Local and couchbase links
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        self.log.debug("Validating link entry in Metadata")
        cmd = "select value lnk from Metadata.`Link` as lnk where\
         lnk.DataverseName = \"{0}\" and lnk.Name = \"{1}\"".format(
            CBASHelper.metadata_format(dataverse_name),
            CBASHelper.unformat_name(link_name))

        if link_type != "Local":
            cmd += " and lnk.`Type` = \"{0}\"".format((link_type).upper())
        cmd += ";"

        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if status == "success":
            if results:
                if is_active:
                    if ((link_type).lower() != "s3" or (link_type).lower() != "azureblob") and results[0]["IsActive"]:
                        return True
                    else:
                        return False
                else:
                    return True
            else:
                return False
        else:
            return False

    def is_link_active(self, cluster, link_name, dataverse_name,
                       link_type="Local", username=None, password=None,
                       timeout=120, analytics_timeout=120):
        """
        Validates whether a link is active or not. Valid only for
        Local and couchbase links
        :param link_name: str, Name of the link whose status has to be checked.
        :param dataverse_name: str, dataverse name where the link is present
        :param link_type: str, type of link, valid values are Local, s3 or couchbase
        :param username : str
        :param password : str
        :param timeout int, REST API timeout
        :param analytics_timeout int, analytics query timeout
        """
        return self.validate_link_in_metadata(
            cluster, link_name, dataverse_name, link_type,
            username, password, True, timeout, analytics_timeout)

    def create_link_on_cbas(self, cluster, link_name=None, username=None,
                            password=None, validate_error_msg=False,
                            expected_error=None, expected_error_code=None,
                            timeout=120, analytics_timeout=120):
        """
        This method will fail in all the scenarios. It is only
        there to test negative scenario.
        """
        cmd = "create link {0};"

        if not link_name:
            link_name = "Local"

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

    def create_link(self, cluster, link_properties, username=None, password=None,
                    validate_error_msg=False, expected_error=None,
                    expected_error_code=None, create_if_not_exists=False,
                    timeout=120, analytics_timeout=120, create_dataverse=True):
        """
        Creates Link.
        :param link_properties: dict, parameters required for creating link.
        Common for both AWS and couchbase link.
        <Required> name : name of the link to be created.
        <Required> scope : name of the dataverse under which the link has to be created.
        <Required> type : s3/azure/couchbase

        For links to external couchbase cluster.
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

        For links to AWS S3
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
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if "region" in link_properties:
            self.log.info(
                "Creating link - {0}.{1} in region {2}".format(
                    link_properties["dataverse"], link_properties["name"],link_properties["region"]))
        else:
            self.log.info(
                "Creating link - {0}.{1}".format(
                    link_properties["dataverse"], link_properties["name"]))
        exists = False
        if create_if_not_exists:
            exists = self.validate_link_in_metadata(
                cluster, link_properties["name"], link_properties["dataverse"],
                link_properties["type"], username, password)

        if not exists:
            # If dataverse does not exits
            if create_dataverse and not self.create_dataverse(
                cluster, CBASHelper.format_name(link_properties["dataverse"]),
                if_not_exists=True, timeout=timeout,
                analytics_timeout=analytics_timeout):
                return False

            link_prop = copy.deepcopy(link_properties)
            params = dict()
            uri = ""
            if "dataverse" in link_prop:
                uri += "/{0}".format(urllib.quote_plus(CBASHelper.metadata_format(
                    link_prop["dataverse"]), safe=""))
                del link_prop["dataverse"]
            if "name" in link_prop:
                uri += "/{0}".format(urllib.quote_plus(
                    CBASHelper.unformat_name(link_prop["name"]), safe=""))
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
            return status
        else:
            return exists

    def drop_link(self, cluster, link_name, username=None, password=None,
                  validate_error_msg=False, expected_error=None,
                  expected_error_code=None, if_exists=False,
                  timeout=120, analytics_timeout=120):
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
        cmd = "drop link %s" % link_name

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

    def get_link_info(self, cluster, dataverse=None, link_name=None, link_type=None,
                      username=None, password=None, timeout=120, restapi=True,
                      validate_error_msg=False, expected_error=None,
                      expected_error_code=None):
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
        :param restapi : True, if you want to create link using rest API. False, if you
        want to create link using DDL.
        """
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        if restapi:
            params = dict()
            uri = ""
            if dataverse:
                uri += "/{0}".format(urllib.quote_plus(
                    CBASHelper.metadata_format(dataverse), safe=""))
            if link_name:
                uri += "/{0}".format(urllib.quote_plus(
                    CBASHelper.unformat_name(link_name), safe=""))
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

    def update_external_link_properties(
            self, cluster, link_properties, username=None, password=None,
            timeout=120, validate_error_msg=False, expected_error=None,
            expected_error_code=None):
        """
        Update all the link properties with the new values.
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
        if "dataverse" in link_prop:
            uri += "/{0}".format(urllib.quote(CBASHelper.metadata_format(
                link_prop["dataverse"]), safe=""))
            del link_prop["dataverse"]
        if "name" in link_prop:
            uri += "/{0}".format(urllib.quote(
                CBASHelper.unformat_name(link_prop["name"]), safe=""))
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
                     expected_error=None, expected_error_code=None, timeout=120,
                     analytics_timeout=120):
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
        cmd_connect_bucket = "connect link %s" % link_name

        if not with_force:
            cmd_connect_bucket += " with {'force':false}"

        retry_attempt = 5
        connect_bucket_failed = True
        while connect_bucket_failed and retry_attempt > 0:
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd_connect_bucket, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout)

            if errors:
                # Below errors are to be fixed in Alice, until they are fixed retry is only option
                actual_error = errors[0]["msg"]
                if "Failover response The vbucket belongs to another server" in actual_error or "Bucket configuration doesn't contain a vbucket map" in actual_error:
                    retry_attempt -= 1
                    self.log.debug("Retrying connecting of bucket")
                    sleep(10)
                else:
                    self.log.debug("Not a vbucket error, so don't retry")
                    connect_bucket_failed = False
            else:
                connect_bucket_failed = False
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
                        expected_error_code=None, timeout=120, analytics_timeout=120):
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
        cmd_disconnect_link = 'disconnect link %s;' % link_name

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

    @staticmethod
    def get_link_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("link", {})

    def create_link_from_spec(self, cluster, cbas_spec):
        self.log.info("Creating Links based on CBAS Spec")

        link_spec = self.get_link_spec(cbas_spec)
        results = list()

        if cbas_spec.get("no_of_links", 0) > 0:

            if cbas_spec.get("percent_of_remote_links", 0) == cbas_spec.get(
                    "percent_of_external_links", 0) or cbas_spec.get(
                "percent_of_remote_links", 0) + cbas_spec.get(
                "percent_of_external_links", 0) > 100:
                no_of_remote_links = cbas_spec.get("no_of_links") // 2
                no_of_external_links = cbas_spec.get("no_of_links") // 2
            else:
                no_of_remote_links = (cbas_spec.get("no_of_links") * cbas_spec.get(
                    "percent_of_remote_links")) // 100
                no_of_external_links = (cbas_spec.get("no_of_links") * cbas_spec.get(
                    "percent_of_external_links")) // 100

            for i in range(1, cbas_spec.get("no_of_links") + 1):
                if link_spec.get("name_key", "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = link_spec.get("name_key") + "_{0}".format(str(i))

                dataverse = None
                while not dataverse:
                    dataverse = random.choice(self.dataverses.values())
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

                link = Link(
                    name=name, dataverse_name=dataverse.name,
                    properties=random.choice(link_spec.get("properties", [{}])))

                if link.link_type == "s3":
                    if no_of_external_links > 0:
                        no_of_external_links -= 1
                    else:
                        while link.link_type == "s3":
                            link = Link(
                                name=name, dataverse_name=dataverse.name,
                                properties=random.choice(link_spec.get("properties")))
                        no_of_remote_links -= 1
                elif link.link_type == "couchbase":
                    if no_of_remote_links > 0:
                        no_of_remote_links -= 1
                    else:
                        while link.link_type == "couchbase":
                            link = Link(
                                name=name, dataverse_name=dataverse.name,
                                properties=random.choice(link_spec.get("properties")))
                        no_of_external_links -= 1
                if not self.create_link(
                        cluster, link.properties, create_if_not_exists=True,
                        timeout=cbas_spec.get("api_timeout", 120),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 120)):
                    results.append(False)
                else:
                    dataverse.links[link.name] = link
                    results.append(True)

            return all(results)
        return True

    def get_link_obj(self, cluster, link_name, link_type=None, dataverse_name=None):
        """
        Return Link object if the link with the required name already exists.
        If dataverse_name or link_type is not mentioned then it will return first
        link found with the link_name.
        :param link_name str, name of the link whose object has to returned.
        :param link_type str, s3 or couchbase
        :param dataverse_name str, name of the dataverse where the link is present
        """
        if not dataverse_name:
            cmd = "select value lnk from Metadata.`Link` as lnk where lnk.Name = \"{0}\"".format(
                CBASHelper.unformat_name(link_name))

            if link_type:
                cmd += " and lnk.`Type` = \"{0}\"".format((link_type).upper())
            cmd += ";"

            self.log.debug("Executing cmd - \n{0}\n".format(cmd))
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd)
            if status == "success":
                if results:
                    dataverse_name = CBASHelper.format_name(
                        results[0]["DataverseName"])
                    link_type = results[0]["Type"]
                else:
                    return None
            else:
                return None

        dataverse_obj = self.get_dataverse_obj(dataverse_name)
        link = dataverse_obj.links.get(link_name, None)
        if link and link_type:
            if link_type.lower() == link.link_type:
                return link
            else:
                return None
        return link

    def list_all_link_objs(self, link_type=None):
        """
        Returns list of all link objects across all the dataverses.
        :param link_type str, s3 or couchbase, if None returns all link types
        """
        link_objs = list()
        for dataverse in self.dataverses.values():
            if not link_type:
                link_objs.extend(dataverse.links.values())
            else:
                for link in dataverse.links.values():
                    if link.link_type == link_type.lower():
                        link_objs.append(link)
        return link_objs

    def create_link_obj(
            self, cluster, link_type, dataverse=None, link_cardinality=1,
            hostname=None, username=None, password=None, encryption=None,
            certificate=None, clientCertificate=None, clientKey=None,
            accessKeyId=None, secretAccessKey=None, regions=[],
            serviceEndpoint=None, no_of_objs=1, name_length=30,
            fixed_length=False, link_perm=False):
        """
        Generates Link objects.
        """
        count = 0
        while count < no_of_objs:
            if not dataverse:
                if link_cardinality > 1:
                    dataverse_name = CBASHelper.format_name(self.generate_name(
                        name_cardinality=link_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_dataverse(
                            cluster, dataverse_name, if_not_exists=True):
                        raise Exception("Error while creating dataverse")
                    dataverse = self.get_dataverse_obj(dataverse_name)
                else:
                    dataverse = self.get_dataverse_obj("Default")

            if link_type.lower() == "s3":
                link = Link(
                    name=self.generate_name(
                        name_cardinality=1, max_length=name_length,
                        fixed_length=fixed_length),
                    dataverse_name=dataverse.name,
                    properties={"type": "s3", "accessKeyId": accessKeyId,
                                "secretAccessKey": secretAccessKey,
                                "region": random.choice(regions),
                                "serviceEndpoint": serviceEndpoint})
            elif link_type.lower() == "azureblob":
                if not link_perm:
                    link = Link(
                        name=self.generate_name(
                            name_cardinality=1, max_length=name_length,
                            fixed_length=fixed_length),
                        dataverse_name=dataverse.name,
                        properties={"type": "azureblob",
                                    "endpoint": serviceEndpoint,
                                    "accountName": accessKeyId,
                                    "accountKey": secretAccessKey,
                                    })
                else:
                    link = Link(
                        name=self.generate_name(
                            name_cardinality=1, max_length=name_length,
                            fixed_length=fixed_length),
                        dataverse_name=dataverse.name,
                        properties={"type": "azureblob",
                                    "endpoint": serviceEndpoint,
                                    })
            elif link_type.lower() == "couchbase":
                link = Link(
                    name=self.generate_name(
                        name_cardinality=1, max_length=name_length,
                        fixed_length=fixed_length),
                    dataverse_name=dataverse.name,
                    properties={"type": "couchbase", "hostname": hostname,
                                "username": username, "password": password,
                                "encryption": encryption,
                                "certificate": certificate,
                                "clientCertificate": clientCertificate,
                                "clientKey": clientKey})
            dataverse.links[link.name] = link
            count += 1

    def validate_get_link_info_response(
            self, cluster, link_properties, username=None, password=None,
            timeout=120, restapi=True):
        response = self.get_link_info(
            cluster, link_properties["dataverse"], link_properties["name"],
            link_properties["type"], username=username, password=password,
            timeout=timeout, restapi=restapi)
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


class Dataset_Util(Link_Util):

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(Dataset_Util, self).__init__(server_task)

    def validate_dataset_in_metadata(
            self, cluster, dataset_name, dataverse_name=None, username=None,
            password=None, timeout=120, analytics_timeout=120, **kwargs):
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
        self.log.debug("Validating dataset entry in Metadata")
        cmd = 'SELECT value MD FROM Metadata.`Dataset` as MD WHERE DatasetName="{0}"'.format(
            CBASHelper.unformat_name(dataset_name))
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
            self, cluster, dataset_name, kv_entity, dataverse_name=None,
            if_not_exists=False, compress_dataset=False, with_clause=None,
            link_name=None, where_clause=None, validate_error_msg=False,
            username=None, password=None, expected_error=None, timeout=120,
            analytics_timeout=120, analytics_collection=False, storage_format=None):
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
                cluster, dataverse_name=dataverse_name, if_not_exists=True,
                timeout=timeout, analytics_timeout=analytics_timeout):
            return False

        if analytics_collection:
            cmd = "create analytics collection"
        else:
            cmd = "create dataset"

        if if_not_exists:
            cmd += " if not exists"

        if dataverse_name:
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
            self, cluster, dataset_name, kv_entity, dataverse_name=None,
            if_not_exists=False, compress_dataset=False, with_clause=None,
            link_name=None, where_clause=None, validate_error_msg=False,
            username=None, password=None, expected_error=None,
            timeout=120, analytics_timeout=120, storage_format=None):
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
            cluster, dataset_name, kv_entity, dataverse_name, if_not_exists,
            compress_dataset, with_clause, link_name, where_clause,
            validate_error_msg, username, password, expected_error,
            timeout, analytics_timeout, True, storage_format)

    def create_dataset_on_external_resource_azure(
                self, cluster, dataset_name, azure_container_name, link_name,
                if_not_exists=False, dataverse_name=None, object_construction_def=None,
                path_on_aws_bucket=None, file_format="json", redact_warning=None,
                header=None, null_string=None, include=None, exclude=None,
                validate_error_msg=False, username=None, password=None,
                expected_error=None, expected_error_code=None,
                timeout=120, analytics_timeout=120, create_dv=True):
            """
            Creates a dataset for an external resource like AWS S3 bucket/Azure blob.
            Note - No shadow dataset is created for this type of external datasets.
            :param dataset_name (str) : Name for the dataset to be created.
            :param aws_bucket_name (str): AWS S3 bucket to which this dataset is to be linked. S3 bucket should be in
            the same region as the link, that is used to create this dataset.
            :param dataverse_name str, Dataverse where dataset is to be created.
            :param if_not_exists bool, if this flag is set then, if a dataset with same name is present
            in the same dataverse, then the create statement will pass without creating a new dataset.
            :param link_name (str): external link to AWS S3
            :param object_construction_def (str): It defines how the data read will be parsed.
            Required only for csv and tsv formats.
            :param path_on_aws_bucket (str): Relative path in S3 bucket, from where the files will be read.
            :param file_format (str): Type of files to read. Valid values - json, csv and tsv
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
            :return True/False

            """
            if create_dv and dataverse_name and not self.create_dataverse(
                    cluster, dataverse_name=dataverse_name, username=username,
                    password=password, if_not_exists=True):
                return False

            cmd = "CREATE EXTERNAL DATASET"
            if if_not_exists:
                cmd += " if not exists"

            if dataverse_name:
                cmd += " {0}.{1}".format(dataverse_name, dataset_name)
            else:
                cmd += " {0}".format(dataset_name)

            if object_construction_def:
                cmd += "({0})".format(object_construction_def)

            cmd += " ON `{0}` AT {1}".format(azure_container_name, link_name)

            if path_on_aws_bucket is not None:
                cmd += " USING \"{0}\"".format(path_on_aws_bucket)
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

            cmd += " WITH {0};".format(json.dumps(with_parameters))
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cluster, cmd, username=username, password=password, timeout=timeout,
                analytics_timeout=analytics_timeout)

            if validate_error_msg:
                return self.validate_error_in_response(status, errors,
                                                       expected_error,
                                                       expected_error_code)
            else:
                if status != "success":
                    return False
                else:
                    return True

    def create_dataset_on_external_resource(
            self, cluster, dataset_name, aws_bucket_name, link_name,
            if_not_exists=False, dataverse_name=None, object_construction_def=None,
            path_on_aws_bucket=None, file_format="json", redact_warning=None,
            header=None, null_string=None, include=None, exclude=None,
            validate_error_msg=False, username=None, password=None,
            expected_error=None, expected_error_code=None,
            timeout=120, analytics_timeout=120, create_dv=True,
            parse_json_string=0, convert_decimal_to_double=0,
            timezone=""):
        """
        Creates a dataset for an external resource like AWS S3 bucket.
        Note - No shadow dataset is created for this type of external datasets.
        :param dataset_name (str) : Name for the dataset to be created.
        :param aws_bucket_name (str): AWS S3 bucket to which this dataset is to be linked. S3 bucket should be in
        the same region as the link, that is used to create this dataset.
        :param dataverse_name str, Dataverse where dataset is to be created.
        :param if_not_exists bool, if this flag is set then, if a dataset with same name is present
        in the same dataverse, then the create statement will pass without creating a new dataset.
        :param link_name (str): external link to AWS S3
        :param object_construction_def (str): It defines how the data read will be parsed.
        Required only for csv and tsv formats.
        :param path_on_aws_bucket (str): Relative path in S3 bucket, from where the files will be read.
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
            cluster, dataverse_name=dataverse_name, username=username,
            password=password, if_not_exists=True):
            return False

        cmd = "CREATE EXTERNAL DATASET"

        if if_not_exists:
            cmd += " if not exists"

        if dataverse_name:
            cmd += " {0}.{1}".format(dataverse_name, dataset_name)
        else:
            cmd += " {0}".format(dataset_name)

        if object_construction_def:
            cmd += "({0})".format(object_construction_def)

        cmd += " ON `{0}` AT {1}".format(aws_bucket_name, link_name)

        if path_on_aws_bucket is not None:
            cmd += " USING \"{0}\"".format(path_on_aws_bucket)

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

        cmd += " WITH {0};".format(json.dumps(with_parameters))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cluster, cmd, username=username, password=password, timeout=timeout,
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_dataset(
            self, cluster, dataset_name, username=None, password=None,
            validate_error_msg=False, expected_error=None, expected_error_code=None,
            if_exists=False, analytics_collection=False, timeout=120,
            analytics_timeout=120):
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
            if_exists=False, timeout=120, analytics_timeout=120):
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
            password=None, timeout=120, analytics_timeout=120):
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
        """
        cmd = "Alter collection {0} enable analytics ".format(kv_entity_name)
        if compress_dataset:
            cmd = cmd + "with {'storage-block-compression': {'scheme': 'snappy'}}"
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
            timeout=120, analytics_timeout=120):
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
        query = "select raw BlockLevelStorageCompression.DatasetCompressionScheme from Metadata.`Dataset` where DatasetName='{0}';".format(
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

    def get_dataset_obj(self, cluster, dataset_name, dataverse_name=None):
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

        dataverse_obj = self.get_dataverse_obj(dataverse_name)
        return dataverse_obj.datasets.get(dataset_name, None)

    def list_all_dataset_objs(self, dataset_source=None):
        """
        Returns list of all Dataset/CBAS_Collection objects across all the dataverses.
        """
        dataset_objs = list()
        for dataverse in self.dataverses.values():
            if not dataset_source:
                dataset_objs.extend(dataverse.datasets.values())
            else:
                for dataset in dataverse.datasets.values():
                    if dataset.dataset_source == dataset_source:
                        dataset_objs.append(dataset)
        return dataset_objs

    @staticmethod
    def get_dataset_spec(cbas_spec):
        """
        Fetches link specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("dataset", {})

    @staticmethod
    def update_dataset_spec(cbas_spec, **kwargs):
        dataset_spec = Dataset_Util.get_dataset_spec(cbas_spec)
        for spec_name, spec_value in kwargs.items():
            dataset_spec[spec_name] = spec_value


    def create_dataset_from_spec(self, cluster, cbas_spec, bucket_util):
        self.log.info("Creating Datasets based on CBAS Spec")

        dataset_spec = self.get_dataset_spec(cbas_spec)

        if cbas_spec.get("no_of_datasets_per_dataverse", 0) > 0:
            results = list()

            remote_link_objs = set()
            external_link_objs = set()
            if cbas_spec.get("no_of_links", 0) > 0:
                # Return false if both include and exclude link types are same.
                if dataset_spec.get(
                    "include_link_types", "all") == dataset_spec.get(
                        "exclude_link_types", None):
                    return False

                if dataset_spec.get(
                    "include_link_types", "all").lower() == "all" and not dataset_spec.get(
                        "exclude_link_types", None):
                    remote_link_objs |= set(self.list_all_link_objs("couchbase"))
                    external_link_objs |= set(self.list_all_link_objs("s3"))
                else:
                    if dataset_spec.get(
                            "include_link_types").lower() == "s3" or dataset_spec.get(
                        "exclude_link_types").lower() == "couchbase":
                        external_link_objs |= set(
                            self.list_all_link_objs("s3"))
                    elif dataset_spec.get(
                            "include_link_types").lower() == "couchbase" or dataset_spec.get(
                        "exclude_link_types").lower() == "s3":
                        remote_link_objs |= set(
                            self.list_all_link_objs("couchbase"))

                if dataset_spec.get("include_links", []):
                    for link_name in dataset_spec["include_links"]:
                        link_obj = self.get_link_obj(
                            cluster, CBASHelper.format_name(link_name))
                        if link_obj.link_type == "s3":
                            external_link_objs |= set([link_obj])
                        else:
                            remote_link_objs |= set([link_obj])

                if dataset_spec.get("exclude_links", []):
                    for link_name in dataset_spec["exclude_links"]:
                        link_obj = self.get_link_obj(
                            cluster, CBASHelper.format_name(link_name))
                        if link_obj.link_type == "s3":
                            external_link_objs -= set([link_obj])
                        else:
                            remote_link_objs -= set([link_obj])
            remote_link_objs = list(remote_link_objs)
            external_link_objs = list(external_link_objs)

            if cbas_spec.get("no_of_dataverses", 1) == 0:
                cbas_spec["no_of_dataverses"] = 1
            total_no_of_datasets = cbas_spec.get(
                "no_of_dataverses", 1) * cbas_spec.get(
                "no_of_datasets_per_dataverse", 0)
            if cbas_spec.get("percent_of_local_datasets", 0) == cbas_spec.get(
                    "percent_of_remote_datasets", 0) == cbas_spec.get(
                "percent_of_external_datasets",
                0) or cbas_spec.get(
                "percent_of_local_datasets", 0) + cbas_spec.get(
                "percent_of_remote_datasets", 0) + cbas_spec.get(
                "percent_of_external_datasets", 0) > 100:
                no_of_external_datasets = total_no_of_datasets // 3
                no_of_remote_datasets = total_no_of_datasets // 3
                no_of_local_datasets = total_no_of_datasets // 3
            else:
                no_of_external_datasets = (total_no_of_datasets * cbas_spec.get(
                    "percent_of_external_datasets", 0)) // 100
                no_of_remote_datasets = (total_no_of_datasets * cbas_spec.get(
                    "percent_of_remote_datasets")) // 100
                no_of_local_datasets = total_no_of_datasets - no_of_external_datasets - no_of_remote_datasets

            for dataverse in self.dataverses.values():
                if dataset_spec.get(
                    "include_dataverses", []) and CBASHelper.unformat_name(
                        dataverse.name) not in dataset_spec["include_dataverses"]:
                    dataverse = None
                if dataset_spec.get(
                    "exclude_dataverses", []) and CBASHelper.unformat_name(
                        dataverse.name) in dataset_spec["exclude_dataverses"]:
                    dataverse = None

                if dataverse:
                    for i in range(1, cbas_spec.get(
                        "no_of_datasets_per_dataverse", 1) + 1):
                        dataset_obj = None
                        if dataset_spec.get(
                            "name_key", "random").lower() == "random":
                            name = self.generate_name(name_cardinality=1)
                        else:
                            name = dataset_spec["name_key"] + "_{0}".format(
                                str(i))

                        if dataset_spec.get(
                            "datasource", "all").lower() == "all":
                            datasource = random.choice(["internal", "external"])
                        else:
                            datasource = dataset_spec["datasource"].lower()

                        if datasource == "external":
                            if no_of_external_datasets > 0:
                                no_of_external_datasets -= 1
                            else:
                                datasource = "internal"

                        if not dataset_spec.get("creation_methods", []):
                            dataset_spec["creation_methods"] = [
                                "cbas_collection", "cbas_dataset",
                                "enable_cbas_from_kv"]

                        if datasource == "internal" and (
                                no_of_local_datasets > 0 or no_of_remote_datasets > 0):
                            if len(remote_link_objs) > 0:
                                remote_dataset = random.choice(
                                    ["True", "False"])
                            else:
                                remote_dataset = False

                            if remote_dataset:
                                if no_of_remote_datasets > 0:
                                    no_of_remote_datasets -= 1
                                else:
                                    remote_dataset = False
                            else:
                                if no_of_local_datasets > 0:
                                    no_of_local_datasets -= 1
                                else:
                                    remote_dataset = True

                            if dataset_spec.get("bucket_cardinality", 0) == 0:
                                bucket_cardinality = random.choice([1, 3])
                            else:
                                bucket_cardinality = dataset_spec["bucket_cardinality"]

                            enabled_from_KV = False
                            if remote_dataset:
                                while True:
                                    creation_method = random.choice(
                                        dataset_spec["creation_methods"])
                                    if creation_method != "enable_cbas_from_kv":
                                        break
                                bucket, scope, collection = self.get_kv_entity(
                                    cluster, bucket_util, bucket_cardinality,
                                    dataset_spec.get("include_buckets", []),
                                    dataset_spec.get("exclude_buckets", []),
                                    dataset_spec.get("include_scopes", []),
                                    dataset_spec.get("exclude_scopes", []),
                                    dataset_spec.get("include_collections", []),
                                    dataset_spec.get("exclude_collections", []))
                                link_name = random.choice(
                                    remote_link_objs).full_name
                                if collection:
                                    num_of_items = collection.num_items
                                else:
                                    num_of_items = bucket_util.get_collection_obj(
                                        bucket_util.get_scope_obj(
                                            bucket, "_default"), "_default").num_items
                            else:
                                creation_method = random.choice(
                                    dataset_spec["creation_methods"])
                                bucket, scope, collection = self.get_kv_entity(
                                    cluster,  bucket_util, bucket_cardinality,
                                    dataset_spec.get("include_buckets", []),
                                    dataset_spec.get("exclude_buckets", []),
                                    dataset_spec.get("include_scopes", []),
                                    dataset_spec.get("exclude_scopes", []),
                                    dataset_spec.get("include_collections", []),
                                    dataset_spec.get("exclude_collections", []))
                                link_name = None
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

                            if creation_method == "cbas_collection":
                                dataset_obj = CBAS_Collection(
                                    name=name, dataverse_name=dataverse.name,
                                    link_name=link_name, dataset_source="internal",
                                    dataset_properties={}, bucket=bucket,
                                    scope=scope, collection=collection,
                                    enabled_from_KV=enabled_from_KV,
                                    num_of_items=num_of_items)
                            else:
                                dataset_obj = Dataset(
                                    name=name, dataverse_name=dataverse.name,
                                    link_name=link_name, dataset_source="internal",
                                    dataset_properties={}, bucket=bucket,
                                    scope=scope, collection=collection,
                                    enabled_from_KV=enabled_from_KV,
                                    num_of_items=num_of_items)
                        elif datasource == "external" and no_of_external_datasets > 0:
                            if len(external_link_objs) == 0:
                                return False
                            link = random.choice(external_link_objs)
                            while True:
                                dataset_properties = random.choice(
                                    dataset_spec.get(
                                        "external_dataset_properties", [{}]))
                                if link.properties[
                                    "region"] == dataset_properties.get(
                                    "region", None):
                                    break
                            dataset_obj = Dataset(
                                name=name, dataverse_name=dataverse.name,
                                link_name=link.full_name, dataset_source="external",
                                dataset_properties=dataset_properties,
                                bucket=None, scope=None, collection=None,
                                enabled_from_KV=False)

                        if dataset_obj:
                            dataverse_name = dataset_obj.dataverse_name
                            if dataverse_name == "Default":
                                dataverse_name = None
                            if dataset_obj.dataset_source == "internal":
                                if dataset_obj.enabled_from_KV:
                                    results.append(
                                        self.enable_analytics_from_KV(
                                            cluster, dataset_obj.full_kv_entity_name,
                                            False, False, None, None, None,
                                            timeout=cbas_spec.get(
                                                "api_timeout", 120),
                                            analytics_timeout=cbas_spec.get(
                                                "cbas_timeout", 120)))
                                else:
                                    if isinstance(dataset_obj,
                                                  CBAS_Collection):
                                        analytics_collection = True
                                    elif isinstance(dataset_obj, Dataset):
                                        analytics_collection = False

                                    if dataset_spec["storage_format"] == "mixed":
                                        storage_format = random.choice(["row", "column"])
                                    else:
                                        storage_format = dataset_spec["storage_format"]
                                    results.append(
                                        self.create_dataset(
                                            cluster, dataset_obj.name,
                                            dataset_obj.full_kv_entity_name,
                                            dataverse_name, False, False, None,
                                            dataset_obj.link_name, None, False,
                                            None, None, None,
                                            timeout=cbas_spec.get("api_timeout", 120),
                                            analytics_timeout=cbas_spec.get("cbas_timeout", 120),
                                            analytics_collection=analytics_collection,
                                            storage_format=storage_format))
                            else:
                                results.append(
                                    self.create_dataset_on_external_resource(
                                        cluster, dataset_obj.name,
                                        dataset_obj.dataset_properties[
                                            "aws_bucket_name"],
                                        dataset_obj.link_name, False,
                                        dataverse_name,
                                        dataset_obj.dataset_properties[
                                            "object_construction_def"],
                                        dataset_obj.dataset_properties[
                                            "path_on_aws_bucket"],
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
                                        False, None, None, None, None,
                                        timeout=cbas_spec.get(
                                            "api_timeout", 120),
                                        analytics_timeout=cbas_spec.get(
                                            "cbas_timeout", 120)))
                            if results[-1]:
                                dataverse.datasets[
                                    dataset_obj.name] = dataset_obj

            return all(results)
        return True

    def create_datasets_on_all_collections(
            self, cluster, bucket_util, cbas_name_cardinality=1,
            kv_name_cardinality=1, remote_datasets=False, creation_methods=None,
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
        jobs = Queue()
        results = list()

        if not creation_methods:
            creation_methods = ["cbas_collection", "cbas_dataset",
                                "enable_cbas_from_kv"]

        if remote_datasets:
            remote_link_objs = self.list_all_link_objs("couchbase")
            creation_methods.remove("enable_cbas_from_kv")

        def dataset_creation(bucket, scope, collection, storage_format):
            creation_method = random.choice(creation_methods)

            if remote_datasets:
                link_name = random.choice(remote_link_objs).full_name
            else:
                link_name = None

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

            if creation_method == "cbas_collection":
                dataset_obj = CBAS_Collection(
                    name=name, dataverse_name=dataverse.name,
                    link_name=link_name,
                    dataset_source="internal", dataset_properties={},
                    bucket=bucket, scope=scope, collection=collection,
                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)
            else:
                dataset_obj = Dataset(
                    name=name, dataverse_name=dataverse.name,
                    link_name=link_name,
                    dataset_source="internal", dataset_properties={},
                    bucket=bucket, scope=scope, collection=collection,
                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)

            dataverse_name = dataverse.name

            if dataverse_name == "Default":
                dataverse_name = None

            if dataset_obj.enabled_from_KV:

                if kv_name_cardinality > 1:
                    results.append(
                        self.enable_analytics_from_KV(
                            cluster, dataset_obj.full_kv_entity_name, False,
                            False, None, None, None, 120, 120))
                else:
                    results.append(
                        self.enable_analytics_from_KV(
                            cluster, dataset_obj.get_fully_qualified_kv_entity_name(1),
                            False, False, None, None, None, 120, 120))
            else:

                if isinstance(dataset_obj, CBAS_Collection):
                    analytics_collection = True
                elif isinstance(dataset_obj, Dataset):
                    analytics_collection = False

                if storage_format == "mixed":
                    storage_format = random.choice(["row", "column"])

                if kv_name_cardinality > 1 and cbas_name_cardinality > 1:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.full_kv_entity_name,
                            dataverse_name, False, False, None,
                            dataset_obj.link_name, None, False, None, None,
                            None, 120, 120, analytics_collection, storage_format))
                elif kv_name_cardinality > 1 and cbas_name_cardinality == 1:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.full_kv_entity_name,
                            None, False, False, None, dataset_obj.link_name,
                            None, False, None, None, None, 120, 120,
                            analytics_collection, storage_format))
                elif kv_name_cardinality == 1 and cbas_name_cardinality > 1:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.get_fully_qualified_kv_entity_name(1),
                            dataverse_name, False, False, None,
                            dataset_obj.link_name, None, False, None, None,
                            None, 120, 120, analytics_collection, storage_format))
                else:
                    results.append(
                        self.create_dataset(
                            cluster, dataset_obj.name,
                            dataset_obj.get_fully_qualified_kv_entity_name(1),
                            None, False, False, None, dataset_obj.link_name,
                            None, False, None, None,
                            None, 120, 120, analytics_collection, storage_format))

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
            for_all_kv_entities=False, remote_dataset=False, link=None,
            same_dv_for_link_and_dataset=False, name_length=30,
            fixed_length=False, exclude_bucket=[], exclude_scope=[],
            exclude_collection=[], no_of_objs=1, storage_format=None):
        """
        Generates dataset objects.
        """

        def set_dataset_storage_format(ds_obj, storage_format):
            if storage_format == "mixed":
                ds_obj.storage_format = random.choice(["row", "column"])
            else:
                ds_obj.storage_format = storage_format

        def create_object(
                bucket, scope, collection, enabled_from_KV=enabled_from_KV,
                remote_dataset=remote_dataset, link=link,
                same_dv_for_link_and_dataset=same_dv_for_link_and_dataset,
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

                if remote_dataset:
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
                link_name=None

            dataset_obj = Dataset(
                name=dataset_name, dataverse_name=dataverse_name,
                link_name=link_name,
                dataset_source="internal", dataset_properties={},
                bucket=bucket, scope=scope, collection=collection,
                enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)

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
                    set_dataset_storage_format(dataset_objs[-1], storage_format)
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
                            set_dataset_storage_format(dataset_objs[-1], storage_format)
        else:
            for _ in range(no_of_objs):
                bucket = random.choice(cluster.buckets)
                while bucket.name in exclude_bucket:
                    bucket = random.choice(cluster.buckets)

                if bucket_cardinality == 1:
                    dataset_objs.append(create_object(bucket, None, None))
                    set_dataset_storage_format(dataset_objs[-1], storage_format)
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
                    set_dataset_storage_format(dataset_objs[-1], storage_format)
        return dataset_objs

    def create_external_dataset_azure_obj(
            self, cluster, azure_containers, same_dv_for_link_and_dataset=False,
            dataset_cardinality=1, object_construction_def=None,
            path_on_aws_bucket=None, file_format="json", redact_warning=None,
            header=None, null_string=None, include=None, exclude=None,
            name_length=30, fixed_length=False, no_of_objs=1):
        """
        Creates a Dataset object for external datasets.
        """
        for _ in range(no_of_objs):
            azure_container = random.choice(azure_containers.keys())

            all_links = self.list_all_link_objs("azureblob")
            link = random.choice(all_links)
            while all_links and (link.properties["type"] != azure_containers[azure_container]):
                all_links.remove(link)
                link = random.choice(self.list_all_link_objs("azureblob"))
            if same_dv_for_link_and_dataset:
                dataverse = self.get_dataverse_obj(link.dataverse_name)
            else:
                if dataset_cardinality > 1:
                    dataverse_name = CBASHelper.format_name(self.generate_name(
                        name_cardinality=dataset_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_dataverse(cluster, dataverse_name,
                                                 if_not_exists=True):
                        raise Exception("Error while creating dataverse")
                    dataverse = self.get_dataverse_obj(dataverse_name)
                else:
                    dataverse = self.get_dataverse_obj("Default")

            dataset = Dataset(
                name=self.generate_name(name_cardinality=1, max_length=name_length,
                                        fixed_length=fixed_length),
                dataverse_name=dataverse.name, link_name=link.full_name,
                dataset_source="external", dataset_properties={})
            dataset.dataset_properties["azure_container_name"] = azure_container
            dataset.dataset_properties["object_construction_def"] = object_construction_def
            dataset.dataset_properties["path_on_aws_bucket"] = path_on_aws_bucket
            dataset.dataset_properties["file_format"] = file_format
            dataset.dataset_properties["redact_warning"] = redact_warning
            dataset.dataset_properties["header"] = header
            dataset.dataset_properties["null_string"] = null_string
            dataset.dataset_properties["include"] = include
            dataset.dataset_properties["exclude"] = exclude

            dataverse.datasets[dataset.name] = dataset

    def create_external_dataset_obj(
            self, cluster, aws_bucket_names, same_dv_for_link_and_dataset=False,
            dataset_cardinality=1, object_construction_def=None,
            path_on_aws_bucket=None, file_format="json", redact_warning=None,
            header=None, null_string=None, include=None, exclude=None,
            name_length=30, fixed_length=False, no_of_objs=1,
            parse_json_string=0, convert_decimal_to_double=0, timezone=""):
        """
        Creates a Dataset object for external datasets.
        :param aws_bucket_names: dict, format {"aws_bucket_name":"region"}
        """
        for _ in range(no_of_objs):
            aws_bucket = random.choice(aws_bucket_names.keys())

            all_links = self.list_all_link_objs("s3")
            link = random.choice(all_links)
            while all_links and (
                link.properties["region"] != aws_bucket_names[aws_bucket]):
                all_links.remove(link)
                link = random.choice(self.list_all_link_objs("s3"))

            if same_dv_for_link_and_dataset:
                dataverse = self.get_dataverse_obj(link.dataverse_name)
            else:
                if dataset_cardinality > 1:
                    dataverse_name = CBASHelper.format_name(self.generate_name(
                        name_cardinality=dataset_cardinality - 1,
                        max_length=name_length - 1,
                        fixed_length=fixed_length))
                    if not self.create_dataverse(cluster, dataverse_name,
                                                 if_not_exists=True):
                        raise Exception("Error while creating dataverse")
                    dataverse = self.get_dataverse_obj(dataverse_name)
                else:
                    dataverse = self.get_dataverse_obj("Default")

            dataset = Dataset(
                name=self.generate_name(name_cardinality=1, max_length=name_length,
                                        fixed_length=fixed_length),
                dataverse_name=dataverse.name, link_name=link.full_name,
                dataset_source="external", dataset_properties={})
            dataset.dataset_properties["aws_bucket_name"] = aws_bucket
            dataset.dataset_properties["object_construction_def"] = object_construction_def
            dataset.dataset_properties["path_on_aws_bucket"] = path_on_aws_bucket
            dataset.dataset_properties["file_format"] = file_format
            dataset.dataset_properties["redact_warning"] = redact_warning
            dataset.dataset_properties["header"] = header
            dataset.dataset_properties["null_string"] = null_string
            dataset.dataset_properties["include"] = include
            dataset.dataset_properties["exclude"] = exclude
            dataset.dataset_properties["parse_json_string"] = parse_json_string
            dataset.dataset_properties["convert_decimal_to_double"] = convert_decimal_to_double
            dataset.dataset_properties["timezone"] = timezone

            dataverse.datasets[dataset.name] = dataset

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
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
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
                    results = cbas_helper.get_json(json_data=results)
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


class Synonym_Util(Dataset_Util):

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(Synonym_Util, self).__init__(server_task)

    def validate_synonym_in_metadata(
            self, cluster, synonym_name, synonym_dataverse_name, cbas_entity_name,
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
        sy.SynonymName = \"{0}\" and sy.DataverseName = \"{1}\";".format(
            CBASHelper.unformat_name(synonym_name),
            CBASHelper.metadata_format(synonym_dataverse_name))

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
            username=None, password=None, timeout=120, analytics_timeout=120):
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
            password=None, timeout=120, analytics_timeout=120):
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
        for dataverse in self.dataverses.values():
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

        if cbas_spec.get("no_of_synonyms", 0) > 0:

            results = list()
            all_cbas_entities = self.list_all_dataset_objs() + self.list_all_synonym_objs()
            for i in range(1, cbas_spec["no_of_synonyms"] + 1):
                if synonym_spec.get("name_key", "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = synonym_spec["name_key"] + "_{0}".format(str(i))

                dataverse = None
                while not dataverse:
                    dataverse = random.choice(self.dataverses.values())
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
                                  dataverse_name=dataverse.name,
                                  synonym_on_synonym=synonym_on_synonym)

                if not self.create_analytics_synonym(
                        cluster, synonym.full_name, synonym.cbas_entity_full_name,
                        if_not_exists=False, validate_error_msg=False,
                        expected_error=None, username=None, password=None,
                        timeout=cbas_spec.get("api_timeout", 120),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 120)):
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

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(Index_Util, self).__init__(server_task)

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
                          timeout=120, analytics_timeout=120, if_not_exists=False):
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
                        timeout=120, analytics_timeout=120, if_exists=False):
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
                      dataset_dataverse=None):
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
        return cbas_spec["index"]

    def create_index_from_spec(self, cluster, cbas_spec):
        self.log.info(
            "Creating Secondary indexes on datasets based on CBAS spec")

        index_spec = self.get_index_spec(cbas_spec)

        if cbas_spec.get("no_of_indexes", 0) > 0:
            results = list()
            for i in range(1, cbas_spec["no_of_indexes"] + 1):
                if index_spec.get("name_key", "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = index_spec["name_key"] + "_{0}".format(str(i))

                dataverse = None
                while not dataverse:
                    dataverse = random.choice(self.dataverses.values())
                    if index_spec.get("include_dataverses",
                                      []) and CBASHelper.unformat_name(
                        dataverse.name) not in index_spec[
                        "include_dataverses"]:
                        dataverse = None
                    if index_spec.get("exclude_dataverses",
                                      []) and CBASHelper.unformat_name(
                        dataverse.name) in index_spec["exclude_dataverses"]:
                        dataverse = None
                    if len(dataverse.datasets) == 0:
                        dataverse = None

                dataset = None
                while not dataset:
                    dataset = random.choice(dataverse.datasets.values())
                    if index_spec.get("include_datasets",
                                      []) and CBASHelper.unformat_name(
                        dataset.name) not in index_spec["include_datasets"]:
                        dataset = None
                    if index_spec.get("exclude_datasets",
                                      []) and CBASHelper.unformat_name(
                        dataset.name) in index_spec["exclude_datasets"]:
                        dataset = None

                index = CBAS_Index(name=name, dataset_name=dataset.name,
                                   dataverse_name=dataverse.name,
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
                    validate_error_msg=False, expected_error=None, username=None,
                    password=None, timeout=cbas_spec.get("api_timeout", 120),
                    analytics_timeout=cbas_spec.get("cbas_timeout", 120)):
                    results.append(False)
                else:
                    dataset.indexes[index.name] = index
                    results.append(True)

            return all(results)
        return True

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

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(UDFUtil, self).__init__(server_task)

    def create_udf(self, cluster, name, dataverse=None, or_replace=False,
                   parameters=[], body=None, if_not_exists=False,
                   query_context=False, use_statement=False,
                   validate_error_msg=False, expected_error=None,
                   username=None, password=None, timeout=120,
                   analytics_timeout=120):
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
        if dataverse and not query_context and not use_statement:
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

    def drop_udf(self, cluster, name, dataverse, parameters=[], if_exists=False,
                 use_statement=False, query_context=False,
                 validate_error_msg=False, expected_error=None,
                 username=None, password=None, timeout=120,
                 analytics_timeout=120):
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

    def get_udf_obj(self, cluster, name, dataverse_name=None, parameters=[]):
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
        return [udf for dataverse in self.dataverses.values() for udf in
                dataverse.udfs.values()]

    def validate_udf_in_metadata(
            self, cluster, udf_name, udf_dataverse_name, parameters, body,
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
              "Name=\"{0}\" and DataverseName=\"{1}\"".format(
                  CBASHelper.unformat_name(udf_name),
                  CBASHelper.metadata_format(udf_dataverse_name))

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
            password=None, timeout=120, analytics_timeout=120):
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
            sleep(10, "Wait for atleast one index to be created")
            retries -= 1
        return udfs_created


class CBOUtil(UDFUtil):

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(CBOUtil, self).__init__(server_task)

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

    def __init__(self, server_task=None):
        """
        :param server_task task object
        """
        super(CbasUtil, self).__init__(server_task)

        self.travel_sample_inventory_views = {
            "airline_view" : 149,
            "airport_view" : 1709,
            "hotel_endorsement_view" : 4004,
            "hotel_review_view" : 4104,
            "hotel_view" : 917,
            "landmark_view" : 4495,
            "route_schedule_view" : 505300,
            "route_view" : 24024
        }
        self.travel_sample_inventory_collections = {
            "airline" : 187,
            "airport" : 1968,
            "hotel" : 917,
            "landmark" : 4495,
            "route" : 24024
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

    def set_log_level_on_cbas(self, cluster, log_level_dict, timeout=120,
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
            self, cluster, logger_name, log_level, timeout=120, username=None,
            password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="PUT", params=None, logger_name=logger_name,
                log_level=log_level, timeout=timeout, username=username,
                password=password)
        return status, content, response

    def get_log_level_on_cbas(self, cluster, timeout=120, username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="GET", params="", logger_name=None, timeout=timeout,
                username=username, password=password)
        if content:
            content = json.loads(content)
        return status, content, response

    def get_specific_cbas_log_level(self, cluster, logger_name, timeout=120,
                                    username=None, password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="GET", params=None, logger_name=logger_name,
                timeout=timeout, username=username, password=password)
        return status, content, response

    def delete_all_loggers_on_cbas(self, cluster, timeout=120, username=None,
                                   password=None):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = \
            cbas_helper.operation_log_level_on_cbas(
                method="DELETE", params="", logger_name=None,
                timeout=timeout, username=username, password=password)
        return status, content, response

    def delete_specific_cbas_log_level(self, cluster, logger_name, timeout=120,
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

    def get_analytics_diagnostics(self, cluster, timeout=120):
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

    def create_cbas_infra_from_spec(self, cluster, cbas_spec, bucket_util,
                                    wait_for_ingestion=True):
        """
        Method creates CBAS infra based on the spec data.
        :param cbas_spec dict, spec to create CBAS infra.
        :param local_bucket_util bucket_util_obj, bucket util object of local cluster.
        :param remote_bucket_util bucket_util_obj, bucket util object of remote cluster.
        :param wait_for_ingestion bool
        """
        if not self.create_dataverse_from_spec(cluster, cbas_spec):
            return False, "Failed at create dataverse"
        if not self.create_link_from_spec(cluster, cbas_spec):
            return False, "Failed at create link from spec"
        if not self.create_dataset_from_spec(cluster, cbas_spec, bucket_util):
            return False, "Failed at create dataset from spec"

        jobs = Queue()
        results = list()

        # Connect link only when remote links are present, Local link is connected by default and
        # external links are not required to be connected.
        remote_links = self.list_all_link_objs("couchbase")
        if len(remote_links) > 0:
            self.log.info("Connecting all remote Links")
            for link in remote_links:
                if not self.connect_link(
                        cluster, link.full_name,
                        timeout=cbas_spec.get("api_timeout", 120),
                        analytics_timeout=cbas_spec.get("cbas_timeout", 120)):
                    results.append(False)
                else:
                    results.append(True)

        if not all(results):
            return False, "Failed at connect_link"

        results = []

        if wait_for_ingestion:
            # Wait for data ingestion only for datasets based on either local KV source or remote KV source,
            internal_datasets = self.list_all_dataset_objs(
                dataset_source="internal")
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
            delete_dataverse_object=True):

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
                 "timeout": cbas_spec.get("api_timeout", 120),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 120)}
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
                {"cluster":cluster, "synonym_full_name": synonym.full_name,
                 "if_exists": True, "timeout": cbas_spec.get("api_timeout", 120),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 120)})
        if any(results):
            if expected_synonym_drop_fail:
                print_failures("Synonyms", results)
                results = []
            else:
                return False
        self.log.info("Dropping all the Datasets")
        for dataset in self.list_all_dataset_objs():
            dataset_name = dataset.full_name

            if dataset.dataverse_name == "Default":
                dataset_name = dataset.name

            if isinstance(dataset, CBAS_Collection):
                retry_func(
                    dataset, self.drop_dataset,
                    {"cluster": cluster, "dataset_name": dataset_name,
                     "if_exists": True, "analytics_collection": True,
                     "timeout": cbas_spec.get("api_timeout", 120),
                     "analytics_timeout": cbas_spec.get("cbas_timeout", 120)})
            else:
                retry_func(
                    dataset, self.drop_dataset,
                    {"cluster": cluster, "dataset_name": dataset_name,
                     "if_exists": True, "timeout": cbas_spec.get("api_timeout", 120),
                     "analytics_timeout": cbas_spec.get("cbas_timeout", 120)})
        if any(results):
            if expected_dataset_drop_fail:
                print_failures("Datasets", results)
                results = []
            else:
                return False

        self.log.info("Disconnecting and Dropping all the Links")
        for link in self.list_all_link_objs():
            retry_func(
                link, self.disconnect_link,
                {"cluster":cluster, "link_name": link.full_name,
                 "timeout": cbas_spec.get("api_timeout", 120),
                 "analytics_timeout": cbas_spec.get("cbas_timeout", 120)})
            try:
                results.pop()
            except:
                retry_func(
                    link, self.drop_link,
                    {"cluster":cluster, "link_name": link.full_name,
                     "if_exists": True, "timeout": cbas_spec.get("api_timeout", 120),
                     "analytics_timeout": cbas_spec.get("cbas_timeout", 120)})
            else:
                results.append(link.full_name)

        if any(results):
            if expected_link_drop_fail:
                print_failures("Links", results)
                results = []
            else:
                return False

        self.log.info("Dropping all the Dataverses")
        for dataverse in self.dataverses.values():
            if dataverse.name != "Default":
                if isinstance(dataverse, CBAS_Scope):
                    retry_func(
                        dataverse, self.drop_dataverse,
                        {"cluster":cluster, "dataverse_name": dataverse.name,
                         "if_exists": True, "analytics_scope": True,
                         "delete_dataverse_obj": delete_dataverse_object,
                         "timeout": cbas_spec.get("api_timeout", 120),
                         "analytics_timeout": cbas_spec.get("cbas_timeout",
                                                            120)})
                else:
                    retry_func(
                        dataverse, self.drop_dataverse,
                        {"cluster":cluster, "dataverse_name": dataverse.name,
                         "if_exists": True,
                         "delete_dataverse_obj": delete_dataverse_object,
                         "timeout": cbas_spec.get("api_timeout", 120),
                         "analytics_timeout": cbas_spec.get("cbas_timeout",
                                                            120)})

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
            self.log, node, method=method, params=param, timeout=120,
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
            for node in cluster.nodes_in_cluster:
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
            servers = cluster.nodes_in_cluster
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
                        self.cluster, self.cluster.nodes_in_cluster)
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
                                            check_cbas_running=False,
                                            reset_cluster_servers=True):
        self.task.jython_task_manager.get_task_result(task)
        if reset_cluster_servers:
            cluster.servers = cluster.nodes_in_cluster
        if hasattr(cluster, "cbas_cc_node"):
            self.reset_cbas_cc_node(cluster)
        if task.result and hasattr(cluster, "cbas_nodes"):
            if check_cbas_running:
                return self.cbas_util.is_analytics_running(cluster)
            return True
        return task.result

    def rebalance(self, cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                  cbas_nodes_out=0, available_servers=[], exclude_nodes=[],
                  in_node_services=""):
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

        cluster.servers.extend(servs_in)

        if kv_nodes_in == kv_nodes_out:
            self.vbucket_check = False

        services = list()
        if kv_nodes_in > 0:
            services += ["kv"] * kv_nodes_in
        if cbas_nodes_in > 0:
            services += ["cbas"] * cbas_nodes_in

        if in_node_services:
            services = [in_node_services] * (kv_nodes_in + cbas_nodes_in)

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

        # cluster.servers = nodes_in_cluster

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

    def wait_for_data_load_to_complete(self, cluster, task, skip_validations):
        self.task.jython_task_manager.get_task_result(task)
        if not skip_validations:
            self.bucket_util.validate_doc_loading_results(cluster, task)
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
                self.bucket_util.validate_doc_loading_results(cluster, task)
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
        return status, cbas_helper.get_json(content), response

    def rest_restore_cbas(self, cluster, username=None, password=None, bucket="",
                          include=[], exclude=[], remap="", level="cluster",
                          backup={}):
        cbas_helper = CBASHelper(cluster.cbas_cc_node)
        status, content, response = cbas_helper.restore_cbas(
            username=username, password=password, bucket=bucket,
            include=include, exclude=exclude, remap=remap,
            level=level, backup=backup)
        return status, cbas_helper.get_json(content), response