'''
Created on 08-Dec-2020

@author: Umang
'''
import json
import urllib
import time
from threading import Thread
import threading
import string
import random
import importlib
import copy

from global_vars import logger
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import Dataverse,CBAS_Scope,Link,Dataset,CBAS_Collection,Synonym,CBAS_Index
from remote.remote_util import RemoteMachineShellConnection
from common_lib import sleep
from Queue import Queue


class BaseUtil(object):
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        self.log = logger.get("test")
        self.cbas_node = cbas_node
        self.master = master
        self.task = server_task
        self.cbas_helper = CBASHelper(master, cbas_node)

    def createConn(self, bucket, username=None, password=None):
        self.cbas_helper.createConn(bucket, username, password)

    def closeConn(self):
        self.cbas_helper.closeConn()
    
    def execute_statement_on_cbas_util(self, statement, mode=None,
                                       timeout=120, client_context_id=None,
                                       username=None, password=None,
                                       analytics_timeout=120, time_out_unit="s",
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
        pretty = "true"
        try:
            self.log.debug("Running query on cbas: %s" % statement)
            response = self.cbas_helper.execute_statement_on_cbas(
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

    def execute_parameter_statement_on_cbas_util(self, statement, mode=None,
                                                 timeout=120, client_context_id=None,
                                                 username=None, password=None,
                                                 analytics_timeout=120, parameters=[]):
        """
        Executes a statement on CBAS using the REST API using REST Client
        :param statement str, statement to execute on analytics workbench
        :param mode str, cli, rest, jdk
        :param timeout int, timeout is second for REST API request
        :param client_context_id str
        :param username str
        :param password str
        :param analytics_timeout int, timeout for analytics workbench
        :param parameters list,
        """
        pretty = "true"
        try:
            self.log.debug("Running query on cbas: %s" % statement)
            response = self.cbas_helper.execute_parameter_statement_on_cbas(
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
            actual_error = errors[0]["msg"]
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
                      name_key=None):
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
        :param formatted bool, if True, then enclose names in ``
        """
        if 0 < name_cardinality < 3:
            if name_key:
                return ".".join(name_key for i in range(name_cardinality))
            else:
                max_name_len = max_length/name_cardinality
                if fixed_length:
                    return '.'.join(''.join(random.choice(
                        string.ascii_letters + string.digits) 
                        for _ in range(max_name_len)) 
                        for _ in range(name_cardinality))
                else:
                    return '.'.join(''.join(random.choice(
                        string.ascii_letters + string.digits) 
                        for _ in range(random.randint(1,max_name_len))) 
                        for _ in range(name_cardinality))
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
    def update_cbas_spec(cbas_spec, updated_specs, sub_spec_name=None):
        """
        Updates new dataverse spec in the overall cbas spec.
        :param cbas_spec dict, cbas spec dict
        :param dataverse_spec dict, dataverse spec dict
        """
        for key,value in updated_specs.iteritems():
            if key in cbas_spec:
                cbas_spec[key] = value
            
            if sub_spec_name:
                if key in cbas_spec[sub_spec_name]:
                    cbas_spec[sub_spec_name][key] = value
    
    def run_jobs_in_parallel(self, consumer_func, jobs, results, thread_count, 
                             async_run=False,consume_from_queue_func=None):
        if not consume_from_queue_func:
            def consume_from_queue(jobs, results):
                while not jobs.empty():
                    job = jobs.get()
                    try:
                        results.append(consumer_func(job))
                    except Exception as e:
                        self.log.error(str(e))
                        results.append(False)
                    finally:
                        jobs.task_done()
        else:
            consume_from_queue = consume_from_queue_func
            consume_from_queue(jobs, results)
        #start worker threads
        if jobs.qsize() < thread_count:
            thread_count = jobs.qsize() 
        for tc in range(1,thread_count+1):
            worker = Thread(
                target=consume_from_queue, 
                name="cbas_worker_{0}".format(str(tc)), 
                args=(jobs,results,))
            worker.start()
        if not async_run:
            jobs.join()
    
    @staticmethod
    def get_kv_entity(bucket_util,bucket_cardinality=1,
                      include_buckets=[],exclude_buckets=[],
                      include_scopes=[],exclude_scopes=[],
                      include_collections=[],exclude_collections=[]):
        bucket = None
        while not bucket:
            bucket = random.choice(bucket_util.buckets)
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
                    collection = random.choice(bucket_util.get_active_collections(bucket, scope.name))
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
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        super(Dataverse_Util,self).__init__(master, cbas_node, server_task)
        # Initiate dataverses dict with Dataverse object for Default dataverse
        self.dataverses = dict()
        default_dataverse_obj = Dataverse()
        self.dataverses[default_dataverse_obj.name] = default_dataverse_obj
    
    def validate_dataverse_in_metadata(self, dataverse_name, username=None, password=None, 
                                       timeout=120, analytics_timeout=120):
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
         dv.DataverseName = \"{0}\";".format(CBASHelper.unformat_name(dataverse_name))
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if status == "success":
            if results:
                return True
            else:
                return False
        else:
            return False
    
    def create_dataverse(self, dataverse_name, username=None, password=None,
                         validate_error_msg=False, expected_error=None,
                         expected_error_code=None, if_not_exists=False,
                         analytics_scope=False, timeout=120, analytics_timeout=120):
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
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                if not self.get_dataverse_obj(dataverse_name):
                    self.dataverses[obj.name] = obj
                return True

    def drop_dataverse(self, dataverse_name, username=None, password=None,
                       validate_error_msg=False, expected_error=None,
                       expected_error_code=None, if_exists=False,
                       analytics_scope=False, timeout=120, analytics_timeout=120,
                       delete_dataverse_obj=True):
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
        
        if analytics_scope:
            cmd = "drop analytics scope %s" % dataverse_name
        else:
            cmd = "drop dataverse %s" % dataverse_name
        
        if if_exists:
            cmd += " if exists"
        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password, timeout=timeout, 
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
    
    def create_analytics_scope(self, cbas_scope_name, username=None, password=None,
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
        return self.create_dataverse(cbas_scope_name, username, password, 
                                     validate_error_msg, expected_error, 
                                     expected_error_code, if_not_exists, True,
                                     timeout, analytics_timeout)

    def drop_analytics_scope(self, cbas_scope_name, username=None, password=None,
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
        return self.drop_dataverse(cbas_scope_name, username, password, validate_error_msg, 
                                   expected_error, expected_error_code, if_exists, True,
                                   timeout, analytics_timeout)
    
    def get_dataverse_obj(self, dataverse_name):
        """
        Return Dataverse/CBAS_scope object if dataverse with the required 
        name already exists.
        :param dataverse_name str, fully qualified dataverse_name
        """
        return self.dataverses.get(dataverse_name,None)
    
    @staticmethod
    def get_dataverse_spec(cbas_spec):
        """
        Fetches dataverse specific specs from spec file mentioned.
        :param cbas_spec dict, cbas spec dictonary.
        """
        return cbas_spec.get("dataverse", {})
    
    def create_dataverse_from_spec(self, cbas_spec):
        self.log.info("Creating dataverses based on CBAS Spec")
        jobs = Queue()
        
        dataverse_spec = self.get_dataverse_spec(cbas_spec)
        
        def create_dataverse_object(name):
            if dataverse_spec.get("creation_method","all").lower() == "all":
                jobs.put(random.choice([Dataverse,CBAS_Scope])(name))
            elif dataverse_spec.get("creation_method").lower() == "dataverse":
                jobs.put(Dataverse(name))
            elif dataverse_spec.get("creation_method").lower() == "analytics_scope":
                jobs.put(CBAS_Scope(name))
        
        if cbas_spec.get("no_of_dataverses",1) > 1:
            results = list()
            for i in range(1, cbas_spec.get("no_of_dataverses",1)):
                if dataverse_spec.get("name_key","random").lower() == "random":
                    if dataverse_spec.get("cardinality",0) == 0:
                        name = self.generate_name(
                            name_cardinality=random.choice([1,2]))
                    elif dataverse_spec.get("cardinality") == 1:
                        name = self.generate_name(name_cardinality=1)
                    elif dataverse_spec.get("cardinality") == 2:
                        name = self.generate_name(name_cardinality=2)
                    create_dataverse_object(name)
                else:
                    name_key = dataverse_spec.get("name_key") + "_{0}".format(str(i))
                    if dataverse_spec.get("cardinality",0) == 0:
                        name = self.generate_name(
                            name_cardinality=random.choice([1,2]), name_key=name_key)
                    elif dataverse_spec.get("cardinality") == 1:
                        name = name_key
                    elif dataverse_spec.get("cardinality") == 2:
                        name = self.generate_name(
                            name_cardinality=2, name_key=name_key)
                    create_dataverse_object(name)
            
            def consumer_func(dataverse):
                if isinstance(dataverse, CBAS_Scope):
                    analytics_scope = True
                elif isinstance(dataverse, Dataverse):
                    analytics_scope = False
                return self.create_dataverse(dataverse.name, if_not_exists=True, analytics_scope=analytics_scope)
            
            self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec.get("max_thread_count",1), 
                                      async_run=False, consume_from_queue_func=None)
            
            return all(results)
        return True


class Link_Util(Dataverse_Util):
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        super(Link_Util,self).__init__(master, cbas_node, server_task)
    
    def validate_link_in_metadata(self, link_name, dataverse_name, link_type="Local",
                                  username=None, password=None, is_active=False, 
                                  timeout=120, analytics_timeout=120):
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
             CBASHelper.unformat_name(dataverse_name), CBASHelper.unformat_name(link_name))
        
        if link_type != "Local":
            cmd += " and lnk.`Type` = \"{0}\"".format((link_type).upper())
        cmd += ";"
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if status == "success":
            if results:
                if is_active:
                    if (link_type).lower() != "s3" and results[0]["IsActive"]:
                        return True
                    else:
                        return False
                else:
                    return True
            else:
                return False
        else:
            return False
    
    def is_link_active(self, link_name, dataverse_name, link_type="Local", 
                       username=None, password=None, timeout=120, 
                       analytics_timeout=120):
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
        return self.validate_link_in_metadata(link_name, dataverse_name, link_type, 
                                              username, password, True,
                                              timeout, analytics_timeout)
    
    def create_link_on_cbas(self, link_name=None, username=None, password=None,
                            validate_error_msg=False, expected_error=None,
                            expected_error_code=None, timeout=120, analytics_timeout=120):
        """
        This method will fail in all the scenarios. It is only
        there to test negative scenario. 
        """
        cmd = "create link {0};"

        if not link_name:
            link_name = "Local"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password, timeout=timeout, 
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
    
    def create_link(self,link_properties, username=None, password=None,
                    validate_error_msg=False, expected_error=None, 
                    expected_error_code=None, create_if_not_exists=False,
                    timeout=120, analytics_timeout=120):
        """
        Creates Link.
        :param link_properties: dict, parameters required for creating link.
        Common for both AWS and couchbase link.
        <Required> name : name of the link to be created.
        <Required> scope : name of the dataverse under which the link has to be created.
        <Required> type : s3/couchbase
        
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
        self.log.info("Creating link - {0}.{1}".format(link_properties["scope"],link_properties["name"]))
        exists = False
        if create_if_not_exists:
            exists = self.validate_link_in_metadata(
                link_properties["name"], link_properties["scope"], link_properties["type"], 
                username, password)
        
        if not exists:
            # If dataverse does not exits
            if not self.create_dataverse(
                CBASHelper.format_name(link_properties["scope"]), username=username, password=password, 
                if_not_exists=True, timeout=timeout, analytics_timeout=analytics_timeout):
                return False
            
            params = dict()
            for key, value in link_properties.iteritems():
                if value:
                    if isinstance(value, unicode):
                        params[key] = str(value)
                    else:
                        params[key] = value
            params = urllib.urlencode(params)
            status, status_code, content, errors = self.cbas_helper.analytics_link_operations(
                method="POST", params=params, timeout=timeout, username=username, password=password)
            if validate_error_msg:
                return self.validate_error_in_response(status, errors, expected_error, expected_error_code)
            return status
        else:
            return exists
    
    def drop_link(self,link_name, username=None, password=None,
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
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def get_link_info(self, dataverse=None, link_name=None, link_type=None,
                      username=None, password=None, timeout=120, restapi=True, 
                      validate_error_msg=False, expected_error=None, expected_error_code=None):
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
        if restapi:
            params = dict()
            if dataverse:
                params["scope"] = dataverse
            if link_name:
                params["name"] = link_name
            if link_type:
                params["type"] = link_type
            params = urllib.urlencode(params)
            status, status_code, content, errors = self.cbas_helper.analytics_link_operations(
                method="GET", params=params, timeout=timeout, username=username, password=password)
            if validate_error_msg:
                return self.validate_error_in_response(status, errors, expected_error, expected_error_code)
            if status:
                return content

    def update_external_link_properties(self, link_properties, username=None, password=None, timeout=120,
                                        validate_error_msg=False, expected_error=None, expected_error_code=None):
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
        params = dict()
        for key, value in link_properties.iteritems():
            if value:
                if isinstance(value, unicode):
                    params[key] = str(value)
                else:
                    params[key] = value
        params = urllib.urlencode(params)
        status, status_code, content, errors = self.cbas_helper.analytics_link_operations(
            method="PUT", params=params, timeout=timeout, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error, expected_error_code)
        return status
    
    def connect_link(self, link_name, validate_error_msg=False, with_force=False, 
                     username=None, password=None, expected_error=None, 
                     expected_error_code=None, timeout=120, analytics_timeout=120):
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

        if with_force is True:
            cmd_connect_bucket += " with {'force':true}"

        retry_attempt = 5
        connect_bucket_failed = True
        while connect_bucket_failed and retry_attempt > 0:
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
                cmd_connect_bucket, username=username, password=password, 
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

    def disconnect_link(self, link_name, validate_error_msg=False, username=None, 
                        password=None, expected_error=None, expected_error_code=None, 
                        timeout=120, analytics_timeout=120):
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
            cmd_disconnect_link, username=username, password=password,
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
        return cbas_spec.get("link",{})
    
    def create_link_from_spec(self, cbas_spec):
        self.log.info("Creating Links based on CBAS Spec")
        jobs = Queue()
        
        link_spec = self.get_link_spec(cbas_spec)
        
        if cbas_spec.get("no_of_links",0) > 0:
            
            if cbas_spec.get("percent_of_remote_links",0) == cbas_spec.get(
                "percent_of_external_links",0) or cbas_spec.get(
                    "percent_of_remote_links",0) + cbas_spec.get(
                        "percent_of_external_links",0) > 100:
                no_of_remote_links = cbas_spec.get("no_of_links")//2
                no_of_external_links = cbas_spec.get("no_of_links")//2
            else:
                no_of_remote_links = cbas_spec.get("no_of_links") * (
                    cbas_spec.get("percent_of_remote_links") // 100)
                no_of_external_links = cbas_spec.get("no_of_links")* (
                    cbas_spec.get("percent_of_external_links") // 100)
            
            results = list()
            for i in range(1, cbas_spec.get("no_of_links")+1):
                if link_spec.get("name_key", "random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = link_spec.get("name_key") + "_{0}".format(str(i))
                
                dataverse = None
                while not dataverse: 
                    dataverse = random.choice(self.dataverses.values())
                    if link_spec.get("include_dataverses",[]) and CBASHelper.unformat_name(
                        dataverse.name) not in link_spec.get("include_dataverses"):
                        dataverse = None
                    if link_spec.get("exclude_dataverses",[]) and CBASHelper.unformat_name(
                        dataverse.name) in link_spec.get("exclude_dataverses"):
                        dataverse = None
                
                link = Link(name=name, dataverse_name=dataverse.name, 
                            properties=random.choice(link_spec.get("properties",[{}])))
                
                if link.link_type == "s3":
                    if no_of_external_links > 0:
                        no_of_external_links -= 1
                    else:
                        while link.link_type == "s3":
                            link = Link(name=name, dataverse_name=dataverse.name, 
                                        properties=random.choice(link_spec.get("properties")))
                        no_of_remote_links -= 1
                elif link.link_type == "couchbase":
                    if no_of_remote_links > 0:
                        no_of_remote_links -= 1
                    else:
                        while link.link_type == "couchbase":
                            link = Link(name=name, dataverse_name=dataverse.name, 
                                        properties=random.choice(link_spec.get("properties")))
                        no_of_external_links -= 1
                jobs.put(link)
                dataverse.links[link.name] = link
            
            def consumer_func(link):
                return self.create_link(link.properties,create_if_not_exists=True)
            
            self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec.get("max_thread_count",1), 
                                      async_run=False, consume_from_queue_func=None)
            
            return all(results)
        return True
    
    def get_link_obj(self, link_name, link_type=None, dataverse_name=None):
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
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cmd)
            if status == "success":
                if results:
                    dataverse_name = CBASHelper.format_name(results[0]["DataverseName"])
                    link_type = results[0]["Type"]
                else:
                    return None
            else:
                return None
            
        dataverse_obj = self.get_dataverse_obj(dataverse_name)
        link = dataverse_obj.links.get(link_name,None)
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


class Dataset_Util(Link_Util):
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        super(Dataset_Util,self).__init__(master, cbas_node, server_task)
    
    def validate_dataset_in_metadata(self, dataset_name, dataverse_name=None, username=None, 
                                     password=None, timeout=120, analytics_timeout=120, 
                                     **kwargs):
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
            cmd += ' and DataverseName = "{0}"'.format(CBASHelper.unformat_name(dataverse_name))
        cmd += ";"
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if status == "success" and results:
            for result in results:
                if result["DatasetType"] == "INTERNAL":
                    for key,value in kwargs.items():
                        if value != result.get(key, ''):
                            self.log.error("Data Mismatch. Expected - {0} /t Actual - {1}".format(
                                result.get(key, ''), value))
                            return False
                elif result["DatasetType"] == "EXTERNAL":
                    for prop in results["ExternalDetails"]["Properties"]:
                        if prop["Name"] == "container":
                            actual_link_name = prop["Value"]
                        if prop["Name"] == "name":
                            actual_bucket_name = prop["Value"]
                    if actual_link_name != kwargs["link_name"]:
                        self.log.error("Link name mismatch. Expected - {0} /t Actual - {1}".format(
                            kwargs["link_name"], actual_link_name))
                        return False
                    if actual_bucket_name != kwargs["bucket_name"]:
                        self.log.error("Bucket name mismatch. Expected - {0} /t Actual - {1}".format(
                            kwargs["bucket_name"], actual_bucket_name))
                        return False
            return True
        else:
            return False
    
    def create_dataset(self, dataset_name, kv_entity, dataverse_name=None, 
                       if_not_exists=False, compress_dataset=False, 
                       with_clause = None, link_name=None, where_clause=None, 
                       validate_error_msg=False, username = None,
                       password = None, expected_error=None, 
                       timeout=120, analytics_timeout=120, analytics_collection=False):
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
        """
        if dataverse_name and not self.create_dataverse(
            dataverse_name=dataverse_name, username=username, password=password, 
            if_not_exists=True,timeout=timeout, analytics_timeout=analytics_timeout):
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
        
        if compress_dataset:
            cmd += " with {'storage-block-compression': {'scheme': 'snappy'}}"
        
        if with_clause:
            cmd += " " + with_clause
        
        cmd += " on {0}".format(kv_entity)
        
        if link_name:
            cmd += " at {0}".format(link_name)
        
        if where_clause:
            cmd += " " + where_clause
        
        cmd += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def create_analytics_collection(self, dataset_name, kv_entity, dataverse_name=None, 
                                    if_not_exists=False, compress_dataset=False, 
                                    with_clause = None, link_name=None, where_clause=None, 
                                    validate_error_msg=False, username = None,
                                    password = None, expected_error=None, 
                                    timeout=120, analytics_timeout=120):
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
        """
        return self.create_dataset(dataset_name, kv_entity, dataverse_name, if_not_exists, 
                                   compress_dataset, with_clause, link_name, where_clause, 
                                   validate_error_msg, username, password, expected_error, 
                                   timeout, analytics_timeout, True)
    
    def create_dataset_on_external_resource(
            self, dataset_name, aws_bucket_name, link_name, if_not_exists=False,
            dataverse_name=None, object_construction_def=None,
            path_on_aws_bucket=None, file_format="json", redact_warning=None,
            header=None, null_string=None, include=None, exclude=None,
            validate_error_msg=False, username = None,
            password = None, expected_error=None, expected_error_code=None,
            timeout=120, analytics_timeout=120):
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
        if dataverse_name and not self.create_dataverse(
            dataverse_name=dataverse_name, username=username, password=password, 
            if_not_exists=True):
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

        cmd += " WITH {0};".format(json.dumps(with_parameters))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error, expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def drop_dataset(self, dataset_name,
                     username=None, password=None,
                     validate_error_msg=False,
                     expected_error=None, expected_error_code=None,
                     if_exists=False, analytics_collection=False,
                     timeout=120, analytics_timeout=120):
        """
        Drops the dataverse.
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
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(
                status, errors, expected_error, expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def drop_analytics_collection(self, dataset_name,
                                  username=None, password=None,
                                  validate_error_msg=False,
                                  expected_error=None, expected_error_code=None,
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
        return self.drop_dataset(dataset_name, username, password, validate_error_msg, 
                                 expected_error, expected_error_code, if_exists, True,
                                 timeout, analytics_timeout)
    
    def enable_analytics_from_KV(self, kv_entity_name, compress_dataset=False,
                                 validate_error_msg=False, 
                                 expected_error=None, username=None, password=None,
                                 timeout=120, analytics_timeout=120):
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
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def disable_analytics_from_KV(self, kv_entity_name,
                                 validate_error_msg=False, 
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
            cmd, username=username, password=password, timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def get_num_items_in_cbas_dataset(self, dataset_name, timeout=300,
                                      analytics_timeout=300):
        """
        Gets the count of docs in the cbas dataset
        """
        total_items = -1
        mutated_items = -1
        cmd_get_num_items = "select count(*) from %s;" % dataset_name
        cmd_get_num_mutated_items = "select count(*) from %s where mutated>0;"\
                                    % dataset_name

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(
                cmd_get_num_items,
                timeout=timeout,
                analytics_timeout=analytics_timeout)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.debug("No. of items in CBAS dataset {0} : {1}"
                           .format(dataset_name, results[0]['$1']))
            total_items = results[0]['$1']

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(
                cmd_get_num_mutated_items,
                timeout=timeout,
                analytics_timeout=analytics_timeout)
        if status != "success":
            self.log.error("Query failed")
        else:
            self.log.debug("No. of items mutated in CBAS dataset {0}: {1}"
                           .format(dataset_name, results[0]['$1']))
            mutated_items = results[0]['$1']

        return total_items, mutated_items
    
    def wait_for_ingestion_complete(self, dataset_names, num_items,
                                    timeout=300):

        total_items = 0
        for ds_name in dataset_names:
            total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]

        counter = 0
        while timeout > counter:
            self.log.debug("Total items in CB Bucket to be ingested "
                           "in CBAS datasets %s"
                           % num_items)
            if num_items == total_items:
                self.log.debug("Data ingestion completed in %s seconds."
                               % counter)
                return True
            else:
                sleep(2)
                total_items = 0
                for ds_name in dataset_names:
                    total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]
                counter += 2

        return False

    def validate_cbas_dataset_items_count(self, dataset_name, expected_count,
                                          expected_mutated_count=0,
                                          num_tries=12,
                                          timeout=300,
                                          analytics_timeout=300):
        """
        Compares the count of CBAS dataset total
        and mutated items with the expected values.
        """
        count, mutated_count = self.get_num_items_in_cbas_dataset(
            dataset_name,
            timeout=timeout,
            analytics_timeout=analytics_timeout)
        tries = num_tries
        if expected_mutated_count:
            while (count != expected_count
                   or mutated_count != expected_mutated_count) and tries > 0:
                sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(
                    dataset_name,
                    timeout=timeout,
                    analytics_timeout=analytics_timeout)
                tries -= 1
        else :
            while count != expected_count and tries > 0:
                sleep(10)
                count, mutated_count = self.get_num_items_in_cbas_dataset(
                    dataset_name,
                    timeout=timeout,
                    analytics_timeout=analytics_timeout)
                tries -= 1

        self.log.debug("Expected Count: %s, Actual Count: %s"
                       % (expected_count, count))
        self.log.debug("Expected Mutated Count: %s, Actual Mutated Count: %s"
                       % (expected_mutated_count, mutated_count))

        if count != expected_count:
            return False
        elif mutated_count == expected_mutated_count:
            return True
        else:
            return False
    
    def get_dataset_compression_type(self, dataset_name):
        query = "select raw BlockLevelStorageCompression.DatasetCompressionScheme from Metadata.`Dataset` where DatasetName='{0}';".format(
            CBASHelper.unformat_name(dataset_name))
        _, _, _, ds_compression_type, _ = self.execute_statement_on_cbas_util(query)
        ds_compression_type = ds_compression_type[0]
        if ds_compression_type is not None:
            ds_compression_type = ds_compression_type.encode('ascii', 'ignore')
        self.log.info("Compression Type for Dataset {0} is {1}".format(dataset_name, ds_compression_type))

        return ds_compression_type
    
    def get_dataset_obj(self, dataset_name, dataverse_name=None):
        """
        Return Dataset/CBAS_collection object if the dataset with the required name already exists.
        If dataverse_name is not mentioned then it will return first dataset found with the dataset_name.
        :param dataset_name str, name of the dataset whose object has to returned.
        :param dataverse_name str, name of the dataverse where the dataset is present
        """
        if not dataverse_name:
            cmd = 'SELECT value MD FROM Metadata.`Dataset` as MD WHERE DatasetName="{0}";'.format(
                CBASHelper.unformat_name(dataset_name))
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cmd)
            if status == "success" and results:
                dataverse_name = CBASHelper.format_name(results[0]["DataverseName"])
            else:
                return None
        
        dataverse_obj = self.get_dataverse_obj(dataverse_name)
        return dataverse_obj.datasets.get(dataset_name,None)
    
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
        return cbas_spec.get("dataset",{})
    
    def create_dataset_from_spec(self, cbas_spec, local_bucket_util, remote_bucket_util=None):
        self.log.info("Creating Datasets based on CBAS Spec")
        jobs = Queue()
        dataset_spec = self.get_dataset_spec(cbas_spec)
        
        if cbas_spec.get("no_of_datasets_per_dataverse",0) > 0:
            results = list()
            
            remote_link_objs = set()
            external_link_objs = set()
            if cbas_spec.get("no_of_links",0) > 0:
                # Return false if both include and exclude link types are same.
                if dataset_spec.get("include_link_types","all") == dataset_spec.get("exclude_link_types", None):
                    return False
                
                if dataset_spec.get("include_link_types","all").lower() == "all" and not dataset_spec.get("exclude_link_types",None):
                    remote_link_objs |= set(self.list_all_link_objs("couchbase"))
                    external_link_objs |= set(self.list_all_link_objs("s3"))
                else:
                    if dataset_spec.get("include_link_types").lower() == "s3" or dataset_spec.get("exclude_link_types").lower() == "couchbase":
                        external_link_objs |= set(self.list_all_link_objs("s3"))
                    elif dataset_spec.get("include_link_types").lower() == "couchbase" or dataset_spec.get("exclude_link_types").lower() == "s3":
                        remote_link_objs |= set(self.list_all_link_objs("couchbase"))
                
                if dataset_spec.get("include_links",[]):
                    for link_name in dataset_spec["include_links"]:
                        link_obj = self.get_link_obj(CBASHelper.format_name(link_name))
                        if link_obj.link_type == "s3":
                            external_link_objs |= set([link_obj])
                        else:
                            remote_link_objs |= set([link_obj])
                
                if dataset_spec.get("exclude_links",[]):
                    for link_name in dataset_spec["exclude_links"]:
                        link_obj = self.get_link_obj(CBASHelper.format_name(link_name))
                        if link_obj.link_type == "s3":
                            external_link_objs -= set([link_obj])
                        else:
                            remote_link_objs -= set([link_obj])
            remote_link_objs = list(remote_link_objs)
            external_link_objs = list(external_link_objs)
            
            if cbas_spec.get("no_of_dataverses",1) == 0:
                cbas_spec["no_of_dataverses"] = 1
            total_no_of_datasets = cbas_spec.get("no_of_dataverses",1) * cbas_spec.get("no_of_datasets_per_dataverse",0)
            if cbas_spec.get("percent_of_local_datasets",0) == cbas_spec.get(
                "percent_of_remote_datasets",0) == cbas_spec.get("percent_of_external_datasets",0) or cbas_spec.get(
                    "percent_of_local_datasets",0) + cbas_spec.get("percent_of_remote_datasets",0) + cbas_spec.get(
                        "percent_of_external_datasets",0) > 100:
                no_of_external_datasets = total_no_of_datasets // 3
                no_of_remote_datasets = total_no_of_datasets // 3
                no_of_local_datasets = total_no_of_datasets // 3
            else:
                no_of_external_datasets = total_no_of_datasets * (cbas_spec.get("percent_of_external_datasets",0) // 100)
                no_of_remote_datasets = total_no_of_datasets * (cbas_spec.get("percent_of_remote_datasets") // 100)
                no_of_local_datasets = total_no_of_datasets - no_of_external_datasets - no_of_remote_datasets
            
            for dataverse in self.dataverses.values():
                if dataset_spec.get("include_dataverses",[]) and CBASHelper.unformat_name(
                    dataverse.name) not in dataset_spec["include_dataverses"]:
                    dataverse = None
                if dataset_spec.get("exclude_dataverses",[]) and CBASHelper.unformat_name(
                    dataverse.name) in dataset_spec["exclude_dataverses"]:
                    dataverse = None
                
                if dataverse:
                    for i in range(1, cbas_spec.get("no_of_datasets_per_dataverse",1)+1):
                        dataset_obj = None
                        if dataset_spec.get("name_key","random").lower() == "random":
                            name = self.generate_name(name_cardinality=1)
                        else:
                            name = dataset_spec["name_key"] + "_{0}".format(str(i))
                        
                        if dataset_spec.get("datasource","all").lower() == "all":
                            datasource = random.choice(["internal","external"])
                        else:
                            datasource = dataset_spec["datasource"].lower()
                        
                        if datasource == "external":
                            if no_of_external_datasets > 0:
                                no_of_external_datasets -= 1
                            else:
                                datasource = "internal"
                        
                        if not dataset_spec.get("creation_methods",[]):
                            dataset_spec["creation_methods"] = [
                                "cbas_collection","cbas_dataset","enable_cbas_from_kv"]
                        
                        if datasource == "internal" and (
                            no_of_local_datasets > 0 or no_of_remote_datasets > 0):
                            if len(remote_link_objs) > 0:
                                remote_dataset = random.choice(["True","False"])
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
                            
                            if dataset_spec.get("bucket_cardinality",0) == 0:
                                bucket_cardinality = random.choice([1,3])
                            else:
                                bucket_cardinality = dataset_spec["bucket_cardinality"]
                            
                            enabled_from_KV = False
                            if remote_dataset:
                                while True:
                                    creation_method = random.choice(dataset_spec["creation_methods"])
                                    if creation_method != "enable_cbas_from_kv":
                                        break
                                bucket, scope, collection = self.get_kv_entity(
                                    remote_bucket_util,bucket_cardinality,
                                    dataset_spec.get("include_buckets",[]),dataset_spec.get("exclude_buckets",[]),
                                    dataset_spec.get("include_scopes",[]),dataset_spec.get("exclude_scopes",[]),
                                    dataset_spec.get("include_collections",[]),dataset_spec.get("exclude_collections",[]))
                                link_name = random.choice(remote_link_objs).full_name
                                if collection:
                                    num_of_items = collection.num_items
                                else:
                                    num_of_items = remote_bucket_util.get_collection_obj(
                                        remote_bucket_util.get_scope_obj(bucket, "_default"), "_default").num_items
                            else:
                                creation_method = random.choice(dataset_spec["creation_methods"])
                                bucket, scope, collection = self.get_kv_entity(
                                    local_bucket_util,bucket_cardinality,
                                    dataset_spec.get("include_buckets",[]),dataset_spec.get("exclude_buckets",[]),
                                    dataset_spec.get("include_scopes",[]),dataset_spec.get("exclude_scopes",[]),
                                    dataset_spec.get("include_collections",[]),dataset_spec.get("exclude_collections",[]))
                                link_name = None
                                if creation_method == "enable_cbas_from_kv":
                                    enabled_from_KV = True
                                    temp_scope = scope
                                    temp_collection = collection
                                    if not scope:
                                        temp_scope = local_bucket_util.get_scope_obj(bucket, "_default")
                                        temp_collection = local_bucket_util.get_collection_obj(temp_scope, "_default")
                                        # Check Synonym with name bucket name is present in Default dataverse or not
                                        cmd = "select value sy from Metadata.`Synonym` as sy where \
                                        sy.SynonymName = \"{0}\" and sy.DataverseName = \"{1}\";".format(
                                            bucket.name, "Default")
                                        
                                        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cmd)
                                        if status == "success":
                                            if results:
                                                enabled_from_KV = False
                                        else:
                                            enabled_from_KV = False
                                    
                                    temp_dataverse_name = CBASHelper.format_name(bucket.name,temp_scope.name)
                                    temp_dataverse_obj = self.get_dataverse_obj(temp_dataverse_name)
                                    if temp_dataverse_obj:
                                        if temp_dataverse_obj.datasets.get(CBASHelper.format_name(
                                            temp_collection.name),None):
                                            enabled_from_KV = False
                                    if enabled_from_KV:
                                        if not scope:
                                            synonym_obj = Synonym(bucket.name, temp_collection.name, 
                                                                  temp_dataverse_name, dataverse_name="Default")
                                            self.dataverses["Default"].synonyms[synonym_obj.name] = synonym_obj
                                        scope = temp_scope
                                        collection = temp_collection
                                        name = collection.name
                                        dataverse = Dataverse(temp_dataverse_name)
                                        self.dataverses[temp_dataverse_name] = dataverse
                                    else:
                                        creation_method = random.choice(["cbas_collection","cbas_dataset"])
                                if collection:
                                    num_of_items = collection.num_items
                                else:
                                    num_of_items = local_bucket_util.get_collection_obj(
                                        local_bucket_util.get_scope_obj(bucket, "_default"), "_default").num_items
                            
                            if creation_method == "cbas_collection":
                                dataset_obj = CBAS_Collection(
                                    name=name, dataverse_name=dataverse.name,link_name=link_name, 
                                    dataset_source="internal", dataset_properties={},
                                    bucket=bucket, scope=scope, collection=collection, 
                                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)
                            else:
                                dataset_obj = Dataset(
                                    name=name, dataverse_name=dataverse.name,link_name=link_name, 
                                    dataset_source="internal", dataset_properties={},
                                    bucket=bucket, scope=scope, collection=collection, 
                                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)
                        elif datasource == "external" and no_of_external_datasets > 0:
                            if len(external_link_objs) == 0:
                                return False
                            link=random.choice(external_link_objs)
                            while True:
                                dataset_properties=random.choice(dataset_spec.get("external_dataset_properties",[{}]))
                                if link.properties["region"] == dataset_properties.get("region",None):
                                    break
                            dataset_obj = Dataset(
                                name=name, dataverse_name=dataverse.name, link_name=link.full_name, 
                                dataset_source="external", dataset_properties=dataset_properties,
                                bucket=None, scope=None, collection=None, enabled_from_KV=False)
                        if dataset_obj:
                            jobs.put(dataset_obj)
                            dataverse.datasets[dataset_obj.name] = dataset_obj
            
            def consumer_func(dataset):
                dataverse_name = dataset.dataverse_name
                if dataverse_name == "Default":
                    dataverse_name = None
                if dataset.dataset_source == "internal":
                    if dataset.enabled_from_KV:
                        return self.enable_analytics_from_KV(
                            dataset.full_kv_entity_name, False, False, None, None, None, 120, 120)
                    else:
                        if isinstance(dataset, CBAS_Collection):
                            analytics_collection = True
                        elif isinstance(dataset, Dataset):
                            analytics_collection = False
                        return self.create_dataset(
                            dataset.name, dataset.full_kv_entity_name, dataverse_name, 
                            False, False, None, dataset.link_name, None, False, None, None, 
                            None, 120, 120, analytics_collection)
                else:
                    return self.create_dataset_on_external_resource(
                        dataset.name, dataset.dataset_properties["aws_bucket_name"], dataset.link_name, False,
                        dataverse_name, dataset.dataset_properties["object_construction_def"],
                        dataset.dataset_properties["path_on_aws_bucket"], 
                        dataset.dataset_properties["file_format"], 
                        dataset.dataset_properties["redact_warning"],
                        dataset.dataset_properties["header"], 
                        dataset.dataset_properties["null_string"], 
                        dataset.dataset_properties["include"], 
                        dataset.dataset_properties["exclude"],
                        False, None, None, None, None, 120, 120)
            
            self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec.get("max_thread_count",1), 
                                      async_run=False, consume_from_queue_func=None)
                
            return all(results)
        return True
    
    def create_datasets_on_all_collections(self, bucket_util, cbas_name_cardinality=1, kv_name_cardinality=1, 
                                           remote_datasets=False, max_thread_count=10):
        """
        Create datasets on every collection across all the buckets and scopes.
        :param bucket_util obj, bucket_util obj to perform operations on KV bucket.
        :param cbas_name_cardinality int, no of parts in dataset name. valid value 1,2,3 
        :param kv_name_cardinality int, no of parts in KV entity name. Valid values 1 or 3. 
        :param remote_datasets bool, if True create remote datasets using remote links.
        :param max_thread_count int, max no. of parallel threads to execute.
        """
        self.log.info("Creating Datasets on all KV collections")
        jobs = Queue()
        results = list()
        
        creation_methods = ["cbas_collection","cbas_dataset","enable_cbas_from_kv"]
        
        if remote_datasets:
            remote_link_objs = self.list_all_link_objs("couchbase")
            creation_methods.remove("enable_cbas_from_kv")
        
        def dataset_creation(bucket, scope, collection):
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
                    dataverse = Dataverse(self.generate_name(cbas_name_cardinality-1))
                    self.dataverses[dataverse.name] = dataverse
                else:
                    dataverse = self.get_dataverse_obj("Default")
                                    
            num_of_items = collection.num_items
                
            if creation_method == "cbas_collection":
                dataset_obj = CBAS_Collection(
                    name=name, dataverse_name=dataverse.name,link_name=link_name, 
                    dataset_source="internal", dataset_properties={},
                    bucket=bucket, scope=scope, collection=collection, 
                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)
            else:
                dataset_obj = Dataset(
                    name=name, dataverse_name=dataverse.name,link_name=link_name, 
                    dataset_source="internal", dataset_properties={},
                    bucket=bucket, scope=scope, collection=collection, 
                    enabled_from_KV=enabled_from_KV, num_of_items=num_of_items)
            
            jobs.put(dataset_obj)
            dataverse.datasets[dataset_obj.full_name] = dataset_obj
        
        for bucket in bucket_util.buckets:
            if kv_name_cardinality > 1:
                for scope in bucket_util.get_active_scopes(bucket):
                    for collection in bucket_util.get_active_collections(bucket, scope.name):
                        dataset_creation(bucket, scope, collection)
            else:
                scope = bucket_util.get_scope_obj(bucket, "_default")
                dataset_creation(bucket, scope, bucket_util.get_collection_obj(scope, "_default"))
                
            
        def consumer_func(dataset):
            dataverse_name = dataset.dataverse_name
            if dataverse_name == "Default":
                dataverse_name = None
            if dataset.enabled_from_KV:
                if kv_name_cardinality > 1:
                    return self.enable_analytics_from_KV(
                        dataset.full_kv_entity_name, False, False, None, None, None, 120, 120)
                else:
                    return self.enable_analytics_from_KV(
                        dataset.get_fully_qualified_kv_entity_name(1), False, False, None, None, None, 120, 120)
            else:
                if isinstance(dataset, CBAS_Collection):
                    analytics_collection = True
                elif isinstance(dataset, Dataset):
                    analytics_collection = False
                if kv_name_cardinality > 1 and cbas_name_cardinality > 1:
                    return self.create_dataset(
                        dataset.name, dataset.full_kv_entity_name, dataverse_name, 
                        False, False, None, dataset.link_name, None, False, None, None, 
                        None, 120, 120, analytics_collection)
                elif kv_name_cardinality > 1 and cbas_name_cardinality == 1:
                    return self.create_dataset(
                        dataset.name, dataset.full_kv_entity_name, None, 
                        False, False, None, dataset.link_name, None, False, None, None, 
                        None, 120, 120, analytics_collection)
                elif kv_name_cardinality == 1 and cbas_name_cardinality > 1:
                    return self.create_dataset(
                        dataset.name, dataset.get_fully_qualified_kv_entity_name(1), dataverse_name, 
                        False, False, None, dataset.link_name, None, False, None, None, 
                        None, 120, 120, analytics_collection)
                else:
                    return self.create_dataset(
                        dataset.name, dataset.get_fully_qualified_kv_entity_name(1), None, 
                        False, False, None, dataset.link_name, None, False, None, None, 
                        None, 120, 120, analytics_collection)
        
        self.run_jobs_in_parallel(consumer_func, jobs, results, max_thread_count, 
                                  async_run=False, consume_from_queue_func=None)
            
        return all(results)
    
    def create_dataset_obj(self, bucket_util, dataset_cardinality=1, bucket_cardinality=1,
                           enabled_from_KV=False, name_length=30, fixed_length=False,
                           exclude_bucket=[], exclude_scope=[], exclude_collection=[], 
                           no_of_objs=999999):
        """
        Generates dataset objects.
        """
        
        def create_object(bucket,scope,collection):
            if not scope:
                scope = bucket_util.get_scope_obj(bucket, "_default")
            if not collection:
                collection = bucket_util.get_collection_obj(scope, "_default")
            
            if enabled_from_KV:
                dataverse_name = bucket.name + "." + scope.name
                dataset_name = collection.name 
            else:
                dataverse_name = None
                
                if dataset_cardinality > 1:
                    dataverse_name = self.generate_name(
                        name_cardinality=dataset_cardinality-1, 
                        max_length=name_length-1, fixed_length=fixed_length)
                dataset_name = self.generate_name(
                    name_cardinality=1, max_length=name_length, fixed_length=fixed_length)
            
            if dataverse_name:
                dataverse_obj = self.get_dataverse_obj(dataverse_name)
                if not dataverse_obj:
                    dataverse_obj = Dataverse(dataverse_name)
                    self.dataverses[dataverse_name] = dataverse_obj
            else:
                dataverse_obj = self.get_dataverse_obj("Default")
            
            dataset_obj = Dataset(
                name=dataset_name, dataverse_name=dataverse_name, link_name=None, 
                dataset_source="internal", dataset_properties={},
                bucket=bucket, scope=scope, collection=collection, 
                enabled_from_KV=enabled_from_KV, num_of_items=collection.num_items)
            
            dataverse_obj.datasets[dataset_obj.full_name] = dataset_obj
            
            if enabled_from_KV:
                if collection.name == "_default":
                    dataverse_obj.synonyms[bucket.name] = Synonym(
                        bucket.name, collection.name, dataverse_name, dataverse_name="Default", synonym_on_synonym=False)
        
        count = 0
        for bucket in bucket_util.buckets:
            
            if bucket.name in exclude_bucket:
                    continue
            
            if bucket_cardinality == 1:
                count +=1
                create_object(bucket,None,None)
            else:
                active_scopes = bucket_util.get_active_scopes(bucket)
                for scope in active_scopes:
                    if scope.is_dropped or scope.name in exclude_scope:
                        continue
                    
                    active_collections = bucket_util.get_active_collections(bucket, scope.name, only_names=False)
                    for collection in active_collections:
                        if collection.is_dropped or collection.name in exclude_collection:
                            continue
                        create_object(bucket,scope,collection)
                        count +=1
                        if count > no_of_objs:
                            break
                    if count > no_of_objs:
                            break
            if count > no_of_objs:
                break
            

class Synonym_Util(Dataset_Util):
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        super(Synonym_Util,self).__init__(master, cbas_node, server_task)
    
    def validate_synonym_in_metadata(self, synonym_name, synonym_dataverse_name,
                                     cbas_entity_name, cbas_entity_dataverse_name, 
                                     username=None, password=None):
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
            CBASHelper.unformat_name(synonym_name), CBASHelper.unformat_name(synonym_dataverse_name))
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password)
        if status == "success":
            for result in results:
                if result['ObjectDataverseName'] == CBASHelper.unformat_name(
                    cbas_entity_dataverse_name) and result['ObjectName'] == CBASHelper.unformat_name(
                        cbas_entity_name):
                    return True
            return False
        else:
            return False
    
    def create_analytics_synonym(self, synonym_full_name,
                                 cbas_entity_full_name,
                                 if_not_exists=False,
                                 validate_error_msg=False, expected_error=None, 
                                 username=None, password=None,
                                 timeout=120, analytics_timeout=120):
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
            cmd, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def drop_analytics_synonym(self, synonym_full_name, if_exists=False,
                               validate_error_msg=False, expected_error=None, 
                               username=None, password=None,
                               timeout=120, analytics_timeout=120):
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
            cmd, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def validate_synonym_doc_count(self, synonym_full_name, cbas_entity_full_name,
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
                cmd_get_num_items % synonym_full_name,
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
                               .format(synonym_full_name, synonym_results[0]['$1']))
                synonym_results = synonym_results[0]['$1']
    
            dataset_status, metrics, errors, dataset_results, _ = \
                self.execute_statement_on_cbas_util(
                    cmd_get_num_items % cbas_entity_full_name,
                    timeout=timeout,
                    analytics_timeout=analytics_timeout)
            if dataset_status != "success":
                self.log.error("Querying dataset failed")
                dataset_results = 0
            else:
                self.log.debug("No. of items in dataset {0} : {1}"
                               .format(cbas_entity_full_name, dataset_results[0]['$1']))
                dataset_results = dataset_results[0]['$1']
            
            if dataset_results != synonym_results:
                return False
            return True
    
    def get_synonym_obj(self, synonym_name, synonym_dataverse=None, 
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
            sy.SynonymName = \"{0}\"".format(CBASHelper.unformat_name(synonym_name))
            if cbas_entity_name:
                cmd += " and sy.ObjectName = \"{0}\"".format(CBASHelper.unformat_name(cbas_entity_name))
            if cbas_entity_dataverse:
                cmd += " and sy.ObjectDataverseName = \"{0}\"".format(CBASHelper.unformat_name(cbas_entity_dataverse))
            cmd += ";"
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cmd)
            if status == "success" and results:
                synonym_dataverse = CBASHelper.format_name(results[0]["DataverseName"])
            else:
                return None
        
        dataverse_obj = self.get_dataverse_obj(synonym_dataverse)
        return dataverse_obj.synonyms.get(synonym_name,None)
    
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
    
    def create_synonym_from_spec(self, cbas_spec):
        jobs = Queue()
        
        synonym_spec = self.get_synonym_spec(cbas_spec)
        
        if cbas_spec.get("no_of_synonyms",0) > 0:
            
            results = list()
            all_cbas_entities = self.list_all_dataset_objs() + self.list_all_synonym_objs()
            for i in range(1, cbas_spec["no_of_synonyms"]+1):
                if synonym_spec.get("name_key","random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = synonym_spec["name_key"] + "_{0}".format(str(i)) 
                
                dataverse = None
                while not dataverse: 
                    dataverse = random.choice(self.dataverses.values())
                    if synonym_spec["include_dataverses"] and CBASHelper.unformat_name(
                        dataverse.name) not in synonym_spec["include_dataverses"]:
                        dataverse = None
                    if synonym_spec["exclude_dataverses"] and CBASHelper.unformat_name(
                        dataverse.name) in synonym_spec["exclude_dataverses"]:
                        dataverse = None
                
                cbas_entity = None
                while not cbas_entity:
                    cbas_entity = random.choice(all_cbas_entities)
                    if synonym_spec["include_datasets"] or synonym_spec["include_synonyms"]:
                        if CBASHelper.unformat_name(cbas_entity.name) not in synonym_spec["include_datasets"] + synonym_spec["include_synonyms"]:
                            cbas_entity = None
                    if synonym_spec["exclude_datasets"] or synonym_spec["exclude_synonyms"]:
                        if CBASHelper.unformat_name(cbas_entity.name) in synonym_spec["exclude_datasets"] + synonym_spec["exclude_synonyms"]:
                            cbas_entity = None
                
                if isinstance(cbas_entity,Synonym):
                    synonym_on_synonym = True
                else:
                    synonym_on_synonym = False
                synonym = Synonym(name=name, cbas_entity_name=cbas_entity.name, 
                                  cbas_entity_dataverse=cbas_entity.dataverse_name, 
                                  dataverse_name=dataverse.name,synonym_on_synonym=synonym_on_synonym)
                jobs.put(synonym)
                dataverse.synonyms[synonym.name] = synonym
            
            def consumer_func(synonym):
                return self.create_analytics_synonym(
                    synonym.full_name, synonym.cbas_entity_full_name,
                    if_not_exists=False, validate_error_msg=False, 
                    expected_error=None, username=None, password=None,
                    timeout=120, analytics_timeout=120)
            
            self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec.get("max_thread_count",1), 
                                      async_run=False, consume_from_queue_func=None)            
            return all(results)
        return True 


class Index_Util(Synonym_Util):
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        super(Index_Util,self).__init__(master, cbas_node, server_task)
    
    def verify_index_created(self, index_name, dataset_name, indexed_fields):
        '''
        Validate in cbas index entry in Metadata.Index.
        :param index_name str, name of the index to be validated.
        :param dataset_name str, name of the dataset on which the index was created.
        :param indexed_fields list, list of indexed fields.
        '''
        result = True
        statement = "select * from Metadata.`Index` where DatasetName='{0}' and IsPrimary=False"\
                    .format(CBASHelper.unformat_name(dataset_name))
        status, metrics, errors, content, _ = \
            self.execute_statement_on_cbas_util(statement)
        if status != "success":
            result = False
            self.log.debug("Index not created. Metadata query status = %s",status)
        else:
            index_found = False
            for idx in content:
                if idx["Index"]["IndexName"] == CBASHelper.unformat_name(index_name):
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
    
    def verify_index_used(self, statement, index_used=False, index_name=None):
        '''
        Validate whether an index was used while executing the query.
        :param statement str, query to be executed.
        :param index_used bool, verify whether index was used or not.
        :param index_name str, name of the index that should have been used while 
        executing the query.
        '''
        statement = 'EXPLAIN %s'%statement
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            statement)
        if status == 'success':
            if not errors:
                if index_used:
                    if ("index-search" in str(results)) and ("data-scan" not in str(results)):
                        self.log.info(
                            "INDEX-SEARCH is found in EXPLAIN hence indexed data will be scanned to serve %s"%statement)
                        if index_name:
                            if CBASHelper.format_name(index_name) in str(results):
                                return True
                            else:
                                return False
                        return True
                    else:
                        return False
                else:
                    if ("index-search" not in str(results)) and ("data-scan" in str(results)):
                        self.log.info("DATA-SCAN is found in EXPLAIN hence index is not used to serve %s"%statement)
                        return True
                    else:
                        return False
            else:
                return False
        else:
            return False
    
    def create_cbas_index(self, index_name, indexed_fields, dataset_name, 
                          analytics_index=False, validate_error_msg=False, 
                          expected_error=None, username=None, password=None,
                          timeout=120, analytics_timeout=120):
        '''
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
        '''
        index_fields = ""
        for index_field in indexed_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        
        if analytics_index:
            create_idx_statement = "create analytics index {0} on {1}({2});".format(
                index_name, dataset_name, index_fields)
        else:
            create_idx_statement = "create index {0} on {1}({2});".format(
                index_name, dataset_name, index_fields)
                
        self.log.debug("Executing cmd - \n{0}\n".format(create_idx_statement))
        
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            create_idx_statement, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def drop_cbas_index(self, index_name, dataset_name, analytics_index=False,
                        validate_error_msg=False, expected_error=None, 
                        username=None, password=None,
                        timeout=120, analytics_timeout=120):
        '''
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
        '''
        
        if analytics_index:
            drop_idx_statement = "drop analytics index {0}.{1};".format(
                dataset_name, index_name)
        else:
            drop_idx_statement = "drop index {0}.{1};".format(
                dataset_name, index_name)
                
        self.log.debug("Executing cmd - \n{0}\n".format(drop_idx_statement))
        
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            drop_idx_statement, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors, expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def get_index_obj(self, index_name, dataset_name=None, dataset_dataverse=None):
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
            cmd += " and idx.DatasetName = \"{0}\"".format(CBASHelper.unformat_name(dataset_name))
        if dataset_dataverse:
            cmd += " and idx.DataverseName = \"{0}\"".format(CBASHelper.unformat_name(dataset_dataverse))
        cmd += ";"
        
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(cmd)
        if status == "success" and results:
            dataset_dataverse = CBASHelper.format_name(results[0]["DataverseName"])
            dataset_name = CBASHelper.format_name(results[0]["DatasetName"])
        else:
            return None
        
        dataset_obj = self.get_dataset_obj(dataset_name, dataset_dataverse)
        return dataset_obj.indexes.get(index_name,None)
    
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
    
    def create_index_from_spec(self, cbas_spec):
        jobs = Queue()
        
        index_spec = self.get_index_spec(cbas_spec)
        
        if cbas_spec.get("no_of_indexes",0) > 0:
            results = list()
            for i in range(1, cbas_spec["no_of_indexes"]+1):
                if index_spec.get("name_key","random").lower() == "random":
                    name = self.generate_name(name_cardinality=1)
                else:
                    name = index_spec["name_key"] + "_{0}".format(str(i)) 
                
                dataverse = None
                while not dataverse: 
                    dataverse = random.choice(self.dataverses)
                    if index_spec.get("include_dataverses",[]) and CBASHelper.unformat_name(
                        dataverse.name) not in index_spec["include_dataverses"]:
                        dataverse = None
                    if index_spec.get("exclude_dataverses",[]) and CBASHelper.unformat_name(
                        dataverse.name) in index_spec["exclude_dataverses"]:
                        dataverse = None
                
                dataset = None
                while not dataset:
                    dataset = random.choice(dataverse.datasets.values())
                    if index_spec.get("include_datasets",[]) and CBASHelper.unformat_name(
                        dataset.name) not in index_spec["include_datasets"]:
                        dataset = None
                    if index_spec.get("exclude_datasets",[]) and CBASHelper.unformat_name(
                        dataset.name) in index_spec["exclude_datasets"]:
                        dataset = None
                
                index = CBAS_Index(name=name, dataset_name=dataset.name, 
                                   dataverse_name=dataverse.name, 
                                   indexed_fields=random.choice(index_spec.get("indexed_fields",[])))
                jobs.put(index)
                dataset.indexes[index.name] = index
            
            def consumer_func(index):
                if index_spec.get("creation_method","all") == "all":
                    creation_method = random.choice(["cbas_index", "index"])
                else:
                    creation_method = index_spec["creation_method"].lower()
                if creation_method == "cbas_index":
                    index.analytics_index = True
                else:
                    index.analytics_index = False
                return self.create_cbas_index(
                    index_name=index.name, 
                    indexed_fields=index.indexed_fields, 
                    dataset_name=index.full_dataset_name, 
                    analytics_index=index.analytics_index, 
                    validate_error_msg=False, expected_error=None, 
                    username=None, password=None,
                    timeout=120, analytics_timeout=120)
            
            self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec.get("max_thread_count",1), 
                                      async_run=False, consume_from_queue_func=None)
            
            return all(results)
        return True


class CbasUtil(Index_Util):
    
    def __init__(self, master, cbas_node, server_task=None):
        '''
        :param master cluster's master node object
        :param cbas_node CBAS node object
        :param server_task task object
        '''
        super(CbasUtil,self).__init__(master, cbas_node, server_task)
    
    def delete_request(self, client_context_id, username=None, password=None):
        """
        Deletes a request from CBAS
        """
        try:
            if client_context_id is None:
                payload = "client_context_id=None"
            else:
                payload = "client_context_id=" + client_context_id

            status = self.cbas_helper.delete_active_request_on_cbas(payload,
                                                                    username,
                                                                    password)
            self.log.info(status)
            return status
        except Exception, e:
            raise Exception(str(e))

    def retrieve_request_status_using_handle(self, server, handle, shell=None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(handle,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

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

    def retrieve_result_using_handle(self, server, handle):
        """
        Retrieves result from the /analytics/results endpoint
        """
        shell = RemoteMachineShellConnection(server)

        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(handle,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

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
                return float(items[0])*1000
            if items[1] == "ms":
                return float(items[0])
            if items[1] == "m":
                return float(items[0])*1000*60
            if items[1] == "h":
                return float(items[0])*1000*60*60
        else:
            return None

    def async_query_execute(self, statement, mode, num_queries):
        """
        Asynchronously run queries
        """
        self.log.debug("Executing %s queries concurrently", num_queries)

        cbas_base_url = "http://{0}:8095/analytics/service" \
                        .format(self.cbas_node.ip)

        pretty = "true"
        tasks = []
        for count in range(0, num_queries):
            tasks.append(self.cluster.async_cbas_query_execute(self.master,
                                                               self.cbas_node,
                                                               cbas_base_url,
                                                               statement,
                                                               mode,
                                                               pretty))
        return tasks

    def _run_concurrent_queries(self, query, mode, num_queries, rest=None,
                                batch_size=100, timeout=300,
                                analytics_timeout=300, wait_for_execution=True):
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
        if rest:
            self.cbas_util = rest
        for i in range(0, num_queries):
            threads.append(Thread(target=self._run_query,
                                  name="query_thread_{0}".format(i),
                                  args=(query, mode, rest, False, 0, timeout,
                                        analytics_timeout)))
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
                           % (num_queries, self.failed_count, self.success_count,
                              self.rejected_count, self.cancel_count,
                              self.timeout_count))
            if self.failed_count+self.error_count != 0:
                raise Exception("Queries Failed:%s , Queries Error Out:%s"
                                % (self.failed_count, self.error_count))
        return self.handles

    def _run_query(self, query, mode, rest=None, validate_item_count=False,
                   expected_count=0, timeout=300, analytics_timeout=300):
        # Execute query (with sleep induced)
        name = threading.currentThread().getName()
        client_context_id = name
        if rest:
            self.cbas_util = rest
        try:
            status, metrics, errors, results, handle = \
                self.execute_statement_on_cbas_util(
                    query, mode=mode, rest=rest, timeout=timeout,
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
                    print handle
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
                self.error_count +=1
                self.log.error(str(e))

    def retrieve_cc_ip(self,shell=None):

        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        url = self.cbas_helper.cbas_base_url + "/analytics/cluster"
        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(url,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)

        if not shell:
            shell.disconnect()

        cc_node_id = ""
        cc_node_ip = ""
        cc_node_config_url = None
        if 'ccNodeId' in response:
            cc_node_id = response['ccNodeId']
        if 'nodes' in response:
            nodes = response['nodes']
            for node in nodes:
                if node["nodeId"] == cc_node_id:
                    cc_node_config_url = node['configUri']
                    cc_node_ip = node['nodeName'][:-5]
                    break

        self.log.debug("cc_config_urls=%s, ccNodeId=%s, ccNodeIP=%s"
                       % (cc_node_config_url, cc_node_id, cc_node_ip))
        return cc_node_ip

    def retrieve_nodes_config(self, only_cc_node_url=True, shell=None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        url = self.cbas_helper.cbas_base_url + "/analytics/cluster"
        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(url,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)

        if not shell:
            shell.disconnect()

        cc_node_id = ""
        nodes = None
        cc_node_config_url=None
        if 'ccNodeId' in response:
            cc_node_id = response['ccNodeId']
        if 'nodes' in response:
            nodes = response['nodes']
            for node in nodes:
                if only_cc_node_url and node["nodeId"] == cc_node_id:
                    # node['apiBase'] will not be present in pre-6.5.0 clusters, 
                    #hence the check
                    if 'apiBase' in node:
                        cc_node_config_url = node['apiBase'] + response['nodeConfigUri']
                    else:
                        cc_node_config_url = node['configUri']
                    break

        self.log.debug("cc_config_urls=%s, ccNodeId=%s"
                       % (cc_node_config_url, cc_node_id))
        self.log.debug("Nodes: %s" % nodes)
        return nodes, cc_node_id, cc_node_config_url

    def fetch_analytics_cluster_response(self, shell=None):
        """
        Retrieves response from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        url = self.cbas_helper.cbas_base_url + "/analytics/cluster"
        output, error = shell.execute_command(
            "curl -g -v {0} -u {1}:{2}"
            .format(url,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))
        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        return response

    def retrieve_analyticsHttpAdminListen_address_port(self, node_config_url,
                                                       shell=None):
        """
        Retrieves status of a request from /analytics/status endpoint
        """
        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(node_config_url,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)

        if not shell:
            shell.disconnect()

        analytics_http_admin_listen_addr = None
        analytics_http_admin_listen_port = None

        if 'analyticsHttpAdminListenAddress' in response:
            analytics_http_admin_listen_addr = \
                response['analyticsHttpAdminPublicAddress']
            if analytics_http_admin_listen_addr.find(":") != -1:
                analytics_http_admin_listen_addr = \
                    '['+analytics_http_admin_listen_addr+']'
        if 'analyticsHttpAdminListenPort' in response:
            analytics_http_admin_listen_port = \
                response['analyticsHttpAdminPublicPort']

        return analytics_http_admin_listen_addr,\
               analytics_http_admin_listen_port

    def retrive_replica_from_storage_data(self,
                                          analytics_http_admin_listen_addr,
                                          analytics_http_admin_listen_port,
                                          shell=None):
        url = "http://{0}:{1}/analytics/node/storage" \
              .format(analytics_http_admin_listen_addr,
                      analytics_http_admin_listen_port)

        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(url,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        self.log.debug("Api %s: %s" % (url, response))

        if not shell:
            shell.disconnect()

        for partition in response:
            if 'replicas' in partition and len(partition['replicas']) > 0:
                return partition['replicas']
        return []

    def get_replicas_info(self, shell=None):
        cc__metadata_replicas_info = []
        start_time = time.time()
        cc_node_id = None
        nodes = []
        while (not cc_node_id or not nodes) and start_time + 60 > time.time():
            nodes, cc_node_id, cc_config_url = \
                self.retrieve_nodes_config(shell)
        if cc_config_url:
            address, port = \
                self.retrieve_analyticsHttpAdminListen_address_port(
                    cc_config_url, shell)
            cc__metadata_replicas_info = \
                self.retrive_replica_from_storage_data(address, port, shell)

        return cc__metadata_replicas_info

    def get_num_partitions(self, shell=None):
        partitons = dict()
        nodes, cc_node_id, cc_config_url = \
            self.retrieve_nodes_config(shell=shell)
        for node in nodes:
            address, port = \
                self.retrieve_analyticsHttpAdminListen_address_port(
                    node['apiBase']+"/analytics/node/config", 
                    shell)
            partitons[node['nodeName']] = \
                self.retrieve_number_of_partitions(address, port, shell)

        return partitons

    def retrieve_number_of_partitions(self, analytics_http_admin_listen_addr,
                                      analytics_http_admin_listen_port,
                                      shell=None):
        url = "http://{0}:{1}/analytics/node/storage" \
            .format(analytics_http_admin_listen_addr,
                    analytics_http_admin_listen_port)

        if not shell:
            shell = RemoteMachineShellConnection(self.cbas_node)
        output, error = shell.execute_command(
            """curl -g -v {0} -u {1}:{2}"""
            .format(url,
                    self.cbas_node.rest_username,
                    self.cbas_node.rest_password))

        response = ""
        for line in output:
            response = response + line
        if response:
            response = json.loads(response)
        self.log.debug("Api %s: %s" % (url, response))

        if not shell:
            shell.disconnect()

        return len(response)

    def set_log_level_on_cbas(self, log_level_dict, timeout=120, username=None,
                              password=None):

        payload = ""
        for component, level in log_level_dict.items():
            payload += '{ "level": "' + level + '", "name": "' + component + '" },'
        payload = payload.rstrip(",")
        params = '{ "loggers": [ ' + payload + ' ] }'
        status, content, response = \
            self.cbas_helper.operation_log_level_on_cbas(
                method="PUT", params=params,
                logger_name=None,
                timeout=timeout, username=username,
                password=password)
        return status, content, response

    def set_specific_log_level_on_cbas(self, logger_name, log_level,
                                       timeout=120, username=None,
                                       password=None):
        status, content, response = \
            self.cbas_helper.operation_log_level_on_cbas(
                method="PUT", params=None,
                logger_name=logger_name, log_level=log_level,
                timeout=timeout, username=username,
                password=password)
        return status, content, response

    def get_log_level_on_cbas(self, timeout=120, username=None, password=None):
        status, content, response = \
            self.cbas_helper.operation_log_level_on_cbas(
                method="GET", params="",
                logger_name=None,
                timeout=timeout,
                username=username, password=password)
        return status, content, response

    def get_specific_cbas_log_level(self, logger_name, timeout=120,
                                    username=None, password=None):
        status, content, response = \
            self.cbas_helper.operation_log_level_on_cbas(
                method="GET", params=None,
                logger_name=logger_name,
                timeout=timeout,
                username=username, password=password)
        return status, content, response

    def delete_all_loggers_on_cbas(self, timeout=120, username=None,
                                   password=None):
        status, content, response = \
            self.cbas_helper.operation_log_level_on_cbas(
                method="DELETE", params="",
                logger_name=None,
                timeout=timeout,
                username=username, password=password)
        return status, content, response

    def delete_specific_cbas_log_level(self, logger_name, timeout=120,
                                       username=None, password=None):
        status, content, response = \
            self.cbas_helper.operation_log_level_on_cbas(
                method="DELETE", params=None,
                logger_name=logger_name,
                timeout=timeout,
                username=username, password=password)
        return status, content, response

    def update_config_on_cbas(self, config_name=None, config_value=None,
                              username=None, password=None):
        if config_name and config_value:
            params = '{"' + config_name + '":' + str(config_value) + '}'
        else:
            raise ValueError("Missing config name and/or config value")
        status, content, response = self.cbas_helper.operation_config_on_cbas(
            method="PUT", params=params,
            username=username, password=password)
        return status, content, response

    def fetch_config_on_cbas(self, username=None, password=None):
        status, content, response = self.cbas_helper.operation_config_on_cbas(
            method="GET", username=username, password=password)
        return status, content, response

    def fetch_cbas_stats(self, username=None, password=None):
        status, content, response = self.cbas_helper.fetch_cbas_stats(
            username=username, password=password)
        return status, content, response

    def log_concurrent_query_outcome(self, node_in_test, handles):
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell = RemoteMachineShellConnection(node_in_test)
        for handle in handles:
            status, hand = self.retrieve_request_status_using_handle(
                node_in_test, handle, shell)
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
            self, config_map=None, username=None, password=None):
        if config_map:
            params = json.dumps(config_map)
        else:
            raise ValueError("Missing config map")
        status, content, response = \
            self.cbas_helper.operation_service_parameters_configuration_cbas(
                method="PUT", params=params,
                username=username, password=password)
        return status, content, response

    def fetch_service_parameter_configuration_on_cbas(
            self, username=None, password=None):
        status, content, response = \
            self.cbas_helper.operation_service_parameters_configuration_cbas(
                method="GET", username=username, password=password)
        return status, content, response

    def update_node_parameter_configuration_on_cbas(
            self, config_map=None, username=None, password=None):
        if config_map:
            params = json.dumps(config_map)
        else:
            raise ValueError("Missing config map")
        status, content, response = \
            self.cbas_helper.operation_node_parameters_configuration_cbas(
                method="PUT", params=params,
                username=username, password=password)
        return status, content, response

    def fetch_node_parameter_configuration_on_cbas(self, username=None,
                                                   password=None):
        status, content, response = \
            self.cbas_helper.operation_node_parameters_configuration_cbas(
                method="GET", username=username, password=password)
        return status, content, response

    def restart_analytics_cluster_uri(self, username=None, password=None):
        status, content, response = \
            self.cbas_helper.restart_analytics_cluster_uri(username=username,
                                                           password=password)
        return status, content, response

    def restart_analytics_node_uri(self, node_id, port=8095,
                                   username=None, password=None):
        status, content, response = \
            self.cbas_helper.restart_analytics_node_uri(node_id, port,
                                                        username=username,
                                                        password=password)
        return status, content, response

    def fetch_bucket_state_on_cbas(self):
        status, content, response = \
            self.cbas_helper.fetch_bucket_state_on_cbas(method="GET",
                                                        username=None,
                                                        password=None)
        return status, content, response

    def fetch_pending_mutation_on_cbas_node(self, node_ip, port=9110,
                                            username=None, password=None):
        status, content, response = \
            self.cbas_helper.fetch_pending_mutation_on_cbas_node(
                node_ip, port, method="GET",
                username=username, password=password)
        return status, content, response

    def fetch_pending_mutation_on_cbas_cluster(self, port=9110,
                                               username=None, password=None):
        status, content, response = \
            self.cbas_helper.fetch_pending_mutation_on_cbas_cluster(
                port, method="GET", username=username, password=password)
        return status, content, response

    def fetch_dcp_state_on_cbas(self, data_set, data_verse="Default",
                                username=None, password=None):
        if not data_set:
            raise ValueError("dataset is required field")
        status, content, response = self.cbas_helper.fetch_dcp_state_on_cbas(
            data_set, method="GET", dataverse=data_verse,
            username=username, password=password)
        return status, content, response

    def get_analytics_diagnostics(self, cbas_node, timeout=120):
        response = self.cbas_helper.get_analytics_diagnostics(cbas_node,
                                                              timeout=timeout)
        return response

    def set_global_compression_type(self, compression_type="snappy",
                                    username=None, password=None):
        return self.cbas_helper.set_global_compression_type(compression_type,
                                                            username, password)

    def wait_for_cbas_to_recover(self, timeout=180):
        """
        Returns True if analytics service is recovered/available.
        False if service is unavailable despite waiting for specified "timeout" period.
        """
        analytics_recovered = False
        cluster_recover_start_time = time.time()
        while time.time() < cluster_recover_start_time + timeout:
            try:
                status, _, _, _, _ = self.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping()")
                if status == "success":
                    analytics_recovered = True
                    break
            except:
                sleep(2, "Service unavailable. Will retry..")
        return analytics_recovered

    # Backup Analytics metadata
    def backup_cbas_metadata(self, bucket_name='default',
                             username=None, password=None):
        response = self.cbas_helper.backup_cbas_metadata(bucket_name,
                                                         username=username,
                                                         password=password)
        return response.json()

    # Restore Analytics metadata
    def restore_cbas_metadata(self, metadata, bucket_name='default',
                              username=None, password=None):
        if metadata is None:
            raise ValueError("Missing metadata")
        response = self.cbas_helper.restore_cbas_metadata(metadata,
                                                          bucket_name,
                                                          username=username,
                                                          password=password)
        return response.json()
    
    def create_cbas_infra_from_spec(self, cbas_spec, local_bucket_util, remote_bucket_util=None):
        """
        Method creates CBAS infra based on the spec data.
        :param cbas_spec dict, spec to create CBAS infra.
        :param local_bucket_util bucket_util_obj, bucket util object of local cluster.
        :param remote_bucket_util bucket_util_obj, bucket util object of remote cluster.
        """
        if not self.create_dataverse_from_spec(cbas_spec):
            return False
        if not self.create_link_from_spec(cbas_spec):
            return False
        if not self.create_dataset_from_spec(cbas_spec, local_bucket_util, remote_bucket_util):
            return False
        
        jobs = Queue()
        results = list()
        
        def consumer_func(job):
            return job[0](**job[1])
        
        # Connect link only when remote links are present, Local link is connected by default and
        # external links are not required to be connected.
        remote_links = self.list_all_link_objs("couchbase")
        if len(remote_links) > 0:
            self.log.info("Connecting all remote Links")
            for link in remote_links:
                jobs.put((self.connect_link, {"link_name" : link.full_name}))
        
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if not all(results):
            return False
        results = []
        
        # Wait for data ingestion only for datasets based on either local KV source or remote KV source,
        internal_datasets = self.list_all_dataset_objs(dataset_source="internal")
        if len(internal_datasets) > 0:
            self.log.info("Waiting for data to be ingested into datasets")
            for dataset in internal_datasets:
                jobs.put((self.wait_for_ingestion_complete,
                          {"dataset_names":[dataset.full_name], "num_items":dataset.num_of_items}))
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if not all(results):
            return False
        
        self.log.info("Creating Synonyms based on CBAS Spec")
        if not self.create_synonym_from_spec(cbas_spec):
            return False
        
        self.log.info("Creating Indexes based on CBAS Spec")
        if not self.create_index_from_spec(cbas_spec):
            return False
        
        return True
    
    def delete_cbas_infra_created_from_spec(
            self, cbas_spec, 
            continue_if_index_drop_fail=True,
            continue_if_synonym_drop_fail=True,
            continue_if_dataset_drop_fail=True,
            continue_if_link_drop_fail=True,
            continue_if_dataverse_drop_fail=True,
            delete_dataverse_object=True):
        
        jobs = Queue()
        results = list()
        
        def consumer_func(job):
            retry = 0
            result = False
            while retry < 3 and not result:
                result = job[1](**job[2])
                retry += 1
            if not result:
                return job[0]
        
        def print_failures(name, list_of_failures):
            self.log.error("Failed to drop following {0} -".format(name))
            self.log.error(str(list_of_failures))
        
        self.log.info("Dropping all the indexes")
        for index in self.list_all_index_objs():
            jobs.put((index, self.drop_cbas_index,{"index_name":index.name, 
                                                   "dataset_name":index.full_dataset_name,
                                                   "analytics_index":index.analytics_index}))
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if any(results):
            if continue_if_index_drop_fail:
                print_failures("indexes", results)
                results = []
            else:
                return False
        
        self.log.info("Dropping all the synonyms")
        for synonym in self.list_all_synonym_objs():
            jobs.put((synonym, self.drop_analytics_synonym,{"synonym_full_name":synonym.full_name,
                                                            "if_exists":True}))
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if any(results):
            if continue_if_synonym_drop_fail:
                print_failures("synonyms", results)
                results = []
            else:
                return False
        
        self.log.info("Dropping all the Datasets")
        for dataset in self.list_all_dataset_objs():
            dataset_name = dataset.full_name
            if dataset.dataverse_name == "Default":
                dataset_name = dataset.name
            if isinstance(dataset, CBAS_Collection):
                jobs.put((dataset,self.drop_dataset,{"dataset_name":dataset_name,
                                                     "if_exists":True,
                                                     "analytics_collection":True}))
            else:
                jobs.put((dataset,self.drop_dataset,{"dataset_name":dataset_name,
                                                     "if_exists":True}))
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if any(results):
            if continue_if_dataset_drop_fail:
                print_failures("datasets", results)
                results = []
            else:
                return False
        
        self.log.info("Dropping all the Links")
        for link in self.list_all_link_objs():
            jobs.put((link, self.drop_link,{"link_name":link.full_name,
                                            "if_exists":True}))
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if any(results):
            if continue_if_link_drop_fail:
                print_failures("links", results)
                results = []
            else:
                return False
        
        self.log.info("Dropping all the Dataverses")
        for dataverse in self.dataverses.values():
            if dataverse.name != "Default":
                if isinstance(dataverse, CBAS_Scope):
                    jobs.put((dataverse,self.drop_dataverse,{"dataverse_name":dataverse.name,
                                                             "if_exists":True,
                                                             "analytics_scope":True,
                                                             "delete_dataverse_obj":delete_dataverse_object}))
                else:
                    jobs.put((dataverse,self.drop_dataverse,{"dataverse_name":dataverse.name,
                                                             "if_exists":True,
                                                             "delete_dataverse_obj":delete_dataverse_object}))
        self.run_jobs_in_parallel(consumer_func, jobs, results, cbas_spec["max_thread_count"], 
                                  async_run=False, consume_from_queue_func=None)
        if any(results):
            if continue_if_dataverse_drop_fail:
                print_failures("dataverses", results)
            else:
                return False
        return True