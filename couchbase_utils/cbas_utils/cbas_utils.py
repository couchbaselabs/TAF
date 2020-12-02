"""
Created on Nov 15, 2017

@author: riteshagarwal
"""

import json
import threading
import time
from threading import Thread
import string,random
import urllib

from CbasLib.CBASOperations import CBASHelper
from common_lib import sleep
from global_vars import logger
from remote.remote_util import RemoteMachineShellConnection



class CbasUtil:
    def __init__(self, master, cbas_node, server_task=None):
        self.log = logger.get("test")
        self.cbas_node = cbas_node
        self.master = master
        self.task = server_task
        self.cbas_helper = CBASHelper(master, cbas_node)

    def createConn(self, bucket, username=None, password=None):
        self.cbas_helper.createConn(bucket, username, password)

    def closeConn(self):
        self.cbas_helper.closeConn()

    def execute_statement_on_cbas_util(self, statement, mode=None, rest=None,
                                       timeout=120, client_context_id=None,
                                       username=None, password=None,
                                       analytics_timeout=120, time_out_unit="s",
                                       scan_consistency=None, scan_wait=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
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
                                                 rest=None, timeout=120,
                                                 client_context_id=None,
                                                 username=None, password=None,
                                                 analytics_timeout=120,
                                                 parameters=[]):
        """
        Executes a statement on CBAS using the REST API using REST Client
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

    def create_dataverse_on_cbas(self, dataverse_name=None,
                                 username=None,
                                 password=None,
                                 validate_error_msg=False,
                                 expected_error=None,
                                 expected_error_code=None):

        if dataverse_name == "Default" or dataverse_name is None:
            cmd = "create dataverse Default;"
        else:
            cmd = "create dataverse %s" % dataverse_name

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_dataverse_on_cbas(self, dataverse_name=None,
                               username=None,
                               password=None,
                               validate_error_msg=False,
                               expected_error=None,
                               expected_error_code=None):
        if dataverse_name:
            cmd = "drop dataverse %s;" % dataverse_name
        else:
            cmd = "drop dataverse Default;"

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_link_on_cbas(self, link_name=None,
                            ip_address=None,
                            username=None,
                            password=None,
                            validate_error_msg=False,
                            expected_error=None,
                            expected_error_code=None):

        if link_name == "Local" or link_name is None:
            cmd = "create link Local"
        else:
            cmd = "create link %s WITH {'nodes': %s, 'user': %s, 'password': %s}"\
                  % (link_name, ip_address, username, password)

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_external_link_on_cbas(self, link_properties, username=None, password=None, timeout=120,
                                     validate_error_msg=False, expected_error=None, expected_error_code=None):
        """
        Create an external link to AWS S3 service or an external couchbase server.
        :param username : used for authentication while calling API.
        :param password : used for authentication while calling API.
        :param timeout : timeout for API response
        :param validate_error_msg : boolean, If set to true, it will compare the error raised while creating the link
        with the error msg or error code passed.
        :param expected_error : str, expected error string
        :param expected_error_code: str, expected error code
        :param link_properties: dict, contains all the properties required to create a link.
        Common for both AWS and couchbase link.
        <Required> name : name of the link to be created.
        <Required> dataverse : name of the dataverse under which the link has to be created.
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
        """
        params = dict()
        for key, value in link_properties.iteritems():
            if key == "dataverse":
                key = "scope"
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
        :param username : used for authentication while calling API.
        :param password : used for authentication while calling API.
        :param timeout : timeout for API response
        :param validate_error_msg : boolean, If set to true, it will compare the error raised while creating the link
        with the error msg or error code passed.
        :param expected_error : str, expected error string
        :param expected_error_code: str, expected error code
        :param link_properties: dict, contains all the properties required to create a link.
        Common for both AWS and couchbase link.
        <Required> name : name of the link to be created.
        <Required> dataverse : name of the dataverse under which the link has to be created.
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
        """
        params = dict()
        for key, value in link_properties.iteritems():
            if key == "dataverse":
                key = "scope"
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

    def drop_link_on_cbas(self, link_name=None,
                          username=None,
                          password=None,
                          validate_error_msg=False,
                          expected_error=None,
                          expected_error_code=None):
        if link_name:
            cmd = "drop link %s;" % link_name
        else:
            cmd = "drop link Local;"

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket(self, cbas_bucket_name, cbas_dataset_name,
                                 where_field=None, where_value = None,
                                 validate_error_msg=False, username = None,
                                 password = None, expected_error=None, dataverse=None, compress_dataset=False,
                                 link_name=None, timeout=120, analytics_timeout=120):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        if '`' not in cbas_bucket_name:
            cbas_bucket_name = "`"+cbas_bucket_name+"`"

        cmd_create_dataset = "create dataset {0} ".format(cbas_dataset_name)
        if compress_dataset:
            cmd_create_dataset = cmd_create_dataset + "with {'storage-block-compression': {'scheme': 'snappy'}} "

        cmd_create_dataset = cmd_create_dataset + "on {0} ".format(cbas_bucket_name)
        
        if link_name:
            cmd_create_dataset += "at {0} ".format(link_name)

        if where_field and where_value:
            cmd_create_dataset = cmd_create_dataset + "WHERE `{0}`=\"{1}\";".format(where_field, where_value)
        else:
            cmd_create_dataset = cmd_create_dataset + ";"

            #cmd_create_dataset = "create dataset {0} on {1};".format(
        #    cbas_dataset_name, cbas_bucket_name)
        #if where_field and where_value:
        #    cmd_create_dataset = "create dataset {0} on {1} WHERE `{2}`=\"{3}\";".format(
        #        cbas_dataset_name, cbas_bucket_name, where_field, where_value)

        if dataverse is not None:
            dataverse_prefix = 'use ' + dataverse + ';\n'
            cmd_create_dataset = dataverse_prefix + cmd_create_dataset

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_create_dataset, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_bucket_merge_policy(
            self, cbas_bucket_name, cbas_dataset_name,
            where_field=None,
            where_value=None,
            validate_error_msg=False,
            username=None,
            password=None,
            expected_error=None,
            merge_policy="no-merge",
            max_mergable_component_size=16384,
            max_tolerance_component_count=2):
        """
        Creates a shadow dataset on a CBAS bucket
        """
        if merge_policy == "no-merge":
            cmd_create_dataset = 'create dataset %s with { "merge-policy": {"name": "%s"}} on %s;'\
            % (cbas_dataset_name, merge_policy, cbas_bucket_name)
            if where_field and where_value:
                cmd_create_dataset = 'create dataset %s with { "merge-policy": {"name": "%s" }} on %s WHERE `%s`=\"%s\";'%(
                    cbas_dataset_name, merge_policy, cbas_bucket_name, where_field, where_value)
        else:
            cmd_create_dataset = 'create dataset %s with { "merge-policy": {"name": "%s", "parameters": {"max-mergable-component-size": %s, "max-tolerance-component-count": %s}}} on %s;'\
            %(cbas_dataset_name, merge_policy, max_mergable_component_size, max_tolerance_component_count, cbas_bucket_name)
            if where_field and where_value:
                cmd_create_dataset = 'create dataset %s with { "merge-policy": {"name": "%s", "parameters": {"max-mergable-component-size": %s,"max-tolerance-component-count": %s}}} on %s WHERE `%s`=\"%s\";'%(
                                        cbas_dataset_name, merge_policy,
                                        max_mergable_component_size, max_tolerance_component_count,
                                        cbas_bucket_name, where_field, where_value)

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd_create_dataset,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def create_dataset_on_external_resource(self, cbas_dataset_name, aws_bucket_name, link_name,
                                            object_construction_def=None, dataverse="Default",
                                            path_on_aws_bucket=None, file_format="json", redact_warning=None,
                                            header=None, null_string=None, include=None, exclude=None,
                                            validate_error_msg=False, username = None,
                                            password = None, expected_error=None, expected_error_code=None,
                                            links_dataverse="Default"):
        """
        Creates a dataset for an external resource like AWS S3 bucket. 
        Note - No shadow dataset is created for this type of external datasets.
        :param cbas_dataset_name (str) : Name for the dataset to be created.
        :param aws_bucket_name (str): AWS S3 bucket to which this dataset is to be linked. S3 bucket should be in 
        the same region as the link, that is used to create this dataset.
        :param link_name (str): external link to AWS S3
        :param object_construction_def (str): It defines how the data read will be parsed. 
        Required only for csv and tsv formats.
        :param dataverse (str): Name of the dataverse where dataset is to be created.
        :param path_on_aws_bucket (str): Relative path in S3 bucket, from where the files will be read.
        :param file_format (str): Type of files to read. Valid values - json, csv and tsv
        :param redact_warning (bool): internal information like e.g. filenames are redacted from warning messages.
        :param header (bool): True means every csv, tsv file has a header record at the top and 
        the expected behaviour is that the first record (the header) is skipped.
        False means every csv, tsv file does not have a header.
        :param null_string (str): a string that represents the NULL value if the field accepts NULLs.
        :param validate_error_msg (bool): validate errors that occur while creating dataset.
        :param username (str):
        :param password (str):
        :param expected_error (str):
        :param expected_error_code (str):
        :return True/False
        
        """
        if dataverse != "Default":
            cmd_create_dataset = "CREATE EXTERNAL DATASET {0}.{1}".format(
                dataverse, cbas_dataset_name)
        else:
            cmd_create_dataset = "CREATE EXTERNAL DATASET {0}".format(cbas_dataset_name)

        if object_construction_def:
            cmd_create_dataset += "({0})".format(object_construction_def)

        if links_dataverse != "Default":
            cmd_create_dataset += " ON `{0}` AT {1}.{2}".format(aws_bucket_name, links_dataverse, link_name)
        else:
            cmd_create_dataset += " ON `{0}` AT {1}".format(aws_bucket_name, link_name)

        if path_on_aws_bucket is not None:
            cmd_create_dataset += " USING \"{0}\"".format(path_on_aws_bucket)

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

        cmd_create_dataset += " WITH {0};".format(json.dumps(with_parameters))

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_create_dataset, username=username, password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error, expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def connect_link(self, link_name="Local",
                     validate_error_msg=False,
                     with_force=False,
                     username=None,
                     password=None,
                     expected_error=None,
                     expected_error_code=None,
                     timeout=120,
                     analytics_timeout=120):
        """
        Connects to a Link
        """
        cmd_connect_bucket = "connect link %s" % link_name

        if with_force is True:
            cmd_connect_bucket += " with {'force':true}"

        retry_attempt = 5
        connect_bucket_failed = True
        while connect_bucket_failed and retry_attempt > 0:
            status, metrics, errors, results, _ = \
                self.execute_statement_on_cbas_util(cmd_connect_bucket,
                                                    username=username,
                                                    password=password, 
                                                    timeout=timeout, 
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
                    connect_bucket_failed = False
            else:
                connect_bucket_failed = False
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def disconnect_link(self, link_name="Local",
                        disconnect_if_connected=False,
                        validate_error_msg=False,
                        username=None,
                        password=None,
                        expected_error=None,
                        expected_error_code=None):
        """
        Disconnects from a CBAS bucket
        """
        if disconnect_if_connected:
            cmd_disconnect_link = 'disconnect link %s if connected;' \
                                  % link_name
        else:
            cmd_disconnect_link = 'disconnect link %s;' % link_name

        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd_disconnect_link,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def connect_to_bucket(self, cbas_bucket_name=None, cb_bucket_password=None,
                          validate_error_msg=False,
                          cb_bucket_username="Administrator",
                          username=None, password=None, expected_error=None):
        """
        Connects to a CBAS bucket
        """
        cmd_connect_bucket = "connect link Local;"

        retry_attempt = 5
        connect_bucket_failed = True
        while connect_bucket_failed and retry_attempt > 0:
            status, metrics, errors, results, _ = \
                self.execute_statement_on_cbas_util(cmd_connect_bucket,
                                                    username=username,
                                                    password=password)

            if errors:
                # Below errors are to be fixed in Alice, until they are fixed retry is only option
                actual_error = errors[0]["msg"]
                if "Failover response The vbucket belongs to another server" \
                        in actual_error \
                        or "Bucket configuration doesn't contain a vbucket map" in actual_error:
                    retry_attempt -= 1
                    self.log.debug("Retrying connecting of bucket")
                    sleep(10)
                else:
                    self.log.debug("Not a vbucket error, so don't retry")
                    connect_bucket_failed = False
            else:
                connect_bucket_failed = False
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def disconnect_from_bucket(self, cbas_bucket_name=None,
                               disconnect_if_connected=False,
                               validate_error_msg=False, username=None,
                               password=None, expected_error=None):
        """
        Disconnects from a CBAS bucket
        """

        cmd_disconnect_bucket = 'disconnect link Local;'
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd_disconnect_bucket,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_dataset(self, cbas_dataset_name, dataverse="Default", validate_error_msg=False,
                     username=None, password=None, expected_error=None):
        """
        Drop dataset from CBAS
        """
        if dataverse == "Default":
            cmd_drop_dataset = "drop dataset {0};".format(cbas_dataset_name)
        else:
            cmd_drop_dataset = "drop dataset {0}.{1};".format(dataverse, cbas_dataset_name)
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd_drop_dataset,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_cbas_bucket(self, cbas_bucket_name, validate_error_msg=False,
                         username=None, password=None, expected_error=None):
        """
        Drop a CBAS bucket
        """
        return True

    def wait_for_ingestion_complete(self, cbas_dataset_names, num_items,
                                    timeout=300):

        total_items = 0
        for ds_name in cbas_dataset_names:
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
                for ds_name in cbas_dataset_names:
                    total_items += self.get_num_items_in_cbas_dataset(ds_name)[0]
                counter += 2

        return False

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

    def validate_error_in_response(self, status, errors, expected_error=None,
                                   expected_error_code=None):
        """
        Validates the error response against the expected one.
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

    def verify_index_created(self, index_name, index_fields, dataset):
        result = True

        statement = "select * from Metadata.`Index` where DatasetName='{0}' and IsPrimary=False"\
                    .format(dataset)
        status, metrics, errors, content, _ = \
            self.execute_statement_on_cbas_util(statement)
        if status != "success":
            result = False
            self.log.debug("Index not created. Metadata query status = %s",
                           status)
        else:
            index_found = False
            for index in content:
                if index["Index"]["IndexName"] == index_name:
                    index_found = True
                    field_names = []
                    for index_field in index_fields:
                        if "meta()".lower() in index_field:
                            field_names.append(index_field.split(".")[1])
                        else:
                            field_names.append(str(index_field.split(":")[0]))
                            field_names.sort()
                    self.log.debug(field_names)
                    actual_field_names = []
                    for index_field in index["Index"]["SearchKey"]:
                        if type(index_field) is list:
                            index_field = ".".join(index_field)
                        actual_field_names.append(str(index_field))
                        actual_field_names.sort()

                    actual_field_names.sort()
                    self.log.debug(actual_field_names)
                    if field_names != actual_field_names:
                        result = False
                        self.log.debug("Index fields not correct")
                    break
            result &= index_found
        return result, content

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

    def fetch_config_on_cbas(self, config_name=None, config_value=None,
                             username=None, password=None):
        status, content, response = \
            self.cbas_helper.operation_config_on_cbas(method="GET",
                                                      username=username,
                                                      password=password)
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

    def update_service_parameter_configuration_on_cbas(self, config_map=None,
                                                       username=None,
                                                       password=None):
        if config_map:
            params = json.dumps(config_map)
        else:
            raise ValueError("Missing config map")
        status, content, response = \
            self.cbas_helper.operation_service_parameters_configuration_cbas(
                method="PUT", params=params,
                username=username, password=password)
        return status, content, response

    def fetch_service_parameter_configuration_on_cbas(self, username=None,
                                                      password=None):
        status, content, response = \
            self.cbas_helper.operation_service_parameters_configuration_cbas(
                method="GET", username=username, password=password)
        return status, content, response

    def update_node_parameter_configuration_on_cbas(self, config_map=None,
                                                    username=None,
                                                    password=None):
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

    def get_ds_compression_type(self, ds):
        query = "select raw BlockLevelStorageCompression.DatasetCompressionScheme from Metadata.`Dataset` where DatasetName='{0}';".format(ds)
        _, _, _, ds_compression_type, _ = self.execute_statement_on_cbas_util(query)
        ds_compression_type = ds_compression_type[0]
        if ds_compression_type is not None:
            ds_compression_type = ds_compression_type.encode('ascii', 'ignore')
        self.log.info("Compression Type for Dataset {0} is {1}".format(ds, ds_compression_type))

        return ds_compression_type
    
    def validate_dataset_in_metadata(self, dataset_name, dataverse, 
                                     username=None, password=None, **kwargs):
        """
        validates metadata information about a dataset with entry in Metadata.Dataset collection.
        """
        self.log.debug("Validating dataset entry in Metadata")
        cmd = 'SELECT value MD FROM Metadata.`Dataset` as MD WHERE DatasetName="{0}" \
        and DataverseName = "{1}";'.format(dataset_name, dataverse)
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password)
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
    
    def validate_dataverse_in_metadata(self, dataverse, username=None, password=None):
        """
        Validates whether a dataverse is present in Metadata.Dataverse
        :param dataverse : str, Name of the dataverse, which has to be validated.
        :param username : str
        :param password : str
        :return boolean
        """
        self.log.debug("Validating dataverse entry in Metadata")
        cmd = "select value dv from Metadata.`Dataverse` as dv where\
         dv.DataverseName = \"{0}\";".format(dataverse)
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password)
        if status == "success":
            if results:
                return True
            else:
                return False
        else:
            return False
    
    def create_analytics_scope(self, scope_name=None,
                               username=None, password=None,
                               validate_error_msg=False,
                               expected_error=None,
                               expected_error_code=None):
        """
        Creates analytics scope. This method is synthetic sugar for creating
        dataverse in analytics.
        :param scope_name: str, name of the scope to be created. Supports 1-part and 2-part names.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised 
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :return boolean
        """

        if not scope_name:
            cmd = "create analytics scope Default;"
        else:
            cmd = "create analytics scope %s" % scope_name
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True

    def drop_analytics_scope(self, scope_name=None,
                             username=None, password=None,
                             validate_error_msg=False,
                             expected_error=None,
                             expected_error_code=None):
        """
        Creates analytics scope. This method is synthetic sugar for creating
        dataverse in analytics.
        :param scope_name: str, name of the scope to be created. Supports 1-part and 2-part names.
        :param username: str
        :param password: str
        :param validate_error_msg: boolean, if set to true, then validate error raised 
        with expected error msg and code.
        :param expected_error: str
        :param expected_error_code: str
        :return boolean
        """
        if scope_name:
            cmd = "drop analytics scope %s;" % scope_name
        else:
            cmd = "drop analytics scope Default;"
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error,
                                                   expected_error_code)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def create_analytics_collection(
            self, KV_entity_name, cbas_collection_name, where_field=None, 
            where_value = None, validate_error_msg=False, expected_error=None, 
            dataverse=None, compress_dataset=False, link_name=None,
            username = None, password = None, timeout=120, analytics_timeout=120):
        """
        Creates a analytics collection which is syntactic sugar for creating datasets.
        :param KV_bucket_name: string, name of the KV entity on which dataset is to be created.
        Can be bucket name or collection name.
        :param cbas_collection_name: string, name of the analytics collection to be created.
        :param where field: string, name of the filters to be applied while creating cbas collections.
        :param where_value: string, value of the filters to be applied while creating cbas collections.
        :param validating_error_msg: boolean, if True, then validate error occurred during 
        cbas collection creation.
        :param expected_error: string, error msg to be validated.
        :param dataverse: string, dataverse name under which the cbas collection needs to be created.
        :param compress_cbas_collection: boolean
        :param link_name: string, name of the link to create cbas collection on.
        :param username: string,
        :param password: string,
        :param timeout: int,
        :param analytics_timeout: int,
        """

        cmd_create_dataset = "create analytics collection {0} ".format(cbas_collection_name)
        if compress_dataset:
            cmd_create_dataset = cmd_create_dataset + "with {'storage-block-compression': {'scheme': 'snappy'}} "

        cmd_create_dataset = cmd_create_dataset + "on {0} ".format(KV_entity_name)
        
        if link_name:
            cmd_create_dataset = cmd_create_dataset + "at {0} ".format(link_name)

        if where_field and where_value:
            cmd_create_dataset = cmd_create_dataset + "WHERE `{0}`=\"{1}\";".format(where_field, where_value)
        else:
            cmd_create_dataset = cmd_create_dataset + ";"

        if dataverse is not None:
            dataverse_prefix = 'use ' + dataverse + ';\n'
            cmd_create_dataset = dataverse_prefix + cmd_create_dataset
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd_create_dataset))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd_create_dataset, username=username, password=password,timeout=timeout, 
            analytics_timeout=analytics_timeout)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
    def drop_analytics_collection(self, cbas_collection_name, 
                                  dataverse="Default", validate_error_msg=False, 
                                  expected_error=None, username=None, password=None):
        """
        Drop a analytics collection which is syntactic sugar for dropping datasets.
        :param cbas_collection_name: string, name of the analytics collection to be created.
        :param dataverse: string, dataverse name under which the cbas collection needs to be created.
        :param validating_error_msg: boolean, if True, then validate error occurred during 
        cbas collection creation.
        :param expected_error: string, error msg to be validated.
        :param username: string,
        :param password: string,
        """
        if dataverse == "Default":
            cmd_drop_dataset = "drop analytics collection {0};".format(cbas_collection_name)
        else:
            cmd_drop_dataset = "drop analytics collection {0}.{1};".format(dataverse, cbas_collection_name)
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd_drop_dataset))
        status, metrics, errors, results, _ = \
            self.execute_statement_on_cbas_util(cmd_drop_dataset,
                                                username=username,
                                                password=password)
        if validate_error_msg:
            return self.validate_error_in_response(status, errors,
                                                   expected_error)
        else:
            if status != "success":
                return False
            else:
                return True
    
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
    
    def validate_synonym_in_metadata(self, synonym, synonym_dataverse,
                                     object_dataverse, object_name, 
                                     username=None, password=None):
        """
        Validates whether an entry for Synonym is present in Metadata.Synonym.
        :param synonym : str, Name of the Synonym, which has to be validated.
        :param synonym_dataverse : str, Name of the dataverse under which the synonym is created
        :param object_dataverse : str, Name of the object's dataverse on which the synonym was created.
        :param object_name : str, Name of the object on which the synonym was created.
        :param username : str
        :param password : str
        :return boolean
        """
        self.log.debug("Validating Synonym entry in Metadata")
        cmd = "select value sy from Metadata.`Synonym` as sy where \
        sy.SynonymName = \"{0}\" and sy.DataverseName = \"{1}\";".format(
            synonym,synonym_dataverse)
        
        self.log.debug("Executing cmd - \n{0}\n".format(cmd))
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_util(
            cmd, username=username, password=password)
        if status == "success":
            for result in results:
                if result['ObjectDataverseName'] == object_dataverse and result['ObjectName'] == object_name:
                    return True
            return False
        else:
            return False
    
    def create_analytics_synonym(self, synonym_name, object_name,
                                 synonym_dataverse=None, object_dataverse=None,
                                 if_not_exists=False,
                                 validate_error_msg=False, expected_error=None, 
                                 username=None, password=None,
                                 timeout=120, analytics_timeout=120):
        """
        Creates an analytics synonym on a dataset.
        :param synonym_name : str, Name of the Synonym, which has to be created.
        :param object_name : str, Name of the dataset on which the synonym is to be created.
        :param synonym_dataverse : str, Name of the dataverse under which the synonym is to be created
        :param object_dataverse : str, Name of the dataset's dataverse on which the synonym is to be created.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        """
        cmd = "create analytics synonym "
        
        if synonym_dataverse:
            cmd += "{0}.".format(synonym_dataverse)
        
        cmd += "{0} ".format(synonym_name)
        
        if if_not_exists:
            cmd += "If Not Exists "
        
        cmd += "for "
        
        if object_dataverse:
            cmd += "{0}.".format(object_dataverse)
        
        cmd += "{0};".format(object_name)
        
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
    
    def drop_analytics_synonym(self, synonym_name, synonym_dataverse=None, 
                               validate_error_msg=False, expected_error=None, 
                               username=None, password=None,
                               timeout=120, analytics_timeout=120):
        """
        Drop an analytics synonym.
        :param synonym_name : str, Name of the Synonym, which has to be dropped.
        :param synonym_dataverse : str, Name of the dataverse under which the synonym is present.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param username : str
        :param password : str
        :param timeout : str
        :param analytics_timeout : str
        """
        
        cmd = "drop analytics synonym "
        
        if synonym_dataverse:
            cmd += "{0}.".format(synonym_dataverse)
        
        cmd += "{0};".format(synonym_name)
        
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
    
    def validate_synonym_doc_count(self, full_synonym_name, full_dataset_name,
                                   validate_error_msg=False, expected_error=None,
                                   timeout=300, analytics_timeout=300):
        """
        Validate that doc count in synonym is same as the dataset on which it was created.
        :param full_synonym_name : str, Name of the Synonym
        :param full_dataset_name : str, Name of the dataset on which the synonym was created.
        :param validate_error_msg : boolean, validate error while creating synonym
        :param expected_error : str, error msg
        :param timeout : int
        :param analytics_timeout : int
        """
        
        cmd_get_num_items = "select count(*) from %s;"

        synonym_status, metrics, errors, synonym_results, _ = \
            self.execute_statement_on_cbas_util(
                cmd_get_num_items % full_synonym_name,
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
                               .format(full_synonym_name, synonym_results[0]['$1']))
                synonym_results = synonym_results[0]['$1']
    
            dataset_status, metrics, errors, dataset_results, _ = \
                self.execute_statement_on_cbas_util(
                    cmd_get_num_items % full_dataset_name,
                    timeout=timeout,
                    analytics_timeout=analytics_timeout)
            if dataset_status != "success":
                self.log.error("Querying dataset failed")
                dataset_results = 0
            else:
                self.log.debug("No. of items in dataset {0} : {1}"
                               .format(full_dataset_name, dataset_results[0]['$1']))
                dataset_results = dataset_results[0]['$1']
            
            if dataset_results != synonym_results:
                return False
            return True
        
            
class Dataset:
    """
    This class has helper methods to create datasets and synonyms on randomly select 
    KV buckets or collections.
    """
    
    def __init__(self, 
                 bucket_util,
                 cbas_util,
                 consider_default_KV_scope=True, 
                 consider_default_KV_collection=True,
                 dataset_name_cardinality=1,
                 bucket_cardinality=1,
                 random_dataset_name=True,
                 exclude_bucket=[],
                 exclude_scope=[],
                 exclude_collection=[],
                 set_kv_entity=True
                 ):
        """
        :param bucket_util: object, object of BucketUtil class.
        :param cbas_util: object, object of CBASUtil class.
        :param consider_default_KV_scope: boolean, if set to False, skips
        _default KV scope of the selected bucket while choosing collection to
        create dataset on.
        :param consider_default_KV_collection: boolean, if set to False, skips
        _default KV collection of the selected bucket while choosing collection to
        create dataset on.
        :param dataset_name_cardinality: int, number of parts in dataset name. 
        Valid values are 1,2,3
        :param bucket_cardinality: int, number of parts in KV entity name. 
        Valid values are 1,2,3
        :param random_dataset_name: boolean, if True, create a random dataset name based
        on dataset_name_cardinality, otherwise it is set to KV entity name.
        """
        self.log = logger.get("test")
        self.bucket_util = bucket_util
        self.cbas_util = cbas_util
        self.bucket_cardinality = bucket_cardinality
        if set_kv_entity:
            exclude_scope = list()
            exclude_collection = list()
            if not consider_default_KV_scope:
                exclude_scope.append("_default")
            if not consider_default_KV_collection:
                exclude_collection.append("_default")
            self.set_kv_entity(*self.get_random_kv_entity(
                self.bucket_util, self.bucket_cardinality, 
                exclude_bucket,exclude_scope,exclude_collection))
        
        if random_dataset_name:
            self.full_dataset_name = Dataset.format_name(
                self.create_name_with_cardinality(dataset_name_cardinality))
        else:
            self.full_dataset_name = self.get_fully_quantified_kv_entity_name(dataset_name_cardinality)
        
        self.dataverse, self.name = self.split_dataverse_dataset_name(self.full_dataset_name,
                                                                      strip_back_qoutes=True)
        
    def set_kv_entity(self, kv_bucket_obj=None, kv_scope_obj=None, kv_collection_obj=None):
        """
        Set dataset object's properties.
        :param kv_bucket_obj: KV bucket object
        :param kv_scope_obj: KV scope object
        :param kv_collection_obj: KV collection object
        """
        if kv_bucket_obj:
            self.kv_bucket_obj = kv_bucket_obj
        if kv_scope_obj:
            self.kv_scope_obj = kv_scope_obj
        if kv_collection_obj:
            self.kv_collection_obj = kv_collection_obj
    
    def set_dataset_name(self, dataset_name):
        """
        Set dataset object's name property
        """
        self.name = dataset_name
    
    def set_dataverse_name(self, dataverse_name):
        """
        Set dataset object's dataverse property
        """
        self.dataverse = dataverse_name
    
    def set_bucket_cardinality(self, bucket_cardinality):
        """
        Set dataset object's bucket_cardinality property
        """
        self.bucket_cardinality = bucket_cardinality
    
    @staticmethod
    def split_dataverse_dataset_name(full_dataset_name, strip_back_qoutes=False):
        """
        This method splits multipart dataset name into dataverse and dataset name.
        :param full_dataset_name: string, multipart dataset name
        :param strip_back_qoutes: boolean, strips "`" from each part of dataset name 
        """
        full_dataset_name_split = full_dataset_name.split(".")
        if strip_back_qoutes:
            full_dataset_name_split = [x.strip("`") for x in full_dataset_name_split]
        if len(full_dataset_name_split) > 1:
            dataset_name = full_dataset_name_split[-1]
            dataverse_name = ".".join(full_dataset_name_split[:-1])
        else:
            dataset_name = full_dataset_name_split[0]
            dataverse_name = "Default"
        return dataverse_name, dataset_name
        
    @staticmethod
    def format_name(*args):
        """
        This method encloses each part of the name separated by "." with "`"
        """
        return '.'.join(('`' +  _ + '`') for name in args for _ in name.split("."))
    
    @staticmethod
    def format_name_for_error(check_for_special_char_in_name=False,
                              *args):
        full_name = list()
        for name in args:
            for _ in name.split("."):
                _ = _.strip("`")
                if _[0].isdigit() or (
                    check_for_special_char_in_name and "-" in _):
                    full_name.append("`{0}`".format(_))
                else:
                    full_name.append(_)
        return '.'.join(full_name)
    
    def get_fully_quantified_dataset_name(self):
        """
        Returns fully quantified dataset name. 
        """
        if self.dataverse in "Default":
            return Dataset.format_name(self.name)
        else:
            return Dataset.format_name(self.dataverse, self.name)
    
    def get_fully_quantified_dataverse_name(self):
        """
        Returns fully quantified dataverse name to which dataset belongs.
        """
        return Dataset.format_name(self.dataverse)
    
    def get_fully_quantified_kv_entity_name(self, cardinality=1):
        """
        Returns fully quantified KV collection or bucket name.
        """
        if cardinality == 1:
            return Dataset.format_name(self.kv_bucket_obj.name)
        elif cardinality == 2:
            return Dataset.format_name(self.kv_bucket_obj.name, 
                                       self.kv_scope_obj.name)
        elif cardinality == 3:
            return Dataset.format_name(self.kv_bucket_obj.name, 
                                       self.kv_scope_obj.name, 
                                       self.kv_collection_obj.name)
    
    @staticmethod
    def get_random_kv_entity(bucket_util,
                             bucket_cardinality=1,
                             exclude_bucket=[],
                             exclude_scope=[],
                             exclude_collection=[]):
        """
        Returns KV entity name based on cardinality.
        :param cardinality: int, accepted values are - 1,2,3
        :return bucket, scope and collection object.
        """
        bucket = random.choice(bucket_util.buckets)
        while bucket.name in exclude_bucket:
            bucket = random.choice(bucket_util.buckets)
        if bucket_cardinality == 1:
            scope = bucket_util.get_scope_obj(bucket, "_default")
            collection = bucket_util.get_collection_obj(scope, "_default")
        else:
            scope = random.choice(
                bucket_util.get_active_scopes(bucket))
            while scope.name in exclude_scope:
                scope = random.choice(bucket_util.get_active_scopes(bucket))
            collection = random.choice(bucket_util.get_active_collections(
                bucket, scope.name))
            while collection.name in exclude_collection:
                collection = random.choice(bucket_util.get_active_collections(
                    bucket, scope.name))
        return bucket, scope, collection
    
    @staticmethod
    def create_name_with_cardinality(name_cardinality=0, max_length=30, 
                                     fixed_length=False):
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
        """
        if name_cardinality == 0:
            return "Default"
        elif name_cardinality == 4:
            return "Metadata"
        elif name_cardinality > 4:
            return None
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
    
    @staticmethod
    def get_item_count_in_collection(bucket_util, bucket, 
                                     scope_name="_default", 
                                     collection_name="_default"):
        """
        This is a temporary method, use collection stat when 
        https://issues.couchbase.com/browse/MB-38392 is resolved.
        """
        for collection in bucket_util.get_active_collections(
            bucket, scope_name):
            if collection.name == collection_name:
                return collection.num_items
        return 0
    
    def setup_dataset(self, dataset_creation_method="cbas_dataset",
                      validate_metadata=True,
                      validate_doc_count=True,
                      create_dataverse=True, 
                      validate_error=False, error_msg=None,
                      compress_dataset=False,
                      username=None, password=None,
                      timeout=120, analytics_timeout=120,
                      link_name=None): 
        
        if self.dataverse and self.dataverse != "Default" and create_dataverse:
            self.log.info("Creating Dataverse {0}".format(self.dataverse))
            if not self.cbas_util.create_dataverse_on_cbas(
                dataverse_name=self.get_fully_quantified_dataverse_name()):
                self.log.error("Creation of Dataverse {0} failed".format(self.dataverse))
                return False
        
        if dataset_creation_method == "cbas_collection":
            self.log.info("Creating analytics collection {0}".format(self.full_dataset_name))
            if not self.cbas_util.create_analytics_collection(
                KV_entity_name=self.get_fully_quantified_kv_entity_name(self.bucket_cardinality), 
                cbas_collection_name=self.get_fully_quantified_dataset_name(),
                validate_error_msg=validate_error, 
                expected_error=error_msg,
                username=username, password=password,
                compress_dataset=compress_dataset,
                timeout=timeout, analytics_timeout=analytics_timeout,link_name=link_name):
                self.log.error("Error creating analytics collection {0}".format(self.full_dataset_name))
                return False
        elif dataset_creation_method == "cbas_dataset":
            self.log.info("Creating dataset {0}".format(self.full_dataset_name))
            if not self.cbas_util.create_dataset_on_bucket(
                self.get_fully_quantified_kv_entity_name(self.bucket_cardinality), 
                self.get_fully_quantified_dataset_name(),
                validate_error_msg=validate_error, 
                expected_error=error_msg,
                username=username, password=password,
                compress_dataset=compress_dataset,
                timeout=timeout, analytics_timeout=analytics_timeout,
                link_name=link_name):
                self.log.error("Error creating dataset {0}".format(self.full_dataset_name))
                return False
        elif dataset_creation_method == "enable_cbas_from_kv":
            self.log.info("Enabling Analytics on {0}".format(
                self.get_fully_quantified_kv_entity_name(self.bucket_cardinality)))
            self.full_dataset_name = self.get_fully_quantified_kv_entity_name(3)
            self.dataverse, self.name = self.split_dataverse_dataset_name(self.full_dataset_name,
                                                                          True)
            if not self.cbas_util.enable_analytics_from_KV(
                self.get_fully_quantified_kv_entity_name(self.bucket_cardinality),
                compress_dataset=compress_dataset, 
                validate_error_msg=validate_error, 
                expected_error=error_msg, username=username, password=password,
                timeout=timeout, analytics_timeout=analytics_timeout):
                self.log.error("Error enabling analytics on {0}".format(self.full_dataset_name))
                return False
        
        if not validate_error:
            if validate_metadata:
                self.log.info("Validating Metadata entries")
                if self.bucket_cardinality == 1 and dataset_creation_method != "enable_cbas_from_kv":
                    if not self.cbas_util.validate_dataset_in_metadata(
                        self.name, self.dataverse, BucketName=self.kv_bucket_obj.name):
                        return False
                else:
                    if not self.cbas_util.validate_dataset_in_metadata(
                        self.name, self.dataverse, BucketName=self.kv_bucket_obj.name,
                        ScopeName=self.kv_scope_obj.name, 
                        CollectionName=self.kv_collection_obj.name):
                        return False
            
            if validate_doc_count:
                self.log.info("Validating item count")
                if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.get_fully_quantified_dataset_name(),
                    self.get_item_count_in_collection(
                        self.bucket_util,self.kv_bucket_obj, 
                        self.kv_scope_obj.name, self.kv_collection_obj.name)):
                        return False
        
        return True
    
    def teardown_dataset(self, dataset_drop_method = "cbas_collection",
                         validate_error=False, error_msg=None,
                         validate_metadata=True,
                         username=None, password=None
                         ):
        if dataset_drop_method == "cbas_collection":
            self.log.info("Dropping analytics collection {0}".format(self.full_dataset_name))
            if not self.cbas_util.drop_analytics_collection( 
                cbas_collection_name=self.get_fully_quantified_dataset_name(),
                validate_error_msg=validate_error, 
                expected_error=error_msg,
                username=username, password=password):
                self.log.error("Error Dropping analytics collection {0}".format(self.full_dataset_name))
                return False
        elif dataset_drop_method == "cbas_dataset":
            self.log.info("Dropping dataset {0}".format(self.full_dataset_name))
            if not self.cbas_util.drop_dataset(
                cbas_dataset_name=self.get_fully_quantified_dataset_name(),
                validate_error_msg=validate_error,
                username=username, password=password, expected_error=error_msg):
                self.log.error("Error dropping dataset {0}".format(self.full_dataset_name))
                return False
        elif dataset_drop_method == "disable_cbas_from_kv":
            self.full_dataset_name = self.get_fully_quantified_kv_entity_name(3)
            self.dataverse, self.name = self.split_dataverse_dataset_name(self.full_dataset_name,True)
            self.log.info("Disabling Analytics on {0}".format(self.full_dataset_name))
            if not self.cbas_util.disable_analytics_from_KV(
                kv_entity_name=self.full_dataset_name,
                validate_error_msg=validate_error, 
                expected_error=error_msg, username=username, password=password):
                self.log.error("Error disabling analytics on {0}".format(self.full_dataset_name))
                return False
        
        if (not validate_error) and validate_metadata:
            self.log.info("Validating Metadata entries")
            if self.cbas_util.validate_dataset_in_metadata(
                self.name, self.dataverse, 
                BucketName=self.get_fully_quantified_kv_entity_name(1)):
                self.log.error("Dropped dataset entry still present in Metadata")
                return False
        return True
    
    def setup_synonym(self, new_synonym_name=False,
                      synonym_dataverse="Default",
                      validate_error_msg=False,
                      expected_error=None,
                      validate_metadata=True,
                      validate_doc_count=True,
                      if_not_exists=False):
        
        if new_synonym_name:
            self.synonym_name = self.create_name_with_cardinality(1)
        else:
            self.synonym_name = self.name
            
        if synonym_dataverse == "dataset":
            self.synonym_dataverse = self.dataverse
        elif synonym_dataverse == "new":
            self.synonym_dataverse = self.create_name_with_cardinality(2)
            # create dataverse if it does not exists.
            if not self.cbas_util.validate_dataverse_in_metadata(
                self.synonym_dataverse) and not \
                self.cbas_util.create_dataverse_on_cbas(
                dataverse_name=self.format_name(self.synonym_dataverse)):
                self.log.error("Creation of Dataverse {0} failed".format(
                    self.synonym_dataverse))
                return False
        elif synonym_dataverse == "Default":
            self.synonym_dataverse = "Default"
            
        self.log.info("Creating synonym")
        if not self.cbas_util.create_analytics_synonym(
            synonym_name=self.format_name(self.synonym_name), 
            object_name=self.full_dataset_name,
            synonym_dataverse=self.format_name(self.synonym_dataverse), 
            validate_error_msg=validate_error_msg, 
            expected_error=expected_error,
            if_not_exists=if_not_exists):
            self.log.error("Error while creating synonym {0} on dataset {1}".format(
                self.synonym_name, self.full_dataset_name))
            return False
        
        if not validate_error_msg:
            if validate_metadata:
                self.log.info("Validating created Synonym entry in Metadata")
                if not self.cbas_util.validate_synonym_in_metadata(
                    synonym=self.synonym_name,
                    synonym_dataverse=self.synonym_dataverse,
                    object_dataverse=self.dataverse, object_name=self.name):
                    self.log.error("Synonym metadata entry not created")
                    return False
            
            if validate_doc_count:
                self.log.info(
                    "Validating whether querying synonym return expected result")
                if not self.cbas_util.validate_synonym_doc_count(
                    full_synonym_name=self.format_name(
                        self.synonym_dataverse,self.synonym_name), 
                    full_dataset_name=self.full_dataset_name):
                    self.log.error(
                        "Doc count in Synonym does not match with dataset on which it was created.")
                    return False
        return True
        