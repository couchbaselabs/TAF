"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

from com.couchbase.client.java.analytics import AnalyticsOptions,\
    AnalyticsScanConsistency, AnalyticsStatus, AnalyticsMetrics
from com.couchbase.client.core.error import (
    RequestCanceledException, CouchbaseException, AmbiguousTimeoutException,
    PlanningFailureException,UnambiguousTimeoutException, TimeoutException,
    DatasetExistsException, IndexExistsException, CompilationFailureException,
    InvalidArgumentException, AuthenticationFailureException,
    LinkExistsException, DataverseNotFoundException)

from com.couchbase.client.java.manager.analytics import (
    AnalyticsIndexManager, CreateLinkAnalyticsOptions, GetLinksAnalyticsOptions)
from com.couchbase.client.java.manager.analytics.link import (
    AnalyticsLink, S3ExternalAnalyticsLink, CouchbaseRemoteAnalyticsLink,
    AnalyticsLinkType, RawExternalAnalyticsLink)
from com.couchbase.client.java.manager.analytics.link.CouchbaseRemoteAnalyticsLink import EncryptionLevel

from java.time import Duration

import re
import json
from global_vars import logger


class CBASHelper(object):

    def __init__(self, cluster):
        self.cluster = cluster
        self.sdk_client_pool = cluster.sdk_client_pool
        self.log = logger.get("infra")

    def execute_statement_on_cbas(
            self, statement, mode, pretty=True, timeout=300,
            client_context_id=None, username=None, password=None,
            analytics_timeout=300, time_out_unit="s",
            scan_consistency=None, scan_wait=None):

        client = self.sdk_client_pool.get_cluster_client(self.cluster)

        options = AnalyticsOptions.analyticsOptions()
        options.readonly(False)

        if scan_consistency and scan_consistency != "not_bounded":
            options.scanConsistency(AnalyticsScanConsistency.REQUEST_PLUS)
        else:
            options.scanConsistency(AnalyticsScanConsistency.NOT_BOUNDED)

        if client_context_id:
            options.clientContextId(client_context_id)

        if scan_wait:
            options.scanWait(Duration.ofSeconds(scan_wait))

        if mode:
            options.raw("mode", mode)

        options.raw("pretty", pretty)
        #options.raw("timeout", str(analytics_timeout) + time_out_unit)

        output = {}
        try:
            result = client.cluster.analyticsQuery(statement, options)
            output["status"] = result.metaData().status()
            output["metrics"] = result.metaData().metrics()
            try:
                output["results"] = [row.toMap() for row in
                                     result.rowsAsObject()]
            except:
                output["results"] = None

        except CompilationFailureException as err:
            output["errors"] = self.parse_error(err)
            output["status"] = AnalyticsStatus.FATAL
        except CouchbaseException as err:
            output["errors"] = self.parse_error(err)
            output["status"] = AnalyticsStatus.FATAL
        except Exception as err:
            self.log.error(str(err))
            output["errors"] = self.parse_error(err)
            output["status"] = AnalyticsStatus.FATAL

        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            if output['status'] == AnalyticsStatus.FATAL:
                msg = output['errors']['msg']
                if "Job requirement" in msg and "exceeds capacity" in msg:
                    raise Exception("Capacity cannot meet job requirement")
            elif output['status'] == AnalyticsStatus.SUCCESS:
                output["errors"] = None
            else:
                raise Exception("Analytics Service API failed")

        self.generate_response_object(output)
        return output

    def execute_parameter_statement_on_cbas(
            self, statement, mode, pretty=True, timeout=70,
            client_context_id=None, username=None, password=None,
            analytics_timeout=120, parameters={}):

        client = self.sdk_client_pool.get_cluster_client(self.cluster).cluster

        options = AnalyticsOptions.analyticsOptions()
        options.readonly(False)
        options.scanConsistency(AnalyticsScanConsistency.NOT_BOUNDED)

        if client_context_id:
            options.clientContextId(client_context_id)

        if mode:
            options.raw("mode", mode)

        options.raw("pretty", pretty)
        #options.raw("timeout", str(analytics_timeout) + "s")

        for param in parameters:
            options.raw(param, parameters[param])

        output = {}
        try:
            result = client.cluster.analyticsQuery(statement, options)
            output["status"] = result.metaData().status()
            output["metrics"] = result.metaData().metrics()
            try:
                output["results"] = [row.toMap() for row in
                                     result.rowsAsObject()]
            except:
                output["results"] = None

        except CompilationFailureException as err:
            output["errors"] = self.parse_error(err)
            output["status"] = AnalyticsStatus.FATAL
        except CouchbaseException as err:
            output["errors"] = self.parse_error(err)
            output["status"] = AnalyticsStatus.FATAL
        except Exception as err:
            self.log.error(str(err))
            output["errors"] = self.parse_error(err)
            output["status"] = AnalyticsStatus.FATAL

        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            if output['status'] == AnalyticsStatus.FATAL:
                msg = output['errors']['msg']
                if "Job requirement" in msg and "exceeds capacity" in msg:
                    raise Exception("Capacity cannot meet job requirement")
            elif output['status'] == AnalyticsStatus.SUCCESS:
                output["errors"] = None
            else:
                raise Exception("Analytics Service API failed")

        self.generate_response_object(output)
        return output

    def generate_response_object(self, output):
        for key, value in output.iteritems():
            if key == "metrics" and isinstance(value, AnalyticsMetrics):
                match = re.search(r'{raw=(.*?})', value.toString())
                output["metrics"] = json.loads(match.group(1))
            elif key == "status":
                output["status"] = value.toString().lower()

    def parse_error(self, error):
        errors = {"msg": "", "code": ""}
        try:
            result = error.context().exportAsMap()
            result = result["errors"][0]
            errors["msg"] = result["message"]
            errors["code"] = result["code"]
        except:
            try:
                errors["msg"] = error.getMessage()
            except:
                errors["msg"] = error
        return errors

    def create_remote_link_obj(self, link_properties):
        try:
            remote_link_obj = CouchbaseRemoteAnalyticsLink(link_properties["name"], link_properties["dataverse"])
            remote_link_obj.encryption(EncryptionLevel.FULL if link_properties["encryption"] == "full" else EncryptionLevel.FULL)
            remote_link_obj.hostname(link_properties["hostname"])
            remote_link_obj.username(link_properties["username"])
            remote_link_obj.password(link_properties["password"])
            remote_link_obj.certificate(link_properties["certificate"])
            self.log.debug("Remote Link SDK object - {0}".format(remote_link_obj.toMap()))
            return remote_link_obj
        except Exception as err:
            self.log.error(str(err))
            return None

    def create_s3_link_obj(self, link_properties):
        try:
            s3_link_obj = S3ExternalAnalyticsLink(
                link_properties["name"], link_properties["dataverse"])
            s3_link_obj.accessKeyId(link_properties["accessKeyId"])
            s3_link_obj.secretAccessKey(link_properties["secretAccessKey"])
            s3_link_obj.region(link_properties["region"])
            s3_link_obj.serviceEndpoint(link_properties.get("serviceEndpoint",
                                                            None))
            s3_link_obj.sessionToken(link_properties.get("sessionToken", None))
            self.log.debug("S3 SDK object - {0}".format(s3_link_obj.toMap()))
            return s3_link_obj

        except Exception as err:
            self.log.error(str(err))
            return None

    def create_link(self, link_properties={}, timeout=300):
        status = False
        errors = {"msg": "", "code": ""}
        content = None

        if link_properties["type"] == "s3":
            link_obj = self.create_s3_link_obj(link_properties)
            if not link_obj:
                return status, content, [{
                    "msg": "S3ExternalAnalyticsLink object creation failed",
                    "code": ""}]
        else:
            link_obj = self.create_remote_link_obj(link_properties)
            if not link_obj:
                return status, content, [{
                    "msg": "RemoteLinkObject object creation failed",
                    "code": ""}]
        client = self.sdk_client_pool.get_cluster_client(self.cluster)
        manager = AnalyticsIndexManager(client.cluster)

        try:
            link_options = CreateLinkAnalyticsOptions.createLinkAnalyticsOptions()
            link_options.timeout(Duration.ofSeconds(timeout))
            manager.createLink(link_obj, link_options)
            status = True
            errors = []
        except InvalidArgumentException as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = "Some link arguements are invalid"
            errors = [errors]
        except AuthenticationFailureException as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = "Authentication failure while creating link"
            errors = [errors]
        except LinkExistsException as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = "Link {0}.{1} already exists".format(
                link_properties["dataverse"], link_properties["name"])
            errors = [errors]
        except CouchbaseException as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = str(err)
            errors = [errors]
        except Exception as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = str(err)
            errors = [errors]
        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            return status, content, errors

    def get_link_info(self, dataverse_name=None, link_name=None,
                      link_type=None):
        status = False
        errors = {"msg": "", "code": ""}
        content = []

        client = self.sdk_client_pool.get_cluster_client(self.cluster)
        manager = AnalyticsIndexManager(client.cluster)

        try:
            get_link_options = (
                GetLinksAnalyticsOptions.getLinksAnalyticsOptions())
            if dataverse_name:
                get_link_options.dataverseName(dataverse_name)
            if link_name:
                get_link_options.name(link_name)
            if link_type:
                get_link_options.linkType(AnalyticsLinkType.of(link_type))
            result = manager.getLinks(get_link_options)
            for link in result:
                if isinstance(link, S3ExternalAnalyticsLink) or isinstance(
                        link, CouchbaseRemoteAnalyticsLink):
                    content.append(dict(link.toMap()))
                elif isinstance(link, RawExternalAnalyticsLink):
                    content.append(json.loads(link.json()))
            status = True
            errors = []
        except DataverseNotFoundException as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = "Dataverse {0} does not exists".format(dataverse_name)
            errors = [errors]
        except CouchbaseException as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = str(err)
            errors = [errors]
        except Exception as err:
            self.log.error(str(err))
            status = False
            errors["msg"] = str(err)
            errors = [errors]
        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            return status, content, errors
