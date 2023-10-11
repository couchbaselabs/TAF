"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

from com.couchbase.client.java.analytics import AnalyticsOptions,\
    AnalyticsScanConsistency, AnalyticsStatus, AnalyticsMetrics
from com.couchbase.client.core.error import RequestCanceledException,\
    CouchbaseException, AmbiguousTimeoutException, PlanningFailureException,\
    UnambiguousTimeoutException, TimeoutException, DatasetExistsException,\
    IndexExistsException, CompilationFailureException
import java.time.Duration
import re
import json


class CBASHelper(object):

    def __init__(self, cluster):
        self.cluster = cluster
        self.sdk_client_pool = cluster.sdk_client_pool

    def execute_statement_on_cbas(
            self, statement, mode, pretty=True, timeout=70,
            client_context_id=None, username=None, password=None,
            analytics_timeout=120, time_out_unit="s",
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
        options.raw("timeout", str(analytics_timeout) + time_out_unit)

        output = {}
        try:
            result = client.cluster.analyticsQuery(statement, options)
            output["status"] = result.metaData().status()
            output["metrics"] = result.metaData().metrics()
            try:
                output["results"] = result.rowsAsObject()
            except:
                output["results"] = None

        except CompilationFailureException as err:
            output["errors"] = self.parse_error(err)["errors"]
            output["status"] = AnalyticsStatus.FATAL
        except CouchbaseException as err:
            output["errors"] = self.parse_error(err)["errors"]
            output["status"] = AnalyticsStatus.FATAL

        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            if output['status'] == AnalyticsStatus.FATAL:
                msg = output['errors'][0]['message']
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
        options.raw("timeout", str(analytics_timeout) + "s")

        for param in parameters:
            options.raw(param, parameters[param])

        output = {}
        try:
            result = client.cluster.analyticsQuery(statement, options)
            output["status"] = result.metaData().status()
            output["metrics"] = result.metaData().metrics()
            try:
                output["results"] = result.rowsAsObject()
            except:
                output["results"] = None

        except CompilationFailureException as err:
            output["errors"] = self.parse_error(err)["errors"]
            output["status"] = AnalyticsStatus.FATAL
        except CouchbaseException as err:
            output["errors"] = self.parse_error(err)["errors"]
            output["status"] = AnalyticsStatus.FATAL

        finally:
            self.sdk_client_pool.release_cluster_client(self.cluster, client)
            if output['status'] == AnalyticsStatus.FATAL:
                msg = output['errors'][0]['message']
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
        match = re.search(r"({.*?}.*)", error.getMessage())
        return json.loads(match.group(1))