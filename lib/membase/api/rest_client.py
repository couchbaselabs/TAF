
import base64
import json
import urllib
import httplib2
import traceback
import socket
import time
import uuid
from copy import deepcopy
from threading import Thread

from TestInput import TestInputSingleton
from BucketLib.bucket import Bucket
from Cb_constants import constants
from common_lib import sleep
from global_vars import logger
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, CBAS_QUOTA
from testconstants import COUCHBASE_FROM_VERSION_4, IS_CONTAINER
from exception import \
    InvalidArgumentException, \
    ServerAlreadyJoinedException, \
    ServerUnavailableException
from membase.api.exception import \
    AddNodeException, \
    BucketFlushFailed, \
    CBRecoveryFailedException, \
    CompactViewFailed, \
    DesignDocCreationException, \
    FailoverFailedException, \
    QueryViewException, \
    RebalanceFailedException,\
    ReadDocumentException,\
    ServerSelfJoinException, \
    SetRecoveryTypeFailed, \
    SetViewInfoNotFound, \
    XDCRException


class RestHelper(object):
    def __init__(self, rest_connection):
        self.rest = rest_connection
        self.test_log = logger.get("test")

    def is_ns_server_running(self, timeout_in_seconds=360):
        self.test_log.debug(
            "Checking if ns_server is running with timeout {0}secs"
            .format(timeout_in_seconds))
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            try:
                status = self.rest.get_nodes_self(5)
                if status is not None and status.status == 'healthy':
                    return True
                else:
                    if status is not None:
                        self.test_log.warn("Server {0}:{1} status is {2}"
                                           .format(self.rest.ip,
                                                   self.rest.port,
                                                   status.status))
                    else:
                        self.test_log.warn("Server {0}:{1} status is down"
                                           .format(self.rest.ip,
                                                   self.rest.port))
            except ServerUnavailableException:
                self.test_log.error("Server {0}:{1} is unavailable"
                                    .format(self.rest.ip, self.rest.port))
            # Wait before next retry
            sleep(2)
        msg = 'Unable to connect to the node {0} even after waiting {1} secs'
        self.test_log.fatal(msg.format(self.rest.ip, timeout_in_seconds))
        return False

    def is_cluster_healthy(self, timeout=120):
        # get the nodes and verify that all the nodes.status are healthy
        nodes = self.rest.node_statuses(timeout)
        return all(node.status == 'healthy' for node in nodes)

    def rebalance_reached(self, percentage=100, wait_step=2):
        start = time.time()
        progress = 0
        previous_progress = 0
        retry = 0
        while progress is not -1 and progress < percentage and retry < 40:
            # -1 is error , -100 means could not retrieve progress
            progress = self.rest._rebalance_progress()
            if progress == -100:
                self.test_log.error("Unable to retrieve rebalance progress. "
                                    "Retrying..")
                retry += 1
            else:
                if previous_progress == progress:
                    retry += 0.5
                else:
                    retry = 0
                    previous_progress = progress
            # Wait before fetching rebalance progress
            sleep(wait_step)
        if progress <= 0:
            self.test_log.error("Rebalance progress: {0}".format(progress))

            return False
        elif retry >= 40:
            self.test_log.error("Rebalance stuck at {0}%".format(progress))
            return False
        else:
            duration = time.time() - start
            self.test_log.info('Rebalance reached >{0}% in {1} seconds '
                               .format(progress, duration))
            return True

    # return true if cluster balanced, false if it needs rebalance
    def is_cluster_rebalanced(self):
        command = "ns_orchestrator:needs_rebalance()"
        status, content = self.rest.diag_eval(command)
        if status:
            return content.lower() == "false"
        self.test_log.critical("Can't define if cluster balanced")
        return None

    # this method will rebalance the cluster by passing the remote_node as
    # ejected node
    def remove_nodes(self, knownNodes, ejectedNodes, wait_for_rebalance=True):
        self.test_log.debug("Ejecting nodes '{0}' from cluster"
                            .format(ejectedNodes))
        if len(ejectedNodes) == 0:
            return False
        self.rest.rebalance(knownNodes, ejectedNodes)
        if wait_for_rebalance:
            return self.rest.monitorRebalance()
        else:
            return False

    def wait_for_node_status(self, node, expected_status, timeout_in_seconds):
        status_reached = False
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time and not status_reached:
            nodes = self.rest.node_statuses()
            for n in nodes:
                if node.id == n.id:
                    self.test_log.debug('Node {0} status : {1}'
                                        .format(node.id, n.status))
                    if n.status.lower() == expected_status.lower():
                        status_reached = True
                    break
            if not status_reached:
                self.test_log.debug("Wait before reading the node.status")
                sleep(5)
        self.test_log.debug('Node {0} status_reached: {1}'
                            .format(node.id, status_reached))
        return status_reached

    def _wait_for_task_pid(self, pid, end_time, ddoc_name):
        while time.time() < end_time:
            new_pid, _ = self.rest._get_indexer_task_pid(ddoc_name)
            if pid == new_pid:
                # Sleep before fetching task_pid in retry logic
                sleep(5)
                continue
            else:
                return

    def _wait_for_indexer_ddoc(self, servers, ddoc_name, timeout=300):
        nodes = self.rest.get_nodes()
        servers_to_check = []
        for node in nodes:
            for server in servers:
                if node.ip == server.ip and str(node.port) == str(server.port):
                    servers_to_check.append(server)
        for server in servers_to_check:
            try:
                rest = RestConnection(server)
                self.test_log.debug('Check index for ddoc %s , server %s'
                                    % (ddoc_name, server.ip))
                end_time = time.time() + timeout
                self.test_log.debug('Start getting index for ddoc %s, server %s'
                                    % (ddoc_name, server.ip))
                old_pid, is_pid_blocked = rest._get_indexer_task_pid(ddoc_name)
                if not old_pid:
                    self.test_log.info('Index for ddoc %s is not going on, server %s'
                                       % (ddoc_name, server.ip))
                    continue
                while is_pid_blocked:
                    self.test_log.debug('Index for ddoc %s is blocked, server %s'
                                        % (ddoc_name, server.ip))
                    self._wait_for_task_pid(old_pid, end_time, ddoc_name)
                    old_pid, is_pid_blocked = rest._get_indexer_task_pid(ddoc_name)
                    if time.time() > end_time:
                        self.test_log.error(
                            "Index is still BLOCKED node %s ddoc % pid %"
                            % (server, ddoc_name, old_pid))
                        break
                if old_pid:
                    self.test_log.debug('Index for ddoc %s is running, server %s'
                                        % (ddoc_name, server.ip))
                    self._wait_for_task_pid(old_pid, end_time, ddoc_name)
            except Exception, ex:
                self.test_log.error(
                    "Unable to check index on server %s because of %s"
                    % (server.ip, str(ex)))


class RestConnection(object):
    def __init__(self, serverInfo):
        self.log = logger.get("infra")
        self.test_log = logger.get("test")

        self.index_port = constants.index_port
        self.fts_port = constants.fts_port
        self.query_port = constants.n1ql_port
        self.eventing_port = constants.eventing_port

        # serverInfo can be a json object/dictionary
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            if "index_port" in serverInfo.keys():
                self.index_port = serverInfo["index_port"]
            if "fts_port" in serverInfo.keys():
                if serverInfo['fts_port']:
                    self.fts_port = serverInfo["fts_port"]
            if "eventing_port" in serverInfo.keys():
                if serverInfo['eventing_port']:
                    self.eventing_port = serverInfo["eventing_port"]
            self.hostname = ''
            self.services = ''
            if "hostname" in serverInfo:
                self.hostname = serverInfo["hostname"]
            if "services" in serverInfo:
                self.services = serverInfo["services"]
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port
            self.hostname = ''
            self.services = "kv"
            if hasattr(serverInfo, "services"):
                self.services = serverInfo.services
            if hasattr(serverInfo, 'index_port'):
                self.index_port = serverInfo.index_port
            if hasattr(serverInfo, 'query_port'):
                self.query_port = serverInfo.query_port
            if hasattr(serverInfo, 'fts_port'):
                if serverInfo.fts_port:
                    self.fts_port = serverInfo.fts_port
            if hasattr(serverInfo, 'eventing_port'):
                if serverInfo.eventing_port:
                    self.eventing_port = serverInfo.eventing_port
            if hasattr(serverInfo, 'hostname') and serverInfo.hostname and\
               serverInfo.hostname.find(self.ip) == -1:
                self.hostname = serverInfo.hostname
            if hasattr(serverInfo, 'services'):
                self.services = serverInfo.services

        self.node_services = list()
        self.input = TestInputSingleton.input
        if self.input is not None:
            """ from watson, services param order and format:
                new_services=fts-kv-index-n1ql """
            self.services_node_init = self.input.param("new_services", None)

        url_format = "http://%s:%s/"
        url_host = self.ip
        if self.hostname:
            url_host = self.hostname

        self.baseUrl = url_format % (url_host, self.port)
        self.fts_baseUrl = url_format % (url_host, self.fts_port)
        self.index_baseUrl = url_format % (url_host, self.index_port)
        self.query_baseUrl = url_format % (url_host, self.query_port)
        self.capi_baseUrl = url_format % (url_host, constants.capi_port)
        self.eventing_baseUrl = url_format % (url_host, self.eventing_port)

        # for Node is unknown to this cluster error
        for iteration in xrange(5):
            http_res, success = self.init_http_request(self.baseUrl +
                                                       'nodes/self')
            if not success and type(http_res) == unicode and\
               (http_res.find('Node is unknown to this cluster') > -1 or
                    http_res.find('Unexpected server error, request logged') > -1):
                self.test_log.error(
                    "Error {0} was gotten, 5 seconds sleep before retry"
                    .format(http_res))
                sleep(5)
                if iteration == 2:
                    self.test_log.error("Node {0}:{1} is in a broken state!"
                                        .format(self.ip, self.port))
                    raise ServerUnavailableException(self.ip)
                continue
            else:
                break
        # determine the real couchApiBase for cluster_run
        # couchApiBase appeared in version 2.*
        if not http_res or http_res["version"][0:2] == "1.":
            self.capi_baseUrl = self.baseUrl + "/couchBase"
        else:
            for iteration in xrange(5):
                if "couchApiBase" not in http_res.keys():
                    if self.is_cluster_mixed():
                        self.capi_baseUrl = self.baseUrl + "/couchBase"
                        return
                    # Sleep before next retry
                    sleep(0.5)
                    http_res, success = self.init_http_request(self.baseUrl + 'nodes/self')
                else:
                    self.capi_baseUrl = http_res["couchApiBase"]
                    return
            raise ServerUnavailableException("couchApiBase doesn't exist in nodes/self: %s" % http_res)

    def sasl_streaming_rq(self, bucket, timeout=120):
        api = self.baseUrl + 'pools/default/bucketsStreaming/{0}'.format(bucket)
        if isinstance(bucket, Bucket):
            api = self.baseUrl + 'pools/default/bucketsStreaming/{0}'.format(bucket.name)
        try:
            httplib2.Http(timeout=timeout).request(api, 'GET', '',
                                                   headers=self._create_capi_headers())
        except Exception, ex:
            self.test_log.warn('Exception while streaming: %s' % str(ex))

    def open_sasl_streaming_connection(self, bucket, timeout=1000):
        self.test_log.info("Opening sasl streaming connection for bucket %s"
                           % (bucket, bucket.name)[isinstance(bucket, Bucket)])
        t = Thread(target=self.sasl_streaming_rq,
                   name="streaming_" + str(uuid.uuid4())[:4],
                   args=(bucket, timeout))
        try:
            t.start()
        except Exception:
            self.log.warn("Thread is not started")
            return None
        return t

    def is_cluster_mixed(self):
            http_res, success = self.init_http_request(self.baseUrl + 'pools/default')
            if http_res == u'unknown pool':
                return False
            try:
                versions = list(set([node["version"][:1] for node in http_res["nodes"]]))
            except Exception:
                self.test_log.error('Error while processing cluster info {0}'
                                    .format(http_res))
                # not really clear what to return but False see to be a good start until we figure what is happening
                return False

            if '1' in versions and '2' in versions:
                return True
            return False

    def is_cluster_compat_mode_greater_than(self, version):
        """
        curl -v -X POST -u Administrator:welcome http://10.3.4.186:8091/diag/eval
        -d 'cluster_compat_mode:get_compat_version().'
        Returns : [3,2] if version = 3.2.0
        """
        status, content = self.diag_eval('cluster_compat_mode:get_compat_version().')
        if status:
            json_parsed = json.loads(content)
            cluster_ver = float("%s.%s" % (json_parsed[0], json_parsed[1]))
            if cluster_ver > version:
                return True
        return False

    def is_enterprise_edition(self):
        http_res, success = self.init_http_request(self.baseUrl + 'pools/default')
        if http_res == u'unknown pool':
            return False
        editions = []
        community_nodes = []
        """
        get the last word in node["version"] as in "version": "2.5.1-1073-rel-enterprise"
        """
        for node in http_res["nodes"]:
            editions.extend(node["version"].split("-")[-1:])
            if "community" in node["version"].split("-")[-1:]:
                community_nodes.extend(node["hostname"].split(":")[:1])
        if "community" in editions:
            self.test_log.error("IP(s) for node(s) with community edition {0}"
                                .format(community_nodes))
            return False
        return True

    def init_http_request(self, api):
        content = None
        try:
            status, content, header = self._http_request(api, 'GET',
                                                         headers=self._create_capi_headers())
            json_parsed = json.loads(content)
            if status:
                return json_parsed, True
            else:
                print("{0} with status {1}: {2}".format(api, status, json_parsed))
                return json_parsed, False
        except ValueError as e:
            if content is not None:
                print("{0}: {1}".format(api, content))
            else:
                print e
            return content, False

    def rename_node(self, hostname, username='Administrator',
                    password='password'):
        params = urllib.urlencode({'username': username,
                                   'password': password,
                                   'hostname': hostname})

        api = "%snode/controller/rename" % self.baseUrl
        status, content, header = self._http_request(api, 'POST', params)
        return status, content

    def ns_server_tasks(self, task_type=None):
        api = self.baseUrl + 'pools/default/tasks'
        json_parsed = ""
        try:
            status, content, header = self._http_request(
                api,
                'GET',
                headers=self._create_headers())
            json_parsed = json.loads(content)
            if task_type is not None:
                for t_content in json_parsed:
                    if t_content["type"] == task_type:
                        json_parsed = t_content
        except ValueError:
            pass
        return json_parsed

    # DEPRECATED: use create_ddoc() instead.
    def create_view(self, design_doc_name, bucket_name, views, options=None):
        return self.create_ddoc(design_doc_name, bucket_name, views, options)

    def create_ddoc(self, design_doc_name, bucket, views, options=None):
        design_doc = DesignDocument(design_doc_name, views, options=options)
        if design_doc.name.find('/') != -1:
            design_doc.name = design_doc.name.replace('/', '%2f')
            design_doc.id = '_design/{0}'.format(design_doc.name)
        return self.create_design_document(bucket, design_doc)

    def create_design_document(self, bucket, design_doc):
        design_doc_name = design_doc.id
        api = '%s/%s/%s' % (self.capi_baseUrl, bucket, design_doc_name)
        if isinstance(bucket, Bucket):
            api = '%s/%s/%s' % (self.capi_baseUrl, bucket.name, design_doc_name)

        status, content, header = self._http_request(api, 'PUT', str(design_doc),
                                                     headers=self._create_capi_headers())
        if not status:
            raise DesignDocCreationException(design_doc_name, content)
        return json.loads(content)

    def is_index_triggered(self, ddoc_name, index_type='main'):
        run, block = self._get_indexer_task_pid(ddoc_name, index_type=index_type)
        if run or block:
            return True
        else:
            return False

    def _get_indexer_task_pid(self, ddoc_name, index_type='main'):
        active_tasks = self.ns_server_tasks()
        if u'error' in active_tasks:
            return None
        if active_tasks:
            for task in active_tasks:
                if task['type'] == 'indexer' and task['indexer_type'] == index_type:
                    for ddoc in task['design_documents']:
                        if ddoc == ('_design/%s' % ddoc_name):
                            return task['pid'], False
                if task['type'] == 'blocked_indexer' and task['indexer_type'] == index_type:
                    for ddoc in task['design_documents']:
                        if ddoc == ('_design/%s' % ddoc_name):
                            return task['pid'], True
        return None, None

    def query_view(self, design_doc_name, view_name, bucket, query, timeout=120, invalid_query=False, type="view"):
        status, content, header = self._query(design_doc_name, view_name, bucket, type, query, timeout)
        if not status and not invalid_query:
            stat = 0
            if 'status' in header:
                stat = int(header['status'])
            raise QueryViewException(view_name, content, status=stat)
        return json.loads(content)

    def _query(self, design_doc_name, view_name, bucket, view_type, query, timeout):
        if design_doc_name.find('/') != -1:
            design_doc_name = design_doc_name.replace('/', '%2f')
        if view_name.find('/') != -1:
            view_name = view_name.replace('/', '%2f')
        api = self.capi_baseUrl + '%s/_design/%s/_%s/%s?%s' % (bucket,
                                               design_doc_name, view_type,
                                               view_name,
                                               urllib.urlencode(query))
        if isinstance(bucket, Bucket):
            api = self.capi_baseUrl + '%s/_design/%s/_%s/%s?%s' % (bucket.name,
                                                  design_doc_name, view_type,
                                                  view_name,
                                                  urllib.urlencode(query))
        self.test_log.info("Index query url: {0}".format(api))
        status, content, header = self._http_request(api, headers=self._create_capi_headers(),
                                                     timeout=timeout)
        return status, content, header

    def view_results(self, bucket, ddoc_name, params, limit=100, timeout=120,
                     view_name=None):
        status, json = self._index_results(bucket, "view", ddoc_name, params, limit, timeout=timeout, view_name=view_name)
        if not status:
            raise Exception("unable to obtain view results")
        return json

    # DEPRECATED: Incorrectly named function kept for backwards compatibility.
    def get_view(self, bucket, view):
        self.test_log.info("DEPRECATED function get_view(" + view + "). use get_ddoc()")
        return self.get_ddoc(bucket, view)

    def get_data_path(self):
        node_info = self.get_nodes_self()
        data_path = node_info.storage[0].get_data_path()
        return data_path

    def get_memcached_port(self):
        node_info = self.get_nodes_self()
        return node_info.memcached

    def get_ddoc(self, bucket, ddoc_name):
        status, json, meta = self._get_design_doc(bucket, ddoc_name)
        if not status:
            raise ReadDocumentException(ddoc_name, json)
        return json, meta

    def run_view(self, bucket, view, name):
        api = self.capi_baseUrl + '/%s/_design/%s/_view/%s' % (bucket, view, name)
        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to create view")
        return json_parsed

    def delete_view(self, bucket, view):
        status, json = self._delete_design_doc(bucket, view)
        if not status:
            raise Exception("unable to delete the view")
        return json

    def spatial_results(self, bucket, spatial, params, limit=100):
        status, json = self._index_results(bucket, "spatial", spatial,
                                           params, limit)
        if not status:
            raise Exception("unable to obtain spatial view results")
        return json

    def create_spatial(self, bucket, spatial, function):
        status, json = self._create_design_doc(bucket, spatial, function)
        if status == False:
            raise Exception("unable to create spatial view")
        return json

    def get_spatial(self, bucket, spatial):
        status, json, meta = self._get_design_doc(bucket, spatial)
        if not status:
            raise Exception("unable to get the spatial view definition")
        return json, meta

    def delete_spatial(self, bucket, spatial):
        status, json = self._delete_design_doc(bucket, spatial)
        if not status:
            raise Exception("unable to delete the spatial view")
        return json

    # type_ is "view" or "spatial"
    def _index_results(self, bucket, type_, ddoc_name, params, limit, timeout=120,
                       view_name=None):
        if view_name is None:
            view_name = ddoc_name
        query = '/{0}/_design/{1}/_{2}/{3}'
        api = self.capi_baseUrl + query.format(bucket, ddoc_name, type_, view_name)

        num_params = 0
        if limit != None:
            num_params = 1
            api += "?limit={0}".format(limit)
        for param in params:
            if num_params > 0:
                api += "&"
            else:
                api += "?"
            num_params += 1

            if param in ["key", "startkey", "endkey", "start_range",
                         "end_range"] or isinstance(params[param], bool):
                api += "{0}={1}".format(param,
                                        json.dumps(params[param],
                                                   separators=(',', ':')))
            else:
                api += "{0}={1}".format(param, params[param])

        self.test_log.info("Index query url: {0}".format(api))
        status, content, header = self._http_request(api, headers=self._create_capi_headers(), timeout=timeout)
        json_parsed = json.loads(content)
        return status, json_parsed

    def get_couch_doc(self, doc_id, bucket="default", timeout=120):
        """ use couchBase uri to retrieve document from a bucket """
        api = self.capi_baseUrl + '/%s/%s' % (bucket, doc_id)
        status, content, header = self._http_request(api, headers=self._create_capi_headers(),
                                             timeout=timeout)
        if not status:
            raise ReadDocumentException(doc_id, content)
        return  json.loads(content)

    def _create_design_doc(self, bucket, name, function):
        api = self.capi_baseUrl + '/%s/_design/%s' % (bucket, name)
        status, content, header = self._http_request(
            api, 'PUT', function, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    def _get_design_doc(self, bucket, name):
        api = self.capi_baseUrl + '/%s/_design/%s' % (bucket, name)
        if isinstance(bucket, Bucket):
            api = self.capi_baseUrl + '/%s/_design/%s' % (bucket.name, name)

        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        meta_parsed = ""
        if status:
            # in dp4 builds meta data is in content, not in header
            if 'x-couchbase-meta' in header:
                meta = header['x-couchbase-meta']
                meta_parsed = json.loads(meta)
            else:
                meta_parsed = {}
                meta_parsed["_rev"] = json_parsed["_rev"]
                meta_parsed["_id"] = json_parsed["_id"]
        return status, json_parsed, meta_parsed

    def _delete_design_doc(self, bucket, name):
        status, design_doc, meta = self._get_design_doc(bucket, name)
        if not status:
            raise Exception("unable to find for deletion design document")
        api = self.capi_baseUrl + '/%s/_design/%s' % (bucket, name)
        if isinstance(bucket, Bucket):
            api = self.capi_baseUrl + '/%s/_design/%s' % (bucket.name, name)
        status, content, header = self._http_request(api, 'DELETE',
                                                     headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    def spatial_compaction(self, bucket, design_name):
        api = self.capi_baseUrl + '/%s/_design/%s/_spatial/_compact' % (bucket, design_name)
        if isinstance(bucket, Bucket):
            api = self.capi_baseUrl + \
            '/%s/_design/%s/_spatial/_compact' % (bucket.name, design_name)

        status, content, header = self._http_request(api, 'POST',
                                                     headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    # Make a _design/_info request
    def set_view_info(self, bucket, design_name):
        """Get view diagnostic info (node specific)"""
        api = self.capi_baseUrl
        if isinstance(bucket, Bucket):
            api += '/_set_view/{0}/_design/{1}/_info'.format(bucket.name, design_name)
        else:
            api += '_set_view/{0}/_design/{1}/_info'.format(bucket, design_name)

        status, content, header = self._http_request(api, 'GET',
                                                     headers=self._create_capi_headers())
        if not status:
            raise SetViewInfoNotFound(design_name, content)
        json_parsed = json.loads(content)
        return status, json_parsed

    # Make a _spatial/_info request
    def spatial_info(self, bucket, design_name):
        api = self.capi_baseUrl + \
            '/%s/_design/%s/_spatial/_info' % (bucket, design_name)
        status, content, header = self._http_request(
            api, 'GET', headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    def _create_capi_headers(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    def _create_headers_with_auth(self, username, password):
        authorization = base64.encodestring('%s:%s' % (username, password))
        return {'Authorization': 'Basic %s' % authorization}

    # authorization must be a base64 string of username:password
    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Connection': 'close',
                'Accept': '*/*'}

    # authorization must be a base64 string of username:password
    def _create_headers_encoded_prepared(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/json',
                'Connection': 'close',
                'Authorization': 'Basic %s' % authorization}

    def _get_auth(self, headers):
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
                return "auth: " + base64.decodestring(val[6:])
        return ""

    def _http_request(self, api, method='GET', params='', headers=None,
                      timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        while True:
            try:
                response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers)
                if params.startswith("nodes=ns_1"):
                    self.test_log.debug(response)
                    self.test_log.debug(content)

                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError:
                        json_parsed = dict()
                        json_parsed["error"] = "status: {0}, content: {1}" \
                                               .format(response['status'],
                                                       content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    message = '{0} {1} body: {2} headers: {3} error: {4} reason: {5} {6} {7}'.\
                              format(method, api, params, headers, response['status'], reason,
                                     content.rstrip('\n'), self._get_auth(headers))
                    self.test_log.error(message.decode("utf8"))
                    self.test_log.debug(''.join(traceback.format_stack()))
                    return False, content, response
            except socket.error as e:
                self.test_log.error("Socket error while connecting to {0} error {1} "
                                    .format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            except httplib2.ServerNotFoundError as e:
                self.test_log.error("ServerNotFoundError error while connecting to {0} error {1} "
                                    .format(api, e))
                if time.time() > end_time:
                    raise ServerUnavailableException(ip=self.ip)
            # Sleep before next retry
            sleep(2)

    def init_cluster(self, username='Administrator', password='password',
                     port=constants.port):
        api = self.baseUrl + 'settings/web'
        params = urllib.urlencode({'port': str(port),
                                   'username': username,
                                   'password': password})
        self.test_log.debug('settings/web params on {0}:{1}:{2}'
                            .format(self.ip, self.port, params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def init_node(self):
        """ need a standalone method to initialize a node that could call
            anywhere with quota from testconstant """
        self.node_services = list()
        if self.services_node_init is None and self.services == "":
            self.node_services = ["kv"]
        elif self.services_node_init is None and self.services != "":
            self.node_services = self.services.split(",")
        elif self.services_node_init is not None:
            self.node_services = self.services_node_init.split("-")
        kv_quota = 0
        while kv_quota == 0:
            kv_quota = int(self.get_nodes_self().mcdMemoryReserved)
        info = self.get_nodes_self()

        cb_version = info.version[:5]
        if cb_version in COUCHBASE_FROM_VERSION_4:
            if "index" in self.node_services:
                self.test_log.debug("%s - Index service quota will be %s MB"
                                   % (self.ip, INDEX_QUOTA))
                kv_quota -= INDEX_QUOTA
                self.set_service_memoryQuota(service='indexMemoryQuota',
                                             memoryQuota=INDEX_QUOTA)
            if "fts" in self.node_services:
                self.test_log.debug("%s - Fts service will be %s MB"
                                    % (self.ip, FTS_QUOTA))
                kv_quota -= FTS_QUOTA
                self.test_log.debug("%s - Setting both index and fts quota"
                                    % self.ip)
                self.set_service_memoryQuota(service='ftsMemoryQuota',
                                             memoryQuota=FTS_QUOTA)
            if "cbas" in self.node_services:
                self.test_log.debug("%s - CBAS service quota will be %s MB"
                                    % (self.ip, CBAS_QUOTA))
                kv_quota -= CBAS_QUOTA
                self.set_service_memoryQuota(service = "cbasMemoryQuota",
                                             memoryQuota=CBAS_QUOTA)
            kv_quota -= 1
            if kv_quota < MIN_KV_QUOTA:
                    raise Exception("KV RAM needs to be more than %s MB"
                            " at node  %s"  % (MIN_KV_QUOTA, self.ip))

        self.test_log.debug("%s - KV quota: %s MB" % (self.ip, kv_quota))
        self.init_cluster_memoryQuota(self.username, self.password, kv_quota)
        if cb_version in COUCHBASE_FROM_VERSION_4:
            self.init_node_services(username=self.username,
                                    password=self.password,
                                    services=self.node_services)
        self.init_cluster(username=self.username, password=self.password)

    def init_node_services(self, username='Administrator', password='password',
                           hostname='127.0.0.1', port=constants.port,
                           services=None):
        api = self.baseUrl + '/node/controller/setupServices'
        if services is None:
            self.test_log.critical("services are marked as None, will not work")
            return False
        if hostname == "127.0.0.1":
            hostname = "{0}:{1}".format(hostname, port)
        params = urllib.urlencode({ 'hostname': hostname,
                                    'user': username,
                                    'password': password,
                                    'services': ",".join(services)})
        self.test_log.debug('/node/controller/setupServices params on {0}: {1}:{2}'
                            .format(self.ip, self.port, params))
        status, content, header = self._http_request(api, 'POST', params)
        error_message = "cannot change node services after cluster is provisioned"
        if not status and error_message in content:
            status = True
            self.test_log.warning(
                "This node is already provisioned with services, "
                "we do not consider this as failure for test case")
        return status

    def get_cluster_settings(self):
        settings = {}
        api = self.baseUrl + 'settings/web'
        status, content, header = self._http_request(api, 'GET')
        if status:
            settings = json.loads(content)
        self.test_log.debug('settings/web params on {0}:{1}:{2}'
                            .format(self.ip, self.port, settings))
        return settings

    def init_cluster_memoryQuota(self, username='Administrator',
                                 password='password',
                                 memoryQuota=256):
        api = self.baseUrl + 'pools/default'
        params = urllib.urlencode({'memoryQuota': memoryQuota})
        self.test_log.debug('pools/default params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_service_memoryQuota(self, service, username='Administrator',
                                 password='password',
                                 memoryQuota=256):
        ''' cbasMemoryQuota for cbas service.
            ftsMemoryQuota for fts service.
            indexMemoryQuota for index service.'''
        api = self.baseUrl + 'pools/default'
        params = urllib.urlencode({service: memoryQuota})
        self.test_log.debug('pools/default params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_cluster_name(self, name):
        api = self.baseUrl + 'pools/default'
        if name is None:
            name = ""
        params = urllib.urlencode({'clusterName': name})
        self.test_log.debug('pools/default params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_indexer_storage_mode(self, username='Administrator',
                                 password='password',
                                 storageMode='plasma'):
        """
           From spock, we replace forestdb with plasma
        """
        api = self.baseUrl + 'settings/indexes'
        params = urllib.urlencode({'storageMode': storageMode})
        error_message = "storageMode must be one of plasma, memory_optimized"
        self.test_log.debug('settings/indexes params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status and error_message in content:
            # TODO: Currently it just acknowledges if there is an error.
            # And proceeds with further initialization.
            self.test_log.warning(content)
        return status

    def cleanup_indexer_rebalance(self, server):
        if server:
            api = "http://{0}:{1}/".format(server.ip, self.index_port) + 'cleanupRebalance'
        else:
            api = self.baseUrl + 'cleanupRebalance'
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            self.test_log.error("{0} - Cleanup Rebalance:{1}, content:{2}"
                                .format(self.ip, status, content))
            raise Exception("indexer rebalance cleanup failed")

    def list_indexer_rebalance_tokens(self, server):
        if server:
            api = "http://{0}:{1}/" \
                  .format(server.ip, self.index_port) + 'listRebalanceTokens'
        else:
            api = self.baseUrl + 'listRebalanceTokens'
        print api
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            self.test_log.error("{0} - listRebalanceTokens: {1} ,content: {2}"
                                .format(self.ip, status, content))
            raise Exception("list rebalance tokens failed")

    def get_cluster_ceritificate(self):
        api = self.baseUrl + 'pools/default/certificate'
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            self.test_log.error("{0} - /pools/default/certificate status:{1},content:{2}"
                                .format(self.ip, status, content))
            raise Exception("certificate API failed")

    def regenerate_cluster_certificate(self):
        api = self.baseUrl + 'controller/regenerateCertificate'
        status, content, _ = self._http_request(api, 'POST')
        if status:
            return content
        else:
            self.test_log.error("{0} - controller/regenerateCertificate status:{1},content:{2}"
                                .format(self.ip, status, content))
            raise Exception("regenerateCertificate API failed")

    def __remote_clusters(self, api, op, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=''):
        param_map = {'hostname': "{0}:{1}".format(remoteIp, remotePort),
                        'username': username,
                        'password': password,
                        'name':name}
        if demandEncryption:
            param_map ['demandEncryption'] = 'on'
            param_map['certificate'] = certificate
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        # sample response :
        # [{"name":"two","uri":"/pools/default/remoteClusters/two","validateURI":"/pools/default/remoteClusters/two?just_validate=1","hostname":"127.0.0.1:9002","username":"Administrator"}]
        if status:
            remoteCluster = json.loads(content)
        else:
            self.test_log.error("{0} - /remoteCluster failed : status:{1},content:{2}"
                                .format(self.ip, status, content))
            raise Exception("remoteCluster API '{0} remote cluster' failed".format(op))
        return remoteCluster

    def add_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=''):
        # example : password:password username:Administrator hostname:127.0.0.1:9002 name:two
        msg = "Adding remote cluster hostname:{0}:{1} with username:password {2}:{3} name:{4} to source node: {5}:{6}"
        self.test_log.debug(msg.format(remoteIp, remotePort,
                                       username, password,
                                       name, self.ip, self.port))
        api = self.baseUrl + 'pools/default/remoteClusters'
        return self.__remote_clusters(api, 'add', remoteIp, remotePort, username, password, name, demandEncryption, certificate)

    def modify_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=''):
        self.test_log.debug("Modifying remote cluster name:{0}".format(name))
        api = self.baseUrl + 'pools/default/remoteClusters/' + urllib.quote(name)
        return self.__remote_clusters(api, 'modify', remoteIp, remotePort, username, password, name, demandEncryption, certificate)

    def get_remote_clusters(self):
        remote_clusters = []
        api = self.baseUrl + 'pools/default/remoteClusters/'
        params = urllib.urlencode({})
        status, content, header = self._http_request(api, 'GET', params)
        if status:
            remote_clusters = json.loads(content)
        return remote_clusters

    def remove_all_remote_clusters(self):
        remote_clusters = self.get_remote_clusters()
        for remote_cluster in remote_clusters:
            try:
                if remote_cluster["deleted"] == False:
                    self.remove_remote_cluster(remote_cluster["name"])
            except KeyError:
                # goxdcr cluster references will not contain "deleted" field
                self.remove_remote_cluster(remote_cluster["name"])

    def remove_remote_cluster(self, name):
        # example : name:two
        self.test_log.debug("Removing remote cluster name:{0}"
                            .format(urllib.quote(name)))
        api = self.baseUrl + 'pools/default/remoteClusters/{0}?' \
                             .format(urllib.quote(name))
        params = urllib.urlencode({})
        status, content, header = self._http_request(api, 'DELETE', params)
        #sample response : "ok"
        if not status:
            self.test_log.error("Failed to remove remote cluster: status:{0},content:{1}"
                                .format(status, content))
            raise Exception("remoteCluster API 'remove cluster' failed")

    # replicationType:continuous toBucket:default toCluster:two fromBucket:default
    # defaults at https://github.com/couchbase/goxdcr/metadata/replication_settings.go#L20-L33
    def start_replication(self, replicationType, fromBucket, toCluster, rep_type="xmem", toBucket=None, xdcr_params={}):
        toBucket = toBucket or fromBucket
        msg = "Starting {0} replication type:{1} from {2} to {3} in the " \
              "remote cluster {4} with settings {5}"
        self.test_log.debug(msg.format(replicationType, rep_type,
                                       fromBucket, toBucket,
                                       toCluster, xdcr_params))
        api = self.baseUrl + 'controller/createReplication'
        param_map = {'replicationType': replicationType,
                     'toBucket': toBucket,
                     'fromBucket': fromBucket,
                     'toCluster': toCluster,
                     'type': rep_type}
        param_map.update(xdcr_params)
        params = urllib.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        # response : {"id": "replication_id"}
        if status:
            json_parsed = json.loads(content)
            self.test_log.debug("Replication created with id: {0}"
                                .format(json_parsed['id']))
            return json_parsed['id']
        else:
            self.test_log.error("/controller/createReplication failed : status:{0},content:{1}"
                                .format(status, content))
            raise Exception("create replication failed : status:{0},content:{1}".format(status, content))

    def get_replications(self):
        replications = []
        content = self.ns_server_tasks()
        for item in content:
            if not isinstance(item, dict):
                msg = "Unexpected error while retrieving pools/default/tasks : {0}" \
                      .format(content)
                self.test_log.error(msg)
                raise Exception(msg)
            if item["type"] == "xdcr":
                replications.append(item)
        return replications

    def remove_all_replications(self):
        replications = self.get_replications()
        for replication in replications:
            self.stop_replication(replication["cancelURI"])

    def stop_replication(self, uri):
        self.test_log.debug("Deleting replication {0}".format(uri))
        api = self.baseUrl[:-1] + uri
        self._http_request(api, 'DELETE')

    def remove_all_recoveries(self):
        recoveries = []
        content = self.ns_server_tasks()
        for item in content:
            if item["type"] == "recovery":
                recoveries.append(item)
        for recovery in recoveries:
            api = self.baseUrl + recovery["stopURI"]
            status, content, header = self._http_request(api, 'POST')
            if not status:
                raise CBRecoveryFailedException("impossible to stop cbrecovery by {0}".format(api))
            self.test_log.debug("Recovery stopped by {0}".format(api))

    # params serverIp : the server to add to this cluster
    # raises exceptions when
    # unauthorized user
    # server unreachable
    # can't add the node to itself ( TODO )
    # server already added
    # returns otpNode
    def add_node(self, user='', password='', remoteIp='',
                 port=constants.port, zone_name='', services=None):
        otpNode = None

        # if ip format is ipv6 and enclosing brackets are not found,
        # enclose self.ip and remoteIp
        if self.ip.count(':') and self.ip[0] != '[':
            self.ip = '[' + self.ip + ']'
        if remoteIp.count(':') and remoteIp[0] != '[':
            remoteIp = '[' + remoteIp + ']'

        self.test_log.debug("Adding remote node {0}:{1} to cluster {2}:{3}"
                            .format(remoteIp, port, self.ip, self.port))
        if zone_name == '':
            api = self.baseUrl + 'controller/addNode'
        else:
            api = self.baseUrl + 'pools/default/serverGroups'
            if self.is_zone_exist(zone_name):
                zones = self.get_zone_names()
                api = "/".join((api, zones[zone_name], "addNode"))
                self.test_log.debug("Node {0} will be added to zone {1}"
                                    .format(remoteIp, zone_name))
            else:
                raise Exception("There is not zone with name: %s in cluster" % zone_name)

        params = urllib.urlencode(
            {'hostname': "http://{0}:{1}".format(remoteIp, port),
             'user': user,
             'password': password})
        if services is not None:
            services = ','.join(services)
            params = urllib.urlencode(
                {'hostname': "http://{0}:{1}".format(remoteIp, port),
                 'user': user,
                 'password': password,
                 'services': services})
        status, content, header = self._http_request(api, 'POST',
                                                     params)
        if status:
            json_parsed = json.loads(content)
            otpNodeId = json_parsed['otpNode']
            otpNode = OtpNode(otpNodeId)
            if otpNode.ip == '127.0.0.1':
                otpNode.ip = self.ip
        else:
            self.print_UI_logs()
            try:
                # print logs from node that we want to add
                wanted_node = deepcopy(self)
                wanted_node.ip = remoteIp
                wanted_node.print_UI_logs()
            except Exception, ex:
                self.test_log.error(ex)
            if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=remoteIp)
            elif content.find('Prepare join failed. Joining node to itself is not allowed') >= 0:
                raise ServerSelfJoinException(nodeIp=self.ip,
                                          remoteIp=remoteIp)
            else:
                self.test_log.error('Add_node error: {0}'.format(content))
                raise AddNodeException(nodeIp=self.ip,
                                       remoteIp=remoteIp,
                                       reason=content)
        return otpNode

        # params serverIp : the server to add to this cluster
    # raises exceptions when
    # unauthorized user
    # server unreachable
    # can't add the node to itself ( TODO )
    # server already added
    # returns otpNode
    def do_join_cluster(self, user='', password='', remoteIp='',
                        port=constants.port, zone_name='', services=None):
        otpNode = None
        self.test_log.debug('Adding remote node {0}:{1} to cluster {2}:{3}'
                            .format(remoteIp, port, self.ip, self.port))
        api = self.baseUrl + '/node/controller/doJoinCluster'
        params = urllib.urlencode({'hostname': "{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password})
        if services != None:
            services = ','.join(services)
            params = urllib.urlencode({'hostname': "{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password,
                                   'services': services})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            json_parsed = json.loads(content)
            otpNodeId = json_parsed['otpNode']
            otpNode = OtpNode(otpNodeId)
            if otpNode.ip == '127.0.0.1':
                otpNode.ip = self.ip
        else:
            self.print_UI_logs()
            try:
                # print logs from node that we want to add
                wanted_node = deepcopy(self)
                wanted_node.ip = remoteIp
                wanted_node.print_UI_logs()
            except Exception, ex:
                self.test_log.error(ex)
            if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=remoteIp)
            elif content.find('Prepare join failed. Joining node to itself is not allowed') >= 0:
                raise ServerSelfJoinException(nodeIp=self.ip,
                                          remoteIp=remoteIp)
            else:
                self.test_log.error('Add_node error: {0}'.format(content))
                raise AddNodeException(nodeIp=self.ip,
                                          remoteIp=remoteIp,
                                          reason=content)
        return otpNode

    def eject_node(self, user='', password='', otpNode=None):
        if not otpNode:
            self.test_log.error('Required otpNode parameter')
            return False
        api = self.baseUrl + 'controller/ejectNode'
        params = urllib.urlencode({'otpNode': otpNode,
                                   'user': user,
                                   'password': password})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            self.test_log.debug('EjectNode successful')
        else:
            if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=otpNode)
            else:
                # TODO : raise an exception here
                self.test_log.error('Eject_node error: {0}'.format(content))
        return True

    def force_eject_node(self):
        self.diag_eval("gen_server:cast(ns_cluster, leave).")
        self.check_delay_restart_coucbase_server()

    """ when we do reset couchbase server by force reject, couchbase server
        will not down right away but delay few seconds to be down depend
        on server spec.
        This fx will detect that delay and return true when
        couchbase server down and up again after force reject """
    def check_delay_restart_coucbase_server(self):
        api = self.baseUrl + 'nodes/self'
        headers = self._create_headers()
        break_out = 0
        count_cbserver_up = 0
        while break_out < 60 and count_cbserver_up < 2:
            try:
                response, content = httplib2.Http(timeout=120).request(
                    api, 'GET', '', headers)
                if response['status'] in ['200', '201', '202'] \
                        and count_cbserver_up == 0:
                    self.test_log.debug("Couchbase server is up but down soon")
                    sleep(1)
                    # Time for couchbase server reload after reset config
                    break_out += 1
                elif response['status'] in ['200', '201', '202']:
                    count_cbserver_up = 2
                    self.test_log.debug("Couchbase server is up again")
            except socket.error as e:
                self.test_log.warning("Couchbase server is down. "
                                      "Waiting for couchbase server up")
                sleep(2)
                break_out += 1
                count_cbserver_up = 1
        if break_out >= 60:
            raise Exception("Couchbase server did not start after 60 seconds")

    def fail_over(self, otpNode=None, graceful=False):
        if otpNode is None:
            self.test_log.error('Required otpNode parameter')
            return False
        api = self.baseUrl + 'controller/failOver'
        if graceful:
            api = self.baseUrl + 'controller/startGracefulFailover'
        params = urllib.urlencode({'otpNode': otpNode})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            self.test_log.debug('{0} - Failover successful'.format(otpNode))
        else:
            self.test_log.error('{0} - Failover error: {1}'
                                .format(otpNode, content))
            raise FailoverFailedException(content)
        return status

    def set_recovery_type(self, otpNode=None, recoveryType=None):
        self.test_log.debug("{0} - Setting recoveryType={1}"
                            .format(otpNode, recoveryType))
        if otpNode is None:
            self.test_log.error('{0} - Required otpNode parameter'
                                .format(otpNode))
            return False
        if recoveryType is None:
            self.test_log.error('{0} - RecoveryType is not set'
                                .format(otpNode))
            return False
        api = self.baseUrl + 'controller/setRecoveryType'
        params = urllib.urlencode({'otpNode': otpNode,
                                   'recoveryType': recoveryType})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            self.test_log.debug('{0} - RecoveryType set successful'
                                .format(otpNode))
        else:
            self.test_log.error('{0} - RecoveryType set failed. Error: {1}'
                                .format(otpNode, content))
            raise SetRecoveryTypeFailed(content)
        return status

    def add_back_node(self, otpNode=None):
        self.test_log.debug("{0} - Add-back node".format(otpNode))
        if otpNode is None:
            self.test_log.error('{0} - Required otpNode parameter'
                                .format(otpNode))
            return False
        api = self.baseUrl + 'controller/reAddNode'
        params = urllib.urlencode({'otpNode': otpNode})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            self.test_log.debug('{0} - Add_back successful'.format(otpNode))
        else:
            self.test_log.error('{0} - Add_back error: {1}'.format(otpNode,
                                                                   content))
            raise InvalidArgumentException('controller/reAddNode',
                                           parameters=params)
        return status

    def rebalance(self, otpNodes=[], ejectedNodes=[],
                  deltaRecoveryBuckets=None):
        knownNodes = ','.join(otpNodes)
        ejectedNodesString = ','.join(ejectedNodes)
        if deltaRecoveryBuckets is None:
            params = {'knownNodes': knownNodes,
                      'ejectedNodes': ejectedNodesString,
                      'user': self.username,
                      'password': self.password}
        else:
            deltaRecoveryBuckets = ",".join(deltaRecoveryBuckets)
            params = {'knownNodes': knownNodes,
                      'ejectedNodes': ejectedNodesString,
                      'deltaRecoveryBuckets': deltaRecoveryBuckets,
                      'user': self.username,
                      'password': self.password}
        self.test_log.debug('Rebalance params: {0}'.format(params))
        params = urllib.urlencode(params)
        api = self.baseUrl + "controller/rebalance"
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            self.test_log.debug('Rebalance operation started')
        else:
            self.test_log.error('Rebalance operation failed: {0}'.format(content))
            # extract the error
            raise InvalidArgumentException('controller/rebalance with error message {0}'.format(content),
                                           parameters=params)
        return status

    def get_terse_cluster_info(self):
        api = "%s%s" % (self.baseUrl, "pools/default/terseClusterInfo")
        status, content, _ = self._http_request(api, "GET")
        return status, content

    def diag_eval(self, code, print_log=True):
        api = '{0}{1}'.format(self.baseUrl, 'diag/eval/')
        status, content, header = self._http_request(api, "POST", code)
        if print_log:
            self.test_log.debug(
                "/diag/eval status on {0}:{1}: {2} content: {3} command: {4}"
                .format(self.ip, self.port, status, content, code))
        return status, content

    def set_chk_max_items(self, max_items):
        status, content = self.diag_eval("ns_config:set(chk_max_items, " + str(max_items) + ")")
        return status, content

    def set_chk_period(self, period):
        status, content = self.diag_eval("ns_config:set(chk_period, " + str(period) + ")")
        return status, content

    def set_enable_flow_control(self, flow=True, bucket='default'):
        flow_control = "false"
        if flow:
           flow_control = "true"
        code = "ns_bucket:update_bucket_props(\"" + bucket + "\", [{extra_config_string, \"upr_enable_flow_control=" + flow_control + "\"}])"
        status, content = self.diag_eval(code)
        return status, content

    def diag_master_events(self):
        api = '{0}{1}'.format(self.baseUrl, 'diag/masterEvents?o=1')
        status, content, header = self._http_request(api, "GET")
        self.test_log.debug("diag/masterEvents?o=1 status: {0} content: {1}"
                            .format(status, content))
        return status, content

    def get_admin_credentials(self):
        code = 'ns_config:search_node_prop(node(), ' \
               'ns_config:latest(), memcached, %s)'
        status, id = self.diag_eval(code % "admin_user")
        status, password = self.diag_eval(code % "admin_pass")
        return id.strip('"'), password.strip('"')

    def monitorRebalance(self, stop_if_loop=True):
        start = time.time()
        progress = 0
        retry = 0
        same_progress_count = 0
        previous_progress = 0
        while progress != -1 \
                and (progress != 100
                     or self._rebalance_progress_status() == 'running') \
                and retry < 20:
            # -1 is error , -100 means could not retrieve progress
            progress = self._rebalance_progress()
            if progress == -100:
                self.test_log.error("Unable to retrieve rebalanceProgress. "
                                    "Retrying after 1 second")
                retry += 1
            else:
                retry = 0
            if stop_if_loop:
                # reset same_progress_count if get a different result,
                # or progress is still O
                # it may take long time until the results are different from 0
                if previous_progress != progress or progress == 0:
                    previous_progress = progress
                    same_progress_count = 0
                else:
                    same_progress_count += 1
                if same_progress_count > 50:
                    self.test_log.error("Rebalance progress code in "
                                        "infinite loop: %s" % progress)
                    return False
            # Sleep to printout less log
            sleep(3, log_type="infra")
        if progress < 0:
            self.test_log.error("Rebalance progress code: %s" % progress)
            return False
        else:
            duration = time.time() - start
            sleep_time = duration
            if duration > 10:
                sleep_time = 5
            self.test_log.info("Rebalance done. Taken %s seconds to complete"
                               % duration)
            # Sleep required for ns_server to be ready for further actions
            sleep(sleep_time, "Wait after rebalance complete")
            return True

    def _rebalance_progress_status(self):
        api = self.baseUrl + "pools/default/rebalanceProgress"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if "status" in json_parsed:
                return json_parsed['status']
        else:
            return None

    def _rebalance_status_and_progress(self):
        """
        Returns a 2-tuple capturing the rebalance status and progress, as follows:
            ('running', progress) - if rebalance is running
            ('none', 100)         - if rebalance is not running (i.e. assumed done)
            (None, -100)          - if there's an error getting the rebalance progress
                                    from the server
            (None, -1)            - if the server responds but there's no information on
                                    what the status of rebalance is

        The progress is computed as a average of the progress of each node
        rounded to 2 decimal places.

        Throws RebalanceFailedException if rebalance progress returns an error message
        """
        avg_percentage = -1
        rebalance_status = None
        api = self.baseUrl + "pools/default/rebalanceProgress"
        try:
            status, content, header = self._http_request(api)
        except ServerUnavailableException as e:
            self.test_log.error(e)
            return None, -100
        json_parsed = json.loads(content)
        self.log.debug(json_parsed)
        if status:
            if "status" in json_parsed:
                rebalance_status = json_parsed["status"]
                if "errorMessage" in json_parsed:
                    msg = '{0} - rebalance failed'.format(json_parsed)
                    self.test_log.error(msg)
                    self.print_UI_logs()
                    raise RebalanceFailedException(msg)
                elif rebalance_status == "running":
                    total_percentage = 0
                    count = 0
                    for key in json_parsed:
                        if key.find('@') >= 0:
                            ns_1_dictionary = json_parsed[key]
                            percentage = ns_1_dictionary['progress'] * 100
                            count += 1
                            total_percentage += percentage
                    if count:
                        avg_percentage = (total_percentage / count)
                    else:
                        avg_percentage = 0
                    self.test_log.debug("Rebalance percentage: {0:.02f} %"
                                        .format(round(avg_percentage, 2)))
                else:
                    # Sleep before printing rebalance failure log
                    sleep(5, log_type="infra")
                    status, content, header = self._http_request(api)
                    json_parsed = json.loads(content)
                    if "errorMessage" in json_parsed:
                        msg = '{0} - rebalance failed'.format(json_parsed)
                        self.print_UI_logs()
                        raise RebalanceFailedException(msg)
                    avg_percentage = 100
        else:
            avg_percentage = -100
        return rebalance_status, avg_percentage

    def _rebalance_progress(self):
        return self._rebalance_status_and_progress()[1]

    def log_client_error(self, post):
        api = self.baseUrl + 'logClientError'
        status, content, header = self._http_request(api, 'POST', post)
        if not status:
            self.test_log.error('Unable to logClientError')

    def execute_statement_on_cbas(self, statement, mode, pretty=True,
                                  timeout=70, client_context_id=None):
        self.cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers()

        params = {'statement': statement, 'pretty': pretty, 'client_context_id': client_context_id}

        if mode is not None:
            params['mode'] = mode

        params = json.dumps(params)
        status, content, header = self._http_request(api, 'POST',
                                                     headers=headers,
                                                     params=params,
                                                     timeout=timeout)
        if status:
            return content
        elif str(header['status']) == '503':
            self.test_log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500', '400']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in  msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            self.test_log.error("/analytics/service status:{0},content:{1}".format(
                status, content))
            raise Exception("Analytics Service API failed")

    def get_buckets_itemCount(self):
        # get all the buckets
        bucket_map = {}
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets?basic_stats=true')
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            for item in json_parsed:
                bucket_name = item['name']
                item_count = item['basicStats']['itemCount']
                bucket_map[bucket_name] = item_count
        self.test_log.debug(bucket_map)
        return bucket_map

    # returns node data for this host
    def get_nodes_self(self, timeout=120):
        node = None
        api = self.baseUrl + 'nodes/self'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            node = RestParser().parse_get_nodes_response(json_parsed)
        return node

    # returns node data for this host
    def get_jre_path(self, timeout=120):
        api = self.baseUrl + 'nodes/self'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            return json_parsed
        else:
            raise Exception("Unable to get jre path from ns-server")

    def node_statuses(self, timeout=120):
        nodes = []
        api = self.baseUrl + 'nodeStatuses'
        status, content, header = self._http_request(api, timeout=timeout)
        json_parsed = json.loads(content)
        if status:
            for key in json_parsed:
                # each key contain node info
                value = json_parsed[key]
                # get otp,get status
                node = OtpNode(id=value['otpNode'],
                               status=value['status'])
                if node.ip == '127.0.0.1' or node.ip == 'cb.local':
                    node.ip = self.ip
                node.port = int(key[key.rfind(":") + 1:])
                node.replication = value['replication']
                if 'gracefulFailoverPossible' in value.keys():
                    node.gracefulFailoverPossible = value['gracefulFailoverPossible']
                else:
                    node.gracefulFailoverPossible = False
                nodes.append(node)
        return nodes

    def cluster_status(self):
        parsed = {}
        api = self.baseUrl + 'pools/default'
        status, content, header = self._http_request(api)
        if status:
            parsed = json.loads(content)
        return parsed

#     def fetch_vbucket_map(self, bucket="default"):
#         """Return vbucket map for bucket
#         Keyword argument:
#         bucket -- bucket name
#         """
#         api = self.baseUrl + 'pools/default/buckets/' + bucket
#         status, content, header = self._http_request(api)
#         _stats = json.loads(content)
#         return _stats['vBucketServerMap']['vBucketMap']

#     def get_vbucket_map_and_server_list(self, bucket="default"):
#         """ Return server list, replica and vbuckets map
#         that matches to server list """
#         vbucket_map = self.fetch_vbucket_map(bucket)
#         api = self.baseUrl + 'pools/default/buckets/' + bucket
#         status, content, header = self._http_request(api)
#         _stats = json.loads(content)
#         num_replica = _stats['vBucketServerMap']['numReplicas']
#         vbucket_map = _stats['vBucketServerMap']['vBucketMap']
#         servers = _stats['vBucketServerMap']['serverList']
#         server_list = []
#         for node in servers:
#             node = node.split(":")
#             server_list.append(node[0])
#         return vbucket_map, server_list, num_replica

    def get_pools_info(self):
        parsed = {}
        api = self.baseUrl + 'pools'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            parsed = json_parsed
        return parsed

    def get_pools_default(self, query='', timeout=30):
        parsed = {}
        api = self.baseUrl + 'pools/default'
        if query:
            api += "?" + query

        status, content, header = self._http_request(api, timeout=timeout)
        json_parsed = json.loads(content)
        if status:
            parsed = json_parsed
        return parsed

    def get_pools(self):
        version = None
        api = self.baseUrl + 'pools'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            version = MembaseServerVersion(json_parsed['implementationVersion'], json_parsed['componentsVersion'])
        return version

    def fetch_system_stats(self):
        """Return deserialized system stats."""
        api = self.baseUrl + 'pools/default/'
        status, content, header = self._http_request(api)
        return json.loads(content)

    def get_nodes(self):
        nodes = []
        api = self.baseUrl + 'pools/default'
        status, content, header = self._http_request(api)
        count = 0
        while not content and count < 7:
            self.test_log.warning("Retrying get_nodes() after 5 seconds")
            sleep(5)
            status, content, header = self._http_request(api)
            count += 1
        if count == 7:
            raise Exception("could not get node info after 30 seconds")
        json_parsed = json.loads(content)
        if status:
            if "nodes" in json_parsed:
                for json_node in json_parsed["nodes"]:
                    node = RestParser().parse_get_nodes_response(json_node)
                    node.rest_username = self.username
                    node.rest_password = self.password
                    if node.ip == "127.0.0.1":
                        node.ip = self.ip
                    # Only add nodes which are active on cluster
                    if node.clusterMembership == 'active':
                        nodes.append(node)
                    else:
                        self.test_log.warn("{0} - Node not part of cluster {1}"
                                           .format(node.ip,
                                                   node.clusterMembership))
        return nodes

    # this method returns the number of node in cluster
    def get_cluster_size(self):
        nodes = self.get_nodes()
        node_ip = []
        for node in nodes:
            node_ip.append(node.ip)
        self.test_log.debug("Number of node(s) in cluster is {0} node(s)"
                            .format(len(node_ip)))
        return len(node_ip)

    """ this medthod return version on node that is not initialized yet """
    def get_nodes_version(self):
        node = self.get_nodes_self()
        version = node.version
        self.test_log.debug("Node version in cluster: {0}".format(version))
        return version

    # this method returns the versions of nodes in cluster
    def get_nodes_versions(self, logging=True):
        nodes = self.get_nodes()
        versions = []
        for node in nodes:
            versions.append(node.version)
        return versions

    def check_cluster_compatibility(self, version):
        """
        Check if all nodes in cluster are of versions equal or above the version required.
        :param version: Version to check the cluster compatibility for. Should be of format major_ver.minor_ver.
                        For example: 5.0, 4.5, 5.1
        :return: True if cluster is compatible with the version specified, False otherwise. Return None if cluster is
        uninitialized.
        """
        nodes = self.get_nodes()
        if not nodes:
            # If nodes returned is None, it means that the cluster is not initialized yet and hence cluster
            # compatibility cannot be found. Return None
            return None
        major_ver, minor_ver = version.split(".")
        compatibility = int(major_ver) * 65536 + int(minor_ver)
        is_compatible = True
        for node in nodes:
            clusterCompatibility = int(node.clusterCompatibility)
            if clusterCompatibility < compatibility:
                is_compatible = False
        return is_compatible

    # this method returns the services of nodes in cluster - implemented for Sherlock
    def get_nodes_services(self):
        nodes = self.get_nodes()
        map = {}
        for node in nodes:
            key = "{0}:{1}".format(node.ip, node.port)
            map[key] = node.services
        return map

    # Check node version
    def check_node_versions(self, check_version="4.0"):
        versions = self.get_nodes_versions()
        if versions[0] < check_version:
            return False
        return True

    def get_cluster_stats(self):
        """
        Reads cluster nodes statistics using `pools/default` rest GET method
        :return stat_dict: Dictionary of CPU & Memory status each cluster node
        """
        stat_dict = dict()
        json_output = self.get_pools_default()
        if 'nodes' in json_output:
            for node_stat in json_output['nodes']:
                stat_dict[node_stat['hostname']] = dict()
                stat_dict[node_stat['hostname']]['version'] = node_stat['version']
                stat_dict[node_stat['hostname']]['services'] = node_stat['services']
                stat_dict[node_stat['hostname']]['cpu_utilization'] = node_stat['systemStats'].get('cpu_utilization_rate')
                stat_dict[node_stat['hostname']]['mem_free'] = node_stat['systemStats'].get('mem_free')
                stat_dict[node_stat['hostname']]['mem_total'] = node_stat['systemStats'].get('mem_total')
                stat_dict[node_stat['hostname']]['swap_mem_used'] = node_stat['systemStats'].get('swap_used')
                stat_dict[node_stat['hostname']]['swap_mem_total'] = node_stat['systemStats'].get('swap_total')
                stat_dict[node_stat['hostname']]['active_item_count'] = 0
                stat_dict[node_stat['hostname']]['replica_item_count'] = 0
                if 'curr_items' in node_stat['interestingStats']:
                    stat_dict[node_stat['hostname']]['active_item_count'] = node_stat['interestingStats']['curr_items']
                if 'vb_replica_curr_items' in node_stat['interestingStats']:
                    stat_dict[node_stat['hostname']]['replica_item_count'] = node_stat['interestingStats']['vb_replica_curr_items']
        return stat_dict

    def get_fts_stats(self, index_name, bucket_name, stat_name):
        """
        List of fts stats available as of 03/16/2017 -
        default:default_idx3:avg_queries_latency: 0,
        default:default_idx3:batch_merge_count: 0,
        default:default_idx3:doc_count: 0,
        default:default_idx3:iterator_next_count: 0,
        default:default_idx3:iterator_seek_count: 0,
        default:default_idx3:num_bytes_live_data: 0,
        default:default_idx3:num_bytes_used_disk: 0,
        default:default_idx3:num_mutations_to_index: 0,
        default:default_idx3:num_pindexes: 0,
        default:default_idx3:num_pindexes_actual: 0,
        default:default_idx3:num_pindexes_target: 0,
        default:default_idx3:num_recs_to_persist: 0,
        default:default_idx3:reader_get_count: 0,
        default:default_idx3:reader_multi_get_count: 0,
        default:default_idx3:reader_prefix_iterator_count: 0,
        default:default_idx3:reader_range_iterator_count: 0,
        default:default_idx3:timer_batch_store_count: 0,
        default:default_idx3:timer_data_delete_count: 0,
        default:default_idx3:timer_data_update_count: 0,
        default:default_idx3:timer_opaque_get_count: 0,
        default:default_idx3:timer_opaque_set_count: 0,
        default:default_idx3:timer_rollback_count: 0,
        default:default_idx3:timer_snapshot_start_count: 0,
        default:default_idx3:total_bytes_indexed: 0,
        default:default_idx3:total_bytes_query_results: 0,
        default:default_idx3:total_compactions: 0,
        default:default_idx3:total_queries: 0,
        default:default_idx3:total_queries_error: 0,
        default:default_idx3:total_queries_slow: 0,
        default:default_idx3:total_queries_timeout: 0,
        default:default_idx3:total_request_time: 0,
        default:default_idx3:total_term_searchers: 0,
        default:default_idx3:writer_execute_batch_count: 0,
        :param index_name: name of the index
        :param bucket_name: source bucket
        :param stat_name: any of the above
        :return:
        """
        api = "{0}{1}".format(self.fts_baseUrl, 'api/nsstats')
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        try:
            return status, json_parsed[bucket_name+':'+index_name+':'+stat_name]
        except:
            self.test_log.error("Stat {0} not found for {1} on bucket {2}"
                                .format(stat_name, index_name, bucket_name))

    # return AutoFailoverSettings
    def get_autofailover_settings(self):
        settings = None
        api = self.baseUrl + 'settings/autoFailover'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            settings = AutoFailoverSettings()
            settings.enabled = json_parsed["enabled"]
            settings.count = json_parsed["count"]
            settings.timeout = json_parsed["timeout"]
            settings.can_abort_rebalance = json_parsed["canAbortRebalance"]
            settings.failoverOnDataDiskIssuesEnabled = json_parsed["failoverOnDataDiskIssues"]["enabled"]
            settings.failoverOnDataDiskIssuesTimeout = json_parsed["failoverOnDataDiskIssues"]["timePeriod"]
            settings.maxCount = json_parsed["maxCount"]
            settings.failoverServerGroup = json_parsed["failoverServerGroup"]
        return settings

    def update_autofailover_settings(self, enabled, timeout, canAbortRebalance=False, enable_disk_failure=False,
                                     disk_timeout=120, maxCount=1, enableServerGroup=False):
        params_dict = {}
        params_dict['timeout'] = timeout
        if enabled:
            params_dict['enabled'] = 'true'

        else:
            params_dict['enabled'] = 'false'
        if canAbortRebalance:
            params_dict['canAbortRebalance'] = 'true'
        else:
            params_dict['canAbortRebalance'] = 'false'
        if enable_disk_failure:
            params_dict['failoverOnDataDiskIssues[enabled]'] = 'true'
            params_dict['failoverOnDataDiskIssues[timePeriod]'] = disk_timeout
        else:
            params_dict['failoverOnDataDiskIssues[enabled]'] = 'false'
        params_dict['maxCount'] = maxCount
        if enableServerGroup:
            params_dict['failoverServerGroup'] = 'true'
        else:
            params_dict['failoverServerGroup'] = 'false'
        params = urllib.urlencode(params_dict)
        api = self.baseUrl + 'settings/autoFailover'
        self.test_log.debug('settings/autoFailover params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            self.test_log.error(
                '''failed to change autofailover_settings!
                   See MB-7282. Workaround:
                   wget --user=Administrator --password=asdasd --post-data='rpc:call(mb_master:master_node(), erlang, apply ,[fun () -> erlang:exit(erlang:whereis(mb_master), kill) end, []]).' http://localhost:8091/diag/eval''')
        return status

    # return AutoReprovisionSettings
    def get_autoreprovision_settings(self):
        settings = None
        api = self.baseUrl + 'settings/autoReprovision'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            settings = AutoReprovisionSettings()
            settings.enabled = json_parsed["enabled"]
            settings.count = json_parsed["count"]
            settings.max_nodes = json_parsed["max_nodes"]
        return settings

    def update_autoreprovision_settings(self, enabled, maxNodes=1):
        if enabled:
            params = urllib.urlencode({'enabled': 'true',
                                       'maxNodes': maxNodes})
        else:
            params = urllib.urlencode({'enabled': 'false',
                                       'maxNodes': maxNodes})
        api = self.baseUrl + 'settings/autoReprovision'
        self.test_log.debug('settings/autoReprovision params: {0}'
                            .format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            self.test_log.error('Failed to change autoReprovision_settings!')
        return status

    def reset_autofailover(self):
        api = self.baseUrl + 'settings/autoFailover/resetCount'
        status, content, header = self._http_request(api, 'POST', '')
        return status

    def reset_autoreprovision(self):
        api = self.baseUrl + 'settings/autoReprovision/resetCount'
        status, content, header = self._http_request(api, 'POST', '')
        return status

    def set_alerts_settings(self, recipients, sender, email_username, email_password, email_host='localhost', email_port=25, email_encrypt='false', alerts='auto_failover_node,auto_failover_maximum_reached'):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.urlencode({'enabled': 'true',
                                   'recipients': recipients,
                                   'sender': sender,
                                   'emailUser': email_username,
                                   'emailPass': email_password,
                                   'emailHost': email_host,
                                   'emailPort': email_port,
                                   'emailEncrypt': email_encrypt,
                                   'alerts': alerts})
        self.test_log.debug('settings/alerts params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def get_alerts_settings(self):
        api = self.baseUrl + 'settings/alerts'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to get autofailover alerts settings")
        return json_parsed

    def disable_alerts(self):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.urlencode({'enabled': 'false'})
        self.test_log.debug('settings/alerts params: {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_cas_drift_threshold(self, bucket, ahead_threshold_in_millisecond, behind_threshold_in_millisecond):

        api = self.baseUrl + 'pools/default/buckets/{0}'. format( bucket )
        params_dict = {'driftAheadThresholdMs': ahead_threshold_in_millisecond,
                       'driftBehindThresholdMs': behind_threshold_in_millisecond}
        params = urllib.urlencode(params_dict)
        self.test_log.debug("%s with param: %s" % (api, params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_metadata_purge_interval(self, interval=0.04):
        params = dict()
        api = self.baseUrl + "controller/setAutoCompaction"
        params["purgeInterval"] = interval
        params["parallelDBAndViewCompaction"] = "false"
        status, content, _ = self._http_request(api, "POST", urllib.urlencode(params))
        if status:
            self.test_log.info("Successfully set Metadata Purge Interval to {}".
                               format(interval))
        else:
            self.test_log.error("Setting Metadata Purge Interval failed {0}"
                                .format(content))
        return status

    def stop_rebalance(self, wait_timeout=10):
        api = self.baseUrl + '/controller/stopRebalance'
        status, content, header = self._http_request(api, 'POST')
        if status:
            for i in xrange(wait_timeout):
                if self._rebalance_progress_status() == 'running':
                    self.test_log.warn("Rebalance not stopped after {0} sec"
                                       .format(i + 1))
                    sleep(1)
                    status = False
                else:
                    self.test_log.info("Rebalance stopped")
                    status = True
                    break
        else:
            self.test_log.error("Rebalance not stopped due to {0}"
                                .format(content))
        return status

    def set_data_path(self, data_path=None, index_path=None, cbas_path=[]):
        api = self.baseUrl + '/nodes/self/controller/settings'
        from urllib3._collections import HTTPHeaderDict
        data = HTTPHeaderDict()

        paths = {}
        if data_path:
            data.add('path', data_path)
            paths['path'] = data_path
        if index_path:
            data.add('index_path', index_path)
            paths['index_path'] = index_path
        if cbas_path:
            import ast
            cbas_path = ast.literal_eval(cbas_path)
            for cbas in cbas_path:
                data.add('cbas_path', cbas)
            paths['cbas_path'] = cbas_path
        if paths:
            params = urllib.urlencode(paths)
            self.test_log.debug('/nodes/self/controller/settings params: {0}'
                                .format(urllib.urlencode(data)))
            status, content, header = self._http_request(
                api, 'POST', urllib.urlencode(data))
            if status:
                self.test_log.debug("Setting paths: {0}: status {1}"
                                    .format(data, status))
            else:
                self.test_log.error("Unable to set data_path {0}: {1}"
                                    .format(data, content))
            return status

    def set_jre_path(self,jre_path=None,set=True):
        api = self.baseUrl + '/nodes/self/controller/settings'
        from urllib3._collections import HTTPHeaderDict
        data = HTTPHeaderDict()
        paths={}
        if jre_path:
            data.add('java_home',jre_path)
            paths['java_home']=jre_path

        if paths:
            params = urllib.urlencode(paths)
            self.test_log.debug('/nodes/self/controller/settings params: {0}'
                                .format(urllib.urlencode(data)))
            status, content, header = self._http_request(api, 'POST', urllib.urlencode(data))
            if status:
                self.test_log.debug("Setting paths: {0}: status {1}"
                                    .format(data, status))
            else:
                self.test_log.warning("Unable to set data_path {0}: status {1}"
                                      .format(data, status))
                if not set:
                    return content
                else:
                    exit(1)
            return status

    def get_database_disk_size(self, bucket='default'):
        api = self.baseUrl + "pools/{0}/buckets".format(bucket)
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        # disk_size in MB
        disk_size = (json_parsed[0]["basicStats"]["diskUsed"]) / (1024 * 1024)
        return status, disk_size

    def ddoc_compaction(self, design_doc_id, bucket="default"):
        api = self.baseUrl + "pools/default/buckets/%s/ddocs/%s/controller/compactView" % \
            (bucket, design_doc_id)
        status, content, header = self._http_request(api, 'POST')
        if not status:
            raise CompactViewFailed(design_doc_id, content)
        self.test_log.debug("Compaction for ddoc '%s' was triggered"
                            % design_doc_id)

    def check_compaction_status(self, bucket_name):
        tasks = self.ns_server_tasks()
        if "error" in tasks:
            raise Exception(tasks)
        for task in tasks:
            self.log.debug("Task is {0}".format(task))
            if task["type"] == "bucket_compaction":
                if task["bucket"] == bucket_name:
                    return True, task["progress"]
        return False, None

    def change_memcached_t_option(self, value):
        cmd = '[ns_config:update_key({node, N, memcached}, fun (PList)' + \
              ' -> lists:keystore(verbosity, 1, PList, {verbosity, \'-t ' + str(value) + '\'}) end)' + \
              ' || N <- ns_node_disco:nodes_wanted()].'
        return self.diag_eval(cmd)

    def set_ensure_full_commit(self, value):
        """Dynamic settings changes"""
        # the boolean paramter is used to turn on/off ensure_full_commit(). In XDCR,
        # issuing checkpoint in this function is expensive and not necessary in some
        # test, turning off this function would speed up some test. The default value
        # is ON.
        cmd = 'ns_config:set(ensure_full_commit_enabled, {0}).'.format(value)
        return self.diag_eval(cmd)

    def get_internalSettings(self, param):
            """allows to get internalSettings values for:
            indexAwareRebalanceDisabled, rebalanceIndexWaitingDisabled,
            rebalanceIndexPausingDisabled, maxParallelIndexers,
            maxParallelReplicaIndexers, maxBucketCount"""
            api = self.baseUrl + "internalSettings"
            status, content, header = self._http_request(api)
            json_parsed = json.loads(content)
            param = json_parsed[param]
            return param

    def set_internalSetting(self, param, value):
        "Set any internal setting"
        api = self.baseUrl + "internalSettings"

        if isinstance(value, bool):
            value = str(value).lower()

        params = urllib.urlencode({param : value})
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.debug('Update internal setting {0}={1}'.format(param, value))
        return status

    def get_replication_for_buckets(self, src_bucket_name, dest_bucket_name):
        replications = self.get_replications()
        for replication in replications:
            if src_bucket_name in replication['source'] and \
                replication['target'].endswith(dest_bucket_name):
                return replication
        raise XDCRException("Replication with Src bucket: {0} and Target bucket: {1} not found".
                        format(src_bucket_name, dest_bucket_name))

    """ By default, these are the global replication settings -
        { optimisticReplicationThreshold:256,
        workerBatchSize:500,
        failureRestartInterval:1,
        docBatchSizeKb":2048,
        checkpointInterval":1800,
        maxConcurrentReps":32}
        You can override these using set_xdcr_param()
    """
    def set_xdcr_param(self, src_bucket_name,
                                         dest_bucket_name, param, value):
        replication = self.get_replication_for_buckets(src_bucket_name, dest_bucket_name)
        api = self.baseUrl[:-1] + replication['settingsURI']
        value = str(value).lower()
        params = urllib.urlencode({param: value})
        status, _, _ = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("Unable to set replication setting {0}={1} on bucket {2} on node {3}".
                            format(param, value, src_bucket_name, self.ip))
        self.test_log.debug("Updated {0}={1} on bucket'{2}' on {3}"
                            .format(param, value, src_bucket_name, self.ip))

    # Gets per-replication setting value
    def get_xdcr_param(self, src_bucket_name,
                                    dest_bucket_name, param):
        replication = self.get_replication_for_buckets(src_bucket_name, dest_bucket_name)
        api = self.baseUrl[:-1] + replication['settingsURI']
        status, content, _ = self._http_request(api)
        if not status:
            raise XDCRException("Unable to get replication setting {0} on bucket {1} on node {2}".
                      format(param, src_bucket_name, self.ip))
        json_parsed = json.loads(content)
        # when per-replication settings match global(internal) settings,
        # the param is not returned by rest API
        # in such cases, return internalSetting value for the param
        try:
            return json_parsed[param]
        except KeyError:
            if param == 'pauseRequested':
                return False
            else:
                param = 'xdcr' + param[0].upper() + param[1:]
                self.test_log.debug("Trying to fetch xdcr param:{0} from global settings"
                                    .format(param))
                return self.get_internalSettings(param)

    # Returns a boolean value on whether replication
    def is_replication_paused(self, src_bucket_name, dest_bucket_name):
        return self.get_xdcr_param(src_bucket_name, dest_bucket_name, 'pauseRequested')

    def is_replication_paused_by_id(self, repl_id):
        repl_id = repl_id.replace('/','%2F')
        api = self.baseUrl + 'settings/replications/' + repl_id
        status, content, header = self._http_request(api)
        if not status:
            raise XDCRException("Unable to retrieve pause resume status for replication {0}".
                                format(repl_id))
        repl_stats = json.loads(content)
        return repl_stats['pauseRequested']

    def pause_resume_repl_by_id(self, repl_id, param, value):
        repl_id = repl_id.replace('/','%2F')
        api = self.baseUrl + 'settings/replications/' + repl_id
        params = urllib.urlencode({param: value})
        status, _, _ = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("Unable to update {0}={1} setting for replication {2}".
                            format(param, value, repl_id))
        self.test_log.debug("Updated {0}={1} on {2}"
                            .format(param, value, repl_id))

    def get_recent_xdcr_vb_ckpt(self, repl_id):
        command = 'ns_server_testrunner_api:grab_all_goxdcr_checkpoints().'
        status, content = self.diag_eval(command)
        if not status:
            raise Exception("Unable to get recent XDCR checkpoint information")
        repl_ckpt_list = json.loads(content)
        # a single decoding will only return checkpoint record as string
        # convert string to dict using json
        chkpt_doc_string = repl_ckpt_list['/ckpt/%s/0' % repl_id].replace('"', '\"')
        chkpt_dict = json.loads(chkpt_doc_string)
        return chkpt_dict['checkpoints'][0]

    """ Start of FTS rest apis"""

    def create_fts_index(self, index_name, params):
        """create or edit fts index , returns {"status":"ok"} on success"""
        api = self.fts_baseUrl + "api/index/{0}".format(index_name)
        self.test_log.debug(json.dumps(params))
        status, content, header = self._http_request(api,
                                    'PUT',
                                    json.dumps(params, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        if status:
            self.test_log.info("Index {0} created".format(index_name))
        else:
            raise Exception("Error creating index: {0}".format(content))
        return status

    def update_fts_index(self, index_name, index_def):
        api = self.fts_baseUrl + "api/index/{0}".format(index_name)
        self.test_log.debug(json.dumps(index_def, indent=3))
        status, content, header = self._http_request(api,
                                    'PUT',
                                    json.dumps(index_def, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        if status:
            self.test_log.info("Index/alias {0} updated".format(index_name))
        else:
            raise Exception("Error updating index: {0}".format(content))
        return status

    def get_fts_index_definition(self, name, timeout=30):
        """ get fts index/alias definition """
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        status, content, header = self._http_request(
            api,
            headers=self._create_capi_headers(),
            timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return status, json_parsed

    def get_fts_index_doc_count(self, name, timeout=30):
        """ get number of docs indexed"""
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}/count".format(name)
        status, content, header = self._http_request(
            api,
            headers=self._create_capi_headers(),
            timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['count']

    def get_fts_index_uuid(self, name, timeout=30):
        """ Returns uuid of index/alias """
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}/".format(name)
        status, content, header = self._http_request(
            api,
            headers=self._create_capi_headers(),
            timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['indexDef']['uuid']

    def delete_fts_index(self, name):
        """ delete fts index/alias """
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        status, content, header = self._http_request(
            api,
            'DELETE',
            headers=self._create_capi_headers())
        return status

    def stop_fts_index_update(self, name):
        """ method to stop fts index from updating"""
        api = self.fts_baseUrl + "api/index/{0}/ingestControl/pause".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def freeze_fts_index_partitions(self, name):
        """ method to freeze index partitions asignment"""
        api = self.fts_baseUrl+ "api/index/{0}/planFreezeControl".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def disable_querying_on_fts_index(self, name):
        """ method to disable querying on index"""
        api = self.fts_baseUrl + "api/index/{0}/queryControl/disallow".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def enable_querying_on_fts_index(self, name):
        """ method to enable querying on index"""
        api = self.fts_baseUrl + "api/index/{0}/queryControl/allow".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def run_fts_query(self, index_name, query_json, timeout=70):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        headers = self._create_capi_headers()
        status, content, header = self._http_request(
            api,
            "POST",
            json.dumps(query_json, ensure_ascii=False).encode('utf8'),
            headers,
            timeout=timeout)

        if status:
            content = json.loads(content)
            return content['total_hits'], content['hits'], content['took'], \
                   content['status']

    def run_fts_query_with_facets(self, index_name, query_json):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        headers = self._create_capi_headers()
        status, content, header = self._http_request(
            api,
            "POST",
            json.dumps(query_json, ensure_ascii=False).encode('utf8'),
            headers,
            timeout=70)

        if status:
            content = json.loads(content)
            return content['total_hits'], content['hits'], content['took'], \
                   content['status'], content['facets']


    """ End of FTS rest APIs """

    def set_reb_cons_view(self, disable):
        """Enable/disable consistent view for rebalance tasks"""
        api = self.baseUrl + "internalSettings"
        params = {"indexAwareRebalanceDisabled": str(disable).lower()}
        params = urllib.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.info('Consistent-views during rebalance was set as indexAwareRebalanceDisabled={0}'
                           .format(str(disable).lower()))
        return status

    def set_reb_index_waiting(self, disable):
        """Enable/disable rebalance index waiting"""
        api = self.baseUrl + "internalSettings"
        params = {"rebalanceIndexWaitingDisabled": str(disable).lower()}
        params = urllib.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.info('Rebalance index waiting was set as rebalanceIndexWaitingDisabled={0}'
                           .format(str(disable).lower()))
        return status

    def set_rebalance_index_pausing(self, disable):
        """Enable/disable index pausing during rebalance"""
        api = self.baseUrl + "internalSettings"
        params = {"rebalanceIndexPausingDisabled": str(disable).lower()}
        params = urllib.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.info('Index pausing during rebalance was set as rebalanceIndexPausingDisabled={0}'
                           .format(str(disable).lower()))
        return status

    def set_max_parallel_indexers(self, count):
        """set max parallel indexer threads"""
        api = self.baseUrl + "internalSettings"
        params = {"maxParallelIndexers": count}
        params = urllib.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.info('Max parallel indexer threads was set as maxParallelIndexers={0}'
                           .format(count))
        return status

    def set_max_parallel_replica_indexers(self, count):
        """set max parallel replica indexers threads"""
        api = self.baseUrl + "internalSettings"
        params = {"maxParallelReplicaIndexers": count}
        params = urllib.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.info('Max parallel replica indexers threads was set as maxParallelReplicaIndexers={0}'
                           .format(count))
        return status

    def get_internal_replication_type(self):
        buckets = self.get_buckets()
        cmd = "\'{ok, BC} = ns_bucket:get_bucket(%s), ns_bucket:replication_type(BC).\'" % buckets[0].name
        return self.diag_eval(cmd)

    def set_mc_threads(self, mc_threads=4):
        """
        Change number of memcached threads and restart the cluster
        """
        cmd = "[ns_config:update_key({node, N, memcached}, " \
              "fun (PList) -> lists:keystore(verbosity, 1, PList," \
              " {verbosity, \"-t %s\"}) end) " \
              "|| N <- ns_node_disco:nodes_wanted()]." % mc_threads

        return self.diag_eval(cmd)

    def set_indexer_compaction(self, mode="circular", indexDayOfWeek=None, indexFromHour=0,
                                indexFromMinute=0, abortOutside=False,
                                indexToHour=0, indexToMinute=0, fragmentation=30):
        """Reset compaction values to default, try with old fields (dp4 build)
        and then try with newer fields"""
        params = {}
        api = self.baseUrl + "controller/setAutoCompaction"
        params["indexCompactionMode"] = mode
        params["indexCircularCompaction[interval][fromHour]"] = indexFromHour
        params["indexCircularCompaction[interval][fromMinute]"] = indexFromMinute
        params["indexCircularCompaction[interval][toHour]"] = indexToHour
        params["indexCircularCompaction[interval][toMinute]"] = indexToMinute
        if indexDayOfWeek:
            params["indexCircularCompaction[daysOfWeek]"] = indexDayOfWeek
        params["indexCircularCompaction[interval][abortOutside]"] = str(abortOutside).lower()
        params["parallelDBAndViewCompaction"] = "false"
        if mode == "full":
            params["indexFragmentationThreshold[percentage]"] = fragmentation
        self.test_log.info("Indexer Compaction Settings: %s" % params)
        params = urllib.urlencode(params)
        return self._http_request(api, "POST", params)

    def set_global_loglevel(self, loglevel='error'):
        """Set cluster-wide logging level for core components

        Possible loglevel:
            -- debug
            -- info
            -- warn
            -- error
        """

        api = self.baseUrl + 'diag/eval'
        request_body = 'rpc:eval_everywhere(erlang, apply, [fun () -> \
                        [ale:set_loglevel(L, {0}) || L <- \
                        [ns_server, couchdb, user, menelaus, ns_doctor, stats, \
                        rebalance, cluster, views, stderr]] end, []]).'.format(loglevel)
        return self._http_request(api=api, method='POST', params=request_body,
                                  headers=self._create_headers())

    def set_indexer_params(self, parameter, val):
        """
        :Possible  parameters:
            -- indexerThreads
            -- memorySnapshotInterval
            -- stableSnapshotInterval
            -- maxRollbackPoints
            -- logLevel
        """
        params = {}
        api = self.baseUrl + 'settings/indexes'
        params[parameter] = val
        params = urllib.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        self.test_log.info('Indexer {0} set to {1}'.format(parameter, val))
        return status

    def get_global_index_settings(self):
        api = self.baseUrl + "settings/indexes"
        status, content, header = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def set_couchdb_option(self, section, option, value):
        """Dynamic settings changes"""

        cmd = 'ns_config:set({{couchdb, {{{0}, {1}}}}}, {2}).'.format(section,
                                                                      option,
                                                                      value)
        return self.diag_eval(cmd)

    def get_alerts(self):
        api = self.baseUrl + "pools/default/"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if "alerts" in json_parsed:
                return json_parsed['alerts']
        else:
            return None

    def get_nodes_data_from_cluster(self, param="nodes"):
        api = self.baseUrl + "pools/default/"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if param in json_parsed:
                return json_parsed[param]
        else:
            return None

    def flush_bucket(self, bucket="default"):
        if isinstance(bucket, Bucket):
            bucket_name = bucket.name
        else:
            bucket_name = bucket
        api = self.baseUrl + "pools/default/buckets/%s/controller/doFlush" % (bucket_name)
        status, content, header = self._http_request(api, 'POST')
        if not status:
            raise BucketFlushFailed(self.ip, bucket_name)
        self.test_log.info("Flush for bucket '%s' was triggered" % bucket_name)

    def update_notifications(self, enable):
        api = self.baseUrl + 'settings/stats'
        params = urllib.urlencode({'sendStats' : enable})
        self.test_log.info('settings/stats params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def get_notifications(self):
        api = self.baseUrl + 'settings/stats'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            return json_parsed["sendStats"]
        return None

    def create_stats_snapshhot(self):
        api = self.baseUrl + "_createStatsSnapshot"
        return self._http_request(api,
                                  method='POST',
                                  headers=self._create_headers())

    def perform_cb_collect(self, params):
        headers = self._create_headers()
        api = self.baseUrl + "controller/startLogsCollection"
        return self._http_request(api, 'POST',
                                  urllib.urlencode(params), headers)

    def get_logs(self, last_n=10, contains_text=None):
        api = self.baseUrl + 'logs'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        logs = json_parsed['list']
        logs.reverse()
        result = []
        for i in xrange(min(last_n, len(logs))):
            result.append(logs[i])
            if contains_text is not None and contains_text in logs[i]["text"]:
                break
        return result

    def print_UI_logs(self, last_n=10, contains_text=None):
        logs = self.get_logs(last_n, contains_text)
        self.test_log.info("Latest logs from UI on {0}:".format(self.ip))
        for lg in logs:
            self.test_log.error(lg)

    def get_ro_user(self):
        api = self.baseUrl + 'settings/readOnlyAdminName'
        status, content, header = self._http_request(api, 'GET', '')
        return content, status

    def delete_ro_user(self):
        api = self.baseUrl + 'settings/readOnlyUser'
        status, content, header = self._http_request(api, 'DELETE', '')
        return status

    def create_ro_user(self, username, password):
        api = self.baseUrl + 'settings/readOnlyUser'
        params = urllib.urlencode({'username' : username, 'password' : password})
        self.test_log.info('settings/readOnlyUser params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    # Change password for readonly user
    def changePass_ro_user(self, username, password):
        api = self.baseUrl + 'settings/readOnlyUser'
        params = urllib.urlencode({'username' : username, 'password' : password})
        self.test_log.info('settings/readOnlyUser params : {0}'.format(params))
        status, content, header = self._http_request(api, 'PUT', params)
        return status

    '''Start Monitoring/Profiling Rest Calls'''
    def set_completed_requests_collection_duration(self, server, min_time):
        http = httplib2.Http()
        api = self.query_baseUrl + "admin/settings"
        body = {"completed-threshold": min_time}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers,
                                         body=json.dumps(body))
        return response, content

    def set_completed_requests_max_entries(self, server, no_entries):
        http = httplib2.Http()
        api = self.query_baseUrl + "admin/settings"
        body = {"completed-limit": no_entries}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers,
                                         body=json.dumps(body))
        return response, content

    def set_profiling(self, server, setting):
        http = httplib2.Http()
        api = self.query_baseUrl + "admin/settings"
        body = {"profile": setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers,
                                        body=json.dumps(body))
        return response, content

    def set_profiling_controls(self, server, setting):
        http = httplib2.Http()
        api = self.query_baseUrl + "admin/settings"
        body = {"controls": setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers,
                                         body=json.dumps(body))
        return response, content

    def get_query_admin_settings(self, server):
        http = httplib2.Http()
        api = self.query_baseUrl + "admin/settings"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "GET", headers=headers)
        result = json.loads(content)
        return result

    def get_query_vitals(self,server):
        http = httplib2.Http()
        api = self.query_baseUrl + "admin/vitals"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "GET", headers=headers)
        return response, content
    '''End Monitoring/Profiling Rest Calls'''

    def query_tool(self, query, port=8093, timeout=650, query_params={},
                   is_prepared=False, named_prepare=None,
                   verbose=True, encoded_plan=None, servers=None):
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http()
                if len(servers) > 1:
                    url = "http://%s:%s/query/service" % (servers[1].ip, port)
                else:
                    url = "http://%s:%s/query/service" % (self.ip, port)

                headers = self._create_headers_encoded_prepared()
                body = {'prepared': named_prepare,
                        'encoded_plan': encoded_plan}

                response, content = http.request(url, 'POST',
                                                 headers=headers,
                                                 body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.quote(prepared, '~()')
                params = 'prepared="%s"' % named_prepare
            else:
                prepared = json.dumps(query)
                prepared = str(prepared.encode('utf-8'))
                params = 'prepared=' + urllib.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(
                    query_params['creds'][0]['user'].encode('utf-8'),
                    query_params['creds'][0]['pass'].encode('utf-8'))
            api = "http://%s:%s/query/service?%s" % (self.ip, port, params)
            self.test_log.debug("%s" % api)
        else:
            params = {key: query}
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(
                    query_params['creds'][0]['user'].encode('utf-8'),
                    query_params['creds'][0]['pass'].encode('utf-8'))
                del query_params['creds']
            params.update(query_params)
            params = urllib.urlencode(params)
            if verbose:
                self.test_log.debug('Query params: {0}'.format(params))
            api = "http://%s:%s/query?%s" % (self.ip, port, params)

        status, content, header = self._http_request(api, 'POST',
                                                     timeout=timeout,
                                                     headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def analytics_tool(self, query, port=8095, timeout=650, query_params={},
                       is_prepared=False, named_prepare=None,
                       verbose=True, encoded_plan=None, servers=None):
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http()
                if len(servers)>1:
                    url = "http://%s:%s/query/service" % (servers[1].ip, port)
                else:
                    url = "http://%s:%s/query/service" % (self.ip, port)

                headers = {'Content-type': 'application/json'}
                body = {'prepared': named_prepare, 'encoded_plan':encoded_plan}

                response, content = http.request(url, 'POST',
                                                 headers=headers,
                                                 body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.quote(prepared, '~()')
                params = 'prepared="%s"'% named_prepare
            else:
                prepared = json.dumps(query)
                prepared = str(prepared.encode('utf-8'))
                params = 'prepared=' + urllib.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(
                    query_params['creds'][0]['user'].encode('utf-8'),
                    query_params['creds'][0]['pass'].encode('utf-8'))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
            self.test_log.info("%s" % api)
        else:
            params = {key : query}
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(
                    query_params['creds'][0]['user'].encode('utf-8'),
                    query_params['creds'][0]['pass'].encode('utf-8'))
                del query_params['creds']
            params.update(query_params)
            params = urllib.urlencode(params)
            if verbose:
                self.test_log.info('Query params: {0}'.format(params))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
        status, content, header = self._http_request(api, 'POST',
                                                     timeout=timeout,
                                                     headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def query_tool_stats(self):
        self.test_log.info('Query n1ql stats')
        api = self.query_baseUrl + "/admin/stats"
        status, content, header = self._http_request(api, 'GET')
        self.test_log.debug(content)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def index_tool_stats(self):
        self.test_log.info('Index n1ql stats')
        api = "http://%s:%s/indexStatus" % (self.ip, self.port)
        params = ""
        status, content, header = self._http_request(api, 'GET', params)
        self.test_log.debug(content)
        try:
            return json.loads(content)
        except ValueError:
            return content

    # return all rack/zone info
    def get_all_zones_info(self, timeout=120):
        zones = dict()
        api = self.baseUrl + 'pools/default/serverGroups'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            zones = json.loads(content)
        else:
            raise Exception("Failed to get all zones info.\n \
                  Zone only supports from couchbase server version >= 2.5")
        return zones

    # return group name and unique uuid
    def get_zone_names(self):
        zone_names = {}
        zone_info = self.get_all_zones_info()
        if zone_info and len(zone_info["groups"]) >= 1:
            for i in range(0, len(zone_info["groups"])):
                # pools/default/serverGroups/ = 27 chars
                zone_names[zone_info["groups"][i]["name"]] = zone_info["groups"][i]["uri"][28:]
        return zone_names

    def add_zone(self, zone_name):
        api = self.baseUrl + 'pools/default/serverGroups'
        request_name = "name={0}".format(zone_name)
        status, content, header = self._http_request(api, "POST",
                                                     params=request_name)
        if status:
            self.test_log.info("Zone {0} added".format(zone_name))
            return True
        else:
            raise Exception("Failed to add zone with name: %s " % zone_name)

    def delete_zone(self, zone_name):
        api = self.baseUrl + 'pools/default/serverGroups/'
        # check if zone exist
        found = False
        zones = self.get_zone_names()
        for zone in zones:
            if zone_name == zone:
                api += zones[zone_name]
                found = True
                break
        if not found:
            raise Exception("There is not zone with name: %s in cluster"
                            % zone_name)
        status, content, header = self._http_request(api, "DELETE")
        if status:
            self.test_log.info("Zone {0} deleted".format(zone_name))
        else:
            raise Exception("Failed to delete zone with name: %s " % zone_name)

    def rename_zone(self, old_name, new_name):
        api = self.baseUrl + 'pools/default/serverGroups/'
        # check if zone exist
        found = False
        zones = self.get_zone_names()
        for zone in zones:
            if old_name == zone:
                api += zones[old_name]
                request_name = "name={0}".format(new_name)
                found = True
                break
        if not found:
            raise Exception("There is not zone with name: %s in cluster"
                            % old_name)
        status, content, header = self._http_request(api, "PUT",
                                                     params=request_name)
        if status:
            self.test_log.info("Zone %s renamed to %s" % (old_name, new_name))
        else:
            raise Exception("Failed to rename zone with name: %s " % old_name)

    # get all nodes info in one zone/rack/group
    def get_nodes_in_zone(self, zone_name):
        nodes = {}
        tmp = {}
        zone_info = self.get_all_zones_info()
        if zone_name != "":
            found = False
            if len(zone_info["groups"]) >= 1:
                for i in range(0, len(zone_info["groups"])):
                    if zone_info["groups"][i]["name"] == zone_name:
                        tmp = zone_info["groups"][i]["nodes"]
                        if not tmp:
                            self.test_log.info(
                                "Zone {0} is existed but no node in it"
                                .format(zone_name))
                        # remove port
                        for node in tmp:
                            node["hostname"] = node["hostname"].split(":")
                            node["hostname"] = node["hostname"][0]
                            nodes[node["hostname"]] = node
                        found = True
                        break
            if not found:
                raise Exception("There is not zone with name: %s in cluster" % zone_name)
        return nodes

    def get_zone_and_nodes(self):
        """ only return zones with node in its """
        zones = {}
        tmp = {}
        zone_info = self.get_all_zones_info()
        if len(zone_info["groups"]) >= 1:
            for i in range(0, len(zone_info["groups"])):
                tmp = zone_info["groups"][i]["nodes"]
                if not tmp:
                    self.test_log.info("Zone {0} is existed but no node in it"
                                       .format(tmp))
                # remove port
                else:
                    nodes = []
                    for node in tmp:
                        node["hostname"] = node["hostname"].split(":")
                        node["hostname"] = node["hostname"][0]
                        print node["hostname"][0]
                        nodes.append(node["hostname"])
                    zones[zone_info["groups"][i]["name"]] = nodes
        return zones

    def get_zone_uri(self):
        zone_uri = {}
        zone_info = self.get_all_zones_info()
        if zone_info and len(zone_info["groups"]) >= 1:
            for i in range(0, len(zone_info["groups"])):
                zone_uri[zone_info["groups"][i]["name"]] = zone_info["groups"][i]["uri"]
        return zone_uri

    def shuffle_nodes_in_zones(self, moved_nodes, source_zone, target_zone):
        # moved_nodes should be a IP list like
        # ["192.168.171.144", "192.168.171.145"]
        request = ""
        for i in range(0, len(moved_nodes)):
            moved_nodes[i] = "ns_1@" + moved_nodes[i]

        all_zones = self.get_all_zones_info()
        api = self.baseUrl + all_zones["uri"][1:]

        moved_node_json = []
        for i in range(0, len(all_zones["groups"])):
            for node in all_zones["groups"][i]["nodes"]:
                if all_zones["groups"][i]["name"] == source_zone:
                    for n in moved_nodes:
                        if n == node["otpNode"]:
                            moved_node_json.append({"otpNode": node["otpNode"]})

        zone_json = {}
        group_json = []
        for i in range(0, len(all_zones["groups"])):
            node_j = []
            zone_json["uri"] = all_zones["groups"][i]["uri"]
            zone_json["name"] = all_zones["groups"][i]["name"]
            zone_json["nodes"] = node_j

            if not all_zones["groups"][i]["nodes"]:
                if all_zones["groups"][i]["name"] == target_zone:
                    for i in range(0, len(moved_node_json)):
                        zone_json["nodes"].append(moved_node_json[i])
                else:
                    zone_json["nodes"] = []
            else:
                for node in all_zones["groups"][i]["nodes"]:
                    if all_zones["groups"][i]["name"] == source_zone and \
                                           node["otpNode"] in moved_nodes:
                        pass
                    else:
                        node_j.append({"otpNode": node["otpNode"]})
                if all_zones["groups"][i]["name"] == target_zone:
                    for k in range(0, len(moved_node_json)):
                        node_j.append(moved_node_json[k])
                    zone_json["nodes"] = node_j
            group_json.append({"name": zone_json["name"], "uri": zone_json["uri"], "nodes": zone_json["nodes"]})
        request = '{{"groups": {0} }}'.format(json.dumps(group_json))
        status, content, header = self._http_request(api, "PUT", params=request)
        # sample request format
        # request = ' {"groups":[{"uri":"/pools/default/serverGroups/0","nodes": [] },\
        #                       {"uri":"/pools/default/serverGroups/c8275b7a88e6745c02815dde4a505e70","nodes": [] },\
        #                        {"uri":"/pools/default/serverGroups/1acd9810a027068bd14a1ddd43db414f","nodes": \
        #                               [{"otpNode":"ns_1@192.168.171.144"},{"otpNode":"ns_1@192.168.171.145"}]} ]} '
        return status

    def is_zone_exist(self, zone_name):
        found = False
        zones = self.get_zone_names()
        if zones:
            for zone in zones:
                if zone_name == zone:
                    found = True
                    return True
                    break
        if not found:
            self.test_log.error("There is not zone with name: {0} in cluster."
                                .format(zone_name))
            return False

    def start_cluster_logs_collection(self, nodes="*", upload=False, \
                                      uploadHost=None, customer="", ticket=""):
        if not upload:
            params = urllib.urlencode({"nodes":nodes})
        else:
            params = urllib.urlencode({"nodes":nodes, "uploadHost":uploadHost, \
                                       "customer":customer, "ticket":ticket})
        api = self.baseUrl + "controller/startLogsCollection"
        status, content, header = self._http_request(api, "POST", params)
        return status, content

    def get_cluster_logs_collection_info(self):
        api = self.baseUrl + "pools/default/tasks/"
        status, content, header = self._http_request(api, "GET")
        if status:
            tmp = json.loads(content)
            for k in tmp:
                if k["type"] == "clusterLogsCollection":
                    content = k
                    return content
        return None

    """ result["progress"]: progress logs collected at cluster level
        result["status]: status logs collected at cluster level
        result["perNode"]: all information logs collected at each node """
    def get_cluster_logs_collection_status(self):
        result = self.get_cluster_logs_collection_info()
        if result:
            return result["progress"], result["status"], result["perNode"]
        return None, None, None

    def cancel_cluster_logs_collection(self):
        api = self.baseUrl + "controller/cancelLogsCollection"
        status, content, header = self._http_request(api, "POST")
        return status, content

    def get_recovery_task(self):
        content = self.ns_server_tasks()
        for item in content:
            if item["type"] == "recovery":
                return item
        return None


    def get_recovery_progress(self, recoveryStatusURI):
        api = '%s%s' % (self.baseUrl, recoveryStatusURI)
        status, content, header = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def get_warming_up_tasks(self):
        tasks = self.ns_server_tasks()
        tasks_warmup = []
        for task in tasks:
            if task["type"] == "warming_up":
                tasks_warmup.append(task)
        return tasks_warmup

    '''LDAP Rest API '''
    '''
    clearLDAPSettings - Function to clear LDAP settings
    Parameter - None
    Returns -
    status of LDAPAuth clear command
    '''
    def clearLDAPSettings(self):
        api = self.baseUrl + 'settings/saslauthdAuth'
        params = urllib.urlencode({'enabled':'false'})
        status, content, header = self._http_request(api, 'POST', params)
        return status, content, header

    '''
    ldapUserRestOperation - Execute LDAP REST API
    Input Parameter -
        authOperation - this is for auth need to be enabled or disabled - True or 0
        currAdmmins - a list of username to add to full admin matching with ldap
        currROAdmins - a list of username to add to RO Admin
    Returns - status, content and header for the command executed
    '''
    def ldapUserRestOperation(self, authOperation, adminUser='', ROadminUser=''):
        authOperation = authOperation
        currAdmins = ''
        currROAdmins = ''

        if (adminUser != ''):
            for user in adminUser:
                currAdmins = user[0] + "\n\r" + currAdmins

        if (ROadminUser != ''):
            for user in ROadminUser:
                currROAdmins = user[0] + "\n\r" + currROAdmins
        content = self.executeLDAPCommand(authOperation, currAdmins, currROAdmins)

    '''
    executeLDAPCommand - Execute LDAP REST API
    Input Parameter -
        authOperation - this is for auth need to be enabled or disabled - True or 0
        currAdmmins - a list of username to add to full admin matching with ldap
        currROAdmins - a list of username to add to RO Admin
    Returns - status, content and header for the command executed
    '''
    def executeLDAPCommand(self, authOperation, currAdmins, currROAdmins, exclude=None):
        api = self.baseUrl + "settings/saslauthdAuth"

        if exclude is None:
            self.test_log.info("Into exclude is None")
            params = urllib.urlencode({'enabled': authOperation,
                                       'admins': '{0}'.format(currAdmins),
                                       'roAdmins':'{0}'.format(currROAdmins),
                                       })
        else:
            self.test_log.debug("Into exclude for value of fullAdmin {0}"
                                .format(exclude))
            if exclude == 'fullAdmin':
                params = urllib.urlencode({'enabled': authOperation,
                                           'roAdmins':'{0}'.format(currROAdmins),
                                           })
            else:
                self.test_log.debug("Into exclude for value of fullAdmin {0}"
                                    .format(exclude))
                params = urllib.urlencode({'enabled': authOperation,
                                           'admins': '{0}'.format(currAdmins),
                                           })

        status, content, header = self._http_request(api, 'POST', params)
        return content
    '''
    validateLogin - Validate if user can login using a REST API
    Input Parameter - user and password to check for login. Also take a boolean to
    decide if the status should be 200 or 400 and everything else should be
    false
    Returns - True of false based if user should login or login fail
    '''
    def validateLogin(self, user, password, login, getContent=False):
        api = self.baseUrl + "uilogin"
        header = {'Content-type': 'application/x-www-form-urlencoded'}
        params = urllib.urlencode({'user':'{0}'.format(user), 'password':'{0}'.format(password)})
        self.test_log.info("value of param is {0}".format(params))
        http = httplib2.Http()
        status, content = http.request(api, 'POST', headers=header, body=params)
        self.test_log.info("Status of login command - {0}".format(status))
        if getContent:
            return status, content
        if (status['status'] == "200" and login == True) \
                or (status ['status'] == "400" and login == False):
            return True
        else:
            return False

    '''
    ldapRestOperationGet - Get setting of LDAPAuth - Settings
    Returns - list of Admins, ROAdmins and is LDAPAuth enabled or not
    '''
    def ldapRestOperationGetResponse(self):
        self.test_log.info("GET command for LDAP Auth")
        api = self.baseUrl + "settings/saslauthdAuth"
        status, content, header = self._http_request(api, 'GET')
        return json.loads(content)

    '''
    executeValidateCredentials - API to check credentials of users
    Input - user and password that needs validation
    Returns -
        [role]:<currentrole>
        [source]:<saslauthd,builtin>
    '''
    def executeValidateCredentials(self, user, password):
        api = self.baseUrl + "validateCredentials"
        params = urllib.urlencode({
                                   'user':'{0}'.format(user),
                                   'password':'{0}'.format(password)
                                   })
        status, content, header = self._http_request(api, 'POST', params)
        self.test_log.test_log("Status of executeValidateCredentials - {0}"
                               .format(status))
        return status, json.loads(content)

    '''
    Audit Commands
    '''
    '''
    getAuditSettings - API returns audit settings for Audit
    Input - None
    Returns -
        [archive_path]:<path for archieve>
        [auditd_enabled]:<enabled disabled status for auditd>
        [log_path]:<path for logs>
        [rotate_interval]:<log rotate interval>
    '''
    def getAuditSettings(self):
        api = self.baseUrl + "settings/audit"
        status, content, header = self._http_request(api, 'GET')
        return json.loads(content)

    '''
    getAuditSettings - API returns audit settings for Audit
    Input -
        [archive_path]:<path for archieve>
        [auditd_enabled]:<enabled disabled status for auditd>
        [rotate_interval]:<log rotate interval in seconds>
        [disabled]:<Event Id for which Audit is to be disabled>
        [users]:<Comma seperated list of whitelisted users in the format username/local or username/external>
    '''

    def setAuditSettings(self, enabled='true', rotateInterval=86400, logPath='/opt/couchbase/var/lib/couchbase/logs',
                         disabled='', users=''):
        api = self.baseUrl + "settings/audit"
        params = urllib.urlencode({
                                    'rotateInterval':'{0}'.format(rotateInterval),
                                    'auditdEnabled':'{0}'.format(enabled),
                                    'logPath':'{0}'.format(logPath),
                                    'disabled':'{0}'.format(disabled),
                                    'disabledUsers':'{0}'.format(users)
                                    })
        status, content, header = self._http_request(api, 'POST', params)
        self.test_log.info("Value os status is {0}".format(status))
        self.test_log.info("Value of content is {0}".format(content))
        if (status):
            return status
        else:
            return status, json.loads(content)

    'Get list of all roles that exist in the system'
    def retrive_all_user_role(self):
        url = "/settings/rbac/roles"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'GET')
        if not status:
            raise Exception(content)
        return json.loads(content)

    'Get list of current users and rols assigned to them'
    def retrieve_user_roles(self):
        url = "/settings/rbac/users"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'GET')
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Add/Update user role assignment
    user_id=userid of the user to act on
    payload=name=<nameofuser>&roles=admin,cluster_admin'''
    def set_user_roles(self, user_id, payload):
        url = "settings/rbac/users/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete user from couchbase role assignment
    user_id=userid of user to act on'''
    def delete_user_roles(self, user_id):
        url = "/settings/rbac/users/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'DELETE')
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Return list of permission with True/False if user has permission or not
    user_id = userid for checking permission
    password = password for userid
    permission_set=cluster.bucket[default].stats!read,cluster.bucket[default]!write
    '''
    def check_user_permission(self, user_id, password, permission_set):
        url = "pools/default/checkPermissions/"
        api = self.baseUrl + url
        authorization = base64.encodestring('%s:%s' % (user_id, password))
        header = {'Content-Type': 'application/x-www-form-urlencoded',
              'Authorization': 'Basic %s' % authorization,
              'Accept': '*/*'}
        status, content, header = self._http_request(api, 'POST', params=permission_set, headers=header)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Add/Update user role assignment
    user_id=userid of the user to act on
    payload=name=<nameofuser>&roles=admin,cluster_admin&password=<password>
    if roles=<empty> user will be created with no roles'''
    def add_set_builtin_user(self, user_id, payload):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete built-in user
    '''
    def delete_builtin_user(self, user_id):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'DELETE')
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Add/Update user role assignment
    user_id=userid of the user to act on
    password=<new password>'''
    def change_password_builtin_user(self, user_id, password):
        url = "controller/changePassword/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'POST', password)
        if not status:
            raise Exception(content)
        return json.loads(content)

    # the same as Preview a Random Document on UI
    def get_random_key(self, bucket):
        api = self.baseUrl + 'pools/default/buckets/%s/localRandomKey' % (bucket)
        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to get random document/key for bucket %s" % (bucket))
        return json_parsed

    # These methods are added change rebalance settings like rebalanceMovesPerNode
    # See https://docs.couchbase.com/server/current/rest-api/rest-limit-rebalance-moves.html for more details
    def set_rebalance_settings(self, body):
        url = "settings/rebalance"
        api = self.baseUrl + url
        params = urllib.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', headers=headers, params=params)

        if not status:
            raise Exception(content)
        return content

    def get_rebalance_settings(self):
        url = "settings/rebalance"
        api = self.baseUrl + url
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'GET', headers=headers)

        if not status:
            raise Exception(content)
        return content

    # These methods are added for Auto-Rebalance On Failure tests
    def set_retry_rebalance_settings(self, body):
        url = "settings/retryRebalance"
        api = self.baseUrl + url
        params = urllib.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', headers=headers, params=params)

        if not status:
            raise Exception(content)
        return content

    def get_retry_rebalance_settings(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "settings/retryRebalance"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)

        if not status:
            raise Exception(content)
        return content

    def get_pending_rebalance_info(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "pools/default/pendingRetryRebalance"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)

        if not status:
            raise Exception(content)
        return content

    def cancel_pending_rebalance(self, id):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        url = "controller/cancelRebalanceRetry/" + str(id)
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)

        if not status:
            raise Exception(content)
        return content


class MembaseServerVersion:
    def __init__(self, implementationVersion='', componentsVersion=''):
        self.implementationVersion = implementationVersion
        self.componentsVersion = componentsVersion


# this class will also contain more node related info
class OtpNode(object):
    def __init__(self, id='', status=''):
        self.id = id
        self.ip = ''
        self.replication = ''
        self.port = constants.port
        self.gracefulFailoverPossible = 'true'
        # extract ns ip from the otpNode string
        # its normally ns_1@10.20.30.40
        if id.find('@') >= 0:
            self.ip = id[id.index('@') + 1:]
            if self.ip.count(':') > 0:
                # raw ipv6? enclose in square brackets
                self.ip = '[' + self.ip + ']'
        self.status = status


class NodeInfo(object):
    def __init__(self):
        self.availableStorage = None  # list
        self.memoryQuota = None


class NodeDataStorage(object):
    def __init__(self):
        self.type = ''  # hdd or ssd
        self.path = ''
        self.index_path = ''
        self.cbas_path = ''
        self.data_path = ''
        self.quotaMb = ''
        self.state = ''  # ok

    def __str__(self):
        return '{0}'.format({'type': self.type,
                             'path': self.path,
                             'index_path' : self.index_path,
                             'quotaMb': self.quotaMb,
                             'state': self.state})

    def get_data_path(self):
        return self.path

    def get_index_path(self):
        return self.index_path


class NodeDiskStorage(object):
    def __init__(self):
        self.type = 0
        self.path = ''
        self.sizeKBytes = 0
        self.usagePercent = 0


class Node(object):
    def __init__(self):
        self.uptime = 0
        self.memoryTotal = 0
        self.memoryFree = 0
        self.mcdMemoryReserved = 0
        self.mcdMemoryAllocated = 0
        self.status = ""
        self.hostname = ""
        self.clusterCompatibility = ""
        self.clusterMembership = ""
        self.version = ""
        self.os = ""
        self.ports = []
        self.availableStorage = []
        self.storage = []
        self.memoryQuota = 0
        self.moxi = constants.moxi_port
        self.memcached = constants.memcached_port
        self.id = ""
        self.ip = ""
        self.rest_username = ""
        self.rest_password = ""
        self.port = constants.port
        self.services = []
        self.storageTotalRam = 0


class AutoFailoverSettings(object):
    def __init__(self):
        self.enabled = True
        self.timeout = 0
        self.count = 0
        self.failoverOnDataDiskIssuesEnabled = False
        self.failoverOnDataDiskIssuesTimeout = 0
        self.maxCount = 1
        self.failoverServerGroup = False
        self.can_abort_rebalance = False


class AutoReprovisionSettings(object):
    def __init__(self):
        self.enabled = True
        self.max_nodes = 0
        self.count = 0


class NodePort(object):
    def __init__(self):
        self.proxy = 0
        self.direct = 0


class RestParser(object):
    def parse_index_status_response(self, parsed):
        index_map = dict()
        for index_map in parsed["indexes"]:
            bucket_name = index_map['bucket'].encode('ascii', 'ignore')
            if bucket_name not in index_map.keys():
                index_map[bucket_name] = {}
            index_name = index_map['index'].encode('ascii', 'ignore')
            index_map[bucket_name][index_name] = {}
            index_map[bucket_name][index_name]['status'] = \
                index_map['status'].encode('ascii', 'ignore')
            index_map[bucket_name][index_name]['progress'] = \
                str(index_map['progress']).encode('ascii', 'ignore')
            index_map[bucket_name][index_name]['definition'] = \
                index_map['definition'].encode('ascii', 'ignore')
            index_map[bucket_name][index_name]['hosts'] = \
                index_map['hosts'][0].encode('ascii', 'ignore')
            index_map[bucket_name][index_name]['id'] = index_map['id']
        return index_map

    def parse_get_nodes_response(self, parsed):
        node = Node()
        node.uptime = parsed['uptime']
        node.memoryFree = parsed['memoryFree']
        node.memoryTotal = parsed['memoryTotal']
        node.mcdMemoryAllocated = parsed['mcdMemoryAllocated']
        node.mcdMemoryReserved = parsed['mcdMemoryReserved']

        if 'indexMemoryQuota' in parsed:
            node.indexMemoryQuota = parsed['indexMemoryQuota']
        if 'ftsMemoryQuota' in parsed:
            node.ftsMemoryQuota = parsed['ftsMemoryQuota']
        if 'cbasMemoryQuota' in parsed:
            node.cbasMemoryQuota = parsed['cbasMemoryQuota']
        if 'eventingMemoryQuota' in parsed:
            node.eventingMemoryQuota = parsed['eventingMemoryQuota']

        node.status = parsed['status']
        node.hostname = parsed['hostname']
        node.clusterCompatibility = parsed['clusterCompatibility']
        node.clusterMembership = parsed['clusterMembership']
        node.version = parsed['version']
        node.curr_items = 0
        if 'interestingStats' in parsed \
                and 'curr_items' in parsed['interestingStats']:
            node.curr_items = parsed['interestingStats']['curr_items']
        node.port = parsed["hostname"][parsed["hostname"].rfind(":") + 1:]
        node.os = parsed['os']

        if "services" in parsed:
            node.services = parsed["services"]
        if "otpNode" in parsed:
            node.id = parsed["otpNode"]
        if "hostname" in parsed:
            # should work for both: ipv4 and ipv6
            node.ip = parsed["hostname"].rsplit(":", 1)[0]

        # memoryQuota
        if 'memoryQuota' in parsed:
            node.memoryQuota = parsed['memoryQuota']
        if 'availableStorage' in parsed:
            available_storage = parsed['availableStorage']
            for key in available_storage:
                # let's assume there is only one disk in each node
                dict_parsed = parsed['availableStorage']
                if 'path' in dict_parsed and 'sizeKBytes' in dict_parsed \
                        and 'usagePercent' in dict_parsed:
                    disk_storage = NodeDiskStorage()
                    disk_storage.path = dict_parsed['path']
                    disk_storage.sizeKBytes = dict_parsed['sizeKBytes']
                    disk_storage.type = key
                    disk_storage.usagePercent = dict_parsed['usagePercent']
                    node.availableStorage.append(disk_storage)
                    self.test_log.info(disk_storage)

        if 'storage' in parsed:
            storage = parsed['storage']
            for key in storage:
                disk_storage_list = storage[key]
                for dict_parsed in disk_storage_list:
                    if 'path' in dict_parsed and 'state' in dict_parsed \
                            and 'quotaMb' in dict_parsed:
                        data_storage = NodeDataStorage()
                        data_storage.path = dict_parsed['path']
                        data_storage.index_path = dict_parsed.get('index_path',
                                                                  '')
                        data_storage.quotaMb = dict_parsed['quotaMb']
                        data_storage.state = dict_parsed['state']
                        data_storage.type = key
                        node.storage.append(data_storage)

        # Format: ports={"proxy":11211,"direct":11210}
        if "ports" in parsed:
            ports = parsed["ports"]
            if "proxy" in ports:
                node.moxi = ports["proxy"]
            if "direct" in ports:
                node.memcached = ports["direct"]

        if "storageTotals" in parsed:
            storage_totals = parsed["storageTotals"]
            if storage_totals.get("ram"):
                if storage_totals["ram"].get("total"):
                    ram_kb = storage_totals["ram"]["total"]
                    node.storageTotalRam = ram_kb/(1024*1024)

                    if IS_CONTAINER:
                        # the storage total values are more accurate than
                        # mcdMemoryReserved - which is container host memory
                        node.mcdMemoryReserved = node.storageTotalRam * 0.70
        return node
