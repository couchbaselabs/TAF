"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

import json
import time
import urllib

from bucket import Bucket
from membase.api.exception import \
    BucketCreationException, GetBucketInfoFailed, \
    BucketCompactionException
from Rest_Connection import RestConnection


class BucketHelper(RestConnection):
    def __init__(self, server):
        super(BucketHelper, self).__init__(server)

    def bucket_exists(self, bucket):
        try:
            buckets = self.get_buckets_json()
            names = [item.name for item in buckets]
            self.log.debug("Node %s existing buckets: %s" % (self.ip, names))
            for item in buckets:
                if item.name == bucket:
                    self.log.debug("Node %s found bucket %s"
                                   % (bucket, self.ip))
                    return True
            return False
        except Exception:
            return False

    def get_bucket_from_cluster(self, bucket, num_attempt=1, timeout=1):
        api = '%s%s%s?basic_stats=true' \
               % (self.baseUrl, 'pools/default/buckets/', bucket.name)
        status, content, _ = self._http_request(api)
        num = 1
        while not status and num_attempt > num:
            self.sleep("Try to get {0} again after {1} sec"
                       .format(api, timeout))
            time.sleep(timeout)
            status, content, _ = self._http_request(api)
            num += 1
        if status:
            parsed = json.loads(content)
            if 'vBucketServerMap' in parsed:
                vBucketServerMap = parsed['vBucketServerMap']
                serverList = vBucketServerMap['serverList']
                bucket.servers.extend(serverList)
                if "numReplicas" in vBucketServerMap:
                    bucket.replicaNumber = vBucketServerMap["numReplicas"]
                # vBucketMapForward
                if 'vBucketMapForward' in vBucketServerMap:
                    # let's gather the forward map
                    vBucketMapForward = vBucketServerMap['vBucketMapForward']
                    counter = 0
                    for vbucket in vBucketMapForward:
                        # there will be n number of replicas
                        vbucketInfo = Bucket.vBucket()
                        vbucketInfo.master = serverList[vbucket[0]]
                        if vbucket:
                            for i in range(1, len(vbucket)):
                                if vbucket[i] != -1:
                                    vbucketInfo.replica.append(serverList[vbucket[i]])
                        vbucketInfo.id = counter
                        counter += 1
                        bucket.forward_map.append(vbucketInfo)
                vBucketMap = vBucketServerMap['vBucketMap']
                counter = 0
                # Reset the list to avoid appending through multiple calls
                bucket.vbuckets = list()
                for vbucket in vBucketMap:
                    # there will be n number of replicas
                    vbucketInfo = Bucket.vBucket()
                    vbucketInfo.master = serverList[vbucket[0]]
                    if vbucket:
                        for i in range(1, len(vbucket)):
                            if vbucket[i] != -1:
                                vbucketInfo.replica.append(serverList[vbucket[i]])
                    vbucketInfo.id = counter
                    counter += 1
                    bucket.vbuckets.append(vbucketInfo)
            bucket.vbActiveNumNonResident = 100
            if "vbActiveNumNonResident" in parsed["basicStats"]:
                bucket.vbActiveNumNonResident = \
                    parsed["basicStats"]["vbActiveNumNonResident"]
            bucket.maxTTL = parsed["maxTTL"]
        return bucket

    def get_buckets_json(self):
        api = '{0}{1}'.format(self.baseUrl,
                              'pools/default/buckets?basic_stats=true')
        status, content, _ = self._http_request(api)
        if not status:
            self.log.error("Error while getting {0}. Please retry".format(api))
            raise GetBucketInfoFailed("all_buckets", content)
        return json.loads(content)

    def vbucket_map_ready(self, bucket, timeout_in_seconds=360):
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            vBuckets = self.get_vbuckets(bucket)
            if vBuckets:
                return True
            else:
                time.sleep(0.5)
        msg = 'Vbucket map not ready for bucket {0} after waiting {1} seconds'
        self.log.warn(msg.format(bucket, timeout_in_seconds))
        return False

    def get_vbuckets(self, bucket):
        self.get_bucket_from_cluster(bucket)
        return None if not bucket.vbuckets else bucket.vbuckets

    def _get_vbuckets(self, servers, bucket_name='default'):
        target_server = list()
        if bucket_name is None:
            bucket_name = self.get_buckets_json()[0]["name"]
        bucket_to_check = self.get_bucket_json(bucket_name)
        bucket_servers = bucket_to_check["vBucketServerMap"]["serverList"]
        bucket_servers = [ip.split(":")[0] for ip in bucket_servers]

        vbuckets_servers = dict()
        for server in servers:
            vbuckets_servers[server] = dict()
            vbuckets_servers[server]['active_vb'] = list()
            vbuckets_servers[server]['replica_vb'] = list()

        for server in bucket_servers:
            for tem_server in servers:
                if tem_server.ip == server:
                    target_server.append(tem_server)

        target_server_len = len(target_server)
        for vb_num, vb_map in enumerate(bucket_to_check["vBucketServerMap"]["vBucketMap"]):
            for index, vb_index in enumerate(vb_map):
                if index >= target_server_len:
                    continue
                vb_index = int(vb_index)
                if vb_index == 0:
                    vbuckets_servers[target_server[index]]["active_vb"].append(vb_num)
                elif vb_index > 0:
                    vbuckets_servers[target_server[index]]["replica_vb"].append(vb_num)
        return vbuckets_servers

    def fetch_vbucket_map(self, bucket="default"):
        """Return vbucket map for bucket
        Keyword argument:
        bucket -- bucket name
        """
        api = self.baseUrl + 'pools/default/buckets/' + bucket
        _, content, _ = self._http_request(api)
        _stats = json.loads(content)
        return _stats['vBucketServerMap']['vBucketMap']

    def get_vbucket_map_and_server_list(self, bucket="default"):
        """ Return server list, replica and vbuckets map
        that matches to server list """
        # vbucket_map = self.fetch_vbucket_map(bucket)
        api = self.baseUrl + 'pools/default/buckets/' + bucket
        _, content, _ = self._http_request(api)
        _stats = json.loads(content)
        num_replica = _stats['vBucketServerMap']['numReplicas']
        vbucket_map = _stats['vBucketServerMap']['vBucketMap']
        servers = _stats['vBucketServerMap']['serverList']
        server_list = []
        for node in servers:
            node = node.split(":")
            server_list.append(node[0])
        return vbucket_map, server_list, num_replica

    def get_bucket_stats_for_node(self, bucket='default', node=None):
        if not node:
            self.log.critical('node_ip not specified')
            return None
        stats = {}
        api = "{0}{1}{2}{3}{4}:{5}{6}" \
              .format(self.baseUrl, 'pools/default/buckets/',
                      bucket, "/nodes/", node.ip, node.port, "/stats")
        status, content, _ = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                if stat_name not in stats:
                    if len(samples[stat_name]) == 0:
                        stats[stat_name] = []
                    else:
                        stats[stat_name] = samples[stat_name][-1]
                else:
                    raise Exception("Duplicate entry in the stats command {0}"
                                    .format(stat_name))
        return stats

    def get_bucket_status(self, bucket):
        if not bucket:
            self.log.critical("Bucket Name not Specified")
            return None
        api = self.baseUrl + 'pools/default/buckets'
        status, content, _ = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            for item in json_parsed:
                if item["name"] == bucket:
                    return item["nodes"][0]["status"]
            self.log.warning("Bucket {} doesn't exist".format(bucket))
            return None

    def fetch_bucket_stats(self, bucket='default', zoom='minute'):
        """Return deserialized buckets stats.
        Keyword argument:
        bucket -- bucket name
        zoom -- stats zoom level (minute | hour | day | week | month | year)
        """
        api = self.baseUrl + 'pools/default/buckets/{0}/stats?zoom={1}' \
                             .format(bucket, zoom)
        status, content, _ = self._http_request(api)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def fetch_bucket_xdcr_stats(self, bucket='default', zoom='minute'):
        """Return deserialized bucket xdcr stats.
        Keyword argument:
        bucket -- bucket name
        zoom -- stats zoom level (minute | hour | day | week | month | year)
        """
        api = self.baseUrl \
              + 'pools/default/buckets/@xdcr-{0}/stats?zoom={1}' \
                .format(bucket, zoom)
        _, content, _ = self._http_request(api)
        return json.loads(content)

    def get_bucket_stats(self, bucket='default'):
        stats = {}
        status, json_parsed = self.get_bucket_stats_json(bucket)
        if status:
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                if samples[stat_name]:
                    last_sample = len(samples[stat_name]) - 1
                    if last_sample:
                        stats[stat_name] = samples[stat_name][last_sample]
        return stats

    def get_bucket_stats_json(self, bucket_name='default'):
        api = "{0}{1}{2}{3}".format(self.baseUrl, 'pools/default/buckets/',
                                    bucket_name, "/stats")
        status, content, _ = self._http_request(api)
        json_parsed = json.loads(content)
        return status, json_parsed

    def get_bucket_json(self, bucket_name='default'):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
                                 bucket_name)
        status, content, _ = self._http_request(api)
        if not status:
            self.log.error("Error while getting {0}. Please retry".format(api))
            raise GetBucketInfoFailed(bucket_name, content)
        return json.loads(content)

    def delete_bucket(self, bucket='default'):
        api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket)
        status, _, header = self._http_request(api, 'DELETE')

        if int(header['status']) == 500:
            # According to http://docs.couchbase.com/couchbase-manual-2.5/cb-rest-api/#deleting-buckets
            # the cluster will return with 500 if it failed to nuke
            # the bucket on all of the nodes within 30 secs
            self.log.warn("Bucket deletion timed out waiting for all nodes")

        return status

    '''Load any of the three sample buckets'''
    def load_sample(self, sample_name):
        api = '{0}{1}'.format(self.baseUrl, "sampleBuckets/install")
        data = '["{0}"]'.format(sample_name)
        status, _, _ = self._http_request(api, 'POST', data)
        # Sleep to allow the sample bucket to be loaded
        time.sleep(10)
        return status

    # figure out the proxy port
    def create_bucket(self, bucket_params={}):
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets')
        init_params = {'name': bucket_params.get('name'),
                       'ramQuotaMB': bucket_params.get('ramQuotaMB'),
                       'replicaNumber': bucket_params.get('replicaNumber'),
                       'bucketType': bucket_params.get('bucketType'),
                       'replicaIndex': bucket_params.get('replicaIndex'),
                       'threadsNumber': bucket_params.get('threadsNumber'),
                       'flushEnabled': bucket_params.get('flushEnabled'),
                       'evictionPolicy': bucket_params.get('evictionPolicy'),
                       'compressionMode': bucket_params.get('compressionMode'),
                       'maxTTL': bucket_params.get('maxTTL'),
                       'storageBackend': bucket_params.get('storageBackend')}

        # Remove 'replicaNumber' in case of MEMCACHED bucket
        if bucket_params.get("bucketType") == Bucket.Type.MEMCACHED:
            init_params.pop('replicaNumber', None)

        # Remove 'replicaIndex' parameter in case of EPHEMERAL bucket
        if bucket_params.get("bucketType") == Bucket.Type.EPHEMERAL:
            init_params.pop('replicaIndex', None)

        if bucket_params.get('lww'):
            init_params['conflictResolutionType'] = 'lww'

        params = urllib.urlencode(init_params)

        self.log.info("Creating '%s' bucket %s"
                      % (init_params['bucketType'], init_params['name']))
        self.log.debug("{0} with param: {1}".format(api, params))
        create_start_time = time.time()

        maxwait = 60
        for numsleep in range(maxwait):
            status, content, header = self._http_request(api, 'POST', params)
            if status:
                break
            elif (int(header['status']) == 503 and
                    '{"_":"Bucket with given name still exists"}' in content):
                self.sleep(1, "The bucket still exists, sleep 1 sec and retry")
            else:
                raise BucketCreationException(
                    ip=self.ip, bucket_name=bucket_params.get('name'))

        if (numsleep + 1) == maxwait:
            self.log.warning("Failed creating the bucket after {0} secs"
                             .format(maxwait))
            raise BucketCreationException(
                ip=self.ip, bucket_name=bucket_params.get('name'))

        create_time = time.time() - create_start_time
        self.log.debug("{0:.02f} seconds to create bucket {1}"
                       .format(round(create_time, 2),
                               bucket_params.get('name')))
        return status

    def update_memcached_settings(self, num_writer_threads="default",
                                  num_reader_threads="default"):
        api = "%s%s" % (self.baseUrl,
                        "pools/default/settings/memcached/global")
        params_dict = dict()
        params_dict["num_writer_threads"] = num_writer_threads
        params_dict["num_reader_threads"] = num_reader_threads

        params = urllib.urlencode(params_dict)
        self.log.info("Updating memcached properties")
        self.log.debug("%s with param: %s" % (api, params))

        status, content, _ = self._http_request(api, 'POST', params)
        if not status:
            self.log.error("Failed to update memcached settings: %s"
                           % content)
        self.log.debug("Memcached settings updated")
        return status

    def change_bucket_props(self, bucket, ramQuotaMB=None, authType=None,
                            saslPassword=None, replicaNumber=None,
                            proxyPort=None, replicaIndex=None,
                            flushEnabled=None, timeSynchronization=None,
                            maxTTL=None, compressionMode=None):

        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
                                 bucket)
        params_dict = {}
        if ramQuotaMB:
            params_dict["ramQuotaMB"] = ramQuotaMB
        if authType:
            params_dict["authType"] = authType
        if saslPassword:
            params_dict["authType"] = "sasl"
            params_dict["saslPassword"] = saslPassword
        if replicaNumber is not None:
            params_dict["replicaNumber"] = replicaNumber
        # if proxyPort:
        #     params_dict["proxyPort"] = proxyPort
        if replicaIndex:
            params_dict["replicaIndex"] = replicaIndex
        if flushEnabled:
            params_dict["flushEnabled"] = flushEnabled
        if timeSynchronization:
            params_dict["timeSynchronization"] = timeSynchronization
        if maxTTL:
            params_dict["maxTTL"] = maxTTL
        if compressionMode:
            params_dict["compressionMode"] = compressionMode
        params = urllib.urlencode(params_dict)

        self.log.info("Updating bucket properties for %s" % bucket)
        self.log.debug("%s with param: %s" % (api, params))
        status, content, _ = self._http_request(api, 'POST', params)
        if timeSynchronization:
            if status:
                raise Exception("Erroneously able to set bucket settings %s for bucket on time-sync" % (params, bucket))
            return status, content
        if not status:
            raise Exception("Unable to set bucket settings %s for bucket"
                            % (params, bucket))
        self.log.debug("Bucket %s updated" % bucket)
        bucket.__dict__.update(params_dict)
        return status

    def get_auto_compaction_settings(self):
        api = self.baseUrl + "settings/autoCompaction"
        _, content, _ = self._http_request(api)
        return json.loads(content)

    def set_auto_compaction(self, parallelDBAndVC="false",
                            dbFragmentThreshold=None,
                            viewFragmntThreshold=None,
                            dbFragmentThresholdPercentage=None,
                            viewFragmntThresholdPercentage=None,
                            allowedTimePeriodFromHour=None,
                            allowedTimePeriodFromMin=None,
                            allowedTimePeriodToHour=None,
                            allowedTimePeriodToMin=None,
                            allowedTimePeriodAbort=None,
                            bucket=None):
        """Reset compaction values to default, try with old fields (dp4 build)
        and then try with newer fields"""
        params = {}
        api = self.baseUrl

        if bucket is None:
            # setting is cluster wide
            api = api + "controller/setAutoCompaction"
        else:
            # overriding per/bucket compaction setting
            api = api + "pools/default/buckets/" + bucket
            params["autoCompactionDefined"] = "true"
            # reuse current ram quota in mb per node
#             num_nodes = len(self.node_statuses())
            bucket_info = self.get_bucket_json(bucket)
#             quota = self.get_bucket_json(bucket)["quota"]["ram"] / (1048576 * num_nodes)
#             params["ramQuotaMB"] = quota
            if bucket_info["authType"] == "sasl" and bucket_info["name"] != "default":
                params["authType"] = self.get_bucket_json(bucket)["authType"]
                params["saslPassword"] = self.get_bucket_json(bucket)["saslPassword"]

        params["parallelDBAndViewCompaction"] = parallelDBAndVC
        # Need to verify None because the value could be = 0
        if dbFragmentThreshold is not None:
            params["databaseFragmentationThreshold[size]"] = dbFragmentThreshold
        if viewFragmntThreshold is not None:
            params["viewFragmentationThreshold[size]"] = viewFragmntThreshold
        if dbFragmentThresholdPercentage is not None:
            params["databaseFragmentationThreshold[percentage]"] = dbFragmentThresholdPercentage
        if viewFragmntThresholdPercentage is not None:
            params["viewFragmentationThreshold[percentage]"] = viewFragmntThresholdPercentage
        if allowedTimePeriodFromHour is not None:
            params["allowedTimePeriod[fromHour]"] = allowedTimePeriodFromHour
        if allowedTimePeriodFromMin is not None:
            params["allowedTimePeriod[fromMinute]"] = allowedTimePeriodFromMin
        if allowedTimePeriodToHour is not None:
            params["allowedTimePeriod[toHour]"] = allowedTimePeriodToHour
        if allowedTimePeriodToMin is not None:
            params["allowedTimePeriod[toMinute]"] = allowedTimePeriodToMin
        if allowedTimePeriodAbort is not None:
            params["allowedTimePeriod[abortOutside]"] = allowedTimePeriodAbort

        params = urllib.urlencode(params)
        self.log.debug("Bucket '%s' settings will be changed with params: %s"
                       % (bucket, params))
        return self._http_request(api, "POST", params)

    def disable_auto_compaction(self):
        """
           Cluster-wide Setting
              Disable autocompaction on doc and view
        """
        api = self.baseUrl + "controller/setAutoCompaction"
        self.log.info("Disable autocompaction in cluster-wide setting")
        status, _, _ = self._http_request(api, "POST",
                                          "parallelDBAndViewCompaction=false")
        return status

    def flush_bucket(self, bucket="default"):
        bucket_name = bucket
        self.log.info("Triggering bucket flush for '%s'" % bucket_name)
        api = self.baseUrl + "pools/default/buckets/{0}/controller/doFlush" \
            .format(bucket_name)
        status, _, _ = self._http_request(api, 'POST')
        self.log.debug("Bucket flush '%s' triggered" % bucket_name)
        return status

    def get_bucket_CCCP(self, bucket):
        self.log.debug("Getting CCCP config")
        api = '%spools/default/b/%s' % (self.baseUrl, bucket)
        status, content, _ = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def compact_bucket(self, bucket="default"):
        self.log.debug("Triggering bucket compaction for '%s'" % bucket)
        api = self.baseUrl \
              + 'pools/default/buckets/{0}/controller/compactBucket' \
                .format(bucket)
        status, _, _ = self._http_request(api, 'POST')
        if status:
            self.log.debug('Bucket compaction successful')
        else:
            raise BucketCompactionException(bucket)

        return True

    def cancel_bucket_compaction(self, bucket="default"):
        self.log.debug("Stopping bucket compaction for '%s'" % bucket)
        api = self.baseUrl \
              + 'pools/default/buckets/{0}/controller/cancelBucketCompaction' \
                .format(bucket)
        status, _, _ = self._http_request(api, 'POST')
        if status:
            self.log.debug('Cancel bucket compaction successful')
        else:
            raise BucketCompactionException(bucket)
        return True

    def get_xdc_queue_size(self, bucket):
        """Fetch bucket stats and return the latest value of XDC replication
        queue size"""
        bucket_stats = self.fetch_bucket_xdcr_stats(bucket)
        return bucket_stats['op']['samples']['replication_changes_left'][-1]

    def get_dcp_queue_size(self, bucket):
        """Fetch bucket stats and return the latest value of DCP
        queue size"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['ep_dcp_xdcr_items_remaining'][-1]

    def get_active_key_count(self, bucket):
        """Fetch bucket stats and return the bucket's curr_items count"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['curr_items'][-1]

    def get_replica_key_count(self, bucket):
        """Fetch bucket stats and return the bucket's replica count"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['vb_replica_curr_items'][-1]

    # the same as Preview a Random Document on UI
    def get_random_key(self, bucket):
        api = self.baseUrl + 'pools/default/buckets/{0}/localRandomKey' \
                             .format(bucket)
        status, content, _ = self._http_request(
            api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to get random document/key for bucket %s"
                            % bucket)
        return json_parsed

    '''
        Add/Update user role assignment
        user_id=userid of the user to act on
        payload=name=<nameofuser>&roles=admin,cluster_admin&password=<password>
        if roles=<empty> user will be created with no roles'''

    def add_set_builtin_user(self, user_id, payload):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        status, content, _ = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete built-in user
    '''

    def delete_builtin_user(self, user_id):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        status, content, _ = self._http_request(api, 'DELETE')
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
        status, content, _ = self._http_request(api,
                                                'POST',
                                                password)
        if not status:
            raise Exception(content)
        return json.loads(content)

    # Collection/Scope specific APIs
    def create_collection(self, bucket, scope, collection):
        api = self.baseUrl + 'pools/default/buckets/%s/collections/%s' \
                             % (bucket, scope)
        params = {'name': collection}
        params = urllib.urlencode(params)
        headers = self._create_headers()
        status, content, _ = self._http_request(api,
                                                'POST',
                                                params=params,
                                                headers=headers)
        return status, content

    def create_scope(self, bucket, scope):
        api = self.baseUrl + 'pools/default/buckets/%s/collections' % bucket
        params = urllib.urlencode({'name': scope})
        headers = self._create_headers()
        status, content, _ = self._http_request(api,
                                                'POST',
                                                params=params,
                                                headers=headers)
        return status, content

    def delete_scope(self, bucket, scope):
        api = self.baseUrl + 'pools/default/buckets/%s/collections/%s' \
                             % (bucket, scope)
        headers = self._create_headers()
        status, content, _ = self._http_request(api,
                                                'DELETE',
                                                headers=headers)
        return status, content

    def delete_collection(self, bucket, scope, collection):
        api = self.baseUrl + 'pools/default/buckets/%s/collections/%s/%s' \
                             % (bucket, scope, collection)
        headers = self._create_headers()
        status, content, _ = self._http_request(api,
                                                'DELETE',
                                                headers=headers)
        return status, content
