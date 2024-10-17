"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

import json
import time
import urllib

import requests.utils

import global_vars
from BucketLib.bucket import Bucket
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from common_lib import sleep
from custom_exceptions.exception import \
    GetBucketInfoFailed, \
    BucketCompactionException


class BucketHelper(BucketRestApi):
    def __init__(self, server):
        super(BucketHelper, self).__init__(server)
        self.server = server

    def bucket_exists(self, bucket):
        try:
            buckets = self.get_buckets_json()
            names = [item["name"] for item in buckets]
            self.log.debug("Node %s existing buckets: %s" % (self.ip, names))
            for item in buckets:
                if item["name"] == bucket:
                    self.log.debug("Node %s found bucket %s"
                                   % (bucket, self.ip))
                    return True
            return False
        except Exception as e:
            self.log.info(e)
            return False

    def get_bucket_from_cluster(self, bucket, num_attempt=1, timeout=1):
        status, parsed = self.get_bucket_info(bucket.name, basic_stats=True)
        num = 1
        while not status and num_attempt > num:
            sleep(timeout, f"{bucket.name} - Retrying get_basic_stats...",
                  log_type="infra")
            status, parsed = self.get_bucket_info(bucket, basic_stats=True)
            num += 1
        if status:
            if 'vBucketServerMap' in parsed:
                vBucketServerMap = parsed['vBucketServerMap']
                serverList = vBucketServerMap['serverList']
                bucket.servers.extend(serverList)
                bucket.servers = list(set(bucket.servers))
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
            if "basicStats" in parsed and "vbActiveNumNonResident" in parsed["basicStats"]:
                bucket.vbActiveNumNonResident = \
                    parsed["basicStats"]["vbActiveNumNonResident"]
            bucket.maxTTL = parsed.get("maxTTL")
            bucket.rank = parsed.get("rank")
        return bucket

    def get_buckets_json(self, bucket_name=None):
        status, content = self.get_bucket_info(bucket_name)
        if not status:
            self.log.error(f"Failed to get {bucket_name} info")
            raise GetBucketInfoFailed(bucket_name, content)
        return content

    def vbucket_map_ready(self, bucket, timeout_in_seconds=360):
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            v_buckets = self.get_vbuckets(bucket)
            if v_buckets:
                return True
            sleep(0.5, "Wait before retrying get_vbs call", log_type="infra")
        msg = 'Vbucket map not ready for bucket {0} after waiting {1} seconds'
        self.log.warn(msg.format(bucket, timeout_in_seconds))
        return False

    def get_vbuckets(self, bucket):
        self.get_bucket_from_cluster(bucket)
        return None if not bucket.vbuckets else bucket.vbuckets

    def _get_vbuckets(self, servers, bucket_name='default'):
        target_servers = [server.ip for server in servers]
        if bucket_name is None:
            bucket_name = self.get_buckets_json()[0]["name"]
        bucket_to_check = self.get_buckets_json(bucket_name)
        bucket_servers = bucket_to_check["vBucketServerMap"]["serverList"]
        bucket_servers = [ip.split(":")[0] for ip in bucket_servers]

        vbuckets_servers = dict()
        for server in bucket_servers:
            vbuckets_servers[server] = dict()
            vbuckets_servers[server]['active_vb'] = list()
            vbuckets_servers[server]['replica_vb'] = list()

        for vb_num, vb_map in enumerate(bucket_to_check["vBucketServerMap"]["vBucketMap"]):
            vbuckets_servers[bucket_servers[int(vb_map[0])]]["active_vb"].append(vb_num)
            for vb_index in vb_map[1:]:
                if vb_index == -1:
                    continue
                vbuckets_servers[bucket_servers[int(vb_index)]]["replica_vb"].append(vb_num)

        for server in set(bucket_servers) - set(target_servers):
            vbuckets_servers.pop(server)
        return vbuckets_servers

    def fetch_vbucket_map(self, bucket="default"):
        """Return vbucket map for bucket
        Keyword argument:
        bucket -- bucket name
        """
        _, json_parsed = self.get_bucket_info(bucket_name=bucket)
        return json_parsed['vBucketServerMap']['vBucketMap']

    def get_vbucket_map_and_server_list(self, bucket="default"):
        """ Return server list, replica and vbuckets map
        that matches to server list """
        _, json_parsed = self.get_bucket_info(bucket_name=bucket)
        num_replica = json_parsed['vBucketServerMap']['numReplicas']
        vbucket_map = json_parsed['vBucketServerMap']['vBucketMap']
        servers = json_parsed['vBucketServerMap']['serverList']
        server_list = list()
        for node in servers:
            node = node.split(":")
            server_list.append(node[0])
        return vbucket_map, server_list, num_replica

    def get_bucket_stats_for_node(self, bucket='default', node=None):
        if not node:
            self.log.critical('node_ip not specified')
            return None
        stats = dict()
        status, json_parsed = super().get_bucket_stats_from_node(bucket, node)
        if status:
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
        status, json_parsed = self.get_bucket_info()
        if status:
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
        status, content = super().get_bucket_stats(bucket, zoom=zoom)
        if not status:
            raise Exception(content)
        return content

    def fetch_bucket_xdcr_stats(self, bucket='default', zoom='minute'):
        """Return deserialized bucket xdcr stats.
        Keyword argument:
        bucket -- bucket name
        zoom -- stats zoom level (minute | hour | day | week | month | year)
        """
        api = self.base_url \
              + 'pools/default/buckets/@xdcr-{0}/stats?zoom={1}' \
                .format(urllib.quote_plus("%s" % bucket), zoom)
        _, content, _ = self._http_request(api)
        return json.loads(content)

    def get_bucket_stats(self, bucket='default', zoom=None):
        stats = {}
        status, json_parsed = super().get_bucket_stats(bucket, zoom=zoom)
        if status:
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                if samples[stat_name]:
                    last_sample = len(samples[stat_name]) - 1
                    if last_sample:
                        stats[stat_name] = samples[stat_name][last_sample]
        return stats

    def pause_bucket(self, bucket, s3_path=None, blob_storage_region=None, rate_limit=1024):
        api = '{0}{1}'.format(self.base_url, 'controller/pause')
        param_dict = {}
        param_dict["bucket"] = bucket
        param_dict["remote_path"] = s3_path
        param_dict["blob_storage_region"] = blob_storage_region
        param_dict["rate_limit"] = rate_limit
        params = json.dumps(param_dict)
        self.log.critical("Params: {0}".format(params))
        header = self.get_headers_for_content_type_json()
        status, content, _ = self._http_request(api, 'POST', params, headers=header)
        if not status:
            self.log.error("Failed to pause: {0}".format(content))
        json_parsed = json.loads(content)
        return status, json_parsed

    def stop_pause(self, bucket):
        api = '{0}{1}'.format(self.base_url, 'controller/stopPause')
        param_dict = {}
        param_dict["bucket"] = bucket
        params = json.dumps(param_dict)
        self.log.critical("Params: {0}".format(params))
        header = self.get_headers_for_content_type_json()
        status, content, _= self._http_request(api, 'POST', params, headers=header)
        if not status:
            self.log.error("Failed to stop pause: {0}".format(content))
        json_parsed = json.loads(content)
        return status, json_parsed

    def resume_bucket(self, bucket, s3_path=None, blob_storage_region=None, rate_limit=1024):
        api = '{0}{1}'.format(self.base_url, 'controller/resume')
        param_dict = {}
        param_dict["bucket"] = bucket
        param_dict["remote_path"] = s3_path
        param_dict["blob_storage_region"] = blob_storage_region
        param_dict["rate_limit"] = rate_limit
        params = json.dumps(param_dict)
        self.log.critical("Params: {0}".format(params))
        header = self.get_headers_for_content_type_json()
        status, content, _ = self._http_request(api, 'POST', params, headers=header)
        if not status:
            self.log.error("Failed to resume: {0}".format(content))
        json_parsed = json.loads(content)
        return status, json_parsed

    def stop_resume(self, bucket):
        api = '{0}{1}'.format(self.base_url, 'controller/stopResume')
        param_dict = {}
        param_dict["bucket"] = bucket
        params = json.dumps(param_dict)
        self.log.critical("Params: {0}".format(params))
        header = self.get_headers_for_content_type_json()
        status, content, _= self._http_request(api, 'POST', params, headers=header)
        if not status:
            self.log.error("Failed to stop resume: {0}".format(content))
        json_parsed = json.loads(content)
        return status, json_parsed

    def delete_bucket(self, bucket):

        b_name = requests.utils.quote(bucket.name)
        api = f"{self.base_url}/pools/default/buckets/{b_name}"
        status, content, response = self.http_request(api, self.DELETE)
        if not status:
            pass
        if int(response.status_code) == 500:
            # According to http://docs.couchbase.com/couchbase-manual-2.5/cb-rest-api/#deleting-buckets
            # the cluster will return with 500 if it failed to nuke
            # the bucket on all of the nodes within 30 secs
            self.log.warn("Bucket deletion timed out waiting for all nodes")

        return status

    '''Load any of the three sample buckets'''
    def load_sample(self, sample_name):
        status, _, _ = BucketRestApi.load_sample_bucket(self, [sample_name])
        return status

    # figure out the proxy port
    def create_bucket(self, bucket_params=dict()):
        init_params = {
            Bucket.name: bucket_params.get(Bucket.name),
            Bucket.ramQuotaMB: bucket_params.get(Bucket.ramQuotaMB),
            Bucket.replicaNumber: bucket_params.get(Bucket.replicaNumber),
            Bucket.bucketType: bucket_params.get(Bucket.bucketType),
            Bucket.priority: bucket_params.get(Bucket.priority),
            Bucket.flushEnabled: bucket_params.get(Bucket.flushEnabled),
            Bucket.evictionPolicy: bucket_params.get(Bucket.evictionPolicy),
            Bucket.storageBackend: bucket_params.get(Bucket.storageBackend),
            Bucket.conflictResolutionType:
                bucket_params.get(Bucket.conflictResolutionType),
            Bucket.durabilityMinLevel:
                bucket_params.get(Bucket.durabilityMinLevel)}

        # Set Bucket's width/weight param only if serverless is enabled
        if bucket_params.get('serverless'):
            serverless = bucket_params['serverless']
            init_params[Bucket.weight] = serverless.weight
            init_params[Bucket.width] = serverless.width
        num_vbs = bucket_params.get("numVBuckets")
        if num_vbs:
            init_params[Bucket.numVBuckets] = num_vbs

        is_enterprise = global_vars.cluster_util.is_enterprise_edition(
            cluster=None, server=self.server)
        if is_enterprise:
            init_params[Bucket.replicaIndex] = bucket_params.get(Bucket.replicaIndex)
            init_params[Bucket.compressionMode] = bucket_params.get(Bucket.compressionMode)
            init_params[Bucket.maxTTL] = bucket_params.get(Bucket.maxTTL)
        if bucket_params.get(Bucket.bucketType) == Bucket.Type.MEMBASE and\
           'autoCompactionDefined' in bucket_params:
            init_params["autoCompactionDefined"] = bucket_params.get('autoCompactionDefined')
            init_params["parallelDBAndViewCompaction"] = "false"
            init_params["databaseFragmentationThreshold%5Bpercentage%5D"] = 50
            init_params["viewFragmentationThreshold%5Bpercentage%5D"] = 50
            init_params["indexCompactionMode"] = "circular"
            init_params["purgeInterval"] = 3

        if bucket_params.get(Bucket.storageBackend) == Bucket.StorageBackend.magma:
            if bucket_params.get("fragmentationPercentage"):
                init_params["magmaFragmentationPercentage"] \
                    = bucket_params.get("fragmentationPercentage")
            if bucket_params.get("magmaKeyTreeDataBlockSize"):
                init_params["magmaKeyTreeDataBlockSize"] \
                    = bucket_params.get("magmaKeyTreeDataBlockSize")
            if bucket_params.get("magmaSeqTreeDataBlockSize"):
                init_params["magmaSeqTreeDataBlockSize"] \
                    = bucket_params.get("magmaSeqTreeDataBlockSize")

            # Set the following only if not 'None'
            for b_param in [Bucket.historyRetentionCollectionDefault,
                            Bucket.historyRetentionBytes,
                            Bucket.historyRetentionSeconds,
                            Bucket.magmaKeyTreeDataBlockSize,
                            Bucket.magmaSeqTreeDataBlockSize,
                            Bucket.durabilityImpossibleFallback]:
                val = bucket_params.get(b_param, None)
                if val is not None:
                    init_params[b_param] = val

        if init_params[Bucket.priority] == "high":
            init_params[Bucket.threadsNumber] = 8
        init_params.pop(Bucket.priority)

        if bucket_params.get(Bucket.bucketType) == Bucket.Type.EPHEMERAL:
            # Remove 'replicaIndex' parameter in case of EPHEMERAL bucket
            init_params.pop(Bucket.replicaIndex, None)
            # Add purgeInterval only for Ephemeral case
            init_params['purgeInterval'] = bucket_params.get('purge_interval')

        # Setting bucket ranking only if it is not 'None'
        bucket_rank = bucket_params.get(Bucket.rank, None)
        if bucket_rank is not None:
            init_params[Bucket.rank] = bucket_rank

        self.log.info("Creating '%s' bucket %s"
                      % (init_params['bucketType'], init_params['name']))
        create_start_time = time.time()

        maxwait = 60
        for numsleep in range(maxwait):
            status, response = BucketRestApi.create_bucket(self, init_params)
            if status:
                create_time = time.time() - create_start_time
                self.log.debug("{0:.02f} seconds to create bucket {1}"
                               .format(round(create_time, 2),
                                       bucket_params.get('name')))
                break
            elif (int(response.status_code) == 503
                  and ('{"_":"Bucket with given name still exists"}'
                       in response.text)):
                sleep(1, "Bucket still exists, will retry..")
            else:
                return False
        else:
            self.log.warning("Failed creating the bucket after {0} secs"
                             .format(maxwait))
            return False
        return True

    def set_magma_quota_percentage(self, bucket="default", storageQuotaPercentage=10):
        api = '{0}{1}{2}'.format(self.base_url, 'pools/default/buckets/',
                                 urllib.quote_plus("%s" % bucket))
        params_dict = {}
        params_dict["storageQuotaPercentage"] = storageQuotaPercentage
        params = urllib.urlencode(params_dict)
        self.log.info("Updating bucket properties for %s" % bucket)
        self.log.debug("%s with param: %s" % (api, params))
        status, content, _ = self._http_request(api, 'POST', params)
        if not status:
            self.log.error("Failed to update magma storage quota percentage: %s"
                           % content)
        return status

    def set_throttle_n_storage_limit(self, bucket_name, throttle_limit=5000, storage_limit=500, service="data"):
        key_throttle_limit = service + "ThrottleLimit"
        key_storage_limit = service + "StorageLimit"
        api = '{0}{1}{2}'.format(self.base_url, 'pools/default/buckets/',
                              urllib.quote_plus("%s" % bucket_name))
        params = urllib.urlencode({key_throttle_limit: throttle_limit, key_storage_limit: storage_limit})
        self.log.debug("%s with param: %s" % (api, params))
        status, content, _ = self._http_request(api, 'POST', params)
        if not status:
            self.log.error("Failed to update throttle limit: %s"
                           % content)
        return status, content

    def update_memcached_settings(self, **params):
        api = self.base_url + "pools/default/settings/memcached/global"
        params = urllib.urlencode(params)
        self.log.info("Updating memcached properties")
        self.log.debug("%s with param: %s" % (api, params))

        status, content, _ = self._http_request(api, 'POST', params)
        if not status:
            self.log.error("Failed to update memcached settings: %s"
                           % content)
        self.log.debug("Memcached settings updated")
        return status

    def change_bucket_props(self, bucket, ramQuotaMB=None,
                            replicaNumber=None ,proxyPort=None,
                            replicaIndex=None, flushEnabled=None,
                            timeSynchronization=None, maxTTL=None,
                            compressionMode=None, bucket_durability=None,
                            bucket_rank=None, storageBackend=None,
                            bucketWidth=None, bucketWeight=None,
                            history_retention_collection_default=None,
                            history_retention_bytes=None,
                            history_retention_seconds=None,
                            magma_key_tree_data_block_size=None,
                            magma_seq_tree_data_block_size=None,
                            durability_impossible_fallback=None):

        params_dict = {}
        if ramQuotaMB:
            params_dict["ramQuotaMB"] = ramQuotaMB
        if replicaNumber is not None:
            params_dict["replicaNumber"] = replicaNumber
        if bucket_rank is not None:
            params_dict[Bucket.rank] = bucket_rank
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
        if bucket_durability:
            params_dict[Bucket.durabilityMinLevel] = bucket_durability
        if bucketWidth:
            params_dict["width"] = bucketWidth
        if bucketWeight:
            params_dict["weight"] = bucketWeight
        if history_retention_collection_default is not None:
            params_dict[Bucket.historyRetentionCollectionDefault] \
                = history_retention_collection_default
        if history_retention_bytes is not None:
            params_dict[Bucket.historyRetentionBytes] \
                = history_retention_bytes
        if history_retention_seconds is not None:
            params_dict[Bucket.historyRetentionSeconds] \
                = history_retention_seconds
        if magma_key_tree_data_block_size is not None:
            params_dict[Bucket.magmaKeyTreeDataBlockSize] \
                = magma_key_tree_data_block_size
        if magma_seq_tree_data_block_size is not None:
            params_dict[Bucket.magmaSeqTreeDataBlockSize] \
                = magma_seq_tree_data_block_size
        if storageBackend is not None:
            params_dict[Bucket.storageBackend] = storageBackend
        if durability_impossible_fallback is not None:
            params_dict["durabilityImpossibleFallback"] = durability_impossible_fallback

        self.log.info("Updating bucket properties for {}".format(bucket.name))
        status, content = self.edit_bucket(bucket.name, params_dict)
        if timeSynchronization:
            if status:
                raise Exception("Erroneously able to set bucket settings %s for bucket %s on time-sync" % (params_dict, bucket.name))
            return status, content
        if not status:
            raise Exception("Failure while setting bucket %s param %s: %s"
                            % (bucket.name, params_dict, content))
        self.log.debug("Bucket %s updated" % bucket.name)
        bucket.__dict__.update(params_dict)
        return status

    def set_collection_history(self, bucket_name, scope, collection,
                               history="false"):
        return self.edit_collection(bucket_name, scope, collection,
                                    collection_spec={"history": history})

    def set_bucket_rr_guardrails(self, couch_min_rr=None, magma_min_rr=None):
        api = self.base_url + "settings/resourceManagement/bucket/residentRatio"
        params = {}
        if couch_min_rr is not None:
            params['couchstoreMinimum'] = couch_min_rr
        if magma_min_rr is not None:
            params['magmaMinimum'] = magma_min_rr
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "POST", params)
        return status, content

    def set_max_data_per_bucket_guardrails(self, couch_max_data=None, magma_max_data=None):
        api = self.base_url + "settings/resourceManagement/bucket/dataSizePerNode"
        params = {}
        if couch_max_data is not None:
            params['couchstoreMaximum'] = couch_max_data
        if magma_max_data is not None:
            params['magmaMaximum'] = magma_max_data
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "POST", params)
        return status, content

    def set_max_disk_usage_guardrails(self, max_disk_usage):
        api = self.base_url + "settings/resourceManagement/diskUsage"
        params = {}
        params['maximum'] = max_disk_usage
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "POST", params)
        return status, content

    def get_auto_compaction_settings(self):
        api = self.base_url + "settings/autoCompaction"
        _, content, _ = self._http_request(api)
        return json.loads(content)

    def set_auto_compaction(self, bucket_name, parallelDBAndVC="false",
                            dbFragmentThreshold=None,
                            viewFragmentThreshold=None,
                            dbFragmentThresholdPercentage=None,
                            viewFragmentThresholdPercentage=None,
                            allowedTimePeriodFromHour=None,
                            allowedTimePeriodFromMin=None,
                            allowedTimePeriodToHour=None,
                            allowedTimePeriodToMin=None,
                            allowedTimePeriodAbort=None):
        """Reset compaction values to default, try with old fields (dp4 build)
        and then try with newer fields"""
        status, content = super().set_auto_compaction(
            bucket_name=bucket_name,
            parallel_db_and_vc=parallelDBAndVC,
            db_fragment_threshold=dbFragmentThreshold,
            view_fragment_threshold=viewFragmentThreshold,
            db_fragment_threshold_percentage=dbFragmentThresholdPercentage,
            view_fragment_threshold_percentage=viewFragmentThresholdPercentage,
            allowed_time_period_from_hour=allowedTimePeriodFromHour,
            allowed_time_period_from_min=allowedTimePeriodFromMin,
            allowed_time_period_to_hour=allowedTimePeriodToHour,
            allowed_time_period_to_min=allowedTimePeriodToMin,
            allowed_time_period_abort=allowedTimePeriodAbort)
        return status, content

    def disable_auto_compaction(self):
        """
           Cluster-wide Setting
              Disable autocompaction on doc and view
        """
        api = self.base_url + "controller/setAutoCompaction"
        self.log.info("Disable autocompaction in cluster-wide setting")
        status, _, _ = self._http_request(api, "POST",
                                          "parallelDBAndViewCompaction=false")
        return status

    def flush_bucket(self, bucket="default"):
        status, _ = super().flush_bucket(bucket)
        self.log.debug("Bucket flush '%s' triggered" % bucket)
        return status

    def get_bucket_CCCP(self, bucket):
        self.log.debug("Getting CCCP config")
        api = '%spools/default/b/%s' % (self.base_url,
                                        urllib.quote_plus("%s" % bucket))
        status, content, _ = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def compact_bucket(self, bucket="default"):
        self.log.debug("Triggering bucket compaction for '%s'" % bucket)
        status, _ = super().compact_bucket(bucket)
        if not status:
            raise BucketCompactionException(bucket)
        self.log.debug('Bucket compaction successful')
        return True

    def cancel_bucket_compaction(self, bucket="default"):
        self.log.debug("Triggering bucket compaction for '%s'" % bucket)
        status, _ = super().cancel_compaction(bucket)
        if not status:
            raise BucketCompactionException(bucket)
        self.log.debug('Bucket compaction successful')
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
        api = self.base_url + 'pools/default/buckets/{0}/localRandomKey' \
                             .format(urllib.quote_plus("%s" % bucket))
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
        api = self.base_url + url
        status, content, _ = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete built-in user
    '''

    def delete_builtin_user(self, user_id):
        url = "settings/rbac/users/local/" + user_id
        api = self.base_url + url
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
        api = self.base_url + url
        status, content, _ = self._http_request(api,
                                                'POST',
                                                password)
        if not status:
            raise Exception(content)
        return json.loads(content)

    # Collection/Scope specific APIs
    def create_collection(self, bucket, scope, collection_spec, session=None):
        params = dict()
        for key, value in collection_spec.items():
            if key in ['name', 'maxTTL', 'history']:
                params[key] = value
        return super().create_collection(bucket, scope, params)

    def create_scope(self, bucket, scope, session=None):
        return super().create_scope(bucket, scope)

    def delete_scope(self, bucket, scope, session=None):
        return self.drop_scope(bucket, scope)

    def delete_collection(self, bucket, scope, collection, session=None):
        return self.drop_collection(bucket, scope, collection)

    def list_collections(self, bucket):
        return self.list_scope_collections(bucket.name)

    def get_total_collections_in_bucket(self, bucket):
        _, json_parsed = self.list_scope_collections(bucket.name)
        collection_count = 0
        for scope in json_parsed["scopes"]:
            collection_count += len(scope["collections"])
        return collection_count

    def get_bucket_manifest_uid(self, bucket):
        _, json_parsed = self.list_scope_collections(bucket.name)
        return json_parsed["uid"]

    def get_scope_id(self, bucket, scope_name):
        _, json_parsed = self.list_scope_collections(bucket.name)
        for scope_data in json_parsed["scopes"]:
            if scope_data["name"] == scope_name:
                sid = scope_data["uid"]
                return sid

    def get_collection_id(self, bucket, scope_name, collection_name):
        _, json_parsed = self.list_scope_collections(bucket.name)
        for scope_data in json_parsed["scopes"]:
            if scope_data["name"] == scope_name:
                collections_data = scope_data["collections"]
                for collection_data in collections_data:
                    if collection_data["name"] == collection_name:
                        cid = collection_data["uid"]
                        return cid
