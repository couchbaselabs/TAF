"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

import json
import time
import urllib

from bucket import Bucket
from common_lib import sleep
from custom_exceptions.exception import \
    GetBucketInfoFailed, \
    BucketCompactionException
from Rest_Connection import RestConnection
from datetime import datetime, timedelta
from membase.api.rest_client import RestConnection as RC


class BucketHelper(RestConnection):
    def __init__(self, server):
        super(BucketHelper, self).__init__(server)

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

    def create_secret_params(self, secret_type="auto-generated-aes-key-256",
                             name="Default secret", usage=None,
                             autoRotation=True, rotationIntervalInDays=60,
                             rotationIntervalInSeconds=None, keyARN=None,
                             region=None, useIMDS=None, credentialsFile=None,
                             configFile=None, profile=None):
        if usage is None:
            usage = ["bucket-encryption-*"]

        data = {
            "autoRotation": autoRotation,
            "rotationIntervalInDays": rotationIntervalInDays
        }

        if rotationIntervalInSeconds is not None:
            data["nextRotationTime"] = (datetime.utcnow() + timedelta(
                seconds=rotationIntervalInSeconds)).isoformat() + "Z"
        else:
            data["nextRotationTime"] = (datetime.utcnow() + timedelta(
                days=rotationIntervalInDays)).isoformat() + "Z"

        if keyARN is not None:
            data["keyARN"] = keyARN
        if region is not None:
            data["region"] = region
        if useIMDS is not None:
            data["useIMDS"] = useIMDS
        if credentialsFile is not None:
            data["credentialsFile"] = credentialsFile
        if configFile is not None:
            data["configFile"] = configFile
        if profile is not None:
            data["profile"] = profile

        params = {
            "type": secret_type,
            "name": name,
            "usage": usage,
            "data": data
        }
        return params

    def get_bucket_from_cluster(self, bucket, num_attempt=1, timeout=1):
        api = '%s%s%s?basic_stats=true' \
              % (self.baseUrl, 'pools/default/buckets/',
                 urllib.quote_plus(bucket.name))
        status, content, _ = self._http_request(api)
        num = 1
        while not status and num_attempt > num:
            sleep(timeout, "Will retry to get %s" % api, log_type="infra")
            status, content, _ = self._http_request(api)
            num += 1
        if status:
            parsed = json.loads(content)
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
            bucket.accessScannerEnabled = parsed.get("accessScannerEnabled")
            bucket.expiryPagerSleepTime = parsed.get("expiryPagerSleepTime")
            bucket.warmupBehavior = parsed.get("warmupBehavior")
            bucket.memoryLowWatermark = parsed.get("memoryLowWatermark")
            bucket.memoryHighWatermark = parsed.get("memoryHighWatermark")
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
        bucket_to_check = self.get_bucket_json(bucket_name)
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
        api = self.baseUrl + 'pools/default/buckets/' \
              + urllib.quote_plus("%s" % bucket)
        _, content, _ = self._http_request(api)
        _stats = json.loads(content)
        return _stats['vBucketServerMap']['vBucketMap']

    def get_vbucket_map_and_server_list(self, bucket="default"):
        """ Return server list, replica and vbuckets map
        that matches to server list """
        # vbucket_map = self.fetch_vbucket_map(bucket)
        api = self.baseUrl + 'pools/default/buckets/' \
              + urllib.quote_plus("%s" % bucket)
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
                      urllib.quote_plus("%s" % bucket), "/nodes/",
                      node.ip, node.port, "/stats")
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
                             .format(urllib.quote_plus("%s" % bucket), zoom)
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
                .format(urllib.quote_plus("%s" % bucket), zoom)
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
                                    urllib.quote_plus("%s" % bucket_name),
                                    "/stats")
        status, content, _ = self._http_request(api)
        json_parsed = json.loads(content)
        return status, json_parsed

    def get_bucket_json(self, bucket_name='default'):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
                                 urllib.quote_plus("%s" % bucket_name))
        status, content, _ = self._http_request(api)
        if not status:
            self.log.error("Error while getting {0}. Please retry".format(api))
            raise GetBucketInfoFailed(bucket_name, content)
        return json.loads(content)

    def pause_bucket(self, bucket, s3_path=None, blob_storage_region=None, rate_limit=1024):
        api = '{0}{1}'.format(self.baseUrl, 'controller/pause')
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
        api = '{0}{1}'.format(self.baseUrl, 'controller/stopPause')
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
        api = '{0}{1}'.format(self.baseUrl, 'controller/resume')
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
        api = '{0}{1}'.format(self.baseUrl, 'controller/stopResume')
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
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
                          urllib.quote_plus("{0}".format(bucket)))
        status, _, header = self._http_request(api, 'DELETE')
        if "status" in header:
            status_code = header['status']
        else:
            status_code = header.status_code
        if int(status_code) == 500:
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
        return status

    # figure out the proxy port
    def create_bucket(self, bucket_params=dict()):
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets')
        init_params = {
            Bucket.name: bucket_params.get(Bucket.name),
            Bucket.ramQuotaMB: bucket_params.get(Bucket.ramQuotaMB),
            Bucket.replicaNumber: bucket_params.get(Bucket.replicaNumber),
            Bucket.bucketType: bucket_params.get(Bucket.bucketType),
            Bucket.priority: bucket_params.get(Bucket.priority),
            Bucket.flushEnabled: bucket_params.get(Bucket.flushEnabled),
            Bucket.evictionPolicy: bucket_params.get(Bucket.evictionPolicy),
            Bucket.storageBackend: bucket_params.get(Bucket.storageBackend),
            Bucket.conflictResolutionType: bucket_params.get(Bucket.conflictResolutionType),
            Bucket.durabilityMinLevel: bucket_params.get(Bucket.durabilityMinLevel),
            Bucket.encryptionAtRestSecretId: bucket_params.get(Bucket.encryptionAtRestSecretId),
            Bucket.encryptionAtRestDekRotationInterval: bucket_params.get(Bucket.encryptionAtRestDekRotationInterval),
            Bucket.encryptionAtRestDekLifetime: bucket_params.get(Bucket.encryptionAtRestDekLifetime)}

        # Set Bucket's width/weight param only if serverless is enabled
        if bucket_params.get('serverless'):
            serverless = bucket_params['serverless']
            init_params[Bucket.weight] = serverless.weight
            init_params[Bucket.width] = serverless.width
        num_vbs = bucket_params.get("num_vbuckets")
        if num_vbs:
            init_params[Bucket.numVBuckets] = num_vbs

        server_info = dict({"ip": self.ip, "port": self.port,
                            "username": self.username,
                            "password": self.password})
        rest = RC(server_info)
        if rest.is_enterprise_edition():
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
                            Bucket.magmaSeqTreeDataBlockSize]:
                val = bucket_params.get(b_param, None)
                if val is not None:
                    init_params[b_param] = val

        if init_params[Bucket.priority] == "high":
            init_params[Bucket.threadsNumber] = 8
        init_params.pop(Bucket.priority)

        if bucket_params.get(Bucket.bucketType) == Bucket.Type.MEMCACHED:
            # Remove 'replicaNumber' in case of MEMCACHED bucket
            init_params.pop(Bucket.replicaNumber, None)
        elif bucket_params.get(Bucket.bucketType) == Bucket.Type.EPHEMERAL:
            # Remove 'replicaIndex' parameter in case of EPHEMERAL bucket
            init_params.pop(Bucket.replicaIndex, None)
            # Add purgeInterval only for Ephemeral case
            init_params['purgeInterval'] = bucket_params.get('purge_interval')

        # Setting bucket ranking only if it is not 'None'
        bucket_rank = bucket_params.get(Bucket.rank, None)
        if bucket_rank is not None:
            init_params[Bucket.rank] = bucket_rank

        params = urllib.urlencode(init_params)
        self.log.info("Creating '%s' bucket %s"
                      % (init_params['bucketType'], init_params['name']))
        self.log.debug("{0} with param: {1}".format(api, params))
        create_start_time = time.time()

        maxwait = 60
        for numsleep in range(maxwait):
            status, content, header = self._http_request(api, 'POST', params)
            if status:
                create_time = time.time() - create_start_time
                self.log.debug("{0:.02f} seconds to create bucket {1}"
                               .format(round(create_time, 2),
                                       bucket_params.get('name')))
                break
            elif (int(header['status']) == 503 and
                  '{"_":"Bucket with given name still exists"}' in content):
                sleep(1, "Bucket still exists, will retry..")
            else:
                return False
        else:
            self.log.warning("Failed creating the bucket after {0} secs"
                             .format(maxwait))
            return False

        return True

    def set_magma_quota_percentage(self, bucket="default", storageQuotaPercentage=10):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
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
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
                              urllib.quote_plus("%s" % bucket_name))
        params = urllib.urlencode({key_throttle_limit: throttle_limit, key_storage_limit: storage_limit})
        self.log.debug("%s with param: %s" % (api, params))
        status, content, _ = self._http_request(api, 'POST', params)
        if not status:
            self.log.error("Failed to update throttle limit: %s"
                           % content)
        return status, content

    def update_memcached_settings(self, **params):
        api = self.baseUrl + "pools/default/settings/memcached/global"
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
                            replicaNumber=None, proxyPort=None,
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
                            accessScannerEnabled=None,
                            expiryPagerSleepTime=None,
                            warmupBehavior=None,
                            memoryLowWatermark=None,
                            memoryHighWatermark=None,
                            encryptionAtRestSecretId=None,
                            encryptionAtRestDekRotationInterval=None,
                            encryptionAtRestDekLifetime=None):

        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/',
                                 urllib.quote_plus("%s" % bucket))
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
            params_dict[Bucket.historyRetentionCollectionDefault]\
                = history_retention_collection_default
        if history_retention_bytes is not None:
            params_dict[Bucket.historyRetentionBytes]\
                = history_retention_bytes
        if history_retention_seconds is not None:
            params_dict[Bucket.historyRetentionSeconds]\
                = history_retention_seconds
        if magma_key_tree_data_block_size is not None:
            params_dict[Bucket.magmaKeyTreeDataBlockSize]\
                = magma_key_tree_data_block_size
        if magma_seq_tree_data_block_size is not None:
            params_dict[Bucket.magmaSeqTreeDataBlockSize]\
                = magma_seq_tree_data_block_size
        if storageBackend is not None:
            params_dict[Bucket.storageBackend] = storageBackend
        if accessScannerEnabled is not None:
            params_dict["accessScannerEnabled"] = accessScannerEnabled
        if expiryPagerSleepTime is not None:
            params_dict["expiryPagerSleepTime"] = expiryPagerSleepTime
        if warmupBehavior is not None:
            params_dict["warmupBehavior"] = warmupBehavior
        if memoryLowWatermark is not None:
            params_dict["memoryLowWatermark"] = memoryLowWatermark
        if memoryHighWatermark is not None:
            params_dict["memoryHighWatermark"] = memoryHighWatermark
        if encryptionAtRestSecretId is not None:
            params_dict[Bucket.encryptionAtRestSecretId] = encryptionAtRestSecretId
        if encryptionAtRestDekRotationInterval is not None:
            params_dict[Bucket.encryptionAtRestDekRotationInterval] = encryptionAtRestDekRotationInterval
        if encryptionAtRestDekLifetime is not None:
            params_dict[Bucket.encryptionAtRestDekLifetime] = encryptionAtRestDekLifetime

        params = urllib.urlencode(params_dict)

        self.log.info("Updating bucket properties for %s" % bucket)
        self.log.debug("%s with param: %s" % (api, params))
        status, content, _ = self._http_request(api, 'POST', params)
        if timeSynchronization:
            if status:
                raise Exception("Erroneously able to set bucket settings %s for bucket %s on time-sync" % (params, bucket))
            return status, content
        if not status:
            raise Exception("Failure while setting bucket %s param %s: %s"
                            % (bucket, params, content))
        self.log.debug("Bucket %s updated" % bucket)
        bucket.__dict__.update(params_dict)
        return status

    def set_collection_history(self, bucket_name, scope, collection,
                               history="false"):
        api = self.baseUrl \
            + "pools/default/buckets/%s/scopes/%s/collections/%s" \
            % (bucket_name, scope, collection)
        params = {"history": history}
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "PATCH", params)
        return status, content

    def set_collection_maxttl(self, bucket_name, scope, collection,
                               maxTTL):
        api = self.baseUrl \
            + "pools/default/buckets/%s/scopes/%s/collections/%s" \
            % (bucket_name, scope, collection)
        params = {"maxTTL": maxTTL}
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "PATCH", params)
        return status, content

    def set_bucket_rr_guardrails(self, couch_min_rr=None, magma_min_rr=None):

        api = self.baseUrl + "settings/resourceManagement/bucket/residentRatio"
        params = {}
        if couch_min_rr is not None:
            params['couchstoreMinimum'] = couch_min_rr
        if magma_min_rr is not None:
            params['magmaMinimum'] = magma_min_rr
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "POST", params)
        return status, content

    def set_max_data_per_bucket_guardrails(self, couch_max_data=None, magma_max_data=None):

        api = self.baseUrl + "settings/resourceManagement/bucket/dataSizePerNode"
        params = {}
        if couch_max_data is not None:
            params['couchstoreMaximum'] = couch_max_data
        if magma_max_data is not None:
            params['magmaMaximum'] = magma_max_data
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "POST", params)
        return status, content

    def set_max_disk_usage_guardrails(self, max_disk_usage):

        api = self.baseUrl + "settings/resourceManagement/diskUsage"
        params = {}
        params['maximum'] = max_disk_usage
        params = urllib.urlencode(params)
        status, content, _ = self._http_request(api, "POST", params)
        return status, content

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

        params["parallelDBAndViewCompaction"] = parallelDBAndVC
        # Need to verify None because the value could be = 0
        if dbFragmentThreshold is not None:
            params["databaseFragmentationThreshold[size]"] = \
                dbFragmentThreshold
        if viewFragmntThreshold is not None:
            params["viewFragmentationThreshold[size]"] = viewFragmntThreshold
        if dbFragmentThresholdPercentage is not None:
            params["databaseFragmentationThreshold[percentage]"] = \
                dbFragmentThresholdPercentage
        if viewFragmntThresholdPercentage is not None:
            params["viewFragmentationThreshold[percentage]"] = \
                viewFragmntThresholdPercentage
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
            .format(urllib.quote_plus("%s" % bucket_name))
        status, _, _ = self._http_request(api, 'POST')
        self.log.debug("Bucket flush '%s' triggered" % bucket_name)
        return status

    def get_bucket_CCCP(self, bucket):
        self.log.debug("Getting CCCP config")
        api = '%spools/default/b/%s' % (self.baseUrl,
                                        urllib.quote_plus("%s" % bucket))
        status, content, _ = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def compact_bucket(self, bucket="default"):
        self.log.debug("Triggering bucket compaction for '%s'" % bucket)
        api = self.baseUrl \
              + 'pools/default/buckets/{0}/controller/compactBucket' \
                .format(urllib.quote_plus("%s" % bucket))
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
                .format(urllib.quote_plus("%s" % bucket))
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
    def create_collection(self, bucket, scope, collection_spec, session=None):
        api = self.baseUrl \
              + 'pools/default/buckets/%s/scopes/%s/collections' \
              % (urllib.quote_plus("%s" % bucket), urllib.quote_plus(scope))
        params = dict()
        for key, value in collection_spec.items():
            if key in ['name', 'maxTTL', 'history']:
                params[key] = value
        params = urllib.urlencode(params)
        headers = self._create_headers()
        if session is None:
            status, content, _ = self._http_request(api,
                                                    'POST',
                                                    params=params,
                                                    headers=headers)
        else:
            status, content, _ = self._urllib_request(api,
                                                      'POST',
                                                      params=params,
                                                      headers=headers,
                                                      session=session)
        return status, content

    def create_scope(self, bucket, scope, session=None):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes' \
                             % urllib.quote_plus("%s" % bucket)
        params = urllib.urlencode({'name': scope})
        headers = self._create_headers()
        if session is None:
            status, content, _ = self._http_request(api,
                                                    'POST',
                                                    params=params,
                                                    headers=headers)
        else:
            status, content, _ = self._urllib_request(api,
                                                      'POST',
                                                      params=params,
                                                      headers=headers,
                                                      session=session)
        return status, content

    def delete_scope(self, bucket, scope, session=None):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes/%s' \
                             % (urllib.quote_plus("%s" % bucket),
                                urllib.quote_plus(scope))
        headers = self._create_headers()
        if session is None:
            status, content, _ = self._http_request(api,
                                                    'DELETE',
                                                    headers=headers)
        else:
            status, content, _ = self._urllib_request(api,
                                                      'DELETE',
                                                      headers=headers,
                                                      session=session)
        return status, content

    def delete_collection(self, bucket, scope, collection, session=None):
        api = self.baseUrl \
              + 'pools/default/buckets/%s/scopes/%s/collections/%s' \
              % (urllib.quote_plus("%s" % bucket),
                 urllib.quote_plus(scope),
                 urllib.quote_plus(collection))
        headers = self._create_headers()
        if session is None:
            status, content, _ = self._http_request(api,
                                                    'DELETE',
                                                    headers=headers)
        else:
            status, content, _ = self._urllib_request(api,
                                                      'DELETE',
                                                      headers=headers,
                                                      session=session)
        return status, content

    def wait_for_collections_warmup(self, bucket, uid, session=None):
        api = self.baseUrl \
              + "pools/default/buckets/%s/scopes/@ensureManifest/%s" \
              % (bucket.name, uid)
        headers = self._create_headers()
        if session is None:
            status, content, _ = self._http_request(api,
                                                    'POST',
                                                    headers=headers)
        else:
            status, content, _ = self._urllib_request(api,
                                                      'POST',
                                                      headers=headers,
                                                      session=session)
        return status, content

    def list_collections(self, bucket):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes' \
                             % (urllib.quote_plus("%s" % bucket))
        headers = self._create_headers()
        status, content, _ = self._http_request(api,
                                                'GET',
                                                headers=headers)
        return status, content

    def get_total_collections_in_bucket(self, bucket):
        status, content = self.list_collections(bucket)
        json_parsed = json.loads(content)
        scopes = json_parsed["scopes"]
        collection_count = 0
        for scope in scopes:
            collections = len(scope["collections"])
            collection_count += collections
        return collection_count

    def get_bucket_manifest_uid(self, bucket):
        status, content = self.list_collections(bucket)
        json_parsed = json.loads(content)
        manifest_uid = json_parsed["uid"]
        return manifest_uid

    def get_scope_id(self, bucket, scope_name):
        status, content = self.list_collections(bucket)
        json_parsed = json.loads(content)
        scopes_data = json_parsed["scopes"]
        for scope_data in scopes_data:
            if scope_data["name"] == scope_name:
                sid = scope_data["uid"]
                return sid

    def get_collection_id(self, bucket, scope_name, collection_name):
        status, content = self.list_collections(bucket)
        json_parsed = json.loads(content)
        scopes_data = json_parsed["scopes"]
        for scope_data in scopes_data:
            if scope_data["name"] == scope_name:
                collections_data = scope_data["collections"]
                for collection_data in collections_data:
                    if collection_data["name"] == collection_name:
                        cid = collection_data["uid"]
                        return cid

    def import_collection_using_manifest(self, bucket_name, manifest_data):
        url = "pools/default/buckets/%s/scopes" \
              % urllib.quote_plus(bucket_name)
        json_header = self.get_headers_for_content_type_json()
        api = self.baseUrl + url
        status, content, _ = self._http_request(api, 'PUT', manifest_data,
                                                headers=json_header)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def get_buckets_itemCount(self):
        # get all the buckets
        bucket_map = {}
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets?basic_stats=true')
        status, content, _ = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            for item in json_parsed:
                bucket_name = item['name']
                item_count = item['basicStats']['itemCount']
                bucket_map[bucket_name] = item_count
        return bucket_map
