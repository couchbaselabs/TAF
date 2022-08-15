import zlib
from TestInput import TestInputServer

from BucketLib.BucketOperations import BucketHelper
from common_lib import sleep
from global_vars import logger
from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection
from Cb_constants.CBServer import CbServer


class MemcachedClientHelper(object):
    @staticmethod
    def direct_client(server, bucket, timeout=30,
                      admin_user=None, admin_pass=None):
        log = logger.get("test")
        if isinstance(server, dict):
            log.info("creating memcached client: {0}:{1} {2}"
                     .format(server["ip"], server.memcached_port, bucket.name))
        else:
            log.info("creating memcached client: {0}:{1} {2}"
                     .format(server.ip, server.memcached_port, bucket.name))
        BucketHelper(server).vbucket_map_ready(bucket, 60)
        vbuckets = BucketHelper(server).get_vbuckets(bucket)
        if isinstance(server, dict):
            client = MemcachedClient(server["ip"], server.memcached_port,
                                     timeout=timeout)
        else:
            client = MemcachedClient(server.ip, server.memcached_port,
                                     timeout=timeout)
        if vbuckets is not None:
            client.vbucket_count = len(vbuckets)
        else:
            client.vbucket_count = 0
        # todo raise exception for not bucket_info
        admin_user = admin_user or server.rest_username
        admin_pass = admin_pass or server.rest_password
        bucket_name = bucket.name.encode('ascii')
        client.sasl_auth_plain(admin_user, admin_pass)
        client.bucket_select(bucket_name)
        return client

    @staticmethod
    def flush_bucket(server, bucket, admin_user='cbadminbucket',
                     admin_pass='password'):
        # if memcached throws OOM error try again ?
        client = MemcachedClientHelper.direct_client(server, bucket,
                                                     admin_user=admin_user,
                                                     admin_pass=admin_pass)
        retry_attempt = 5
        while retry_attempt > 0:
            try:
                client.flush()
                logger.get("test").info("Bucket %s flushed" % bucket)
                break
            except MemcachedError:
                retry_attempt -= 1
                sleep(5, "Flush raised memcached error. Will retry..")
        client.close()
        return


class VBucketAwareMemcached(object):
    def __init__(self, rest, bucket, info=None, collections=None):
        self.info = info or dict({"ip": rest.ip, "port": rest.port,
                                  "username": rest.username,
                                  "password": rest.password})
        self.bucket = bucket
        self.memcacheds = {}
        self.vBucketMap = {}
        self.vBucketMapReplica = {}
        self.rest = rest
        self.reset(rest)
        self.collections = collections
        self.log = logger.get("test")

    def reset(self, rest=None):
        if not rest:
            self.rest = RestConnection(self.info)
        m, v, r = self.request_map(self.rest, self.bucket)
        self.memcacheds = m
        self.vBucketMap = v
        self.vBucketMapReplica = r

    def request_map(self, rest, bucket):
        memcacheds = {}
        vb_map = {}
        vb_map_replica = {}
        vb_ready = BucketHelper(self.info).vbucket_map_ready(bucket, 60)
        if not vb_ready:
            raise Exception("vbucket map is not ready for bucket %s" % bucket)
        vbs = BucketHelper(self.info).get_vbuckets(bucket)
        for vBucket in vbs:
            vb_map[vBucket.id] = vBucket.master
            self.add_memcached(vBucket.master, memcacheds, rest, bucket)

            vb_map_replica[vBucket.id] = vBucket.replica
            for replica in vBucket.replica:
                self.add_memcached(replica, memcacheds, rest, bucket)
        return memcacheds, vb_map, vb_map_replica

    def add_memcached(self, server_str, memcacheds, rest, bucket,
                      admin_user='cbadminbucket', admin_pass='password'):
        if server_str not in memcacheds:
            server_ip = server_str.rsplit(":", 1)[0]
            server_port = int(server_str.rsplit(":", 1)[1])
            nodes = rest.get_nodes()

            server = TestInputServer()
            server.ip = server_ip
            server.port = rest.port
            server.rest_username = rest.username
            server.rest_password = rest.password
            try:
                for node in nodes:
                    if node.ip == server_ip and node.memcached == server_port:
                        if server_str not in memcacheds:
                            server.port = node.port
                            memcacheds[server_str] = \
                                MemcachedClientHelper.direct_client(
                                    server, bucket,
                                    admin_user=admin_user,
                                    admin_pass=admin_pass)
                        break
            except Exception as ex:
                msg = "unable to establish connection to {0}. cleanup open connections"
                self.log.warn(msg.format(server_ip))
                self.done()
                raise ex

    def _get_vBucket_id(self, key):
        return (zlib.crc32(key) >> 16) & (len(self.vBucketMap) - 1)

    def memcached(self, key, replica_index=None):
        vb_id = self._get_vBucket_id(key)
        if replica_index is None:
            return self.memcached_for_vbucket(vb_id)
        else:
            return self.memcached_for_replica_vbucket(vb_id, replica_index)

    def memcached_for_vbucket(self, vb_id):
        if vb_id not in self.vBucketMap:
            msg = "vbucket map does not have an entry for vb : {0}"
            raise Exception(msg.format(vb_id))
        if self.vBucketMap[vb_id] not in self.memcacheds:
            msg = "does not have a mc connection for server : {0}"
            raise Exception(msg.format(self.vBucketMap[vb_id]))
        return self.memcacheds[self.vBucketMap[vb_id]]

    def memcached_for_replica_vbucket(self, vBucketId, replica_index=0,
                                      log_on=False):
        if vBucketId not in self.vBucketMapReplica:
            msg = "replica vbucket map does not have an entry for vb : {0}"
            raise Exception(msg.format(vBucketId))
        if log_on:
            self.log.info("Replica vbucket: vBucketId {0}, server{1}"
                          .format(vBucketId,
                                  self.vBucketMapReplica[vBucketId][replica_index]))
        if self.vBucketMapReplica[vBucketId][replica_index] not in self.memcacheds:
            msg = "does not have a mc connection for server : {0}"
            raise Exception(msg.format(self.vBucketMapReplica[vBucketId][replica_index]))
        return self.memcacheds[self.vBucketMapReplica[vBucketId][replica_index]]

    def done(self):
        [self.memcacheds[ip].close() for ip in self.memcacheds]
