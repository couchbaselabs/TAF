'''
Created on Sep 27, 2017

@author: riteshagarwal
'''
from remote.remote_util import RemoteMachineShellConnection
import json
import time
import urllib
from bucket import *
import logger
from membase.api.rest_client import Node
from memcached.helper.kvstore import KVStore
from rest import Rest_Connection
from BucketOperations_Rest import BucketHelper as bucket_helper_rest
from com.couchbase.client.java import Bucket
log = logger.Logger.get_logger()
    
class BucketHelper(bucket_helper_rest):

    def __init__(self, server, username, password, cb_version=None):
        self.server = server
        self.hostname = "%s:%s" % (server.ip, server.port)
        self.username = username
        self.password = password
        self.cb_version = cb_version

    def bucket_create(self, name, bucket_type, quota,
                      eviction_policy, replica_count, enable_replica_indexes,
                      priority, enable_flush, wait):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if bucket_type is not None:
            options += " --bucket-type " + bucket_type
        if quota is not None:
            options += " --bucket-ramsize " + str(quota)
        if eviction_policy is not None:
            options += " --bucket-eviction-policy " + eviction_policy
        if replica_count is not None:
            options += " --bucket-replica " + str(replica_count)
        if enable_replica_indexes is not None:
            options += " --enable-index-replica " + str(enable_replica_indexes)
        if priority is not None:
            options += " --bucket-priority " + priority
        if enable_flush is not None:
            options += " --enable-flush " + str(enable_flush)
        if wait:
            options += " --wait"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-create",
                                                     self.hostname.split(":")[0], options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket created")

    def bucket_compact(self, bucket_name, data_only, views_only):
        options = self._get_default_options()
        if bucket_name is not None:
            options += " --bucket " + bucket_name
        if data_only:
            options += " --data-only"
        if views_only:
            options += " --view-only"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-compact",
                                                     self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Bucket compaction started")

    def bucket_delete(self, bucket_name):
        options = self._get_default_options()
        if bucket_name is not None:
            options += " --bucket " + bucket_name

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-delete",
                                                     self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket deleted")

    def bucket_edit(self, name, quota, eviction_policy,
                    replica_count, priority, enable_flush):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if quota is not None:
            options += " --bucket-ramsize " + str(quota)
        if eviction_policy is not None:
            options += " --bucket-eviction-policy " + eviction_policy
        if replica_count is not None:
            options += " --bucket-replica " + str(replica_count)
        if priority is not None:
            options += " --bucket-priority " + priority
        if enable_flush is not None:
            options += " --enable-flush " + str(enable_flush)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-edit",
                                                     self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket edited")

    def bucket_flush(self, name, force):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if force:
            options += " --force"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-flush",
                                                     self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket flushed")
