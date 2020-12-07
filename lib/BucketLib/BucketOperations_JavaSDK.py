"""
Created on Sep 25, 2017

@author: riteshagarwal
"""

from BucketOperations_Rest import BucketHelper as BucketHelperRest
from sdk_client3 import SDKClient
from com.couchbase.client.core.endpoint.kv import AuthenticationException
from com.couchbase.client.java.bucket import BucketType
from com.couchbase.client.java.cluster import DefaultBucketSettings
from com.couchbase.client.java.error import BucketDoesNotExistException


class BucketHelper(BucketHelperRest, SDKClient):
    def __init__(self, server):
        self.server = server
        super(BucketHelper, self).__init__(server)
        super(SDKClient, self).__init__(server)

    def bucket_exists(self, bucket):
        try:
            self.connectCluster()
            hasBucket = self.clusterManager.hasBucket(bucket)
            self.disconnectCluster()
            return hasBucket
        except BucketDoesNotExistException as e:
            self.log.error(e)
            self.disconnectCluster()
            return False
        except AuthenticationException as e:
            self.log.fatal(e)
            self.disconnectCluster()
            return False

    def delete_bucket(self, bucket_name):
        '''
        Boolean removeBucket(String name)
        Removes a Bucket identified by its name with the default management timeout.

        This method throws:

        java.util.concurrent.TimeoutException: If the timeout is exceeded.
        com.couchbase.client.core.CouchbaseException: If the underlying resources could not be enabled properly.
        com.couchbase.client.java.error.TranscodingException: If the server response could not be decoded.
        Note: Removing a Bucket is an asynchronous operation on the server side, so even if the response is returned there is no guarantee that the operation has finished on the server itself.

        Parameters:
        name - the name of the bucket.
        Returns:
        true if the removal was successful, false otherwise.
        '''
        try:
            self.connectCluster()
            self.clusterManager.removeBucket(bucket_name);
            self.disconnectCluster()
            return True
        except BucketDoesNotExistException as e:
            self.log.error(e)
            self.disconnectCluster()
            return False
        except AuthenticationException as e:
            self.log.fatal(e)
            self.disconnectCluster()
            return False

    def create_bucket(self, bucket_params=dict()):
        self.log.debug("Connecting Cluster")
        self.connectCluster()
        try:
            bucketSettings = DefaultBucketSettings.builder()

            if bucket_params.get('bucketType') == "memcached":
                bucketSettings.type(BucketType.MEMCACHED)
            elif bucket_params.get('bucketType') == "ephemeral":
                bucketSettings.type(BucketType.EPHEMERAL)
            else:
                bucketSettings.type(BucketType.COUCHBASE)

            bucketSettings.replicas(bucket_params.get('replicaNumber'))
            bucketSettings.name(bucket_params.get('name'))
            bucketSettings.quota(bucket_params.get('ramQuotaMB'))
            bucketSettings.enableFlush(bucket_params.get('flushEnabled'))
            bucketSettings.indexReplicas(bucket_params.get('replicaIndex'))
            bucketSettings.build()
            self.clusterManager.insertBucket(bucketSettings)
            self.log.debug("Disconnecting Cluster")
            self.disconnectCluster()
            return True
        except BucketDoesNotExistException as e:
            self.log.error(e)
            self.log.debug("Disconnecting Cluster")
            self.disconnectCluster()
            return False
        except AuthenticationException as e:
            self.log.fatal(e)
            self.log.debug("Disconnecting Cluster")
            self.disconnectCluster()
            return False
