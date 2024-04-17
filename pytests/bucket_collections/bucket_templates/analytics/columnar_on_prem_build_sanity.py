'''
Created on 06-Oct-2020

@author: umang
'''

from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 3,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaIndex: 1,
    Bucket.maxTTL: 0,
}
