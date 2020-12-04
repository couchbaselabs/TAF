from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 3,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 10000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
    Bucket.ramQuotaMB: 100,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.couchstore,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.ACTIVE,
    "buckets": {
        "default": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 20,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 50,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 80000,
            Bucket.ramQuotaMB: 17000
        },
        "bucket1": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.ramQuotaMB: 100,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 100
        },
        "bucket2": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.ramQuotaMB: 300,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 100
        },
        "bucket3": {
            Bucket.bucketType: Bucket.Type.EPHEMERAL,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.NRU_EVICTION,
            Bucket.ramQuotaMB: 100,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 100
        }
    }
}
