from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 6,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 10000,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
    Bucket.ramQuotaMB: 256,
    Bucket.width: 2,
    Bucket.weight: 30,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.ACTIVE,
    "buckets": {
        "bucket1": {
            Bucket.weight: 1000,
            MetaConstants.NUM_SCOPES_PER_BUCKET: 40,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 10000,
            Bucket.priority: Bucket.Priority.HIGH
        },
        "bucket2": {
            Bucket.width: 3,
            Bucket.weight: 3000,
            Bucket.ramQuotaMB: 512,
            MetaConstants.NUM_SCOPES_PER_BUCKET: 2,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 40,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 10000,
        },
        "bucket3": {
            Bucket.weight: 1000,
            MetaConstants.NUM_SCOPES_PER_BUCKET: 20,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 4,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
        }
    }
}
