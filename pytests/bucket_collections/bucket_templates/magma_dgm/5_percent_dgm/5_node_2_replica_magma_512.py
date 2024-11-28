from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 3,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 1,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.ACTIVE,
    Bucket.warmupBehavior: Bucket.WarmupBehavior.BACKGROUND,
    "buckets": {
        "default": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 25,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 139942,
            Bucket.ramQuotaMB: 512,
            Bucket.priority: Bucket.Priority.HIGH
        },
        "bucket1": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 10,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 500,
            Bucket.storageBackend: Bucket.StorageBackend.couchstore,
            Bucket.ramQuotaMB: 2000
        },
        "bucket2": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 10,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 500,
            Bucket.ramQuotaMB: 1000,
            Bucket.warmupBehavior: Bucket.WarmupBehavior.BLOCKING
        }
    }
}
