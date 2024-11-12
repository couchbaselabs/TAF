from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 2,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 1,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
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
            Bucket.warmupBehavior: Bucket.WarmupBehavior.BACKGROUND,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 25000000,
            Bucket.ramQuotaMB: 4096,
            Bucket.storageBackend: Bucket.StorageBackend.couchstore
        },
        "bucket1": {
            Bucket.warmupBehavior: Bucket.WarmupBehavior.BACKGROUND,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 25000000,
            Bucket.ramQuotaMB: 4096
        }
    }
}
