from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants
spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
    Bucket.width: 1,
    Bucket.weight: 30,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 8,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 800,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.ACTIVE,
}
