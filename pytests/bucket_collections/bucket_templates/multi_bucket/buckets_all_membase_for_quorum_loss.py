from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 2,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 5,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 70,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 500,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.THREE,
    Bucket.ramQuotaMB: 800,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.couchstore,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.VALUE_ONLY,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE
}