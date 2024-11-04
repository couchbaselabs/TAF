from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
    Bucket.storageBackend: Bucket.StorageBackend.couchstore,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 10,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    "buckets": {
        "bucket-0": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000,
        },
        "bucket-1": {
            Bucket.bucketType: Bucket.Type.EPHEMERAL,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.NO_EVICTION,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000,
        }
    }
}
