from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 2,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 3,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 200000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.VALUE_ONLY,
    Bucket.ramQuotaMB: 512,
    Bucket.replicaIndex: 1,

    "buckets": {
        "default": {
            Bucket.durabilityMinLevel:
                Bucket.DurabilityMinLevel.MAJORITY,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection_1": {
                        },
                    }
                },
            }
        },
        "bucket-0": {
            Bucket.ramQuotaMB: 512,
            Bucket.replicaNumber: Bucket.ReplicaNum.THREE,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 200000,
            Bucket.storageBackend: Bucket.StorageBackend.couchstore,
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection_2": {
                        },
                    }
                },
            }
        }
    }
}
