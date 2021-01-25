from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 20,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 20,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
    Bucket.ramQuotaMB: 800,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.couchstore,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.ACTIVE,

    "buckets": {
        "default": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            "privileges": [
                "Perm1"
            ],
            "scopes": {
                "scope1": {
                    "privileges": [
                        "Perm1"
                    ],
                    "collections": {
                        "collection_1": {
                            "rbac": "rbac1",
                            Bucket.maxTTL: 0
                        },
                        "collections_2": {
                            "rbac": "rbac2",
                            Bucket.maxTTL: 0
                        }
                    }
                },
                "scope2": {
                    "privileges": [
                        "Perm1"
                    ],
                    "collections": {
                        "collection1": {
                            "rbac": "rbac1",
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            "rbac": "rbac2",
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        }
    }
}