from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.THREE,
    Bucket.ramQuotaMB: 100,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 500,
    Bucket.storageBackend: Bucket.StorageBackend.couchstore,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.VALUE_ONLY,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,

    "buckets": {
        "default": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000,
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
                            Bucket.maxTTL: 100
                        },
                        "collection_2": {
                            "rbac": "rbac2",
                            Bucket.maxTTL: 100
                        }
                    }
                },
                "scope2": {
                    "privileges": [
                        "Perm1"
                    ],
                    "collections": {
                        "collection_1": {
                            "rbac": "rbac1",
                            Bucket.maxTTL: 100
                        },
                        "collection_2": {
                            "rbac": "rbac2",
                            Bucket.maxTTL: 100
                        }
                    }
                }
            }
        },
    }
}
