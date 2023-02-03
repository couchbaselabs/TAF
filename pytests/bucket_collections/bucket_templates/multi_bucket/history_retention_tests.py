from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 5,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 500000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: False,
    MetaConstants.USE_SIMPLE_NAMES: True,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
    Bucket.ramQuotaMB: 512,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,

    "buckets": {
        # Default bucket conf
        "bucket-1": {
            Bucket.priority: Bucket.Priority.LOW,
        },
        "bucket-2": {
            Bucket.priority: Bucket.Priority.HIGH,
            Bucket.historyRetentionCollectionDefault: "true",
            Bucket.historyRetentionSeconds: 86400,
            "scopes": {
                "scope_1": {
                    "collections": {
                        "c1": { "history": "false" },
                        "c2": { "history": "false" }
                    }
                },
                "scope_2": {
                    "collections": {
                        "c2": { "history": "false" },
                        "c3": { "history": "false" },
                    }
                },
                "scope_3": {
                    "collections": {
                        "c1": { "history": "false" },
                        "c4": { "history": "false" },
                    }
                },
                "scope_4": {
                    "collections": {
                        "c4": { "history": "false" },
                        "c5": { "history": "false" },
                    }
                },
                "scope_5": {
                    "collections": {
                        "c1": { "history": "false" },
                    }
                }
            }
        },
        "bucket-3": {
            Bucket.priority: Bucket.Priority.HIGH,
            Bucket.historyRetentionCollectionDefault: "true",
            Bucket.historyRetentionBytes: 10737418240,
             "scopes": {
                "scope_1": {
                    "collections": {
                        "c1": { "history": "false" },
                        "c2": { "history": "false" }
                    }
                },
                "scope_2": {
                    "collections": {
                        "c2": { "history": "false" },
                        "c3": { "history": "false" },
                    }
                },
                "scope_3": {
                    "collections": {
                        "c1": { "history": "false" },
                        "c4": { "history": "false" },
                    }
                },
                "scope_4": {
                    "collections": {
                        "c4": { "history": "false" },
                        "c5": { "history": "false" },
                    }
                },
                "scope_5": {
                    "collections": {
                        "c1": { "history": "false" },
                    }
                }
            }
        },
        "bucket-4": {
            Bucket.priority: Bucket.Priority.HIGH,
            Bucket.historyRetentionCollectionDefault: "false",
            Bucket.historyRetentionSeconds: 60,
            Bucket.historyRetentionBytes: 10737418240,
             "scopes": {
                "scope_1": {
                    "collections": {
                        "c1": { "history": "true" },
                        "c2": { "history": "true" },
                        "c3": { "history": "true" },
                        "c4": { "history": "true" },
                        "c5": { "history": "true" },
                    }
                },
                "scope_3": {
                    "collections": {
                        "c1": { "history": "true" },
                        "c2": { "history": "true" },
                        "c3": { "history": "true" },
                        "c4": { "history": "true" },
                        "c5": { "history": "true" },
                    }
                },
                "scope_4": {
                    "collections": {
                        "c1": { "history": "true" },
                        "c2": { "history": "true" },
                        "c3": { "history": "true" },
                        "c4": { "history": "true" },
                        "c5": { "history": "true" },
                    }
                },
            }
        },
    }
}
