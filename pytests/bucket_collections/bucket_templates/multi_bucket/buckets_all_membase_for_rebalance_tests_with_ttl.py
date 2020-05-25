from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants


spec = {
    MetaConstants.NUM_BUCKETS: 3,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 2,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
    Bucket.ramQuotaMB: 100,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.couchstore,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.VALUE_ONLY,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,

    "buckets": {
        "default": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 2,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
            Bucket.ramQuotaMB: 1500,
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
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 50000,
                            Bucket.maxTTL: 50
                        },
                        "collections_2": {
                            "rbac": "rbac2",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 50000,
                            Bucket.maxTTL: 50
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
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 100
                        },
                        "collection2": {
                            "rbac": "rbac2",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 100
                        }
                    }
                }
            }
        },
        "bucket1": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            MetaConstants.NUM_SCOPES_PER_BUCKET: 2,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
            "privileges": [
                "Perm2"
            ],
            "scopes": {
                "scope1": {
                    "privileges": [
                        "Perm1"
                    ],
                    "collections": {
                        "collection1": {
                            "rbac": "rbac1",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 150
                        },
                        "collection2": {
                            "rbac": "rbac2",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 150
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
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 200
                        },
                        "collection2": {
                            "rbac": "rbac2",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 200
                        }
                    }
                }
            }
        },
        "bucket2": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
            "privileges": [
                "Perm3"
            ],
            "scopes": {
                "scope1": {
                    "privileges": [
                        "Perm1"
                    ],
                    "collections": {
                        "collection1": {
                            "rbac": "rbac1",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 15000,
                            Bucket.maxTTL: 300
                        },
                        "collection2": {
                            "rbac": "rbac2",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 15000,
                            Bucket.maxTTL: 300
                        }
                    }
                }
            }
        }
    }
}
