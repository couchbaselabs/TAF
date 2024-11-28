from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 3,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 5,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 40,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.THREE,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,
    Bucket.warmupBehavior: Bucket.WarmupBehavior.BACKGROUND,

    "buckets": {
        "default": {
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1900,
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
        },
        "bucket1": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 15,
            Bucket.warmupBehavior: Bucket.WarmupBehavior.BLOCKING,
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
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
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
        },
        "bucket2": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 15,
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
