from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 3,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 5,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 3000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
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
            Bucket.bucketType: Bucket.Type.MEMBASE,
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection_1": {
                            Bucket.maxTTL: 0
                        },
                        "collections_2": {
                            Bucket.maxTTL: 0
                        }
                    }
                },
                "scope2": {
                    "collections": {
                        "collection1": {
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "bucket1": {
            Bucket.bucketType: Bucket.Type.MEMBASE,
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection1": {
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            Bucket.maxTTL: 0
                        }
                    }
                },
                "scope2": {
                    "collections": {
                        "collection1": {
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "bucket2": {
            Bucket.bucketType: Bucket.Type.EPHEMERAL,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.NO_EVICTION,
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection1": {
                            Bucket.maxTTL: 0
                        }
                    }
                },
                "scope2": {
                    "collections": {
                        "collection1": {
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        }
    }
}
