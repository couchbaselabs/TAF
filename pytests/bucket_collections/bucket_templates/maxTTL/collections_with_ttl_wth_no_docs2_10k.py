from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

# 10k-collection variant of maxTTL.collections_with_ttl_wth_no_docs2.
#
# Suites that scale this family up (collection_factor/collection_scale, e.g.
# collections/max_ttl_10k_backup_8.5_P0) expand the declared 3 scopes x 2
# collections into thousands of collections. The parent template declares
# num_items at bucket level - 1000, despite the "wth_no_docs" name - so every
# generated collection preloads 1000 docs, roughly 4M in total, and the spec
# load then runs for minutes. That is longer than both the 100s collection
# maxTTL and the 500s bucket maxTTL below, so docs expire mid-load and the
# setUp doc-count validation can never settle. The surviving doc count breaks
# too: the conf declares remaining_docs=2000, which only holds at the
# unscaled topology.
#
# So here num_items is 0 at bucket level, leaving every generated scope and
# collection empty, and the docs the test expects to survive are pinned onto
# scope3. The surviving count is then exactly 2000 at any collection_factor,
# and the preload is small enough that nothing can expire mid-load.
#
# Reached via the bucket_spec_remap job parameter, so the shared
# conf/collections/collections_with_ttl.conf and the unscaled suites that run
# from it are left untouched.
spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.replicaNumber: Bucket.ReplicaNum.THREE,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 500,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,

    "buckets": {
        "default": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
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
                },
                # No maxTTL here: these collections inherit the bucket's
                # maxTTL (500), which is deliberately longer than the test's
                # validation window, so they still hold their docs when the
                # 100s collection maxTTL above has already expired.
                "scope3": {
                    "privileges": [
                        "Perm1"
                    ],
                    "collections": {
                        "collection_1": {
                            "rbac": "rbac1",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000
                        },
                        "collection_2": {
                            "rbac": "rbac2",
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000
                        }
                    }
                }
            }
        },
    }
}
