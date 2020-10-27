from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 3,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 10,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,

    Bucket.maxTTL: 0,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,

    "buckets": {
        "default": {
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection_1": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 20,
                        },
                        "collections_2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 30,
                        },
                        "collections_3": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 40,
                        }
                    }
                },
                "scope2": {
                    "collections": {
                        "collection_1": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 25,
                        },
                        "collections_2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 35,
                        },
                        "collections_3": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 45,
                        }
                    }
                },
                "_default": {
                    "collections": {
                        "collections_2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 50,
                        },
                        "collections_3": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 60,
                        }
                    }
                }
            }
        }
    }
}