'''
Created on 06-Oct-2020

@author: umang
'''

from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 2,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 2,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,

    Bucket.ramQuotaMB: 100,
    Bucket.replicaIndex: 1,
    Bucket.maxTTL: 0,

    "buckets": {
        "default": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 3,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection_1": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        },
                        "collections_2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        }
                    }
                },
                "scope2": {
                    "collections": {
                        "collection1": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "bucket1": {
            "scopes": {
                "scope1": {
                    "collections": {
                        "collection1": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        }
                    }
                },
                "scope2": {
                    "collections": {
                        "collection1": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        },
                        "collection2": {
                            MetaConstants.NUM_ITEMS_PER_COLLECTION: 5000,
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        }
    }
}
