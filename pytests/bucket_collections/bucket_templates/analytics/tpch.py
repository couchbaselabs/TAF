'''
Created on 06-Oct-2020

@author: umang
'''

from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 8,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 0,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.USE_SIMPLE_NAMES: True,

    Bucket.ramQuotaMB: 100,
    Bucket.replicaIndex: 1,
    Bucket.maxTTL: 0,

    "buckets": {
        "region": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "nation": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "supplier": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "customer": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "part": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "partsupp": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "orders": {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
        "lineitem": {
            Bucket.ramQuotaMB: 200,
            MetaConstants.NUM_SCOPES_PER_BUCKET: 1,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 1,
            "scopes": {
                "_default": {
                    "collections": {
                        "_default": {
                            Bucket.maxTTL: 0
                        }
                    }
                }
            }
        },
    }
}
