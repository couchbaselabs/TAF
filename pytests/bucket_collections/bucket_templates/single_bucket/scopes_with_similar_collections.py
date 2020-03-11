from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 5,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 10,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 10,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: True,

    "buckets": {
        "default": {
            "scopes": {
                CbServer.default_scope: {
                    "collections": {
                        "c0": {},
                        "c1": {},
                        "c2": {},
                        "c3": {},
                        "c4": {},
                        "c5": {},
                        "c6": {},
                        "c7": {},
                        "c8": {},
                        "c9": {},
                    }
                },
                "scope_2": {
                    "collections": {
                        "c0": {},
                        "c1": {},
                        "c2": {},
                        "c3": {},
                        "c4": {},
                        "c5": {},
                        "c6": {},
                        "c7": {},
                        "c8": {},
                        "c9": {},
                    }
                },
                "scope_3": {
                    "collections": {
                        "c0": {},
                        "c1": {},
                        "c2": {},
                        "c3": {},
                        "c4": {},
                        "c5": {},
                        "c6": {},
                        "c7": {},
                        "c8": {},
                        "c9": {},
                    }
                },
                "scope_4": {
                    "collections": {
                        "c0": {},
                        "c1": {},
                        "c2": {},
                        "c3": {},
                        "c4": {},
                        "c5": {},
                        "c6": {},
                        "c7": {},
                        "c8": {},
                        "c9": {},
                    }
                },
                "scope_5": {
                    "collections": {
                        "c0": {},
                        "c1": {},
                        "c2": {},
                        "c3": {},
                        "c4": {},
                        "c5": {},
                        "c6": {},
                        "c7": {},
                        "c8": {},
                        "c9": {},
                    }
                }
            }
        }
    }
}
