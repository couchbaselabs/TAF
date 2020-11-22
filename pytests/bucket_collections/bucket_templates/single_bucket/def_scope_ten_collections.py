from collections_helper.collections_spec_constants import MetaConstants
from BucketLib.bucket import Bucket


spec = {
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 2,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 2000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: True,
    
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
}
