from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    # Topology
    MetaConstants.USE_SIMPLE_NAMES: True,
    MetaConstants.NUM_BUCKETS: 1,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 10,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 10,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: False,
    MetaConstants.LOAD_COLLECTIONS_EXPONENTIALLY: False,

    # Bucket Settings
    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.ramQuotaMB: 256,
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,
    Bucket.autoCompactionDefined: "true",

    # Continuous Backup / PITR Settings
    Bucket.historyRetentionSeconds: 6000,
    Bucket.historyRetentionCollectionDefault: "true",
    Bucket.continuousBackupEnabled: "true",
}