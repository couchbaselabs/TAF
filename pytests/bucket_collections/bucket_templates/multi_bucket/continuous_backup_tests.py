"""
Two-bucket Magma spec for continuous backup / PITR multi-bucket tests.

Both buckets use:
  - Magma storage backend (PITR is only supported on Magma buckets)
  - historyRetentionCollectionDefault = true (enables per-collection history)
  - historyRetentionSeconds = 6000  (matches single_bucket.continuous_backup_tests)
  - continuousBackupEnabled = true

Topology is intentionally smaller than the single-bucket spec (5 scopes x 5
collections x 500 items) so the cluster's RAM quota can comfortably hold two
buckets simultaneously in a standard 4-node test environment.
"""

from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import MetaConstants

spec = {
    # Topology
    MetaConstants.USE_SIMPLE_NAMES: True,
    MetaConstants.NUM_BUCKETS: 2,
    MetaConstants.NUM_SCOPES_PER_BUCKET: 5,
    MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
    MetaConstants.NUM_ITEMS_PER_COLLECTION: 500,
    MetaConstants.REMOVE_DEFAULT_COLLECTION: False,
    MetaConstants.CREATE_COLLECTIONS_USING_MANIFEST_IMPORT: False,
    MetaConstants.LOAD_COLLECTIONS_EXPONENTIALLY: False,

    # Bucket settings — same profile as single_bucket.continuous_backup_tests
    Bucket.bucketType: Bucket.Type.MEMBASE,
    Bucket.ramQuotaMB: 100,
    Bucket.replicaNumber: Bucket.ReplicaNum.ONE,
    Bucket.replicaIndex: 1,
    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
    Bucket.priority: Bucket.Priority.LOW,
    Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
    Bucket.maxTTL: 0,

    # Magma is required for PITR / continuous backup
    Bucket.storageBackend: Bucket.StorageBackend.magma,
    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
    Bucket.compressionMode: Bucket.CompressionMode.PASSIVE,
    Bucket.autoCompactionDefined: "true",

    # Continuous Backup / PITR settings
    Bucket.historyRetentionSeconds: 6000,
    Bucket.historyRetentionCollectionDefault: "true",
    Bucket.continuousBackupEnabled: "true",
}
