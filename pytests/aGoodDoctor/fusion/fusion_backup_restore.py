"""End-to-end Fusion snapshot backup and restore tests for Capella Dedicated.

This is the canonical Fusion backup/restore suite. Each test is one row of the
backup/restore matrix (source fusion state x source guest volumes x target
fusion state x target guest volumes), named so the scenario is obvious. All run
through FusionBackupRestoreBase.run_backup_restore_case, which provisions the
clusters, loads data, takes the backup, restores, and validates (data
integrity, pre-existing target guest-volume deletion, fusion S3 flush, and the
expected post-restore fusion state).

Naming key:
  <source fusion state>_source[_with/without_guest_volumes]_to_<target state>_
  target[_with/without_guest_volumes]   (or *_self_cluster_restore /
  *_while_enabling / *_while_disabling for the transitional cases).
"""

from pytests.aGoodDoctor.fusion.fusion_backup_restore_base import (
    FusionBackupRestoreBase,
)


class FusionBackupRestore(FusionBackupRestoreBase):

    # ==================================================================
    # Basic matrix — stable target fusion states (sheet TC1-TC9)
    # ==================================================================

    def test_disabled_source_to_disabled_target(self):
        """TC1: Fusion-disabled source -> Fusion-disabled target.
        Standard snapshot-only backup/restore; no fusion operations. Validates
        data integrity (doc-count match) and that the target stays Fusion-free.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=False, expect_target_enabled=False)
        self.log.info("test_disabled_source_to_disabled_target PASSED")

    def test_disabled_source_to_enabled_target_without_guest_volumes(self):
        """TC2: Fusion-disabled source -> Fusion-enabled target (no target guest
        volumes). Restoring a non-fusion backup leaves the target Fusion-free
        (target S3 flushed); data integrity verified.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            expect_target_enabled=False)
        self.log.info(
            "test_disabled_source_to_enabled_target_without_guest_volumes "
            "PASSED")

    def test_disabled_source_to_enabled_target_with_guest_volumes(self):
        """TC3: Fusion-disabled source -> Fusion-enabled target that already has
        its OWN guest volumes. Restore deletes the pre-existing target guest
        volumes, flushes target S3, and leaves the target Fusion-free.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            expect_target_enabled=False)
        self.log.info(
            "test_disabled_source_to_enabled_target_with_guest_volumes PASSED")

    def test_enabled_source_without_guest_volumes_to_disabled_target(self):
        """TC4: Fusion-enabled source with NO guest volumes -> Fusion-disabled
        target. Server enables fusion on the destination; target ends enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=False,
            target_fusion_enabled=False, expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_without_guest_volumes_to_disabled_target "
            "PASSED")

    def test_enabled_source_without_guest_volumes_to_enabled_target_without_guest_volumes(self):
        """TC5: Fusion-enabled source (no guest volumes) -> Fusion-enabled
        target (no guest volumes). Target S3 flushed; target ends enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=False,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_without_guest_volumes_to_enabled_target_"
            "without_guest_volumes PASSED")

    def test_enabled_source_without_guest_volumes_to_enabled_target_with_guest_volumes(self):
        """TC6: Fusion-enabled source (no guest volumes) -> Fusion-enabled
        target that has its OWN guest volumes. Pre-existing target guest volumes
        deleted; target S3 flushed; target ends enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=False,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_without_guest_volumes_to_enabled_target_"
            "with_guest_volumes PASSED")

    def test_enabled_source_with_guest_volumes_to_disabled_target(self):
        """TC7: Fusion-enabled source WITH guest volumes -> Fusion-disabled
        target. The target transitions disabled -> enabled (server enables
        fusion on the destination); data integrity verified.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=False, expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_with_guest_volumes_to_disabled_target PASSED")

    def test_enabled_source_with_guest_volumes_to_enabled_target_without_guest_volumes(self):
        """TC8: Fusion-enabled source WITH guest volumes -> Fusion-enabled
        target with no guest volumes. Target S3 flushed; guest volumes are
        (re)created on the target (here: regenerated by a post-restore
        rebalance); target ends enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_with_guest_volumes_to_enabled_target_"
            "without_guest_volumes PASSED")

    def test_enabled_source_with_guest_volumes_to_enabled_target_with_guest_volumes(self):
        """TC9: Fusion-enabled source WITH guest volumes -> Fusion-enabled
        target that also has its OWN guest volumes. Pre-existing target guest
        volumes deleted, target S3 flushed, guest volumes (re)created; target
        ends enabled. (Both clusters have guest volumes.)
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_with_guest_volumes_to_enabled_target_"
            "with_guest_volumes PASSED")

    # ==================================================================
    # Transitional target states — restore fired DURING enable/disable,
    # without waiting for the transition to finish (sheet TC10-TC15)
    # ==================================================================

    def test_disabled_source_to_target_while_enabling(self):
        """TC10: Fusion-disabled source -> target restored WHILE it is still
        enabling (enable triggered, restore fired immediately, no wait). The
        restore adopts the source's DISABLED state, so the in-flight enable is
        cancelled and the target ends Fusion-free (disabled); data integrity
        verified and target S3 flushed. You cannot end up enabled by restoring
        a disabled backup, even if the target was mid-enabling.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=False, target_transition="enabling",
            expect_target_enabled=False)
        self.log.info("test_disabled_source_to_target_while_enabling PASSED")

    def test_enabled_source_to_target_while_enabling(self):
        """TC11: Fusion-enabled source (with guest volumes) -> target restored
        WHILE it is still enabling. In-flight enable cancelled, target S3
        flushed, guest volumes (re)created; target ends enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=False, target_transition="enabling",
            expect_target_enabled=True)
        self.log.info("test_enabled_source_to_target_while_enabling PASSED")

    def test_disabled_source_to_target_while_disabling_without_guest_volumes(self):
        """TC12: Fusion-disabled source -> target restored WHILE it is still
        disabling (no target guest volumes). Disabling cancelled; data
        integrity verified; target ends disabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            target_transition="disabling", expect_target_enabled=False)
        self.log.info(
            "test_disabled_source_to_target_while_disabling_without_guest_"
            "volumes PASSED")

    def test_enabled_source_to_target_while_disabling_without_guest_volumes(self):
        """TC13: Fusion-enabled source (with guest volumes) -> target restored
        WHILE it is still disabling (no target guest volumes). Disabling
        cancelled, target S3 flushed, target transitions disabling -> enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            target_transition="disabling", expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_to_target_while_disabling_without_guest_"
            "volumes PASSED")

    def test_disabled_source_to_target_while_disabling_with_guest_volumes(self):
        """TC14: Fusion-disabled source -> target restored WHILE it is still
        disabling and the target HAS guest volumes. Disabling cancelled,
        pre-existing target guest volumes deleted, target S3 deleted; target
        ends disabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            target_transition="disabling", expect_target_enabled=False)
        self.log.info(
            "test_disabled_source_to_target_while_disabling_with_guest_"
            "volumes PASSED")

    def test_enabled_source_to_target_while_disabling_with_guest_volumes(self):
        """TC15: Fusion-enabled source (with guest volumes) -> target restored
        WHILE it is still disabling and the target HAS guest volumes. Disabling
        cancelled, pre-existing target guest volumes deleted, target S3 flushed,
        target transitions disabling -> enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            target_transition="disabling", expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_to_target_while_disabling_with_guest_"
            "volumes PASSED")

    # ==================================================================
    # Self-cluster restore (source == target)
    # ==================================================================

    def test_enabled_source_with_guest_volumes_self_cluster_restore(self):
        """TC16 (self-cluster): a Fusion-enabled cluster with guest volumes is
        backed up and restored onto ITSELF. Validates data integrity and that
        the cluster stays Fusion-enabled (guest volumes regenerate on a
        post-restore rebalance).
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, same_cluster=True,
            expect_target_enabled=True, check_pre_existing_gv_deleted=False)
        self.log.info(
            "test_enabled_source_with_guest_volumes_self_cluster_restore "
            "PASSED")

    # ==================================================================
    # Cross-region restore (source in aws_region, target in the alternate
    # region, chosen automatically from aws_region)
    # ==================================================================

    def test_enabled_source_with_guest_volumes_cross_region_to_enabled_target(self):
        """TC17 (CR1): Fusion-enabled source WITH guest volumes in the
        source region -> Fusion-enabled target in the ALTERNATE region. The
        source snapshot is restored across regions; guest volumes regenerate in
        the target's region and its fusion S3 log-store is created there.
        Validates data integrity and that the target ends Fusion-enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            cross_region=True, expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_with_guest_volumes_cross_region_to_enabled_"
            "target PASSED")

    def test_disabled_source_cross_region_to_disabled_target(self):
        """TC18 (CR2): Fusion-disabled source in the source region
        -> Fusion-disabled target in the ALTERNATE region. Plain snapshot
        restore across regions; validates data integrity and that the target
        stays Fusion-free.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=False, cross_region=True,
            expect_target_enabled=False)
        self.log.info(
            "test_disabled_source_cross_region_to_disabled_target PASSED")

    def test_enabled_source_with_guest_volumes_cross_region_to_enabled_target_with_guest_volumes(self):
        """TC19 (CR3): Fusion-enabled source WITH
        guest volumes -> Fusion-enabled target in the ALTERNATE region that
        already has its OWN guest volumes. The restore deletes the target's
        pre-existing guest volumes (in the target region), then guest volumes
        regenerate there; validates data integrity and target ends enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            cross_region=True, expect_target_enabled=True)
        self.log.info(
            "test_enabled_source_with_guest_volumes_cross_region_to_enabled_"
            "target_with_guest_volumes PASSED")

    # ==================================================================
    # Cross-region BACKUP (copy the snapshot to the alternate region at
    # backup time, verify the copy landed, then restore from it there).
    # ==================================================================

    def test_cross_region_backup_disabled_source_to_disabled_target(self):
        """TC20 (CRB1): Fusion-disabled source — snapshot is COPIED to the
        alternate region at backup time (cross-region backup), the copy is
        verified on the backup record, then it is restored into a disabled
        target in that region. Validates data integrity and target stays
        Fusion-free.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=False, source_has_guest_volumes=False,
            target_fusion_enabled=False, cross_region_backup=True,
            expect_target_enabled=False)
        self.log.info(
            "test_cross_region_backup_disabled_source_to_disabled_target "
            "PASSED")

    def test_cross_region_backup_enabled_source_with_guest_volumes_to_enabled_target(self):
        """TC21 (CRB2): Fusion-enabled source WITH guest volumes — snapshot is
        COPIED to the alternate region at backup time, the copy is verified,
        then restored into a Fusion-enabled target in that region. Validates
        data integrity and target ends Fusion-enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=False,
            cross_region_backup=True, expect_target_enabled=True)
        self.log.info(
            "test_cross_region_backup_enabled_source_with_guest_volumes_to_"
            "enabled_target PASSED")

    def test_cross_region_backup_enabled_source_with_guest_volumes_to_enabled_target_with_guest_volumes(self):
        """TC22 (CRB3): Fusion-enabled source WITH guest volumes — snapshot is
        COPIED to the alternate region at backup time, the copy is verified,
        then restored into a Fusion-enabled target in that region that already
        has its OWN guest volumes. Validates data integrity and target ends
        Fusion-enabled.
        """
        self.run_backup_restore_case(
            source_fusion_enabled=True, source_has_guest_volumes=True,
            target_fusion_enabled=True, target_has_guest_volumes=True,
            cross_region_backup=True, expect_target_enabled=True)
        self.log.info(
            "test_cross_region_backup_enabled_source_with_guest_volumes_to_"
            "enabled_target_with_guest_volumes PASSED")
