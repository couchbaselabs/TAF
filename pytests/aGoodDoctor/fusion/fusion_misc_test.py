"""
Fusion Miscellaneous Regression Tests

Regression tests for fusion bugs that do not fit into focused lifecycle or
enable/disable test files.  Each test deploys and destroys its own Capella
cluster at 500M scale so failures are isolated and the bug scenario can be
reproduced deterministically without shared-cluster contamination.

Tests:
  1. test_concurrent_node_scale_down_and_disk_scale_up  — AV-134300
"""

import time

from TestInput import TestInputSingleton
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from pytests.basetestcase import BaseTestCase

from .fusion_test_base import _FusionTestBase


class FusionMiscTest(_FusionTestBase):
    """
    Miscellaneous fusion regression tests.

    Lifecycle policy
    ----------------
    Unlike sibling test classes that share a single cluster across a run,
    every test in this class deploys its own cluster in setUp and destroys
    it in tearDown.  This is intentional: the scenarios here involve
    destructive or irreversible cluster-state changes (simultaneous node
    scale-down + disk grow, etc.) that would corrupt a shared cluster for
    subsequent tests.
    """

    _FAILED_STATES = frozenset([
        "deployment_failed", "deploymentFailed", "redeploymentFailed",
        "rebalance_failed", "rebalanceFailed", "scaleFailed",
    ])

    # ------------------------------------------------------------------ #
    # setUp / tearDown                                                     #
    # ------------------------------------------------------------------ #

    def setUp(self):
        # Reset shared-cluster tracking so _FusionTestBase.setUp always
        # deploys a fresh cluster for this test rather than reusing one
        # left by a previous test.
        _FusionTestBase._shared_cluster_ids = None
        TestInputSingleton.input.capella.pop("clusters", None)

        super().setUp()

        self.log.info(f"[FusionMiscTest.setUp] deployed cluster={self.cluster.id}")
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy cluster before test setup", timeout=600)

        for bucket in list(self.cluster.buckets):
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        self.create_buckets(self.pod, self.tenant, self.cluster)

    def tearDown(self):
        # Stop any background threads started by _FusionTestBase
        if hasattr(self, "stop_run_event"):
            self.stop_run_event.set()
        self.stop_run = True

        # Best-effort: bring cluster to healthy before destruction so that
        # any in-flight deploy does not hold resources after the cluster is
        # deleted from the CP.
        try:
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id,
                "Wait before cluster destruction", timeout=1800)
        except Exception as e:
            self.log.warning(
                f"Could not wait for healthy state before destroy: {e}")

        # Delete test buckets (best effort)
        for bucket in list(self.cluster.buckets):
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []

        # Destroy the per-test cluster
        try:
            CapellaAPI.destroy_cluster(self.pod, self.tenant, self.cluster)
            self.log.info(
                f"[FusionMiscTest.tearDown] cluster {self.cluster.id} destroyed")
        except Exception as e:
            self.log.error(
                f"[FusionMiscTest.tearDown] failed to destroy cluster "
                f"{self.cluster.id}: {e}")

        # Clear shared-cluster state so the next test's setUp deploys fresh
        TestInputSingleton.input.capella.pop("clusters", None)
        _FusionTestBase._shared_cluster_ids = None

        # Call BaseTestCase.tearDown directly — _FusionTestBase.tearDown
        # has last-test-in-run logic that must not run here because we
        # already destroyed the cluster above.
        BaseTestCase.tearDown(self)

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _get_cluster_disk_gb(self, service_type="kv"):
        """Return the disk size in GB reported by CP for the given service type.

        Queries the internal cluster info endpoint and parses the first spec
        group whose services list contains service_type.
        """
        info = CapellaAPI.get_cluster_info(self.pod, self.tenant, self.cluster.id)
        specs = (info.get("data") or {}).get("specs") or []
        for spec in specs:
            services = [s.get("type", "") for s in (spec.get("services") or [])]
            if service_type in services:
                return (spec.get("disk") or {}).get("sizeInGb")
        return None

    def _scale_to_initial_config(self, initial_nodes, initial_disk_gb):
        """Scale the cluster to initial_nodes nodes with initial_disk_gb disk.

        No-op if the cluster is already at the requested configuration.
        Uses a single CP spec update so node count and disk size change
        atomically.
        """
        current_nodes = self.num_nodes["data"]
        current_disk = self.disk["data"]
        if current_nodes == initial_nodes and current_disk == initial_disk_gb:
            self.log.info(
                f"Cluster already at {initial_nodes} nodes / {initial_disk_gb} GB — "
                "skipping initial scale")
            return

        self.log.info(
            f"Scaling to initial config: {initial_nodes} nodes / "
            f"{initial_disk_gb} GB (from {current_nodes} / {current_disk})")
        self.disk["data"] = initial_disk_gb
        delta = initial_nodes - current_nodes
        config = self.rebalance_config("data", delta)
        self.wait_for_rebalances([
            self.task.async_rebalance_capella(
                self.pod, self.tenant, self.cluster, config,
                timeout=self.index_timeout)
        ])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy after initial scale", timeout=1800)

    # ------------------------------------------------------------------ #
    # Test 1: AV-134300 — concurrent node scale-down + disk grow          #
    # ------------------------------------------------------------------ #

    def test_concurrent_node_scale_down_and_disk_scale_up(self):
        """Regression for AV-134300.

        Scenario
        --------
        Start with initial_kv_nodes data nodes at initial_disk_gb GB disk.
        Enable fusion and load 500 M documents.  Then submit a single CP
        spec update that simultaneously:
          - scales nodes down from initial_kv_nodes → target_kv_nodes
          - grows disk from initial_disk_gb → target_disk_gb

        Bug
        ---
        With the bug present, the grow-disk API call from CP to dp-agent
        returns non-200 (timeout race — dp-agent is busy processing the
        scale-down), so CP marks the deploy as failed even though EBS
        volumes expanded successfully (~12 h later).

        Assertions
        ----------
        1. CP reaches "healthy" state after the combined operation.
        2. Disk size reported by CP matches target_disk_gb.
        3. Node count reported by CP matches target_kv_nodes.

        Parameters
        ----------
        initial_kv_nodes  (int, default 11)  — node count before the op
        target_kv_nodes   (int, default  3)  — node count after the op
        initial_disk_gb   (int, default 1024) — disk size before the op
        target_disk_gb    (int, default 2048) — disk size after the op
        """
        initial_nodes = self.input.param("initial_kv_nodes", 11)
        target_nodes = self.input.param("target_kv_nodes", 3)
        initial_disk_gb = self.input.param("initial_disk_gb", 1024)
        target_disk_gb = self.input.param("target_disk_gb", 2048)

        self.log.info(
            f"AV-134300: initial={initial_nodes} nodes/{initial_disk_gb} GB  "
            f"target={target_nodes} nodes/{target_disk_gb} GB  scale=500M docs")

        # ── Phase 1: reach initial cluster configuration ──────────────── #
        self._scale_to_initial_config(initial_nodes, initial_disk_gb)

        # ── Phase 2: enable fusion ────────────────────────────────────── #
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # ── Phase 3: load 500 M documents ────────────────────────────── #
        self._load_data(self.cluster, create_start=0, create_end=500_000_000)
        self.sleep(120, "Allow initial S3 sync before triggering scaling op")

        # ── Phase 4: submit simultaneous node scale-down + disk grow ──── #
        # Build a single CP spec with BOTH changes so they are sent in one
        # update_cluster_specs call — this reproduces the race from AV-134300.
        self.log.info(
            f"Submitting concurrent spec update: "
            f"{initial_nodes}→{target_nodes} nodes, "
            f"{initial_disk_gb}→{target_disk_gb} GB disk")
        self.disk["data"] = target_disk_gb
        delta = target_nodes - self.num_nodes["data"]
        combined_config = self.rebalance_config("data", delta)

        scale_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            combined_config,
            timeout=self.index_timeout)

        self.wait_for_rebalances([scale_task])

        # ── Phase 5: assert CP reached healthy state ──────────────────── #
        final_state = CapellaAPI.get_cluster_state(
            self.pod, self.tenant, self.cluster.id)

        self.assertNotIn(
            final_state, self._FAILED_STATES,
            f"CP stuck in '{final_state}' after concurrent node scale-down + "
            f"disk grow (AV-134300 regression): CP gave up on grow-disk API "
            f"even though EBS volumes expanded.")
        self.assertEqual(
            final_state, "healthy",
            f"Expected cluster state 'healthy' after combined scale op, "
            f"got '{final_state}'")

        # ── Phase 6: verify disk size reported by CP ─────────────────── #
        actual_disk_gb = self._get_cluster_disk_gb("kv")
        self.log.info(
            f"CP reports disk size after combined scale: {actual_disk_gb} GB "
            f"(expected {target_disk_gb} GB)")
        self.assertEqual(
            actual_disk_gb, target_disk_gb,
            f"Disk size mismatch after combined scale op: "
            f"expected {target_disk_gb} GB, CP reports {actual_disk_gb} GB")

        # ── Phase 7: verify node count reported by CP ─────────────────── #
        nodes = CapellaAPI.get_nodes(self.pod, self.tenant, self.cluster.id)
        data_nodes = [
            n for n in nodes
            if "Data" in (n.get("services") or [])
        ]
        self.log.info(
            f"CP reports {len(data_nodes)} data nodes after combined scale "
            f"(expected {target_nodes})")
        self.assertEqual(
            len(data_nodes), target_nodes,
            f"Node count mismatch after combined scale op: "
            f"expected {target_nodes}, CP reports {len(data_nodes)}")

        self.log.info(
            "AV-134300 regression test passed: CP reached healthy state with "
            f"{len(data_nodes)} nodes at {actual_disk_gb} GB disk")
