"""
Fusion Miscellaneous Regression Tests

Regression tests for fusion bugs that do not fit into focused lifecycle or
enable/disable test files.  Each test deploys and destroys its own Capella
cluster at 500M scale so failures are isolated and the bug scenario can be
reproduced deterministically without shared-cluster contamination.

Tests:
  1. test_concurrent_node_scale_down_and_disk_scale_up  — AV-134300
  2. test_concurrent_node_and_disk_scale_up
  3. test_hydration_fills_disk_triggers_disk_auto_scaling — AV-137329
  4. test_fusion_accelerator_with_io2_storage — accelerator lifecycle on
     IO2 disk storage at 200M-item scale
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

    # Main-volume usage percent at which we consider diskAutoScaling to have
    # unambiguously failed to react (AV-137329: volume observed pinned at
    # 100% used / 20K free with zero resize attempts across all nodes).
    _DISK_AUTOSCALE_FAIL_PCT = 97

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

    def _get_cluster_disk_type(self, service_type="kv"):
        """Return the disk storage type (e.g. "GP3", "IO2") reported by CP
        for the given service type.

        Mirrors _get_cluster_disk_gb but reads the "type" field of the disk
        spec instead of "sizeInGb".
        """
        info = CapellaAPI.get_cluster_info(self.pod, self.tenant, self.cluster.id)
        specs = (info.get("data") or {}).get("specs") or []
        for spec in specs:
            services = [s.get("type", "") for s in (spec.get("services") or [])]
            if service_type in services:
                return (spec.get("disk") or {}).get("type")
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
                timeout=self.rebalance_timeout)
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
            timeout=self.rebalance_timeout)

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

    # ------------------------------------------------------------------ #
    # Test 2: concurrent node scale-up + disk scale-up                    #
    # ------------------------------------------------------------------ #

    def test_concurrent_node_and_disk_scale_up(self):
        """Concurrent scale-up of both node count and disk size with fusion enabled.

        Scenario
        --------
        Start with initial_kv_nodes data nodes at initial_disk_gb GB disk.
        Enable fusion and load 500 M documents.  Then submit a single CP
        spec update that simultaneously:
          - adds nodes from initial_kv_nodes → target_kv_nodes
          - grows disk from initial_disk_gb → target_disk_gb

        This exercises the CP/dp-agent interaction path for combined
        horizontal and vertical scale-up under a live fusion dataset,
        complementing AV-134300 which covers the node-down + disk-up path.

        Assertions
        ----------
        1. CP reaches "healthy" state after the combined operation.
        2. Disk size reported by CP matches target_disk_gb.
        3. Node count reported by CP matches target_kv_nodes.

        Parameters
        ----------
        initial_kv_nodes  (int, default  3) — node count before the op
        target_kv_nodes   (int, default  6) — node count after the op
        initial_disk_gb   (int, default 200) — disk size before the op
        target_disk_gb    (int, default 400) — disk size after the op
        """
        initial_nodes = self.input.param("initial_kv_nodes", 3)
        target_nodes = self.input.param("target_kv_nodes", 6)
        initial_disk_gb = self.input.param("initial_disk_gb", 200)
        target_disk_gb = self.input.param("target_disk_gb", 400)

        self.log.info(
            f"Concurrent scale-up: initial={initial_nodes} nodes/{initial_disk_gb} GB  "
            f"target={target_nodes} nodes/{target_disk_gb} GB  dataset=500M docs")

        # ── Phase 1: reach initial cluster configuration ──────────────── #
        self._scale_to_initial_config(initial_nodes, initial_disk_gb)

        # ── Phase 2: enable fusion ────────────────────────────────────── #
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # ── Phase 3: load 500 M documents ────────────────────────────── #
        self._load_data(self.cluster, create_start=0, create_end=500_000_000)
        self.sleep(120, "Allow initial S3 sync before triggering scaling op")

        # ── Phase 4: submit simultaneous node scale-up + disk grow ────── #
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
            timeout=self.rebalance_timeout)

        self.wait_for_rebalances([scale_task])

        # ── Phase 5: assert CP reached healthy state ──────────────────── #
        final_state = CapellaAPI.get_cluster_state(
            self.pod, self.tenant, self.cluster.id)

        self.assertNotIn(
            final_state, self._FAILED_STATES,
            f"CP stuck in '{final_state}' after concurrent node scale-up + "
            f"disk grow")
        self.assertEqual(
            final_state, "healthy",
            f"Expected cluster state 'healthy' after combined scale-up, "
            f"got '{final_state}'")

        # ── Phase 6: verify disk size reported by CP ─────────────────── #
        actual_disk_gb = self._get_cluster_disk_gb("kv")
        self.log.info(
            f"CP reports disk size after combined scale: {actual_disk_gb} GB "
            f"(expected {target_disk_gb} GB)")
        self.assertEqual(
            actual_disk_gb, target_disk_gb,
            f"Disk size mismatch after combined scale-up: "
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
            f"Node count mismatch after combined scale-up: "
            f"expected {target_nodes}, CP reports {len(data_nodes)}")

        self.log.info(
            "Concurrent node+disk scale-up test passed: CP reached healthy "
            f"state with {len(data_nodes)} nodes at {actual_disk_gb} GB disk")

    # ------------------------------------------------------------------ #
    # Test 3: AV-137329 — hydration fills disk, diskAutoScaling must react #
    # ------------------------------------------------------------------ #

    def test_hydration_fills_disk_triggers_disk_auto_scaling(self):
        """Regression for AV-137329.

        Scenario
        --------
        Deploy a small cluster with diskAutoScaling enabled (the default,
        see hostedOPD.rebalance_config) but a deliberately small main disk
        (initial_disk_gb). Enable fusion and fire off a data load sized to
        exceed initial_disk_gb of local hydration well before it finishes,
        then poll both:
          - the actual OS-level disk usage % on every node's main
            persistent-data volume (/opt/couchbase/var/lib/couchbase)
          - the disk size CP reports for the cluster

        Bug
        ---
        With the bug present (AV-137329), Capella's control plane never
        grows the main EBS/LVM volume despite diskAutoScaling.enabled=true:
        local disk usage climbs to 100% on every node with zero resize
        attempts, and the cluster suffers cluster-wide ENOSPC failures
        (ns_server bootstrap, goxdcr, event/ns log, KV logger all fail)
        instead of the volume being grown ahead of time.

        Assertions
        ----------
        Fail as soon as any node's main-volume usage reaches
        _DISK_AUTOSCALE_FAIL_PCT while CP's reported disk size is still
        initial_disk_gb — this is the AV-137329 signature. Pass as soon as
        CP's reported disk size grows above initial_disk_gb first.

        Parameters
        ----------
        initial_kv_nodes   (int, default 3)           — node count
        initial_disk_gb    (int, default 100)          — deliberately small
                            main disk (smallest AWS gp3 tier) so hydration
                            can outpace it within the test timeout
        num_items          (int, default 150_000_000)  — sized so local
                            hydration volume exceeds initial_disk_gb well
                            before the load itself completes
        poll_interval_secs (int, default 60)
        disk_fill_timeout  (int, default 7200)         — max time to wait
                            for either an autoscale or the fail threshold
        """
        initial_nodes = self.input.param("initial_kv_nodes", 3)
        initial_disk_gb = self.input.param("initial_disk_gb", 100)
        num_items = self.input.param("num_items", 150_000_000)
        poll_interval_secs = self.input.param("poll_interval_secs", 60)
        disk_fill_timeout = self.input.param("disk_fill_timeout", 7200)

        self.log.info(
            f"AV-137329: {initial_nodes} nodes / {initial_disk_gb} GB disk, "
            f"loading {num_items} docs to force hydration past capacity")

        # ── Phase 1: reach initial cluster configuration ──────────────── #
        # diskAutoScaling is enabled by default (self.diskAutoScaling, see
        # hostedOPD.rebalance_config) and is included in every spec update.
        self._scale_to_initial_config(initial_nodes, initial_disk_gb)
        self.assertTrue(
            self.diskAutoScaling,
            f"Test requires diskAutoScaling enabled, got {self.diskAutoScaling}")

        # ── Phase 2: enable fusion ─────────────────────────────────────── #
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # ── Phase 3: fire off a load sized to outpace the small disk ──── #
        # wait_for_load=False: if the bug reproduces, nodes never finish
        # loading because the disk fills up, so we must not block on it —
        # poll disk usage / CP disk size independently instead.
        self._load_data(
            self.cluster, create_start=0, create_end=num_items,
            wait_for_load=False)

        # ── Phase 4: poll OS-level disk usage vs CP-reported disk size ── #
        baseline_disk_gb = self._get_cluster_disk_gb("kv")
        self.assertEqual(
            baseline_disk_gb, initial_disk_gb,
            f"Expected CP to report {initial_disk_gb} GB before load, got "
            f"{baseline_disk_gb} GB")

        deadline = time.time() + disk_fill_timeout
        autoscaled = False
        max_usage_seen = 0
        current_disk_gb = baseline_disk_gb
        while time.time() < deadline:
            current_disk_gb = self._get_cluster_disk_gb("kv")
            if current_disk_gb and current_disk_gb > initial_disk_gb:
                self.log.info(
                    f"CP grew main disk {initial_disk_gb} -> {current_disk_gb} GB "
                    "— diskAutoScaling triggered as expected")
                autoscaled = True
                break

            usage = self.cp_monitor.get_main_volume_disk_usage_percent(self.cluster)
            node_max = max(
                [v for v in usage.values() if v is not None], default=0)
            max_usage_seen = max(max_usage_seen, node_max)
            self.log.info(
                f"Main volume usage: max={node_max}% across nodes "
                f"(CP disk size still {current_disk_gb} GB)")

            if node_max >= self._DISK_AUTOSCALE_FAIL_PCT:
                break

            time.sleep(poll_interval_secs)

        # ── Phase 5: assert autoscaling kicked in before the disk filled ── #
        final_disk_gb = self._get_cluster_disk_gb("kv")
        self.assertTrue(
            autoscaled or (final_disk_gb is not None
                           and final_disk_gb > initial_disk_gb),
            f"AV-137329 regression: main EBS volume usage reached "
            f"{max_usage_seen}% while CP-reported disk size stayed at "
            f"{initial_disk_gb} GB for up to {disk_fill_timeout}s — "
            "diskAutoScaling.enabled=true never triggered a resize before "
            "the volume filled up.")

        self.log.info(
            "AV-137329 regression test passed: CP grew disk to "
            f"{final_disk_gb} GB before main volume usage became critical "
            f"(max observed {max_usage_seen}%)")

    # ------------------------------------------------------------------ #
    # Test 4: fusion accelerator lifecycle on IO2 disk storage             #
    # ------------------------------------------------------------------ #

    def test_fusion_accelerator_with_io2_storage(self):
        """Fusion accelerator lifecycle validation on IO2-backed disk storage.

        Scenario
        --------
        This class's setUp deploys a dedicated cluster whose data-service
        disk uses IO2 storage instead of the usual GP3 (driven entirely by
        the conf-file 'type=IO2' param — dedicatedbasetestcase.py reads
        'type' before the test method runs, so the disk type cannot be
        chosen here in code). Enable fusion, load num_items documents, then
        trigger a horizontal scale-out rebalance — the only path that
        brings up fusion accelerator EC2/EBS nodes, since loading data
        alone never exercises them — and verify the whole accelerator
        lifecycle behaves correctly on this less-common disk backend.

        Assertions
        ----------
        1. The cluster's data-service disk actually came up as IO2 (sanity
           check that the conf-driven deploy param took effect).
        2. CP reaches "healthy" state after the scale-out rebalance (no
           deployment_failed/rebalance_failed/scaleFailed).
        3. Fusion state is still "enabled" after the rebalance.
        4. Fusion accelerator instances that appeared during the rebalance
           were killed again after it completed.
        5. No CRITICAL errors surfaced in memcached logs post-rebalance.

        Parameters
        ----------
        initial_kv_nodes (int, default 3)          — node count before
                          scale-out; must match the conf-file 'kv_nodes'
                          param used for the initial deploy
        target_kv_nodes  (int, default 6)          — node count after the
                          scale-out rebalance that triggers accelerators
        num_items        (int, default 200_000_000) — load volume, well
                          above fusion_threshold_gib so accelerators launch
        """
        initial_nodes = self.input.param("initial_kv_nodes", 3)
        target_nodes = self.input.param("target_kv_nodes", 6)
        num_items = self.input.param("num_items", 200_000_000)

        self.PrintStep(
            f"IO2 accelerator lifecycle: {initial_nodes}->{target_nodes} "
            f"nodes, {num_items} docs")

        # ── Phase 1: sanity-check the cluster actually deployed on IO2 ─── #
        disk_type = self._get_cluster_disk_type("kv")
        self.assertEqual(
            (disk_type or "").upper(), "IO2",
            f"Expected cluster to be deployed with IO2 disk storage, CP "
            f"reports disk.type={disk_type!r} — check conf param 'type=IO2'")

        # ── Phase 2: enable fusion ───────────────────────────────────── #
        self.PrintStep("Enabling fusion on IO2-backed cluster")
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # ── Phase 3: load num_items documents ───────────────────────── #
        self.PrintStep(f"Loading {num_items} documents on IO2-backed cluster")
        self._load_data(self.cluster, create_start=0, create_end=num_items)
        self.sleep(120, "Allow initial S3 sync before triggering scale-out")

        # ── Phase 4: trigger a horizontal scale-out rebalance ───────── #
        # Scale-out is the path that brings up fusion accelerator nodes —
        # loading data alone never exercises them.
        self.PrintStep(
            f"Triggering scale-out rebalance {initial_nodes}->{target_nodes} "
            "nodes to exercise fusion accelerator lifecycle")
        delta = target_nodes - self.num_nodes["data"]
        scale_out_config = self.rebalance_config("data", delta)

        scale_task = self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster,
            scale_out_config, timeout=self.rebalance_timeout)

        self.wait_for_rebalances([scale_task])

        # ── Phase 5: assert CP reached healthy state ────────────────── #
        final_state = CapellaAPI.get_cluster_state(
            self.pod, self.tenant, self.cluster.id)
        self.assertNotIn(
            final_state, self._FAILED_STATES,
            f"CP stuck in '{final_state}' after IO2 scale-out rebalance")
        self.assertEqual(
            final_state, "healthy",
            f"Expected cluster state 'healthy' after IO2 scale-out rebalance, "
            f"got '{final_state}'")

        # ── Phase 6: assert fusion state is still enabled ───────────── #
        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion state drifted away from 'enabled' after IO2 scale-out "
            f"rebalance: {fusion_status}")

        # ── Phase 7: accelerator nodes appeared and were cleaned up ─── #
        self.PrintStep(
            "Verifying fusion accelerator nodes were killed after rebalance")
        self.assertTrue(
            self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                self.cluster),
            "Fusion accelerator nodes were not cleaned up after the IO2 "
            "scale-out rebalance completed")

        # ── Phase 8: no CRITICAL errors in memcached logs ───────────── #
        self.assertFalse(
            self.cp_monitor.scan_memcached_logs_for_errors(self.cluster),
            "CRITICAL errors found in memcached logs after IO2 scale-out "
            "rebalance")

        self.log.info(
            "IO2 accelerator lifecycle test passed: cluster healthy, fusion "
            "enabled, accelerator nodes cleaned up, no CRITICAL errors")
