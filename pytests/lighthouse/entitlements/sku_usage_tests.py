# -*- coding: utf-8 -*-
"""
UCP Portal SKU usage tests -- what GET /api/v1/entitlements/usage reports as
the fleet changes underneath it.

    - no SKU covering the cluster: it is visible to the portal but matches no
      entitlement, so nothing is counted against an entitled figure. This is
      the control for the two cases below -- without it, a usage body that
      never populates would still look like a pass. Note this is set up with a
      profile whose SKUs do not cover the cluster, NOT by emptying the
      profile: emptying it is impossible on current builds (see the test's own
      docstring).
    - one SKU configured: the same cluster, tagged to that SKU's
      classification, now shows up as a contributor to that SKU's row with
      the entitled count the SKU declares.
    - a node removed: rebalancing one node out of the cluster must lower the
      consumed count by exactly one, and rebalancing it back must restore it.
      Matching is evaluated when usage is read, not frozen at ingest, so the
      same SKU has to follow the topology in both directions.

These run against the REAL cluster and its collector, not synthetic ingest:
the node-removal case is only meaningful if an actual rebalance drives it.
The collector's default reporting interval is 2 h, which no test can wait
for, so every report here is forced by dropping the interval to one second
via diag/eval and restoring it to 2 h immediately afterwards -- see
_trigger_report_and_restore_interval. The portal is addressed by its default
DNS domain (the collector's own default endpoint), the same way the collector
and failover suites address it; the collector resolves that name through the
lab resolver, so an IP would not exercise the same path.

Every usage assertion is scoped to THIS cluster's UUID. The lab portal is a
shared fleet: clusters can never be deleted, dozens of old ones are still on
it, and any of them may report while these tests run. A usage row's `used` is
therefore a fleet-wide number that moves on its own -- observed climbing 83 ->
95 over a handful of runs -- so it is never asserted on, not even as a
before/after delta, because a foreign cluster reporting between two reads
would break the delta for a reason unrelated to the test.

What IS asserted is usage_nodes_for_cluster(): the `nodes` this cluster
contributes, summed out of the row's own contributor list. That number moves
only when this cluster's topology moves. The row's `used` is still logged, as
context for anyone reading a failure.

The SKU is sized well above any lab node by default (see sku_logical_cores /
sku_ram_bytes) so that instance-size matching cannot be what makes a case
fail; the per-dimension ceiling behaviour is a separate concern from these
three.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.collector_helper_methods import (
    LIGHTHOUSE_DEFAULT_PORTAL_PORT,
    get_cb_cluster_uuid,
    get_portal_cluster,
    set_lighthouse_interval_via_diag_eval,
    set_lighthouse_ns_config_via_diag_eval,
    wait_for_cluster_on_portal,
    wait_for_portal_node_count,
)
from lighthouse.entitlement_helper_methods import (
    active_window,
    describe_usage,
    find_usage_row,
    get_entitlement_profile,
    get_profile_subscriptions,
    get_usage,
    rows_containing_cluster,
    set_entitlement_subscriptions,
    usage_nodes_for_cluster,
    tag_portal_cluster,
    usage_row_cluster_uuids,
    usage_row_entitled,
    usage_row_used,
)
from lighthouse.ucp_helper_methods import (
    build_subscription_payload,
    create_session,
    get_session_cookie,
)
from unified_control_plane.constants import COLLECTOR_DEFAULT_ENDPOINT


class SkuUsageTests(LighthouseBase):

    def setUp(self):
        super(SkuUsageTests, self).setUp()
        self.portal_domain = self.input.param(
            "portal_domain", COLLECTOR_DEFAULT_ENDPOINT)
        # Report forcing: one-second interval plus a short settle, instead of
        # the collector's 2 h default. Both are conf-tunable because the
        # settle time needed depends on how loaded the lab portal is.
        self.report_interval_seconds = self.input.param(
            "report_interval_seconds", 1)
        self.report_sleep_seconds = self.input.param(
            "report_sleep_seconds", 10)
        self.portal_poll_timeout = self.input.param("portal_poll_timeout", 120)
        # SKU under test. Cores/RAM are per-node ceilings, deliberately set
        # far above any lab node so instance-size matching is never the
        # reason a case here fails.
        self.sku_classification = self.input.param(
            "sku_classification", "production")
        self.sku_support_level = self.input.param(
            "sku_support_level", "platinum")
        self.sku_node_limit = self.input.param("sku_node_limit", 100)
        self.sku_logical_cores = self.input.param("sku_logical_cores", 256)
        self.sku_ram_bytes = self.input.param(
            "sku_ram_bytes", 1099511627776)
        # Used by the control case: a SKU for a classification this cluster is
        # NOT tagged with, so nothing can match it.
        self.nonmatching_classification = self.input.param(
            "nonmatching_classification", "development")
        self.nonmatching_support_level = self.input.param(
            "nonmatching_support_level", "silver")

        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)

        # The entitlement profile is a single global singleton, so it must be
        # put back exactly as found for the next test or run.
        profile = get_entitlement_profile(self.ucp_client)
        self.assertIsNotNone(
            profile, "Could not read the baseline entitlement profile")
        self._original_subscriptions = get_profile_subscriptions(profile)
        self.log.info(
            "Baseline entitlement profile holds %d subscription(s)"
            % len(self._original_subscriptions))
        # Set by the node-removal test so tearDown can put the node back if
        # the test dies between the rebalance-out and the rebalance-in.
        self._node_pending_add_back = None

    def tearDown(self):
        try:
            if self._node_pending_add_back is not None:
                self.log.warning(
                    "tearDown: node %s was left out of the cluster; "
                    "rebalancing it back in"
                    % self._node_pending_add_back.ip)
                task = self.task.async_rebalance(
                    self.cluster, to_add=[self._node_pending_add_back],
                    to_remove=[])
                self.task_manager.get_task_result(task)
        except Exception as e:
            self.log.warning("tearDown: node add-back failed: %s" % e)
        try:
            ok, detail = set_entitlement_subscriptions(
                self.ucp_client, self._original_subscriptions)
            if not ok:
                self.log.warning(
                    "tearDown: failed to restore the entitlement profile: %s"
                    % detail)
        except Exception as e:
            self.log.warning(
                "tearDown: failed to restore the entitlement profile: %s" % e)
        try:
            # Defensive: every trigger restores the interval itself, but a
            # test that failed mid-trigger would leave it at one second.
            set_lighthouse_interval_via_diag_eval(self.cluster.master, 2)
        except Exception as e:
            self.log.warning(
                "tearDown: failed to restore the reporting interval: %s" % e)
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(SkuUsageTests, self).tearDown()

    # ==================== Helpers ====================

    def _trigger_report_and_restore_interval(self, server, label=""):
        """
        Force an immediate telemetry report and put the interval back.

        Drops reporting_interval_hours to report_interval_seconds via
        diag/eval, waits report_sleep_seconds for the report to fire and the
        portal to process it, then restores the 2 h default. Nothing else --
        no portal assertions belong here.
        """
        interval_hours = float(self.report_interval_seconds) / 3600.0
        diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
            server,
            reporting_endpoint=self.portal_domain,
            reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
            reporting_interval_hours=interval_hours)
        self.assertTrue(
            diag_status,
            "%s: diag/eval for report trigger failed: %s"
            % (label, diag_content))
        self.sleep(self.report_sleep_seconds,
                   "waiting for report to fire (%s)" % label)
        restore_status, restore_content = \
            set_lighthouse_interval_via_diag_eval(server, 2)
        self.assertTrue(
            restore_status,
            "%s: failed to restore the reporting interval to 2 h: %s"
            % (label, restore_content))

    def _report_and_confirm_on_portal(self, cluster_uuid, label=""):
        """
        Force a report and return the cluster's node count as the portal sees
        it, failing the test if the cluster never appears.
        """
        self._trigger_report_and_restore_interval(self.cluster.master, label)
        appeared = wait_for_cluster_on_portal(
            self.ucp_client, cluster_uuid, timeout=60, poll_interval=5)
        self.assertTrue(
            appeared,
            "%s: cluster %s did not appear on the portal within 60 s after "
            "a forced report" % (label, cluster_uuid))
        portal_cluster = get_portal_cluster(self.ucp_client, cluster_uuid)
        self.assertIsNotNone(
            portal_cluster,
            "%s: cluster %s is listed but its record could not be fetched"
            % (label, cluster_uuid))
        nodes = portal_cluster.get('telemetry', {}).get('nodes', [])
        self.log.info(
            "%s: portal reports %d node(s) for cluster %s"
            % (label, len(nodes), cluster_uuid))
        return len(nodes)

    def _tag_cluster(self, cluster_uuid, classification):
        """
        Tag the cluster's classification, failing the test if it does not
        take.

        The cluster must already be known to the portal: tagging is a
        conditional PUT on an existing record, so calling this before the
        cluster's first telemetry report returns 404 "Cluster not found."
        Always force a report first.
        """
        ok, detail = tag_portal_cluster(
            self.ucp_client, cluster_uuid, classification=classification)
        self.assertTrue(
            ok, "Could not tag cluster %s as %s: %s"
            % (cluster_uuid, classification, detail))
        self.log.info(
            "Tagged cluster %s classification=%s"
            % (cluster_uuid, classification))

    def _store_sku(self):
        """
        Put exactly one SKU on the profile, replacing whatever was there.

        The upload is a full replace, so no clear step is needed (and none is
        possible -- see entitlement_helper_methods.clear_entitlement_
        subscriptions).

        Returns the subscription that was stored so a caller can assert
        against its declared limits.
        """
        start_at, end_at = active_window()
        subscription = build_subscription_payload(
            start_at=start_at, end_at=end_at,
            nodes=self.sku_node_limit,
            logical_cores=self.sku_logical_cores,
            ram_bytes=self.sku_ram_bytes,
            classification=self.sku_classification,
            support_level=self.sku_support_level)
        ok, detail = set_entitlement_subscriptions(
            self.ucp_client, [subscription])
        self.assertTrue(
            ok, "Could not store the SKU under test: %s" % detail)
        self.log.info(
            "Stored SKU: %s/%s, nodes=%d, per-node ceiling %d cores / %d "
            "bytes RAM, active %s -> %s"
            % (self.sku_classification, self.sku_support_level,
               self.sku_node_limit, self.sku_logical_cores,
               self.sku_ram_bytes, start_at, end_at))
        return subscription

    def _read_usage(self, label=""):
        """Read usage, fail if unreadable, and log the raw body."""
        usage = get_usage(self.ucp_client)
        self.assertIsNotNone(
            usage, "%s: GET /entitlements/usage could not be read" % label)
        self.log.info("%s: usage body = %s" % (label, describe_usage(usage)))
        return usage

    # ==================== Tests ====================

    def test_usage_without_matching_sku(self):
        """
        With no SKU covering it, a reporting cluster is visible to the portal
        but is counted against no entitled row.

        This is the control for the other two cases: it establishes what the
        usage body looks like before a covering SKU exists, so a later
        assertion that a SKU row appeared cannot be satisfied by a row that
        was there all along.

        "No SKU at all" is deliberately NOT how this is set up. Emptying the
        profile is impossible on current builds -- an empty, null, or absent
        subscriptions array is rejected with 400 "Failed to decode uploaded
        file." (confirmed live 2026-08-18), so once a profile exists there is
        no API route back to unconfigured. The reachable equivalent is a
        profile whose only SKU is for a different classification than the one
        this cluster is tagged with: no SKU can match, and the nodes land in
        the row with classification null and limit 0.
        """
        cluster_uuid = get_cb_cluster_uuid(self.cluster.master)
        self.assertIsNotNone(
            cluster_uuid, "Could not read the cluster UUID from /pools")

        start_at, end_at = active_window()
        decoy = build_subscription_payload(
            start_at=start_at, end_at=end_at,
            nodes=self.sku_node_limit,
            logical_cores=self.sku_logical_cores,
            ram_bytes=self.sku_ram_bytes,
            classification=self.nonmatching_classification,
            support_level=self.nonmatching_support_level)
        ok, detail = set_entitlement_subscriptions(self.ucp_client, [decoy])
        self.assertTrue(
            ok, "Could not store the non-matching profile: %s" % detail)
        profile = get_entitlement_profile(self.ucp_client)
        self.assertEqual(
            len(get_profile_subscriptions(profile)), 1,
            "Expected exactly one (non-matching) SKU on the profile, got: %s"
            % profile)

        # Report first: tagging is a PUT on an existing portal record, so a
        # cluster that has never reported cannot be tagged (404).
        node_count = self._report_and_confirm_on_portal(
            cluster_uuid, "no-matching-SKU")
        self.assertTrue(
            node_count > 0,
            "Portal reports 0 nodes for cluster %s, so there is nothing for "
            "usage to account for" % cluster_uuid)

        # Tag to a classification the profile has no SKU for. This must be
        # explicit: an UNTAGGED cluster defaults to the highest tier present
        # in the profile, which would match the decoy instead of missing it.
        self._tag_cluster(cluster_uuid, self.sku_classification)

        usage = self._read_usage("no-matching-SKU")
        rows = rows_containing_cluster(usage, cluster_uuid)
        self.assertTrue(
            rows,
            "Cluster %s is absent from usage entirely; an unmatched cluster "
            "must still be visible (unclassified), not dropped. Usage was: %s"
            % (cluster_uuid, describe_usage(usage)))
        entitled_rows = []
        for row in rows:
            entitled = usage_row_entitled(row)
            if entitled is not None and entitled > 0:
                entitled_rows.append(row)
        self.assertEqual(
            entitled_rows, [],
            "No SKU covers classification '%s', yet cluster %s is counted "
            "against %d entitled row(s): %s"
            % (self.sku_classification, cluster_uuid, len(entitled_rows),
               entitled_rows))
        self.log.info(
            "PASS -- with no covering SKU, cluster %s (%d node(s)) is visible "
            "in %d usage row(s) and counted against no entitled row"
            % (cluster_uuid, node_count, len(rows)))

    def test_usage_with_sku_configured(self):
        """
        With one SKU configured and the cluster tagged to its
        classification, usage reports that SKU's row with this cluster as a
        contributor and the SKU's declared node limit as the entitled count.
        """
        cluster_uuid = get_cb_cluster_uuid(self.cluster.master)
        self.assertIsNotNone(
            cluster_uuid, "Could not read the cluster UUID from /pools")

        self._store_sku()

        node_count = self._report_and_confirm_on_portal(
            cluster_uuid, "with-SKU")
        self.assertTrue(
            node_count > 0,
            "Portal reports 0 nodes for cluster %s, so no node can match the "
            "SKU" % cluster_uuid)
        self._tag_cluster(cluster_uuid, self.sku_classification)

        usage = self._read_usage("with-SKU")
        row = find_usage_row(
            usage, classification=self.sku_classification,
            support_level=self.sku_support_level)
        self.assertIsNotNone(
            row,
            "Usage carries no %s/%s row after storing that SKU. Usage was: %s"
            % (self.sku_classification, self.sku_support_level,
               describe_usage(usage)))

        contributors = usage_row_cluster_uuids(row)
        self.assertIn(
            cluster_uuid, contributors,
            "Cluster %s is not listed as a contributor to the %s/%s row; "
            "contributors were %s. Row: %s"
            % (cluster_uuid, self.sku_classification, self.sku_support_level,
               contributors, row))

        entitled = usage_row_entitled(row)
        self.assertEqual(
            entitled, self.sku_node_limit,
            "The %s/%s row reports %s entitled node(s), expected the SKU's "
            "declared limit of %d. Row: %s"
            % (self.sku_classification, self.sku_support_level, entitled,
               self.sku_node_limit, row))

        # Assert on THIS cluster's contribution, not the row's `used`: that
        # total also counts every other cluster in the fleet.
        ours = usage_nodes_for_cluster(usage, cluster_uuid, row=row)
        self.assertEqual(
            ours, node_count,
            "The %s/%s row credits this cluster with %d node(s), but the "
            "portal reports it has %d. Row: %s"
            % (self.sku_classification, self.sku_support_level, ours,
               node_count, row))
        self.log.info(
            "PASS -- cluster %s contributes %d of the %s row's %s consumed "
            "node(s), entitled=%d"
            % (cluster_uuid, ours, self.sku_classification,
               usage_row_used(row), entitled))

    def test_usage_decreases_when_node_removed(self):
        """
        Rebalancing a node out of the cluster lowers the SKU's consumed count
        by exactly one; rebalancing it back restores it.

        Both directions are asserted in one test on purpose: a drop that
        never recovers would leave the cluster short a node for every later
        run, so the add-back is part of the scenario rather than cleanup, and
        asserting on it is free once it has to happen anyway.

        The consumed count is asserted as a delta, never as an absolute: the
        row also counts nodes from clusters this test does not own.

        Requires nodes_init >= 3 so a non-master node can leave while the
        cluster stays viable.
        """
        cluster_uuid = get_cb_cluster_uuid(self.cluster.master)
        self.assertIsNotNone(
            cluster_uuid, "Could not read the cluster UUID from /pools")

        self._store_sku()

        baseline_nodes = self._report_and_confirm_on_portal(
            cluster_uuid, "before-removal")
        self.assertTrue(
            baseline_nodes >= 2,
            "Portal reports %d node(s) for cluster %s; this test needs at "
            "least 2 so one can be removed (conf requires nodes_init >= 3)"
            % (baseline_nodes, cluster_uuid))
        self._tag_cluster(cluster_uuid, self.sku_classification)

        usage = self._read_usage("before-removal")
        row = find_usage_row(
            usage, classification=self.sku_classification,
            support_level=self.sku_support_level)
        self.assertIsNotNone(
            row,
            "Usage carries no %s/%s row before the removal, so there is no "
            "baseline to compare against. Usage was: %s"
            % (self.sku_classification, self.sku_support_level,
               describe_usage(usage)))
        ours_before = usage_nodes_for_cluster(usage, cluster_uuid, row=row)
        self.assertEqual(
            ours_before, baseline_nodes,
            "The %s/%s row credits this cluster with %d node(s) before the "
            "removal, but the portal reports it has %d. Row: %s"
            % (self.sku_classification, self.sku_support_level, ours_before,
               baseline_nodes, row))
        self.log.info(
            "Baseline: this cluster contributes %d node(s); the row's "
            "fleet-wide consumed count is %s (not asserted on -- other "
            "clusters move it)"
            % (ours_before, usage_row_used(row)))

        non_master_nodes = [n for n in self.cluster.nodes_in_cluster
                            if n.ip != self.cluster.master.ip]
        self.assertTrue(
            non_master_nodes,
            "Cluster has no non-master node to remove; conf requires "
            "nodes_init >= 3 for this test")
        node_to_remove = non_master_nodes[-1]
        self.log.info("Rebalancing out node %s" % node_to_remove.ip)

        rebalance_task = self.task.async_rebalance(
            self.cluster, to_add=[], to_remove=[node_to_remove])
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(
            rebalance_task.result,
            "Rebalance-out of %s failed" % node_to_remove.ip)
        # From here the cluster is short a node until it is added back;
        # tearDown restores it if this test does not get that far.
        self._node_pending_add_back = node_to_remove

        self._trigger_report_and_restore_interval(
            self.cluster.master, "after-removal")
        reflected = wait_for_portal_node_count(
            self.ucp_client, cluster_uuid, baseline_nodes - 1,
            timeout=self.portal_poll_timeout, poll_interval=5)
        self.assertTrue(
            reflected,
            "Portal did not drop to %d node(s) for cluster %s within %d s of "
            "the rebalance-out" % (baseline_nodes - 1, cluster_uuid,
                                   self.portal_poll_timeout))

        usage = self._read_usage("after-removal")
        row = find_usage_row(
            usage, classification=self.sku_classification,
            support_level=self.sku_support_level)
        self.assertIsNotNone(
            row,
            "The %s/%s row disappeared from usage after the rebalance-out. "
            "Usage was: %s"
            % (self.sku_classification, self.sku_support_level,
               describe_usage(usage)))
        ours_after = usage_nodes_for_cluster(usage, cluster_uuid, row=row)
        self.assertEqual(
            ours_after, ours_before - 1,
            "After removing one node the %s/%s row credits this cluster with "
            "%d node(s), expected %d (one fewer than the baseline %d). Row: %s"
            % (self.sku_classification, self.sku_support_level, ours_after,
               ours_before - 1, ours_before, row))
        self.log.info(
            "This cluster's contribution dropped from %d to %d after the "
            "rebalance-out" % (ours_before, ours_after))

        self.log.info("Rebalancing node %s back in" % node_to_remove.ip)
        rebalance_task = self.task.async_rebalance(
            self.cluster, to_add=[node_to_remove], to_remove=[])
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(
            rebalance_task.result,
            "Rebalance-in of %s failed" % node_to_remove.ip)
        self._node_pending_add_back = None

        self._trigger_report_and_restore_interval(
            self.cluster.master, "after-add-back")
        reflected = wait_for_portal_node_count(
            self.ucp_client, cluster_uuid, baseline_nodes,
            timeout=self.portal_poll_timeout, poll_interval=5)
        self.assertTrue(
            reflected,
            "Portal did not return to %d node(s) for cluster %s within %d s "
            "of the rebalance-in" % (baseline_nodes, cluster_uuid,
                                     self.portal_poll_timeout))

        usage = self._read_usage("after-add-back")
        row = find_usage_row(
            usage, classification=self.sku_classification,
            support_level=self.sku_support_level)
        self.assertIsNotNone(
            row,
            "The %s/%s row disappeared from usage after the rebalance-in. "
            "Usage was: %s"
            % (self.sku_classification, self.sku_support_level,
               describe_usage(usage)))
        ours_restored = usage_nodes_for_cluster(usage, cluster_uuid, row=row)
        self.assertEqual(
            ours_restored, ours_before,
            "After adding the node back the %s/%s row credits this cluster "
            "with %d node(s), expected the baseline %d. Row: %s"
            % (self.sku_classification, self.sku_support_level, ours_restored,
               ours_before, row))
        self.log.info(
            "PASS -- this cluster's contribution tracked the topology in both "
            "directions: %d -> %d -> %d"
            % (ours_before, ours_after, ours_restored))
