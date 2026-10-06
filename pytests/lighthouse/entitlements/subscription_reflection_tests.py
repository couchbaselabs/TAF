# -*- coding: utf-8 -*-
"""
UCP Portal subscription-reflection tests -- an uploaded subscription file is
reflected consistently on every read surface.

supportLevel (platinum/gold/silver) and the DEFAULT classification of an
untagged cluster (the highest active environment in the subscription file:
production > backup > development > test) are derived from the uploaded
subscription file and computed on every read. So the FIRST read after an
upload must already show the new values -- there is deliberately no wait or
poll between an upload and the assertions here.

Each case checks the same six surfaces and requires them to agree:
    (1) cluster list          GET /clusters
    (2) cluster detail        GET /clusters/{uuid}
    (3) cluster filters       GET /clusters?supportLevel= / ?classification=
    (4) entitlement usage     GET /entitlements/usage
    (5) JSON usage report     GET /reports/usage?format=json
    (6) overview chart        GET /overview/breakdown (latest bucket)

Cases (happy path only):
    - a subscription is reflected on every surface (data-driven: one conf
      line per classification/supportLevel pair)
    - a re-upload re-resolves every cluster (gold -> platinum)
    - a manual classification tag overrides the subscription default, and
      clearing it falls back to the default again
    - an expired subscription leaves untagged clusters unclassified and
      unmatched

The clusters under test are SYNTHETIC (pushed through the unauthenticated
ingest endpoint) rather than the real CB cluster: every node is exactly
node_logical_cores / node_ram_bytes and the SKU is sized to the same values,
so instance-size matching can never be why a case fails (this also keeps the
suite clear of the oversized-cluster bug CFM-377). Their UUIDs are stable conf
params because the portal has no cluster DELETE -- re-runs refresh the same
two clusters instead of leaking new ones. setUp re-ingests them (fresh
collectedAt) and clears any tag a failed previous run may have left.

The lab portal is a shared fleet, so every assertion is scoped to OUR cluster
UUIDs: per-cluster node contributions, never a row's fleet-wide `used`. The
SKU node limit defaults well above 10 for the same reason -- other small
untagged clusters on the lab fleet land in the same row, and the report's
`within` status must not flip because of them. The overview is a per-day
fleet-wide count, so it is checked as ">= our nodes" and asserted last, so
that an overview-only mismatch is reported without hiding the other surfaces.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.collector_helper_methods import (
    get_portal_cluster,
    wait_for_portal_node_count,
)
from lighthouse.entitlement_helper_methods import (
    active_window,
    clear_portal_cluster_classification,
    clusters_by_uuid,
    describe_usage,
    expired_window,
    find_unmatched_row_for_cluster,
    find_usage_row,
    get_entitlement_profile,
    get_overview_breakdown,
    get_profile_subscriptions,
    get_usage,
    get_usage_report_json,
    latest_overview_groups,
    list_portal_clusters,
    set_entitlement_subscriptions,
    tag_portal_cluster,
    usage_nodes_for_cluster,
)
from lighthouse.ucp_helper_methods import (
    build_subscription_payload,
    build_synthetic_cluster_payload,
    create_session,
    get_session_cookie,
)
from unified_control_plane.constants import (
    CLASSIFICATION_BACKUP,
    CLASSIFICATION_DEVELOPMENT,
    CLASSIFICATION_PRODUCTION,
    CLASSIFICATION_TEST,
    REPORT_STATUS_UNMATCHED,
    REPORT_STATUS_WITHIN,
    SUPPORT_LEVEL_GOLD,
    SUPPORT_LEVEL_NO_MATCHING_ENTITLEMENT,
    SUPPORT_LEVEL_PLATINUM,
    SUPPORT_LEVEL_SILVER,
)

_ALL_CLASSIFICATIONS = (CLASSIFICATION_PRODUCTION, CLASSIFICATION_BACKUP,
                        CLASSIFICATION_DEVELOPMENT, CLASSIFICATION_TEST)
_ALL_SUPPORT_LEVELS = (SUPPORT_LEVEL_PLATINUM, SUPPORT_LEVEL_GOLD,
                       SUPPORT_LEVEL_SILVER)


class SubscriptionReflectionTests(LighthouseBase):

    def setUp(self):
        super(SubscriptionReflectionTests, self).setUp()
        # Stable synthetic clusters -- see the module docstring.
        self.cluster_uuids = [
            self.input.param("synthetic_cluster_uuid_1",
                             "5f7c1a30-0000-4000-8000-200000000001"),
            self.input.param("synthetic_cluster_uuid_2",
                             "5f7c1a30-0000-4000-8000-200000000002"),
        ]
        self.nodes_per_cluster = self.input.param("nodes_per_cluster", 2)
        # Per-node size of every synthetic node AND of the SKU, so every
        # node fits the SKU exactly.
        self.node_logical_cores = self.input.param("node_logical_cores", 4)
        self.node_ram_bytes = self.input.param("node_ram_bytes", 8589934592)
        self.sku_node_limit = self.input.param("sku_node_limit", 1000)
        self.portal_poll_timeout = self.input.param("portal_poll_timeout", 60)
        # Data-driven case
        self.sub_classification = self.input.param(
            "sub_classification", CLASSIFICATION_PRODUCTION)
        self.sub_support_level = self.input.param(
            "sub_support_level", SUPPORT_LEVEL_GOLD)
        # Re-upload case
        self.initial_support_level = self.input.param(
            "initial_support_level", SUPPORT_LEVEL_GOLD)
        self.reupload_support_level = self.input.param(
            "reupload_support_level", SUPPORT_LEVEL_PLATINUM)
        # Manual-tag case: a classification the subscription does NOT cover
        self.tag_classification = self.input.param(
            "tag_classification", CLASSIFICATION_TEST)

        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)

        # The entitlement profile is a global singleton -- restore in tearDown.
        profile = get_entitlement_profile(self.ucp_client)
        self.assertIsNotNone(
            profile, "Could not read the baseline entitlement profile")
        self._original_subscriptions = get_profile_subscriptions(profile)
        self.log.info("Baseline entitlement profile holds %d subscription(s)"
                      % len(self._original_subscriptions))

        for index, cluster_uuid in enumerate(self.cluster_uuids):
            payload = build_synthetic_cluster_payload(
                cluster_uuid, self.nodes_per_cluster,
                self.node_logical_cores, self.node_ram_bytes,
                hostname_prefix="sub-reflect-%d" % (index + 1))
            status, content, _ = self.ucp_client.ingest_telemetry(payload)
            self.assertTrue(
                status, "Ingest for synthetic cluster %s failed: %s"
                % (cluster_uuid, content))
            self.assertTrue(
                wait_for_portal_node_count(
                    self.ucp_client, cluster_uuid, self.nodes_per_cluster,
                    timeout=self.portal_poll_timeout, poll_interval=5),
                "Synthetic cluster %s did not show %d node(s) on the portal "
                "within %d s" % (cluster_uuid, self.nodes_per_cluster,
                                 self.portal_poll_timeout))
            # Every case starts from untagged clusters.
            ok, detail = clear_portal_cluster_classification(
                self.ucp_client, cluster_uuid)
            self.assertTrue(ok, "Could not untag cluster %s: %s"
                            % (cluster_uuid, detail))

    def tearDown(self):
        for cluster_uuid in self.cluster_uuids:
            try:
                ok, detail = clear_portal_cluster_classification(
                    self.ucp_client, cluster_uuid)
                if not ok:
                    self.log.warning("tearDown: untag of %s failed: %s"
                                     % (cluster_uuid, detail))
            except Exception as e:
                self.log.warning("tearDown: untag of %s failed: %s"
                                 % (cluster_uuid, e))
        try:
            if self._original_subscriptions:
                ok, detail = set_entitlement_subscriptions(
                    self.ucp_client, self._original_subscriptions)
                if not ok:
                    self.log.warning(
                        "tearDown: failed to restore the entitlement "
                        "profile: %s" % detail)
            else:
                self.log.warning(
                    "tearDown: baseline profile was empty and cannot be "
                    "restored (an empty upload is rejected)")
        except Exception as e:
            self.log.warning(
                "tearDown: failed to restore the entitlement profile: %s" % e)
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(SubscriptionReflectionTests, self).tearDown()

    # ==================== Helpers ====================

    def _upload_subscription(self, classification, support_level,
                             window=None):
        """Replace the profile with one SKU sized to the synthetic nodes."""
        start_at, end_at = window or active_window()
        subscription = build_subscription_payload(
            start_at=start_at, end_at=end_at,
            nodes=self.sku_node_limit,
            logical_cores=self.node_logical_cores,
            ram_bytes=self.node_ram_bytes,
            classification=classification,
            support_level=support_level)
        ok, detail = set_entitlement_subscriptions(
            self.ucp_client, [subscription])
        self.assertTrue(ok, "Upload of %s/%s failed: %s"
                        % (classification, support_level, detail))
        self.log.info("Uploaded subscription %s/%s, active %s -> %s"
                      % (classification, support_level, start_at, end_at))

    def _assert_resolved_everywhere(self, label, expected):
        """
        Assert every surface resolves each cluster as expected.

        Args:
            label:    step name for failure messages
            expected: dict uuid -> (classification or None, supportLevel).
                      supportLevel no_matching_entitlement means the
                      cluster's nodes belong in the usage "unmatched" row.
        """
        uuids = list(expected.keys())

        # (1) list and (2) detail
        clusters = list_portal_clusters(self.ucp_client)
        self.assertIsNotNone(clusters, "%s: GET /clusters failed" % label)
        listed = clusters_by_uuid(clusters, uuids)
        for uuid, want in expected.items():
            self.assertIn(uuid, listed,
                          "%s: cluster %s missing from GET /clusters"
                          % (label, uuid))
            detail = get_portal_cluster(self.ucp_client, uuid)
            self.assertIsNotNone(
                detail, "%s: GET /clusters/%s failed" % (label, uuid))
            for surface, record in (("list", listed[uuid]),
                                    ("detail", detail)):
                actual = (record.get('classification'),
                          record.get('supportLevel'))
                self.assertEqual(
                    actual, want,
                    "%s: %s shows cluster %s as (classification, "
                    "supportLevel)=%s, expected %s"
                    % (label, surface, uuid, actual, want))

        # (3) filters -- each filter returns exactly the clusters expected
        # at that value, so both inclusion and exclusion are checked.
        for level in _ALL_SUPPORT_LEVELS:
            filtered = list_portal_clusters(self.ucp_client,
                                            support_level=level)
            self.assertIsNotNone(
                filtered, "%s: ?supportLevel=%s failed" % (label, level))
            self.assertEqual(
                sorted(clusters_by_uuid(filtered, uuids).keys()),
                sorted(u for u, w in expected.items() if w[1] == level),
                "%s: ?supportLevel=%s returned the wrong test clusters"
                % (label, level))
        for classification in _ALL_CLASSIFICATIONS:
            filtered = list_portal_clusters(self.ucp_client,
                                            classification=classification)
            self.assertIsNotNone(
                filtered, "%s: ?classification=%s failed"
                % (label, classification))
            self.assertEqual(
                sorted(clusters_by_uuid(filtered, uuids).keys()),
                sorted(u for u, w in expected.items()
                       if w[0] == classification),
                "%s: ?classification=%s returned the wrong test clusters"
                % (label, classification))

        # (4) usage and (5) JSON report -- same rows, report adds status
        for surface, body in (("usage", get_usage(self.ucp_client)),
                              ("report",
                               get_usage_report_json(self.ucp_client))):
            self.assertIsNotNone(body, "%s: %s could not be read"
                                 % (label, surface))
            self.log.info("%s: %s body = %s"
                          % (label, surface, describe_usage(body)))
            for uuid, (classification, level) in expected.items():
                if level == SUPPORT_LEVEL_NO_MATCHING_ENTITLEMENT:
                    row = find_unmatched_row_for_cluster(body, uuid)
                    want_status = REPORT_STATUS_UNMATCHED
                else:
                    row = find_usage_row(
                        body, classification=classification,
                        support_level=level,
                        logical_cores=self.node_logical_cores,
                        ram_bytes=self.node_ram_bytes)
                    want_status = REPORT_STATUS_WITHIN
                self.assertIsNotNone(
                    row, "%s: %s has no row for cluster %s at %s/%s"
                    % (label, surface, uuid, classification, level))
                self.assertEqual(
                    usage_nodes_for_cluster(body, uuid, row=row),
                    self.nodes_per_cluster,
                    "%s: %s row credits cluster %s with the wrong node "
                    "count. Row: %s" % (label, surface, uuid, row))
                self.assertEqual(
                    usage_nodes_for_cluster(body, uuid),
                    self.nodes_per_cluster,
                    "%s: %s counts cluster %s's nodes in more than one row"
                    % (label, surface, uuid))
                if surface == "report":
                    self.assertEqual(
                        row.get('status'), want_status,
                        "%s: report row status for cluster %s is %s, "
                        "expected %s. Row: %s"
                        % (label, uuid, row.get('status'), want_status, row))

        # (6) overview -- last, so a mismatch here never hides the above
        groups = latest_overview_groups(
            get_overview_breakdown(self.ucp_client))
        ours = {}
        for classification, _ in expected.values():
            if classification:
                ours[classification] = \
                    ours.get(classification, 0) + self.nodes_per_cluster
        for environment, count in ours.items():
            self.assertTrue(
                groups.get(environment, 0) >= count,
                "%s: overview mismatch -- latest bucket counts %s node(s) "
                "under '%s', expected at least our %d. Groups: %s"
                % (label, groups.get(environment), environment, count,
                   groups))
        self.log.info("%s: all surfaces agree: %s" % (label, expected))

    # ==================== Tests ====================

    def test_subscription_reflected_on_all_surfaces(self):
        """
        Upload one active subscription; both untagged clusters resolve to its
        classification and supportLevel on every surface on the first read.
        """
        self._upload_subscription(self.sub_classification,
                                  self.sub_support_level)
        self._assert_resolved_everywhere(
            "%s/%s" % (self.sub_classification, self.sub_support_level),
            dict((uuid, (self.sub_classification, self.sub_support_level))
                 for uuid in self.cluster_uuids))
        self.log.info("PASS -- %s/%s reflected on every surface"
                      % (self.sub_classification, self.sub_support_level))

    def test_reupload_re_resolves_all_clusters(self):
        """
        Re-uploading with a different supportLevel re-resolves ALL clusters;
        classification is unchanged.
        """
        for level in (self.initial_support_level,
                      self.reupload_support_level):
            self._upload_subscription(CLASSIFICATION_PRODUCTION, level)
            self._assert_resolved_everywhere(
                "upload %s" % level,
                dict((uuid, (CLASSIFICATION_PRODUCTION, level))
                     for uuid in self.cluster_uuids))
        self.log.info("PASS -- re-upload moved every cluster %s -> %s"
                      % (self.initial_support_level,
                         self.reupload_support_level))

    def test_manual_tag_overrides_subscription_default(self):
        """
        A user tag beats the subscription default: the tagged cluster takes
        the tag (and, with no subscription for it, no_matching_entitlement),
        the untagged one is unchanged. Clearing the tag restores the default.
        """
        default = (CLASSIFICATION_DEVELOPMENT, SUPPORT_LEVEL_SILVER)
        tagged_uuid, other_uuid = self.cluster_uuids
        self._upload_subscription(*default)
        self._assert_resolved_everywhere(
            "before tag", {tagged_uuid: default, other_uuid: default})

        ok, detail = tag_portal_cluster(
            self.ucp_client, tagged_uuid,
            classification=self.tag_classification)
        self.assertTrue(ok, "Could not tag cluster %s as %s: %s"
                        % (tagged_uuid, self.tag_classification, detail))
        self._assert_resolved_everywhere(
            "tagged %s" % self.tag_classification,
            {tagged_uuid: (self.tag_classification,
                           SUPPORT_LEVEL_NO_MATCHING_ENTITLEMENT),
             other_uuid: default})

        ok, detail = clear_portal_cluster_classification(
            self.ucp_client, tagged_uuid)
        self.assertTrue(ok, "Could not clear the tag on %s: %s"
                        % (tagged_uuid, detail))
        self._assert_resolved_everywhere(
            "tag cleared", {tagged_uuid: default, other_uuid: default})
        self.log.info("PASS -- tag overrode the default and clearing it "
                      "restored %s/%s" % default)

    def test_expired_subscription_leaves_clusters_unmatched(self):
        """
        With only an expired subscription, untagged clusters have no
        classification (field omitted) and resolve to
        no_matching_entitlement; their nodes are in the unmatched row.
        """
        self._upload_subscription(CLASSIFICATION_PRODUCTION,
                                  SUPPORT_LEVEL_GOLD,
                                  window=expired_window())
        self._assert_resolved_everywhere(
            "expired",
            dict((uuid, (None, SUPPORT_LEVEL_NO_MATCHING_ENTITLEMENT))
                 for uuid in self.cluster_uuids))
        self.log.info("PASS -- expired subscription left both clusters "
                      "unmatched")
