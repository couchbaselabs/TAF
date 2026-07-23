# -*- coding: utf-8 -*-
"""
UCP Portal Entitlement API tests -- entitlement profile storage and
subscription validation (Architecture Spec sections 10.4, 12.2, 16.5).

    - Unconfigured entitlement profile still returns an ETag (GET must
      never omit the ETag header, even with an empty subscriptions list --
      an admin needs it to make the first PUT).
    - A valid subscription is stored and returned correctly (full
      round-trip of startAt/endAt/classification/supportLevel/limits).
    - startAt > endAt is rejected as a validation error (422 per the
      error model in section 12.5 / 16.5 -- "Request validation failure
      returns 422").

Only system_admin may update entitlements (section 12.3), so setUp opens
an admin session. Every test snapshots the current profile + ETag and
tearDown restores it -- entitlements is a single global profile
(`entitlements::default`, not per-cluster), so it must be left exactly as
found for the next test/run.

Response parsing lives on UCPResponse (.etag/.json); the subscription
payload shape lives in ucp_helper_methods.build_subscription_payload --
this class holds test methods only.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_session_cookie,
    build_subscription_payload,
)


class EntitlementTests(LighthouseBase):

    def setUp(self):
        super(EntitlementTests, self).setUp()
        self.expected_validation_status = self.input.param(
            "expected_validation_status", 422)
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)

        status, content, header = self.ucp_client.get_entitlements()
        baseline = UCPResponse(status, content, header)
        self.assertTrue(
            status, "Could not fetch baseline entitlements: %s" % content)
        self._original_etag = baseline.etag
        self._original_subscriptions = (
            baseline.json.get('subscriptions', []) if baseline.json else [])

    def tearDown(self):
        try:
            status, content, header = self.ucp_client.get_entitlements()
            current = UCPResponse(status, content, header)
            if status and current.etag:
                self.ucp_client.update_entitlements(
                    etag=current.etag,
                    subscriptions=self._original_subscriptions)
        except Exception as e:
            self.log.warning(
                "tearDown: failed to restore entitlements: %s" % e)
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(EntitlementTests, self).tearDown()

    def test_unconfigured_entitlement_returns_etag(self):
        """
        Driving the profile to an explicitly unconfigured (empty
        subscriptions) state must still return a usable ETag on GET.
        """
        status, content, header = self.ucp_client.update_entitlements(
            etag=self._original_etag, subscriptions=[])
        cleared = UCPResponse(status, content, header)
        self.assertTrue(
            status, "Clearing subscriptions failed: %s" % content)

        status, content, header = self.ucp_client.get_entitlements()
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "GET /entitlements failed on an unconfigured profile: "
            "%s" % content)
        self.assertEqual(
            response.json.get('subscriptions') if response.json else None,
            [], "Expected an empty subscriptions list on an unconfigured "
            "profile, got: %s" % content)
        self.assertIsNotNone(
            response.etag,
            "Unconfigured entitlement profile returned no ETag")
        self.log.info(
            "PASS -- unconfigured entitlement profile returned ETag %s"
            % response.etag)

    def test_valid_subscription_stored_and_returned(self):
        """
        A well-formed subscription is stored via PUT and every field comes
        back unchanged on the next GET.
        """
        subscription = build_subscription_payload(
            start_at='2026-01-01T00:00:00Z',
            end_at='2026-12-31T23:59:59Z',
            nodes=100, logical_cores=400, ram_bytes=10995116277760,
            classification='production', support_level='platinum')

        status, content, header = self.ucp_client.update_entitlements(
            etag=self._original_etag, subscriptions=[subscription])
        put_response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "Storing a valid subscription failed: %s" % content)

        status, content, header = self.ucp_client.get_entitlements()
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "GET /entitlements failed after storing a subscription: "
            "%s" % content)
        subscriptions = response.json.get('subscriptions') if response.json \
            else None
        self.assertTrue(
            subscriptions, "Expected a stored subscription, got: %s"
            % content)
        stored = subscriptions[0]
        for field in ('startAt', 'endAt', 'classification', 'supportLevel'):
            self.assertEqual(
                stored.get(field), subscription[field],
                "Field '%s' mismatch: expected %s, got %s"
                % (field, subscription[field], stored.get(field)))
        self.assertEqual(
            stored.get('limits'), subscription['limits'],
            "limits mismatch: expected %s, got %s"
            % (subscription['limits'], stored.get('limits')))
        self.log.info(
            "PASS -- subscription stored and returned correctly: %s"
            % stored)

    def test_start_at_after_end_at_returns_validation_error(self):
        """
        A subscription with startAt after endAt must be rejected as a
        validation error (422) rather than silently accepted.
        """
        subscription = build_subscription_payload(
            start_at='2026-12-31T23:59:59Z',
            end_at='2026-01-01T00:00:00Z',
            nodes=100, logical_cores=400, ram_bytes=10995116277760)

        status, content, header = self.ucp_client.update_entitlements(
            etag=self._original_etag, subscriptions=[subscription])
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "startAt > endAt: expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "startAt > endAt: expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code,
               content))
        self.log.info(
            "PASS -- startAt > endAt rejected with %s"
            % self.expected_validation_status)
