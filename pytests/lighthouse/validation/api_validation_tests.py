# -*- coding: utf-8 -*-
"""
UCP Portal API Validation Tests -- list-endpoint pagination/query handling
plus ingest-payload input-validation and security cases.

List endpoints (cases 97, 98, 86):
    - 97: offset=-1                 -> rejected (422 validation_error)
    - 98: offset beyond total       -> 200 with empty items and correct total
    - 86: unknown query parameter   -> rejected (422 validation_error)
    - negative limit                -> rejected (422 validation_error)
    - non-integer limit             -> rejected (422 validation_error)

Ingest payload (POST /api/v1/ingest/telemetry, requires a session):
    - unknown fields (__proto__, adminKey)    -> accepted by design (2xx); the
                                                 portal allows unknown request-
                                                 body fields for Sync Gateway
                                                 forward-compat (AV-132481)
    - oversized payload                        -> rejected cleanly with a 4xx,
                                                 service stays up (no crash)
    - arbitrary clusterUuid                    -> accepted by design (2xx)

The portal answers 422 Unprocessable Entity for validation errors (a
well-formed request carrying a semantically invalid value). The
expected status is a conf param (expected_validation_status, default 422) so
it is a one-line change if the product ever switches to 400.

List cases target GET /api/v1/users by default (admin-accessible and
deterministic -- the bootstrap admin always exists, so total >= 1). The
endpoint is a conf param (list_path) so the same logic can be pointed at
/clusters or /audit. Inherits LighthouseBase for portal wiring; setUp opens
an admin session (ingest requires one).

Response parsing lives on UCPResponse (.items/.total); the raw GET for the
unknown-parameter case lives in ucp_helper_methods.get_raw, and ingest
payloads are built by ucp_helper_methods.build_minimal_ingest_payload --
this class holds test methods only.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_session_cookie,
    get_raw,
    build_minimal_ingest_payload,
)


class ApiValidationTests(LighthouseBase):

    def setUp(self):
        super(ApiValidationTests, self).setUp()
        self.list_path = self.input.param("list_path", "api/v1/users")
        # Validation errors come back as 422 Unprocessable Entity, not 400.
        self.expected_validation_status = self.input.param(
            "expected_validation_status", 422)
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)

    def tearDown(self):
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: logout failed: %s" % e)
        super(ApiValidationTests, self).tearDown()

    def test_negative_offset_rejected(self):
        """Case 97: offset=-1 on a list endpoint -> 422 validation_error."""
        status, content, header = self.ucp_client.list_users(offset=-1)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "offset=-1: expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "offset=-1: expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- offset=-1 rejected with %s"
                      % self.expected_validation_status)

    def test_negative_limit_rejected(self):
        """A negative limit on a list endpoint -> 422 validation_error."""
        status, content, header = self.ucp_client.list_users(limit=-1)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "limit=-1: expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "limit=-1: expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- limit=-1 rejected with %s"
                      % self.expected_validation_status)

    def test_non_integer_limit_rejected(self):
        """A non-integer limit value -> 422 validation_error."""
        status, content, header = self.ucp_client.list_users(limit='abc')
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "limit='abc': expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "limit='abc': expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- non-integer limit rejected with %s"
                      % self.expected_validation_status)

    def test_unknown_query_param_rejected(self):
        """Case 86: an unknown query parameter -> 422 validation_error."""
        status, content, header = get_raw(
            self.ucp_client, self.list_path, "unexpectedParam=1")
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "unknown query param: expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "unknown query param: expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- unknown query param rejected with %s"
                      % self.expected_validation_status)

    def test_offset_beyond_total_returns_empty(self):
        """
        Case 98: an offset past the end returns 200 with an empty item list
        and the correct (unchanged) total.
        """
        status, content, header = self.ucp_client.list_users()
        base = UCPResponse(status, content, header)
        self.assertTrue(status, "Baseline list failed: %s" % content)
        self.assertIsNotNone(
            base.total,
            "List response has no recognizable total field; body=%s" % content)

        beyond = base.total + 50
        status, content, header = self.ucp_client.list_users(offset=beyond)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "List with large offset failed (HTTP %s): %s"
            % (response.status_code, content))
        self.assertEqual(
            len(response.items), 0,
            "offset=%s beyond total=%s should return no items, got %d"
            % (beyond, base.total, len(response.items)))
        self.assertEqual(
            response.total, base.total,
            "Total changed with a large offset: baseline=%s, got=%s"
            % (base.total, response.total))
        self.log.info(
            "PASS -- offset=%s returned 0 items, total stayed %s"
            % (beyond, response.total))

    # ==================== Ingest payload validation / security ============

    # A valid-format cluster UUID for the baseline payload; the specific
    # value does not matter for the validation cases below.
    _BASELINE_UUID = '11111111-1111-1111-1111-111111111111'

    def test_ingest_proto_pollution_field_accepted(self):
        """
        A __proto__ key in the ingest body is accepted (2xx). The portal
        deliberately allows unknown request-body fields for Sync Gateway
        forward-compatibility (AV-132481, closed As Designed), and __proto__
        carries no prototype-pollution risk on the non-JS backend.
        """
        payload = build_minimal_ingest_payload(self._BASELINE_UUID)
        payload['__proto__'] = {'polluted': True}
        status, content, header = self.ucp_client.ingest_telemetry(payload)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "__proto__ field should be accepted (unknown fields "
            "allowed by design, AV-132481), got HTTP %s: %s"
            % (response.status_code, content))
        self.log.info("PASS -- __proto__ field accepted (HTTP %s)"
                      % response.status_code)

    def test_ingest_admin_like_field_accepted(self):
        """
        An admin-like field (adminKey) in the ingest body is accepted (2xx)
        and simply ignored. Unknown request-body fields are allowed by design
        (AV-132481, closed As Designed); adminKey has no special meaning to the
        backend, so it cannot cause privilege escalation.
        """
        payload = build_minimal_ingest_payload(self._BASELINE_UUID)
        payload['adminKey'] = 'granted'
        status, content, header = self.ucp_client.ingest_telemetry(payload)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "adminKey field should be accepted and ignored (unknown "
            "fields allowed by design, AV-132481), got HTTP %s: %s"
            % (response.status_code, content))
        self.log.info("PASS -- adminKey field accepted and ignored (HTTP %s)"
                      % response.status_code)

    def test_ingest_oversized_payload_rejected_cleanly(self):
        """
        An oversized ingest payload is rejected with a 4xx client error (not
        a 5xx / crash), and the ingest service stays up afterwards.
        """
        payload = build_minimal_ingest_payload(self._BASELINE_UUID)
        # Bloat the payload well past any reasonable body limit (~2 MB).
        payload['nodes'][0]['hostname'] = 'a' * (2 * 1024 * 1024)
        status, content, header = self.ucp_client.ingest_telemetry(payload)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "oversized payload: expected rejection but ingest "
            "succeeded")
        self.assertTrue(
            400 <= response.status_code < 500,
            "oversized payload: expected a 4xx client error (clean rejection),"
            " got %s: %s" % (response.status_code, content))

        # Service must still be alive after rejecting the oversized body.
        health_status, health_content, _ = self.ucp_client.ingest_health()
        self.assertTrue(
            health_status,
            "ingest service did not stay healthy after an oversized payload: "
            "%s" % health_content)
        self.log.info(
            "PASS -- oversized payload rejected with %s, service healthy"
            % response.status_code)

    def test_ingest_rogue_cluster_uuid_accepted(self):
        """
        A client may send telemetry for an arbitrary clusterUuid -- this is
        accepted by design (2xx).
        """
        rogue_uuid = 'deadbeef-0000-0000-0000-000000000000'
        payload = build_minimal_ingest_payload(rogue_uuid)
        status, content, header = self.ucp_client.ingest_telemetry(payload)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "arbitrary clusterUuid should be accepted by design, "
            "got HTTP %s: %s" % (response.status_code, content))
        self.log.info(
            "PASS -- telemetry for arbitrary clusterUuid accepted (HTTP %s)"
            % response.status_code)

    def test_ingest_malformed_cluster_uuid_rejected(self):
        """
        A clusterUuid that is not a valid UUID is rejected as a validation
        error.  An arbitrary *well-formed* UUID is accepted by design (see
        test_ingest_rogue_cluster_uuid_accepted); this asserts the format is
        still validated -- a malformed value must not slip through.
        """
        payload = build_minimal_ingest_payload('not-a-valid-uuid')
        status, content, header = self.ucp_client.ingest_telemetry(payload)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "malformed clusterUuid: expected %s but ingest succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "malformed clusterUuid: expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- malformed clusterUuid rejected with %s"
                      % self.expected_validation_status)
