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
    - limit=0                       -> rejected (422 validation_error; below
                                        the documented minimum of 1)

Request body / header validation (added 2026-07-29, confirmed live against
the QE lab portal before writing -- several diverge from the status a naive
guess would produce):
    - malformed (non-JSON) body           -> 400 Bad Request (a parse
                                              failure, distinct from the 422
                                              "well-formed but invalid" family
                                              above)
    - missing required field (POST users
      without 'roles')                    -> 422 validation_error
    - PUT with no If-Match header at all  -> 422 validation_error (the header
                                              itself is a required parameter)
    - PUT with a stale (superseded) ETag  -> 412 Precondition Failed
    - DELETE with no If-Match header      -> succeeds (204); confirmed live
                                              that, unlike PUT, DELETE has no
                                              conditional-request requirement
                                              in current builds -- this is
                                              asserted directly rather than
                                              assumed, so a future product
                                              change here fails loudly

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
it is a one-line change if the product ever switches to 400. Malformed-body
and stale-ETag statuses are separate conf params for the same reason
(expected_malformed_json_status default 400, expected_precondition_failed_status
default 412).

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
    raw_request,
    post_raw_body,
    safe_delete_user,
    get_user_with_etag,
    build_minimal_ingest_payload,
)
from unified_control_plane.constants import ROLE_SYSTEM_VIEWER, ROLE_SYSTEM_ADMIN


class ApiValidationTests(LighthouseBase):

    def setUp(self):
        super(ApiValidationTests, self).setUp()
        self.list_path = self.input.param("list_path", "api/v1/users")
        # Validation errors come back as 422 Unprocessable Entity, not 400.
        self.expected_validation_status = self.input.param(
            "expected_validation_status", 422)
        # A syntactically broken (non-JSON) body is a parse failure, not a
        # semantic validation error -- confirmed live 2026-07-29 the portal
        # answers 400 Bad Request for this, distinct from the 422 above.
        self.expected_malformed_json_status = self.input.param(
            "expected_malformed_json_status", 400)
        # A stale (previously valid, now outdated) If-Match value -- confirmed
        # live 2026-07-29 as 412 Precondition Failed.
        self.expected_precondition_failed_status = self.input.param(
            "expected_precondition_failed_status", 412)
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

    def test_limit_zero_rejected(self):
        """limit=0 on a list endpoint -> 422 validation_error (below the
        documented minimum of 1)."""
        status, content, header = self.ucp_client.list_users(limit=0)
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "limit=0: expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "limit=0: expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- limit=0 rejected with %s"
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

    # ==================== Request body / header validation =================

    def test_malformed_json_payload_rejected(self):
        """
        A syntactically broken (non-JSON) request body -> 400 Bad Request.
        Distinct from a well-formed body carrying an invalid value (422,
        see test_missing_required_field_rejected below) -- this body never
        even parses as JSON.
        """
        status, content, header = post_raw_body(
            self.ucp_client, 'api/v1/users', '{not valid json,,,')
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "malformed JSON body: expected %s but the call succeeded"
            % self.expected_malformed_json_status)
        self.assertEqual(
            response.status_code, self.expected_malformed_json_status,
            "malformed JSON body: expected HTTP %s, got %s: %s"
            % (self.expected_malformed_json_status, response.status_code,
               content))
        self.log.info("PASS -- malformed JSON body rejected with %s"
                      % self.expected_malformed_json_status)

    def test_missing_required_field_rejected(self):
        """A user-create body missing the required 'roles' field ->
        422 validation_error."""
        status, content, header = raw_request(
            self.ucp_client, 'POST', 'api/v1/users',
            body={'userId': 'probe_missing_field@example.com',
                  'authType': 'local', 'password': 'Probe#2026xyz'})
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status, "missing 'roles': expected %s but the call succeeded"
            % self.expected_validation_status)
        self.assertEqual(
            response.status_code, self.expected_validation_status,
            "missing 'roles': expected HTTP %s, got %s: %s"
            % (self.expected_validation_status, response.status_code, content))
        self.log.info("PASS -- missing required field rejected with %s"
                      % self.expected_validation_status)

    def test_put_without_if_match_rejected(self):
        """PUT /users/{userId} with no If-Match header at all ->
        422 validation_error (the header itself is a required parameter)."""
        probe_id = 'probe_no_if_match@example.com'
        safe_delete_user(self.ucp_client, probe_id)
        status, content, _ = self.ucp_client.create_user(
            probe_id, roles=[ROLE_SYSTEM_VIEWER], auth_type='local',
            password='Probe#2026xyz')
        self.assertTrue(status, "Could not create probe user: %s" % content)
        try:
            status, content, header = raw_request(
                self.ucp_client, 'PUT', 'api/v1/users/%s' % probe_id,
                body={'roles': [ROLE_SYSTEM_ADMIN]})
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status, "PUT without If-Match: expected %s but the call "
                "succeeded" % self.expected_validation_status)
            self.assertEqual(
                response.status_code, self.expected_validation_status,
                "PUT without If-Match: expected HTTP %s, got %s: %s"
                % (self.expected_validation_status, response.status_code,
                   content))
            self.log.info("PASS -- PUT without If-Match rejected with %s"
                          % self.expected_validation_status)
        finally:
            safe_delete_user(self.ucp_client, probe_id)

    def test_put_stale_etag_rejected(self):
        """
        A PUT reusing an ETag that was valid but has since been superseded
        by another successful update -> 412 Precondition Failed.
        """
        probe_id = 'probe_stale_etag@example.com'
        safe_delete_user(self.ucp_client, probe_id)
        status, content, _ = self.ucp_client.create_user(
            probe_id, roles=[ROLE_SYSTEM_VIEWER], auth_type='local',
            password='Probe#2026xyz')
        self.assertTrue(status, "Could not create probe user: %s" % content)
        try:
            _, stale_etag = get_user_with_etag(self.ucp_client, probe_id)
            self.assertIsNotNone(
                stale_etag, "Could not fetch initial ETag for probe user")

            # A first, successful update supersedes stale_etag with a new one.
            status, content, _ = self.ucp_client.update_user(
                probe_id, stale_etag, roles=[ROLE_SYSTEM_ADMIN])
            self.assertTrue(
                status, "Baseline update (to make the etag stale) failed: %s"
                % content)

            # Reusing the now-superseded etag must be rejected.
            status, content, header = self.ucp_client.update_user(
                probe_id, stale_etag, roles=[ROLE_SYSTEM_VIEWER])
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status, "stale ETag: expected %s but the call succeeded"
                % self.expected_precondition_failed_status)
            self.assertEqual(
                response.status_code, self.expected_precondition_failed_status,
                "stale ETag: expected HTTP %s, got %s: %s"
                % (self.expected_precondition_failed_status,
                   response.status_code, content))
            self.log.info("PASS -- stale ETag rejected with %s"
                          % self.expected_precondition_failed_status)
        finally:
            safe_delete_user(self.ucp_client, probe_id)

    def test_delete_does_not_require_if_match(self):
        """
        DELETE /users/{userId} succeeds with no If-Match header at all --
        confirmed live 2026-07-29 that, unlike PUT, DELETE has no
        conditional-request requirement in current builds. This documents
        the confirmed asymmetry: if a future build starts requiring If-Match
        on DELETE too, this test will start failing and should be updated
        alongside the product change, not silently left asserting the old
        behaviour.
        """
        probe_id = 'probe_delete_no_if_match@example.com'
        safe_delete_user(self.ucp_client, probe_id)
        status, content, _ = self.ucp_client.create_user(
            probe_id, roles=[ROLE_SYSTEM_VIEWER], auth_type='local',
            password='Probe#2026xyz')
        self.assertTrue(status, "Could not create probe user: %s" % content)

        status, content, header = self.ucp_client.delete_user(probe_id)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status, "DELETE without If-Match unexpectedly failed (HTTP %s): "
            "%s" % (response.status_code, content))
        self.log.info(
            "PASS -- DELETE without If-Match succeeded (HTTP %s), confirming "
            "current builds have no conditional-delete requirement"
            % response.status_code)

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
