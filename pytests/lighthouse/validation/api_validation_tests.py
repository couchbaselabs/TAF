# -*- coding: utf-8 -*-
"""
UCP Portal API Validation Tests -- pagination and query-parameter handling
on list endpoints (cases 97, 98, 86).

    - 97: offset=-1                 -> rejected (422 validation_error)
    - 98: offset beyond total       -> 200 with empty items and correct total
    - 86: unknown query parameter   -> rejected (422 validation_error)

The portal answers 422 Unprocessable Entity for validation errors (a
well-formed request carrying a semantically invalid value). The
expected status is a conf param (expected_validation_status, default 422) so
it is a one-line change if the product ever switches to 400.

Targets GET /api/v1/users by default (admin-accessible and deterministic --
the bootstrap admin always exists, so total >= 1). The endpoint is a conf
param (list_path) so the same logic can be pointed at /clusters or /audit.
Inherits LighthouseBase for portal wiring; needs only an admin session.

Response parsing lives on UCPResponse (.items/.total); the raw GET used for
the unknown-parameter case lives in ucp_helper_methods.get_raw -- this class
holds test methods only.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_session_cookie,
    get_raw,
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
