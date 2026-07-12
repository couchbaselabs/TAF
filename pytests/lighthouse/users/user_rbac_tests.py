# -*- coding: utf-8 -*-
"""
UCP Portal RBAC Tests -- system_viewer role.

Validates the portal's authorization decisions for a system_viewer user
(cases 75, 78, 79, 80):
    - 75: user-management endpoints  -> 403
    - 78: audit endpoints            -> 403
    - 79: config read AND update     -> 403
    - 80: allowed read endpoints     -> 200

setUp provisions a fresh local system_viewer user on the fly (admin creates
it, then the temporary password is swapped for a usable one and we log in as
the viewer). tearDown deletes the viewer user again. Inherits LighthouseBase
for cluster/portal infrastructure.

All portal connection details (IP, port, credentials) come from the
[LHPortal] ini section plus ucp_* test params -- nothing is hard-coded here.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    get_session_cookie,
    open_local_user_session,
)
from unified_control_plane.constants import ROLE_SYSTEM_VIEWER


class SystemViewerRbacTests(LighthouseBase):

    def setUp(self):
        super(SystemViewerRbacTests, self).setUp()
        self.viewer_id = self.input.param("viewer_user_id",
                                          "lh_viewer@example.com")
        # Both passwords must satisfy the CBS password policy. A too-weak
        # password currently returns 500 (not 400) from POST /users -- see
        # the bug filed for that; keep these long + mixed-class.
        self.viewer_temp_password = self.input.param(
            "viewer_temp_password", "TempView#2026xyz")
        self.viewer_password = self.input.param(
            "viewer_password", "Viewer#2026xyz")
        # A second user id the viewer will try (and fail) to create in case 75.
        self.probe_user_id = self.input.param("probe_user_id",
                                              "lh_probe@example.com")
        self.viewer_client = None
        # Admin session is required to create/delete the viewer user.
        status, content, _ = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        self.assertTrue(status, "Admin login failed: %s" % content)
        # Remove any stale users left behind by a previous aborted run.
        self._safe_delete_user(self.viewer_id)
        self._safe_delete_user(self.probe_user_id)
        # Provision the viewer and log in as it.
        viewer_client, err = open_local_user_session(
            self.ucp_portal, self.ucp_client, self.viewer_id,
            self.viewer_temp_password, self.viewer_password,
            [ROLE_SYSTEM_VIEWER])
        self.assertIsNone(
            err, "Could not provision system_viewer session: %s" % err)
        self.viewer_client = viewer_client

    def tearDown(self):
        # Delete the viewer (and probe) user via the admin session, then log
        # everyone out. tearDown must never raise.
        try:
            if not get_session_cookie(self.ucp_client):
                create_session(self.ucp_client, self.ucp_portal.username,
                               self.ucp_portal.password)
            self._safe_delete_user(self.viewer_id)
            self._safe_delete_user(self.probe_user_id)
        except Exception as e:
            self.log.warning("tearDown: viewer cleanup failed: %s" % e)
        for client in (self.viewer_client, self.ucp_client):
            try:
                if client is not None and get_session_cookie(client):
                    client.session_logout()
            except Exception as e:
                self.log.warning("tearDown: logout failed: %s" % e)
        super(SystemViewerRbacTests, self).tearDown()

    def _safe_delete_user(self, user_id):
        """Best-effort delete of a user via the admin session."""
        try:
            self.ucp_client.delete_user(user_id)
        except Exception as e:
            self.log.warning("delete_user(%s) failed: %s" % (user_id, e))

    def _assert_forbidden(self, status, content, header, what):
        response = UCPResponse(status, content, header)
        self.assertFalse(
            status,
            "%s: expected 403 for system_viewer but call succeeded" % what)
        self.assertEqual(
            response.status_code, 403,
            "%s: expected HTTP 403, got %s: %s"
            % (what, response.status_code, content))

    def _assert_allowed(self, status, content, header, what):
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status,
            "%s: expected 200 for system_viewer, got HTTP %s: %s"
            % (what, response.status_code, content))
        self.assertEqual(
            response.status_code, 200,
            "%s: expected HTTP 200, got %s" % (what, response.status_code))

    def test_system_viewer_forbidden_on_user_management(self):
        """
        Case 75: a system_viewer cannot access user-management endpoints.

        Checks both a read (list users) and a write (create user); each must
        return 403.
        """
        status, content, header = self.viewer_client.list_users()
        self._assert_forbidden(status, content, header, "GET /users")

        status, content, header = self.viewer_client.create_user(
            self.probe_user_id, roles=[ROLE_SYSTEM_VIEWER],
            auth_type='local', password=self.viewer_temp_password)
        self._assert_forbidden(status, content, header, "POST /users")
        self.log.info("PASS -- system_viewer blocked from user management")

    def test_system_viewer_forbidden_on_audit(self):
        """Case 78: a system_viewer cannot read audit logs -> 403."""
        status, content, header = self.viewer_client.list_audit_events()
        self._assert_forbidden(status, content, header, "GET /audit")
        self.log.info("PASS -- system_viewer blocked from audit logs")

    def test_system_viewer_forbidden_on_config(self):
        """
        Case 79: a system_viewer cannot read or update config -> 403.

        For the update leg we fetch a valid ETag with the admin session first
        so authorization is the only failing dimension (a stale/missing ETag
        would otherwise let the server answer 400/412 instead of 403).
        """
        status, content, header = self.viewer_client.get_config()
        self._assert_forbidden(status, content, header, "GET /config")

        admin_status, admin_content, admin_header = self.ucp_client.get_config()
        admin_config = UCPResponse(admin_status, admin_content, admin_header)
        self.assertTrue(
            admin_status,
            "Admin could not read config to obtain ETag: %s" % admin_content)
        etag = admin_config.etag

        status, content, header = self.viewer_client.update_config(
            etag=etag, telemetry_retention_days=30)
        self._assert_forbidden(status, content, header, "PUT /config")
        self.log.info("PASS -- system_viewer blocked from config read/update")

    def test_system_viewer_allowed_reads(self):
        """
        Case 80: a system_viewer can read the endpoints it is entitled to.

        clusters, entitlements, entitlement usage and its own session info
        must all return 200.
        """
        checks = [
            ("GET /session/me", self.viewer_client.session_me),
            ("GET /clusters", self.viewer_client.list_clusters),
            ("GET /entitlements", self.viewer_client.get_entitlements),
            ("GET /entitlements/usage",
             self.viewer_client.get_entitlement_usage),
        ]
        for what, call in checks:
            status, content, header = call()
            self._assert_allowed(status, content, header, what)
        self.log.info("PASS -- system_viewer allowed reads all returned 200")
