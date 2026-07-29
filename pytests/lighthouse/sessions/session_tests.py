# -*- coding: utf-8 -*-
"""
Portal Session Tests
Validates the UCP portal session lifecycle (login, logout
invalidation, session-ID rotation) and admin protection rules: an
admin cannot disable, demote or delete their own account, and (added
2026-07-29, confirmed live) a DIFFERENT system_admin is likewise
rejected when targeting the bootstrap ("break-glass") admin -- the
portal protects that specific account rather than checking "is this
the caller's own record?", which is the mechanism guaranteeing the
system can never be left with zero enabled system_admin users.

Inherits from LighthouseBase for cluster/test infrastructure.

All portal connection details (IP, port, credentials) come from the
[LHPortal] ini section plus ucp_* test params -- nothing is
hard-coded in this module. Collector settings tests live in
lighthouse/collector/collector_tests.py, NOT here.
"""
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.response import UCPResponse
from lighthouse.ucp_helper_methods import (
    create_session,
    verify_session_active,
    get_session_cookie,
    set_session_cookie,
    extract_session_id,
    get_user_with_etag,
    open_local_user_session,
    safe_delete_user,
)
from unified_control_plane.constants import ROLE_SYSTEM_VIEWER, ROLE_SYSTEM_ADMIN


class CollectorConfigTests(LighthouseBase):
    # NOTE: class name kept as CollectorConfigTests so existing job
    # references keep working; it contains ONLY portal session tests.

    def tearDown(self):
        # Best-effort logout so no test leaves an active portal session.
        try:
            if get_session_cookie(self.ucp_client):
                self.ucp_client.session_logout()
        except Exception as e:
            self.log.warning("tearDown: portal logout failed: %s" % e)
        super(CollectorConfigTests, self).tearDown()

    def _login_as_admin(self):
        """
        Login with the portal admin credentials, assert success and
        return the UCPResponse.
        """
        status, content, header = create_session(
            self.ucp_client, self.ucp_portal.username,
            self.ucp_portal.password)
        response = UCPResponse(status, content, header)
        self.assertTrue(
            status,
            "Portal login failed (HTTP %s): %s"
            % (response.status_code, content))
        return response

    def test_valid_login_creates_active_session(self):
        """
        Case 57: valid creds + valid mapping -> successful login.

        The portal currently answers 204 No Content (the manual test
        case said 200), so the expected code is carried by the
        expected_login_status conf param (default 204) -- a one-line
        conf change if product settles on 200 instead.

        Steps:
        1. POST /session/login with the portal admin credentials
        2. Assert HTTP status equals expected_login_status
        3. Assert a session cookie was issued
        4. Assert GET /session/me confirms the session is active
        """
        expected_status = self.input.param("expected_login_status", 204)
        try:
            status, content, header = create_session(
                self.ucp_client, self.ucp_portal.username,
                self.ucp_portal.password)
            response = UCPResponse(status, content, header)
            self.assertTrue(
                status,
                "Portal login failed (HTTP %s): %s"
                % (response.status_code, content))
            self.assertEqual(
                response.status_code, expected_status,
                "Login expected HTTP %s, got %s"
                % (expected_status, response.status_code))
            cookie = get_session_cookie(self.ucp_client)
            self.assertTrue(
                cookie,
                "Login succeeded but no session cookie was issued")
            self.assertTrue(
                verify_session_active(self.ucp_client),
                "GET /session/me failed for a freshly logged-in session")
            self.log.info("PASS -- login returned HTTP %s, session active"
                          % response.status_code)
        finally:
            self.ucp_client.session_logout()

    def test_logout_invalidates_session(self):
        """
        Cases 63/67: logout terminates the session AND the pre-logout
        session cookie is rejected afterwards.

        Steps:
        1. Login as admin, assert session active
        2. Save the session cookie, POST /session/logout, assert success
        3. Replay the saved (pre-logout) cookie on GET /session/me
        4. Assert HTTP 401 -- the session is dead server-side, not
           merely forgotten by the client
        """
        try:
            self._login_as_admin()
            self.assertTrue(
                verify_session_active(self.ucp_client),
                "Session not active right after login")
            stale_cookie = get_session_cookie(self.ucp_client)
            self.assertTrue(stale_cookie, "No session cookie after login")

            status, content, header = self.ucp_client.session_logout()
            logout_response = UCPResponse(status, content, header)
            self.assertTrue(
                status,
                "Logout failed (HTTP %s): %s"
                % (logout_response.status_code, content))

            # Replay the pre-logout cookie -- the portal must reject it.
            set_session_cookie(self.ucp_client, stale_cookie)
            status, content, header = self.ucp_client.session_me()
            me_response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "Pre-logout session cookie still accepted after logout")
            self.assertEqual(
                me_response.status_code, 401,
                "Expected 401 for a logged-out session cookie, got %s"
                % me_response.status_code)
            self.log.info(
                "PASS -- logout invalidated the session cookie (401)")
        finally:
            set_session_cookie(self.ucp_client, None)

    def test_session_id_rotated_per_login(self):
        """
        Case 66: every login mints a fresh session ID -- a second
        login as the same user must not reuse the first session's ID.

        Steps:
        1. Login as admin, record the session ID
        2. Login again (first session still alive), record second ID
        3. Assert the two IDs differ
        4. Cleanup: logout both sessions
        """
        first_cookie = None
        try:
            self._login_as_admin()
            first_cookie = get_session_cookie(self.ucp_client)
            first_id = extract_session_id(first_cookie)
            self.assertTrue(
                first_id,
                "Could not extract session ID from first login")

            self._login_as_admin()
            second_id = extract_session_id(
                get_session_cookie(self.ucp_client))
            self.assertTrue(
                second_id,
                "Could not extract session ID from second login")

            self.assertNotEqual(
                first_id, second_id,
                "Session ID was reused across two logins")
            self.log.info(
                "PASS -- two logins produced distinct session IDs")
        finally:
            # Logout the current (second) session, then the first.
            try:
                self.ucp_client.session_logout()
            except Exception as e:
                self.log.warning("Logout of second session failed: %s" % e)
            if first_cookie:
                try:
                    set_session_cookie(self.ucp_client, first_cookie)
                    self.ucp_client.session_logout()
                except Exception as e:
                    self.log.warning(
                        "Logout of first session failed: %s" % e)
                set_session_cookie(self.ucp_client, None)

    def test_admin_cannot_disable_own_account(self):
        """
        Case 82: an admin must not be able to disable their own account.

        NOTE: current builds have no 'enabled' field on the user
        resource -- the API rejects it as an unexpected property on
        write. So the only acceptable behaviours are "rejected as
        unexpected property" (today) or "rejected as self-disable"
        (if the field ever returns). Either way the request must NOT
        succeed and the admin session must stay usable.

        Steps:
        1. Login as admin, fetch own user record + ETag
        2. PUT /users/{self} with enabled=false
        3. Assert the request is rejected (non-2xx)
        4. Assert the admin session is still active
        """
        admin_id = self.ucp_portal.username
        try:
            self._login_as_admin()
            user, etag = get_user_with_etag(self.ucp_client, admin_id)
            self.assertIsNotNone(
                user, "Could not fetch own user record '%s'" % admin_id)
            self.assertIsNotNone(
                etag, "No ETag returned for user '%s'" % admin_id)

            status, content, header = self.ucp_client.update_user(
                admin_id, etag, enabled=False)
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "Self-disable unexpectedly succeeded (HTTP %s): %s"
                % (response.status_code, content))
            self.log.info("Self-disable rejected with HTTP %s: %s"
                          % (response.status_code, content))

            self.assertTrue(
                verify_session_active(self.ucp_client),
                "Admin session died after a rejected self-disable")
            self.log.info(
                "PASS -- self-disable rejected, admin session intact")
        finally:
            self.ucp_client.session_logout()

    def test_admin_cannot_demote_own_role(self):
        """
        Case 83: an admin demoting their own role must be rejected
        with 409 Conflict and the role must remain unchanged.

        Steps:
        1. Login as admin, fetch own user record + ETag
        2. PUT /users/{self} with roles=[system_viewer]
        3. Assert HTTP 409 (expected_self_demote_status conf param)
        4. Assert roles unchanged and session still active
        """
        admin_id = self.ucp_portal.username
        expected_status = self.input.param("expected_self_demote_status",
                                           409)
        try:
            self._login_as_admin()
            user, etag = get_user_with_etag(self.ucp_client, admin_id)
            self.assertIsNotNone(
                user, "Could not fetch own user record '%s'" % admin_id)
            self.assertIsNotNone(
                etag, "No ETag returned for user '%s'" % admin_id)
            original_roles = user.get('roles')

            status, content, header = self.ucp_client.update_user(
                admin_id, etag, roles=[ROLE_SYSTEM_VIEWER])
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "Self-demotion unexpectedly succeeded (HTTP %s): %s"
                % (response.status_code, content))
            self.assertEqual(
                response.status_code, expected_status,
                "Self-demotion expected HTTP %s, got %s: %s"
                % (expected_status, response.status_code, content))

            user_after, _ = get_user_with_etag(self.ucp_client, admin_id)
            self.assertIsNotNone(
                user_after,
                "Could not re-fetch user '%s' after rejected demotion"
                % admin_id)
            self.assertEqual(
                user_after.get('roles'), original_roles,
                "Roles changed despite rejected self-demotion: %r -> %r"
                % (original_roles, user_after.get('roles')))
            self.assertTrue(
                verify_session_active(self.ucp_client),
                "Admin session died after a rejected self-demotion")
            self.log.info(
                "PASS -- self-demotion rejected with %s, roles unchanged"
                % response.status_code)
        finally:
            self.ucp_client.session_logout()

    def test_admin_cannot_delete_own_account(self):
        """
        Case 84: an admin deleting their own account must be rejected
        and the account must still exist and work afterwards.

        Expected status defaults to 409 (same self-protection family
        as case 83); expected_self_delete_status conf param overrides.

        Steps:
        1. Login as admin
        2. DELETE /users/{self}
        3. Assert HTTP 409 (expected_self_delete_status conf param)
        4. Assert the user record still exists and session is active
        """
        admin_id = self.ucp_portal.username
        expected_status = self.input.param("expected_self_delete_status",
                                           409)
        try:
            self._login_as_admin()

            status, content, header = self.ucp_client.delete_user(admin_id)
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "Self-delete unexpectedly succeeded (HTTP %s): %s"
                % (response.status_code, content))
            self.assertEqual(
                response.status_code, expected_status,
                "Self-delete expected HTTP %s, got %s: %s"
                % (expected_status, response.status_code, content))

            user_after, _ = get_user_with_etag(self.ucp_client, admin_id)
            self.assertIsNotNone(
                user_after,
                "Admin account '%s' missing after rejected self-delete"
                % admin_id)
            self.assertTrue(
                verify_session_active(self.ucp_client),
                "Admin session died after a rejected self-delete")
            self.log.info(
                "PASS -- self-delete rejected with %s, account intact"
                % response.status_code)
        finally:
            self.ucp_client.session_logout()

    def test_third_party_admin_cannot_modify_break_glass_admin(self):
        """
        The self-protection tests above (82/83/84) all act on the admin's
        OWN account. This confirms the same protection extends past "self":
        a DIFFERENT system_admin -- one who is not being asked to touch its
        own account -- is still rejected when it targets the bootstrap
        ("break-glass") admin, which the portal recognises as a specific,
        always-protected account rather than checking "is this the caller's
        own record?". Confirmed live 2026-07-29: both role-demotion and
        deletion of the break-glass admin by a third-party admin return 409
        Conflict ("Cannot modify or delete the break-glass admin user"),
        exactly like the self-protection cases -- this is the mechanism that
        guarantees the system can never be left with zero enabled
        system_admin users.

        Steps:
        1. Provision a second, temporary system_admin user (not the
           break-glass admin) and log in as it.
        2. As that third-party admin, attempt to demote the break-glass
           admin's role to system_viewer -> assert 409.
        3. As the same third-party admin, attempt to delete the break-glass
           admin -> assert 409.
        4. Assert the break-glass admin's roles are unchanged throughout.
        """
        third_party_id = self.input.param(
            "third_party_admin_id", "lh_third_party_admin@example.com")
        temp_password = self.input.param(
            "third_party_admin_temp_password", "TempAdmin3P#2026xyz")
        final_password = self.input.param(
            "third_party_admin_password", "Admin3P#2026xyz")
        break_glass_id = self.ucp_portal.username
        third_party_client = None
        try:
            self._login_as_admin()
            safe_delete_user(self.ucp_client, third_party_id)
            original_user, _ = get_user_with_etag(
                self.ucp_client, break_glass_id)
            self.assertIsNotNone(
                original_user,
                "Could not fetch break-glass admin's own record")
            original_roles = original_user.get('roles')

            third_party_client, err = open_local_user_session(
                self.ucp_portal, self.ucp_client, third_party_id,
                temp_password, final_password, [ROLE_SYSTEM_ADMIN])
            self.assertIsNone(
                err, "Could not provision third-party system_admin: %s" % err)

            break_glass_user, break_glass_etag = get_user_with_etag(
                third_party_client, break_glass_id)
            self.assertIsNotNone(
                break_glass_etag,
                "Third-party admin could not fetch break-glass admin's ETag")

            status, content, header = third_party_client.update_user(
                break_glass_id, break_glass_etag,
                roles=[ROLE_SYSTEM_VIEWER])
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "Third-party demotion of break-glass admin unexpectedly "
                "succeeded (HTTP %s): %s" % (response.status_code, content))
            self.assertEqual(
                response.status_code, 409,
                "Third-party demotion of break-glass admin: expected HTTP "
                "409, got %s: %s" % (response.status_code, content))

            status, content, header = third_party_client.delete_user(
                break_glass_id)
            response = UCPResponse(status, content, header)
            self.assertFalse(
                status,
                "Third-party deletion of break-glass admin unexpectedly "
                "succeeded (HTTP %s): %s" % (response.status_code, content))
            self.assertEqual(
                response.status_code, 409,
                "Third-party deletion of break-glass admin: expected HTTP "
                "409, got %s: %s" % (response.status_code, content))

            current_user, _ = get_user_with_etag(
                self.ucp_client, break_glass_id)
            self.assertEqual(
                current_user.get('roles'), original_roles,
                "Break-glass admin roles changed despite rejected "
                "third-party operations: %r -> %r"
                % (original_roles, current_user.get('roles')))
            self.log.info(
                "PASS -- third-party admin blocked from modifying/deleting "
                "the break-glass admin (409), roles unchanged")
        finally:
            if third_party_client is not None:
                try:
                    if get_session_cookie(third_party_client):
                        third_party_client.session_logout()
                except Exception as e:
                    self.log.warning(
                        "Logout of third-party admin session failed: %s" % e)
            safe_delete_user(self.ucp_client, third_party_id)
            self.ucp_client.session_logout()
