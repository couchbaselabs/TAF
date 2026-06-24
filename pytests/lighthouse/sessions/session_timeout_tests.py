
"""
Session Timeout Validation Tests
Validates session idle timeout behavior of the Unified Control Plane.
Inherits from LighthouseBase for cluster/test infrastructure.
"""
import json
from common_lib import sleep
from unified_control_plane import UnifiedControlPlaneClient
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.ucp_helper_methods import (
    create_session,
    verify_session_active,
    verify_session_expired,
    get_session_idle_timeout,
    set_session_idle_timeout,
    keep_session_alive
)

class SessionTimeoutTests(LighthouseBase):
    """
    Tests for UCP session idle timeout behavior.
    Validates that:
    - An idle session expires after sessionIdleTimeoutMinutes
    - An active session (touched within the window) stays alive
    - Timeout config changes take effect immediately
    """
    def setUp(self):
        super(SessionTimeoutTests, self).setUp()
        self.idle_timeout_minutes = self.input.param(
            "idle_timeout_minutes", 5)
        # Credentials come from the portal config (set in LighthouseBase)
        self.ucp_username = self.ucp_portal.username
        self.ucp_password = self.ucp_portal.password
        # Login the admin client (self.ucp_client from LighthouseBase)
        status, content, _ = create_session(
            self.ucp_client, self.ucp_username, self.ucp_password)
        if not status:
            self.fail("Admin session login failed: %s" % content)
        # Store original config so we can restore in tearDown
        self._original_idle_timeout = get_session_idle_timeout(
            self.ucp_client)
        self.log.info("Original idle timeout: %s minutes"
                      % self._original_idle_timeout)

    def tearDown(self):
        # Restore original idle timeout if we changed it
        if hasattr(self, '_original_idle_timeout') \
                and self._original_idle_timeout is not None:
            try:
                set_session_idle_timeout(
                    self.ucp_client, self._original_idle_timeout)
                self.log.info("Restored idle timeout to %s minutes"
                              % self._original_idle_timeout)
            except Exception as e:
                self.log.warning("Failed to restore idle timeout: %s" % e)
        super(SessionTimeoutTests, self).tearDown()

    def _create_fresh_client(self):
        """
        Create a new UCP client instance with its own session.
        Uses the same portal config but a fresh connection.
        """
        return UnifiedControlPlaneClient(self.ucp_portal)
    
    def test_session_idle_timeout(self):
        """
        Validate session idle timeout behavior.
        Steps:
        1. Set idle timeout to minimum (5 minutes)
        2. Create two sessions (session_A and session_B)
        3. Leave session_A completely idle
        4. Touch session_B at 3 minutes to keep it alive
        5. After 5+ minutes, verify:
           - session_A is expired (401)
           - session_B is still active (200)
        """
        idle_timeout = self.idle_timeout_minutes
        self.log.info("Setting session idle timeout to %d minutes"
                      % idle_timeout)
        # Step 1: Configure idle timeout
        status, content, _ = set_session_idle_timeout(
            self.ucp_client, idle_timeout)
        self.assertTrue(status,
                        "Failed to set idle timeout: %s" % content)
        self.log.info("Idle timeout set to %d minutes" % idle_timeout)
        # Step 2: Create two independent sessions
        self.log.info("Creating session_A (will be left idle)")
        client_a = self._create_fresh_client()
        status_a, content_a, _ = create_session(
            client_a, self.ucp_username, self.ucp_password)
        self.assertTrue(status_a,
                        "Session A login failed: %s" % content_a)
        self.log.info("Creating session_B (will be kept alive)")
        client_b = self._create_fresh_client()
        status_b, content_b, _ = create_session(
            client_b, self.ucp_username, self.ucp_password)
        self.assertTrue(status_b,
                        "Session B login failed: %s" % content_b)
        # Verify both sessions are active right now
        self.assertTrue(verify_session_active(client_a),
                        "Session A should be active immediately after login")
        self.assertTrue(verify_session_active(client_b),
                        "Session B should be active immediately after login")
        self.log.info("Both sessions confirmed active")
        # Step 3 & 4: Wait and selectively touch session_B
        touch_at_seconds = (idle_timeout - 2) * 60
        total_wait_seconds = (idle_timeout + 1) * 60
        self.log.info("Waiting %d seconds before touching session_B"
                      % touch_at_seconds)
        sleep(touch_at_seconds, "Waiting for touch interval")
        # Touch session_B to reset idle timer
        self.log.info("Touching session_B to reset idle timer")
        alive = keep_session_alive(client_b)
        self.assertTrue(alive,
                        "Session B should still be alive at touch point")
        self.log.info("Session B touched successfully at %d seconds"
                      % touch_at_seconds)
        # Wait remaining time for session_A to expire
        remaining_wait = total_wait_seconds - touch_at_seconds
        self.log.info("Waiting remaining %d seconds for session_A to expire"
                      % remaining_wait)
        sleep(remaining_wait, "Waiting for idle session to expire")
        # Step 5: Verify session states
        self.log.info("Verifying session_A has expired")
        session_a_expired = verify_session_expired(client_a)
        self.assertTrue(session_a_expired,
                        "Session A should have expired after %d minutes "
                        "of inactivity" % idle_timeout)
        self.log.info("PASS: Session A expired as expected")
        self.log.info("Verifying session_B is still active")
        session_b_active = verify_session_active(client_b)
        self.assertTrue(session_b_active,
                        "Session B should still be active since it was "
                        "touched within the idle window")
        self.log.info("PASS: Session B still active as expected")
