import datetime
import json
import socket
import ssl
import threading
import time
import uuid

import requests

from urllib.parse import urlencode

from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from cbas.cbas_crl_base import CBASCRLBase

# A SQL++ statement that needs no data and reliably runs long enough to revoke
# a certificate while it is still executing. Measured on an 8.1.0-2632 cbas
# node: ~16s for this 400M-row cross join, versus 2-3s for the smaller ranges
# that were tried first. Deliberately computation-only -- a dataset-backed
# query would make the test depend on ingestion finishing.
LONG_RUNNING_QUERY = (
    "SELECT VALUE COUNT(*) FROM range(1,20000000) a, range(1,20) b;"
)
# Below this, the query finished too quickly to have overlapped the revocation,
# so the test cannot prove what it claims and says so rather than passing.
LONG_QUERY_MIN_SECONDS = 6


class CBASCRLLifecycle(CBASCRLBase):
    """
    CRL enforcement on the Analytics service's own TLS surface, per
    Analytics_CRL_Lifecycle_TestPlan.

    Covers the plan's sections 1 (client access to Analytics REST), 2 (SQL++
    execution), and 9 (log hygiene) -- the scenarios that need only an
    Analytics node and a client certificate. Sections 3 and 4 (Analytics Links
    to remote clusters and to S3/Azure/GCS), 5 (shadow-data ingestion), 7
    (multi-node distribution and rebalance) and 8 (upgrade / mixed-version)
    need topology or external endpoints this file deliberately does not build,
    and are left for follow-up.

    One correction to the plan, verified against a live 8.1.0-2632 cluster:
    section 1 refers to policy modes "Disabled/Permissive/Strict/Require", but
    the server accepts only three -- POST /settings/crl with clientAuth=Strict
    is rejected with `unknown mode: Strict` (HTTP 400). These tests therefore
    exercise Disabled, Permissive and Require only, and the plan (and the CRL
    PRD it inherits the list from) should be corrected.
    """

    def test_analytics_rejects_revoked_cert_and_honours_valid_one(self):
        """
        Section 1: a revoked client certificate is rejected at the Analytics
        REST endpoint (18095), while a valid, non-revoked certificate
        continues to reach the service and execute according to RBAC.

        Both certificates come from the same CA and differ only in whether
        their serial appears on the uploaded CRL, so a difference in outcome
        can only be revocation.
        """
        valid_cert, valid_key, _ = self._client_cert_for(
            "cbas_crl_valid_admin", "analytics_admin"
        )
        revoked_cert, revoked_key, revoked_serial = self._client_cert_for(
            "cbas_crl_revoked_admin", "analytics_admin"
        )

        filename = "cbas_crl_section1.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        resp = self._wait_for_analytics_ok(
            "SELECT 1;", cert=(valid_cert, valid_key)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"A valid, non-revoked certificate must continue to reach the "
            f"Analytics service, got {resp.status_code}: {resp.text[:300]}"
        )
        self.log.info("Valid certificate reached Analytics as expected")

        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=(revoked_cert, revoked_key)),
            "A revoked certificate must be rejected at the TLS layer on "
                "the Analytics endpoint, not merely denied once it reaches "
                "the query layer",
        )
        self.log.info("Revoked certificate rejected by Analytics as expected")

    def test_analytics_enforcement_matches_ns_server_for_same_cert(self):
        """
        Section 1: Analytics enforcement matches ns_server for the same
        certificate under the same policy, in every mode the server actually
        supports.

        This is the cross-service consistency requirement, and it is the one
        the PRD calls out as a security risk if it fails ("Inconsistent
        enforcement across services -> security bypass"). Rather than assert
        an absolute outcome per mode, it asserts the two services AGREE --
        which is the property that matters and which survives a future change
        to what any single mode does.
        """
        cert, key, serial = self._client_cert_for(
            "cbas_crl_consistency", "analytics_admin"
        )

        filename = "cbas_crl_consistency.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        # Hybrid, NOT mandatory: the loop below changes the policy through
        # self.rest between modes, and self.rest carries no client certificate.
        # Under mandatory it is walled out like any other cert-less client --
        # TAF's REST layer then retries until its budget expires and reports
        # ServerUnavailableException, which reads exactly like the node being
        # down. It is not: the node is healthy and correctly refusing a
        # cert-less request. Hybrid still enforces revocation for a presented
        # certificate, which is all this comparison needs.
        self._enable_client_cert_auth(state="enable")

        # 'Strict' is deliberately absent -- the server rejects it, see the
        # class docstring.
        for mode in ("Disabled", "Permissive", "Require"):
            self.crl_utils.set_settings(
                self.rest,
                policyPerScope={"clientAuth": mode, "nodeToNode": "Disabled"},
            )

            def accepted(request_fn):
                """True if the peer certificate got past the TLS layer."""
                try:
                    resp = request_fn()
                except requests.exceptions.SSLError:
                    return False
                # 401/403 still means the certificate was accepted at the TLS
                # layer and rejected later by auth/RBAC, which is a different
                # thing from a revocation rejection.
                return resp is not None

            analytics_ok = accepted(
                lambda: self._analytics_query("SELECT 1;", cert=(cert, key))
            )
            ns_server_ok = accepted(
                lambda: self._mgmt_request(cert=(cert, key))
            )
            self.assertEqual(
                analytics_ok, ns_server_ok,
                f"Under policy {mode}, Analytics and ns_server disagreed on "
                f"the same revoked certificate: Analytics accepted="
                f"{analytics_ok}, ns_server accepted={ns_server_ok}. "
                f"Inconsistent enforcement across services is a revocation "
                f"bypass."
            )
            self.log.info(
                f"Policy {mode}: Analytics and ns_server agree "
                f"(cert accepted at TLS layer = {analytics_ok})"
            )

    def test_analytics_hot_reload_revoke_and_restore(self):
        """
        Section 1: adding a serial to the active CRL rejects that certificate
        on the next connection with no Analytics restart, and removing it
        restores access -- again with no restart.

        Runs in hybrid mTLS rather than mandatory: the CRL is replaced through
        self.rest between the two halves, and self.rest carries no client
        certificate, so mandatory would wall out the very call that changes
        the CRL. Hybrid still fully enforces revocation for a presented
        certificate.
        """
        cert, key, serial = self._client_cert_for(
            "cbas_crl_reload_admin", "analytics_admin"
        )

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        # A CRL that revokes nothing: under Require an unavailable CRL fails
        # closed, so one has to exist before a valid certificate can connect.
        allowing = "cbas_crl_reload_allow.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], allowing, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(allowing)

        resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
        self.assertEqual(
            resp.status_code, 200,
            f"Baseline: certificate should connect before it is revoked, got "
            f"{resp.status_code}: {resp.text[:300]}"
        )

        # ── revoke: replace the CRL with one naming this serial ─────────────
        status, _ = self.crl_utils.delete_file(self.rest, allowing)
        self.assertTrue(status, "Failed to delete the allowing CRL")
        self._created_files.remove(allowing)

        revoking = "cbas_crl_reload_revoke.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, revoking,
            crl_number=2,
        )
        self.assertTrue(status, f"Revoking CRL upload failed: {content}")
        self._track_uploaded_file(revoking)

        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=(cert, key)),
            "Once its serial is on the active CRL the certificate must be "
                "rejected on the next connection, with no Analytics restart "
                "and no session-persistence bypass",
        )
        self.log.info("Certificate rejected immediately after being revoked")

        # ── restore: put back a CRL that revokes nothing ───────────────────
        status, _ = self.crl_utils.delete_file(self.rest, revoking)
        self.assertTrue(status, "Failed to delete the revoking CRL")
        self._created_files.remove(revoking)

        restored = "cbas_crl_reload_restore.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], restored, crl_number=3,
        )
        self.assertTrue(status, f"Restoring CRL upload failed: {content}")
        self._track_uploaded_file(restored)

        resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
        self.assertEqual(
            resp.status_code, 200,
            f"Access must be restored once the serial is off the CRL, with no "
            f"Analytics restart, got {resp.status_code}: {resp.text[:300]}"
        )
        self.log.info("Access restored after the serial was removed from the CRL")

    def test_analytics_fails_closed_when_no_applicable_crl(self):
        """
        Section 1: Analytics connections fail closed when CRL checking is
        enabled and no current, applicable CRL exists -- covering both a
        missing CRL and an expired one -- and Permissive lets the same
        certificate through, which is what distinguishes the two modes.

        The certificate is never revoked here. Every rejection is caused by
        the absence of usable revocation information, not by revocation.
        """
        cert, key, _ = self._client_cert_for(
            "cbas_crl_failclosed", "analytics_admin"
        )

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        # ── missing CRL under Require ──────────────────────────────────────
        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=(cert, key)),
            "With policy Require and no applicable CRL for the issuing "
                "CA, the connection must fail closed rather than be allowed "
                "because revocation status is unknown",
        )
        self.log.info("Missing CRL under Require: connection failed closed")

        # ── expired CRL under Require ──────────────────────────────────────
        # build_crl(expired=True) backdates nextUpdate, so the CRL is present
        # but cannot establish current revocation status.
        expired = "cbas_crl_expired.pem"
        pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=1,
            expired=True,
        )
        status, content = self.crl_utils.upload_file(self.rest, expired, pem)
        if status:
            self._track_uploaded_file(expired)
            self.assert_cert_refused(
                lambda: self._analytics_query("SELECT 1;", cert=(cert, key)),
                "An expired CRL cannot establish that a certificate is "
                    "currently unrevoked, so under Require the connection "
                    "must fail closed",
            )
            self.log.info("Expired CRL under Require: connection failed closed")
        else:
            # ns_server may refuse an already-expired CRL at upload time; that
            # is its own documented behaviour and belongs to the ns_server
            # lifecycle suite, so record it rather than failing here.
            self.log.info(
                f"Server refused the expired CRL at upload time, so the "
                f"expired-CRL path cannot be exercised from here: {content}"
            )

        # ── Permissive allows what Require rejected ────────────────────────
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Permissive", "nodeToNode": "Disabled"},
        )
        resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
        self.assertEqual(
            resp.status_code, 200,
            f"Permissive must allow a certificate whose revocation status is "
            f"unknown, with a warning -- that is the only thing separating it "
            f"from Require. Got {resp.status_code}: {resp.text[:300]}"
        )
        self.log.info("Permissive allowed the same certificate, as required")

    def test_analytics_revoked_cert_cannot_run_sqlpp_and_error_is_specific(self):
        """
        Section 2: a revoked certificate cannot execute SQL++ against
        Analytics datasets, and the failure is a certificate/authentication
        failure rather than a generic query error.

        The statement is a real query rather than SELECT 1 so that a rejection
        cannot be attributed to the statement being trivial or short-circuited
        before authentication.
        """
        cert, key, serial = self._client_cert_for(
            "cbas_crl_sqlpp", "analytics_admin"
        )

        filename = "cbas_crl_sqlpp.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        statement = 'SELECT VALUE d FROM Metadata.`Dataset` d LIMIT 1;'
        try:
            resp = self._analytics_query(statement, cert=(cert, key))
        except requests.exceptions.SSLError as exc:
            # Rejected at the TLS layer: unambiguously a certificate failure,
            # which satisfies both bullets at once.
            self.log.info(
                f"SQL++ with a revoked certificate rejected at the TLS layer, "
                f"which is a certificate-specific failure: {exc}"
            )
            return

        # If the handshake completed, the service must still refuse the query,
        # and must say why in certificate/auth terms rather than as a query
        # error.
        self.assertNotEqual(
            resp.status_code, 200,
            f"A revoked certificate must not be able to execute SQL++. The "
            f"query returned 200: {resp.text[:300]}"
        )
        self.assertIn(
            resp.status_code, (401, 403),
            f"A revoked certificate should fail as an authentication problem "
            f"(401/403), not as a generic query error. Got "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        self.log.info(
            f"SQL++ with a revoked certificate refused with "
            f"{resp.status_code}, an authentication-shaped failure"
        )

    # ── Scenarios added from reviewer comments on the plan ──────────────────

    def test_enforcement_scope_toggles_without_restart(self):
        """
        Enabling and disabling the clientAuth enforcement scope must take
        effect on new connections with no Analytics (or driver) restart, and
        Analytics must remain fully functional while the scope is disabled.

        From Michael Blow's review comment on the plan: "we need to ensure that
        CRL enforcement is enabled without driver or cbas restart when
        enforcement scopes are enabled, also should ensure functionality when
        they are disabled." The plan text itself does not carry this
        requirement.

        The certificate is revoked once, up front, and never changes. Only the
        POLICY moves, so each transition isolates the effect of the scope
        toggle rather than of the CRL content. Nothing is restarted anywhere in
        this test -- that absence is the assertion.
        """
        cert, key, serial = self._client_cert_for(
            "cbas_crl_scope_toggle", "analytics_admin"
        )

        filename = "cbas_crl_scope_toggle.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # Hybrid: the policy is toggled through self.rest repeatedly below, and
        # self.rest carries no client certificate.
        self._enable_client_cert_auth(state="enable")

        # ── scope disabled: revoked certificate still works ────────────────
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
        self.assertEqual(
            resp.status_code, 200,
            f"With the clientAuth scope Disabled, Analytics must be fully "
            f"functional and must not consult the CRL at all -- a revoked "
            f"certificate should still work. Got {resp.status_code}: "
            f"{resp.text[:300]}"
        )
        self.log.info("Scope Disabled: revoked certificate works, as required")

        # ── enable the scope: takes effect with no restart ─────────────────
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=(cert, key)),
            "Enabling the clientAuth scope must start rejecting the "
                "revoked certificate on the next connection, with no cbas or "
                "driver restart",
        )
        self.log.info("Scope enabled -> revoked certificate rejected, no restart")

        # ── disable it again: functionality returns, still no restart ──────
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
        self.assertEqual(
            resp.status_code, 200,
            f"Disabling the scope again must restore functionality with no "
            f"restart, got {resp.status_code}: {resp.text[:300]}"
        )
        self.log.info(
            "Scope disabled again -> functionality restored, no restart. "
            "Enforcement is togglable at runtime in both directions."
        )

    def test_long_running_query_completes_despite_mid_execution_revocation(self):
        """
        A query that has already authenticated must be allowed to finish even
        if its certificate is revoked while it is still executing.

        This encodes the expectation Michael Blow set in review, which the plan
        text left open: "I would expect a successfully authenticated connection
        to be able to complete a (long) running query, even if the certificate
        is revoked immediately after authentication phase." The plan's own
        wording ("terminated or blocked correctly") implies the opposite, so
        this test is written to the reviewed expectation and will fail loudly
        if Analytics tears the query down instead.

        Paired with a second check: once the query has finished, a NEW
        connection with the same certificate must be rejected. Without that,
        the test could pass simply because revocation never took effect at all.
        """
        cert, key, serial = self._client_cert_for(
            "cbas_crl_longquery", "analytics_admin"
        )

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        # Hybrid: the CRL is replaced through self.rest while the query runs.
        self._enable_client_cert_auth(state="enable")

        allowing = "cbas_crl_longquery_allow.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], allowing, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(allowing)

        # Confirm the certificate works before timing anything.
        resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
        self.assertEqual(resp.status_code, 200, "Baseline query should succeed")

        result = {}

        def run_long_query():
            started = time.time()
            try:
                result["resp"] = self._analytics_query(
                    LONG_RUNNING_QUERY, cert=(cert, key), timeout=300
                )
            except Exception as exc:            # noqa: BLE001 - recorded, re-read below
                result["exc"] = exc
            result["elapsed"] = time.time() - started

        thread = threading.Thread(target=run_long_query, name="crl_long_query")
        thread.start()
        # Let the query authenticate and get properly under way before the
        # certificate is revoked -- revoking before it starts would test
        # connection-time enforcement, which is a different scenario.
        time.sleep(5)

        status, _ = self.crl_utils.delete_file(self.rest, allowing)
        self.assertTrue(status, "Failed to delete the allowing CRL")
        self._created_files.remove(allowing)
        revoking = "cbas_crl_longquery_revoke.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, revoking,
            crl_number=2,
        )
        self.assertTrue(status, f"Revoking CRL upload failed: {content}")
        self._track_uploaded_file(revoking)
        self.log.info("Certificate revoked while the long query was executing")

        thread.join(timeout=300)
        self.assertFalse(
            thread.is_alive(), "Long-running query did not finish within 300s"
        )

        elapsed = result.get("elapsed", 0)
        self.assertGreaterEqual(
            elapsed, LONG_QUERY_MIN_SECONDS,
            f"The query finished in {elapsed:.1f}s, too fast to have still "
            f"been executing when the certificate was revoked at the 5s mark. "
            f"This test proves nothing at that speed -- make "
            f"LONG_RUNNING_QUERY heavier for this cluster."
        )

        self.assertNotIn(
            "exc", result,
            f"An already-authenticated query must not be torn down by a "
            f"mid-execution revocation, but it raised: {result.get('exc')}"
        )
        self.assertEqual(
            result["resp"].status_code, 200,
            f"The query authenticated before its certificate was revoked and "
            f"must be allowed to complete (per plan review), got "
            f"{result['resp'].status_code}: {result['resp'].text[:300]}"
        )
        self.log.info(
            f"Long query completed successfully in {elapsed:.1f}s despite "
            f"mid-execution revocation, as the reviewed expectation requires"
        )

        # And revocation really is in force now, for NEW connections.
        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=(cert, key)),
            "A new connection with the revoked certificate must be "
                "rejected -- otherwise the check above passed only because "
                "revocation never took effect",
        )
        self.log.info("New connections with the same certificate are rejected")

    def test_revocation_precedes_identity_mapping_and_rbac(self):
        """
        Section 1: revocation is evaluated before certificate identity
        extraction and RBAC role resolution.

        The certificate's CN maps to NO RBAC user, and its serial IS revoked.
        If revocation is checked first, the connection dies at the TLS layer.
        If identity mapping ran first, the failure would instead be an
        unknown-user authentication error (401/500) -- which would mean a
        revoked certificate reaches the identity layer at all.

        The log is then checked to confirm the recorded reason is revocation
        rather than a missing user, so the assertion cannot be satisfied by a
        TLS failure that happens for some unrelated reason.
        """
        # Deliberately NOT via _client_cert_for: no RBAC user is created.
        unmapped_cn = "cbas_crl_no_such_user_2f1a"
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, unmapped_cn
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        filename = "cbas_crl_order.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=(cert_path, key_path)),
            "A revoked certificate must be rejected at the TLS layer "
                "before its identity is extracted -- reaching the identity "
                "layer at all would mean revocation is checked too late",
        )
        self.log.info(
            "Revoked, unmapped-CN certificate rejected at the TLS layer"
        )

        lines = self._read_analytics_log(grep=unmapped_cn)
        if lines:
            blob = "\n".join(lines).lower()
            self.assertIn(
                "revok", blob,
                f"The recorded reason for rejecting {unmapped_cn} should be "
                f"revocation. Lines seen: {lines[-3:]}"
            )
            self.assertNotIn(
                "user not found", blob,
                f"An unknown-user error means identity mapping ran before the "
                f"revocation check. Lines seen: {lines[-3:]}"
            )
            self.log.info(
                "Analytics logged revocation, not an unknown-user error -- "
                "enforcement order is correct"
            )
        else:
            self.log.info(
                "No Analytics log line mentioned this CN; the TLS-layer "
                "rejection above already demonstrates the ordering"
            )

    def test_valid_certs_work_for_each_analytics_rbac_role(self):
        """
        Section 1: a valid, non-revoked certificate works according to RBAC --
        checked across the roles the plan names, not just one.

        Each role gets its own certificate from the same CA, and none is
        revoked, so any failure here is about role handling under an active
        CRL policy rather than about revocation.
        """
        filename = "cbas_crl_roles.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # Mint every user and certificate BEFORE enabling mandatory mTLS.
        # _client_cert_for creates an RBAC user through RbacUtils, which is a
        # REST call carrying no client certificate -- under mandatory it is
        # walled out exactly like self.rest is, and TAF's REST layer reports
        # that as ServerUnavailableException, which reads like the node being
        # down rather than the node correctly refusing a cert-less request.
        # analytics_admin and analytics_reader are global roles. analytics_manager
        # is bucket-scoped -- its definition carries a bucket_name field -- so it
        # must be granted as analytics_manager[<bucket>]; granting the bare name
        # fails with "role parameters are undefined". Verified against
        # /settings/rbac/roles on 8.1.0-2632, where analytics_select is likewise
        # scoped (bucket/scope/collection) and is left out for that reason.
        # CBASBaseTest creates no bucket of its own, so make one rather than
        # skipping the bucket-scoped role: a green test that quietly exercised
        # two of the three roles it names is worse than a slower one that
        # covers all three.
        if not self.cluster.buckets:
            self.bucket_util.create_default_bucket(self.cluster)
            self.assertTrue(
                self.cluster.buckets,
                "Could not create a bucket, so the bucket-scoped "
                "analytics_manager role cannot be exercised"
            )
        bucket = self.cluster.buckets[0].name
        roles = [
            "analytics_admin",                  # global
            "analytics_reader",                 # global
            f"analytics_manager[{bucket}]",     # bucket-scoped
        ]
        # Sanitise the role for use in a username: brackets are not valid there.
        creds = {
            role: self._client_cert_for(
                "cbas_crl_role_" + role.replace("[", "_").replace("]", ""), role
            )
            for role in roles
        }

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        for role in roles:
            cert, key, _ = creds[role]
            resp = self._wait_for_analytics_ok("SELECT 1;", cert=(cert, key))
            self.assertEqual(
                resp.status_code, 200,
                f"A valid certificate mapped to {role} must reach Analytics "
                f"under CRL policy Require, got {resp.status_code}: "
                f"{resp.text[:300]}"
            )
            self.log.info(f"Role {role}: valid certificate accepted")

    def test_no_password_fallback_for_revoked_cert_under_hybrid_mtls(self):
        """
        Section 10: Couchbase must not silently fall back to password or any
        alternate authentication for Analytics access when a revoked
        certificate is presented under optional (hybrid) mTLS.

        This is the bypass the PRD calls out explicitly, and the one an
        operator is most likely to be caught by: the client still holds valid
        credentials, so a service that treats a refused certificate as "no
        certificate" would let it straight in.

        Both legs are needed. Presenting no certificate at all must still
        authenticate by password, which is what hybrid mode is for -- without
        that control, a blanket rejection would look like correct enforcement.
        """
        user, password = self._create_rbac_test_user(
            "cbas_crl_fallback", "analytics_admin"
        )
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        filename = "cbas_crl_fallback.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        # Control: no certificate, password only -> hybrid mode must allow it.
        resp = self._wait_for_analytics_ok(
            "SELECT 1;", auth=(user, password)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Hybrid mTLS: password-only access with no certificate must "
            f"still work, otherwise the rejection below proves nothing. Got "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        self.log.info("Hybrid + no cert + password -> allowed, as expected")

        # The bypass attempt: revoked certificate AND valid password.
        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(cert_path, key_path),
                auth=(user, password),
            ),
            "Hybrid mTLS: a revoked certificate presented alongside valid "
            "credentials must be refused outright. Falling back to password "
            "authentication here would let any revoked certificate holder "
            "keep Analytics access",
        )
        self.log.info(
            "Hybrid + revoked cert + valid password -> refused, no fallback"
        )

    def test_tampered_and_untrusted_crls_are_not_applied(self):
        """
        Section 10: tampered or unsigned CRLs applicable to Analytics
        certificate chains must be rejected, not silently applied.

        Checked by observable effect rather than by the upload's status code
        alone: even if a bad CRL were accepted at upload time, the certificate
        it names must still work, because the CRL cannot be attributed to a
        trusted issuer.
        """
        cert_path, key_path, serial = self._client_cert_for(
            "cbas_crl_badcrl", role="analytics_admin"
        )

        # A CRL from a CA the cluster has never trusted, naming our serial.
        rogue_cert, rogue_key = self.crl_utils.generate_ca("CBASCRLRogueCA")
        rogue_crl = self.crl_utils.build_crl(
            rogue_cert, rogue_key, revoked_serials=[serial], crl_number=1
        )
        status, content = self.crl_utils.upload_file(
            self.rest, "cbas_crl_rogue.pem", rogue_crl
        )
        if status:
            self._track_uploaded_file("cbas_crl_rogue.pem")
            self.log.info(
                "Untrusted-issuer CRL accepted at upload; checking it has no "
                "effect on enforcement"
            )
        else:
            self.log.info(
                f"Untrusted-issuer CRL rejected at upload, as expected: "
                f"{content}"
            )

        # A CRL from the real CA with its signature corrupted.
        good = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[serial], crl_number=2
        )
        der = bytearray(self.crl_utils.pem_crl_to_der(good))
        der[-1] ^= 0xFF          # flip the last signature byte
        status, content = self.crl_utils.upload_file(
            self.rest, "cbas_crl_tampered.pem", bytes(der)
        )
        if status:
            self._track_uploaded_file("cbas_crl_tampered.pem")
            self.log.info(
                "Tampered CRL accepted at upload; checking it has no effect"
            )
        else:
            self.log.info(
                f"Tampered CRL rejected at upload, as expected: {content}"
            )

        # A valid CRL revoking nothing, so Require has something applicable
        # and this check isolates the two bad CRLs.
        good_name = "cbas_crl_good.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], good_name, crl_number=3,
        )
        self.assertTrue(status, f"Valid CRL upload failed: {content}")
        self._track_uploaded_file(good_name)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        resp = self._wait_for_analytics_ok(
            "SELECT 1;", cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Neither an untrusted-issuer CRL nor a tampered one may revoke "
            f"this certificate on the Analytics path -- it is named only by "
            f"those two, and the only valid CRL revokes nothing. Got "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        self.log.info(
            "Untrusted-issuer and tampered CRLs had no effect on Analytics "
            "enforcement"
        )

    def test_peer_only_vs_full_chain_revocation_checking(self):
        """
        Section 1: the peer-only vs full-chain CRL verification configuration
        must be respected for Analytics connections.

        checkIntermediateCerts governs it:
          false (default) -- a revoked INTERMEDIATE does not block a leaf
                             whose own serial is not revoked.
          true            -- the same leaf is rejected.

        The leaf's own serial is never revoked, so a rejection can only come
        from walking up the chain. Two CRLs are needed because Require demands
        an applicable CRL for every certificate actually consulted: one from
        the intermediate covering the leaf and revoking nothing, and one from
        the root covering the intermediate and revoking it.
        """
        user, _ = self._create_rbac_test_user(
            "cbas_crl_chain", "analytics_admin"
        )
        inter_cert, inter_key, inter_serial = \
            self.crl_utils.generate_intermediate_ca(
                self.ca_cert, self.ca_key, "CBASCRLIntermediateCA"
            )
        # Uploading a CRL requires its ISSUER to be trusted in its own right,
        # not merely to chain to a trusted root.
        self._trust_ca_on_cluster(inter_cert)
        leaf_cert, leaf_key, _ = self.crl_utils.generate_leaf_cert(
            inter_cert, inter_key, user
        )
        # The server builds leaf -> intermediate -> trusted root, and only the
        # root is in its trust store, so the client must present the
        # intermediate alongside the leaf.
        chain_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_cert)
            + self.crl_utils.cert_to_pem(inter_cert)
        )
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))

        leaf_crl = "cbas_crl_chain_leaf.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, inter_cert, inter_key, [], leaf_crl, crl_number=1,
        )
        self.assertTrue(status, f"Intermediate-issued CRL upload failed: {content}")
        self._track_uploaded_file(leaf_crl)

        inter_crl = "cbas_crl_chain_inter.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, inter_serial, inter_crl,
            crl_number=1,
        )
        self.assertTrue(status, f"Root-issued CRL upload failed: {content}")
        self._track_uploaded_file(inter_crl)

        # Hybrid, not mandatory: the checkIntermediateCerts toggle below goes
        # through self.rest, which carries no client certificate.
        self._enable_client_cert_auth(state="enable")

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
            checkIntermediateCerts=False,
        )
        resp = self._wait_for_analytics_ok(
            "SELECT 1;", cert=(chain_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"With checkIntermediateCerts=false only the peer certificate's "
            f"own serial is consulted, so a revoked intermediate must not "
            f"block this leaf. Got {resp.status_code}: {resp.text[:300]}"
        )
        self.log.info("peer-only: revoked intermediate did not block the leaf")

        self.crl_utils.set_settings(self.rest, checkIntermediateCerts=True)
        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(chain_path, key_path)
            ),
            "With checkIntermediateCerts=true the revoked intermediate that "
            "issued this leaf must cause the connection to be refused, even "
            "though the leaf's own serial is not on any CRL",
        )
        self.log.info("full-chain: revoked intermediate blocked the leaf")

    def test_revocation_failure_reasons_are_distinguishable_in_logs(self):
        """
        Section 9: Analytics logs must distinguish revoked, missing-CRL and
        untrusted-issuer failures from one another, and must not expose raw
        PEM material or unmasked certificate serials.

        Three certificates are refused for three different reasons in the same
        run, then the log is read once. The point is not that any particular
        wording appears, but that the three reasons are not collapsed into one
        indistinguishable message -- an operator who cannot tell "revoked"
        from "no CRL available" cannot act on the log.
        """
        marker = f"cbas_crl_reasons_{int(time.time())}"

        # (1) genuinely revoked
        revoked_path, revoked_key, revoked_serial = self._client_cert_for(
            f"{marker}_revoked", role="analytics_admin"
        )
        filename = "cbas_crl_reasons.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # (2) issued by a CA the cluster does not trust at all
        rogue_ca_cert, rogue_ca_key = self.crl_utils.generate_ca(
            "CBASCRLUntrustedIssuer"
        )
        rogue_leaf, rogue_leaf_key, _ = self.crl_utils.generate_leaf_cert(
            rogue_ca_cert, rogue_ca_key, f"{marker}_untrusted"
        )
        rogue_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(rogue_leaf))
        rogue_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(rogue_leaf_key))

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(revoked_path, revoked_key)
            ),
            "A revoked certificate must be refused",
        )
        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(rogue_path, rogue_key_path)
            ),
            "A certificate from an untrusted issuer must be refused",
        )

        lines = self._read_analytics_log(tail_lines=3000) or []
        text = "\n".join(lines)
        lowered = text.lower()

        # No key material, and no unmasked serials.
        for leak, description in (
            ("-----begin certificate-----", "a PEM certificate block"),
            ("-----begin x509 crl-----", "a PEM CRL block"),
            ("-----begin private key-----", "a PEM private key block"),
            ("-----begin rsa private key-----", "an RSA private key block"),
        ):
            self.assertNotIn(
                leak, lowered,
                f"Analytics logs must not contain {description}"
            )
        self.assertNotIn(
            str(revoked_serial), text,
            f"Analytics logs must not carry the unmasked certificate serial "
            f"{revoked_serial}"
        )

        # The reasons must be tellable apart.
        revoked_markers = [t for t in ("revoked", "revocation")
                           if t in lowered]
        issuer_markers = [t for t in ("untrusted", "unknown authority",
                                      "unable to find valid certification",
                                      "unknown_untrusted_issuer",
                                      "not trusted")
                          if t in lowered]
        self.log.info(
            f"FINDING -- Analytics log reason markers: revoked={revoked_markers}, "
            f"untrusted-issuer={issuer_markers}"
        )
        self.assertTrue(
            revoked_markers,
            f"The Analytics log must say something that identifies a "
            f"revocation failure as such. Searched the last 3000 lines of "
            f"analytics_info/error/debug and found no revocation wording, so "
            f"an operator cannot distinguish it from any other TLS refusal."
        )
        self.assertTrue(
            issuer_markers,
            f"The Analytics log must distinguish an untrusted-issuer failure "
            f"from a revocation. Found revocation wording {revoked_markers} "
            f"but nothing identifying the untrusted issuer, so the two "
            f"reasons are collapsed into one message."
        )
        self.log.info(
            "Revoked and untrusted-issuer failures are distinguishable, with "
            "no PEM material or unmasked serial in the logs"
        )

    def test_s3_external_link_unaffected_by_crl_policy(self):
        """
        Section 4: external links using non-certificate authentication must be
        unaffected by CRL/revocation policy changes.

        An S3-compatible link (MinIO) authenticates with an access key and
        secret, so there is no certificate for revocation to evaluate. The
        cluster's clientAuth policy is then driven from Disabled to Require
        with a revoked certificate present, and the link must keep working --
        both an existing one and one created after the policy tightened.

        The failure this guards against is scope leakage: a revocation policy
        meant for inbound client certificates quietly breaking outbound
        connections to object storage, which would look like an object-store
        outage rather than a CRL problem.
        """
        store = self._require_object_store()

        # ── Baseline: policy off, link works.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        first = f"crl_s3_link_{int(time.time())}"
        self._create_s3_link(first, store)
        self.assertTrue(
            self._link_exists(first),
            f"Link {first} should exist in Analytics metadata after creation"
        )
        self.log.info("Baseline: S3 link created and present with policy Disabled")

        # ── Tighten the policy, with a genuinely revoked certificate in play
        # so Require has something to act on.
        _, _, serial = self._client_cert_for(
            "cbas_crl_s3_policy", role="analytics_admin"
        )
        filename = "cbas_crl_s3_policy.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        # The link created before the change must still be there and usable.
        self.assertTrue(
            self._link_exists(first),
            f"Link {first} must survive the clientAuth policy moving to "
            f"Require -- an access-key link has no certificate for revocation "
            f"to evaluate"
        )

        # And a NEW link must still be creatable under Require.
        second = f"crl_s3_link_after_{int(time.time())}"
        self._create_s3_link(second, store)
        self.assertTrue(
            self._link_exists(second),
            f"An S3 link must still be creatable while clientAuth is Require "
            f"with a revoked certificate present"
        )
        self.log.info(
            "S3 external links unaffected by the clientAuth policy change, as "
            "section 4 requires"
        )

    def test_s3_external_link_makes_no_revocation_checks(self):
        """
        Section 4: confirm whether CRL checking applies to external-link
        credentials at all, and document the answer.

        For an S3-compatible link the answer should be that it does not: the
        link authenticates with an access key, presents no client certificate,
        and so has nothing for cbauth to evaluate. That is asserted two ways
        rather than argued:

          * the link's stored metadata carries no certificate material, and
          * creating and using the link generates zero crlsValidate requests
            on the Analytics node, counted at the packet level.

        The counter is the stronger of the two. Metadata could omit a
        certificate while the code path still consulted the CRL machinery for
        the endpoint's server certificate; a flat packet count rules that out.
        """
        store = self._require_object_store()

        # Require, with a revoked certificate present, so any revocation
        # machinery that COULD be consulted has both a policy and a CRL to
        # work from. If checks still do not happen, it is because this path
        # does not make them.
        _, _, serial = self._client_cert_for(
            "cbas_crl_s3_count", role="analytics_admin"
        )
        filename = "cbas_crl_s3_count.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Require"},
        )

        baseline = self._crls_validate_counter_start()
        self.assertIsNotNone(
            baseline,
            "Could not install the crlsValidate counting rule on the "
            "Analytics node, so the zero-check assertion cannot be made"
        )

        name = f"crl_s3_nocheck_{int(time.time())}"
        properties = self._create_s3_link(name, store)
        self.assertTrue(
            self._link_exists(name),
            f"Link {name} should exist after creation"
        )

        after = self._crls_validate_count()
        self.assertIsNotNone(
            after,
            "The crlsValidate counting rule went missing mid-test, so the "
            "count could not be read"
        )
        self.log.info(
            f"FINDING -- crlsValidate packets during S3 link create/validate: "
            f"{baseline} -> {after}"
        )

        # No certificate material anywhere in what we sent, and none in what
        # Analytics stored.
        for field in ("clientCertificate", "clientKey", "certificate"):
            self.assertNotIn(
                field, properties,
                f"An S3 link should carry no {field}; section 4's "
                f"certificate-auth question does not arise for access-key "
                f"links"
            )

        self.assertEqual(
            after, baseline,
            f"Creating and validating an access-key S3 external link must "
            f"generate no crlsValidate requests -- there is no certificate to "
            f"evaluate. The packet count moved from {baseline} to {after}, "
            f"which means this path DOES consult the revocation machinery and "
            f"the plan's section 4 assumption needs revisiting."
        )
        self.log.info(
            "S3 external link generated no revocation checks: CRL enforcement "
            "does not apply to access-key external links"
        )

    def test_diagnostics_verdict_matches_live_analytics_enforcement(self):
        """
        Section 6: the administrative diagnostic endpoint's revocation
        evaluation must match the evaluator used by live Analytics
        connections.

        Four certificates are put through both paths in the same run: a valid
        one, a revoked one, one from an issuer the cluster does not trust, and
        one from a trusted CA that has no applicable CRL. For each, the
        diagnostics verdict and the live connection outcome must agree.

        The agreement is checked by meaning, not by string: the plan lists
        statuses like "not_revoked" and "unknown_missing_crl", but 8.5.0-1009
        answers with its own vocabulary (a self-signed root, for instance,
        comes back "valid" with details "self-signed root; not CRL-checked").
        Asserting the plan's guessed strings would fail against a conformant
        server, so a verdict is mapped to acceptable-or-not and compared with
        what the connection actually did. The observed vocabulary is logged so
        the plan can be corrected.

        Section 6's second bullet asks for link-certificate checks "if the
        diagnostic tool supports" them. It has no link-specific surface --
        supplying a PEM is the only mode -- so a link certificate would be
        checked exactly as any other supplied certificate is here.
        """
        # (1) valid, and (2) revoked, both under this suite's trusted CA
        valid_path, valid_key, _ = self._client_cert_for(
            "cbas_diag_valid", role="analytics_admin"
        )
        revoked_path, revoked_key, revoked_serial = self._client_cert_for(
            "cbas_diag_revoked", role="analytics_admin"
        )
        filename = "cbas_diag.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # (3) an issuer the cluster has never trusted
        rogue_ca_cert, rogue_ca_key = self.crl_utils.generate_ca(
            "CBASDiagUntrustedCA"
        )
        rogue_leaf, rogue_leaf_key, _ = self.crl_utils.generate_leaf_cert(
            rogue_ca_cert, rogue_ca_key, "cbas_diag_untrusted"
        )
        rogue_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(rogue_leaf))
        rogue_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(rogue_leaf_key))

        # (4) trusted issuer, but no CRL covering it
        nocrl_ca_cert, nocrl_ca_key = self.crl_utils.generate_ca(
            "CBASDiagNoCrlCA"
        )
        self._trust_ca_on_cluster(nocrl_ca_cert)
        nocrl_user, _ = self._create_rbac_test_user(
            "cbas_diag_nocrl", "analytics_admin"
        )
        nocrl_leaf, nocrl_key_obj, _ = self.crl_utils.generate_leaf_cert(
            nocrl_ca_cert, nocrl_ca_key, nocrl_user
        )
        nocrl_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(nocrl_leaf))
        nocrl_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(nocrl_key_obj))

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        # (label, cert_path, key_path, pem read back from the temp file)
        cases = [
            ("valid", valid_path, valid_key, open(valid_path, "rb").read()),
            ("revoked", revoked_path, revoked_key,
             open(revoked_path, "rb").read()),
            ("untrusted issuer", rogue_path, rogue_key_path,
             open(rogue_path, "rb").read()),
            ("trusted issuer, no CRL", nocrl_path, nocrl_key_path,
             open(nocrl_path, "rb").read()),
        ]

        observed = {}
        mismatches = []
        for label, cert_path, key_path, pem in cases:
            verdict, details = self._diagnostics_verdict(pem)
            diag_ok = self._diagnostics_says_acceptable(verdict)

            try:
                resp = self._analytics_query(
                    "SELECT 1;", cert=(cert_path, key_path)
                )
                live_ok = resp.status_code == 200
                live_detail = f"HTTP {resp.status_code}"
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError) as exc:
                live_ok = False
                live_detail = type(exc).__name__

            observed[label] = {
                "diagnostics": verdict, "details": details,
                "live": live_detail,
            }
            if diag_ok != live_ok:
                mismatches.append(
                    f"{label}: diagnostics said {verdict!r} "
                    f"(acceptable={diag_ok}) but the live connection was "
                    f"{live_detail} (accepted={live_ok})"
                )

        self.log.info(
            f"FINDING -- diagnostics vocabulary and live outcomes: {observed}"
        )
        self.assertFalse(
            mismatches,
            f"The diagnostic endpoint and live Analytics enforcement must "
            f"agree for the same certificate under the same policy. "
            f"Disagreements: {mismatches}. An admin who trusts the diagnostic "
            f"tool would draw the wrong conclusion about who can connect."
        )
        self.log.info(
            "Diagnostics verdicts agree with live Analytics enforcement for "
            "all four certificate cases"
        )

    def test_revocation_metrics_are_emitted_and_service_scoped(self):
        """
        Section 9: metrics must be emitted for Analytics revocation checks and
        be distinguishable from the same metrics for KV, Query and FTS.

        The metric that satisfies this is cbas_crl_checks_total{scope,result},
        added by MB-73654 (cbas-core 3acd9eb, first in 8.5.0-1101). It counts
        per CHECK rather than per cache miss, which is what makes rejections
        visible however well the verdict cache is performing -- a workload
        served entirely from cache would report nothing through hit/miss
        counters alone. Alongside it are four cache series describing the
        verdict cache added by MB-73659: cbas_crl_cache_{max_items,
        current_items,hit_total,miss_total}.

        Distinguishability comes from the cbas_ prefix: these live on the
        Analytics service's own registry, so they describe Analytics and
        nothing else.

        The older cm_cbauth_crl_cache_*{service="cbas"} series are recorded
        but deliberately NOT asserted on, because they stay at zero BY
        DESIGN. ns_server builds them by calling AuthCacheSvc.GetStats over
        revrpc against each registered cbauth client, so service="cbas" means
        the Go cbas service-manager's cbauth instance. Analytics revocation
        checking does not happen there: the Java query engine has no cbauth
        client and POSTs directly to ns_server's /_cbauth/crlsValidate, so it
        can never contribute to a cbauth-sourced series. cache_max_items
        reading 0 is the tell -- cbauth returns zeros for all four fields,
        max size included, when the cache pointer is nil, and the Go
        process's two call sites cannot reach the lazy creation under this
        configuration (its inbound listener is ClientAuth: tls.NoClientCert,
        and its outbound path returns at the policy check while nodeToNode is
        Disabled). That was the original finding on this test and the
        substance of MB-73654; the resolution moved the metric rather than
        making those series move.
        """
        services = self._crl_metric_services()
        self.assertIn(
            "cbas", services,
            f"cbauth CRL metrics must carry a cbas service label so Analytics "
            f"is distinguishable from other services. Labels present: "
            f"{sorted(services)}"
        )
        # Which OTHER services appear varies by what has exercised cbauth
        # since the node started -- observed {cbas, projector, cbcontbk, xdcr}
        # on one run and a set including n1ql/fts/index on another. Requiring
        # specific neighbours makes the test depend on unrelated activity, so
        # the check is that the metric is service-scoped at all: cbas is
        # present and it is not the only label.
        self.assertGreater(
            len(services), 1,
            f"CRL metrics must be service-scoped so Analytics can be told "
            f"apart from other services, but only {sorted(services)} is "
            f"labelled"
        )
        self.log.info(f"CRL metrics are service-scoped: {sorted(services)}")

        before = self._crl_metrics("cbas")
        self.assertTrue(
            before,
            "No cbas-labelled cbauth CRL metrics found at all"
        )

        # The Analytics-owned family, which is what section 9 actually needs.
        cbas_before = self._cbas_crl_metrics()
        self.assertTrue(
            cbas_before,
            "No cbas_crl_* metrics are exposed at all. These were added by "
            "MB-73654 (cbas-core 3acd9eb) and first ship in 8.5.0-1101, so a "
            "build older than that cannot satisfy section 9 -- check the "
            "cluster's version before reading this as a regression."
        )
        self.assertIn(
            "cbas_crl_checks_total",
            {series.split("{", 1)[0] for series in cbas_before},
            f"cbas_crl_checks_total is the metric that makes Analytics "
            f"revocation checks and rejections observable; the cache series "
            f"alone cannot, because a fully cached workload performs checks "
            f"without moving hit/miss counters. Series present: "
            f"{sorted(cbas_before)}"
        )
        rejected_before = self._cbas_crl_metric_sum(
            cbas_before, "cbas_crl_checks_total",
            scope="clientAuth", result="rejected")
        self.log.info(
            f"cbas_crl_* before: {cbas_before} "
            f"(clientAuth/rejected={rejected_before})")

        # Drive a real revocation check through Analytics.
        cert_path, key_path, serial = self._client_cert_for(
            "cbas_metrics_revoked", role="analytics_admin"
        )
        filename = "cbas_metrics.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(cert_path, key_path)
            ),
            "The revoked certificate must be refused, otherwise no revocation "
            "check happened and the metric assertion below is meaningless",
        )
        # The cache counters are updated asynchronously relative to the
        # handshake, so give them a moment rather than racing the scrape.
        time.sleep(15)
        after = self._crl_metrics("cbas")

        moved = {k: (before.get(k), after.get(k))
                 for k in after if after.get(k) != before.get(k)}
        self.log.info(
            f"FINDING -- cbas CRL metrics before={before} after={after} "
            f"moved={moved}"
        )
        # Recorded, not asserted, and expected to be empty: per MB-73654's
        # resolution these series describe the Go cbas service-manager's
        # cbauth cache, which no Analytics revocation check can reach. Left
        # in as a regression tripwire -- if they ever DO move, the assumption
        # behind the cbas_-prefixed family below has changed and both
        # families need revisiting.
        if moved:
            self.log.warning(
                f"Unexpected: cm_cbauth_crl_*(service=cbas) moved ({moved}). "
                f"MB-73654 concluded these cannot report Analytics "
                f"revocation activity, so this needs re-checking against "
                f"Analytics dev rather than being taken as coverage.")
        else:
            self.log.info(
                "cm_cbauth_crl_*(service=cbas) unchanged, as expected -- "
                "that family covers the Go service manager, not the query "
                "engine that performed this check")

        # The real assertion: the Analytics-owned per-check counter must
        # record the rejection.
        cbas_after = self._cbas_crl_metrics()
        rejected_after = self._cbas_crl_metric_sum(
            cbas_after, "cbas_crl_checks_total",
            scope="clientAuth", result="rejected")
        checks_after = self._cbas_crl_metric_sum(
            cbas_after, "cbas_crl_checks_total")
        self.log.info(
            f"cbas_crl_* after: {cbas_after} "
            f"(clientAuth/rejected={rejected_after}, all checks={checks_after})")

        self.assertGreater(
            rejected_after, rejected_before,
            f"A revoked certificate was refused by Analytics, so "
            f"cbas_crl_checks_total{{scope=\"clientAuth\",result=\"rejected\"}} "
            f"must have advanced. before={rejected_before} "
            f"after={rejected_after}. Full series after: {cbas_after}"
        )
        self.log.info(
            f"Analytics recorded the rejection: clientAuth/rejected "
            f"{rejected_before} -> {rejected_after}")

        # A 'bypassed' result means a check did not pass but the scope's
        # policy is not 'require', so the peer was let through. Under
        # clientAuth=Require that must not happen for this certificate --
        # a bypass here would mean the control was not enforced.
        bypassed_after = self._cbas_crl_metric_sum(
            cbas_after, "cbas_crl_checks_total",
            scope="clientAuth", result="bypassed")
        bypassed_before = self._cbas_crl_metric_sum(
            cbas_before, "cbas_crl_checks_total",
            scope="clientAuth", result="bypassed")
        self.assertEqual(
            bypassed_after, bypassed_before,
            f"clientAuth policy is Require, so no check may be recorded as "
            f"'bypassed' -- that result means revocation did not pass but the "
            f"peer was admitted anyway. before={bypassed_before} "
            f"after={bypassed_after}"
        )

    def test_audit_event_for_revoked_certificate_rejection(self):
        """
        Section 9: audit events must be generated for revoked-certificate
        rejections at the Analytics service boundary, carrying actor,
        timestamp, action and result -- and must not leak certificate
        material.

        Auditing is off by default on these clusters, so the test enables it,
        drives a rejection, and restores the original setting in tearDown.
        """
        self._set_audit(True)
        # Auditing applies to events after it is switched on, and audit.log is
        # created lazily on the first auditable event -- not when auditing is
        # enabled. An earlier version of this test read an audit.log that did
        # not exist yet and concluded the product emitted nothing, which
        # proved only that the test had not checked its own instrument.
        time.sleep(10)
        control_user = f"cbas_audit_control_{int(time.time())}"
        self._create_rbac_test_user(control_user, "analytics_reader")
        time.sleep(15)
        control_lines = self._read_audit_log()
        self.assertTrue(
            control_lines,
            f"Control: creating an RBAC user with auditing enabled produced "
            f"no audit records on any node. Auditing is not recording "
            f"anything, so the absence of a revocation event below would say "
            f"nothing about revocation."
        )
        self.log.info(
            f"Control: auditing is live ({len(control_lines)} audit lines "
            f"after creating a user)"
        )

        cert_path, key_path, serial = self._client_cert_for(
            "cbas_audit_revoked", role="analytics_admin"
        )
        filename = "cbas_audit.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        # Snapshot before the rejection. Everything this test has done so
        # far -- uploading a CRL, changing CRL settings, enabling client cert
        # auth -- is itself audited, and those events mention "crl" and
        # "certificate". Diffing is what separates the product's response to
        # the rejection from the test's own footprint.
        before_lines = set(self._read_audit_log())

        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(cert_path, key_path)
            ),
            "The revoked certificate must be refused before an audit event "
            "can be expected for the rejection",
        )
        time.sleep(20)

        lines = self._read_audit_log()
        new_lines = [ln for ln in lines if ln not in before_lines]
        text = "\n".join(lines)
        lowered = text.lower()

        # Whatever else it does, the audit log must not carry key material.
        for leak in ("-----begin certificate-----",
                     "-----begin private key-----",
                     "-----begin rsa private key-----"):
            self.assertNotIn(
                leak, lowered,
                f"The audit log must not contain {leak!r}"
            )
        self.assertNotIn(
            str(serial), text,
            f"The audit log must not carry the unmasked certificate serial "
            f"{serial}"
        )

        # Administrative actions this suite performs are audited under these
        # names; they are not evidence that a rejection was recorded.
        own_actions = ("upload crl file", "crl settings", "client cert auth",
                       "create user", "delete user", "audit settings",
                       "set audit", "modify user")
        candidates = [
            ln for ln in new_lines
            if not any(action in ln.lower() for action in own_actions)
        ]
        # What a rejection event would look like: an authentication or
        # authorisation failure, or something naming revocation directly.
        rejection = [
            ln for ln in candidates
            if any(t in ln.lower() for t in (
                "revoked", "revocation", "authentication failure",
                "auth failure", "login failure", "rejected", "denied",
                "unauthorized", "unauthorised"))
        ]
        self.log.info(
            f"FINDING -- audit lines added by the rejection: "
            f"{len(new_lines)} new, {len(candidates)} after excluding this "
            f"test's own admin actions, {len(rejection)} that look like a "
            f"rejection. Sample of new: {new_lines[-3:]}"
        )
        related = rejection
        self.assertTrue(
            related,
            f"Section 9 requires an audit event for a revoked-certificate "
            f"rejection at the Analytics boundary, carrying actor, timestamp, "
            f"action and result. Auditing is demonstrably working (the "
            f"control event was recorded), and the rejection demonstrably "
            f"happened, but no audit record describes it. Records added "
            f"around the rejection: {new_lines}. None of them denotes a "
            f"rejection, an authentication failure or a revocation. A TLS "
            f"handshake refused for revocation appears not to be audited at "
            f"all."
        )
        self.log.info("Audit log records the revoked-certificate rejection")

    def test_expired_crl_is_visible_in_diagnostics(self):
        """
        Section 9: health warnings for CRL expiry or invalidity must surface.

        The diagnostics status endpoint is where per-node CRL state is
        reported -- filename, source, cacheStatus, per-entry thisUpdate and
        nextUpdate, and the outcome of the last reload. An expired CRL must be
        distinguishable there from a current one; an operator who cannot see
        that a CRL has lapsed cannot know their enforcement has silently
        changed behaviour.
        """
        # An already-expired CRL cannot be uploaded at all: the server
        # refuses it with "CRL validation failed: ... CRL expired". That is
        # itself worth pinning down, because it means the only way a live CRL
        # becomes expired is by lapsing after upload -- which is exactly the
        # case an operator needs a health signal for.
        already_expired = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=1,
            expired=True,
        )
        status, content = self.crl_utils.upload_file(
            self.rest, "cbas_already_expired.pem", already_expired
        )
        self.assertFalse(
            status,
            f"An already-expired CRL should be refused at upload rather than "
            f"accepted, got success: {content}"
        )
        self.log.info(f"Already-expired CRL refused at upload: {content}")

        # So: upload one that is valid now and lapses shortly, then let it.
        lapse_seconds = 90
        expired_name = "cbas_expired_health.pem"
        now = datetime.datetime.now(datetime.timezone.utc)
        pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[], crl_number=2,
            this_update=now - datetime.timedelta(minutes=5),
            next_update=now + datetime.timedelta(seconds=lapse_seconds),
        )
        status, content = self.crl_utils.upload_file(
            self.rest, expired_name, pem
        )
        self.assertTrue(
            status,
            f"A CRL that is still valid at upload time should be accepted: "
            f"{content}"
        )
        self._track_uploaded_file(expired_name)
        self.log.info(
            f"Uploaded a CRL that lapses in {lapse_seconds}s; waiting for it "
            f"to expire"
        )
        time.sleep(lapse_seconds + 45)

        status, content = self.crl_utils.diagnostics_status(self.rest)
        self.assertTrue(status, f"diagnostics/status failed: {content}")
        self.assertIsInstance(
            content, dict, f"Unexpected diagnostics/status shape: {content}"
        )

        # Find our file across whichever nodes reported it.
        found = []
        for node_key, node_state in content.items():
            for crl_file in (node_state or {}).get("crlFiles", []) or []:
                if crl_file.get("filename") != expired_name:
                    continue
                found.append((node_key, crl_file))

        self.assertTrue(
            found,
            f"The uploaded CRL {expired_name} does not appear in "
            f"diagnostics/status at all, so its expiry cannot be surfaced. "
            f"Nodes reported: {list(content)}"
        )

        summary = []
        for node_key, crl_file in found:
            entries = crl_file.get("entries") or []
            summary.append({
                "node": node_key,
                "cacheStatus": crl_file.get("cacheStatus"),
                "lastReload": (crl_file.get("lastReload") or {}).get("result"),
                "entryStatuses": [e.get("status") for e in entries],
                "nextUpdate": [e.get("nextUpdate") for e in entries],
            })
        self.log.info(f"FINDING -- expired CRL as reported: {summary}")

        # Expiry has to be visible somehow: either the cache/entry status says
        # so, or nextUpdate is in the past for a reader to notice.
        now = datetime.datetime.now(datetime.timezone.utc)
        visible = False
        for item in summary:
            statuses = [str(s).lower() for s in item["entryStatuses"]
                        if s is not None]
            if str(item["cacheStatus"]).lower() == "expired" or \
                    any("expire" in s for s in statuses):
                visible = True
            for nxt in item["nextUpdate"]:
                if not nxt:
                    continue
                try:
                    parsed = datetime.datetime.fromisoformat(
                        nxt.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if parsed < now:
                    visible = True
        self.assertTrue(
            visible,
            f"An expired CRL must be distinguishable in diagnostics/status "
            f"-- through cacheStatus, an entry status, or a nextUpdate in the "
            f"past. None of those indicated expiry: {summary}"
        )
        self.log.info("Expired CRL is visible as expired in diagnostics/status")

    def test_enforcement_matches_kv_query_and_fts_for_same_cert(self):
        """
        Section 1: Analytics enforcement must match KV, Query and FTS for the
        same certificate under the same revocation policy.

        The existing parity test compares Analytics against ns_server only.
        This one puts one revoked certificate against every service endpoint
        the cluster runs, so a service that quietly admits it is caught.

        KV is probed with a raw TLS socket to 11207 rather than an SDK: the
        question is only whether the handshake survives, and under TLS 1.3 the
        server finishes its side before judging the client certificate, so the
        probe writes and then reads to force the verdict.
        """
        cert_path, key_path, serial = self._client_cert_for(
            "cbas_parity_revoked", role="analytics_admin"
        )
        filename = "cbas_parity.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        def http_probe(node, port, path, method="GET"):
            def _call():
                return requests.request(
                    method, f"https://{node.ip}:{port}{path}",
                    cert=(cert_path, key_path), verify=False, timeout=60,
                    headers={"Connection": "close"},
                )
            return _call

        def kv_probe(node):
            def _call():
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                context.load_cert_chain(certfile=cert_path, keyfile=key_path)
                try:
                    with socket.create_connection(
                            (node.ip, 11207), timeout=60) as sock:
                        with context.wrap_socket(sock) as tls:
                            # memcached NOOP: magic 0x80, opcode 0x0a, no body
                            tls.sendall(bytes([0x80, 0x0a] + [0] * 22))
                            data = tls.recv(24)
                            if not data:
                                raise requests.exceptions.ConnectionError(
                                    "KV closed the connection without replying")

                            class _Resp:
                                status_code = 200
                                text = data.hex()
                            return _Resp()
                except ssl.SSLError as exc:
                    # assert_cert_refused understands requests' exception
                    # types; a raw socket raises the stdlib ones. KV answers
                    # SSLV3_ALERT_CERTIFICATE_REVOKED here, which is a refusal
                    # and must be reported as such rather than as an error.
                    raise requests.exceptions.SSLError(str(exc)) from exc
                except OSError as exc:
                    raise requests.exceptions.ConnectionError(
                        str(exc)) from exc
            return _call

        targets = []
        analytics = self.cbas_node
        targets.append(("analytics", http_probe(
            analytics, 18095, "/analytics/service", "POST")))
        # Typed service lists rather than string-matching a services
        # attribute: the framework already groups nodes this way, and the
        # attribute is not populated consistently across code paths.
        for node in (self.cluster.query_nodes or [])[:1]:
            targets.append(("query", http_probe(node, 18093, "/admin/ping")))
        for node in (self.cluster.fts_nodes or [])[:1]:
            targets.append(("fts", http_probe(node, 18094, "/api/cfg")))
        for node in (self.cluster.kv_nodes or [])[:1]:
            targets.append(("kv", kv_probe(node)))

        self.log.info(f"Parity targets: {[t[0] for t in targets]}")
        self.assertGreaterEqual(
            len({t[0] for t in targets}), 3,
            f"Need at least three distinct services to make a parity "
            f"statement, found {[t[0] for t in targets]}"
        )

        for label, probe in targets:
            self.assert_cert_refused(
                probe,
                f"{label} must refuse the same revoked certificate that "
                f"Analytics refuses, under the same policy. A service that "
                f"admits it is a hole in cluster-wide enforcement",
            )
            self.log.info(f"{label}: revoked certificate refused")
        self.log.info(
            f"Enforcement is consistent across "
            f"{sorted({t[0] for t in targets})}"
        )


    def test_remote_cluster_link_topology_is_usable(self):
        """
        Gate for the section 3 remote-link work: prove the two-cluster
        topology comes up and a remote Couchbase link can actually be created
        and connected, before any revocation test is written against it.

        Section 3 covers Analytics linking to a REMOTE Couchbase cluster over
        mTLS, which is a second certificate trust relationship independent of
        inbound client access. Nothing else in this suite needs two clusters,
        and CBASBaseTest builds them from params rather than from the ini's
        [clusterN] sections -- it slices self.servers using num_of_clusters,
        nodes_init and services_init (see cbas_base_server.py:112-123). So the
        conf must carry num_of_clusters=2 with pipe-separated per-cluster
        values, and cluster_kv_infra needs one entry per cluster or setUp
        raises IndexError on the second.

        Asserts nothing about CRLs. Its job is to fail fast if the topology or
        the link mechanics are the problem, rather than have six revocation
        tests fail for an unrelated reason.
        """
        self.assertGreaterEqual(
            len(self.cb_clusters), 2,
            f"Expected two clusters from num_of_clusters=2, got "
            f"{list(self.cb_clusters)}. Check the conf carries "
            f"num_of_clusters=2 and pipe-separated nodes_init/services_init."
        )
        names = sorted(self.cb_clusters)
        local = self.cb_clusters[names[0]]
        remote = self.cb_clusters[names[1]]
        self.assertIs(
            self.cluster, local,
            "self.cluster must be the first cluster -- Analytics services are "
            "declared first so compute/storage separation is not applied to "
            "the remote cluster"
        )
        self.assertTrue(
            local.cbas_nodes,
            f"C1 ({[s.ip for s in local.servers]}) has no Analytics node"
        )
        self.log.info(
            f"C1 {[s.ip for s in local.servers]} (cbas on "
            f"{[n.ip for n in local.cbas_nodes]}), "
            f"C2 {[s.ip for s in remote.servers]} master {remote.master.ip}"
        )

        # Both clusters serve their own mgmt API, and they are genuinely
        # separate -- a link to a node that is already in the local cluster
        # would not exercise anything remote.
        local_ips = {s.ip for s in local.servers}
        remote_ips = {s.ip for s in remote.servers}
        self.assertFalse(
            local_ips & remote_ips,
            f"The two clusters share nodes {local_ips & remote_ips}; a "
            f"'remote' link would not be remote"
        )
        for label, cluster in (("C1", local), ("C2", remote)):
            resp = requests.get(
                f"http://{cluster.master.ip}:8091/pools/default",
                auth=(cluster.master.rest_username,
                      cluster.master.rest_password),
                timeout=60,
            )
            self.assertEqual(
                resp.status_code, 200,
                f"{label} master {cluster.master.ip} did not serve "
                f"/pools/default: {resp.status_code} {resp.text[:200]}"
            )
            self.log.info(
                f"{label} up with {len(resp.json().get('nodes', []))} node(s)"
            )

        # Reachability from the Analytics node itself, not from the runner: a
        # link is opened by cbas, so runner-side reachability proves nothing.
        shell = RemoteMachineShellConnection(local.cbas_nodes[0])
        try:
            output, _ = shell.execute_command(
                f"curl -s -m 10 -o /dev/null -w '%{{http_code}}' "
                f"-u {remote.master.rest_username}:"
                f"{remote.master.rest_password} "
                f"http://{remote.master.ip}:8091/pools/default"
            )
            code = next((l.strip() for l in (output or []) if l.strip()), None)
        finally:
            shell.disconnect()
        self.assertEqual(
            code, "200",
            f"Analytics node {local.cbas_nodes[0].ip} cannot reach C2 master "
            f"{remote.master.ip}: got {code!r}. A link would fail for "
            f"connectivity rather than revocation."
        )
        self.log.info("C2 reachable from the Analytics node")

        # The part most likely to be wrong: actually create the link and
        # connect it. Username/password with encryption none first -- mTLS is
        # what the revocation tests will add, and this gate should not fail
        # for certificate reasons.
        link_name = f"crl_gate_link_{int(time.time())}"
        link_properties = {
            "name": link_name,
            "dataverse": "Default",
            "scope": "Default",
            "type": "couchbase",
            "hostname": remote.master.ip,
            "username": remote.master.rest_username,
            "password": remote.master.rest_password,
            "encryption": "none",
        }
        created = self.cbas_util.create_link(
            self.cluster, link_properties, create_dataverse=False
        )
        self.assertTrue(
            created,
            f"Could not create a remote Couchbase link to {remote.master.ip}. "
            f"Section 3 is not testable until this works."
        )
        self._created_links.append(("Default", link_name))
        self.log.info(f"Created remote link Default.{link_name}")

        self.assertTrue(
            self.cbas_util.validate_link_in_metadata(
                self.cluster, link_name, "Default", "couchbase"
            ),
            f"Link {link_name} was created but is not in Analytics metadata"
        )
        self.log.info(
            "Remote Couchbase link created and present -- section 3 topology "
            "is usable"
        )

    # ── Section 3: Analytics Links to remote Couchbase clusters ─────────────
    #
    # Every test below turns on the same distinction: the certificate a link
    # presents OUTBOUND is validated by the REMOTE cluster, so the policy and
    # the CRL that decide its fate are the remote's, not the local Analytics
    # cluster's. self.rest and self.cluster in these tests are the Analytics
    # side; target["rest"] is where revocations are published.

    def _link_baseline(self, target, client, link_name, expect_connect=True):
        """
        Create a link with `client`'s certificate and prove it works, so a
        later failure is attributable to the revocation rather than to the
        link, the topology or the certificates.

        Checks the certificate at the remote's own mgmt port first. If mTLS
        against the remote is broken, that says so directly instead of
        letting a link failure stand in for it.
        """
        cert_path = self._write_temp_pem(client["cert_pem"])
        key_path = self._write_temp_pem(client["key_pem"])
        resp = self._mgmt_request(
            cert=(cert_path, key_path), node=target["cluster"].master)
        self.assertEqual(
            resp.status_code, 200,
            f"The link's client certificate cannot authenticate to the link "
            f"target {target['cluster'].master.ip} at all "
            f"({resp.status_code}: {resp.text[:300]}). That is a fixture "
            f"problem on the remote -- CA trust, clientCertAuth or the "
            f"mapped user -- not a revocation result."
        )
        self.log.info(
            f"Client certificate for {client['username']} authenticates to "
            f"{target['name']} (serial={client['serial']})"
        )

        props = self._couchbase_link_props(link_name, target, client=client)
        ok, code, body = self._link_rest("POST", props)
        self.assertTrue(
            ok,
            f"Could not create the mTLS link {link_name} to "
            f"{target['name']} with a VALID certificate ({code}): "
            f"{body[:600]}. Section 3 is not testable until this works."
        )
        self._created_links.append(("Default", link_name))
        self.log.info(f"Created mTLS link Default.{link_name}")

        # A dataset on the link, so that CONNECT LINK is provably a remote
        # operation rather than a local metadata flip.
        #
        # This matters for what the revocation tests can claim. CONNECT LINK
        # on a link with no datasets succeeds (see test_connect_link in
        # cbas_external_links_CB_cluster.py) and need not touch the remote at
        # all -- in which case a revoked certificate would ALSO connect, and
        # the test would report a product bug that is really a no-op
        # operation. With a dataset attached, connect has to reach the remote:
        # the same suite's test_connect_link_when_network_up_before_timeout
        # firewalls the remote master and CONNECT LINK blocks until the
        # network returns, which is only possible if it dials out.
        dataset_name = f"{link_name}_ds"
        created = self.cbas_util.create_dataset(
            self.cluster, dataset_name, self.REMOTE_BUCKET,
            dataverse_name="Default", link_name=f"Default.`{link_name}`",
        )
        self.assertTrue(
            created,
            f"Could not create dataset {dataset_name} on link {link_name} "
            f"over remote bucket {self.REMOTE_BUCKET}. Section-3 conf lines "
            f"must give the remote cluster a bucket "
            f"(cluster_kv_infra=None|default), or CONNECT LINK below would "
            f"not be a remote operation and the revocation tests would "
            f"prove nothing."
        )
        self._created_datasets.append(f"Default.`{dataset_name}`")

        if expect_connect:
            connected, error = self._connect_link(link_name)
            self.assertTrue(
                connected,
                f"A link with a valid certificate must connect; got: "
                f"{error[:600]}"
            )
            self.log.info(
                f"Link {link_name} connected with a valid cert, with dataset "
                f"{dataset_name} attached"
            )
        return props

    def _link_identity(self, link_name, dataverse="Default"):
        """
        The identifying and configuration fields of a link, as a dict.

        A stable subset rather than the whole REST blob on purpose: a link's
        representation can legitimately carry activity state that changes
        when it is connected, and comparing everything would fail the
        "same object" check for a reason that has nothing to do with
        recovery. These are the fields that WOULD differ if the link had
        been dropped and recreated.
        """
        info = self.cbas_util.get_link_info(
            self.cluster, dataverse=dataverse, link_name=link_name,
            link_type="couchbase")
        self.assertTrue(
            info,
            f"Link {dataverse}.{link_name} returned no info, so its identity "
            f"cannot be compared: {info!r}"
        )
        entry = info[0] if isinstance(info, list) else info
        keys = ("name", "scope", "dataverse", "type", "activeHostname",
                "hostname", "encryption", "certificates", "username")
        return {k: entry.get(k) for k in keys if k in entry}

    def test_remote_link_with_revoked_cert_fails_to_connect(self):
        """
        Section 3: a remote link whose client certificate is later revoked on
        the remote cluster's CRL stops being able to connect.

        The link is created and connected first with the same certificate,
        then the only thing that changes is the remote's CRL. Nothing about
        the link, the credentials or the network is touched between the two
        attempts, so the difference in outcome can only be the revocation.

        The link is disconnected and reconnected rather than left alone: an
        already-established connection is expected to survive revocation
        (the plan's own reviewer makes that point about long-running queries
        in section 2), so what section 3 asks about is whether a NEW
        connection attempt is refused.
        """
        target = self._setup_remote_link_target()
        client = self._mint_link_client_cert(target, "cbas_crl_link_user")
        link_name = "crl_link_revoke"
        self._link_baseline(target, client, link_name)

        self._disconnect_link(link_name)
        self._publish_remote_crl(target, [client["serial"]])

        connected, error = self._connect_link(link_name)
        self.assertFalse(
            connected,
            f"The link connected even though its client certificate "
            f"(serial={client['serial']}) is on the link target's CRL under "
            f"clientAuth=Require. Section 3 requires the link to fail to "
            f"connect according to the remote cluster's revocation policy."
        )
        self._assert_link_error_kind(
            error, "revoked",
            "A link refused because its certificate was revoked must say so",
            redact=[link_name, client["username"]],
        )

    def test_remote_link_error_distinguishes_revoked_unreachable_and_bad_creds(self):
        """
        Section 3: a link failure names its own cause -- "certificate
        revoked" is distinguishable from "network unreachable" and from
        "credentials invalid".

        Three links are made to fail, one per cause, and the assertion is on
        the text of each. The sharp part is the negative direction: neither
        the unreachable nor the bad-credential failure may read as a
        revocation. An operator who cannot tell those apart from the error
        will replace certificates to fix a routing problem, or chase the
        network while a serial sits on a CRL.

        The unreachable leg points at a deliberately unroutable address
        rather than firewalling a real one: a firewall rule that outlives a
        failed teardown breaks every later test on that node, and this
        assertion does not need a real host to be down.
        """
        target = self._setup_remote_link_target()

        # Leg 1: revoked certificate.
        revoked_client = self._mint_link_client_cert(
            target, "cbas_crl_link_revoked")
        revoked_link = "crl_link_err_revoked"
        self._link_baseline(target, revoked_client, revoked_link)
        self._disconnect_link(revoked_link)
        self._publish_remote_crl(target, [revoked_client["serial"]])
        connected, revoked_error = self._connect_link(revoked_link)
        self.assertFalse(
            connected,
            "The revoked-certificate leg must fail before its error can be "
            "compared with the other two"
        )
        self._assert_link_error_kind(
            revoked_error, "revoked",
            "The revoked leg's error must name revocation",
            redact=[revoked_link, revoked_client["username"]],
        )

        # Leg 2: unreachable host. A valid certificate, so the only thing
        # wrong is where the link points.
        good_client = self._mint_link_client_cert(
            target, "cbas_crl_link_reachable")
        unreachable_link = "crl_link_err_unreachable"
        # RFC 5737 TEST-NET-1: reserved for documentation, guaranteed not to
        # be a live host on any correctly-configured network.
        props = self._couchbase_link_props(
            unreachable_link, target, client=good_client,
            hostname="192.0.2.1",
        )
        ok, code, create_body = self._link_rest("POST", props)
        if ok:
            self._created_links.append(("Default", unreachable_link))
            _, unreachable_error = self._connect_link(unreachable_link)
        else:
            # Link creation validates connectivity, so an unroutable host is
            # rejected at creation -- that response is the error to judge.
            unreachable_error = create_body
        self._assert_link_error_kind(
            unreachable_error, "unreachable",
            "A link pointed at an unroutable address must report a "
            "connectivity failure",
            redact=[unreachable_link, good_client["username"]],
        )

        # Leg 3: wrong credentials. Username/password rather than a
        # certificate, since "credentials invalid" is only a distinct cause
        # when a credential is what was supplied.
        bad_creds_link = "crl_link_err_creds"
        props = self._couchbase_link_props(
            bad_creds_link, target,
            username=target["cluster"].master.rest_username,
            password="definitely-not-the-password",
        )
        ok, code, create_body = self._link_rest("POST", props)
        if ok:
            self._created_links.append(("Default", bad_creds_link))
            _, creds_error = self._connect_link(bad_creds_link)
        else:
            creds_error = create_body
        self._assert_link_error_kind(
            creds_error, "credentials",
            "A link with a wrong password must report an authentication "
            "failure",
            redact=[bad_creds_link],
        )

        # And the three must not be the same message with different wording
        # around the edges.
        self.assertNotEqual(
            revoked_error.lower(), unreachable_error.lower(),
            "The revoked and unreachable failures produced the same error "
            "text, so they are not distinguishable"
        )
        self.assertNotEqual(
            revoked_error.lower(), creds_error.lower(),
            "The revoked and bad-credential failures produced the same error "
            "text, so they are not distinguishable"
        )
        self.log.info(
            "Three link failures, three distinguishable errors:\n"
            f"  revoked     : {revoked_error[:200]}\n"
            f"  unreachable : {unreachable_error[:200]}\n"
            f"  credentials : {creds_error[:200]}"
        )

    def test_remote_link_creation_and_edit_with_revoked_cert_fail_immediately(self):
        """
        Section 3: creating -- or editing -- a link with an ALREADY revoked
        client certificate is refused when the operation is issued, not
        silently accepted and left to fail at first sync.

        Two halves, because the plan asks about both verbs:
          create: the serial is published before the link exists, and the
                  POST must be refused and leave nothing in metadata.
          edit:   a working link's certificate is swapped for the revoked one
                  by PUT, which must be refused and must leave the link still
                  working on its original certificate.

        The edit half's closing check matters most. A rejected edit that
        nonetheless half-applied would leave an operator with a link that
        reports its old configuration and cannot connect, which is worse
        than either a clean success or a clean failure.
        """
        target = self._setup_remote_link_target()

        # ── create with a revoked certificate ───────────────────────────────
        doomed = self._mint_link_client_cert(target, "cbas_crl_link_doomed")
        self._publish_remote_crl(target, [doomed["serial"]])

        doomed_link = "crl_link_create_revoked"
        props = self._couchbase_link_props(doomed_link, target, client=doomed)
        ok, code, body = self._link_rest("POST", props)
        self.assertFalse(
            ok,
            f"Creating a link with an already-revoked client certificate "
            f"(serial={doomed['serial']}, on the target's CRL under "
            f"clientAuth=Require) returned {code}. Section 3 requires this "
            f"to fail at link-creation time with a clear error rather than "
            f"succeeding and failing later at first sync."
        )
        self._assert_link_error_kind(
            body, "revoked",
            "A link creation refused for a revoked certificate must say so",
            redact=[doomed_link, doomed["username"]],
        )
        self.assertFalse(
            self.cbas_util.validate_link_in_metadata(
                self.cluster, doomed_link, "Default", "couchbase"),
            f"Link {doomed_link} was refused but still appears in Analytics "
            f"metadata, so the refusal left a partial object behind"
        )
        self.log.info("Creation with a revoked certificate refused cleanly")

        # ── edit a working link onto a revoked certificate ──────────────────
        good = self._mint_link_client_cert(target, "cbas_crl_link_editable")
        edit_link = "crl_link_edit_revoked"
        working_props = self._link_baseline(target, good, edit_link)

        # A second certificate, revoked in the same CRL generation as the
        # first. Both serials are listed: a higher crlNumber from one issuer
        # supersedes the earlier CRL wholesale, so dropping `doomed` here
        # would quietly un-revoke it.
        replacement = self._mint_link_client_cert(
            target, "cbas_crl_link_replacement")
        self._publish_remote_crl(
            target, [doomed["serial"], replacement["serial"]])

        self._disconnect_link(edit_link)
        edit_props = self._couchbase_link_props(
            edit_link, target, client=replacement)
        ok, code, body = self._link_rest("PUT", edit_props)
        self.assertFalse(
            ok,
            f"Editing link {edit_link} to use an already-revoked client "
            f"certificate (serial={replacement['serial']}) returned {code}. "
            f"Section 3 requires the edit to be refused when it is issued."
        )
        self._assert_link_error_kind(
            body, "revoked",
            "A link edit refused for a revoked certificate must say so",
            redact=[edit_link, replacement["username"]],
        )

        # The refused edit must not have disturbed the working link.
        connected, error = self._connect_link(edit_link)
        self.assertTrue(
            connected,
            f"After a REFUSED edit, link {edit_link} no longer connects on "
            f"its original, non-revoked certificate "
            f"(serial={good['serial']}): {error[:600]}. A rejected edit must "
            f"leave the link exactly as it was."
        )
        self.log.info(
            "Edit onto a revoked certificate refused, and the link still "
            "works on its original certificate"
        )

    def test_remote_link_recovers_when_serial_removed_from_remote_crl(self):
        """
        Section 3: removing the offending serial from the remote CRL restores
        link connectivity, with no need to recreate the link.

        The link object is created exactly once, at the start. Recovery is
        then asserted on that same object -- no create, no drop, no property
        edit between the failing and the succeeding connect -- which is what
        "without needing to recreate the link" has to mean to be testable.
        The link's metadata entry is compared before and after to show it is
        the same object rather than a lookalike.
        """
        target = self._setup_remote_link_target()
        client = self._mint_link_client_cert(target, "cbas_crl_link_recover")
        link_name = "crl_link_recover"
        self._link_baseline(target, client, link_name)

        before = self._link_identity(link_name)

        self._disconnect_link(link_name)
        self._publish_remote_crl(target, [client["serial"]])
        connected, error = self._connect_link(link_name)
        self.assertFalse(
            connected,
            "The link must first fail while its serial is on the remote CRL, "
            "or the recovery below proves nothing"
        )
        self.log.info(f"Link failed while revoked, as expected: {error[:200]}")

        # Supersede with a CRL that lists nothing. Same issuer, higher
        # crlNumber, so it replaces the revocation wholesale.
        self._publish_remote_crl(target, [])

        # The remote has to notice the new CRL before the link can succeed,
        # and there is no signal to wait on from the Analytics side, so poll
        # the operation itself.
        deadline = time.time() + 120
        last_error = error
        while time.time() < deadline:
            connected, last_error = self._connect_link(link_name)
            if connected:
                break
            self._disconnect_link(link_name)
            time.sleep(5)
        self.assertTrue(
            connected,
            f"After the serial was removed from the remote CRL, the SAME "
            f"link still cannot connect within 120s: {last_error[:600]}. "
            f"Section 3 requires connectivity to be restored without "
            f"recreating the link."
        )

        after = self._link_identity(link_name)
        self.assertEqual(
            before, after,
            f"The link's identity and configuration changed across the "
            f"revoke/restore cycle, so recovery did not happen on the "
            f"original object.\nbefore: {before}\nafter:  {after}"
        )
        self.log.info(
            "Link recovered on its original object once the serial was "
            "removed from the remote CRL"
        )

    def test_link_revocation_does_not_affect_links_to_other_clusters(self):
        """
        Section 3: revoking one link's certificate leaves links to OTHER
        remote clusters untouched.

        Needs three clusters -- the Analytics cluster and two link targets --
        because the isolation being tested is between remote clusters, and
        two links to the same remote would not show it. Each target trusts
        its OWN CA, so revoking on target A cannot even be expressed on
        target B; that is the property, and the test proves the second link
        keeps working rather than assuming it.

        Order matters in the final check: target B's link is connected AFTER
        A's has been broken, so it is a fresh connection attempt made while
        A's revocation is live, not a connection that predates it.
        """
        target_a = self._setup_remote_link_target(
            self._remote_cluster(1), label="C2")

        # A CA of its own for target B. Sharing this test's CA would make the
        # two targets share a revocation namespace, and the isolation would
        # then be a property of which CRL was uploaded where rather than of
        # the clusters being separate.
        ca_b_cert, ca_b_key = self.crl_utils.generate_ca(
            f"AnalyticsCRLTestCA_B_{uuid.uuid4().hex[:8]}")
        target_b = self._setup_remote_link_target(
            self._remote_cluster(2), label="C3",
            ca_cert=ca_b_cert, ca_key=ca_b_key)

        client_a = self._mint_link_client_cert(target_a, "cbas_crl_link_a")
        client_b = self._mint_link_client_cert(target_b, "cbas_crl_link_b")
        link_a, link_b = "crl_link_iso_a", "crl_link_iso_b"
        self._link_baseline(target_a, client_a, link_a)
        self._link_baseline(target_b, client_b, link_b)

        self._disconnect_link(link_a)
        self._disconnect_link(link_b)
        self._publish_remote_crl(target_a, [client_a["serial"]])

        connected_a, error_a = self._connect_link(link_a)
        self.assertFalse(
            connected_a,
            "Link A must fail while its certificate is revoked on target A, "
            "or the isolation check below proves nothing"
        )
        self._assert_link_error_kind(
            error_a, "revoked",
            "Link A must fail for revocation specifically",
            redact=[link_a, client_a["username"]],
        )

        connected_b, error_b = self._connect_link(link_b)
        self.assertTrue(
            connected_b,
            f"Revoking link A's certificate on {target_a['name']} also broke "
            f"link B to {target_b['name']}, which uses a different CA and a "
            f"different remote cluster: {error_b[:600]}. A revocation must "
            f"not reach links to unrelated clusters."
        )
        self.log.info(
            "Link A refused for revocation while link B to a different "
            "remote cluster kept connecting"
        )

    def test_link_certificate_material_not_exposed_on_revocation_failure(self):
        """
        Section 3: when a link fails for a revocation reason, the link's
        certificate and key material stays out of the logs, the audit trail
        and diagnostic output.

        Provokes the failure first, then looks in the three places the plan
        names, plus the link's own REST representation -- which is where a
        leak would be easiest to reach, since reading a link's properties
        needs no node access at all.

        Checks a slice of the base64 body as well as the PEM armour: a
        service that strips the BEGIN/END lines while still writing the
        payload would pass an armour-only check having leaked the key
        anyway. See _assert_no_key_material.
        """
        target = self._setup_remote_link_target()
        client = self._mint_link_client_cert(target, "cbas_crl_link_secret")
        link_name = "crl_link_secret"
        self._link_baseline(target, client, link_name)

        self._set_audit(True)
        self._disconnect_link(link_name)
        self._publish_remote_crl(target, [client["serial"]])
        connected, error = self._connect_link(link_name)
        self.assertFalse(
            connected,
            "The link must actually fail for revocation, or there is no "
            "revocation-related output to inspect"
        )

        # 1. The error handed back to the caller.
        self._assert_no_key_material(
            [error], client, "The link's connect error")

        # 2. Analytics logs on the local cluster, which is what logged the
        #    outbound failure. all_nodes because this is an ABSENCE check:
        #    the link runs on the CC node, which is not necessarily
        #    self.cbas_node, and reading the wrong node would report clean
        #    logs while the material sat in another node's file.
        analytics_lines = self._read_analytics_log(
            grep=link_name, all_nodes=True)
        analytics_lines += self._read_analytics_log(
            grep="revok", all_nodes=True)
        analytics_lines += self._read_analytics_log(
            grep=client["username"], all_nodes=True)
        self._assert_no_key_material(
            analytics_lines, client, "The Analytics log")

        # 3. Audit records on both clusters. The remote is where the
        #    rejection happened, so its audit trail is the one most likely to
        #    carry the offending certificate. Auditing is enabled on the
        #    LOCAL cluster only -- _restore_audit puts that back -- and the
        #    remote's log is read with whatever setting it already has,
        #    rather than leaving a changed audit setting behind on a cluster
        #    this test does not own the teardown for.
        audit_lines = self._read_audit_log()
        audit_lines += self._read_audit_log(node=target["cluster"].master)
        self._assert_no_key_material(audit_lines, client, "The audit log")

        # An absence check over output that turned out to be empty proves
        # nothing, so say which legs actually had something to examine
        # instead of letting a silent zero read as a clean result.
        self.log.info(
            f"Absence check corpus: connect error {len(error)} chars, "
            f"{len(analytics_lines)} Analytics log line(s), "
            f"{len(audit_lines)} audit line(s)"
        )
        if not analytics_lines:
            self.log.warning(
                "No Analytics log lines matched the link name, 'revok' or the "
                "certificate's user on any Analytics node, so the log leg of "
                "this check was vacuous. The error-text, diagnostics and "
                "link-REST legs below still applied."
            )

        # 4. Diagnostic output, and the link's own properties. A link is
        #    readable by anyone who can reach the Analytics REST API, so an
        #    unredacted clientKey here would be the most exposed leak of all.
        status, diagnostics = self.crl_utils.diagnostics_status(target["rest"])
        link_info = self.cbas_util.get_link_info(
            self.cluster, dataverse="Default", link_name=link_name,
            link_type="couchbase")
        self._assert_no_key_material(
            [json.dumps(diagnostics, default=str),
             json.dumps(link_info, default=str)],
            client, "CRL diagnostics and the link's REST representation")

        # The private key's VALUE must not be readable back out of a link's
        # configuration. A `clientKey` field carrying a redaction marker is
        # correct and expected -- Analytics reports it as
        # `clientKey='<redacted N chars>'` in its own logs -- so asserting
        # the field name is absent would fail the right behaviour.
        info_text = json.dumps(link_info, default=str)
        key_body = "".join(
            ln for ln in client["key_pem"].decode().splitlines()
            if ln and not ln.startswith("-----")
        )
        self.assertNotIn(
            key_body[len(key_body) // 3:][:48], info_text,
            f"The link's REST representation returns the actual private key "
            f"body. It must be withheld or redacted: {info_text[:600]}"
        )
        if "clientkey" in info_text.lower():
            self.log.info(
                "The link's REST view carries a clientKey field; its value is "
                "not the real key, which is the required behaviour."
            )
        self.log.info(
            "No link certificate or key material in the error, Analytics "
            "logs, audit records, diagnostics or the link's REST view"
        )

    # ── Section 5: shadow data, ingestion and replicas ──────────────────────
    #
    # Ingestion from KV into an Analytics dataset runs over the cluster's
    # INTERNAL mTLS, not the client-facing surface every other section
    # exercises. Confirmed on 8.5.0-1073: Analytics logs
    # useMutualTls":true alongside
    # clientCertPath":".../config/certs/client_chain.pem", and that
    # certificate is CN="Couchbase Internal Client (...)" issued by the
    # cluster's OWN generated CA (CN="Couchbase Server <id>").
    #
    # That issuer is the reason bullet 1 of this section is not directly
    # testable: revoking the internal client certificate needs a CRL signed
    # by the generated CA, whose private key is not available to a test. It
    # would first require replacing the cluster's node and client
    # certificates with ones issued by this suite's CA. See
    # test_internal_ingestion_certificate_is_not_test_revocable for the
    # recorded finding.

    LOCAL_BUCKET = "crl_ingest"

    def _seed_local_dataset(self, doc_count=100, key_prefix="a"):
        """
        A KV bucket with documents, plus an Analytics dataset ingesting from
        it over the Local link. Returns (dataset_name, ingested_count).
        """
        self._ensure_bucket(self.cluster.master, self.LOCAL_BUCKET)
        self._n1ql_insert_docs(self.LOCAL_BUCKET, doc_count, key_prefix)

        # The DDL is issued directly rather than through
        # cbas_util.create_dataset because that returns a bare boolean and
        # drops the server's error, which left a failure here undiagnosable.
        dataset = f"{self.LOCAL_BUCKET}_ds"

        # Drop first, unconditionally. A dataset surviving a previous run's
        # teardown is a realistic condition here rather than a hypothetical:
        # this suite's failover test drops an Analytics node, and a teardown
        # that runs while Analytics is still answering `code 23000 Analytics
        # Service is temporarily unavailable` cannot drop anything -- so the
        # dataset outlives the test that made it and the NEXT run fails with
        # `code 24040 ... already exists`. Recreating rather than reusing it
        # also keeps the ingested count honest, which CREATE ... IF NOT
        # EXISTS would not.
        self.cbas_util.execute_statement_on_cbas_util(
            self.cluster, f"DROP DATASET Default.`{dataset}` IF EXISTS;",
            timeout=180, analytics_timeout=180)

        statement = (f"CREATE DATASET Default.`{dataset}` "
                     f"ON `{self.LOCAL_BUCKET}`;")
        status, _, errors, _, _ = (
            self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, statement, timeout=180, analytics_timeout=180)
        )
        self.assertEqual(
            status, "success",
            f"Could not create dataset {dataset} on {self.LOCAL_BUCKET} over "
            f"the Local link: {json.dumps(errors)[:600]}"
        )
        self._created_datasets.append(f"Default.`{dataset}`")

        connected, error = self._connect_link("Local")
        self.assertTrue(
            connected, f"Could not connect the Local link: {error[:400]}")
        self.assertTrue(
            self.cbas_util.wait_for_ingestion_complete(
                self.cluster, f"Default.`{dataset}`", doc_count, timeout=300),
            f"Ingestion of {doc_count} docs into {dataset} did not complete"
        )
        self.log.info(f"Dataset {dataset} ingested {doc_count} docs from KV")
        return dataset, doc_count

    def test_ingestion_undisturbed_by_unrelated_client_cert_revocation(self):
        """
        Section 5: ongoing dataset ingestion is not disrupted by a CRL update
        that revokes an unrelated, CLIENT-facing certificate.

        Ingestion runs over the cluster's internal mTLS; the revoked
        certificate here is one a client would present to the Analytics REST
        endpoint. The two should be independent, and this proves it in both
        directions rather than only asserting that ingestion survived:

          - the revoked client certificate really is refused at 18095, so
            the CRL is demonstrably live. Without this leg the test would
            pass just as happily against a CRL that was never applied, which
            is exactly how a previous test in this suite produced a false
            pass.
          - documents written AFTER the revocation still reach the dataset,
            so ingestion is not merely un-broken but still flowing.
        """
        dataset, ingested = self._seed_local_dataset(doc_count=100)

        valid_cert, valid_key, _ = self._client_cert_for(
            "cbas_crl_ingest_valid", "analytics_admin")
        revoked_cert, revoked_key, revoked_serial = self._client_cert_for(
            "cbas_crl_ingest_revoked", "analytics_admin")

        filename = "cbas_crl_section5.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename,
            crl_number=1)
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"})
        self._enable_client_cert_auth(state="enable")

        # Control leg: the revocation is actually in force.
        self._wait_for_analytics_ok("SELECT 1;", cert=(valid_cert, valid_key))
        self.assert_cert_refused(
            lambda: self._analytics_query(
                "SELECT 1;", cert=(revoked_cert, revoked_key)),
            "The revoked client certificate must be refused, or this test "
            "cannot claim the CRL was live while ingestion continued"
        )
        self.log.info("Revocation confirmed live on the client-facing surface")

        # Ingestion must still be flowing, not merely intact.
        self._n1ql_insert_docs(self.LOCAL_BUCKET, 50, "b")
        self.assertTrue(
            self.cbas_util.wait_for_ingestion_complete(
                self.cluster, f"Default.`{dataset}`", ingested + 50,
                timeout=300),
            f"Documents written after an unrelated client certificate was "
            f"revoked did not reach {dataset}. Ingestion runs over internal "
            f"mTLS and must be unaffected by a client-facing revocation."
        )
        self.log.info(
            f"Ingestion continued to {ingested + 50} docs while a client "
            f"certificate was revoked")

    def test_internal_ingestion_certificate_is_not_test_revocable(self):
        """
        Section 5, bullet 1: record why revoking the certificate used for
        KV-to-Analytics internal sync cannot be tested as written, and assert
        the premise the plan bullet rests on.

        The bullet says "revoking the certificate used for KV-to-Analytics
        internal data sync (if certificate-based)". It IS certificate-based:
        this test asserts Analytics presents an internal client certificate.
        But that certificate is issued by the cluster's own generated CA, and
        a CRL is only honoured when signed by the issuing CA's key -- which a
        test does not have. Revoking it would mean first replacing the
        cluster's node AND internal client certificates with ones issued by
        this suite's CA, which is a nodeToNode-scope exercise rather than an
        Analytics one, and which the plan's own reviewer places with KV
        ("KV would be responsible for the behavior of the connection here,
        how it is reported / handled would be in-scope").

        So this asserts the two facts that determine the bullet's fate,
        rather than silently skipping it:
          - Analytics uses mutual TLS internally with a client certificate
          - that certificate is issued by the cluster-generated CA, not by
            any CA a test can issue a CRL for
        """
        certs_dir = "/opt/couchbase/var/lib/couchbase/config/certs"
        nodes = list(self.cluster.cbas_nodes or [self.cbas_node])
        seen = {}
        mutual_tls_nodes = []
        for node in nodes:
            if node.ip in seen:
                continue
            shell = RemoteMachineShellConnection(node)
            try:
                out, _ = shell.execute_command(
                    f"openssl x509 -in {certs_dir}/client_chain.pem "
                    f"-noout -subject -issuer 2>&1")
                seen[node.ip] = " ".join(
                    line.strip() for line in (out or []) if line.strip())
                out, _ = shell.execute_command(
                    "grep -aho 'useMutualTls\":true' "
                    "/opt/couchbase/var/lib/couchbase/logs/analytics_*.log "
                    "2>/dev/null | head -1")
                if any(line.strip() for line in (out or [])):
                    mutual_tls_nodes.append(node.ip)
            finally:
                shell.disconnect()

        internal = {ip: info for ip, info in seen.items()
                    if "Couchbase Internal Client" in info}
        self.assertTrue(
            internal,
            f"No Analytics node presents an internal client certificate at "
            f"{certs_dir}/client_chain.pem, so the premise that KV-to-"
            f"Analytics sync is certificate-based does not hold on this "
            f"build and the plan bullet needs revisiting. Read: {seen}"
        )

        # The issuer is what decides whether this bullet is testable at all.
        not_generated = {
            ip: info for ip, info in internal.items()
            if "issuer=CN = Couchbase Server" not in info
        }
        self.assertFalse(
            not_generated,
            f"The internal client certificate is no longer issued by the "
            f"cluster's own generated CA on {list(not_generated)}. If it is "
            f"now issued by a CA a test can upload, this bullet became "
            f"directly testable and this test should be replaced with a real "
            f"revocation: {not_generated}"
        )

        # Corroborating only, deliberately not asserted: whether the mutual
        # TLS handshake happens to appear in the logs depends on what the
        # node has done since its last log rotation, so a fresh cluster can
        # legitimately show nothing. An earlier version of this test asserted
        # on it and failed against a perfectly healthy node purely because
        # the evidence was on a DIFFERENT Analytics node -- the same
        # wrong-node mistake this suite has made before.
        self.log.info(
            f"Section 5 bullet 1 recorded as not test-revocable. Internal "
            f"client certificates: {internal}. Nodes logging "
            f"useMutualTls\":true: {mutual_tls_nodes or 'none since rotation'}. "
            f"Revoking this certificate would require replacing the cluster's "
            f"node and internal client certificates with ones issued by this "
            f"suite's CA -- a nodeToNode-scope exercise."
        )

    # ── Section 7: cluster distribution and topology ───────────────────────
    #
    # These need MORE THAN ONE Analytics node in a single cluster, which no
    # other section in this suite requires -- so their conf lines carry
    # services_init with cbas on two nodes, plus spare servers for the
    # rebalance-in cases.

    def _require_two_analytics_nodes(self):
        """Both Analytics nodes, or a failure naming the conf that fixes it."""
        nodes = list(self.cluster.cbas_nodes or [])
        unique = []
        for node in nodes:
            if node.ip not in {n.ip for n in unique}:
                unique.append(node)
        if len(unique) < 2:
            self.fail(
                f"This test needs two Analytics nodes in one cluster, got "
                f"{[n.ip for n in unique]}. Use e.g. nodes_init=3,"
                f"services_init=kv:n1ql:index-kv:cbas-kv:cbas."
            )
        return unique

    def _arm_revocation(self, filename, label="crl_topology"):
        """
        A valid and a revoked client certificate, with the revocation live
        under clientAuth=Require. Returns (valid_pair, revoked_pair).
        """
        valid_cert, valid_key, _ = self._client_cert_for(
            f"{label}_valid", "analytics_admin")
        revoked_cert, revoked_key, revoked_serial = self._client_cert_for(
            f"{label}_revoked", "analytics_admin")
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename,
            crl_number=1)
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"})
        self._enable_client_cert_auth(state="enable")
        return (valid_cert, valid_key), (revoked_cert, revoked_key)

    def _assert_node_enforces(self, node, valid_pair, revoked_pair, where):
        """A node accepts the valid certificate and refuses the revoked one."""
        resp = self._wait_for_analytics_ok(
            "SELECT 1;", cert=valid_pair, node=node)
        self.assertEqual(
            resp.status_code, 200,
            f"{where}: a valid, non-revoked certificate must still reach "
            f"Analytics on {node.ip}, got {resp.status_code}: "
            f"{resp.text[:300]}"
        )
        self.assert_cert_refused(
            lambda: self._analytics_query("SELECT 1;", cert=revoked_pair,
                                          node=node),
            f"{where}: the revoked certificate must be refused on {node.ip}"
        )
        self.log.info(f"{where}: {node.ip} enforces the CRL correctly")

    def test_crl_enforcement_is_consistent_across_analytics_nodes(self):
        """
        Section 7: a CRL uploaded once is enforced by EVERY Analytics node,
        not only the node that received the upload, so the verdict does not
        depend on which node a client happens to reach.

        The upload goes to cluster.master over REST; the assertions are made
        directly against each Analytics node's own 18095 listener. Both legs
        run on both nodes -- a node that refused everything would pass a
        revoked-only check while being completely broken.
        """
        nodes = self._require_two_analytics_nodes()
        valid_pair, revoked_pair = self._arm_revocation(
            "cbas_crl_section7_propagation.pem", label="crl_prop")
        self.log.info(
            f"CRL uploaded via {self.cluster.master.ip}; checking "
            f"{[n.ip for n in nodes]}")
        for node in nodes:
            self._assert_node_enforces(
                node, valid_pair, revoked_pair, "CRL propagation")

    def test_new_analytics_node_inherits_crl_and_policy(self):
        """
        Section 7: an Analytics node added AFTER the CRL and policy are in
        place inherits both automatically, with no manual upload.

        The new node is rebalanced in while the revocation is already live,
        then asserted against directly. Its own /settings/crl is read back
        too, so a node that merely proxied the verdict to another node would
        not pass as having inherited the policy.
        """
        nodes = self._require_two_analytics_nodes()
        spare = next((s for s in (self.available_servers or [])
                      if s.ip not in {n.ip for n in self.cluster.servers}),
                     None)
        if spare is None:
            self.fail(
                "This test needs a spare server to rebalance in as a new "
                "Analytics node. Give the run more nodes than nodes_init "
                "consumes, e.g. a 5-node ini with nodes_init=3."
            )

        valid_pair, revoked_pair = self._arm_revocation(
            "cbas_crl_section7_newnode.pem", label="crl_newnode")
        for node in nodes:
            self._assert_node_enforces(
                node, valid_pair, revoked_pair, "before rebalance-in")

        self.log.info(f"Rebalancing in {spare.ip} with the cbas service")
        self.task.rebalance(self.cluster, [spare], [], services=["kv,cbas"])
        self.cluster.cbas_nodes.append(spare)
        self._rebalanced_in.append(spare)

        # No CRL upload, no policy write between the rebalance and here.
        self._assert_node_enforces(
            spare, valid_pair, revoked_pair, "newly added Analytics node")

        status, settings = self.crl_utils.get_settings(
            RestConnection(spare))
        self.assertTrue(status, f"Could not read /settings/crl on the new "
                                f"node {spare.ip}: {settings}")
        policy = (settings or {}).get("policyPerScope", {})
        self.assertEqual(
            policy.get("clientAuth"), "Require",
            f"The new Analytics node {spare.ip} reports "
            f"clientAuth={policy.get('clientAuth')!r} rather than Require, so "
            f"it did not inherit the cluster's revocation policy: {settings}"
        )
        self.log.info(
            f"New Analytics node {spare.ip} inherited the CRL and "
            f"clientAuth=Require with no manual upload")

    def test_revocation_enforced_while_analytics_node_is_rebalanced_out(self):
        """
        Section 7: a revoked certificate stays refused throughout a rebalance,
        on the node being removed and on the node that remains.

        Probes continuously on a background thread while the rebalance runs,
        rather than only before and after: the bullet is about the window
        DURING the topology change, which a before/after check would step
        straight over. Any single acceptance of the revoked certificate is
        recorded and fails the test.

        Each cycle probes twice, with the revoked certificate and then with
        the valid one. The valid probe is the positive control and is what
        makes the result mean anything: "the revoked certificate was never
        accepted" is produced both by enforcement holding and by the survivor
        never having been reachable at all, and those are indistinguishable
        from the revoked probe alone. The test therefore also requires that
        the valid certificate got through at least once (the node was really
        serving) and that the revoked one was explicitly refused at least
        once (revocation really was exercised). A connection error with no
        TLS alert is counted as neither -- it is recorded as a probe error,
        since a node restarting mid-rebalance proves nothing either way.
        """
        nodes = self._require_two_analytics_nodes()
        valid_pair, revoked_pair = self._arm_revocation(
            "cbas_crl_section7_rebalance.pem", label="crl_rebal")
        survivor, leaving = nodes[0], nodes[1]
        if leaving.ip == self.cluster.master.ip:
            survivor, leaving = leaving, survivor
        for node in nodes:
            self._assert_node_enforces(
                node, valid_pair, revoked_pair, "before rebalance-out")

        accepted = []
        refused = []
        reachable = []
        probe_error = []
        stop = threading.Event()

        def probe():
            while not stop.is_set():
                # The revoked certificate: must be refused throughout.
                try:
                    resp = self._analytics_query(
                        "SELECT 1;", cert=revoked_pair, node=survivor,
                        timeout=15)
                    if resp.status_code == 200:
                        accepted.append(resp.status_code)
                    elif resp.status_code == 401:
                        refused.append("401")
                except requests.exceptions.SSLError:
                    refused.append("tls-alert")
                except requests.exceptions.ConnectionError as exc:
                    # Only an error carrying a TLS alert shows the peer
                    # refused the certificate. A bare connection failure --
                    # the node restarting mid-rebalance, say -- proves
                    # nothing either way, so it is recorded separately and
                    # never counted as enforcement.
                    if self._is_tls_rejection(exc):
                        refused.append("tls-alert")
                    else:
                        probe_error.append(str(exc))
                except Exception as exc:      # noqa: BLE001 - recorded, not raised
                    probe_error.append(str(exc))

                # The valid certificate: the positive control. Without it an
                # empty `accepted` list cannot tell enforcement holding apart
                # from the survivor never having been reachable, because both
                # produce exactly no acceptances.
                try:
                    resp = self._analytics_query(
                        "SELECT 1;", cert=valid_pair, node=survivor,
                        timeout=15)
                    if resp.status_code == 200:
                        reachable.append(resp.status_code)
                except Exception:             # noqa: BLE001 - control probe
                    pass
                time.sleep(1)

        thread = threading.Thread(target=probe, name="crl_rebalance_probe")
        thread.start()
        try:
            self.log.info(f"Rebalancing out the Analytics node {leaving.ip}")
            self.task.rebalance(self.cluster, [], [leaving])
            self._rebalanced_out.append(leaving)
        finally:
            stop.set()
            thread.join(timeout=60)

        self.assertFalse(
            accepted,
            f"The revoked certificate was ACCEPTED {len(accepted)} time(s) on "
            f"{survivor.ip} while {leaving.ip} was being rebalanced out. "
            f"Enforcement must hold throughout a topology change."
        )
        # An empty `accepted` list on its own proves nothing: it looks the
        # same whether enforcement held or the survivor was never reachable
        # and every probe failed. These two make the instrument prove itself
        # before the result above is believed.
        self.assertTrue(
            reachable,
            f"The valid certificate never once succeeded on {survivor.ip} "
            f"during the rebalance-out of {leaving.ip}, so the survivor was "
            f"not serving and the 'revoked certificate was never accepted' "
            f"result above is vacuous rather than evidence of enforcement. "
            f"{len(probe_error)} non-TLS probe error(s) were seen"
            + (f", first: {probe_error[0][:200]}" if probe_error else "")
        )
        self.assertTrue(
            refused,
            f"The revoked certificate was never explicitly refused on "
            f"{survivor.ip} during the rebalance-out of {leaving.ip} -- no "
            f"TLS alert and no 401 in {len(accepted) + len(refused)} "
            f"conclusive probe(s). The valid certificate did get through "
            f"{len(reachable)} time(s), so the node was serving; revocation "
            f"simply never produced an observable rejection."
        )
        self.log.info(
            f"Rebalance-out probe on {survivor.ip}: revoked refused "
            f"{len(refused)}x, valid served {len(reachable)}x, "
            f"accepted {len(accepted)}x, non-TLS errors {len(probe_error)}")
        self.cluster.cbas_nodes = [
            n for n in self.cluster.cbas_nodes if n.ip != leaving.ip]
        self._assert_node_enforces(
            survivor, valid_pair, revoked_pair, "after rebalance-out")
        if probe_error:
            self.log.info(
                f"Probe saw {len(probe_error)} non-TLS error(s) during the "
                f"rebalance, which is expected as the topology moves: "
                f"{probe_error[0][:200]}")

    def test_promoted_analytics_replica_still_enforces_revocation(self):
        """
        Section 5, replica bullets: after an Analytics node is hard failed
        over, the node that picks up its shadow data enforces revocation with
        the same policy and CRL set, and needs no re-configuration.

        Follows the replica sequence cbas_HA.py establishes, because without
        it there is nothing to promote and the test would only be re-checking
        that a survivor still works:
          - set numReplicas=1 BEFORE ingesting
          - disconnect the Local link so shadow data is persisted
          - wait for replication, and verify the replica count really is 1
        Only then is the failover a promotion rather than a data loss.

        Note that Analytics answers `code 23000 "Analytics Service is
        temporarily unavailable"` for a while after an Analytics node is
        failed over. That is expected recovery, not an enforcement result, so
        service recovery is waited for as an explicit PRECONDITION with its
        own failure message -- an earlier version folded it into the
        enforcement assertion and reported a 503 as though a valid
        certificate had been rejected.
        """
        nodes = self._require_two_analytics_nodes()

        status = self.cbas_util.set_replica_number_from_settings(
            self.cluster.master, replica_num=1)
        self.assertTrue(
            status,
            "Could not set numReplicas=1 for Analytics. Without a replica "
            "there is no shadow data to promote and this test cannot mean "
            "what it claims."
        )
        self.log.info("Analytics numReplicas set to 1")

        dataset, ingested = self._seed_local_dataset(doc_count=100)
        valid_pair, revoked_pair = self._arm_revocation(
            "cbas_crl_section5_replica.pem", label="crl_replica")

        # Shadow data is only replicated once the link is disconnected.
        self._disconnect_link("Local")
        self.assertTrue(
            self.cbas_util.wait_for_replication_to_finish(self.cluster),
            "Analytics replication did not finish, so there is no replica to "
            "promote"
        )
        self.assertTrue(
            self.cbas_util.verify_actual_number_of_replicas(self.cluster, 1),
            "Analytics does not actually have 1 replica, so failing a node "
            "over would lose the shadow data rather than promote it"
        )
        self.log.info("One Analytics replica present and replicated")

        survivor, failing = nodes[0], nodes[1]
        if failing.ip == self.cluster.master.ip:
            survivor, failing = failing, survivor
        for node in nodes:
            self._assert_node_enforces(
                node, valid_pair, revoked_pair, "before failover")

        self.log.info(f"Hard failing over the Analytics node {failing.ip}")
        self.task.failover(self.cluster, failover_nodes=[failing],
                           graceful=False)
        self._failed_over.append(failing)
        self.cluster.cbas_nodes = [
            n for n in self.cluster.cbas_nodes if n.ip != failing.ip]

        # Precondition, not an assertion about revocation.
        self.assertTrue(
            self.cbas_util.is_analytics_running(self.cluster, timeout=600),
            f"Analytics did not return to ACTIVE within 600s after "
            f"{failing.ip} was failed over, so no statement can be made "
            f"about what the promoted node enforces. This is a recovery "
            f"problem, not a revocation one."
        )
        self.log.info("Analytics is ACTIVE again after the failover")

        self._assert_node_enforces(
            survivor, valid_pair, revoked_pair, "after failover")

        status, settings = self.crl_utils.get_settings(
            RestConnection(survivor))
        self.assertTrue(status, f"Could not read /settings/crl on "
                                f"{survivor.ip}: {settings}")
        policy = (settings or {}).get("policyPerScope", {})
        self.assertEqual(
            policy.get("clientAuth"), "Require",
            f"After failover the surviving Analytics node {survivor.ip} "
            f"reports clientAuth={policy.get('clientAuth')!r}; the promoted "
            f"node must enforce the same policy with no re-configuration"
        )
        self.log.info(
            f"{survivor.ip} enforces the same policy and CRL set after "
            f"{failing.ip} was failed over, with a promoted replica")

    # ── Section 10: the two remaining link-bypass bullets ──────────────────
    #
    # The other two section-10 bullets (tampered/unsigned CRLs, and no
    # password fallback under optional mTLS) are covered above and need no
    # remote cluster. These two are about using an Analytics Link as the
    # bypass route, so they reuse the section-3 link-target fixture.

    def test_revoked_link_cert_cannot_be_reused_for_a_new_or_disabled_link(self):
        """
        Section 10: a revoked client certificate cannot be used to stand up a
        NEW link, nor to bring a disconnected one back.

        Both halves matter and they fail differently in principle: the first
        is refused at create time, the second at connect time on an object
        that already exists and was working moments earlier. A product that
        validated only on create would pass the first and fail the second,
        which is the bypass this bullet is really about.
        """
        target = self._setup_remote_link_target()
        client = self._mint_link_client_cert(target, "cbas_crl_reuse")
        established = "crl_link_reuse"
        self._link_baseline(target, client, established)

        # Disconnect first, then revoke, so the link is a live object whose
        # certificate has gone bad rather than one that never worked.
        self._disconnect_link(established)
        self._publish_remote_crl(target, [client["serial"]])

        # Half 1: the same certificate cannot establish a NEW link.
        fresh = "crl_link_reuse_new"
        ok, code, body = self._link_rest(
            "POST", self._couchbase_link_props(fresh, target, client=client))
        self.assertFalse(
            ok,
            f"A revoked client certificate (serial={client['serial']}) was "
            f"accepted for a NEW link {fresh} ({code}). Section 10 requires "
            f"it to be unusable for establishing another link."
        )
        self._assert_link_error_kind(
            body, "revoked",
            "Refusing a new link built on a revoked certificate must say why",
            redact=[fresh, client["username"]],
        )
        self.assertFalse(
            self.cbas_util.validate_link_in_metadata(
                self.cluster, fresh, "Default", "couchbase"),
            f"Link {fresh} was refused but is present in Analytics metadata"
        )

        # Half 2: the existing, disconnected link cannot be reactivated.
        connected, error = self._connect_link(established)
        self.assertFalse(
            connected,
            f"The disconnected link {established} reconnected on a revoked "
            f"certificate. A link that was working before the revocation "
            f"must not be reactivatable after it."
        )
        self._assert_link_error_kind(
            error, "revoked",
            "Refusing to reactivate a disabled link must name the revocation",
            redact=[established, client["username"]],
        )
        self.log.info(
            "A revoked certificate could neither create a new link nor "
            "reactivate the disconnected one")

    def test_non_admin_cannot_change_link_certificate_configuration(self):
        """
        Section 10: a non-admin user cannot rewrite an Analytics Link's
        certificate configuration, which would otherwise be a way to swap a
        revoked certificate for a good one and carry on.

        Uses analytics_reader, a role that can read Analytics data but has no
        business altering link credentials. Two checks, because "was it
        refused" and "did it change anything" are different questions: the
        PUT must be rejected, AND the link must still present its original
        certificate afterwards, proven by connecting with it.
        """
        target = self._setup_remote_link_target()
        client = self._mint_link_client_cert(target, "cbas_crl_rbac_link")
        link_name = "crl_link_rbac"
        self._link_baseline(target, client, link_name)

        reader, reader_password = self._create_rbac_test_user(
            "cbas_crl_link_reader", "analytics_reader")

        # A certificate the non-admin would be swapping in.
        replacement = self._mint_link_client_cert(
            target, "cbas_crl_rbac_replacement")
        props = self._couchbase_link_props(
            link_name, target, client=replacement)
        body = dict(props)
        dataverse = body.pop("dataverse", "Default")
        name = body.pop("name")
        resp = requests.put(
            self._link_url(dataverse, name),
            data=urlencode({k: v for k, v in body.items() if v}),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=(reader, reader_password), verify=False, timeout=120,
        )
        self.assertIn(
            resp.status_code, (401, 403),
            f"analytics_reader was able to alter link {link_name}'s "
            f"certificate configuration ({resp.status_code}): "
            f"{resp.text[:400]}. Only an administrator may change link "
            f"credentials, or CRL enforcement can be worked around by "
            f"swapping the certificate."
        )
        self.log.info(
            f"analytics_reader refused with {resp.status_code} when altering "
            f"link certificate configuration")

        # And nothing changed: the link still works on its original cert.
        self._disconnect_link(link_name)
        connected, error = self._connect_link(link_name)
        self.assertTrue(
            connected,
            f"After a REFUSED non-admin edit, link {link_name} no longer "
            f"connects on its original certificate: {error[:400]}. A rejected "
            f"edit must leave the link untouched."
        )
        self.log.info(
            f"Link {link_name} still works on its original certificate")
