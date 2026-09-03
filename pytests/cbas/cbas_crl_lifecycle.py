import datetime
import socket
import ssl
import threading
import time

import requests

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
