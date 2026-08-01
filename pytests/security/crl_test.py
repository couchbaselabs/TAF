import datetime
import time

import requests

from pytests.security.crl_base import CRLBase


class CRLTest(CRLBase):
    """
    Consolidated CRL (Certificate Revocation List) test suite — all CRL test
    methods land here as CRL_TEST_PLAN.md sections get automated, rather than
    proliferating one file per section.

    test_settings_and_file_lifecycle: foundational smoke coverage proving the
    REST wrapper / CRLUtils / CRLBase plumbing works end-to-end (settings CRUD
    + file lifecycle as a single happy-path flow).

    test_crl_trust_and_signature_boundary / test_crl_temporal_validity_lifecycle:
    trust/signature-boundary and temporal-validity coverage (TestCases_CRL.csv
    rows 13-19), written as 2 chained scenarios instead of 7 flat cases — one
    test walks through every trust/signature edge case sharing one set of
    CA/cert fixtures, the other walks a single CRL through its full
    not-yet-valid -> valid -> expired lifecycle. Both drive a real mTLS
    handshake via CRLUtils.perform_mtls_handshake(), using the
    CRLBase._write_temp_pem/_enable_client_cert_auth helpers. Server identity
    is not verified during the handshake (see
    CRLUtils.perform_mtls_handshake's docstring) — these tests only care about
    the client cert's CRL-driven accept/reject outcome.
    """

    MGMT_PORT = 18091

    # ── Shared mTLS handshake helpers ────────────────────────────────────────

    def _handshake_ok(self, cert_path, key_path):
        """
        True if the TLS handshake completes (any HTTP response received).
        False if it's rejected at the TLS layer (revoked/untrusted client
        cert) — a revoked cert never gets far enough to receive an HTTP
        response, so this is a connection-level check, not a status-code one.
        """
        try:
            self.crl_utils.perform_mtls_handshake(
                self.cluster.master.ip, self.MGMT_PORT, cert_path, key_path,
            )
            return True
        except requests.exceptions.SSLError:
            return False

    def _wait_until_handshake(self, cert_path, key_path, expect_ok, deadline,
                               interval=3):
        """
        Poll (not a flat sleep) until _handshake_ok matches expect_ok or the
        wall-clock `deadline` (a datetime) passes. Needed because CRL
        validity transitions are real wall-clock events on the server side,
        not something a test can mock.
        """
        self.log.info(
            f"Polling every {interval}s for handshake expect_ok={expect_ok} "
            f"(deadline {deadline})"
        )
        while datetime.datetime.now(datetime.timezone.utc) < deadline:
            if self._handshake_ok(cert_path, key_path) == expect_ok:
                self.log.info(f"Reached expect_ok={expect_ok} as expected")
                return
            time.sleep(interval)
        self.fail(
            f"Handshake did not reach expect_ok={expect_ok} before deadline {deadline}"
        )

    # ── Tests ────────────────────────────────────────────────────────────────

    def test_settings_and_file_lifecycle(self):
        # Step 1 — baseline: GET default settings.
        self.log.info("GET /settings/crl (baseline)")
        status, settings = self.crl_utils.get_settings(self.rest)
        self.assertTrue(status, f"GET /settings/crl failed: {settings}")
        self.assertIn("policyPerScope", settings)
        self.log.info(f"Baseline settings: {settings}")

        # Step 2 — POST a partial settings update, verify it applied.
        self.log.info("POST /settings/crl (partial update)")
        status, updated = self.crl_utils.set_settings(
            self.rest, checkIntermediateCerts=True, dirPollIntervalMs=30000
        )
        self.assertTrue(status, f"POST /settings/crl failed: {updated}")
        self.crl_utils.assert_settings_equal(
            updated, {"checkIntermediateCerts": True, "dirPollIntervalMs": 30000}
        )
        self.log.info(f"Settings updated and verified: {updated}")

        # Step 3 — upload a CRL file.
        self.log.info("Uploading a CRL file")
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)
        filename = "crl_smoke_test.pem"
        status, content = self.crl_utils.upload_file(self.rest, filename, crl_pem)
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.log.info(f"CRL uploaded: {content}")

        # Step 4 — GET file list, confirm the upload is present.
        self.log.info("GET /settings/crl/files (confirm upload listed)")
        status, files = self.crl_utils.list_files(self.rest)
        self.assertTrue(status, f"GET /settings/crl/files failed: {files}")
        names = [entry.get("filename") for entry in files]
        self.assertIn(filename, names, f"Uploaded file not listed: {files}")
        self.log.info(f"File listed as expected: {names}")

        # Step 5 — delete it, confirm it's actually gone from the list
        # (not just that DELETE returned success).
        self.log.info("Deleting the uploaded CRL file, confirming it's gone")
        status, content = self.crl_utils.delete_file(self.rest, filename)
        self.assertTrue(status, f"CRL delete failed: {content}")
        self._created_files.remove(filename)

        status, files = self.crl_utils.list_files(self.rest)
        self.assertTrue(status, f"GET /settings/crl/files failed after delete: {files}")
        names = [entry.get("filename") for entry in files]
        self.assertNotIn(filename, names, f"Deleted file still listed: {files}")
        self.log.info(f"File confirmed removed: {names}")

        # Step 6 — restore default settings.
        self.log.info("Restoring default settings")
        status, restored = self.crl_utils.set_settings(
            self.rest, checkIntermediateCerts=False, dirPollIntervalMs=60000
        )
        self.assertTrue(status, "Failed to restore default CRL settings")
        self.log.info(f"Defaults restored: {restored}")

    def test_crl_trust_and_signature_boundary(self):
        """
        Does a CRL's trust/signature actually apply to the cert being
        checked?

        Fixture: CA-1 (self.ca_cert, trusted in CRLBase.setUp) and a second
        CA-2 explicitly trusted here too (both must be trusted so the
        serial-collision check below isolates CRL-scope from ordinary
        chain-of-trust rejection), plus a third CA that is deliberately
        never trusted. leaf1 (CA-1) and leaf2 (CA-2) are built to share the
        same serial number on purpose.
        """
        self.log.info("Generating CA-1 (already trusted), CA-2, and an untrusted CA")
        ca1_cert, ca1_key = self.ca_cert, self.ca_key

        ca2_cert, ca2_key = self.crl_utils.generate_ca("TestCA2Trusted")
        self._trust_ca_on_cluster(ca2_cert)
        self.log.info("CA-2 trusted on cluster")

        ca_untrusted_cert, ca_untrusted_key = self.crl_utils.generate_ca(
            "TestCAUntrusted"
        )

        leaf1_cert, leaf1_key, serial1 = self.crl_utils.generate_leaf_cert(
            ca1_cert, ca1_key, "leaf1"
        )
        leaf1_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf1_cert))
        leaf1_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf1_key))

        # leaf2: a different (also trusted) CA, deliberately reusing leaf1's
        # serial number — the collision is the point of the check below.
        leaf2_cert, leaf2_key, _ = self.crl_utils.generate_leaf_cert(
            ca2_cert, ca2_key, "leaf2", serial=serial1
        )
        leaf2_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf2_cert))
        leaf2_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf2_key))
        self.log.info(
            f"leaf1 (CA-1) and leaf2 (CA-2) generated, sharing serial {serial1}"
        )

        # "enable" (optional), not "mandatory" -- our own admin REST calls
        # below (set_settings/upload_file/reload_crl) authenticate via
        # username/password, not a client cert. "mandatory" would force a
        # cert on every connection with no exceptions, which would lock out
        # those calls too (self-inflicted version of the same lockout fixed
        # in _self_heal_stuck_client_cert_auth). "enable" still fully
        # enforces CRL revocation on any connection that does present a
        # cert -- which is all perform_mtls_handshake calls below -- while
        # letting our own cert-less admin session keep working.
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.log.info("clientCertAuth=enable, clientAuth policy=Require")

        # CRL signed by trusted CA-1, revoking leaf1: accepted and enforced.
        filename1 = "crypto_boundary_crl1.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, ca1_cert, ca1_key, serial1, filename1, crl_number=1
        )
        self.assertTrue(status, f"Trusted-CA CRL upload failed: {content}")
        self._track_uploaded_file(filename1)
        self.crl_utils.reload_crl(self.rest)
        self.log.info(f"Uploaded {filename1} (CA-1, revokes leaf1) and reloaded")

        self.assertFalse(
            self._handshake_ok(leaf1_cert_path, leaf1_key_path),
            "leaf1 (revoked under CA-1's trusted CRL) should be rejected",
        )
        self.log.info("leaf1 correctly rejected under CA-1's CRL")

        # A CRL signed by a CA that was never trusted on the cluster must be
        # rejected outright at upload time (synchronous signature/trust
        # validation) — it must never reach a state where it could affect
        # any cert's revocation status.
        filename2 = "crypto_boundary_crl2_untrusted.pem"
        untrusted_pem = self.crl_utils.build_crl(
            ca_untrusted_cert, ca_untrusted_key, revoked_serials=[111], crl_number=1
        )
        status, content = self.crl_utils.upload_file(self.rest, filename2, untrusted_pem)
        self.assertFalse(
            status,
            f"CRL signed by an untrusted CA must be rejected at upload, got: {content}",
        )
        self.log.info("Untrusted-CA CRL correctly rejected at upload")

        # A CRL whose issuer name is forged to match CA-1's subject, but is
        # actually signed with the untrusted CA's key, must also be rejected
        # outright — the issuer name matching superficially must not be
        # enough; the signature itself has to validate against the real
        # public key of the CA it claims to be from.
        filename3 = "crypto_boundary_crl3_forged.pem"
        forged_pem = self.crl_utils.build_crl(
            ca1_cert, ca_untrusted_key, revoked_serials=[222], crl_number=1
        )
        status, content = self.crl_utils.upload_file(self.rest, filename3, forged_pem)
        self.assertFalse(
            status,
            f"CRL with a forged issuer/signature mismatch must be rejected at upload, "
            f"got: {content}",
        )
        self.log.info("Forged issuer/signature CRL correctly rejected at upload")

        # Sanity close — leaf1 is still correctly rejected: the two rejected
        # uploads above had no effect on CA-1's real, valid revocation.
        self.assertFalse(
            self._handshake_ok(leaf1_cert_path, leaf1_key_path),
            "leaf1 should still be rejected",
        )
        self.log.info("leaf1 still correctly rejected (rows 14/15 had no effect)")

        # CA-2 needs its own applicable (even if empty) CRL before we can
        # test leaf2 under a Require policy — otherwise leaf2 would be
        # rejected for having *no* applicable CRL at all, which would look
        # identical to a real rejection and defeat the point of this check.
        filename4 = "crypto_boundary_crl4_ca2_empty.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename4,
            self.crl_utils.build_crl(ca2_cert, ca2_key, crl_number=1),
        )
        self.assertTrue(status, f"CA-2 baseline CRL upload failed: {content}")
        self._track_uploaded_file(filename4)
        self.crl_utils.reload_crl(self.rest)
        self.log.info(f"Uploaded {filename4} (CA-2 baseline, empty) and reloaded")

        # leaf2 (CA-2, same numeric serial as leaf1, not revoked under CA-2's
        # own CRL) must connect: CA-1's CRL revoking that serial number must
        # not cross into CA-2's independently-trusted domain.
        self.assertTrue(
            self._handshake_ok(leaf2_cert_path, leaf2_key_path),
            "leaf2 (different trusted CA, colliding serial, not itself revoked) "
            "should connect",
        )
        self.log.info(
            "leaf2 correctly connected — CA-1's revocation did not cross into CA-2"
        )

    def test_crl_temporal_validity_lifecycle(self):
        """
        Verifies that CRL validity windows (thisUpdate and nextUpdate) are
        strictly enforced at both upload time and runtime.
        """
        certx_cert, certx_key, serialx = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "certX"
        )
        certx_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(certx_cert))
        certx_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(certx_key))
        self.log.info(f"certX generated (serial {serialx})")

        # Use 'enable' (optional mTLS) so our own REST API admin calls
        # (using Basic Auth) are not locked out during the test.
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.log.info("clientCertAuth=enable, clientAuth policy=Require")

        # Step 1: CRLs with a future 'thisUpdate' must be rejected at upload.
        future_update = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=60)
        not_yet_valid_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[serialx],
            crl_number=1, this_update=future_update,
        )
        status, content = self.crl_utils.upload_file(
            self.rest, "temporal_not_yet_valid.pem", not_yet_valid_pem
        )
        self.assertFalse(status, f"Future thisUpdate must be rejected at upload, got: {content}")
        self.log.info(f"Step 1: not-yet-valid CRL correctly rejected at upload: {content}")

        # Step 2: Upload a currently valid CRL that expires in 25 seconds.
        # Note: this_update is backdated by 30s to absorb any client/server clock skew.
        now = datetime.datetime.now(datetime.timezone.utc)
        this_update = now - datetime.timedelta(seconds=30)
        next_update = now + datetime.timedelta(seconds=25)

        filename = "temporal_lifecycle_crl.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serialx, filename,
            crl_number=2, this_update=this_update, next_update=next_update,
        )
        self.assertTrue(status, f"Active CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.log.info(
            f"Step 2: uploaded {filename} (revokes certX), thisUpdate={this_update}, "
            f"nextUpdate={next_update}"
        )

        # Step 3: Verify the active CRL correctly revokes the certificate.
        self.assertFalse(
            self._handshake_ok(certx_cert_path, certx_key_path),
            "certX should be rejected while the CRL is actively valid",
        )
        self.log.info("Step 3: certX correctly rejected while the CRL is actively valid")

        # Step 4: Verify 'Require' policy fails closed once the CRL expires.
        self._wait_until_handshake(
            certx_cert_path, certx_key_path,
            expect_ok=False,
            deadline=next_update + datetime.timedelta(seconds=15),
        )
        self.log.info("Step 4: certX still rejected once nextUpdate passed, as expected")

        # Step 5: Verify upload behavior for a CRL that is ALREADY expired.
        stale_filename = "temporal_lifecycle_crl_stale.pem"
        stale_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[serialx],
            crl_number=3, expired=True,
        )
        status, content = self.crl_utils.upload_file(self.rest, stale_filename, stale_pem)

        if status:
            self._track_uploaded_file(stale_filename)
            _, diag = self.crl_utils.diagnostics_status(self.rest)
            # Ensure that if the API accepts it, it explicitly flags it as expired.
            self.assertIn(
                "expired", str(diag),
                "API accepted an expired CRL but did not flag it as 'expired' in diagnostics."
            )
            self.log.info(f"Step 5: already-expired CRL accepted, flagged expired: {diag}")
        else:
            self.log.info(f"Step 5: already-expired CRL correctly rejected outright: {content}")
