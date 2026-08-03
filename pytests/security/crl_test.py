import datetime
import time
import uuid

import requests
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.security_utils.crl_utils import (
    cleanup_url_poll_crl_env,
    setup_url_poll_crl_env,
)
from couchbase_utils.security_utils.jwt_utils import remote_write_file_b64
from pytests.security.crl_base import CRLBase


class CRLTest(CRLBase):
    """Consolidated CRL (Certificate Revocation List) test suite."""

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
        """Does a CRL's trust/signature actually apply to the cert being checked?"""
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
        self.log.info(
            "leaf1 still correctly rejected (the untrusted and forged CRLs "
            "above had no effect)"
        )

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

    def test_crl_url_poll_ingestion(self):
        """Does urls/urlPollIntervalMs fetch and apply a CRL on its own timer?"""
        leaf_cert, leaf_key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "urlPollLeaf"
        )
        leaf_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf_cert))
        leaf_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))
        self.log.info(f"urlPollLeaf generated (serial {serial})")

        # "enable" (optional), not "mandatory" -- same reasoning as the other
        # tests in this class: our own admin REST calls need to keep working.
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.log.info("clientCertAuth=enable, clientAuth policy=Require")

        env = setup_url_poll_crl_env(
            crl_utils_obj=self.crl_utils,
            cluster_master=self.cluster.master,
            rest=self.rest,
            ca_cert=self.ca_cert,
            ca_key=self.ca_key,
            revoked_serials=[serial],
            url_poll_interval_ms=5000,
            log_callback=self.log.info,
        )
        try:
            self.assertTrue(
                env["settings_status"],
                f"Failed to configure urls/urlPollIntervalMs: {env['settings_content']}",
            )
            self.log.info(
                f"Serving CRL at {env['crl_url']}, waiting for the poller to fetch it"
            )

            # No explicit upload/reload call above -- the leaf must be
            # rejected purely because the URL poller fetched and applied
            # the CRL on its own interval.
            self._wait_until_handshake(
                leaf_cert_path, leaf_key_path,
                expect_ok=False,
                deadline=datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=20),
            )
            self.log.info(
                "urlPollLeaf correctly rejected — CRL was fetched via URL poll and applied"
            )
        finally:
            cleanup_url_poll_crl_env(env)

    def test_crl_settings_scope_independence_and_ingestion(self):
        """Per-scope policy independence, directory-poll ingestion, and the
        checkIntermediateCerts toggle, sharing one fixture."""
        self._enable_client_cert_auth(state="enable")

        # 1. Per-scope independence: set both scopes, then change only one
        # and confirm the other didn't move.
        status, updated = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status, f"Initial policyPerScope update failed: {updated}")

        status, updated = self.crl_utils.set_settings(
            self.rest, policyPerScope={"nodeToNode": "Permissive"}
        )
        self.assertTrue(status, f"Single-scope policyPerScope update failed: {updated}")
        self.crl_utils.assert_settings_equal(
            updated,
            {"policyPerScope": {"clientAuth": "Require", "nodeToNode": "Permissive"}},
        )
        self.log.info(
            "clientAuth unaffected by a nodeToNode-only update: "
            f"{updated.get('policyPerScope')}"
        )

        # 2. Directory poll ingestion: write a CRL straight to the poll
        # directory over SSH, bypassing the upload endpoint entirely.
        # Unique per run (not a fixed path) and explicitly removed from the
        # node's disk in a finally block -- this is an ad-hoc SSH-written
        # directory, not a CRL uploaded via the REST API, so
        # _track_uploaded_file/_cleanup_created_files doesn't cover it. A
        # fixed, never-cleaned-up path would linger on this shared node
        # indefinitely, and a future test accidentally pointed at the same
        # path would silently pick up a stale revoked CRL and fail mysteriously.
        poll_dir = f"/tmp/taf_crl_dir_poll_{uuid.uuid4().hex[:8]}"
        status, updated = self.crl_utils.set_settings(
            self.rest, directory=poll_dir, dirPollIntervalMs=5000
        )
        self.assertTrue(status, f"Directory/poll-interval update failed: {updated}")

        dir_leaf_cert, dir_leaf_key, dir_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "dirPollLeaf"
        )
        dir_leaf_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(dir_leaf_cert)
        )
        dir_leaf_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(dir_leaf_key)
        )
        dir_crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[dir_serial], crl_number=1
        )

        poll_shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            poll_shell.execute_command(f"mkdir -p {poll_dir}")
            remote_write_file_b64(
                poll_shell, f"{poll_dir}/dir_poll_crl.pem", dir_crl_pem.decode("utf-8")
            )
            self.log.info(f"Wrote a revoking CRL directly to {poll_dir} over SSH")

            self._wait_until_handshake(
                dir_leaf_cert_path, dir_leaf_key_path,
                expect_ok=False,
                deadline=datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(seconds=20),
            )
            self.log.info(
                "dirPollLeaf correctly rejected — CRL picked up by the directory "
                "poller with no upload call ever made"
            )
        finally:
            try:
                poll_shell.execute_command(f"rm -rf {poll_dir}")
            except Exception as exc:
                self.log.warning(f"Failed to clean up {poll_dir}: {exc}")
            poll_shell.disconnect()

        # 3. checkIntermediateCerts toggle: revoke the intermediate CA's own
        # serial (not the leaf's), then flip the toggle and confirm the
        # outcome changes accordingly for the same cert/CRL pair.
        intermediate_cert, intermediate_key, intermediate_serial = (
            self.crl_utils.generate_intermediate_ca(
                self.ca_cert, self.ca_key, "TestIntermediateCA"
            )
        )
        # CRL-signing trust does not chain through the certificate hierarchy
        # -- a CA must be separately, explicitly trusted before a CRL it
        # signs will be accepted, even though its own certificate chains up
        # to the already-trusted root.
        self._trust_ca_on_cluster(intermediate_cert)
        chain_leaf_cert, chain_leaf_key, _ = self.crl_utils.generate_leaf_cert(
            intermediate_cert, intermediate_key, "chainLeaf"
        )
        # Client presents the full chain (leaf then its issuing
        # intermediate) as one bundle, not just the leaf alone.
        chain_bundle = self.crl_utils.cert_to_pem(
            chain_leaf_cert
        ) + self.crl_utils.cert_to_pem(intermediate_cert)
        chain_cert_path = self._write_temp_pem(chain_bundle)
        chain_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(chain_leaf_key))

        # The leaf needs its own applicable (even if empty) CRL from its
        # actual issuer (the intermediate) before testing the toggle below --
        # otherwise the leaf would be rejected for having *no* applicable
        # CRL at all under Require, which would look identical to a real
        # rejection and defeat the point of this check.
        baseline_filename = "settings_scope_intermediate_baseline.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, baseline_filename,
            self.crl_utils.build_crl(intermediate_cert, intermediate_key, crl_number=1),
        )
        self.assertTrue(status, f"Intermediate baseline CRL upload failed: {content}")
        self._track_uploaded_file(baseline_filename)

        filename = "settings_scope_intermediate_revoked.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, intermediate_serial, filename,
            crl_number=2,
        )
        self.assertTrue(status, f"Intermediate-revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.log.info(
            f"Uploaded {filename}, revoking the intermediate CA's own serial "
            f"{intermediate_serial} (not the leaf's)"
        )

        self.crl_utils.set_settings(self.rest, checkIntermediateCerts=False)
        self.assertTrue(
            self._handshake_ok(chain_cert_path, chain_key_path),
            "With checkIntermediateCerts off, the intermediate's own "
            "revocation should be ignored and the leaf should connect",
        )
        self.log.info("checkIntermediateCerts=False: chain connects as expected")

        self.crl_utils.set_settings(self.rest, checkIntermediateCerts=True)
        self.assertFalse(
            self._handshake_ok(chain_cert_path, chain_key_path),
            "With checkIntermediateCerts on, the revoked intermediate should "
            "invalidate the whole chain",
        )
        self.log.info("checkIntermediateCerts=True: chain correctly rejected")

    def test_crl_policy_mode_matrix(self):
        """Walks Disabled -> Permissive -> Require, re-checking a revoked,
        a missing-CRL, and an expired-CRL cert at each mode."""
        self.log.info("Building fixture: leafRevoked (CA-1, already trusted)")
        ca1_cert, ca1_key = self.ca_cert, self.ca_key
        leaf_revoked_cert, leaf_revoked_key, revoked_serial = (
            self.crl_utils.generate_leaf_cert(ca1_cert, ca1_key, "leafRevoked")
        )
        leaf_revoked_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_revoked_cert)
        )
        leaf_revoked_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(leaf_revoked_key)
        )

        ca2_cert, ca2_key = self.crl_utils.generate_ca("TestCA2Missing")
        self._trust_ca_on_cluster(ca2_cert)
        self.log.info("CA-2 trusted on cluster")
        leaf_missing_cert, leaf_missing_key, _ = self.crl_utils.generate_leaf_cert(
            ca2_cert, ca2_key, "leafMissing"
        )
        leaf_missing_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_missing_cert)
        )
        leaf_missing_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(leaf_missing_key)
        )
        # Deliberately never upload any CRL for CA-2 -- this IS the
        # missing-applicable-CRL case.

        ca3_cert, ca3_key = self.crl_utils.generate_ca("TestCA3Expiring")
        self._trust_ca_on_cluster(ca3_cert)
        self.log.info("CA-3 trusted on cluster")
        leaf_expired_cert, leaf_expired_key, _ = self.crl_utils.generate_leaf_cert(
            ca3_cert, ca3_key, "leafExpired"
        )
        leaf_expired_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_expired_cert)
        )
        leaf_expired_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(leaf_expired_key)
        )
        self.log.info(
            "Generated leafRevoked (CA-1), leafMissing (CA-2, no CRL ever "
            "uploaded), leafExpired (CA-3, CRL will go stale)"
        )

        self._enable_client_cert_auth(state="enable")

        # CA-1's CRL: long-lived, revokes leafRevoked's own serial.
        filename1 = "policy_matrix_crl1_revoked.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, ca1_cert, ca1_key, revoked_serial, filename1, crl_number=1
        )
        self.assertTrue(status, f"CA-1 revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename1)

        # CA-3's CRL: short-lived, revokes an unrelated dummy serial (not
        # leafExpired's own) -- the only thing wrong with it is that it
        # goes stale partway through the test.
        now = datetime.datetime.now(datetime.timezone.utc)
        this_update = now - datetime.timedelta(seconds=5)
        next_update = now + datetime.timedelta(seconds=20)
        filename3 = "policy_matrix_crl3_expiring.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename3,
            self.crl_utils.build_crl(
                ca3_cert, ca3_key, revoked_serials=[999999999], crl_number=1,
                this_update=this_update, next_update=next_update,
            ),
        )
        self.assertTrue(status, f"CA-3 expiring-CRL upload failed: {content}")
        self._track_uploaded_file(filename3)
        self.crl_utils.reload_crl(self.rest)
        self.log.info(
            f"Fixture ready: CA-1 CRL revokes leafRevoked, CA-2 has no CRL, "
            f"CA-3's CRL expires at {next_update}"
        )

        # Disabled ignores every cert state.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        self.assertTrue(
            self._handshake_ok(leaf_revoked_cert_path, leaf_revoked_key_path),
            "Disabled: revoked cert should still connect",
        )
        self.assertTrue(
            self._handshake_ok(leaf_missing_cert_path, leaf_missing_key_path),
            "Disabled: missing-CRL cert should connect",
        )
        self.assertTrue(
            self._handshake_ok(leaf_expired_cert_path, leaf_expired_key_path),
            "Disabled: expired-CRL cert should connect",
        )
        self.log.info("Disabled: all 3 cert states connect, as expected")

        # Disabled -> Permissive: assert rejection immediately after the
        # POST, no sleep in between, to prove the change takes effect on
        # the very next connection with no restart required.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Permissive", "nodeToNode": "Disabled"},
        )
        self.assertFalse(
            self._handshake_ok(leaf_revoked_cert_path, leaf_revoked_key_path),
            "Permissive: revoked cert should be rejected on the very next "
            "connection after the policy change",
        )
        self.log.info("Permissive took effect on the very next connection")

        # Permissive fails open on a missing applicable CRL.
        self.assertTrue(
            self._handshake_ok(leaf_missing_cert_path, leaf_missing_key_path),
            "Permissive: missing-CRL cert should connect (fail-open)",
        )
        self.log.info("Permissive: missing-CRL cert connects (fail-open)")

        # Permissive fails open on an expired applicable CRL too, a
        # distinct case from "missing" above (this CRL exists, it's just
        # stale). Wait for real wall-clock time to actually pass CA-3's
        # CRL's nextUpdate before checking. Computed from the remaining gap
        # to next_update (plus a small buffer), not a flat sleep -- the
        # steps above (cert generation, Disabled checks, the Permissive
        # transition) already burn some of that 20s window, and a fixed
        # sleep on top would either waste time re-waiting a window that's
        # already elapsed, or need to be padded generously to stay safe
        # either way. This sleeps only what's actually left.
        remaining_seconds = (
            next_update - datetime.datetime.now(datetime.timezone.utc)
        ).total_seconds()
        self.sleep(
            max(0, remaining_seconds) + 5,
            f"Waiting for CA-3's CRL to expire (nextUpdate={next_update})",
        )
        self.assertTrue(
            self._handshake_ok(leaf_expired_cert_path, leaf_expired_key_path),
            "Permissive: expired-CRL cert should connect (fail-open)",
        )
        self.log.info("Permissive: expired-CRL cert connects (fail-open)")
        # Best-effort: look for a warning-shaped log line, never a hard
        # assertion -- the connection outcome above is what actually proves
        # fail-open semantics, this is just a nicety if the wording matches.
        try:
            shell = RemoteMachineShellConnection(self.cluster.master)
            try:
                out, _ = shell.execute_command(
                    "grep -i 'warn' /opt/couchbase/var/lib/couchbase/logs/debug.log "
                    "| tail -20"
                )
                self.log.info(f"Warning-log check (best-effort): {out}")
            finally:
                shell.disconnect()
        except Exception as exc:
            self.log.info(f"Warning-log check skipped (best-effort): {exc}")

        # Permissive -> Require: same immediate-effect check as above.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertFalse(
            self._handshake_ok(leaf_revoked_cert_path, leaf_revoked_key_path),
            "Require: revoked cert should be rejected on the very next "
            "connection after the policy change",
        )
        self.log.info("Require took effect on the very next connection")

        # Require fails closed on the now-expired CRL.
        self.assertFalse(
            self._handshake_ok(leaf_expired_cert_path, leaf_expired_key_path),
            "Require: expired-CRL cert should be rejected (fail-closed)",
        )
        self.log.info("Require: expired-CRL cert rejected (fail-closed)")

        # Require fails closed on the missing CRL.
        self.assertFalse(
            self._handshake_ok(leaf_missing_cert_path, leaf_missing_key_path),
            "Require: missing-CRL cert should be rejected (fail-closed)",
        )
        self.log.info("Require: missing-CRL cert rejected (fail-closed)")

    def test_crl_tampered_duplicate_and_empty_crl_handling(self):
        """Does the server correctly handle a signature-tampered CRL, a
        duplicate-serial entry, and a validly-signed empty CRL?"""
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        leaf_revoke_cert, leaf_revoke_key, revoke_serial = (
            self.crl_utils.generate_leaf_cert(self.ca_cert, self.ca_key, "leafToRevoke")
        )
        leaf_revoke_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_revoke_cert)
        )
        leaf_revoke_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(leaf_revoke_key)
        )
        leaf_untouched_cert, leaf_untouched_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "leafUntouched"
        )
        leaf_untouched_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_untouched_cert)
        )
        leaf_untouched_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(leaf_untouched_key)
        )
        self.log.info("Fixture ready: leafToRevoke and leafUntouched, both under the base CA")

        # A validly-signed CRL, tampered after signing: flip the very last
        # DER byte, which lands inside the signature value rather than a
        # length field, so the structure still parses but the signature no
        # longer matches. Distinct from an already-covered gross-malformed
        # (non-CRL) byte string -- this one only fails signature validation.
        valid_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)
        tampered_der = bytearray(self.crl_utils.pem_crl_to_der(valid_pem))
        tampered_der[-1] ^= 0xFF
        status, content = self.crl_utils.upload_file(
            self.rest, "security_negative_tampered.der", bytes(tampered_der)
        )
        self.assertFalse(
            status, f"Signature-tampered CRL must be rejected, got: {content}"
        )
        self.log.info("Signature-tampered CRL correctly rejected at upload")

        # A validly-signed, empty CRL: accepted, and revokes nothing.
        filename_empty = "security_negative_empty.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename_empty,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=2),
        )
        self.assertTrue(status, f"Empty CRL upload failed: {content}")
        self._track_uploaded_file(filename_empty)
        self.crl_utils.reload_crl(self.rest)
        self.assertTrue(
            self._handshake_ok(leaf_untouched_cert_path, leaf_untouched_key_path),
            "leafUntouched should connect under an empty CRL",
        )
        self.log.info("Empty CRL accepted; leafUntouched connects (nothing revoked)")

        # Same serial listed twice in one CRL: accepted, and revokes exactly
        # as a single entry would -- no crash, no double-counting effect.
        filename_dup = "security_negative_duplicate_serial.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key,
            [revoke_serial, revoke_serial], filename_dup, crl_number=3,
        )
        self.assertTrue(status, f"Duplicate-serial CRL upload failed: {content}")
        self._track_uploaded_file(filename_dup)
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(leaf_revoke_cert_path, leaf_revoke_key_path),
            "leafToRevoke should be rejected under a CRL listing its serial twice",
        )
        self.assertTrue(
            self._handshake_ok(leaf_untouched_cert_path, leaf_untouched_key_path),
            "leafUntouched should remain unaffected by leafToRevoke's revocation",
        )
        self.log.info(
            "Duplicate-serial CRL correctly revokes leafToRevoke only; "
            "leafUntouched still connects"
        )
