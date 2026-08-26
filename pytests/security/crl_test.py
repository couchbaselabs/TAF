import base64
import concurrent.futures
import datetime
import ssl
import statistics
import threading
import time
import uuid

import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.x509.oid import ExtendedKeyUsageOID
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.security_utils.crl_utils import (
    audit_keyword_count,
    cleanup_url_poll_crl_env,
    find_remote_pid,
    get_audit_event,
    grep_remote_log,
    setup_url_poll_crl_env,
    tail_remote_log,
)
from couchbase_utils.security_utils.jwt_utils import remote_write_file_b64
from pytests.security.crl_base import CRLBase


class CRLTest(CRLBase):
    """Consolidated CRL (Certificate Revocation List) test suite."""

    MGMT_PORT = 18091
    KV_SSL_PORT = 11207
    AUDIT_LOG_PATH = "/opt/couchbase/var/lib/couchbase/logs/current-audit.log"
    DEBUG_LOG_PATH = "/opt/couchbase/var/lib/couchbase/logs/debug.log"

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

    def _assert_audit_event_shape(self, event, event_id):
        """
        Common actor/timestamp/network-envelope assertions shared by every
        CRL audit event (8307-8310) -- see crl_utils.get_audit_event(). The
        actor is hardcoded to the Administrator REST user since that's the
        only identity every admin-API call in this suite ever authenticates
        as; live-verified (not just source-derived) that a plain Basic-Auth
        REST call has no sessionid/authenticated_userid and that the
        'admin' domain is reported as "builtin" in the actual log line.
        """
        self.assertIsNotNone(
            event, f"No audit event with id={event_id} found in the audit log"
        )
        self.assertEqual(
            event.get("real_userid"), {"domain": "builtin", "user": "Administrator"},
            f"Unexpected actor for audit event {event_id}: {event}",
        )
        self.assertNotIn(
            "sessionid", event,
            f"Unexpected sessionid on a plain Basic-Auth REST call (event {event_id})",
        )
        for key in ("remote", "local"):
            self.assertIn(key, event, f"Missing '{key}' in audit event {event_id}")
            self.assertIn("ip", event[key])
            self.assertIn("port", event[key])
        timestamp = event.get("timestamp")
        self.assertIsNotNone(timestamp, f"Missing timestamp in audit event {event_id}")
        parsed = datetime.datetime.fromisoformat(timestamp)
        delta = abs((datetime.datetime.now(datetime.timezone.utc) - parsed).total_seconds())
        self.assertLess(
            delta, 120,
            f"Audit event {event_id} timestamp {timestamp!r} is not close "
            f"to wall-clock time (delta={delta:.1f}s)",
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

        # Step 6 — directory validation: POST accepts any path regardless
        # of whether it actually exists -- there's no synchronous check.
        # The two failure modes are only distinguishable later, via
        # diagnostics/status: a nonexistent path settles to "notFound"
        # with an empty errors list (silent -- indistinguishable from a
        # poll directory intentionally left unconfigured), while an
        # existing-but-permission-denied one settles to "unreadable" with
        # a populated errors list. Known gap: a typo'd path produces no
        # observable error anywhere.
        self.log.info("POST /settings/crl with a nonexistent directory path")
        node_key = f"{self.cluster.master.ip}:8091"
        status, updated = self.crl_utils.set_settings(
            self.rest, directory="/nonexistent/totally/bogus/path/crls"
        )
        self.assertTrue(
            status, f"A nonexistent directory should still be accepted at "
            f"POST time (no synchronous path validation): {updated}"
        )
        deadline = time.monotonic() + 30
        poll_dir = {}
        while time.monotonic() < deadline:
            status, diag = self.crl_utils.diagnostics_status(self.rest)
            poll_dir = diag.get(node_key, {}).get("pollDirectory", {})
            if poll_dir.get("directory") == "/nonexistent/totally/bogus/path/crls":
                break
            time.sleep(2)
        self.assertEqual(
            poll_dir.get("status"), "notFound",
            f"Expected a nonexistent path to settle to 'notFound': {poll_dir}",
        )
        self.assertEqual(
            poll_dir.get("errors"), [],
            f"Known gap: a nonexistent/typo'd poll directory produces no "
            f"observable error anywhere -- 'notFound' with an empty errors "
            f"list is indistinguishable from an intentionally-unconfigured "
            f"poll directory. Got: {poll_dir}",
        )
        self.log.info(
            "Nonexistent directory: accepted at POST time, settles to a "
            "silent 'notFound' with no error surfaced"
        )

        # Contrast: a directory that exists but the couchbase user can't
        # read DOES surface a real error, via the same field.
        unreadable_dir = "/opt/couchbase/var/lib/couchbase/settings_test_unreadable"
        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            shell.execute_command(f"mkdir -p {unreadable_dir}")
            shell.execute_command(f"chmod 000 {unreadable_dir}")
            status, updated = self.crl_utils.set_settings(
                self.rest, directory=unreadable_dir
            )
            self.assertTrue(status, f"POST /settings/crl failed: {updated}")
            deadline = time.monotonic() + 30
            poll_dir = {}
            while time.monotonic() < deadline:
                status, diag = self.crl_utils.diagnostics_status(self.rest)
                poll_dir = diag.get(node_key, {}).get("pollDirectory", {})
                if poll_dir.get("status") == "unreadable":
                    break
                time.sleep(2)
            self.assertEqual(
                poll_dir.get("status"), "unreadable",
                f"Expected an existing-but-permission-denied directory to "
                f"report 'unreadable': {poll_dir}",
            )
            self.assertTrue(
                poll_dir.get("errors"),
                f"Unlike 'notFound', 'unreadable' should populate the "
                f"errors list: {poll_dir}",
            )
        finally:
            shell.execute_command(f"chmod 755 {unreadable_dir} && rm -rf {unreadable_dir}")
            shell.disconnect()
        self.log.info(
            "Existing-but-unreadable directory correctly surfaces a real "
            "error, unlike the silent 'notFound' case"
        )

        # Step 7 — restore default settings.
        self.log.info("Restoring default settings")
        status, restored = self.crl_utils.set_settings(
            self.rest, checkIntermediateCerts=False, dirPollIntervalMs=60000,
            directory="/opt/couchbase/var/lib/couchbase/inbox/crls",
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
        # Confirm rejection was actually the issuer-trust check, not a
        # generic failure.
        self.assertIn(
            "issuer", str(content.get("error", "")).lower(),
            f"Expected a CRL-issuer-not-trusted error, got: {content}",
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
        # Same expected error as the untrusted-CA case -- the server doesn't
        # distinguish "right name, wrong key" from "unknown issuer".
        self.assertIn(
            "issuer", str(content.get("error", "")).lower(),
            f"Expected a CRL-issuer-not-trusted error, got: {content}",
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
        # certY is never revoked -- it's the control that proves CRL expiry
        # itself triggers fail-closed (certX alone can't, since it's already
        # rejected for being revoked regardless of expiry).
        certy_cert, certy_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "certY"
        )
        certy_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(certy_cert))
        certy_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(certy_key))
        self.log.info(f"certX generated (serial {serialx}); certY generated (never revoked)")

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
        # Confirm rejection was actually the date check, not a generic failure.
        self.assertIn(
            "not yet valid", str(content.get("error", "")).lower(),
            f"Expected a 'not yet valid' validation error, got: {content}",
        )
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

        # Step 3: Verify the active CRL correctly revokes certX, and leaves
        # certY (never revoked, same applicable CRL) alone.
        self.assertFalse(
            self._handshake_ok(certx_cert_path, certx_key_path),
            "certX should be rejected while the CRL is actively valid",
        )
        self.assertTrue(
            self._handshake_ok(certy_cert_path, certy_key_path),
            "certY should connect while the CRL is actively valid and it "
            "isn't revoked",
        )
        self.log.info(
            "Step 3: certX correctly rejected, certY correctly connects, "
            "while the CRL is actively valid"
        )

        # Step 4: Verify 'Require' fails closed once the CRL expires. Must
        # genuinely wait for next_update to pass -- checking certX alone
        # would pass instantly (already revoked) without proving anything
        # about expiry.
        remaining_seconds = (
            next_update - datetime.datetime.now(datetime.timezone.utc)
        ).total_seconds()
        self.sleep(
            max(0, remaining_seconds) + 5,
            f"Waiting for the CRL to expire (nextUpdate={next_update})",
        )
        self.assertFalse(
            self._handshake_ok(certy_cert_path, certy_key_path),
            "Require: certY should now be rejected -- the CRL is stale, "
            "fails closed, even though certY itself was never revoked",
        )
        self.assertFalse(
            self._handshake_ok(certx_cert_path, certx_key_path),
            "certX should still be rejected once the CRL has expired",
        )
        self.log.info(
            "Step 4: certY flipped from connecting to rejected purely due to "
            "CRL expiry; certX remains rejected"
        )

        # Step 5: Verify upload behavior for a CRL that is ALREADY expired.
        stale_filename = "temporal_lifecycle_crl_stale.pem"
        stale_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[serialx],
            crl_number=3, expired=True,
        )
        status, content = self.crl_utils.upload_file(self.rest, stale_filename, stale_pem)

        # Deterministic: an already-expired CRL is rejected outright at
        # upload, not accepted-and-flagged.
        self.assertFalse(
            status, f"An already-expired CRL must be rejected at upload, got: {content}"
        )
        self.assertIn(
            "expired", str(content.get("error", "")).lower(),
            f"Expected an 'expired' validation error, got: {content}",
        )
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

        # Baseline empty CRL, uploaded directly (not via URL): without this,
        # urlPollLeaf has no applicable CRL yet and Require would fail
        # closed regardless of whether the poller works.
        baseline_filename = "url_poll_baseline.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, baseline_filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1),
        )
        self.assertTrue(status, f"Baseline CRL upload failed: {content}")
        self._track_uploaded_file(baseline_filename)
        self.assertTrue(
            self._handshake_ok(leaf_cert_path, leaf_key_path),
            "urlPollLeaf should connect before the URL-polled revoking CRL "
            "is ever fetched -- if this fails, the later rejection wouldn't "
            "prove the URL poller did anything",
        )
        self.log.info("Baseline confirmed: urlPollLeaf connects before URL polling begins")

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
        # Confirm rejection was actually CRL validation, not a generic failure.
        self.assertIn(
            "CRL validation failed", str(content.get("error", "")),
            f"Expected a CRL validation error, got: {content}",
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
        # Baseline: leafToRevoke also connects before it's actually revoked.
        self.assertTrue(
            self._handshake_ok(leaf_revoke_cert_path, leaf_revoke_key_path),
            "leafToRevoke should also connect under an empty CRL, before it's revoked",
        )
        self.log.info(
            "Empty CRL accepted; both leafUntouched and leafToRevoke connect "
            "(nothing revoked yet)"
        )

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

    def test_mtls_client_cert_auth_mode_matrix(self):
        """Optional vs Mandatory clientCertAuth x {no cert, valid cert,
        revoked cert}."""
        valid_user, valid_password = self._create_rbac_test_user(
            "mtls_matrix_valid_user", "admin"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, valid_user
        )
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))

        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "mtls_matrix_revoked_user"
        )
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))

        filename = "mtls_matrix_crl.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename, crl_number=1
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.log.info(
            f"Fixture ready: {valid_user} (not revoked), mtls_matrix_revoked_user "
            f"(revoked, serial {revoked_serial})"
        )

        base_url = f"https://{self.cluster.master.ip}:{self.MGMT_PORT}"
        auth = (valid_user, valid_password)

        # ── Optional mode ────────────────────────────────────────────────────
        self._enable_client_cert_auth(state="enable")

        # No cert, password only -> succeeds via non-cert auth.
        r = requests.get(f"{base_url}/whoami", auth=auth, verify=False, timeout=30)
        self.assertEqual(
            r.status_code, 200,
            f"Optional mode: password-only auth (no cert) should succeed, got "
            f"{r.status_code}: {r.text}",
        )
        self.log.info("Optional + no cert + password -> succeeded")

        # Valid cert, no password header -> succeeds via cert, genuinely
        # authenticated as valid_user (not just a TLS-layer pass).
        whoami = self.crl_utils.get_identity_via_mtls(
            self.cluster.master.ip, self.MGMT_PORT, valid_cert_path, valid_key_path,
        )
        self.assertEqual(
            whoami.get("id"), valid_user,
            f"Optional mode: valid cert should authenticate as {valid_user}, "
            f"got whoami={whoami}",
        )
        self.log.info(f"Optional + valid cert -> authenticated as {valid_user}")

        # Revoked cert presented together with a valid password -- must be
        # hard-rejected at the TLS layer, not silently fall back to password.
        with self.assertRaises(
            requests.exceptions.SSLError,
            msg="Optional mode: revoked cert must be rejected even with a valid "
                "password available, not silently fall back to password auth",
        ):
            requests.get(
                f"{base_url}/whoami",
                cert=(revoked_cert_path, revoked_key_path),
                auth=auth, verify=False, timeout=30,
            )
        self.log.info("Optional + revoked cert + password -> rejected, no fallback")

        # ── Mandatory mode ───────────────────────────────────────────────────
        # try/finally: 'mandatory' blocks password-only admin calls, so we
        # must revert before tearDown's own cleanup needs one, even on failure.
        self._enable_client_cert_auth(state="mandatory")
        try:
            # No cert at all (password-only) -> rejected, cert is
            # non-negotiable in mandatory mode.
            with self.assertRaises(
                requests.exceptions.SSLError,
                msg="Mandatory mode: a request with no client cert must be rejected",
            ):
                requests.get(f"{base_url}/whoami", auth=auth, verify=False, timeout=30)
            self.log.info("Mandatory + no cert -> rejected")

            # Valid, non-revoked cert -> succeeds.
            whoami = self.crl_utils.get_identity_via_mtls(
                self.cluster.master.ip, self.MGMT_PORT, valid_cert_path, valid_key_path,
            )
            self.assertEqual(
                whoami.get("id"), valid_user,
                f"Mandatory mode: valid cert should still authenticate as {valid_user}, "
                f"got whoami={whoami}",
            )
            self.log.info("Mandatory + valid cert -> authenticated correctly")

            # Revoked cert -> rejected.
            self.assertFalse(
                self._handshake_ok(revoked_cert_path, revoked_key_path),
                "Mandatory mode: revoked cert should be rejected",
            )
            self.log.info("Mandatory + revoked cert -> rejected")
        finally:
            self._disable_client_cert_auth()

    def test_mtls_revocation_ordering_and_cross_ca_isolation(self):
        """Revocation checked before identity/RBAC; valid cert unaffected by
        an unrelated CA's CRL; cross-CA isolation under a serial collision."""
        self._enable_client_cert_auth(state="enable")

        # A revoked cert mapped to a full-admin user: if revocation ran after
        # RBAC, this would reach the HTTP layer instead of failing at TLS.
        admin_user, _ = self._create_rbac_test_user(
            "mtls_ordering_admin_user", "admin"
        )
        admin_cert, admin_key, admin_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, admin_user
        )
        admin_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(admin_cert))
        admin_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(admin_key))

        filename1 = "mtls_ordering_crl.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, admin_serial, filename1, crl_number=1
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename1)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        with self.assertRaises(
            requests.exceptions.SSLError,
            msg="A revoked cert mapped to a full-admin RBAC user must still be "
                "rejected at the TLS layer -- revocation must be checked before "
                "identity extraction/RBAC, never after",
        ):
            self.crl_utils.perform_mtls_handshake(
                self.cluster.master.ip, self.MGMT_PORT, admin_cert_path, admin_key_path,
            )
        self.log.info(
            "Revoked cert mapped to a full-admin user rejected at the TLS "
            "layer -- revocation is checked before RBAC, as required"
        )

        # A second, independently-trusted CA. leaf2 deliberately reuses
        # admin_serial -- a serial actually revoked under CA-1 -- not an
        # arbitrary unrevoked one, so the collision actually proves isolation.
        ca2_cert, ca2_key = self.crl_utils.generate_ca("TestCA2MtlsIsolation")
        self._trust_ca_on_cluster(ca2_cert)

        valid_user, _ = self._create_rbac_test_user(
            "mtls_isolation_valid_user", "admin"
        )
        leaf1_cert, leaf1_key, serial1 = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, valid_user
        )
        leaf1_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf1_cert))
        leaf1_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf1_key))

        leaf2_cn = "mtls_isolation_ca2_leaf"
        leaf2_cert, leaf2_key, _ = self.crl_utils.generate_leaf_cert(
            ca2_cert, ca2_key, leaf2_cn, serial=admin_serial
        )
        leaf2_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf2_cert))
        leaf2_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf2_key))

        filename2 = "mtls_isolation_crl2_ca2.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename2,
            self.crl_utils.build_crl(
                ca2_cert, ca2_key, revoked_serials=[999999], crl_number=1,
            ),
        )
        self.assertTrue(status, f"CA-2 unrelated CRL upload failed: {content}")
        self._track_uploaded_file(filename2)
        self.log.info(
            f"Fixture ready: leaf1 (base CA, serial {serial1}, not revoked), "
            f"CA-2 trusted with its own CRL revoking an unrelated serial, "
            f"leaf2 (CA-2, serial {admin_serial} -- the serial CA-1 has "
            f"actually revoked above, not merely an arbitrary unrevoked one -- "
            f"not itself revoked under CA-2's own CRL)"
        )

        # leaf1 unaffected by CA-2's unrelated, already-loaded CRL.
        whoami1 = self.crl_utils.get_identity_via_mtls(
            self.cluster.master.ip, self.MGMT_PORT, leaf1_cert_path, leaf1_key_path,
        )
        self.assertEqual(
            whoami1.get("id"), valid_user,
            f"leaf1 should authenticate normally despite CA-2's unrelated CRL "
            f"being loaded, got whoami={whoami1}",
        )
        self.log.info("leaf1 connects normally; CA-2's unrelated CRL has no effect")

        # leaf2 must connect -- CA-1's revocation must not leak into CA-2's
        # domain just because the serial numbers match.
        self.assertTrue(
            self._handshake_ok(leaf2_cert_path, leaf2_key_path),
            "leaf2 (different trusted CA, colliding with a serial actually "
            "revoked under CA-1, not itself revoked) should connect -- CRL "
            "scoping must be issuer-based, not raw serial number",
        )
        self.log.info(
            "leaf2 connects despite colliding with a serial genuinely revoked "
            "under a different CA -- cross-CA isolation holds"
        )

    def test_crl_diagnostics_endpoints(self):
        """diagnostics/status shape, concurrent reloadCrl idempotency,
        per-node behaviour when one node is down, and diagnostics/validate
        (real 4-value enum, untrusted-issuer, policy override, cert-count
        boundary, no-certs cluster-cert mode, CA untrusted after its CRL
        was already loaded, an expired-vs-missing CRL distinction, and a
        parity check against a real live mTLS handshake for the same
        certs)."""
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "diagRevoked"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "diagValid"
        )
        valid_pem = self.crl_utils.cert_to_pem(valid_cert).decode()
        revoked_pem = self.crl_utils.cert_to_pem(revoked_cert).decode()

        filename = "diag_endpoint.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial], filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        node_key = f"{self.cluster.master.ip}:8091"

        # -- diagnostics/status: full response-shape assertion --
        status, content = self.crl_utils.diagnostics_status(self.rest)
        self.assertTrue(status, f"diagnostics/status failed: {content}")
        entry = self.crl_utils.find_diagnostics_file_entry(content, node_key, filename)
        self.assertIsNotNone(entry, f"Uploaded file missing from status: {content}")
        for key in ("filename", "source", "cacheStatus", "entries", "lastReload"):
            self.assertIn(key, entry, f"Missing key {key!r} in file status: {entry}")
        for reload_key in ("result", "time", "errors"):
            self.assertIn(reload_key, entry["lastReload"])
        self.assertEqual(entry["cacheStatus"], "active")
        self.log.info("diagnostics/status response shape correct")

        # -- Concurrent reloadCrl: idempotent, byte-identical results --
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            results = list(
                pool.map(lambda _: self.crl_utils.reload_crl(self.rest), range(10))
            )
        for ok, resp in results:
            self.assertTrue(ok, f"Concurrent reloadCrl failed: {resp}")
        first = results[0][1]
        for ok, resp in results[1:]:
            self.assertEqual(resp, first, "Concurrent reloadCrl responses diverged")
        self.log.info("10 concurrent reloadCrl calls returned identical results")

        # -- diagnostics/validate: baseline valid/revoked, real 4-value enum --
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[valid_pem]
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(content["results"][0]["status"], "valid")
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[revoked_pem]
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(content["results"][0]["status"], "revoked")

        # A cert from a CA that was never trusted at all collapses to the
        # same "undetermined" status as a genuinely missing CRL -- there is
        # no distinct "untrusted issuer" value in the real (4-value) enum.
        untrusted_ca_cert, untrusted_ca_key = self.crl_utils.generate_ca(
            "DiagCAUntrusted"
        )
        untrusted_cert, _, _ = self.crl_utils.generate_leaf_cert(
            untrusted_ca_cert, untrusted_ca_key, "diagUntrustedIssuer"
        )
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require",
            certs=[self.crl_utils.cert_to_pem(untrusted_cert).decode()],
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(content["results"][0]["status"], "undetermined")
        self.log.info(
            "Baseline valid/revoked correct; untrusted-issuer cert reports "
            "'undetermined' -- confirms the real 4-value enum"
        )

        # -- policy override actually overrides the cluster's real policy;
        #    Disabled is rejected outright (nothing to test with it) --
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[revoked_pem]
        )
        self.assertTrue(status)
        self.assertEqual(
            content["results"][0]["status"], "revoked",
            "policy override must reflect the supplied policy, not the "
            "cluster's actually-configured Disabled policy",
        )
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Disabled", certs=[revoked_pem]
        )
        self.assertFalse(status, f"policy=Disabled should be rejected, got: {content}")
        self.log.info("Policy override works; policy=Disabled correctly rejected")

        # -- omitting certs evaluates the cluster's own installed certs --
        status, content = self.crl_utils.diagnostics_validate(self.rest, policy="Require")
        self.assertTrue(status, f"diagnostics/validate (no certs) failed: {content}")
        self.assertTrue(
            content.get("usingClusterCertificates"),
            f"Expected usingClusterCertificates=true: {content}",
        )
        cert_types = {r["certificateType"] for r in content["results"]}
        self.assertIn("client_cert", cert_types)
        self.assertIn("node_cert", cert_types)
        self.log.info("No-certs mode evaluates the cluster's own client_cert+node_cert")

        # -- cert-count boundary: 100 accepted, 101 rejected --
        hundred_certs = [valid_pem] * 100
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=hundred_certs
        )
        self.assertTrue(status, f"100 certs should be accepted: {content}")
        self.assertEqual(len(content["results"]), 100)
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=hundred_certs + [valid_pem]
        )
        self.assertFalse(status, f"101 certs should be rejected, got: {content}")
        self.log.info("100/101-cert boundary enforced correctly")

        # -- CA untrusted after its CRL was already loaded/applied: status
        #    flips to "untrusted", and diagnostics/validate's free-text
        #    details distinguish a rejected CRL from one that never existed --
        status, content = self.crl_utils.untrust_ca_by_cn(self.rest, "TestCA1")
        self.assertTrue(status, f"Untrust-by-CN diag/eval failed: {content}")
        try:
            status, content = self.crl_utils.diagnostics_status(self.rest)
            self.assertTrue(status)
            entry = self.crl_utils.find_diagnostics_file_entry(content, node_key, filename)
            self.assertIsNotNone(entry)
            self.assertEqual(
                entry["cacheStatus"], "untrusted",
                "CRL's issuing CA is no longer trusted -- status should flip",
            )
            status, content = self.crl_utils.diagnostics_validate(
                self.rest, policy="Require", certs=[revoked_pem]
            )
            self.assertTrue(status)
            result = content["results"][0]
            self.assertEqual(result["status"], "undetermined")
            self.assertIn(
                "rejected", result.get("details", "").lower(),
                f"Expected details to distinguish a rejected (untrusted) CRL "
                f"from a genuinely missing one, got: {result}",
            )
        finally:
            self._trust_ca_on_cluster(self.ca_cert)
        self.log.info(
            "CA untrusted after CRL load: cacheStatus flips to 'untrusted', "
            "diagnostics/validate details distinguish rejected-CRL from missing-CRL"
        )

        # -- diagnostics/validate distinguishes an expired CRL from a
        # genuinely missing one, via the same "expired CRLs: ..." detail
        # text already confirmed for the runtime enforcement path.
        # Clears every other file for this CA first: "freshest CRL wins
        # per issuer" doesn't mean "only the single freshest one counts
        # for everything" -- with the earlier diag_endpoint.pem (still
        # active, non-expired) also loaded for the same issuer, the
        # system falls back to it once the newer one expires, and
        # expired_cert (revoked by neither file) came back "valid"
        # instead of "undetermined" -- caught live, not assumed. --
        self._cleanup_created_files()
        expired_cert, _, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "diagExpiredLeaf"
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        expired_filename = "diag_expired.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, expired_filename,
            self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                this_update=now - datetime.timedelta(seconds=3),
                next_update=now + datetime.timedelta(seconds=6),
                crl_number=2,
            ),
        )
        self.assertTrue(status, f"Short-lived CRL upload failed: {content}")
        self._track_uploaded_file(expired_filename)
        self.crl_utils.reload_crl(self.rest)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status, diag = self.crl_utils.diagnostics_status(self.rest)
            entry = next(
                (f for f in diag.get(node_key, {}).get("crlFiles", [])
                 if f["filename"] == expired_filename),
                None,
            )
            if entry and entry.get("cacheStatus") == "expired":
                break
            time.sleep(2)
        else:
            self.fail(f"{expired_filename} never reported cacheStatus=expired")
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require",
            certs=[self.crl_utils.cert_to_pem(expired_cert).decode()],
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        result = content["results"][0]
        self.assertEqual(result["status"], "undetermined")
        self.assertIn(
            "expired crls", result.get("details", "").lower(),
            f"Expected details to distinguish an expired CRL from a "
            f"genuinely missing one, got: {result}",
        )
        self.log.info(
            "diagnostics/validate distinguishes an expired CRL from a "
            "genuinely missing one via the 'expired CRLs: ...' detail text"
        )

        # -- diagnostics/validate's verdict for a cert matches what a real
        # live mTLS handshake with that same cert actually does, once the
        # cluster's real policy is set to match what's passed to
        # diagnostics/validate -- every other diagnostics/validate check
        # in this test uses the policy *parameter* only, independent of
        # the cluster's actually-configured (Disabled) policy. --
        self._enable_client_cert_auth(state="enable")
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status, f"Policy change failed: {content}")
        # The earlier _cleanup_created_files() (for the expired-CRL
        # sub-case above) also removed diag_endpoint.pem, so revoked_pem
        # is no longer actually revoked by any loaded CRL -- re-establish
        # the revocation under a fresh filename/crl_number before using
        # revoked_pem/revoked_cert as "known revoked" below.
        parity_filename = "diag_parity.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial],
            parity_filename, crl_number=3,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(parity_filename)
        self.crl_utils.reload_crl(self.rest)
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))

        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[revoked_pem]
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(content["results"][0]["status"], "revoked")
        self.assertFalse(
            self._handshake_ok(revoked_cert_path, revoked_key_path),
            "A live handshake with the same cert diagnostics/validate "
            "calls 'revoked' must also actually be rejected",
        )

        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[valid_pem]
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(content["results"][0]["status"], "valid")
        self.assertTrue(
            self._handshake_ok(valid_cert_path, valid_key_path),
            "A live handshake with the same cert diagnostics/validate "
            "calls 'valid' must also actually connect",
        )
        self.log.info(
            "diagnostics/validate's verdict matches a real live mTLS "
            "handshake outcome for the same certs"
        )

        # Per-node behaviour when one node is down: explicit `nodes` list
        # should surface it as an error entry; the default (no explicit
        # nodes) call silently omits it instead -- known gap, asserting
        # current actual behaviour.
        second = next(
            s for s in self.cluster.servers[:self.nodes_init]
            if s.ip != self.cluster.master.ip
        )
        second_key = f"{second.ip}:8091"
        # Fetched once while still healthy -- only its .id is needed for the
        # later status polls, and that stays stable regardless of health.
        second_otp_node = next(
            n for n in self.cluster_util.get_otp_nodes(self.cluster.master)
            if n.ip == second.ip
        )
        shell = RemoteMachineShellConnection(second)
        try:
            shell.stop_couchbase()
            self.assertTrue(
                self.cluster_util.wait_for_node_status(
                    self.cluster, second_otp_node, "unhealthy",
                    timeout_in_seconds=180,
                ),
                f"{second.ip} never reached status=unhealthy",
            )

            status, content = self.crl_utils.diagnostics_status(
                self.rest, nodes=[node_key, second_key]
            )
            self.assertTrue(status, f"Explicit-nodes diagnostics/status failed: {content}")
            self.assertIn(node_key, content)
            self.assertIn(second_key, content)
            self.assertIn(
                "error", content[second_key],
                f"Down node should surface as an error entry, got: {content[second_key]}",
            )

            status, content = self.crl_utils.diagnostics_status(self.rest)
            self.assertTrue(status, f"Default diagnostics/status failed: {content}")
            self.assertIn(node_key, content)
            self.assertNotIn(
                second_key, content,
                "Known gap: the down node is silently dropped from the "
                "default (no explicit nodes) diagnostics/status response "
                "instead of surfacing as an error entry. If this assertion "
                "now fails, the gap has been fixed -- flip it to assertIn "
                "+ assert an error entry.",
            )
        finally:
            shell.start_couchbase()
            shell.disconnect()
            self.assertTrue(
                self.cluster_util.wait_for_node_status(
                    self.cluster, second_otp_node, "healthy",
                    timeout_in_seconds=180,
                ),
                f"{second.ip} never recovered to status=healthy",
            )
        self.log.info(
            "Down node: explicit nodes list surfaces it as an error entry; "
            "default call silently omits it (known gap, asserted as-is)"
        )

    def test_crl_auditing_logs_and_metrics(self):
        """Audit events for CRL admin actions, RBAC-denied attempts, and a
        revoked-cert connection rejection (the last two via a generic
        event, not a CRL-specific one); no serial/PEM leakage into logs;
        revoked vs missing vs expired vs already-loaded-then-untrusted-CA
        CRL all producing distinguishable runtime log text; and the
        cm_crl_status_checks_total revocation-check metric.

        Note: untrusted-issuer/forged-signature CRL rejection producing a
        distinct upload-time error message is already covered by
        test_crl_trust_and_signature_boundary -- not re-tested here.
        """
        server = self.cluster.master

        def set_audit_enabled(enabled):
            # /settings/audit takes classic form-encoded params, not a
            # JSON body (unlike /settings/crl) -- confirmed via a 400 the
            # hard way.
            return requests.post(
                f"http://{server.ip}:8091/settings/audit",
                auth=(server.rest_username, server.rest_password),
                data={"auditdEnabled": "true" if enabled else "false"},
                timeout=30,
            )

        resp = set_audit_enabled(True)
        self.assertEqual(
            resp.status_code, 200, f"Failed to enable auditing: {resp.text}"
        )
        try:
            self._test_crl_auditing_logs_and_metrics_body(server)
        finally:
            # No other CRLBase teardown step touches /settings/audit --
            # this test is the only one that enables it, so it's on this
            # test to turn it back off regardless of outcome.
            set_audit_enabled(False)

    def _test_crl_auditing_logs_and_metrics_body(self, server):
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "auditRevoked"
        )
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))

        baseline_filename = "audit_baseline.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, baseline_filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1),
        )
        self.assertTrue(status, f"Baseline CRL upload failed: {content}")
        self._track_uploaded_file(baseline_filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertTrue(
            self._handshake_ok(revoked_cert_path, revoked_key_path),
            "Baseline: cert should connect before being revoked",
        )

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            # Each admin-API action (upload, delete, settings change,
            # reload) should add its own audit entry.
            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "upload crl file")
            filename = "audit_admin_actions.pem"
            status, content = self.crl_utils.upload_file(
                self.rest, filename,
                self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=2),
            )
            self.assertTrue(status, f"Upload failed: {content}")
            self._track_uploaded_file(filename)
            self.assertGreater(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "upload crl file"), before,
                "Expected a new upload crl file audit entry",
            )
            upload_event = get_audit_event(shell, self.AUDIT_LOG_PATH, 8308)
            self._assert_audit_event_shape(upload_event, 8308)
            self.assertEqual(
                upload_event["filename"], filename,
                "Audited filename does not match the uploaded file",
            )

            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "delete crl file")
            status, content = self.crl_utils.delete_file(self.rest, filename)
            self.assertTrue(status, f"Delete failed: {content}")
            self._created_files.remove(filename)
            self.assertGreater(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "delete crl file"), before,
                "Expected a new delete crl file audit entry",
            )
            delete_event = get_audit_event(shell, self.AUDIT_LOG_PATH, 8309)
            self._assert_audit_event_shape(delete_event, 8309)
            self.assertEqual(
                delete_event["filename"], filename,
                "Audited filename does not match the deleted file",
            )

            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "crl settings")
            status, content = self.crl_utils.set_settings(
                self.rest,
                policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
            )
            self.assertTrue(status, f"Settings change failed: {content}")
            self.assertGreater(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "crl settings"), before,
                "Expected a new crl settings audit entry",
            )
            settings_event = get_audit_event(shell, self.AUDIT_LOG_PATH, 8307)
            self._assert_audit_event_shape(settings_event, 8307)
            self.assertEqual(
                settings_event["settings"], content,
                "Audited settings payload does not match the merged config "
                "the API itself returned",
            )
            # Known product gap (menelaus_web_crl.erl:handle_post_settings):
            # the pre-change config is never read before the new one is
            # written, so this event has no architectural way to capture
            # what the policy *was* before this change -- confirmed live,
            # not just from source. This pins today's behavior so a future
            # fix that adds an old-value field is caught here as a
            # passing-test signal to flip into a positive assertion.
            for legacy_key in ("previousSettings", "old_policy", "oldSettings"):
                self.assertNotIn(
                    legacy_key, settings_event,
                    f"Unexpected '{legacy_key}' key in the crl settings audit "
                    f"event -- if the old-policy-value gap has been fixed, "
                    f"update this test to assert on its actual value instead "
                    f"of its absence",
                )

            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "reload crl")
            status, content = self.crl_utils.reload_crl(self.rest)
            self.assertTrue(status, f"Reload failed: {content}")
            self.assertGreater(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "reload crl"), before,
                "Expected a new reload crl audit entry",
            )
            # Event 8310 carries no CRL-specific payload field at all (per
            # audit_descriptor.json) -- only the shared actor/timestamp
            # envelope is asserted here, intentionally.
            reload_event = get_audit_event(shell, self.AUDIT_LOG_PATH, 8310)
            self._assert_audit_event_shape(reload_event, 8310)
            self.log.info(
                "Admin-API audit events present for upload/delete/settings/"
                "reload, each with the correct actor/timestamp/payload"
            )

            # A rejected (invalid value) settings change must not add a
            # new audit entry.
            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "crl settings")
            before_settings_event = get_audit_event(shell, self.AUDIT_LOG_PATH, 8307)
            status, content = self.crl_utils.set_settings(
                self.rest, policyPerScope={"clientAuth": "NotARealPolicy"},
            )
            self.assertFalse(status, f"Invalid policy value should be rejected, got: {content}")
            self.assertEqual(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "crl settings"), before,
                "A rejected settings POST should not itself add a crl settings entry",
            )
            # Stronger, corroborating proof of the same thing: not just "the
            # keyword count didn't change" but "the specific event's own
            # timestamp is identical before and after" -- no new 8307 line
            # was appended at all.
            after_settings_event = get_audit_event(shell, self.AUDIT_LOG_PATH, 8307)
            self.assertEqual(
                before_settings_event["timestamp"], after_settings_event["timestamp"],
                "A rejected settings POST must not append a new crl settings audit entry",
            )
            self.log.info("Rejected (400) settings change does not add a new audit entry")

            # A revoked-cert connection rejection is audited via the
            # generic "authentication failure" event (its free-text
            # `reason` names the TLS alert, e.g. "...Certificate Revoked"),
            # not a CRL-specific event -- same pattern as the RBAC-denial
            # check below, which also uses a generic event.
            status, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, [revoked_serial],
                baseline_filename, crl_number=3,
            )
            self.assertTrue(status, f"Revoke upload failed: {content}")
            self.crl_utils.reload_crl(self.rest)
            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "revok")
            self.assertFalse(
                self._handshake_ok(revoked_cert_path, revoked_key_path),
                "Cert should now be rejected as revoked",
            )
            self.assertGreater(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "revok"), before,
                "Expected a new audit entry mentioning the revocation "
                "(a generic authentication-failure event, not CRL-specific)",
            )
            self.log.info(
                "Revoked-cert connection rejection is audited via the "
                "generic authentication-failure event"
            )

            # No raw serial number or PEM/DER bytes should leak into the
            # rejection log line. Greps for cb_crl's own log lines
            # specifically, rather than a plain tail -- unrelated
            # ns_server activity (memcached config pushes, RBAC/roles-
            # cache rebuilds) can easily push the actual CRL-check line
            # out of a small fixed-size tail before it's ever read.
            recent = grep_remote_log(shell, self.DEBUG_LOG_PATH, "(CRL)", lines=3)
            self.assertIn(
                "<ud>", recent,
                "Expected redaction tags around the cert subject in the "
                "rejection log line",
            )
            serial_hex = format(revoked_serial, "X")
            self.assertNotIn(
                serial_hex, recent,
                "Raw serial number must not appear in the rejection log line",
            )
            self.assertNotIn(
                "BEGIN CERTIFICATE", recent,
                "Raw PEM content must not leak into logs",
            )
            self.log.info("No raw serial number or PEM content found in the rejection log line")

            # The same leakage check, but against the audit log
            # specifically -- current-audit.log carries admin-action
            # payloads (filenames, full settings JSON) that debug.log
            # doesn't, so it needs its own check rather than assuming
            # debug.log's check covers it. The <ud> redaction-tag check
            # above doesn't have an audit-log equivalent: these fields are
            # structured JSON values (filenames, actor identity), not
            # free-text log lines needing redaction tags in the same sense.
            audit_tail = tail_remote_log(shell, self.AUDIT_LOG_PATH, lines=1000)
            self.assertNotIn(
                serial_hex, audit_tail,
                "Raw serial number must not appear in the audit log",
            )
            self.assertNotIn(
                "BEGIN CERTIFICATE", audit_tail,
                "Raw PEM content must not leak into the audit log",
            )
            self.log.info("No raw serial number or PEM content found in the audit log")

            # Revoked, missing, and expired CRL should each produce
            # distinguishable log text. Revoked's line is already in
            # `recent` above ("Certificate revoked ...").
            self.assertIn("Certificate revoked", recent)

            # An unauthorized (RBAC-denied) admin action IS audited, via
            # the generic access-forbidden event, not a CRL-specific one.
            low_priv_user, low_priv_pass = self._create_rbac_test_user(
                "audit_low_priv_user", "views_admin[*]"
            )
            before = audit_keyword_count(shell, self.AUDIT_LOG_PATH, "access forbidden")
            resp = requests.post(
                f"http://{server.ip}:8091/settings/crl",
                auth=(low_priv_user, low_priv_pass),
                headers={"Content-Type": "application/json"},
                json={"policyPerScope": {"clientAuth": "Disabled"}},
                timeout=30,
            )
            self.assertEqual(
                resp.status_code, 403,
                f"Low-privilege user should get 403, got {resp.status_code}: {resp.text}",
            )
            self.assertGreater(
                audit_keyword_count(shell, self.AUDIT_LOG_PATH, "access forbidden"), before,
                "Expected a new generic access-forbidden audit entry",
            )
            self.log.info(
                "RBAC-denied CRL settings change audited via the generic "
                "access-forbidden event"
            )

            # Missing: delete every CRL file so nothing applies to this CA.
            self._cleanup_created_files()
            self.crl_utils.reload_crl(self.rest)
            missing_cert, missing_key, _ = self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, "auditMissing"
            )
            missing_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(missing_cert))
            missing_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(missing_key))
            self.crl_utils.wait_for_crl_log_text(
                shell, self.DEBUG_LOG_PATH, self.cluster.master.ip, self.MGMT_PORT,
                missing_cert_path, missing_key_path,
                ["undetermined", "no usable crl"],
            )

            # Expired: a short-lived CRL, uploaded valid, waited past its
            # own next_update via real wall-clock time.
            now = datetime.datetime.now(datetime.timezone.utc)
            expired_filename = "audit_short_lived.pem"
            short_lived_pem = self.crl_utils.build_crl(
                self.ca_cert, self.ca_key, revoked_serials=[],
                this_update=now - datetime.timedelta(seconds=2),
                next_update=now + datetime.timedelta(seconds=8),
                crl_number=4,
            )
            status, content = self.crl_utils.upload_file(
                self.rest, expired_filename, short_lived_pem
            )
            self.assertTrue(status, f"Short-lived CRL upload failed: {content}")
            self._track_uploaded_file(expired_filename)
            self.crl_utils.reload_crl(self.rest)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status, content = self.crl_utils.diagnostics_status(self.rest)
                entry = next(
                    (f for f in content.get(f"{server.ip}:8091", {}).get("crlFiles", [])
                     if f["filename"] == expired_filename),
                    None,
                )
                if entry and entry.get("cacheStatus") == "expired":
                    break
                time.sleep(2)
            else:
                self.fail(f"{expired_filename} never reported cacheStatus=expired")
            # Expired-CRL rejection should be distinguishable from a
            # genuinely missing CRL -- if "expired" stops appearing, the
            # two wordings have collapsed back together.
            self.crl_utils.wait_for_crl_log_text(
                shell, self.DEBUG_LOG_PATH, self.cluster.master.ip, self.MGMT_PORT,
                missing_cert_path, missing_key_path,
                ["undetermined", "expired"],
            )
            self.log.info(
                "Revoked/missing/expired CRL rejections all produce "
                "distinguishable log text"
            )

            # An already-loaded CRL whose issuing CA becomes untrusted
            # afterward also gets a distinguishable runtime log line --
            # but confirmed live that a real handshake won't show it: once
            # untrusted, the connection is rejected at the generic TLS
            # chain-validation layer, before cb_crl:apply_policy ever
            # runs, so no "(CRL)" line appears for it. diagnostics/validate
            # exercises that exact same runtime code path directly (its
            # own response text already matches this log line format), so
            # it's used here instead of wait_for_crl_log_text's
            # live-handshake polling.
            untrusted_ca_cert, untrusted_ca_key = self.crl_utils.generate_ca(
                "AuditUntrustedCA"
            )
            self._trust_ca_on_cluster(untrusted_ca_cert)
            untrusted_leaf_cert, _, _ = self.crl_utils.generate_leaf_cert(
                untrusted_ca_cert, untrusted_ca_key, "auditUntrustedLeaf"
            )
            untrusted_ca_filename = "audit_untrusted_ca.pem"
            status, content = self.crl_utils.upload_file(
                self.rest, untrusted_ca_filename,
                self.crl_utils.build_crl(untrusted_ca_cert, untrusted_ca_key, crl_number=1),
            )
            self.assertTrue(status, f"CRL upload failed: {content}")
            self._track_uploaded_file(untrusted_ca_filename)
            self.crl_utils.reload_crl(self.rest)
            status, content = self.crl_utils.untrust_ca_by_cn(self.rest, "AuditUntrustedCA")
            self.assertTrue(status, f"Untrust-by-CN diag/eval failed: {content}")
            # Wait for the untrust to actually reach cb_crl_manager's cache
            # before validating -- diagnostics/status flipping to
            # "untrusted" is the confirmed signal that it has (same pattern
            # already proven in test_crl_diagnostics_endpoints).
            node_key = f"{self.cluster.master.ip}:8091"
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status, diag = self.crl_utils.diagnostics_status(self.rest)
                entry = self.crl_utils.find_diagnostics_file_entry(
                    diag, node_key, untrusted_ca_filename
                )
                if entry and entry.get("cacheStatus") == "untrusted":
                    break
                time.sleep(2)
            else:
                self.fail(f"{untrusted_ca_filename} never reported cacheStatus=untrusted")
            status, content = self.crl_utils.diagnostics_validate(
                self.rest, policy="Require",
                certs=[self.crl_utils.cert_to_pem(untrusted_leaf_cert).decode()],
            )
            self.assertTrue(status, f"diagnostics/validate failed: {content}")
            log_text = grep_remote_log(shell, self.DEBUG_LOG_PATH, "(CRL)", lines=3).lower()
            self.assertTrue(
                all(s in log_text for s in ("undetermined", "rejected")),
                f"Expected the runtime enforcement log to say "
                f"'undetermined'+'rejected' for an already-loaded CRL "
                f"whose issuing CA later became untrusted, got: {log_text}",
            )
            self.log.info(
                "Runtime log line for an already-loaded CRL whose issuing "
                "CA became untrusted also says 'undetermined'+'rejected', "
                "distinguishable from missing/expired"
            )

            # No CRL load-success/failure metric should exist.
            all_metrics = self.crl_utils.get_all_metrics_text(server)
            self.assertNotIn(
                "cm_crl_load", all_metrics,
                "Did not expect a dedicated CRL load-success/failure metric",
            )

            # The revocation-check metric should increment correctly for a
            # fresh valid check and a fresh revoked check. Re-establish a
            # clean, non-expired, non-revoking CRL first -- the only one
            # still active right now is the expired one from above, and
            # under Require that would reject any cert, valid or not.
            # Fresh certs are used for both checks below (not a cert
            # already checked earlier in this test): the decision cache is
            # keyed by cert+CRL-version, so re-checking an already-seen
            # cert would hit cache instead of producing the fresh miss
            # this needs.
            metrics_filename = "audit_metrics_clean.pem"
            status, content = self.crl_utils.upload_file(
                self.rest, metrics_filename,
                self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=5),
            )
            self.assertTrue(status, f"Clean CRL upload failed: {content}")
            self._track_uploaded_file(metrics_filename)
            self.crl_utils.reload_crl(self.rest)

            valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, "auditMetricsValid"
            )
            valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
            valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))
            before_valid = self.crl_utils.get_metric_value(
                server, "cm_crl_status_checks_total",
                {"cache": "miss", "verdict": "valid"},
            ) or 0
            self.assertTrue(
                self._handshake_ok(valid_cert_path, valid_key_path),
                "Fresh valid cert should connect",
            )
            after_valid = self.crl_utils.get_metric_value(
                server, "cm_crl_status_checks_total",
                {"cache": "miss", "verdict": "valid"},
            ) or 0
            self.assertEqual(after_valid, before_valid + 1)

            metrics_revoked_cert, metrics_revoked_key, metrics_revoked_serial = (
                self.crl_utils.generate_leaf_cert(
                    self.ca_cert, self.ca_key, "auditMetricsRevoked"
                )
            )
            metrics_revoked_cert_path = self._write_temp_pem(
                self.crl_utils.cert_to_pem(metrics_revoked_cert)
            )
            metrics_revoked_key_path = self._write_temp_pem(
                self.crl_utils.key_to_pem(metrics_revoked_key)
            )
            status, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, [metrics_revoked_serial],
                metrics_filename, crl_number=6,
            )
            self.assertTrue(status, f"Revoke upload failed: {content}")
            self.crl_utils.reload_crl(self.rest)
            before_revoked = self.crl_utils.get_metric_value(
                server, "cm_crl_status_checks_total",
                {"cache": "miss", "verdict": "revoked"},
            ) or 0
            self.assertFalse(
                self._handshake_ok(metrics_revoked_cert_path, metrics_revoked_key_path),
                "Fresh revoked cert should be rejected",
            )
            after_revoked = self.crl_utils.get_metric_value(
                server, "cm_crl_status_checks_total",
                {"cache": "miss", "verdict": "revoked"},
            ) or 0
            self.assertEqual(after_revoked, before_revoked + 1)
            self.log.info(
                "cm_crl_status_checks_total increments correctly for valid "
                "and revoked checks"
            )

            # A metric reflecting CRL expiry status should exist
            # (structural check only -- the full expiry-alert lifecycle
            # is a separate test).
            self.assertIn("cm_alerts_triggered_total", all_metrics)
        finally:
            shell.disconnect()

    def test_crl_rbac_permission_boundaries(self):
        """RBAC across every CRL endpoint: a full-access security role
        reaches business logic everywhere; a read-only variant succeeds on
        read-shaped endpoints and is cleanly denied on write-shaped ones;
        a zero-permission user is denied everywhere; unauthenticated and
        invalid-credential requests get 401, distinct from an
        authenticated-but-unauthorized 403; and a live role downgrade
        takes effect on the very next request, no caching.

        Note: a denied action's own audit trail (via the generic
        access-forbidden event) is already covered by
        test_crl_auditing_logs_and_metrics -- not re-tested here.
        """
        base = f"http://{self.cluster.master.ip}:8091"

        def hit(method, path, auth, json_body=None, upload=False):
            url = base + path
            if upload:
                resp = requests.request(
                    method, url, auth=auth,
                    files={"file": ("rbac_probe.pem",
                                    self.crl_utils.build_crl(self.ca_cert, self.ca_key,
                                                             crl_number=1))},
                    timeout=30,
                )
            elif json_body is not None:
                resp = requests.request(
                    method, url, auth=auth,
                    headers={"Content-Type": "application/json"},
                    json=json_body, timeout=30,
                )
            else:
                resp = requests.request(method, url, auth=auth, timeout=30)
            return resp.status_code

        # Every CRL endpoint, split by whether the code gates it on the
        # read or write half of the {[admin,security], read|write}
        # permission -- upload is handled separately below (multipart).
        read_endpoints = [
            ("GET", "/settings/crl", None),
            ("GET", "/settings/crl/files", None),
            ("GET", "/settings/crl/diagnostics/status", None),
            ("POST", "/settings/crl/diagnostics/status", {}),
            ("POST", "/settings/crl/diagnostics/validate", {"policy": "Require"}),
        ]
        write_endpoints = [
            ("POST", "/settings/crl", {"policyPerScope": {"clientAuth": "Disabled"}}),
            ("DELETE", "/settings/crl/files/rbac_nonexistent.pem", None),
            ("POST", "/node/controller/reloadCrl", None),
        ]

        full_user, full_pass = self._create_rbac_test_user(
            "rbac_full_admin", "security_admin"
        )
        ro_user, ro_pass = self._create_rbac_test_user(
            "rbac_ro_admin", "ro_security_admin"
        )
        zero_user, zero_pass = self._create_rbac_test_user(
            "rbac_zero_perm", "views_admin[*]"
        )

        # A full-access role reaches business logic on every endpoint --
        # never blocked at the authorization gate, whatever the actual
        # outcome (200/400/404) turns out to be.
        for method, path, body in read_endpoints + write_endpoints:
            code = hit(method, path, (full_user, full_pass), body)
            self.assertNotIn(
                code, (401, 403),
                f"{method} {path} as a full-access role should not be "
                f"blocked, got {code}",
            )
        upload_code = hit("POST", "/settings/crl/files", (full_user, full_pass), upload=True)
        self.assertNotIn(
            upload_code, (401, 403),
            f"Upload as a full-access role should not be blocked, got {upload_code}",
        )
        self._track_uploaded_file("rbac_probe.pem")
        self.log.info("Full-access role reaches business logic on all 9 endpoints")

        # A read-only role succeeds on every read-shaped endpoint and is
        # cleanly denied on every write-shaped one -- no over- or
        # under-granting.
        for method, path, body in read_endpoints:
            code = hit(method, path, (ro_user, ro_pass), body)
            self.assertEqual(
                code, 200, f"{method} {path} as a read-only role should succeed, got {code}"
            )
        for method, path, body in write_endpoints:
            code = hit(method, path, (ro_user, ro_pass), body)
            self.assertEqual(
                code, 403, f"{method} {path} as a read-only role should be denied, got {code}"
            )
        upload_code = hit("POST", "/settings/crl/files", (ro_user, ro_pass), upload=True)
        self.assertEqual(
            upload_code, 403, f"Upload as a read-only role should be denied, got {upload_code}"
        )
        self.log.info("Read-only role: read endpoints succeed, write endpoints denied")

        # A user with no relevant permission at all is denied everywhere.
        for method, path, body in read_endpoints + write_endpoints:
            code = hit(method, path, (zero_user, zero_pass), body)
            self.assertEqual(
                code, 403, f"{method} {path} with no relevant permission should be denied, got {code}"
            )
        upload_code = hit("POST", "/settings/crl/files", (zero_user, zero_pass), upload=True)
        self.assertEqual(upload_code, 403)
        self.log.info("Zero-permission user denied on all 9 endpoints")

        # A fully unauthenticated request gets 401, distinct from the 403
        # an authenticated-but-unauthorized user gets above.
        code = hit("GET", "/settings/crl", None)
        self.assertEqual(code, 401, f"Unauthenticated request should get 401, got {code}")

        # Invalid credentials (wrong password; nonexistent username) also
        # get 401, not a 403 and not a hang/500.
        code = hit("GET", "/settings/crl", (ro_user, "totally-wrong-password"))
        self.assertEqual(code, 401, f"Wrong password should get 401, got {code}")
        code = hit("GET", "/settings/crl", ("rbac_nonexistent_user_xyz", "whatever"))
        self.assertEqual(code, 401, f"Nonexistent user should get 401, got {code}")
        self.log.info(
            "Unauthenticated and invalid-credential requests correctly get "
            "401, not 403"
        )

        # A live role downgrade takes effect on the very next request --
        # no re-login, no caching window.
        code = hit("GET", "/settings/crl", (ro_user, ro_pass))
        self.assertEqual(code, 200, "Baseline: read-only role should succeed before downgrade")
        self._grant_rbac_role(ro_user, "views_admin[*]", password=ro_pass)
        code = hit("GET", "/settings/crl", (ro_user, ro_pass))
        self.assertEqual(
            code, 403,
            "Downgraded role should take effect on the very next request, "
            "not after some caching delay",
        )
        self.log.info("Live role downgrade took effect immediately, no staleness")

    def test_crl_cbauth_crls_validate(self):
        """POST /_cbauth/crlsValidate -- ns_server's actual contract with
        cbauth-registered GO services (Query/FTS/Analytics/Indexer/XDCR).
        Unlike diagnostics/validate, it has no policy override param: it
        always honors the cluster's real configured policy per scope, and
        (per menelaus_cbauth:handle_crls_validate_post/1) requires
        {[admin,internal], all} -- a strictly narrower RBAC grant than
        every other CRL endpoint, which only need {[admin,security], *}."""

        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "cbauthRevoked"
        )
        revoked_b64 = self.crl_utils.cert_to_der_b64(revoked_cert)
        garbage_b64 = base64.b64encode(b"not a real der cert").decode()

        filename = "cbauth_crls_validate.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial], filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

        # -- Real policy honored per scope, independently -- no override
        # param exists here, unlike diagnostics/validate. --
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status, f"Policy change failed: {content}")

        status, content = self.crl_utils.cbauth_crls_validate(
            self.rest, [revoked_b64], "clientAuth"
        )
        self.assertTrue(status, f"crlsValidate failed: {content}")
        self.assertEqual(content["statuses"][0]["status"], "revoked")

        status, content = self.crl_utils.cbauth_crls_validate(
            self.rest, [revoked_b64], "nodeToNode"
        )
        self.assertTrue(status)
        self.assertEqual(
            content["statuses"][0]["status"], "valid",
            "nodeToNode is Disabled -- same revoked cert must not be "
            "affected by clientAuth's Require policy",
        )
        self.log.info(
            "crlsValidate honors the real per-scope policy, independently "
            "(no override param, unlike diagnostics/validate)"
        )

        # -- Malformed-but-valid-base64 input: decode is only attempted
        # (and only then can fail) when the scope's policy actually
        # enforces -- under Disabled the verdict short-circuits to "valid"
        # before ever touching the bytes. Confirmed live, not assumed. --
        status, content = self.crl_utils.cbauth_crls_validate(
            self.rest, [garbage_b64], "clientAuth"
        )
        self.assertTrue(status)
        self.assertEqual(content["statuses"][0]["status"], "failed")
        self.assertIn("decode", content["statuses"][0].get("details", "").lower())
        status, content = self.crl_utils.cbauth_crls_validate(
            self.rest, [garbage_b64], "nodeToNode"
        )
        self.assertTrue(status)
        self.assertEqual(
            content["statuses"][0]["status"], "valid",
            "Disabled scope must short-circuit before attempting to "
            "decode the cert at all",
        )
        self.log.info(
            "Undecodable cert: 'failed' under an enforcing policy, "
            "'valid' (short-circuited) under Disabled"
        )

        # -- Chain ordering: response preserves input order, leaf first --
        status, content = self.crl_utils.cbauth_crls_validate(
            self.rest, [revoked_b64, self.crl_utils.cert_to_der_b64(self.ca_cert)], "clientAuth"
        )
        self.assertTrue(status)
        statuses = [r["status"] for r in content["statuses"]]
        self.assertEqual(
            statuses, ["revoked", "valid"],
            "Response order must match input order (leaf, then CA); the "
            "root CA itself is never CRL-checked",
        )
        self.log.info("Multi-cert chain response preserves input order")

        # -- Validation edge cases --
        base_url = f"http://{self.cluster.master.ip}:8091/_cbauth/crlsValidate"
        auth = (self.rest.username, self.rest.password)

        def post(body):
            resp = requests.post(
                base_url, auth=auth, headers={"Content-Type": "application/json"},
                json=body, timeout=30,
            )
            return resp.status_code

        self.assertEqual(post({"certs": [], "scope": "clientAuth"}), 400)
        self.assertEqual(post({"certs": [revoked_b64], "scope": "bogusScope"}), 400)
        self.assertEqual(post({"certs": ["not valid base64 !!!"], "scope": "clientAuth"}), 400)
        self.assertEqual(
            post({"certs": [revoked_b64], "scope": "clientAuth", "policy": "Require"}), 400,
            "Unlike diagnostics/validate, there is no policy override param "
            "-- supplying one must be rejected, not silently ignored",
        )
        self.assertEqual(post({"certs": [revoked_b64] * 100, "scope": "clientAuth"}), 200)
        self.assertEqual(post({"certs": [revoked_b64] * 101, "scope": "clientAuth"}), 400)
        self.log.info("Validation edge cases (empty/bad-scope/bad-base64/"
                       "unsupported-field/cert-count boundary) all correct")

        # -- RBAC: strictly narrower than every other CRL endpoint. A
        # security_admin role has full access to /settings/crl itself but
        # is cleanly denied here -- this endpoint alone requires
        # {[admin,internal], all}. --
        sec_user, sec_pass = self._create_rbac_test_user(
            "cbauth_crls_secadmin", "security_admin"
        )
        code = requests.post(
            base_url, auth=(sec_user, sec_pass),
            headers={"Content-Type": "application/json"},
            json={"certs": [revoked_b64], "scope": "clientAuth"}, timeout=30,
        ).status_code
        self.assertEqual(
            code, 403,
            "security_admin has full /settings/crl access but must still "
            "be denied on crlsValidate -- it needs [admin,internal], a "
            "strictly narrower grant than every other CRL endpoint",
        )
        code = requests.get(
            f"http://{self.cluster.master.ip}:8091/settings/crl",
            auth=(sec_user, sec_pass), timeout=30,
        ).status_code
        self.assertEqual(
            code, 200,
            "Contrast check: the same role has normal access to the "
            "regular admin CRL endpoints",
        )
        self.log.info(
            "crlsValidate correctly requires [admin,internal] -- strictly "
            "narrower than every other CRL endpoint's [admin,security]"
        )

    def test_crl_cbauth_push_config(self):
        """The CRL 'push config' (cb_crl_manager:get_push_config/0) is what
        actually reaches cbauth-registered GO services and memcached --
        crlPolicyPerScope and a version number consumers use to know when
        to re-pull. Covers: the version bumps for every input that can
        change a revocation verdict (policy, checkIntermediateCerts, CRL
        files, trusted CAs), stays put on a genuine no-op re-post and on
        poll-interval-only changes (an 'operational' key, not 'hashed'),
        and the pushed policy always matches the real configured one.
        Also confirms the per-service cbauth CRL cache metrics exist with
        the right shape -- actually driving their hit/miss counters needs
        real downstream Go-service CRL-check activity, which is outside
        ns_server's control and out of scope here."""

        def version():
            return self.crl_utils.get_push_config_version(self.rest)

        v0 = version()

        # -- policyPerScope is a 'hashed' key: any change bumps the version --
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status, f"Policy change failed: {content}")
        v1 = version()
        self.assertNotEqual(v0, v1, "policyPerScope is hashed -- version must change")

        # -- Re-posting the exact same value is a genuine no-op --
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status)
        self.assertEqual(
            version(), v1, "Re-posting an unchanged value must not bump the version"
        )

        # -- checkIntermediateCerts is also a 'hashed' key --
        status, content = self.crl_utils.set_settings(
            self.rest, checkIntermediateCerts=True
        )
        self.assertTrue(status, f"Settings change failed: {content}")
        v2 = version()
        self.assertNotEqual(
            v1, v2, "checkIntermediateCerts is hashed -- version must change"
        )

        # -- dirPollIntervalMs is 'operational' -- version must NOT change --
        status, content = self.crl_utils.set_settings(
            self.rest, dirPollIntervalMs=45000
        )
        self.assertTrue(status, f"Settings change failed: {content}")
        self.assertEqual(
            version(), v2, "A poll-interval-only change must not affect the version"
        )
        self.crl_utils.set_settings(self.rest, dirPollIntervalMs=60000)
        self.log.info(
            "crlVersion bumps for hashed config keys, ignores a genuine "
            "no-op and an operational-only key change"
        )

        # -- Uploading/deleting a CRL file bumps it too, via file checksums --
        filename = "push_config_test.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1),
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        v3 = version()
        self.assertNotEqual(v2, v3, "Uploading a CRL file must bump the version")

        status, content = self.crl_utils.delete_file(self.rest, filename)
        self.assertTrue(status, f"CRL delete failed: {content}")
        v4 = version()
        self.assertNotEqual(v3, v4, "Deleting a CRL file must bump the version again")
        self.log.info("crlVersion reflects the loaded-CRL-file set too")

        # -- Trusting/untrusting a CA bumps it too, via the trusted-CA set.
        # self.ca_cert is already trusted from setUp -- re-trusting it would
        # be a genuine no-op, so a second, genuinely new CA is used here. --
        extra_ca_cert, _ = self.crl_utils.generate_ca("PushConfigExtraCA")
        self._trust_ca_on_cluster(extra_ca_cert)
        v5 = version()
        self.assertNotEqual(v4, v5, "Trusting a new CA must bump the version")
        self._cleanup_trusted_cas()
        v6 = version()
        self.assertNotEqual(v5, v6, "Untrusting a CA must bump the version again")
        self.log.info("crlVersion reflects the trusted-CA set too")

        # -- The pushed policy always matches the real configured policy --
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Permissive", "nodeToNode": "Require"},
        )
        self.assertTrue(status, f"Policy change failed: {content}")
        pushed_policy = self.crl_utils.get_push_config_policy_per_scope(self.rest)
        self.assertEqual(
            pushed_policy, {"clientAuth": "Permissive", "nodeToNode": "Require"},
            "Push payload's policy must match the real configured policy",
        )
        self.log.info("Push payload's policy always matches the real configured policy")

        # -- notify_crl_change is callable and doesn't error even with no
        # real connected consumers in this test environment --
        status, content = self.rest.diag_eval(
            "atom_to_list(menelaus_cbauth:notify_crl_change())."
        )
        self.assertTrue(status, f"notify_crl_change failed: {content}")
        text = content.decode() if isinstance(content, bytes) else content
        self.assertIn("ok", text)

        # -- Per-service cbauth CRL cache metrics exist with the right
        # shape. Actually driving their hit/miss counters needs real
        # downstream Go-service CRL-check activity (e.g. XDCR or projector
        # validating a cert), which ns_server-side test code cannot
        # trigger -- structural presence is as far as this can go. --
        all_metrics = self.crl_utils.get_all_metrics_text(self.cluster.master)
        for metric in (
            "cm_cbauth_crl_cache_current_items", "cm_cbauth_crl_cache_max_items",
            "cm_cbauth_crl_cache_hit_total", "cm_cbauth_crl_cache_miss_total",
        ):
            self.assertIn(metric, all_metrics, f"Expected {metric} to be exposed")
        self.log.info(
            "Per-service cbauth CRL cache metrics exist with the right "
            "shape (hit/miss counters need real downstream service "
            "activity to actually increment, outside ns_server's control)"
        )

    def test_crl_cert_chain_usage_encoding(self):
        """CRL checks across chain depth (root-direct, intermediate-issued),
        a fully untrusted chain rejected at chain validation before
        revocation is even considered, EKU-agnostic checking (server-auth
        and unsupported EKUs behave like any other cert), PEM vs
        base64-DER encoding, a multi-cert PEM chain evaluated as separate
        results, malformed cert input, and AKI/SKI disambiguation between
        two trusted CAs sharing the same subject name.

        Note: a multi-level chain where revoking the intermediate cert
        itself propagates to the leaf (checkIntermediateCerts) is already
        covered by test_crl_settings_scope_independence_and_ingestion;
        every leaf cert generated throughout this suite already carries
        the CLIENT_AUTH EKU used for real mTLS -- neither is re-tested
        here.
        """
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        # A cert issued directly by a (root) CA is revoked via that CA's
        # own CRL -- the baseline every other test in this suite already
        # relies on, asserted explicitly here for completeness.
        root_leaf_cert, root_leaf_key, root_leaf_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "chainRootDirect"
        )
        root_leaf_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(root_leaf_cert))
        root_leaf_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(root_leaf_key))
        filename = "cert_chain_root_direct.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [root_leaf_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"Root-direct revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(root_leaf_cert_path, root_leaf_key_path),
            "A cert issued directly by a root CA should be rejected via "
            "that CA's own CRL",
        )
        self.log.info("Root-direct cert correctly revoked via the root's own CRL")

        # A cert issued by an intermediate CA is revoked via a CRL issued
        # by that intermediate -- CRL-issuer trust doesn't chain through
        # the hierarchy, so the intermediate must be separately, explicitly
        # trusted even though its own cert chains to an already-trusted
        # root.
        intermediate_cert, intermediate_key, _ = self.crl_utils.generate_intermediate_ca(
            self.ca_cert, self.ca_key, "ChainIntermediateCA"
        )
        self._trust_ca_on_cluster(intermediate_cert)
        intermediate_leaf_cert, intermediate_leaf_key, intermediate_leaf_serial = (
            self.crl_utils.generate_leaf_cert(
                intermediate_cert, intermediate_key, "chainIntermediateLeaf"
            )
        )
        # Client presents the full chain (leaf then its issuing
        # intermediate), not just the leaf alone.
        chain_bundle = (
            self.crl_utils.cert_to_pem(intermediate_leaf_cert)
            + self.crl_utils.cert_to_pem(intermediate_cert)
        )
        chain_leaf_cert_path = self._write_temp_pem(chain_bundle)
        chain_leaf_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(intermediate_leaf_key)
        )
        filename = "cert_chain_intermediate_issued.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, intermediate_cert, intermediate_key, [intermediate_leaf_serial],
            filename, crl_number=1,
        )
        self.assertTrue(status, f"Intermediate-issued revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(chain_leaf_cert_path, chain_leaf_key_path),
            "A cert issued by an intermediate CA should be rejected via a "
            "CRL issued by that intermediate",
        )
        self.log.info(
            "Intermediate-issued cert correctly revoked via the "
            "intermediate's own CRL"
        )

        # A chain rooted in a completely untrusted CA is rejected at chain
        # validation -- before revocation is even considered, and
        # distinguishably so (a different TLS alert than a revocation).
        untrusted_ca_cert, untrusted_ca_key = self.crl_utils.generate_ca("ChainUntrustedCA")
        untrusted_leaf_cert, untrusted_leaf_key, _ = self.crl_utils.generate_leaf_cert(
            untrusted_ca_cert, untrusted_ca_key, "chainUntrustedLeaf"
        )
        untrusted_leaf_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(untrusted_leaf_cert)
        )
        untrusted_leaf_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(untrusted_leaf_key)
        )
        status, content = self.crl_utils.upload_file(
            self.rest, "cert_chain_untrusted.pem",
            self.crl_utils.build_crl(untrusted_ca_cert, untrusted_ca_key, crl_number=1),
        )
        self.assertFalse(status, f"CRL from an untrusted CA should be rejected, got: {content}")
        try:
            self.crl_utils.perform_mtls_handshake(
                self.cluster.master.ip, self.MGMT_PORT,
                untrusted_leaf_cert_path, untrusted_leaf_key_path,
            )
            self.fail("Handshake with an untrusted-CA cert should be rejected")
        except requests.exceptions.SSLError as exc:
            self.assertIn(
                "unknown ca", str(exc).lower(),
                f"Expected an unknown-CA chain-validation alert, distinct "
                f"from a revocation rejection, got: {exc}",
            )
        self.log.info(
            "Untrusted-CA chain rejected at chain validation, distinctly "
            "from a revocation rejection"
        )

        # CRL checks apply to server-auth-EKU certs too, via the admin
        # diagnostic endpoint (EKU-agnostic by design).
        server_eku_cert, _, server_eku_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "chainServerEku",
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH],
        )
        filename = "cert_chain_server_eku.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [server_eku_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"Server-EKU revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require",
            certs=[self.crl_utils.cert_to_pem(server_eku_cert).decode()],
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(content["results"][0]["status"], "revoked")
        self.log.info("Server-auth-EKU cert correctly checked via diagnostics/validate")

        # A cert with an EKU the CRL check doesn't specifically recognize
        # (neither client- nor server-auth) is still checked identically
        # -- there is no distinct "unsupported EKU" status; the check is
        # EKU-agnostic by design.
        odd_eku_cert, _, odd_eku_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "chainOddEku",
            extended_key_usage=[ExtendedKeyUsageOID.EMAIL_PROTECTION],
        )
        filename = "cert_chain_odd_eku.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [odd_eku_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"Odd-EKU revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require",
            certs=[self.crl_utils.cert_to_pem(odd_eku_cert).decode()],
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(
            content["results"][0]["status"], "revoked",
            "An unsupported EKU should not change or suppress the "
            "revocation status",
        )
        self.log.info("Cert with an unsupported EKU checked identically to any other cert")

        # PEM and base64-DER encodings of the same cert are accepted and
        # evaluated identically.
        pem_form = self.crl_utils.cert_to_pem(server_eku_cert).decode()
        der_b64_form = self.crl_utils.cert_to_der_b64(server_eku_cert)
        status, content_pem = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[pem_form]
        )
        self.assertTrue(status, f"diagnostics/validate (PEM) failed: {content_pem}")
        status, content_der = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[der_b64_form]
        )
        self.assertTrue(status, f"diagnostics/validate (base64-DER) failed: {content_der}")
        self.assertEqual(
            content_pem["results"], content_der["results"],
            "PEM and base64-DER of the same cert should evaluate identically",
        )
        self.log.info("PEM and base64-DER encodings evaluate identically")

        # A multi-block PEM chain (leaf + its issuing intermediate
        # concatenated) supplied as one certs[] entry is parsed and
        # evaluated as two separate results.
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[chain_bundle.decode()]
        )
        self.assertTrue(status, f"diagnostics/validate failed: {content}")
        self.assertEqual(
            len(content["results"]), 2,
            f"A concatenated leaf+intermediate PEM should evaluate as 2 "
            f"separate results, got: {content['results']}",
        )
        self.log.info("Multi-block PEM chain parsed and evaluated as separate results")

        # Malformed certificate input is rejected with a clear error --
        # two distinct shapes depending on whether the string is even
        # valid base64. A PEM-shaped but truncated block (e.g. "MIIB") is
        # still valid base64 as far as that check is concerned, so it
        # falls through to the same per-cert "failed" path as any other
        # well-formed-but-not-a-real-certificate input -- only a string
        # that isn't valid base64 at all trips the request-level check.
        garbage_b64 = base64.b64encode(b"not a real certificate, just bytes").decode()
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[garbage_b64]
        )
        self.assertTrue(
            status, f"Garbage-but-valid-base64 input should still get a 200: {content}"
        )
        self.assertEqual(content["results"][0]["status"], "failed")

        not_base64_at_all = "!!!not-base64-at-all###"
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy="Require", certs=[not_base64_at_all]
        )
        self.assertFalse(
            status, f"A string that isn't valid base64 at all should be rejected, got: {content}"
        )
        self.log.info("Malformed cert input rejected with two distinct, clear error shapes")

        # AKI/SKI disambiguates between two trusted CAs that share the
        # same subject name -- the right CA's CRL applies only to its own
        # certs, not the name-colliding one.
        colliding_cn = "ChainCollidingCA"
        ca_a_cert, ca_a_key = self.crl_utils.generate_ca(colliding_cn)
        ca_b_cert, ca_b_key = self.crl_utils.generate_ca(colliding_cn)
        self._trust_ca_on_cluster(ca_a_cert)
        self._trust_ca_on_cluster(ca_b_cert)
        leaf_a_cert, leaf_a_key, leaf_a_serial = self.crl_utils.generate_leaf_cert(
            ca_a_cert, ca_a_key, "chainCollidingLeafA"
        )
        leaf_a_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf_a_cert))
        leaf_a_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_a_key))
        leaf_b_cert, leaf_b_key, _ = self.crl_utils.generate_leaf_cert(
            ca_b_cert, ca_b_key, "chainCollidingLeafB"
        )
        leaf_b_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf_b_cert))
        leaf_b_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_b_key))

        filename = "cert_chain_colliding_ca.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename,
            self.crl_utils.build_crl(
                ca_a_cert, ca_a_key, revoked_serials=[leaf_a_serial], crl_number=1,
                add_authority_key_id=True,
            ),
        )
        self.assertTrue(status, f"Colliding-CA CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(leaf_a_cert_path, leaf_a_key_path),
            "leafA (revoked under CA-A) should be rejected",
        )
        self.assertTrue(
            self._handshake_ok(leaf_b_cert_path, leaf_b_key_path),
            "leafB (issued by a different CA that merely shares CA-A's "
            "subject name, not itself revoked) should connect -- AKI/SKI "
            "must disambiguate rather than misapplying CA-A's CRL",
        )
        self.log.info(
            "AKI/SKI correctly disambiguates two trusted CAs sharing a "
            "subject name"
        )

    def test_crl_bypass_hardening(self):
        """A revoked cert's cached TLS 1.2 session cannot be resumed to
        skip re-checking; revoked-serial matching is robust to DER
        leading-zero-padding; every CRL revocation-reason code rejects
        equally; a CRL with an unrecognized critical extension is rejected
        on upload, not silently applied; concurrent conflicting CRL
        updates never leave a transient weaker-enforcement window;
        clientAuth and nodeToNode enforce from their own policy setting
        only, never leaking into each other; and revoking a certificate
        does not touch a same-named password credential.

        Note: that the admin diagnostics endpoint itself requires
        authorization is already covered by
        test_crl_rbac_permission_boundaries; a fault-injected internal
        exception failing closed, and a rolled-back system clock, aren't
        exercised here -- neither has safe tooling available in this
        environment (matches the reasoning already applied to the
        equivalent manual checks).
        """
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        # A cached TLS 1.2 session for a certificate that is later revoked
        # cannot be resumed to skip re-checking -- resumption itself must
        # be rejected (a full re-check that then rejects the cert).
        resume_cert, resume_key, resume_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "bypassResumption"
        )
        resume_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(resume_cert))
        resume_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(resume_key))
        baseline_filename = "bypass_resumption_baseline.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, baseline_filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1),
        )
        self.assertTrue(status, f"Baseline CRL upload failed: {content}")
        self._track_uploaded_file(baseline_filename)
        self.crl_utils.reload_crl(self.rest)

        reused, session, resp = self.crl_utils.tls_handshake(
            self.cluster.master.ip, self.MGMT_PORT, resume_cert_path, resume_key_path,
            tls_version="1.2",
        )
        self.assertFalse(reused, "The first handshake should be full, not resumed")
        self.assertIsNotNone(resp, "Expected a real HTTP response over the full handshake")

        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [resume_serial], baseline_filename,
            crl_number=2,
        )
        self.assertTrue(status, f"Revoke upload failed: {content}")
        self.crl_utils.reload_crl(self.rest)

        try:
            reused2, _, resp2 = self.crl_utils.tls_handshake(
                self.cluster.master.ip, self.MGMT_PORT, resume_cert_path, resume_key_path,
                tls_version="1.2", session=session,
            )
            self.fail(
                f"Resuming a session for a now-revoked cert should be "
                f"rejected, got reused={reused2}, response={resp2}"
            )
        except ssl.SSLError as exc:
            self.assertIn(
                "revoked", str(exc).lower(),
                f"Expected a certificate-revoked alert on the resumption "
                f"attempt, got: {exc}",
            )
        self.log.info("TLS 1.2 session resumption of a revoked cert is correctly rejected")

        # Revoked-serial matching is robust to DER INTEGER leading-zero-
        # padding encoding variance -- a serial whose top byte has the
        # high bit set requires a leading 0x00 padding byte in its
        # canonical DER encoding.
        padded_serial = 0x80112233445566778899
        padded_cert, padded_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "bypassPaddedSerial", serial=padded_serial
        )
        padded_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(padded_cert))
        padded_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(padded_key))
        filename = "bypass_padded_serial.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [padded_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"Padded-serial revoking CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(padded_cert_path, padded_key_path),
            "A serial requiring DER leading-zero padding should still "
            "match and be rejected",
        )
        self.log.info("Revoked-serial matching is robust to DER leading-zero padding")

        # Every CRL revocation-reason code rejects equally -- none is
        # silently exempted.
        reason_certs = []
        reason_serials = {}
        for reason, cn in [
            (x509.ReasonFlags.key_compromise, "bypassReasonKeyCompromise"),
            (x509.ReasonFlags.cessation_of_operation, "bypassReasonCessation"),
            (x509.ReasonFlags.certificate_hold, "bypassReasonHold"),
            (x509.ReasonFlags.affiliation_changed, "bypassReasonAffiliation"),
        ]:
            cert, key, serial = self.crl_utils.generate_leaf_cert(self.ca_cert, self.ca_key, cn)
            cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
            key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
            reason_certs.append((cert_path, key_path))
            reason_serials[serial] = reason
        filename = "bypass_reason_codes.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename,
            self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=list(reason_serials.keys()),
                revocation_reasons=reason_serials, crl_number=1,
            ),
        )
        self.assertTrue(status, f"Reason-code CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        for cert_path, key_path in reason_certs:
            self.assertFalse(
                self._handshake_ok(cert_path, key_path),
                f"Cert revoked under reason code should be rejected regardless "
                f"of which reason ({cert_path})",
            )
        self.log.info("Every revocation-reason code rejects the cert equally")

        # A CRL with an unrecognized critical extension is rejected on
        # upload -- RFC 5280 says an application that can't process a
        # critical extension must not use the CRL at all, not silently
        # ignore just that extension.
        now = datetime.datetime.now(datetime.timezone.utc)
        unrecognized_oid = x509.ObjectIdentifier("1.2.3.4.5.6.7.8.9.99")
        bad_crl = (
            x509.CertificateRevocationListBuilder()
            .issuer_name(self.ca_cert.subject)
            .last_update(now - datetime.timedelta(days=1))
            .next_update(now + datetime.timedelta(days=30))
            .add_extension(
                x509.UnrecognizedExtension(unrecognized_oid, b"\x04\x04\xDE\xAD\xBE\xEF"),
                critical=True,
            )
            .sign(self.ca_key, hashes.SHA256())
            .public_bytes(serialization.Encoding.PEM)
        )
        status, content = self.crl_utils.upload_file(
            self.rest, "bypass_critical_extension.pem", bad_crl
        )
        self.assertFalse(
            status, f"A CRL with an unrecognized critical extension should be rejected, got: {content}"
        )
        self.assertIn(
            "critical extension", str(content.get("error", "")).lower(),
            f"Rejection should specifically name the unrecognized critical "
            f"extension, not fail for some unrelated reason, got: {content}",
        )
        self.log.info("CRL with an unrecognized critical extension rejected on upload")

        # Concurrent conflicting CRL updates never leave a transient
        # window of weaker enforcement -- a revoked cert stays rejected
        # throughout a delete/re-upload/reload race against its own file.
        # Clears every other file uploaded so far first: "freshest CRL
        # wins" is scoped per issuer, not per filename, so a leftover file
        # from an earlier check above (same CA, doesn't revoke this cert)
        # could otherwise become briefly authoritative during the DELETE
        # gap below and mask a real regression, or -- as caught live --
        # produce a false failure that has nothing to do with the race
        # itself.
        self._cleanup_created_files()
        race_cert, race_key, race_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "bypassRace"
        )
        race_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(race_cert))
        race_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(race_key))
        race_filename = "bypass_race.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [race_serial], race_filename, crl_number=1,
        )
        self.assertTrue(status, f"Race-fixture CRL upload failed: {content}")
        self._track_uploaded_file(race_filename)
        self.crl_utils.reload_crl(self.rest)

        stop_probing = threading.Event()
        probe_results = []

        def prober():
            while not stop_probing.is_set():
                probe_results.append(self._handshake_ok(race_cert_path, race_key_path))
                time.sleep(0.3)

        prober_thread = threading.Thread(target=prober)
        prober_thread.start()
        try:
            for i in range(5):
                self.crl_utils.delete_file(self.rest, race_filename)
                self.crl_utils.revoke_and_upload(
                    self.rest, self.ca_cert, self.ca_key, [race_serial], race_filename,
                    crl_number=2 + i,
                )
                self.crl_utils.reload_crl(self.rest)
        finally:
            stop_probing.set()
            prober_thread.join()
        self.assertGreater(len(probe_results), 0, "The prober thread never got a sample")
        self.assertTrue(
            all(result is False for result in probe_results),
            f"The revoked cert must never connect during a concurrent "
            f"delete/re-upload/reload race, got: {probe_results}",
        )
        self.log.info(
            f"Revoked cert stayed rejected across all {len(probe_results)} "
            f"probes during a concurrent CRL update race"
        )

        # clientAuth and nodeToNode each enforce from their own policy
        # setting only -- flipping the other scope to the opposite
        # extreme must not change this scope's own enforcement outcome.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Require"},
        )
        self.assertTrue(
            self._handshake_ok(race_cert_path, race_key_path),
            "clientAuth=Disabled should let a revoked cert connect, "
            "regardless of nodeToNode=Require",
        )
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertFalse(
            self._handshake_ok(race_cert_path, race_key_path),
            "clientAuth=Require should reject the same revoked cert once "
            "flipped, regardless of nodeToNode's value",
        )
        self.log.info(
            "clientAuth and nodeToNode enforce independently -- neither "
            "scope's policy leaks into the other's outcome"
        )

        # Revoking a certificate does not touch a same-named password
        # credential -- CRL revocation only ever governs cert-based
        # authentication, not a separate password credential for the
        # same logical identity.
        username, password = self._create_rbac_test_user("bypassRace", "views_admin[*]")
        resp = requests.get(
            f"http://{self.cluster.master.ip}:8091/whoami",
            auth=(username, password), timeout=30,
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Password login for an identity whose same-named cert is "
            f"revoked should still succeed, got {resp.status_code}: {resp.text}",
        )
        self.log.info(
            "Revoking a cert does not revoke a same-named password "
            "credential -- documented, expected behaviour"
        )

    def test_crl_restart_persistence(self):
        """CRL config and uploaded files survive process restarts, and
        enforcement resumes immediately afterward with zero observed
        window where a revoked cert is let through -- across three
        restart mechanisms: a graceful full-node restart, a kill of just
        the ns_server child process (other components unaffected), and a
        kill -9 of the top-level babysitter simulating an unclean crash.
        """
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "restartRevoked"
        )
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "restartValid"
        )
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))

        filename = "restart_persistence.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"Baseline CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

        def probe_state(cert_path, key_path):
            return self.crl_utils.probe_mtls_state(
                self.cluster.master.ip, self.MGMT_PORT, cert_path, key_path,
            )

        self.assertEqual(probe_state(revoked_cert_path, revoked_key_path), "rejected")
        self.assertEqual(probe_state(valid_cert_path, valid_key_path), "connected")
        self.log.info("Baseline: revoked cert rejected, valid cert connects")

        def probe_across_outage(trigger, max_wait=180):
            # Probes continuously through the whole outage and recovery,
            # not just before/after -- the point is to catch a transient
            # window, which a before/after check can't see at all.
            stop = threading.Event()
            recovered = threading.Event()
            revoked_states, valid_states = [], []

            def prober():
                while not stop.is_set():
                    revoked_states.append(probe_state(revoked_cert_path, revoked_key_path))
                    valid_states.append(probe_state(valid_cert_path, valid_key_path))
                    if valid_states[-1] == "connected":
                        recovered.set()
                    time.sleep(1)

            prober_thread = threading.Thread(target=prober)
            prober_thread.start()
            try:
                trigger()
                recovered.wait(timeout=max_wait)
                time.sleep(3)
            finally:
                stop.set()
                prober_thread.join()
            self.assertTrue(
                recovered.is_set(),
                f"Node never recovered within {max_wait}s -- can't tell "
                f"whether enforcement resumed at all",
            )
            return revoked_states, valid_states

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            # Config + uploaded files survive a full restart.
            status, settings_before = self.crl_utils.get_settings(self.rest)
            self.assertTrue(status)
            status, files_before = self.crl_utils.list_files(self.rest)
            self.assertTrue(status)

            # Continuous probing across a graceful full restart --
            # enforcement must be correct immediately on recovery, with
            # zero window where the revoked cert is ever let through,
            # including throughout the outage itself.
            def graceful_restart():
                shell.stop_couchbase()
                shell.start_couchbase()

            revoked_states, _ = probe_across_outage(graceful_restart)
            self.assertNotIn(
                "connected", revoked_states,
                f"Revoked cert must never connect, including during/around "
                f"a full restart, got: {revoked_states}",
            )
            self.log.info(
                f"Full restart: revoked cert never connected across "
                f"{len(revoked_states)} probes; valid cert correctly recovered"
            )

            status, settings_after = self.crl_utils.get_settings(self.rest)
            self.assertTrue(status, "Node should be fully functional again post-restart")
            self.assertEqual(
                settings_before.get("policyPerScope"), settings_after.get("policyPerScope"),
                "policyPerScope should survive a full restart unchanged",
            )
            status, files_after = self.crl_utils.list_files(self.rest)
            self.assertTrue(status)
            checksum_before = next(
                f["checksum"] for f in files_before if f["filename"] == filename
            )
            checksum_after = next(
                f["checksum"] for f in files_after if f["filename"] == filename
            )
            self.assertEqual(
                checksum_before, checksum_after,
                "The uploaded CRL file's checksum should survive a full "
                "restart unchanged",
            )
            # Confirm the node is fully functional again, not just
            # superficially back up. This still has to revoke the same
            # serial -- an empty/non-revoking CRL for the same CA would
            # become the freshest CRL for that issuer and silently
            # un-revoke revoked_cert for every probe from here on
            # ("freshest CRL wins per issuer", not a union of all files).
            post_restart_filename = "restart_post_restart_upload.pem"
            status, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, [revoked_serial],
                post_restart_filename, crl_number=2,
            )
            self.assertTrue(status, f"Post-restart upload should succeed: {content}")
            self._track_uploaded_file(post_restart_filename)
            self.crl_utils.reload_crl(self.rest)
            self.log.info("CRL config and uploaded files survive a full restart unchanged")

            # Killing only the ns_server child process (not memcached, not
            # the babysitter) restarts that component alone, with no
            # stale pre-restart CRL state and no effect on its siblings.
            memcached_pid_before = find_remote_pid(shell, "/opt/couchbase/bin/memcached ")
            ns_server_pid_before = find_remote_pid(shell, "child_start ns_bootstrap")
            self.assertIsNotNone(ns_server_pid_before, "Could not find the ns_server child PID")
            self.assertIsNotNone(memcached_pid_before, "Could not find the memcached PID")

            def kill_ns_server_child():
                shell.execute_command(f"kill -9 {ns_server_pid_before}")

            revoked_states, _ = probe_across_outage(kill_ns_server_child)
            self.assertNotIn(
                "connected", revoked_states,
                f"Revoked cert must never connect across an ns_server-child "
                f"restart, got: {revoked_states}",
            )
            memcached_pid_after = find_remote_pid(shell, "/opt/couchbase/bin/memcached ")
            ns_server_pid_after = find_remote_pid(shell, "child_start ns_bootstrap")
            self.assertEqual(
                memcached_pid_before, memcached_pid_after,
                "memcached should be completely unaffected by an "
                "ns_server-only restart",
            )
            self.assertNotEqual(
                ns_server_pid_before, ns_server_pid_after,
                "ns_server should have actually gotten a new PID -- a real "
                "restart, not a no-op",
            )
            self.log.info(
                "ns_server-only restart: memcached untouched, ns_server "
                "got a new PID, zero enforcement gap"
            )

            # An unclean crash of the whole node (kill -9 the top-level
            # babysitter, no graceful signal first) recovers automatically
            # via systemd, with the same zero-gap guarantee and correct
            # discrimination (not just failing everything closed by
            # accident of a broken state).
            babysitter_pid_before = find_remote_pid(shell, "run ns_babysitter_bootstrap")
            self.assertIsNotNone(babysitter_pid_before, "Could not find the babysitter PID")

            def kill_babysitter():
                shell.execute_command(f"kill -9 {babysitter_pid_before}")

            revoked_states, _ = probe_across_outage(kill_babysitter)
            self.assertNotIn(
                "connected", revoked_states,
                f"Revoked cert must never connect across an unclean crash, "
                f"got: {revoked_states}",
            )
            babysitter_pid_after = find_remote_pid(shell, "run ns_babysitter_bootstrap")
            self.assertNotEqual(
                babysitter_pid_before, babysitter_pid_after,
                "systemd should have restarted the whole service with a "
                "new babysitter PID",
            )
            status, settings_final = self.crl_utils.get_settings(self.rest)
            self.assertTrue(status)
            self.assertEqual(
                settings_before.get("policyPerScope"), settings_final.get("policyPerScope"),
                "policyPerScope should also survive an unclean crash unchanged",
            )
            self.assertEqual(
                probe_state(valid_cert_path, valid_key_path), "connected",
                "A valid cert should still connect post-recovery -- confirms "
                "correct discrimination, not an accidental fail-everything state",
            )
            self.log.info(
                "Unclean crash: systemd auto-recovered with a new "
                "babysitter PID, zero enforcement gap, config intact, "
                "correct discrimination"
            )
        finally:
            shell.disconnect()

    def test_crl_hot_reload_and_node_scoping(self):
        """An explicit reloadCrl applies a newly revoking CRL on the very
        next connection attempt with no restart; reloadCrl and a locally
        directory-polled CRL file are genuinely per-node (unlike uploaded
        CRL content, which replicates cluster-wide via chronicle regardless
        of reloadCrl); removing a revoking CRL then reloading restores
        access once the resulting missing-CRL state is itself tolerated;
        and re-issuing a newer CRL from the same issuer that simply omits
        a previously-revoked serial also restores access, under Require,
        without deleting the file or needing a missing-CRL-tolerant
        policy -- with identity mapping confirmed still working
        afterward, not just the TLS/CRL gate."""
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        # -- Upload a newly revoking CRL, explicitly reload, confirm
        # rejection on the very next attempt -- no restart. --
        leaf_cert, leaf_key, leaf_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "hotReloadLeaf"
        )
        leaf_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf_cert))
        leaf_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))

        filename = "hot_reload_revoking.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1),
        )
        self.assertTrue(status, f"Baseline (empty) CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertTrue(
            self._handshake_ok(leaf_cert_path, leaf_key_path),
            "Cert should connect before its serial is revoked",
        )

        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [leaf_serial], filename, crl_number=2,
        )
        self.assertTrue(status, f"Revoking CRL re-upload failed: {content}")
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(leaf_cert_path, leaf_key_path),
            "Cert must be rejected on the very next attempt after an "
            "explicit reloadCrl, with no restart",
        )
        self.log.info("Newly revoking CRL applied immediately via reloadCrl, no restart")

        # -- reloadCrl is a per-node REST call (whichever
        # node's mgmt port receives it), and a directory-polled file only
        # exists on the node it was actually written to -- unlike an
        # uploaded CRL's *content*, which chronicle replicates cluster-wide
        # near-instantly regardless of reloadCrl. self.rest is bound to
        # master, so writing the file to master's disk and calling
        # self.crl_utils.reload_crl(self.rest) only ever reloads master. --
        second = next(
            s for s in self.cluster.servers[:self.nodes_init]
            if s.ip != self.cluster.master.ip
        )
        master_key = f"{self.cluster.master.ip}:8091"
        second_key = f"{second.ip}:8091"
        poll_dir = f"/tmp/taf_crl_hot_reload_{uuid.uuid4().hex[:8]}"
        status, content = self.crl_utils.set_settings(
            self.rest, directory=poll_dir, dirPollIntervalMs=60000,
        )
        self.assertTrue(status, f"Directory setting update failed: {content}")
        dir_filename = "hot_reload_dir_poll.pem"
        dir_crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=3)

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            shell.execute_command(f"mkdir -p {poll_dir}")
            remote_write_file_b64(
                shell, f"{poll_dir}/{dir_filename}", dir_crl_pem.decode("utf-8")
            )
            self.crl_utils.reload_crl(self.rest)

            status, content = self.crl_utils.diagnostics_status(self.rest)
            self.assertTrue(status, f"diagnostics/status failed: {content}")
            master_files = {f["filename"] for f in content[master_key]["crlFiles"]}
            second_files = {f["filename"] for f in content[second_key]["crlFiles"]}
            self.assertIn(
                dir_filename, master_files,
                "Master should see the directory-dropped file after its "
                "own reloadCrl",
            )
            self.assertNotIn(
                dir_filename, second_files,
                "Second node never had this file written to its own disk "
                "and was never told to reload -- it must not see it",
            )
        finally:
            try:
                shell.execute_command(f"rm -rf {poll_dir}")
            except Exception as exc:
                self.log.warning(f"Failed to clean up {poll_dir}: {exc}")
            shell.disconnect()
        self.log.info(
            "reloadCrl and directory-polled files are per-node -- "
            "second node saw neither the file nor its effect"
        )

        # -- Removing the (only) revoking CRL and reloading
        # restores access, under a policy that tolerates the resulting
        # missing-CRL state (Permissive) -- otherwise a missing CRL is
        # itself a rejection and would mask whether removal took effect. --
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Permissive", "nodeToNode": "Disabled"},
            directory="/opt/couchbase/var/lib/couchbase/inbox/crls",
        )
        self.assertTrue(status, f"Policy/directory reset failed: {content}")
        self.assertFalse(
            self._handshake_ok(leaf_cert_path, leaf_key_path),
            "Cert should still be rejected -- its revoking CRL hasn't been "
            "removed yet",
        )
        status, content = self.crl_utils.delete_file(self.rest, filename)
        self.assertTrue(status, f"Deleting the revoking CRL failed: {content}")
        self._created_files.remove(filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertTrue(
            self._handshake_ok(leaf_cert_path, leaf_key_path),
            "Cert should connect again once its only revoking CRL is "
            "removed and reloaded, under a policy that tolerates a "
            "missing CRL",
        )
        self.log.info(
            "Removing the revoking CRL and reloading restores access "
            "under Permissive"
        )

        # -- Restoring access via a newer CRL that simply omits the
        # previously-revoked serial, rather than deleting the file
        # entirely -- the more realistic real-world pattern, since a CA
        # typically re-issues an updated CRL rather than removing it
        # outright. Back to Require: unlike the delete case above,
        # there's still an applicable (just non-revoking) CRL here, so
        # Permissive tolerance isn't needed. --
        status, content = self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(status, f"Policy reset failed: {content}")
        reissue_user, _ = self._create_rbac_test_user(
            "hotReloadReissueUser", "ro_security_admin"
        )
        reissue_cert, reissue_key, reissue_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, reissue_user
        )
        reissue_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(reissue_cert))
        reissue_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(reissue_key))

        reissue_filename = "hot_reload_reissue.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [reissue_serial], reissue_filename,
            crl_number=4,
        )
        self.assertTrue(status, f"Revoking CRL upload failed: {content}")
        self._track_uploaded_file(reissue_filename)
        self.crl_utils.reload_crl(self.rest)
        self.assertFalse(
            self._handshake_ok(reissue_cert_path, reissue_key_path),
            "Cert should be rejected while its serial is revoked",
        )

        # The CA re-issues a newer CRL (higher crl_number) that simply no
        # longer lists this serial -- "freshest CRL wins per issuer"
        # means this newer, non-revoking version becomes authoritative.
        status, content = self.crl_utils.upload_file(
            self.rest, reissue_filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=5),
        )
        self.assertTrue(status, f"Reissued CRL upload failed: {content}")
        self.crl_utils.reload_crl(self.rest)
        self.assertTrue(
            self._handshake_ok(reissue_cert_path, reissue_key_path),
            "Cert should connect again once a newer, non-revoking CRL "
            "from the same issuer supersedes the one that revoked it -- "
            "no file deletion or missing-CRL-tolerant policy needed",
        )
        whoami = self.crl_utils.get_identity_via_mtls(
            self.cluster.master.ip, self.MGMT_PORT, reissue_cert_path, reissue_key_path,
        )
        self.assertEqual(
            whoami.get("id"), reissue_user,
            f"Restored access should still go through identity mapping, "
            f"not just the TLS/CRL gate -- got whoami={whoami}",
        )
        self.log.info(
            "Re-issuing a newer CRL that omits a serial restores access "
            "under Require, without deleting the file -- identity "
            "mapping still works"
        )

    def test_crl_health_warnings(self):
        """A 'crl_expires_soon' health warning fires proactively for a CRL
        that hasn't expired yet but has already dropped inside its own
        proportional warning window (min(the configured 3-day window, 1/4
        of the CRL's own total validity period) -- confirmed from
        cb_crl_manager/menelaus_web_alerts_srv source, not assumed), the
        same CRL later flips to a distinctly-worded 'crl_expired' warning
        once it genuinely expires, and -- a known gap -- a CRL whose
        issuing CA becomes untrusted after being loaded is correctly
        detected by diagnostics/status but produces no health warning at
        all, since only these two alert types exist."""
        server = self.cluster.master
        node_key = f"{self.cluster.master.ip}:8091"

        # -- One short-lived CRL, observed through both real state
        # transitions in sequence -- proactively inside its warning window
        # first, then genuinely expired -- with zero extra uploads. --
        now = datetime.datetime.now(datetime.timezone.utc)
        this_update = now - datetime.timedelta(seconds=1000)
        next_update = now + datetime.timedelta(seconds=90)
        # Total validity 1090s -> proportional warning window = 1090/4 =
        # 272s, comfortably wider than the 90s actually remaining -- so
        # this is already inside the warning window at upload time, no
        # need to wait out a separate real-time countdown just to reach it.
        soon_filename = "health_warning_lifecycle.pem"
        expires_soon_baseline = self.crl_utils.get_crl_alert_count(
            server, "crl_expires_soon"
        )
        expired_baseline = self.crl_utils.get_crl_alert_count(server, "crl_expired")
        status, content = self.crl_utils.upload_file(
            self.rest, soon_filename,
            self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                this_update=this_update, next_update=next_update, crl_number=1,
            ),
        )
        self.assertTrue(status, f"Short-validity CRL upload failed: {content}")
        self._track_uploaded_file(soon_filename)
        self.crl_utils.reload_crl(self.rest)

        self.assertTrue(
            self.crl_utils.wait_for_crl_alert_increment(
                server, "crl_expires_soon", expires_soon_baseline, max_wait=100,
            ),
            "Expected 'crl_expires_soon' to fire for a CRL already inside "
            "its own proportional warning window, before actual expiry",
        )
        alert_msgs = self.crl_utils.get_alert_messages(self.rest)
        self.assertTrue(
            any("will expire at" in m and "TestCA1" in m for m in alert_msgs),
            "Expected a human-readable 'will expire at ...' alert naming "
            f"the issuing CA, got: {alert_msgs}",
        )
        self.log.info(
            "'crl_expires_soon' fired proactively, before actual "
            "expiry, with correct human-readable text"
        )

        self.assertTrue(
            self.crl_utils.wait_for_crl_alert_increment(
                server, "crl_expired", expired_baseline, max_wait=150,
            ),
            "Expected the same CRL to later trigger 'crl_expired', "
            "distinct from 'crl_expires_soon', once it genuinely expired",
        )
        alert_msgs = self.crl_utils.get_alert_messages(self.rest)
        self.assertTrue(
            any("has expired" in m and "TestCA1" in m for m in alert_msgs),
            "Expected a distinctly-worded 'has expired' alert naming the "
            f"issuing CA, got: {alert_msgs}",
        )
        self.log.info(
            "The same CRL later triggered a distinctly-worded "
            "'crl_expired' once it genuinely expired"
        )

        # The now-expired CRL above must be removed before the untrusted
        # sub-case below: the 'alerts_triggered' counter increments on
        # every ~60s check tick for as long as ANY CRL remains in the
        # expired state (confirmed from source -- the metric notification
        # is unconditional, only the human-readable alert message itself
        # is deduped), not just once per new alert. Left loaded, it would
        # keep re-incrementing 'crl_expired' independent of the untrust
        # action below and produce a false failure of that known-gap check.
        status, content = self.crl_utils.delete_file(self.rest, soon_filename)
        self.assertTrue(status, f"Deleting the expired CRL failed: {content}")
        self._created_files.remove(soon_filename)
        self.crl_utils.reload_crl(self.rest)

        # -- A separate, currently-valid, non-expiring CRL -- reusing the
        # just-expired one here would confound "no alert because
        # untrusted" with "no alert because it's already expired anyway". --
        untrusted_filename = "health_warning_untrusted.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, untrusted_filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=2),
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(untrusted_filename)
        self.crl_utils.reload_crl(self.rest)

        status, content = self.crl_utils.diagnostics_status(self.rest)
        self.assertTrue(status, f"diagnostics/status failed: {content}")
        entry = self.crl_utils.find_diagnostics_file_entry(content, node_key, untrusted_filename)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["cacheStatus"], "active")

        untrusted_soon_baseline = self.crl_utils.get_crl_alert_count(
            server, "crl_expires_soon"
        )
        untrusted_expired_baseline = self.crl_utils.get_crl_alert_count(
            server, "crl_expired"
        )
        status, content = self.crl_utils.untrust_ca_by_cn(self.rest, "TestCA1")
        self.assertTrue(status, f"Untrust-by-CN diag/eval failed: {content}")
        try:
            status, content = self.crl_utils.diagnostics_status(self.rest)
            self.assertTrue(status)
            entry = self.crl_utils.find_diagnostics_file_entry(
                content, node_key, untrusted_filename
            )
            self.assertIsNotNone(entry)
            self.assertEqual(
                entry["cacheStatus"], "untrusted",
                "CRL's issuing CA is no longer trusted -- diagnostics/status "
                "should flip to reflect it",
            )
            # A generous wait -- long enough for at least one alert-check
            # tick to have genuinely run and found nothing, not merely "not
            # checked yet".
            self.assertFalse(
                self.crl_utils.wait_for_crl_alert_increment(
                    server, "crl_expired", untrusted_expired_baseline, max_wait=90,
                ),
                "Known gap: no health warning exists for a CRL whose "
                "issuing CA became untrusted after load, even though "
                "diagnostics/status correctly detects it. If this now "
                "fails, the gap has been fixed -- flip to assertTrue and "
                "assert on the new alert's text.",
            )
            self.assertEqual(
                self.crl_utils.get_crl_alert_count(server, "crl_expires_soon"),
                untrusted_soon_baseline,
                "Known gap: an untrusted-CA CRL should not trigger "
                "'crl_expires_soon' either -- only 'crl_expired'/"
                "'crl_expires_soon' alert types exist at all, neither "
                "covers 'untrusted'",
            )
        finally:
            self._trust_ca_on_cluster(self.ca_cert)
        self.log.info(
            "Known gap: diagnostics/status correctly flips to 'untrusted', "
            "but no health warning of either type fires for it"
        )

    def test_crl_cross_service_kv_vs_ns_server_consistency(self):
        """The KV service (memcached's own SSL listener) and ns_server's
        mgmt HTTPS listener -- two independently-implemented enforcement
        paths that only share the same CRL configuration, not the same
        code -- reach the same accept/reject outcome for both a revoked
        and a valid cert. KV accepts the handshake optimistically and
        closes it asynchronously (~1s later) with the same "certificate
        revoked" TLS alert once its own async revocation check completes,
        unlike ns_server's mgmt listener, which rejects synchronously
        inside the handshake itself -- tls_handshake_ok() accounts for
        this, so both sides are compared on final outcome, not timing."""
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "crossServiceRevoked"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "crossServiceValid"
        )
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))

        filename = "cross_service_consistency.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

        host = self.cluster.master.ip
        revoked_kv_ok = self.crl_utils.tls_handshake_ok(
            host, self.KV_SSL_PORT, revoked_cert_path, revoked_key_path,
        )
        revoked_mgmt_ok = self.crl_utils.tls_handshake_ok(
            host, self.MGMT_PORT, revoked_cert_path, revoked_key_path,
        )
        self.assertFalse(revoked_kv_ok, "KV service should reject the revoked cert")
        self.assertFalse(revoked_mgmt_ok, "ns_server mgmt should reject the revoked cert")
        self.assertEqual(
            revoked_kv_ok, revoked_mgmt_ok,
            "KV and ns_server mgmt must reach the same outcome for the "
            "same revoked cert",
        )

        valid_kv_ok = self.crl_utils.tls_handshake_ok(
            host, self.KV_SSL_PORT, valid_cert_path, valid_key_path,
        )
        valid_mgmt_ok = self.crl_utils.tls_handshake_ok(
            host, self.MGMT_PORT, valid_cert_path, valid_key_path,
        )
        self.assertTrue(valid_kv_ok, "KV service should accept the valid cert")
        self.assertTrue(valid_mgmt_ok, "ns_server mgmt should accept the valid cert")
        self.assertEqual(
            valid_kv_ok, valid_mgmt_ok,
            "KV and ns_server mgmt must reach the same outcome for the "
            "same valid cert",
        )
        self.log.info(
            "KV service (memcached SSL) and ns_server mgmt HTTPS reach "
            "identical accept/reject outcomes for both a revoked and a "
            "valid cert"
        )

    def test_crl_performance_upload_timeout_and_handshake_overhead(self):
        """A large CRL upload honors a client-configured timeout --
        failing cleanly with no corrupted/partial server state when
        given too little time, and succeeding normally on retry with an
        adequate one -- and a populated CRL set does not measurably
        worsen mTLS handshake latency under Require vs Disabled. (KV-side
        CRUD latency under CRL enforcement is explicitly out of scope
        here -- that's KV-owned, not ns_server's.)"""
        # -- Upload timeout/retry: CRLUtils.upload_file's `timeout` is both
        # the requests.post() socket timeout AND the retry deadline
        # (couchbase_utils/cb_server_rest_util/security/crl.py) -- if the
        # HTTP round-trip completes at all within that window, the call
        # returns normally, no exception, regardless of how long server-
        # side validation took. So this can't rely on an assumed CRL
        # validation duration (that varies with node speed/load and was
        # flaky here) -- instead pick a timeout far too short for any real
        # network round-trip of a multipart upload this size to complete
        # in, independent of server-side processing time entirely.
        large_serials = list(range(1, 10001))
        large_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=large_serials, crl_number=1,
        )
        filename = "perf_large_upload.pem"
        too_short_timeout = 0.01

        start = time.monotonic()
        with self.assertRaises(Exception):
            self.crl_utils.upload_file(
                self.rest, filename, large_pem, timeout=too_short_timeout,
            )
        elapsed = time.monotonic() - start
        self.assertLess(
            elapsed, 30,
            f"A too-short upload timeout should fail promptly, not hang "
            f"indefinitely -- took {elapsed:.1f}s",
        )
        self.log.info(
            f"Too-short ({too_short_timeout}s) timeout for a 10k-entry CRL "
            f"failed cleanly after {elapsed:.1f}s"
        )

        status, content = self.crl_utils.list_files(self.rest)
        self.assertTrue(status)
        self.assertNotIn(
            filename, {f["filename"] for f in content},
            "A timed-out upload must leave no partial/corrupted file entry",
        )
        self.log.info("No partial entry left behind after the timed-out upload attempt")

        status, content = self.crl_utils.upload_file(
            self.rest, filename, large_pem, timeout=60,
        )
        self.assertTrue(status, f"Retry with an adequate timeout should succeed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        sample_cert, sample_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "perfLargeSample", serial=large_serials[0],
        )
        sample_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(sample_cert))
        sample_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(sample_key))
        self.assertFalse(
            self._handshake_ok(sample_cert_path, sample_key_path),
            "The retried upload should be genuinely functional -- a cert "
            "matching one of the 10k revoked serials must be rejected, "
            "not just accepted at upload time",
        )
        self.log.info(
            "Retried upload is genuinely functional: a sample revoked "
            "serial from the 10k-entry CRL is correctly enforced"
        )

        # -- Handshake latency overhead: Disabled vs Require with a
        # populated (5k-entry) CRL set, using a valid (never-revoked)
        # cert throughout -- so any difference reflects CRL-check
        # overhead itself, not a rejection. --
        overhead_cert, overhead_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "perfOverheadValid"
        )
        overhead_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(overhead_cert))
        overhead_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(overhead_key))
        host = self.cluster.master.ip
        sample_count = 15

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        disabled_timings = [
            self.crl_utils.time_tls_handshake(
                host, self.MGMT_PORT, overhead_cert_path, overhead_key_path,
            )
            for _ in range(sample_count)
        ]
        disabled_median = statistics.median(disabled_timings)

        populated_filename = "perf_handshake_overhead.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, populated_filename,
            self.crl_utils.build_crl(
                self.ca_cert, self.ca_key,
                revoked_serials=list(range(1, 5001)), crl_number=2,
            ),
            timeout=60,
        )
        self.assertTrue(status, f"5k-entry CRL upload failed: {content}")
        self._track_uploaded_file(populated_filename)
        self.crl_utils.reload_crl(self.rest)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self.assertTrue(
            self._handshake_ok(overhead_cert_path, overhead_key_path),
            "The valid cert must still connect under Require with the "
            "populated CRL loaded -- otherwise the timings below would "
            "be measuring a rejection path, not an accepted one",
        )
        require_timings = [
            self.crl_utils.time_tls_handshake(
                host, self.MGMT_PORT, overhead_cert_path, overhead_key_path,
            )
            for _ in range(sample_count)
        ]
        require_median = statistics.median(require_timings)

        self.log.info(
            f"Handshake latency medians -- Disabled: "
            f"{disabled_median * 1000:.1f}ms, Require+5k-entry-CRL: "
            f"{require_median * 1000:.1f}ms (Disabled samples: "
            f"{[round(t * 1000, 1) for t in disabled_timings]}, Require "
            f"samples: {[round(t * 1000, 1) for t in require_timings]})"
        )
        # Generous bounds -- this is a "no unexpected multi-fold
        # regression" sanity check on shared lab infrastructure, not a
        # tight perf benchmark. The existing manual QA pass found medians
        # statistically indistinguishable across a 7x CRL-size range.
        self.assertLess(
            require_median, max(disabled_median * 5, 0.5),
            f"Require+populated-CRL handshake latency ({require_median * 1000:.1f}ms) "
            f"is an unexpected multi-fold regression vs Disabled "
            f"({disabled_median * 1000:.1f}ms)",
        )

    def test_crl_rebalance_and_failover_enforcement_continuity(self):
        """A deleted CRL file propagates to every cluster node, not just
        the one it was deleted from; CRL enforcement survives a
        rebalance-out/rebalance-in cycle and an auto-failover event with
        zero observed gap on existing/surviving cluster members, using a
        ~1.5s probe cadence matching the validated manual QA methodology
        for these rows. (A separately-tracked, disputed finding describes
        a much narrower, sub-second-only-detectable CRL enforcement gap
        specific to a node's first ~0.5-7s immediately after joining a
        cluster -- that is out of scope here pending resolution; this
        test intentionally checks the rejoined node's enforcement only
        once rebalance has completed, not via continuous sub-second
        probing of it during the join itself. A separate, confirmed
        finding that a rebalanced-*out* node resets its own CRL policy
        to Disabled -- rather than retaining it -- is also out of scope
        here pending a decision on filing/tracking it.)"""
        self._enable_client_cert_auth(state="enable")
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "topologyRevoked"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, "topologyValid"
        )
        revoked_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key))
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))

        filename = "topology_continuity.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [revoked_serial], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.reload_crl(self.rest)

        # target_server is never the master -- self.rest (bound to master)
        # stays usable for every admin/bookkeeping call throughout, and
        # _handshake_ok/_wait_until_handshake (hardcoded to master) stay valid.
        target_server = self.cluster.servers[self.nodes_init - 1]
        stable_servers = [
            s for s in self.cluster.servers[:self.nodes_init]
            if s.ip != target_server.ip
        ]
        stable_ips = [s.ip for s in stable_servers]

        # -- Delete propagation: a CRL file deleted via one node
        # disappears from every other node's view too, not just the
        # issuing one -- a separate fixture from `filename` above, since
        # that one needs to stay loaded (and revoking) for the rest of
        # this test. --
        delete_propagation_filename = "topology_delete_propagation.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, delete_propagation_filename,
            self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=2),
        )
        self.assertTrue(status, f"Delete-propagation fixture upload failed: {content}")
        for server in stable_servers:
            rest_n = RestConnection(server)
            status, files = self.crl_utils.list_files(rest_n)
            self.assertTrue(status, f"list_files failed on {server.ip}: {files}")
            self.assertIn(
                delete_propagation_filename, {f["filename"] for f in files},
                f"Fixture file missing on {server.ip} before delete",
            )
        status, content = self.crl_utils.delete_file(self.rest, delete_propagation_filename)
        self.assertTrue(status, f"Delete failed: {content}")
        for server in stable_servers:
            rest_n = RestConnection(server)
            status, files = self.crl_utils.list_files(rest_n)
            self.assertTrue(status, f"list_files failed on {server.ip}: {files}")
            self.assertNotIn(
                delete_propagation_filename, {f["filename"] for f in files},
                f"Deleted CRL file still present on {server.ip} -- delete did not propagate",
            )
            status, diag = self.crl_utils.diagnostics_status(rest_n)
            self.assertTrue(status, f"diagnostics_status failed on {server.ip}: {diag}")
            node_key = f"{server.ip}:8091"
            self.assertNotIn(
                delete_propagation_filename,
                {f["filename"] for f in diag.get(node_key, {}).get("crlFiles", [])},
                f"Deleted CRL file still present in {server.ip}'s diagnostics/status",
            )
        self.log.info("CRL delete propagates to every cluster node, not just the issuing one")

        # -- Rebalance-out: cluster-wide CRL config/files survive
        # unchanged on the remaining nodes. --
        self.assertTrue(
            self.task.rebalance(self.cluster, to_add=[], to_remove=[target_server]),
            "Rebalance-out failed",
        )
        for server in stable_servers:
            rest_n = RestConnection(server)
            status, settings = self.crl_utils.get_settings(rest_n)
            self.assertTrue(status, f"get_settings failed on {server.ip}: {settings}")
            self.crl_utils.assert_settings_equal(
                settings,
                {"policyPerScope": {"clientAuth": "Require", "nodeToNode": "Disabled"}},
            )
            status, files = self.crl_utils.list_files(rest_n)
            self.assertTrue(status, f"list_files failed on {server.ip}: {files}")
            self.assertIn(
                filename, {f["filename"] for f in files},
                f"Uploaded CRL file missing on {server.ip} after rebalance-out",
            )
        self.log.info(
            "Rebalance-out: CRL config/files unchanged on both remaining nodes"
        )

        # -- Rebalance back in, probing the existing members throughout. --
        states = self.crl_utils.probe_during(
            lambda: self.task.rebalance(self.cluster, to_add=[target_server], to_remove=[]),
            stable_ips, self.MGMT_PORT, revoked_cert_path, revoked_key_path,
        )
        for ip, samples in states.items():
            self.assertNotIn(
                "connected", samples,
                f"Revoked cert must never connect on {ip} throughout "
                f"rebalance-in, got: {samples}",
            )
        self.log.info(
            f"Rebalance-in: zero enforcement gap on existing members "
            f"throughout ({sum(len(s) for s in states.values())} total samples)"
        )
        self.assertEqual(
            self.crl_utils.probe_mtls_state(
                target_server.ip, self.MGMT_PORT, revoked_cert_path, revoked_key_path,
            ),
            "rejected",
            f"Rejoined node {target_server.ip} should enforce CRL "
            f"identically to existing members once rebalance completes",
        )
        self.log.info("Rejoined node enforces CRL correctly once rebalance completed")

        # -- Auto-failover: capture original settings to restore
        # afterward -- don't assume a disabled default. --
        orig_af = self.rest.get_autofailover_settings()
        self.assertIsNotNone(orig_af, "Failed to read original autoFailover settings")
        self.assertTrue(self.rest.reset_autofailover())
        self.assertTrue(
            self.rest.update_autofailover_settings(True, 10, maxCount=10),
            "Enabling autoFailover failed",
        )

        shell = RemoteMachineShellConnection(target_server)
        try:
            states = self.crl_utils.probe_during(
                shell.stop_couchbase, stable_ips, self.MGMT_PORT,
                revoked_cert_path, revoked_key_path,
            )
            self.assertTrue(
                self.crl_utils.wait_for_failover_count(
                    self.cluster_util, self.cluster.master, 1, timeout=240,
                ),
                "Expected 1 failed-over node within 240s",
            )
            for ip, samples in states.items():
                self.assertNotIn(
                    "connected", samples,
                    f"Revoked cert must never connect on {ip} throughout "
                    f"the failover trigger window, got: {samples}",
                )
            self.log.info(
                "Auto-failover triggered correctly; zero enforcement gap "
                "on survivors during the trigger window"
            )

            # No stale cached state post-failover.
            for ip in stable_ips:
                self.assertEqual(
                    self.crl_utils.probe_mtls_state(
                        ip, self.MGMT_PORT, revoked_cert_path, revoked_key_path,
                    ),
                    "rejected",
                    f"{ip} should still reject the revoked cert post-failover",
                )
                self.assertEqual(
                    self.crl_utils.probe_mtls_state(
                        ip, self.MGMT_PORT, valid_cert_path, valid_key_path,
                    ),
                    "connected",
                    f"{ip} should still accept the valid cert post-failover",
                )
            self.log.info(
                "Post-failover: survivors correctly reject the revoked "
                "cert and accept the valid cert -- no stale cached state"
            )
        finally:
            shell.start_couchbase()
            deadline = time.monotonic() + 90
            target_back_up = False
            while time.monotonic() < deadline:
                if self.crl_utils.probe_mtls_state(
                    target_server.ip, self.MGMT_PORT, valid_cert_path, valid_key_path,
                ) != "down":
                    target_back_up = True
                    break
                time.sleep(5)
            self.assertTrue(
                target_back_up,
                f"{target_server.ip} never became reachable again after start_couchbase()",
            )
            otp_id = f"ns_1@{target_server.ip}"
            self.rest.add_back_node(otp_id)
            self.rest.set_recovery_type(otpNode=otp_id, recoveryType="full")
            self.assertTrue(
                self.cluster_util.rebalance(
                    self.cluster, wait_for_completion=True, ejected_nodes=[],
                    validate_bucket_ranking=False,
                ),
                "Failback rebalance failed",
            )
            shell.disconnect()
            self.rest.update_autofailover_settings(
                orig_af.enabled, orig_af.timeout, maxCount=orig_af.maxCount,
            )
        self.log.info(
            "Cluster restored to 3 healthy members; autoFailover settings restored"
        )
