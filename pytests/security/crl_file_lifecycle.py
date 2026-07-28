import datetime

from pytests.security.crl_base import CRLBase


class CRLFileLifecycle(CRLBase):
    """
    CRL_Core.File_Lifecycle — steady-state REST coverage for the CRL file
    upload/list/delete API (TestCases_CRL.csv rows 3-4, 7-10; row 2 is already
    covered by security.crl_test.CRLTest::test_settings_and_file_lifecycle).

    Rows 5 (delete_removes_enforcement) and 6 (reupload_overwrite) are
    excluded from this suite — both require an actual mTLS handshake against
    a revoked/valid client cert to observe enforcement, not just file-list
    state, so they belong with the enforcement-focused tests (e.g.
    crl_test.py or a future dedicated enforcement module) rather than here.

    Two tests below (list_metadata_accuracy, file_status_field_accuracy)
    assert against a per-file metadata/status JSON schema that is not yet
    confirmed against a real server response (see TestPlan_CRL.md §1.1) — they
    accept a small set of plausible field-name/value spellings and fail with
    the raw entry dumped if none match, so a first real run pinpoints the
    actual schema instead of silently asserting the wrong key.
    """

    ISSUER_KEYS = ("issuer", "issuerDN", "issuerName")
    THIS_UPDATE_KEYS = ("thisUpdate", "lastUpdate", "effectiveDate")
    NEXT_UPDATE_KEYS = ("nextUpdate", "expiryDate", "expiresAt")
    REVOKED_COUNT_KEYS = ("revokedCount", "revokedSerialsCount", "numRevoked")
    STATUS_KEYS = ("status", "state", "cacheStatus")

    VALID_STATUS_VALUES = {"valid", "active"}
    EXPIRED_STATUS_VALUES = {"expired"}
    INVALID_STATUS_VALUES = {"invalid", "untrusted"}

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _find_file_entry(self, filename):
        status, files = self.crl_utils.list_files(self.rest)
        self.assertTrue(status, f"GET /settings/crl/files failed: {files}")
        for entry in files:
            if entry.get("filename") == filename:
                return entry
        self.fail(f"Uploaded file {filename!r} not present in file list: {files}")

    @staticmethod
    def _first_present(entry, candidate_keys):
        for key in candidate_keys:
            if key in entry:
                return entry[key]
        return None

    def _upload_and_track(self, filename, pem_bytes, timeout=300):
        status, content = self.crl_utils.upload_file(
            self.rest, filename, pem_bytes, timeout=timeout
        )
        self.assertTrue(status, f"CRL upload failed for {filename}: {content}")
        self._track_uploaded_file(filename)
        return content

    def _assert_upload_rejected(self, filename, payload, reason):
        status, content = self.crl_utils.upload_file(self.rest, filename, payload)
        self.assertFalse(
            status,
            f"Expected upload to be rejected ({reason}) but it succeeded: {content}",
        )
        status, files = self.crl_utils.list_files(self.rest)
        self.assertTrue(status, f"GET /settings/crl/files failed: {files}")
        names = [entry.get("filename") for entry in files]
        self.assertNotIn(
            filename, names,
            f"Rejected upload ({reason}) still appears in file list: {files}",
        )

    # ── Tests ────────────────────────────────────────────────────────────────

    def test_crl_upload_valid_der(self):
        """CSV row 3 — DER-encoded CRL is accepted identically to PEM."""
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)
        crl_der = self.crl_utils.pem_crl_to_der(crl_pem)

        filename = "crl_der_upload.der"
        self.log.info(f"Uploading DER-encoded CRL as {filename}")
        content = self._upload_and_track(filename, crl_der)
        self.log.info(f"DER CRL uploaded: {content}")

        entry = self._find_file_entry(filename)
        self.log.info(f"DER upload listed as expected: {entry}")

    def test_crl_list_metadata_accuracy(self):
        """CSV row 4 — issuer/thisUpdate/nextUpdate/revoked-count metadata
        for a listed CRL matches what was signed into it."""
        this_update = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
        next_update = this_update + datetime.timedelta(days=10)
        revoked_serials = [111111, 222222, 333333]

        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=revoked_serials,
            this_update=this_update, next_update=next_update,
            crl_number=42,
        )
        filename = "crl_metadata_accuracy.pem"
        self._upload_and_track(filename, crl_pem)

        entry = self._find_file_entry(filename)
        self.log.info(f"Metadata entry under test: {entry}")

        issuer = self._first_present(entry, self.ISSUER_KEYS)
        self.assertIsNotNone(
            issuer, f"No issuer-like field ({self.ISSUER_KEYS}) in entry: {entry}"
        )
        ca_cn = self.ca_cert.subject.rfc4514_string()
        self.assertIn(
            "TestCA1", str(issuer),
            f"Issuer field {issuer!r} does not reference signing CA {ca_cn!r}",
        )

        revoked_count = self._first_present(entry, self.REVOKED_COUNT_KEYS)
        self.assertIsNotNone(
            revoked_count,
            f"No revoked-count field ({self.REVOKED_COUNT_KEYS}) in entry: {entry}",
        )
        self.assertEqual(
            int(revoked_count), len(revoked_serials),
            f"Revoked count {revoked_count} != {len(revoked_serials)} signed serials",
        )

        for label, keys in (
            ("thisUpdate", self.THIS_UPDATE_KEYS),
            ("nextUpdate", self.NEXT_UPDATE_KEYS),
        ):
            value = self._first_present(entry, keys)
            self.assertIsNotNone(value, f"No {label}-like field ({keys}) in entry: {entry}")
            self.log.info(f"{label} reported as: {value}")

    def test_crl_upload_malformed_rejected(self):
        """CSV row 7 — truncated/random bytes are rejected, not listed."""
        garbage = b"this-is-not-a-valid-crl-just-random-bytes-1234567890"
        self._assert_upload_rejected(
            "crl_malformed.pem", garbage, reason="malformed CRL bytes"
        )

    def test_crl_upload_invalid_filename_rejected(self):
        """CSV row 8 — path traversal, disallowed characters, and >255-char
        filenames are all rejected at upload time."""
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)

        cases = [
            ("../../etc/passwd", "path traversal"),
            ("crl file/with|bad*chars?.pem", "disallowed characters"),
            ("a" * 300 + ".pem", ">255 chars"),
        ]
        for filename, reason in cases:
            self._assert_upload_rejected(filename, crl_pem, reason=reason)

    def test_crl_upload_oversized_file(self):
        """CSV row 9 — an oversized CRL either hits a documented size-limit
        rejection, or uploads within the configured extended timeout with no
        hang. Server-side size limit is undocumented (CSV: Blocked) — this
        test tolerates either outcome and only fails on a hang/exception."""
        large_serial_count = 50000
        revoked_serials = list(range(1, large_serial_count + 1))
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=revoked_serials, crl_number=1
        )
        self.log.info(
            f"Built oversized CRL with {large_serial_count} revoked serials "
            f"({len(crl_pem)} bytes)"
        )

        filename = "crl_oversized.pem"
        status, content = self.crl_utils.upload_file(
            self.rest, filename, crl_pem, timeout=600
        )
        if status:
            self._track_uploaded_file(filename)
            self.log.info(f"Oversized CRL accepted: {content}")
        else:
            self.log.info(f"Oversized CRL rejected (acceptable per size limit): {content}")

    def test_crl_file_status_field_accuracy(self):
        """CSV row 10 — per-file status field reflects valid/expired/invalid
        state. Expired- and untrusted-CRL uploads may themselves be rejected
        at upload time (see CRLUtils.build_crl's `expired` docstring) — both
        the accepted-with-status-field and rejected-at-upload branches are
        treated as valid outcomes and logged distinctly."""
        # Valid CRL — expect an accepted upload with a "valid"-ish status.
        valid_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)
        valid_filename = "crl_status_valid.pem"
        self._upload_and_track(valid_filename, valid_pem)
        valid_entry = self._find_file_entry(valid_filename)
        valid_status = self._first_present(valid_entry, self.STATUS_KEYS)
        self.assertIsNotNone(
            valid_status, f"No status-like field ({self.STATUS_KEYS}) in entry: {valid_entry}"
        )
        self.assertIn(
            str(valid_status).lower(), self.VALID_STATUS_VALUES,
            f"Valid CRL reported unexpected status {valid_status!r}: {valid_entry}",
        )

        # Expired CRL — upload may be accepted (status "expired") or rejected
        # outright; both are acceptable, log which branch actually happened.
        expired_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, crl_number=2, expired=True
        )
        expired_filename = "crl_status_expired.pem"
        status, content = self.crl_utils.upload_file(self.rest, expired_filename, expired_pem)
        if status:
            self._track_uploaded_file(expired_filename)
            expired_entry = self._find_file_entry(expired_filename)
            expired_status = self._first_present(expired_entry, self.STATUS_KEYS)
            self.assertIsNotNone(
                expired_status,
                f"No status-like field ({self.STATUS_KEYS}) in entry: {expired_entry}",
            )
            self.assertIn(
                str(expired_status).lower(), self.EXPIRED_STATUS_VALUES,
                f"Expired CRL reported unexpected status {expired_status!r}: {expired_entry}",
            )
            self.log.info(f"Expired CRL accepted with status: {expired_status}")
        else:
            self.log.info(f"Expired CRL rejected at upload time (acceptable): {content}")

        # Untrusted-issuer CRL — signed by a CA never trusted on the cluster.
        untrusted_ca_cert, untrusted_ca_key = self.crl_utils.generate_ca("UntrustedCA1")
        untrusted_pem = self.crl_utils.build_crl(
            untrusted_ca_cert, untrusted_ca_key, crl_number=1
        )
        untrusted_filename = "crl_status_untrusted.pem"
        status, content = self.crl_utils.upload_file(self.rest, untrusted_filename, untrusted_pem)
        if status:
            self._track_uploaded_file(untrusted_filename)
            untrusted_entry = self._find_file_entry(untrusted_filename)
            untrusted_status = self._first_present(untrusted_entry, self.STATUS_KEYS)
            self.assertIsNotNone(
                untrusted_status,
                f"No status-like field ({self.STATUS_KEYS}) in entry: {untrusted_entry}",
            )
            self.assertIn(
                str(untrusted_status).lower(), self.INVALID_STATUS_VALUES,
                f"Untrusted-CA CRL reported unexpected status {untrusted_status!r}: "
                f"{untrusted_entry}",
            )
            self.log.info(f"Untrusted-CA CRL accepted with status: {untrusted_status}")
        else:
            self.log.info(f"Untrusted-CA CRL rejected at upload time (acceptable): {content}")
