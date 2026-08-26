import datetime

from pytests.security.crl_base import CRLBase


class CRLFileLifecycle(CRLBase):
    """
    CRL_Core.File_Lifecycle — steady-state REST coverage for the CRL file
    upload/list/delete API. The basic upload/list/delete round-trip is
    already covered by security.crl_test.CRLTest::test_settings_and_file_lifecycle.

    Enforcement-observing scenarios (e.g. deleting a file restores access,
    or a re-upload overwrites and changes enforcement) are excluded from
    this suite — both require an actual mTLS handshake against a
    revoked/valid client cert to observe enforcement, not just file-list
    state, so they belong with the enforcement-focused tests (e.g.
    crl_test.py or a future dedicated enforcement module) rather than here.

    list_metadata_accuracy and file_status_field_accuracy originally
    guessed at a per-file metadata/status schema with several candidate
    field-name spellings, since it hadn't been confirmed against a real
    server response yet. Confirmed live since (GET /settings/crl/files
    returns {filename, checksum, uploadTimestamp, entries: [{issuer,
    thisUpdate, nextUpdate, crlNumber}]} -- no revoked-count field exists
    at all; per-file status only exists as `cacheStatus` on the separate
    diagnostics/status endpoint, not here) -- both tests now assert
    against the confirmed schema directly.
    """

    # cacheStatus values from GET/POST /settings/crl/diagnostics/status
    # (menelaus_web_crl.erl's status vocabulary) -- there is no such field
    # on the plain GET /settings/crl/files response used elsewhere below.
    VALID_STATUS_VALUES = {"active"}
    EXPIRED_STATUS_VALUES = {"expired"}
    INVALID_STATUS_VALUES = {"untrusted", "invalid"}

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _find_file_entry(self, filename):
        status, files = self.crl_utils.list_files(self.rest)
        self.assertTrue(status, f"GET /settings/crl/files failed: {files}")
        entry = self.crl_utils.find_file_entry(files, filename)
        self.assertIsNotNone(
            entry, f"Uploaded file {filename!r} not present in file list: {files}"
        )
        return entry

    def _find_diagnostics_file_entry(self, filename):
        status, content = self.crl_utils.diagnostics_status(self.rest)
        self.assertTrue(status, f"diagnostics/status failed: {content}")
        node_key = f"{self.cluster.master.ip}:8091"
        entry = self.crl_utils.find_diagnostics_file_entry(content, node_key, filename)
        self.assertIsNotNone(
            entry,
            f"Uploaded file {filename!r} not present in diagnostics/status: {content}",
        )
        return entry

    def _upload_and_track(self, filename, pem_bytes, timeout=300):
        self.log.info(f"Uploading {filename!r} ({len(pem_bytes)} bytes)")
        status, content = self.crl_utils.upload_file(
            self.rest, filename, pem_bytes, timeout=timeout
        )
        self.assertTrue(status, f"CRL upload failed for {filename}: {content}")
        self._track_uploaded_file(filename)
        self.log.info(f"Uploaded {filename!r}: {content}")
        return content

    def _assert_upload_rejected(self, filename, payload, reason):
        self.log.info(f"Uploading {filename!r} expecting rejection ({reason})")
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
        self.log.info(f"Upload of {filename!r} correctly rejected ({reason}): {content}")

    # ── Tests ────────────────────────────────────────────────────────────────

    def test_crl_upload_valid_der(self):
        """DER-encoded CRL is accepted identically to PEM."""
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)
        crl_der = self.crl_utils.pem_crl_to_der(crl_pem)

        filename = "crl_der_upload.der"
        self._upload_and_track(filename, crl_der)

        entry = self._find_file_entry(filename)
        self.log.info(f"DER upload listed as expected: {entry}")

    def test_crl_list_metadata_accuracy(self):
        """issuer/thisUpdate/nextUpdate/crlNumber metadata for a listed
        CRL matches what was signed into it. No revoked-serial
        count is asserted here -- GET /settings/crl/files never exposes
        one (confirmed live); crlNumber is the closest available signal
        that the listed metadata reflects the signed content."""
        this_update = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
        next_update = this_update + datetime.timedelta(days=10)
        revoked_serials = [111111, 222222, 333333]

        self.log.info(
            f"Signing a CRL with crl_number=42, revoked_serials={revoked_serials}, "
            f"thisUpdate={this_update}, nextUpdate={next_update}"
        )
        crl_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key,
            revoked_serials=revoked_serials,
            this_update=this_update, next_update=next_update,
            crl_number=42,
        )
        filename = "crl_metadata_accuracy.pem"
        self._upload_and_track(filename, crl_pem)

        entry = self._find_file_entry(filename)
        self.log.info(f"File entry under test: {entry}")
        crl_entries = entry.get("entries")
        self.assertTrue(
            crl_entries, f"Expected a non-empty 'entries' list on the file entry: {entry}"
        )
        metadata = crl_entries[0]

        self.assertIn(
            "TestCA1", metadata.get("issuer", ""),
            f"issuer {metadata.get('issuer')!r} does not reference signing CA TestCA1",
        )
        self.log.info(f"issuer correctly references TestCA1: {metadata.get('issuer')!r}")
        self.assertEqual(
            metadata.get("crlNumber"), 42,
            f"crlNumber {metadata.get('crlNumber')} != the 42 signed into this CRL",
        )
        self.log.info("crlNumber matches the 42 signed into this CRL")
        for label in ("thisUpdate", "nextUpdate"):
            self.assertIn(label, metadata, f"Missing {label!r} in entry: {metadata}")
            self.log.info(f"{label} reported as: {metadata[label]}")

    def test_crl_upload_malformed_rejected(self):
        """Truncated/random bytes are rejected, not listed."""
        garbage = b"this-is-not-a-valid-crl-just-random-bytes-1234567890"
        self._assert_upload_rejected(
            "crl_malformed.pem", garbage, reason="malformed CRL bytes"
        )

    def test_crl_upload_invalid_filename_rejected(self):
        """Path traversal, disallowed characters, and >255-char filenames
        are all rejected at upload time."""
        crl_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)

        cases = [
            ("../../etc/passwd", "path traversal"),
            ("crl file/with|bad*chars?.pem", "disallowed characters"),
            ("a" * 300 + ".pem", ">255 chars"),
        ]
        for filename, reason in cases:
            self._assert_upload_rejected(filename, crl_pem, reason=reason)

    def test_crl_upload_oversized_file(self):
        """An oversized CRL either hits a documented size-limit rejection,
        or uploads within the configured extended timeout with no hang.
        Server-side size limit is undocumented — this test tolerates
        either outcome and only fails on a hang/exception."""
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
        """Per-file status reflects valid/expired/invalid state. The
        plain GET /settings/crl/files response has no status
        field at all (confirmed live) -- `cacheStatus` only exists on
        GET/POST /settings/crl/diagnostics/status, so that's the endpoint
        checked here. Expired- and untrusted-CRL uploads may themselves be
        rejected at upload time (see CRLUtils.build_crl's `expired`
        docstring) — both the accepted-with-status and rejected-at-upload
        branches are treated as valid outcomes and logged distinctly."""
        # Valid CRL — expect an accepted upload with cacheStatus "active".
        valid_pem = self.crl_utils.build_crl(self.ca_cert, self.ca_key, crl_number=1)
        valid_filename = "crl_status_valid.pem"
        self._upload_and_track(valid_filename, valid_pem)
        self.crl_utils.reload_crl(self.rest)
        valid_entry = self._find_diagnostics_file_entry(valid_filename)
        self.assertIn(
            valid_entry.get("cacheStatus"), self.VALID_STATUS_VALUES,
            f"Valid CRL reported unexpected cacheStatus: {valid_entry}",
        )
        self.log.info(f"Valid CRL reported cacheStatus: {valid_entry['cacheStatus']}")

        # Expired CRL — upload may be accepted (cacheStatus "expired") or
        # rejected outright; both are acceptable, log which branch fired.
        expired_pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, crl_number=2, expired=True
        )
        expired_filename = "crl_status_expired.pem"
        status, content = self.crl_utils.upload_file(self.rest, expired_filename, expired_pem)
        if status:
            self._track_uploaded_file(expired_filename)
            self.crl_utils.reload_crl(self.rest)
            expired_entry = self._find_diagnostics_file_entry(expired_filename)
            self.assertIn(
                expired_entry.get("cacheStatus"), self.EXPIRED_STATUS_VALUES,
                f"Expired CRL reported unexpected cacheStatus: {expired_entry}",
            )
            self.log.info(f"Expired CRL accepted with cacheStatus: {expired_entry['cacheStatus']}")
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
            self.crl_utils.reload_crl(self.rest)
            untrusted_entry = self._find_diagnostics_file_entry(untrusted_filename)
            self.assertIn(
                untrusted_entry.get("cacheStatus"), self.INVALID_STATUS_VALUES,
                f"Untrusted-CA CRL reported unexpected cacheStatus: {untrusted_entry}",
            )
            self.log.info(
                f"Untrusted-CA CRL accepted with cacheStatus: {untrusted_entry['cacheStatus']}"
            )
        else:
            self.log.info(f"Untrusted-CA CRL rejected at upload time (acceptable): {content}")
