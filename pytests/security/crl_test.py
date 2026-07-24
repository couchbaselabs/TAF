from pytests.security.crl_base import CRLBase


class CRLTest(CRLBase):
    """
    PR1 foundational smoke tests — proves the REST wrapper / CRLUtils /
    CRLBase plumbing works end-to-end against a real cluster before further
    test-plan sections (see CRL_AGENTS.md) build on top of it.

    This slice covers settings CRUD and file lifecycle as a single happy-path
    flow rather than one test per operation, to keep coverage broad without
    proliferating test cases — mirrors how the manual CRL test docs
    (CRL_CORE_FLOW_SMOKE_TEST_PLAN.md) sequence steps within one flow. The
    mTLS revoke-then-reject smoke test lands in a follow-up PR (more moving
    parts — cert generation, RBAC user, client-cert-auth config — deserves
    its own focused review).
    """

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
