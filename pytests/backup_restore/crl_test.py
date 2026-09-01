import requests
from shell_util.remote_connection import RemoteMachineShellConnection

from pytests.backup_restore.crl_base import CRLBackupRestoreBase

MGMT_PORT = 18091

# Endpoint groups served directly by cbbs's own listener on 18097 -- NOT
# proxied through ns_server's mgmt port. (get_cluster_info/get_current_config
# in manage_and_config.py go through base_url + "/_p/backup/..." on 18091
# instead, which would only exercise ns_server's own mTLS enforcement --
# already covered by pytests/security/crl_test.py -- so they're deliberately
# not used here.)
ENDPOINT_GROUPS = {
    "plan": "/api/v1/plan",
    "repository": "/api/v1/cluster/self/repository/active",
}


class CRLBackupRestoreTest(CRLBackupRestoreBase):
    """
    CRL enforcement on backup/restore's own TLS paths: the Backup Service
    REST API (18097) and cbbackupmgr's peer-certificate verification against
    the cluster. Covers the P0 scenarios from CRL_Backup_Restore_TestPlan
    that don't need a multi-node backup-service gRPC topology, a
    long-running backup revoked mid-flight, or object-store credentials
    (P0-07, P0-11, P0-14 -- deferred).
    """

    def test_backup_service_rest_revoked_and_valid_cert_rbac_unaffected(self):
        """
        P0-01 / P0-02: a revoked client cert is rejected at the TLS layer on
        the Backup Service REST API (18097) across the endpoint groups
        tried; a valid, non-revoked cert continues to authenticate and
        reach the REST layer exactly as before. Role-level RBAC 403
        differentiation is already covered generically by
        pytests/security/crl_test.py's mTLS suite -- this test's job is
        proving cbbs's own listener enforces/doesn't-break under CRL, not
        re-proving RBAC semantics.
        """
        valid_user, _ = self._create_rbac_test_user(
            "crl_bkp_valid_admin", "backup_admin"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, valid_user
        )
        valid_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(valid_key))

        revoked_user, _ = self._create_rbac_test_user(
            "crl_bkp_revoked_admin", "backup_admin"
        )
        revoked_cert, revoked_key, revoked_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, revoked_user
        )
        revoked_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(revoked_cert)
        )
        revoked_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(revoked_key)
        )

        filename = "bkp_rest_p0_01_02.pem"
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

        for group, path in ENDPOINT_GROUPS.items():
            resp = self._wait_for_backup_service_ok(
                "GET", path, cert=(valid_cert_path, valid_key_path)
            )
            self.assertEqual(
                resp.status_code, 200,
                f"Valid cert should reach the {group} endpoint group ({path}) "
                f"on the Backup Service REST API, got {resp.status_code}: "
                f"{resp.text}"
            )
            self.log.info(f"Valid cert reached {group} endpoint group as expected")

            self.assert_cert_refused(
                lambda: self._backup_service_request(
                        "GET", path, cert=(revoked_cert_path, revoked_key_path)
                    ),
                f"Revoked cert must be rejected at the TLS layer for the "
                    f"{group} endpoint group ({path}), not merely denied at "
                    f"the REST layer",
            )
            self.log.info(
                f"Revoked cert rejected for {group} endpoint group as expected"
            )

    def test_backup_service_enforcement_order_and_hybrid_mtls_password_fallback(self):
        """
        P0-03 / P0-05 / P0-06: revocation is evaluated before identity
        extraction/RBAC (a revoked cert mapped to a full backup-admin user
        is still rejected at the TLS layer); in hybrid mTLS, a request with
        no certificate still authenticates by password; a revoked
        certificate presented in hybrid mode is rejected outright, with no
        fallback to password authentication.
        """
        admin_user, admin_password = self._create_rbac_test_user(
            "crl_bkp_order_admin", "backup_admin"
        )
        admin_cert, admin_key, admin_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, admin_user
        )
        admin_cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(admin_cert))
        admin_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(admin_key))

        filename = "bkp_rest_p0_03_05_06.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, admin_serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        # ── Hybrid first: no cert, password only -> succeeds. Hybrid must
        # run before Mandatory below, not after -- there is no way back from
        # Mandatory to Hybrid via this HTTPS call itself, since Mandatory
        # requires a client cert on every connection, including the one
        # that would ask it to relax. (Confirmed against a live cluster:
        # attempting Mandatory -> enable over HTTPS with no cert just hangs
        # until the REST client's own retry budget gives up.)
        self._enable_client_cert_auth(state="enable")
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], auth=(admin_user, admin_password)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Hybrid mode: password-only auth (no cert) should succeed, got "
            f"{resp.status_code}: {resp.text}"
        )
        self.log.info("Hybrid + no cert + password -> succeeded")

        # ── Hybrid: revoked cert + valid password together -> rejected, no
        # fallback to password auth.
        self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"],
                    cert=(admin_cert_path, admin_key_path),
                    auth=(admin_user, admin_password),
                ),
            "Hybrid mode: a revoked cert must be rejected even with a "
                "valid password available, not silently fall back to "
                "password auth",
        )
        self.log.info("Hybrid + revoked cert + password -> rejected, no fallback")

        # ── Mandatory last: prove revocation is evaluated BEFORE identity
        # extraction and RBAC.
        #
        # A fully-authorised certificate cannot show this. Whether revocation
        # runs first or RBAC runs first, an authorised-but-revoked cert is
        # refused either way, so both orderings look identical from outside.
        # The ordering only becomes observable with an UNDER-PRIVILEGED
        # certificate, where the two paths give different answers:
        #
        #   revocation first -> refused at TLS (or 401), RBAC never consulted
        #   RBAC first       -> 403, because the user may not read this endpoint
        #
        # So the unrevoked leg must first establish that this identity really
        # does get a 403 here; without that baseline a refusal afterwards
        # proves nothing about order.
        # data_backup is bucket-scoped, so it needs the [*] qualifier -- an
        # unqualified name is rejected at user creation. Verified against
        # 8.5.0-1009: data_backup[*] gets 403 on /api/v1/plan (which wants
        # ro_admin), which is exactly the under-privileged-but-valid identity
        # this check needs.
        low_priv_user, _ = self._create_rbac_test_user(
            "crl_bkp_order_lowpriv", "data_backup[*]"
        )
        low_cert, low_key, low_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, low_priv_user
        )
        low_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(low_cert))
        low_key_path = self._write_temp_pem(self.crl_utils.key_to_pem(low_key))

        self._enable_client_cert_auth(state="mandatory")

        # Baseline: not revoked, and RBAC refuses it -> 403.
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            cert=(low_cert_path, low_key_path), expected_status=403,
        )
        self.assertEqual(
            resp.status_code, 403,
            f"Baseline for the ordering check: an unrevoked certificate whose "
            f"user lacks rights on this endpoint must reach RBAC and be "
            f"denied with 403. Without that, a refusal after revocation "
            f"would not distinguish revocation-first from RBAC-first. Got "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        self.log.info(
            "Ordering baseline: under-privileged cert reaches RBAC -> 403"
        )

        # Now revoke that same identity. If revocation is evaluated first the
        # answer changes to a TLS alert or 401; if RBAC still ran first it
        # would stay 403.
        self._disable_client_cert_auth()
        # BOTH serials, not just the new one. A CRL with a higher crlNumber
        # from the same issuer supersedes the earlier one wholesale rather
        # than adding to it, so listing only low_serial here would un-revoke
        # admin_serial and the final leg below would get a 200.
        low_filename = "bkp_rest_p0_03_order.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key,
            [admin_serial, low_serial], low_filename, crl_number=2,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(low_filename)
        self._enable_client_cert_auth(state="mandatory")

        refused = self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"],
                    cert=(low_cert_path, low_key_path),
                ),
            "Revocation must be evaluated before identity extraction and "
                "RBAC. This certificate returned 403 while it was merely "
                "under-privileged; once revoked it must be refused at the "
                "TLS layer (or 401) instead. A 403 here would mean RBAC ran "
                "first and revocation second",
        )
        if refused is not None:
            self.assertNotEqual(
                refused.status_code, 403,
                "A revoked certificate must not come back as an RBAC denial "
                "-- 403 means identity was extracted and evaluated before "
                "revocation was consulted"
            )
        self.log.info(
            "Revoked under-privileged cert refused before RBAC (no 403), as "
            "required"
        )

        # And the original fully-authorised revoked cert is still refused.
        self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"],
                    cert=(admin_cert_path, admin_key_path),
                ),
            "A revoked cert mapped to backup_admin must also be rejected at "
                "the TLS layer",
        )
        self.log.info(
            "Revoked cert mapped to backup_admin rejected, as required"
        )

    def test_backup_service_crl_hot_reload(self):
        """
        P0-04: replacing the CRL restores access on the next connection
        with no cbbs/cluster restart; deleting the only applicable CRL
        rejects under Require and is allowed (with a warning) under
        Permissive.

        Uses Hybrid ('enable'), not Mandatory, clientCertAuth throughout:
        this test uploads/deletes/replaces CRL files via self.rest between
        each cert check, and self.rest carries no client cert -- under
        Mandatory that call is walled out exactly like a revoked cert would
        be, with no way back short of the plain-HTTP escape hatch. Hybrid
        still fully enforces revocation (a revoked cert gets no password
        fallback, per P0-06), so it proves the same hot-reload behavior
        without that self-inflicted lockout.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_reload_admin", "backup_admin"
        )
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        revoking_filename = "bkp_rest_p0_04_revoking.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, revoking_filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(revoking_filename)

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
                ),
            "Revoked cert should be rejected before the CRL is replaced",
        )
        self.log.info("Cert rejected as revoked, before CRL replacement")

        # "Replace" the CRL: delete the revoking file, upload a fresh one
        # (from the same CA) that revokes nothing.
        status, content = self.crl_utils.delete_file(self.rest, revoking_filename)
        self.assertTrue(status, f"CRL delete (for replace) failed: {content}")
        self._created_files.remove(revoking_filename)

        replacement_filename = "bkp_rest_p0_04_replacement.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], replacement_filename,
            crl_number=2,
        )
        self.assertTrue(status, f"CRL replace failed: {content}")
        self._track_uploaded_file(replacement_filename)

        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Access should be restored immediately after the CRL is "
            f"replaced, with no cbbs restart, got {resp.status_code}: "
            f"{resp.text}"
        )
        self.log.info(
            "Access restored on first connection after CRL replace, no restart"
        )

        # Delete the only applicable CRL under Require -> fails closed.
        status, content = self.crl_utils.delete_file(
            self.rest, replacement_filename
        )
        self.assertTrue(status, f"CRL delete failed: {content}")
        self._created_files.remove(replacement_filename)

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
                ),
            "Deleting the only applicable CRL under Require must reject "
                "the connection (fail closed), not silently allow it",
        )
        self.log.info(
            "Deleting the only applicable CRL under Require rejects, as expected"
        )

        # Loosen to Permissive -> missing CRL is allowed (with a warning).
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Permissive", "nodeToNode": "Disabled"},
        )
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Missing CRL under Permissive should be allowed (with a "
            f"warning), got {resp.status_code}: {resp.text}"
        )
        self.log.info("Missing CRL under Permissive allowed, as expected")

    def test_cbbackupmgr_revoked_cert_rejected_and_restore_rejects_before_mutation(self):
        """
        P0-08 / P0-12 (restore half) / P0-09 (observable proxy only):
        cbbackupmgr backup with a revoked client cert fails cleanly, adds no
        new backup to the repository, and restore with a revoked cert
        rejects before touching the target bucket. P0-09 ("cbbackupmgr
        delegates peer verification to cbbs's new gRPC method") is proven
        here only by the observable outcome (rejected) -- a black-box test
        can't trace which internal code path performed the check.

        Only backup/restore are exercised: cbbackupmgr merge operates
        purely on a local archive with no --cluster flag at all, so it has
        no TLS/peer-cert surface to test here.
        """
        user, _ = self._create_rbac_test_user("crl_bkp_mgr_admin", "admin")
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        # cbbackupmgr runs on self.backup_node over SSH, not on this test
        # process's own machine -- it needs its own copy of the cert/key.
        remote_cert_path = self._copy_pem_to_backup_node(cert_path)
        remote_key_path = self._copy_pem_to_backup_node(key_path)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        archive = self._new_archive_dir("cbbackupmgr")
        repo = "crl_repo"
        cluster_host = f"https://{self.cluster.master.ip}:{MGMT_PORT}"

        stdout, stderr = self.backup_mgr.configure_backup(archive, repo)
        self.assertFalse(
            stderr, f"cbbackupmgr config (repo create) failed: {stderr}"
        )

        # Valid, unrevoked cert -> backup succeeds.
        output, error = self.backup_mgr.backup(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert_path, client_key=remote_key_path,
            no_progress_bar=True,
        )
        self.assertFalse(
            error, f"cbbackupmgr backup with a valid cert should succeed: {error}"
        )
        self.log.info("cbbackupmgr backup with a valid cert succeeded")

        backups_before, _ = self.backup_mgr.list_backups(archive, repo)
        count_before = len([line for line in (backups_before or []) if line.strip()])

        # Revoke this same cert's serial, then retry -- must fail cleanly and
        # add no new backup. self.rest carries no client cert, so it has to
        # drop out of Mandatory first (plain-HTTP escape hatch) or this
        # upload call is walled out exactly like a revoked cert would be --
        # confirmed the hard way against a live cluster.
        self._disable_client_cert_auth()
        filename = "cbbackupmgr_p0_08_09_12.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # bucket_util's REST calls carry no client cert either -- get the
        # pre-restore baseline now, while Mandatory is still relaxed, rather
        # than after re-enabling it below.
        bucket = self.cluster.buckets[0]
        items_before = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket.name
        )

        self._enable_client_cert_auth(state="mandatory")

        output, error = self.backup_mgr.backup(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert_path, client_key=remote_key_path,
            no_progress_bar=True,
        )
        self.assertTrue(
            error or not any("Backup successful" in line for line in (output or [])),
            f"cbbackupmgr backup with a revoked cert should fail cleanly, "
            f"got output={output}, error={error}"
        )
        self.log.info(f"cbbackupmgr backup with a revoked cert failed as expected: "
                      f"output={output}, error={error}")

        backups_after, _ = self.backup_mgr.list_backups(archive, repo)
        count_after = len([line for line in (backups_after or []) if line.strip()])
        self.assertEqual(
            count_before, count_after,
            f"A backup attempt with a revoked cert must add no new backup "
            f"to the repository: before={backups_before}, after={backups_after}"
        )

        # Restore with the same revoked cert must reject before any mutation.
        output, error = self.backup_mgr.restore(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert_path, client_key=remote_key_path,
            no_progress_bar=True,
        )
        self.assertTrue(
            error or not any("Restore successful" in line for line in (output or [])),
            f"cbbackupmgr restore with a revoked cert should fail cleanly, "
            f"got output={output}, error={error}"
        )

        # bucket_util's REST calls carry no client cert -- drop Mandatory
        # again before checking the post-restore count.
        self._disable_client_cert_auth()
        items_after = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket.name
        )
        self.assertEqual(
            items_before, items_after,
            "A restore attempt with a revoked cert must not mutate the "
            "target bucket"
        )
        self.log.info(
            "cbbackupmgr restore with a revoked cert rejected before any mutation"
        )

    def test_backup_validator_unreachable_fails_closed(self):
        """
        P0-10: when the Backup Service node cannot reach ns_server's
        /_cbauth/crlsValidate endpoint, the connection must fail closed
        (rejected) rather than being let through because nobody answered.

        Only the crlsValidate REQUESTS are blocked, on the LOOPBACK interface
        of the backup node. Three properties of the real system force that
        shape, each verified against a live cluster:

        1. cbauth posts crlsValidate to the ns_server on the SAME machine as
           the calling service. backup_service.log dispatches every ns_server
           request to localhost:8091 / 127.0.0.1:8091 and never to another
           node's address, so a cross-node partition -- what an earlier
           revision of this test used -- leaves the validator perfectly
           reachable and proves nothing. This test therefore needs no
           particular cluster topology; one node is enough.
        2. That traffic is plain HTTP, so an iptables string match on the
           request path can single it out. Blocking the whole port instead
           takes out metakv, pools and ordinary auth as well, which stalls
           unrelated requests and drives cbbs into a log-flooding MetaKV
           restart loop -- collateral that makes a failure impossible to
           attribute to revocation.
        3. cbauth CACHES verdicts per certificate. Probing with a
           certificate that has already been validated sends no request at
           all (measured: zero packets matched, response in 1.1s), so the
           blocked probe must present a certificate this cluster has never
           seen. Reusing the baseline's certificate would make the test pass
           while never consulting the validator.

        A timeout counts as failing closed -- see the assertion below.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_validator_admin", "backup_admin"
        )
        cert, key, _serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        # A SECOND certificate, never presented to this cluster, reserved for
        # the blocked probe. Both map to the same RBAC user and the same CA,
        # so they are interchangeable to every layer except cbauth's verdict
        # cache -- which is the layer that matters here (see docstring point
        # 3). Minted from self.ca_cert/self.ca_key directly, since the CA key
        # is what signing a sibling leaf requires.
        probe_cert, probe_key, _probe_serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        probe_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(probe_cert)
        )
        probe_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(probe_key)
        )

        # A CRL for the issuing CA has to exist before a valid cert can
        # connect at all: under policy 'Require' an unavailable CRL fails
        # closed at the TLS layer, so without this the baseline below is
        # rejected with a bad certificate alert and the test never reaches
        # the partition it exists to exercise. That fail-closed behaviour is
        # correct and deliberately asserted by
        # test_backup_service_crl_hot_reload -- this CRL revokes nothing, so
        # it satisfies Require while leaving this test's cert valid.
        #
        # This MUST precede _enable_client_cert_auth('mandatory') below:
        # self.rest carries no client cert, so once mandatory is in effect
        # this upload is walled out with a 'certificate required' alert, the
        # same way every other HTTPS call in this class is (see tearDown).
        filename = "bkp_rest_p0_10.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        # Absorbs the propagation delay between the CA-trust/CRL/
        # clientCertAuth changes above landing on self.cluster.master and
        # cbbs (not necessarily co-located) picking them up -- confirmed
        # against a live cluster that an immediate check here can transiently
        # 5xx/reject a genuinely valid cert for a few seconds.
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Baseline valid-cert connection should succeed before the "
            f"validator path is blocked, got {resp.status_code}: {resp.text}"
        )

        shell = RemoteMachineShellConnection(self.backup_node)
        # Drop only packets carrying the crlsValidate request path, on
        # loopback. Everything else cbbs sends to its local ns_server keeps
        # flowing, so the only thing made unreachable is the revocation
        # validator -- which is what P0-10 is about. Verified on a live
        # cluster: this rule matched 8 packets and stalled the handshake,
        # while metakv/pools/auth traffic was unaffected and cbbs stayed out
        # of its log-flooding restart loop.
        block_rules = [self.CRLS_VALIDATE_DROP_RULE]
        # Self-healing safety net: a stray DROP rule would silently break
        # revocation for every later test on this node, so schedule an
        # unconditional removal on the node itself, detached from this SSH
        # session, in case this test dies before its finally-block runs (SSH
        # drop, killed process). The finally-block is still the normal path;
        # these -D calls just fail harmlessly once the rules are gone.
        undo = "; ".join(f"iptables -D {rule} 2>/dev/null" for rule in block_rules)
        shell.execute_command(
            f"nohup setsid bash -c 'sleep 300; {undo}' >/dev/null 2>&1 &"
        )
        try:
            for rule in block_rules:
                shell.execute_command(f"iptables -I {rule}")
            self.log.info(
                f"Blocked crlsValidate requests on {self.backup_node.ip} "
                f"loopback -- cbauth's path to its local ns_server revocation "
                f"validator, and nothing else"
            )

            # Timeout counts as failing closed. Measured against a live
            # cluster: with cbauth's path to ns_server cut, cbbs neither
            # rejects nor answers -- it stalls the request, and the client
            # surfaces requests.exceptions.ReadTimeout rather than an SSL
            # alert. The security property this test exists to protect is
            # "the connection is not let through", and a stall satisfies it,
            # so a timeout must not be read as a test failure.
            #
            # It is, however, weaker than the test plan asks for: P0-10 wants
            # 'status undetermined' to be distinguishable from 'revoked', and
            # a stall conveys no status at all. That gap is an error-quality
            # observation about cbbs, tracked separately -- not something
            # this assertion should conflate with a fail-open.
            #
            # This is the one place that does NOT use assert_cert_refused():
            # that helper accepts a TLS alert or a 401, which is the right
            # contract for a revoked certificate, but it cannot express "the
            # server never answered at all". A stall is the actual observed
            # behaviour here, so the exception tuple stays explicit.
            with self.assertRaises(
                (requests.exceptions.SSLError,
                 requests.exceptions.ConnectionError,
                 requests.exceptions.Timeout),
                msg="With the revocation validator unreachable, the "
                    "connection must fail closed -- rejected or stalled, but "
                    "never served"
            ):
                # The FRESH certificate, so cbauth has no cached verdict and
                # must actually consult the (now unreachable) validator. Using
                # cert_path here would be served from cache and prove nothing.
                self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"],
                    cert=(probe_cert_path, probe_key_path),
                    timeout=15,
                )
            self.log.info("Connection failed closed while validator was unreachable")
        finally:
            for rule in block_rules:
                shell.execute_command(f"iptables -D {rule}")
            # Prove the rules are actually gone rather than trusting the
            # delete: a lingering DROP would break revocation for every later
            # test on this node in ways that look nothing like their own bug.
            # (stdout, stderr) in that order -- reading the second element
            # would silently check stderr and never detect a leftover rule.
            leftover, _ = shell.execute_command(
                "iptables -S OUTPUT | grep -c crlsValidate || true"
            )
            # grep -c always prints a number, so test the value rather than
            # the presence of output -- "0" is a non-empty line.
            remaining = next(
                (int(line.strip()) for line in (leftover or [])
                 if line.strip().isdigit()),
                0,
            )
            if remaining:
                self.log.error(
                    f"{remaining} crlsValidate DROP rule(s) still present "
                    f"after cleanup on {self.backup_node.ip} -- revocation is "
                    f"broken for subsequent tests on this node"
                )
            shell.disconnect()

        # Retry with the PROBE certificate -- the one just denied -- not the
        # baseline one. The baseline cert has a cached verdict and would be
        # served without the validator being consulted at all, so it would
        # report "access restored" even if the validator were still down. The
        # probe cert has no usable verdict, so this genuinely re-exercises the
        # path that was broken.
        #
        # Polled rather than checked once: cbauth may briefly hold the
        # validator failure, and the local ns_server connection has to be
        # re-established before a handshake can be judged again.
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            cert=(probe_cert_path, probe_key_path),
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Access should be restored once the validator path is "
            f"reachable again, got {resp.status_code}: {resp.text}"
        )
        self.log.info("Access restored after unblocking the validator path")

    def test_revocation_failures_are_distinguishable_and_leak_no_key_material(self):
        """
        Test plan section L: backup-path logs must distinguish revoked from
        the other ways a revocation check can fail, and must not leak raw
        certificate material.

        Two properties, both asserted against backup_service.log:

        1. A rejection caused by revocation is identifiable as such -- an
           operator reading the log can tell "this certificate is revoked"
           apart from "the CRL is missing" or "the chain is untrusted".
           Without that, every failure looks like a generic TLS error.
        2. No raw PEM blocks and no unhashed certificate serials appear.
           The PRD lists this explicitly ("Avoid logging raw serials and
           PEMs; hash serials in audit/logs") because CRL material carries
           operational metadata about an organisation's PKI.

        The serial is searched for in both decimal and hex, upper and lower
        case: a serial is a big integer, and which representation a Go
        service prints is not something this test should assume.
        """
        revoked_user, _ = self._create_rbac_test_user(
            "crl_bkp_logs_revoked", "backup_admin"
        )
        revoked_cert, revoked_key, revoked_serial = \
            self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, revoked_user
            )
        revoked_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(revoked_cert)
        )
        revoked_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(revoked_key)
        )

        filename = "bkp_logs_section_l.pem"
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
        self._enable_client_cert_auth(state="enable")

        # Something unique to scan forward from, so the assertions cover only
        # this test's traffic rather than anything the node logged earlier.
        marker_user, _ = self._create_rbac_test_user(
            "crl_bkp_logs_marker", "backup_admin"
        )
        marker_cert, marker_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, marker_user
        )
        marker_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(marker_cert)
        )
        marker_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(marker_key)
        )
        self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            cert=(marker_cert_path, marker_key_path),
        )

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                "GET", ENDPOINT_GROUPS["plan"],
                cert=(revoked_cert_path, revoked_key_path),
            ),
            "A revoked certificate must be refused before the log is examined",
        )

        lines = self._read_backup_service_log(since_marker=marker_user)
        self.assertTrue(
            lines,
            "Could not read backup_service.log on "
            f"{self.backup_node.ip} -- section L cannot be evaluated"
        )
        blob = "\n".join(lines)

        # ── 1. the rejection is attributable to revocation ─────────────────
        revocation_evidence = [
            line for line in lines
            if "revoked" in line.lower() or "crlsvalidate" in line.lower()
        ]
        self.assertTrue(
            revocation_evidence,
            "A connection rejected because the client certificate is revoked "
            "must be identifiable as such in backup_service.log. Nothing "
            "mentioning revocation was logged, so an operator cannot tell "
            "this apart from any other TLS failure. Lines examined: "
            f"{len(lines)}"
        )
        self.log.info(
            f"Revocation rejection is attributable in the log: "
            f"{revocation_evidence[-1][:160]}"
        )

        # ── 2. no raw key material or unhashed serials ─────────────────────
        self.assertNotIn(
            "-----BEGIN", blob,
            "backup_service.log contains a PEM block -- raw certificate or "
            "CRL material must never be written to the log"
        )
        serial_forms = {
            str(revoked_serial),
            format(revoked_serial, "x"),
            format(revoked_serial, "X"),
        }
        leaked = [form for form in serial_forms if form in blob]
        self.assertFalse(
            leaked,
            f"The revoked certificate's serial appears unhashed in "
            f"backup_service.log in form(s) {leaked}. The PRD requires "
            f"serials to be hashed in logs and audit events, since CRL "
            f"material exposes an organisation's PKI metadata."
        )
        self.log.info(
            "No PEM blocks and no unhashed serials in backup_service.log"
        )

    def test_peer_only_vs_full_chain_revocation_checking(self):
        """
        checkIntermediateCerts governs whether revocation is evaluated for
        the whole certificate chain or only for the peer certificate itself
        (PRD P0 goal 11, "configuration to allow verifying the peer or the
        chain using CRLs"). Both halves of that switch must hold on the
        Backup Service listener:

          false (default) -- a revoked INTERMEDIATE does not block a leaf
                             whose own serial is not revoked; only the peer
                             is consulted.
          true            -- the same leaf is rejected, because the
                             intermediate that issued it is revoked.

        The leaf's own serial is never revoked, so any rejection here can
        only come from walking up the chain -- which is exactly the
        behaviour under test. Two CRLs are needed because policy Require
        demands an applicable CRL for every certificate actually consulted:
        one issued by the intermediate (covering the leaf, revoking
        nothing), and one issued by the root (covering the intermediate,
        revoking it).

        Runs in hybrid mTLS, not mandatory: the checkIntermediateCerts
        toggle between the two halves goes through self.rest, which carries
        no client certificate and would be walled out under mandatory.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_chain_admin", "backup_admin"
        )
        inter_cert, inter_key, inter_serial = \
            self.crl_utils.generate_intermediate_ca(
                self.ca_cert, self.ca_key, "CRLBackupIntermediateCA"
            )
        # The intermediate must be trusted in its own right, not merely chain to
        # a trusted root: uploading a CRL requires its ISSUER to be trusted, and
        # ns_server otherwise rejects the intermediate-issued CRL below with
        # "CRL validation failed: ... CRL issuer not trusted".
        self._trust_ca_on_cluster(inter_cert)
        leaf_cert, leaf_key, _leaf_serial = self.crl_utils.generate_leaf_cert(
            inter_cert, inter_key, user
        )
        # The server has to build leaf -> intermediate -> trusted root, and
        # only the root is in its trust store, so the client must present the
        # intermediate alongside the leaf.
        chain_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(leaf_cert)
            + self.crl_utils.cert_to_pem(inter_cert)
        )
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))

        leaf_crl = "bkp_chain_leaf_issuer.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, inter_cert, inter_key, [], leaf_crl, crl_number=1,
        )
        self.assertTrue(status, f"Intermediate-issued CRL upload failed: {content}")
        self._track_uploaded_file(leaf_crl)

        root_crl = "bkp_chain_root_issuer.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, inter_serial, root_crl,
            crl_number=1,
        )
        self.assertTrue(status, f"Root-issued CRL upload failed: {content}")
        self._track_uploaded_file(root_crl)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
            checkIntermediateCerts=False,
        )
        self._enable_client_cert_auth(state="enable")

        # ── peer-only: the revoked intermediate is not consulted ───────────
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(chain_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"With checkIntermediateCerts=false only the peer certificate's "
            f"revocation status is consulted, so a leaf whose own serial is "
            f"not revoked must connect even though its issuing intermediate "
            f"is revoked. Got {resp.status_code}: {resp.text}"
        )
        self.log.info(
            "Peer-only checking: leaf connects despite a revoked intermediate"
        )

        # ── full chain: the revoked intermediate now invalidates the leaf ──
        self.crl_utils.set_settings(self.rest, checkIntermediateCerts=True)
        self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"], cert=(chain_path, key_path)
                ),
            "With checkIntermediateCerts=true the revoked intermediate "
                "must invalidate every certificate issued beneath it, "
                "including this leaf",
        )
        self.log.info(
            "Full-chain checking: revoked intermediate rejects the leaf, "
            "as required"
        )

    def test_backup_crl_policy_disabled_is_noop_baseline(self):
        """
        P0-13: policy 'Disabled' is a true no-op on cbbs's own TLS listener
        -- a certificate that IS present on an uploaded CRL still connects,
        because the policy governing whether that CRL is even consulted is
        off. This is also the upgrade-safety baseline: CRL is Disabled by
        default, so existing backup-service access must be unaffected.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_disabled_admin", "backup_admin"
        )
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        filename = "bkp_rest_p0_13.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"With policy Disabled, a cert present on an uploaded CRL "
            f"should still connect -- Disabled must be a true no-op, got "
            f"{resp.status_code}: {resp.text}"
        )
        self.log.info("Revoked-on-paper cert connects normally under Disabled policy")

    def test_object_store_tls_unaffected_by_crl_policy(self):
        """
        P0-14: external object store TLS is unaffected by cluster CRL policy,
        and generates no crlsValidate calls.

        The object store's certificate chains to its own private CA, which is
        deliberately unrelated to the cluster's trust anchors -- that
        independence is the property under test. Verified against the fixture
        that passing the cluster's own CA as --obj-cacert fails with
        "x509: certificate signed by unknown authority", so the two trust
        domains really are separate rather than incidentally compatible.

        Three things have to hold together, and the first is what stops this
        test passing vacuously:
          1. CRL enforcement is genuinely live -- proven by a revoked cert
             being rejected on the Backup Service REST API in this same setup.
          2. A backup to the object store still succeeds while it is live.
          3. The object-store leg issues zero crlsValidate calls.

        For (3) the count is compared DIFFERENTIALLY against the same backup
        taken to a local archive, not against zero. The crlsValidate counter
        is node-wide: it sees every revocation check cbauth performs on that
        machine, including cbbs's own internal TLS activity, which this test
        provokes by changing clientCertAuth and the CRL policy moments
        earlier. Measured against a live cluster, an absolute
        "count must not change" assertion picked up exactly one such
        unrelated call and failed, while the object-store path itself
        produced a delta of zero across repeated config and backup runs.
        Comparing local-archive and object-store deltas cancels that shared
        background out and isolates the one thing P0-14 actually claims:
        that routing the archive through an object store adds no revocation
        checks.

        The cluster leg is a plain couchbase:// connection with password
        authentication -- an https:// leg would need --cacert or
        --no-ssl-verify for the cluster's self-signed node certificate and
        would fail this test for a reason unrelated to revocation. The
        cluster-wide policy is Require throughout, and step 1 proves it is
        being enforced.
        """
        backup_mgr, archive, _staging = self._object_store_backup_mgr()

        # A revoked cert, so step 1 can prove enforcement is actually on.
        revoked_user, _ = self._create_rbac_test_user(
            "crl_bkp_objstore_revoked", "backup_admin"
        )
        revoked_cert, revoked_key, revoked_serial = \
            self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, revoked_user
            )
        revoked_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(revoked_cert)
        )
        revoked_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(revoked_key)
        )

        filename = "bkp_rest_p0_14.pem"
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
        # Hybrid, not mandatory: cbbackupmgr below connects with a password and
        # no client certificate, which mandatory would refuse outright for
        # reasons that have nothing to do with revocation.
        self._enable_client_cert_auth(state="enable")

        # ── 1. Prove CRL enforcement is live, not silently off ──────────────
        self.assert_cert_refused(
            lambda: self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"],
                    cert=(revoked_cert_path, revoked_key_path),
                ),
            "Revoked cert should be rejected -- without this the zero "
                "crlsValidate count below would prove nothing, since a "
                "disabled policy also produces zero calls",
        )
        self.log.info("CRL enforcement confirmed live under policy Require")

        cluster_host = f"couchbase://{self.cluster.master.ip}"

        # ── 2a. Reference leg: the same backup to a LOCAL archive ───────────
        # Establishes how many revocation calls this backup costs when no
        # object store is involved, under identical cluster configuration.
        local_archive = self._new_archive_dir("p0_14_local")
        local_mgr = self._local_backup_mgr()
        _, stderr = local_mgr.configure_backup(local_archive, "crl_local")
        self.assertFalse(stderr, f"Local archive setup failed: {stderr}")

        baseline = self._crls_validate_counter_start()
        self.assertIsNotNone(
            baseline,
            "Could not read the crlsValidate counter -- the iptables string "
            "module is required on the backup node for this assertion"
        )
        output, error = local_mgr.backup(
            local_archive, "crl_local", cluster_host=cluster_host,
            no_progress_bar=True,
        )
        self.assertTrue(
            any("Backup completed successfully" in line
                for line in (output or [])),
            f"Reference local-archive backup should succeed: "
            f"output={output}, error={error}"
        )
        local_delta = self._crls_validate_count() - baseline
        self.log.info(f"crlsValidate calls for the LOCAL-archive backup: "
                      f"{local_delta}")

        # ── 2b. Object-store leg, measured the same way ────────────────────
        _, stderr = backup_mgr.configure_backup(archive, "crl_objstore")
        self.assertFalse(
            stderr,
            f"Creating the object-store archive should succeed under CRL "
            f"policy Require: {stderr}"
        )

        before_obj = self._crls_validate_count()
        output, error = backup_mgr.backup(
            archive, "crl_objstore", cluster_host=cluster_host,
            no_progress_bar=True,
        )
        self.assertTrue(
            any("Backup completed successfully" in line
                for line in (output or [])),
            f"Backup to the object store must succeed while cluster CRL "
            f"policy is Require -- object-store TLS uses an unrelated CA and "
            f"must not be subject to cluster revocation policy. "
            f"output={output}, error={error}"
        )
        self.log.info("Backup to the object store succeeded under CRL Require")
        obj_delta = self._crls_validate_count() - before_obj

        # ── 3. The object store adds no revocation calls of its own ────────
        self.assertLessEqual(
            obj_delta, local_delta,
            f"Routing the archive through an object store must not add "
            f"revocation checks: the local-archive backup cost "
            f"{local_delta} crlsValidate call(s) while the object-store "
            f"backup cost {obj_delta}. The extra call(s) can only have come "
            f"from the object-store connection, whose certificate chains to "
            f"a CA unrelated to the cluster's trust anchors."
        )
        self.log.info(
            f"crlsValidate calls -- local: {local_delta}, object-store: "
            f"{obj_delta} -- object-store TLS adds no revocation checks, "
            f"as P0-14 requires"
        )
