import datetime
import threading
import time

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
    "cluster": "/api/v1/cluster/self",
    "plan": "/api/v1/plan",
    "repository": "/api/v1/cluster/self/repository/active",
    "repository_archived": "/api/v1/cluster/self/repository/archived",
    "config": "/api/v1/config",
}

# The plan's section A lists "cluster, plan, repository, task, instance, and
# import/export" as the endpoint groups to cover. Probed against 8.5.0-1009,
# the five above are the GET-able groups; /api/v1/task, /api/v1/instance,
# /api/v1/export and their plural forms all 404, and import lives at
# /api/v1/cluster/self/repository/import as a POST (GET returns 400). Task
# history is per-repository rather than a top-level group. Revocation is
# enforced at the TLS handshake, before any handler runs, so these five
# spanning distinct handler groups is what makes the point -- adding paths
# that 404 would prove nothing about the listener.

# Plain-HTTP Backup Service port. No TLS, therefore no client certificate,
# therefore nothing for revocation to evaluate.
BACKUP_SERVICE_HTTP_PORT = 8097


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

        # P0-13 requires not just that the connection succeeds, but that no
        # crlsValidate round trip happens at all -- "Disabled" must short
        # circuit inside cbauth rather than ask ns_server and ignore the
        # answer. Counting has to start before the connection is made.
        baseline = self._crls_validate_counter_start()

        # Polled, not immediate: the clientCertAuth change above propagates
        # asynchronously. Extra polls cost nothing here -- under Disabled
        # none of them should produce a crlsValidate packet, which is
        # exactly what the count below asserts.
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

        calls = self._crls_validate_count()
        self.assertIsNotNone(
            calls,
            "The crlsValidate counting rule went missing mid-test, so the "
            "zero-round-trip half of P0-13 could not be measured"
        )
        self.assertEqual(
            calls, baseline,
            f"Policy Disabled must generate zero crlsValidate requests, but "
            f"the packet count moved from {baseline} to {calls}"
        )
        self.log.info(
            f"Policy Disabled made no crlsValidate calls (count stayed at "
            f"{baseline})"
        )

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

    # ── Scenario-coverage batch: sections A, D and F ────────────────────────

    def test_no_ssl_verify_does_not_bypass_client_cert_revocation(self):
        """
        Section D: --no-ssl-verify must not become a revocation bypass.

        The flag sets InsecureSkipVerify on cbbackupmgr's side, which leaves
        verifiedChains nil. The plan asks to confirm that is not "an
        unintended revocation bypass". This test covers the half a black-box
        test can settle: enforcement of the CLIENT certificate is server-side
        (cbbs/ns_server), so skipping the client's own verification of the
        SERVER certificate must not weaken it -- a revoked client certificate
        stays rejected either way.

        The other half -- whether revocation of the SERVER's certificate is
        skipped when verifiedChains is nil -- needs a revoked node certificate
        and the nodeToNode scope (plan section C), and is not covered here.

        --no-ssl-verify is passed explicitly rather than inherited:
        CbBackupMgr derives it from CbServer.use_https, which is False in
        these runs, so without forcing it the flag would be absent and the
        test would prove nothing.
        """
        user, _ = self._create_rbac_test_user("crl_bkp_nosslverify", "admin")
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        remote_cert = self._copy_pem_to_backup_node(cert_path)
        remote_key = self._copy_pem_to_backup_node(key_path)

        # Force the flag ON for this manager, whatever the cluster's TLS
        # setting happens to be.
        insecure_mgr = self._insecure_backup_mgr()
        self.assertIn(
            "--no-ssl-verify", insecure_mgr.cli_flags,
            "The flag under test is not actually being passed, so this test "
            "would prove nothing"
        )

        archive = self._new_archive_dir("nosslverify")
        repo = "crl_nosslverify"
        cluster_host = f"https://{self.cluster.master.ip}:{MGMT_PORT}"

        _, stderr = insecure_mgr.configure_backup(archive, repo)
        self.assertFalse(stderr, f"Repo creation failed: {stderr}")

        # Baseline: the cert works while unrevoked, with the flag on.
        filename = "bkp_nosslverify_allow.pem"
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

        output, error = insecure_mgr.backup(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert, client_key=remote_key,
            no_progress_bar=True,
        )
        self.assertFalse(
            error,
            f"Baseline: an unrevoked cert should back up even with "
            f"--no-ssl-verify: {error}"
        )
        self.log.info("Baseline backup succeeded with --no-ssl-verify")

        # Revoke it. self.rest carries no client cert, so drop out of
        # mandatory first -- the plain-HTTP escape hatch.
        self._disable_client_cert_auth()
        revoking = "bkp_nosslverify_revoke.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, revoking,
            crl_number=2,
        )
        self.assertTrue(status, f"Revoking CRL upload failed: {content}")
        self._track_uploaded_file(revoking)
        self._enable_client_cert_auth(state="mandatory")

        output, error = insecure_mgr.backup(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert, client_key=remote_key,
            no_progress_bar=True,
        )
        self.assertTrue(
            error or not any("Backup successful" in line
                             for line in (output or [])),
            f"--no-ssl-verify must NOT bypass revocation of the client "
            f"certificate: the backup succeeded with a revoked cert. "
            f"output={output}, error={error}"
        )
        self.log.info(
            "Revoked cert still rejected with --no-ssl-verify -- the flag is "
            "not a revocation bypass for client certificates"
        )

    def test_der_encoded_crl_is_enforced_like_pem(self):
        """
        Section F: both PEM and DER encodings are handled on every backup
        path. The PRD lists "CRL parsing for PEM and DER" as P0, and every
        other test in this suite uploads PEM, so DER is otherwise untested.

        The same CRL content is uploaded in DER form only; if it were ignored
        or mis-parsed, the revoked certificate below would still connect.
        """
        user, _ = self._create_rbac_test_user("crl_bkp_der", "backup_admin")
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        pem = self.crl_utils.build_crl(
            self.ca_cert, self.ca_key, revoked_serials=[serial], crl_number=1
        )
        der = self.crl_utils.pem_crl_to_der(pem)
        self.assertNotEqual(
            pem, der, "DER conversion produced the PEM bytes unchanged"
        )

        filename = "bkp_crl_der.pem"
        status, content = self.crl_utils.upload_file(self.rest, filename, der)
        self.assertTrue(
            status, f"A DER-encoded CRL should be accepted: {content}"
        )
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
            ),
            "A certificate revoked by a DER-encoded CRL must be rejected "
            "exactly as it would be by a PEM one -- otherwise DER CRLs are "
            "silently not applied",
        )
        self.log.info("DER-encoded CRL enforced on the Backup Service path")

    def test_untrusted_and_tampered_crls_are_not_applied(self):
        """
        Section F: a CRL signed by an unknown CA, and a CRL whose signature
        has been altered, must not take effect on backup connections.

        Both are checked by their observable outcome rather than only by the
        upload's status code: even if a bad CRL were accepted at upload time,
        the certificate it names must still connect, because the CRL cannot
        be attributed to a trusted issuer.
        """
        user, _ = self._create_rbac_test_user("crl_bkp_badcrl", "backup_admin")
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        # A CRL from a CA the cluster has never trusted, naming our serial.
        rogue_cert, rogue_key = self.crl_utils.generate_ca("CRLBackupRogueCA")
        rogue_crl = self.crl_utils.build_crl(
            rogue_cert, rogue_key, revoked_serials=[serial], crl_number=1
        )
        status, content = self.crl_utils.upload_file(
            self.rest, "bkp_crl_rogue.pem", rogue_crl
        )
        if status:
            # Accepted at upload time -- it must still have no effect.
            self._track_uploaded_file("bkp_crl_rogue.pem")
            self.log.info(
                "Untrusted-issuer CRL was accepted at upload; checking it has "
                "no effect on enforcement"
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
            self.rest, "bkp_crl_tampered.pem", bytes(der)
        )
        if status:
            self._track_uploaded_file("bkp_crl_tampered.pem")
            self.log.info(
                "Tampered CRL was accepted at upload; checking it has no "
                "effect on enforcement"
            )
        else:
            self.log.info(
                f"Tampered CRL rejected at upload, as expected: {content}"
            )

        # A valid CRL from the trusted CA revoking nothing, so policy Require
        # has something applicable and the check below isolates the bad CRLs.
        good_name = "bkp_crl_good.pem"
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

        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Neither an untrusted-issuer CRL nor a tampered one may revoke "
            f"this certificate -- it is named only by those two, and the only "
            f"valid CRL revokes nothing. Got {resp.status_code}: {resp.text}"
        )
        self.log.info(
            "Untrusted-issuer and tampered CRLs had no effect on enforcement"
        )

    def test_expired_cert_fails_before_revocation_is_consulted(self):
        """
        Section A: an expired (but not revoked) certificate fails chain
        validation before CRL evaluation is reached, and does so distinguishably
        from a revocation failure.

        Expiry is a property of the certificate itself, so it must be caught
        without any CRL being consulted. The log check makes the distinction
        explicit: an expired certificate must not be reported as revoked, or an
        operator chasing a revocation problem is sent the wrong way.
        """
        user, _ = self._create_rbac_test_user("crl_bkp_expired", "backup_admin")
        # An explicit window that closed five days ago. valid_days alone cannot
        # express this: generate_leaf_cert fixes notBefore at now-1d, so
        # valid_days=-1 yields notBefore == notAfter -- a zero-length window a
        # server could justifiably call malformed rather than expired, which
        # would make this test assert the wrong thing.
        now = datetime.datetime.now(datetime.timezone.utc)
        expired_cert, expired_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user,
            not_valid_before=now - datetime.timedelta(days=10),
            not_valid_after=now - datetime.timedelta(days=5),
        )
        cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(expired_cert)
        )
        key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(expired_key)
        )

        filename = "bkp_expired_cert.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="enable")

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
            ),
            "An expired certificate must be refused",
        )
        self.log.info("Expired certificate refused, as required")

        lines = self._read_backup_service_log(tail_lines=200)
        blob = "\n".join(lines).lower()
        if "expired" in blob or "certificate" in blob:
            self.assertNotIn(
                "is revoked", blob,
                f"An expired certificate must not be reported as revoked -- "
                f"its serial is on no CRL. Recent log: {lines[-3:]}"
            )
            self.log.info(
                "Expiry was not misreported as revocation in the log"
            )
        else:
            self.log.info(
                "No relevant log line found; the refusal above already shows "
                "expiry is enforced"
            )

    # ── Section O: out-of-scope confirmations ───────────────────────────────

    def test_backup_does_not_capture_crl_material_or_policy(self):
        """
        Section O: a backup must not capture CRL material or the revocation
        policy. MB-72050 was resolved Won't Do precisely because restoring a
        stale CRL would be actively harmful, so this is a guarantee the
        archive has to keep rather than a nice-to-have.

        Checked by searching the finished archive for four distinct markers:
        PEM CRL armour, the settings field name, the uploaded CRL's filename,
        and this test's own CA common name. The CN is the sharpest of the
        four -- it is unique per test run, so a hit could only have come from
        this cluster's live CRL configuration.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_nocapture", "backup_admin"
        )
        cert, key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        remote_cert = self._copy_pem_to_backup_node(
            self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        )
        remote_key = self._copy_pem_to_backup_node(
            self._write_temp_pem(self.crl_utils.key_to_pem(key))
        )

        filename = "bkp_o_no_capture.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        archive = self._new_archive_dir("no_capture")
        repo = "crl_no_capture"
        cluster_host = f"https://{self.cluster.master.ip}:{MGMT_PORT}"
        _, stderr = self.backup_mgr.configure_backup(archive, repo)
        self.assertFalse(stderr, f"cbbackupmgr repo create failed: {stderr}")

        output, error = self.backup_mgr.backup(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert, client_key=remote_key,
            no_progress_bar=True,
        )
        self.assertFalse(
            error,
            f"Backup should succeed while CRLs are configured: {error}"
        )
        self.log.info("Backup taken with CRLs configured and policy Require")

        ca_cn = self.ca_cert.subject.rfc4514_string().split("CN=")[-1].split(",")[0]
        markers = {
            "PEM CRL armour": "BEGIN X509 CRL",
            "policy field": "policyPerScope",
            "uploaded CRL filename": filename.replace(".pem", ""),
            "this test's CA CN": ca_cn,
        }
        shell = RemoteMachineShellConnection(self.backup_node)
        try:
            for label, marker in markers.items():
                # -a so binary archive files are searched as text, -l for
                # just the filenames, -F so nothing in the marker is treated
                # as a regex.
                hits, _ = shell.execute_command(
                    f"grep -rlaF -- '{marker}' {archive} 2>/dev/null | head -5"
                )
                found = [line.strip() for line in (hits or []) if line.strip()]
                self.assertFalse(
                    found,
                    f"A backup must not capture CRL material or policy, but "
                    f"{label} ('{marker}') was found in the archive at: "
                    f"{found}"
                )
            self.log.info(
                "Archive contains no CRL material, policy, CRL filename or CA CN"
            )
        finally:
            shell.disconnect()

    def test_restore_does_not_alter_cluster_crl_configuration(self):
        """
        Section O: restoring an older backup must not overwrite, clear or
        otherwise alter the target cluster's CRL configuration or policy.

        The backup is taken under one configuration (CRL A, Permissive) and
        restored under a deliberately different one (CRL A + CRL B, Require).
        If restore carried CRL state, the post-restore configuration would
        drift back towards the state captured at backup time.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_restorecfg", "backup_admin"
        )
        cert, key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        remote_cert = self._copy_pem_to_backup_node(
            self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        )
        remote_key = self._copy_pem_to_backup_node(
            self._write_temp_pem(self.crl_utils.key_to_pem(key))
        )

        # ── State 1, captured by the backup.
        first = "bkp_o_restore_state1.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], first, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(first)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Permissive",
                            "nodeToNode": "Disabled"},
        )

        archive = self._new_archive_dir("restore_cfg")
        repo = "crl_restore_cfg"
        cluster_host = f"https://{self.cluster.master.ip}:{MGMT_PORT}"
        _, stderr = self.backup_mgr.configure_backup(archive, repo)
        self.assertFalse(stderr, f"cbbackupmgr repo create failed: {stderr}")
        output, error = self.backup_mgr.backup(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert, client_key=remote_key,
            no_progress_bar=True,
        )
        self.assertFalse(error, f"Backup should succeed: {error}")

        # ── State 2, deliberately different, and what must survive.
        second = "bkp_o_restore_state2.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], second, crl_number=2,
        )
        self.assertTrue(status, f"Second CRL upload failed: {content}")
        self._track_uploaded_file(second)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )

        settings_before = self.crl_utils.get_settings(self.rest)
        files_before = self.crl_utils.list_files(self.rest)

        output, error = self.backup_mgr.restore(
            archive, repo, cluster_host=cluster_host,
            client_cert=remote_cert, client_key=remote_key,
            no_progress_bar=True, force_updates=True,
        )
        self.assertFalse(
            error,
            f"Restore should succeed so the check below is about CRL state "
            f"rather than about a failed restore: {error}"
        )
        self.log.info("Restore of a backup taken under a different CRL config done")

        settings_after = self.crl_utils.get_settings(self.rest)
        files_after = self.crl_utils.list_files(self.rest)
        self.assertEqual(
            settings_before, settings_after,
            f"A restore must not alter the cluster's CRL settings: before="
            f"{settings_before}, after={settings_after}"
        )
        self.assertEqual(
            files_before, files_after,
            f"A restore must not add, remove or replace uploaded CRL files: "
            f"before={files_before}, after={files_after}"
        )
        self.log.info("CRL settings and uploaded CRL files unchanged by restore")


    # ── Section A: remaining Backup Service REST API coverage ───────────────

    def test_mutating_requests_enforce_revocation_like_reads(self):
        """
        Section A: rejection must apply equally to read-only GETs and to
        mutating requests. Every other REST test in this suite uses GETs, so
        without this a write path could in principle be served by a listener
        that never consulted the CRL.

        Uses plan creation as the mutating request: it is a POST with a body,
        it is cheap, and it is trivially reversible.
        """
        valid_user, _ = self._create_rbac_test_user(
            "crl_bkp_mut_ok", "backup_admin"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, valid_user
        )
        valid_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(valid_cert))
        valid_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(valid_key))

        revoked_user, _ = self._create_rbac_test_user(
            "crl_bkp_mut_rev", "backup_admin"
        )
        revoked_cert, revoked_key, revoked_serial = \
            self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, revoked_user
            )
        revoked_cert_path = self._write_temp_pem(
            self.crl_utils.cert_to_pem(revoked_cert))
        revoked_key_path = self._write_temp_pem(
            self.crl_utils.key_to_pem(revoked_key))

        filename = "bkp_a_mutating.pem"
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

        plan_name = "crl_mutating_plan"
        path = f"/api/v1/plan/{plan_name}"
        plan_body = {
            "description": "CRL mutating-request probe",
            "tasks": [{
                "name": "crl_probe_task",
                "task_type": "BACKUP",
                "schedule": {"job_type": "BACKUP", "frequency": 1,
                             "period": "HOURS"},
            }],
        }

        # Clear any leftover from an aborted earlier run, so the valid-cert
        # POST below is a real create rather than a 400 "already exists".
        try:
            self._backup_service_request(
                "DELETE", path, cert=(valid_cert_path, valid_key_path))
        except Exception as exc:
            self.log.info(f"No pre-existing probe plan to remove ({exc})")

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                "POST", path, cert=(revoked_cert_path, revoked_key_path),
                json=plan_body,
            ),
            "A revoked cert must be refused on a mutating POST exactly as it "
            "is on a read-only GET",
        )
        self.log.info("Revoked cert refused on a mutating POST as expected")

        # Absorb the propagation delay with a GET before the POST rather than
        # polling the POST itself: creating a plan is not idempotent, so a
        # retry would hit "already exists" and report a failure that is really
        # a second attempt succeeding at the wrong thing.
        self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            cert=(valid_cert_path, valid_key_path),
        )
        try:
            resp = self._backup_service_request(
                "POST", path, cert=(valid_cert_path, valid_key_path),
                json=plan_body,
            )
            self.assertEqual(
                resp.status_code, 200,
                f"A valid cert must still be able to mutate: "
                f"{resp.status_code}: {resp.text}"
            )
            self.log.info("Valid cert completed the mutating POST as expected")
        finally:
            self._backup_service_request(
                "DELETE", path, cert=(valid_cert_path, valid_key_path))

    def test_plain_http_port_has_no_revocation_check(self):
        """
        Section A: the plain HTTP Backup Service port (8097) is unchanged and
        no revocation check applies where no TLS certificate is presented.

        The user here owns a revoked certificate, but presents none -- the
        connection carries no TLS at all. Under Require that must still
        authenticate by password, and must generate no crlsValidate round
        trip, since there is nothing to validate.
        """
        user, password = self._create_rbac_test_user(
            "crl_bkp_plainhttp", "backup_admin"
        )
        _, _, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )

        filename = "bkp_a_plain_http.pem"
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
        # Deliberately NOT enabling client cert auth: mandatory mTLS would
        # reject a certificate-less connection before the point of this test
        # could be reached.

        baseline = self._crls_validate_counter_start()
        url = (f"http://{self.backup_node.ip}:{BACKUP_SERVICE_HTTP_PORT}"
               f"{ENDPOINT_GROUPS['plan']}")
        resp = requests.get(
            url, auth=(user, password), timeout=30,
            headers={"Connection": "close"},
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Password auth over the plain HTTP port must be unaffected by a "
            f"Require policy, got {resp.status_code}: {resp.text}"
        )
        self.log.info("Password auth over plain HTTP 8097 succeeded under Require")

        calls = self._crls_validate_count()
        self.assertIsNotNone(
            calls,
            "The crlsValidate counting rule went missing mid-test, so the "
            "zero-round-trip half of this test could not be measured"
        )
        self.assertEqual(
            calls, baseline,
            f"A connection presenting no TLS certificate must generate no "
            f"crlsValidate request, but the count moved from {baseline} to "
            f"{calls}"
        )
        self.log.info("Plain HTTP connection made no crlsValidate calls")

    def test_chain_missing_intermediate_is_rejected(self):
        """
        Section A: behaviour when the client presents a chain missing its
        intermediate certificate.

        Policy is Disabled throughout, deliberately: that removes revocation
        from the picture entirely, so a refusal can only be attributed to the
        incomplete chain. The second leg is the control -- the same leaf and
        key, with the intermediate appended, must succeed, which is what
        proves the first leg failed for the missing intermediate rather than
        because the leaf was unusable to begin with.
        """
        inter_cert, inter_key, _ = self.crl_utils.generate_intermediate_ca(
            self.ca_cert, self.ca_key, "BackupCRLChainInter"
        )
        user, _ = self._create_rbac_test_user(
            "crl_bkp_chain", "backup_admin"
        )
        leaf, leaf_key, _ = self.crl_utils.generate_leaf_cert(
            inter_cert, inter_key, user
        )
        leaf_pem = self.crl_utils.cert_to_pem(leaf)
        inter_pem = self.crl_utils.cert_to_pem(inter_cert)
        leaf_only_path = self._write_temp_pem(leaf_pem)
        full_chain_path = self._write_temp_pem(leaf_pem + inter_pem)
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))

        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        self.assert_cert_refused(
            lambda: self._backup_service_request(
                "GET", ENDPOINT_GROUPS["plan"],
                cert=(leaf_only_path, key_path),
            ),
            "A client chain missing its intermediate must not authenticate "
            "against the Backup Service REST API",
        )
        self.log.info("Leaf presented without its intermediate was refused")

        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            cert=(full_chain_path, key_path),
        )
        self.assertEqual(
            resp.status_code, 200,
            f"Control leg: the same leaf with its intermediate appended must "
            f"authenticate, otherwise the refusal above cannot be attributed "
            f"to the missing intermediate. Got {resp.status_code}: "
            f"{resp.text}"
        )
        self.log.info("Same leaf with the intermediate appended authenticated")

    def test_concurrent_mixed_certs_are_judged_independently(self):
        """
        Section A: concurrent connections carrying a mix of revoked and valid
        certificates are judged independently and correctly.

        This is the case a per-connection cache bug or a shared verdict would
        break: three valid and three revoked certificates, all issued by the
        same CA, all hitting the same endpoint at the same moment. Every
        valid one must get 200 and every revoked one must be refused.

        Uses plain threads rather than a ThreadPoolExecutor -- the executor's
        non-daemon worker threads have hung this suite at interpreter exit
        before.
        """
        valid_certs, revoked_certs, revoked_serials = [], [], []
        for i in range(3):
            user, _ = self._create_rbac_test_user(
                f"crl_bkp_par_ok{i}", "backup_admin"
            )
            cert, key, _ = self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, user
            )
            valid_certs.append((
                self._write_temp_pem(self.crl_utils.cert_to_pem(cert)),
                self._write_temp_pem(self.crl_utils.key_to_pem(key)),
            ))
        for i in range(3):
            user, _ = self._create_rbac_test_user(
                f"crl_bkp_par_rev{i}", "backup_admin"
            )
            cert, key, serial = self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, user
            )
            revoked_certs.append((
                self._write_temp_pem(self.crl_utils.cert_to_pem(cert)),
                self._write_temp_pem(self.crl_utils.key_to_pem(key)),
            ))
            revoked_serials.append(serial)

        filename = "bkp_a_concurrent.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serials, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        results = {}

        def probe(label, cert_pair):
            try:
                resp = self._backup_service_request(
                    "GET", ENDPOINT_GROUPS["plan"], cert=cert_pair
                )
                results[label] = resp.status_code
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError) as exc:
                results[label] = type(exc).__name__
            except Exception as exc:                      # noqa: BLE001
                results[label] = f"UNEXPECTED:{type(exc).__name__}:{exc}"

        threads = []
        for i, pair in enumerate(valid_certs):
            threads.append(threading.Thread(
                target=probe, args=(f"valid{i}", pair)))
        for i, pair in enumerate(revoked_certs):
            threads.append(threading.Thread(
                target=probe, args=(f"revoked{i}", pair)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        self.assertEqual(
            len(results), len(threads),
            f"Every concurrent probe should have recorded an outcome, got "
            f"{results}"
        )
        for i in range(3):
            self.assertEqual(
                results.get(f"valid{i}"), 200,
                f"Concurrent valid cert {i} must be judged on its own merits "
                f"and get 200, got {results.get(f'valid{i}')}. Full result "
                f"set: {results}"
            )
        for i in range(3):
            outcome = results.get(f"revoked{i}")
            self.assertIn(
                outcome, (401, "SSLError", "ConnectionError"),
                f"Concurrent revoked cert {i} must be refused -- a TLS alert "
                f"or 401 -- got {outcome}. Full result set: {results}"
            )
        self.log.info(
            f"Six concurrent mixed-cert connections judged independently: "
            f"{results}"
        )

    # ── Section B: cbbs inbound - internal gRPC (9124) ──────────────────────

    def test_internal_grpc_listener_revocation_and_governing_scope(self):
        """
        Section B: a peer presenting a revoked certificate must not establish
        the internal gRPC channel, and -- the plan's explicit open question --
        which scope that listener applies, clientAuth or nodeToNode.

        The scope is settled empirically by probing the same revoked
        certificate under two configurations that differ only in which scope
        is switched on. Whatever the answer, one invariant has to hold in the
        clientAuth/Require pass: a revoked certificate must not come away
        with a usable channel while a valid one does.

        Note this listener is probed with a raw HTTP/2 preface rather than
        gRPC proper. That is enough to tell a granted channel from a refused
        one, which is what revocation enforcement turns on, without needing
        cbbs's protobuf definitions.
        """
        valid_user, _ = self._create_rbac_test_user(
            "crl_bkp_grpc_ok", "backup_admin"
        )
        valid_cert, valid_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, valid_user
        )
        valid_pair = (
            self._write_temp_pem(self.crl_utils.cert_to_pem(valid_cert)),
            self._write_temp_pem(self.crl_utils.key_to_pem(valid_key)),
        )

        revoked_user, _ = self._create_rbac_test_user(
            "crl_bkp_grpc_rev", "backup_admin"
        )
        revoked_cert, revoked_key, revoked_serial = \
            self.crl_utils.generate_leaf_cert(
                self.ca_cert, self.ca_key, revoked_user
            )
        revoked_pair = (
            self._write_temp_pem(self.crl_utils.cert_to_pem(revoked_cert)),
            self._write_temp_pem(self.crl_utils.key_to_pem(revoked_key)),
        )

        filename = "bkp_b_grpc.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, revoked_serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # ── Pass 1: clientAuth governs.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")

        no_cert = self._grpc_channel_probe(cert=None)
        revoked_under_client_auth = self._grpc_channel_probe(cert=revoked_pair)
        valid_under_client_auth = self._grpc_channel_probe(cert=valid_pair)
        self.log.info(
            f"clientAuth=Require, mandatory mTLS -- gRPC 9124 probes: "
            f"no cert={no_cert}, revoked={revoked_under_client_auth}, "
            f"valid={valid_under_client_auth}"
        )

        # ── Pass 2: only nodeToNode is on. Drop out of mandatory first over
        # plain HTTP, exactly as the cbbackupmgr tests do, or self.rest is
        # walled out while changing the policy.
        self._disable_client_cert_auth()
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Require"},
        )
        self._enable_client_cert_auth(state="mandatory")
        revoked_under_node_to_node = self._grpc_channel_probe(cert=revoked_pair)
        valid_under_node_to_node = self._grpc_channel_probe(cert=valid_pair)
        self.log.info(
            f"clientAuth=Disabled, nodeToNode=Require -- gRPC 9124 probes: "
            f"revoked={revoked_under_node_to_node}, "
            f"valid={valid_under_node_to_node}"
        )

        governing = []
        if revoked_under_client_auth != "SETTINGS":
            governing.append("clientAuth")
        if revoked_under_node_to_node != "SETTINGS":
            governing.append("nodeToNode")
        self.log.info(
            f"FINDING -- scope(s) under which gRPC 9124 refused a revoked "
            f"certificate: {governing or 'none'}"
        )

        self.assertEqual(
            valid_under_client_auth, "SETTINGS",
            f"A valid, unrevoked certificate must still be granted the "
            f"internal gRPC channel, got {valid_under_client_auth}"
        )
        self.assertNotEqual(
            revoked_under_client_auth, "SETTINGS",
            f"Under clientAuth=Require with mandatory mTLS, a revoked "
            f"certificate must not be granted a usable internal gRPC "
            f"channel, but the listener answered with an HTTP/2 SETTINGS "
            f"frame. Probes -- no cert={no_cert}, "
            f"revoked={revoked_under_client_auth}, "
            f"valid={valid_under_client_auth}"
        )
        self.assertNotEqual(
            no_cert, "SETTINGS",
            f"Under mandatory mTLS, a connection presenting no certificate "
            f"at all must not be granted a usable internal gRPC channel -- "
            f"the verify-peer-cert method sits behind this listener and must "
            f"not be reachable by an unauthenticated remote caller. Got "
            f"{no_cert}"
        )

        # Pass 2 is pinned, not merely logged. The commit claims this test
        # answers the plan's open question about which scope governs 9124, and
        # an unasserted probe would let a future change flip that answer
        # silently -- delete pass 2 entirely and no assertion would notice.
        self.assertEqual(
            valid_under_node_to_node, "SETTINGS",
            f"Control for pass 2: a valid certificate must still be granted "
            f"the channel here, otherwise a refusal of the revoked one cannot "
            f"be attributed to the nodeToNode scope rather than to the "
            f"listener being down. Got {valid_under_node_to_node}"
        )
        self.assertEqual(
            revoked_under_node_to_node, "SETTINGS",
            f"With clientAuth=Disabled and only nodeToNode=Require, the "
            f"revoked certificate must still be granted the channel -- that "
            f"is what makes clientAuth, not nodeToNode, the governing scope "
            f"for this listener. A refusal here ({revoked_under_node_to_node}) "
            f"would contradict the finding this test records and is worth "
            f"revisiting rather than silently accepting."
        )

    def test_internal_grpc_listener_survives_malformed_payloads(self):
        """
        Section B: the listener must reject malformed, empty and oversized
        payloads without crashing or hanging cbbs.

        Health is judged by cbbs's pid being unchanged (it neither died nor
        was restarted) and by the REST API on 18097 still serving afterwards.
        A pid change would mean a crash even if the service came back.
        """
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        filename = "bkp_b_malformed.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        pid_before, fds_before = self._backup_service_pid_and_fds()
        self.assertIsNotNone(
            pid_before, "Could not read cbbs's pid before the probes"
        )

        payloads = {
            "empty": b"",
            "garbage": bytes(range(256)) * 4,
            "truncated h2 preface": self.H2_PREFACE[:8],
            "h2 preface + bad frame": self.H2_PREFACE + bytes([255] * 32),
            # An HTTP/2 frame header claiming a body far larger than the
            # default 16KiB maximum frame size, followed by nothing.
            "oversized frame header": (
                self.H2_PREFACE + bytes([0xFF, 0xFF, 0xFF, 4, 0, 0, 0, 0, 0])
            ),
            "1MiB of noise": self.H2_PREFACE + (b"\xde\xad\xbe\xef" * 262144),
        }
        for label, payload in payloads.items():
            outcome = self._grpc_channel_probe(payload=payload)
            self.log.info(f"gRPC 9124 with a {label} payload -> {outcome}")

        pid_after, fds_after = self._backup_service_pid_and_fds()
        self.assertEqual(
            pid_before, pid_after,
            f"cbbs must survive malformed gRPC payloads without crashing or "
            f"restarting, but its pid changed from {pid_before} to {pid_after}"
        )
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            auth=(self.cluster.master.rest_username,
                  self.cluster.master.rest_password),
        )
        self.assertEqual(
            resp.status_code, 200,
            f"The Backup Service REST API must still serve after the "
            f"malformed-payload probes, got {resp.status_code}: {resp.text}"
        )
        self.log.info(
            f"cbbs healthy after malformed payloads (pid {pid_after} "
            f"unchanged, fds {fds_before} -> {fds_after})"
        )

    def test_internal_grpc_listener_handles_rapid_connections(self):
        """
        Section B: rapid repeated calls -- one per DCP connection in the real
        system -- must not exhaust connections, file descriptors or goroutines
        on cbbs.

        Sixty sequential connect/preface/close cycles, then a descriptor
        count. The assertion is deliberately about unbounded growth rather
        than an exact number: a handful of descriptors may legitimately be in
        flight when the count is taken, but sixty leaked sockets would not be.
        """
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        filename = "bkp_b_rapid.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, [], filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        pid_before, fds_before = self._backup_service_pid_and_fds()
        self.assertIsNotNone(
            pid_before, "Could not read cbbs's pid before the probes"
        )

        attempts = 60
        outcomes = {}
        for _ in range(attempts):
            outcome = self._grpc_channel_probe(timeout=10)
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
        self.log.info(f"{attempts} rapid gRPC 9124 probes -> {outcomes}")

        pid_after, fds_after = self._backup_service_pid_and_fds()
        self.assertEqual(
            pid_before, pid_after,
            f"cbbs must survive rapid repeated gRPC connections without "
            f"crashing or restarting, but its pid changed from {pid_before} "
            f"to {pid_after}"
        )
        self.assertIsNotNone(
            fds_after, "Could not read cbbs's descriptor count after the probes"
        )
        self.assertLess(
            fds_after, fds_before + attempts // 2,
            f"cbbs looks to be leaking descriptors across rapid gRPC "
            f"connections: {fds_before} before, {fds_after} after "
            f"{attempts} probes"
        )
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            auth=(self.cluster.master.rest_username,
                  self.cluster.master.rest_password),
        )
        self.assertEqual(
            resp.status_code, 200,
            f"The Backup Service REST API must still serve after {attempts} "
            f"rapid gRPC probes, got {resp.status_code}: {resp.text}"
        )
        self.log.info(
            f"cbbs healthy after {attempts} rapid probes (pid unchanged, "
            f"fds {fds_before} -> {fds_after})"
        )

    # ── Section C: cbbs outbound - nodeToNode scope ─────────────────────────

    def test_client_auth_and_node_to_node_scopes_are_independent(self):
        """
        Section C: clientAuth and nodeToNode can be set to different
        strictness levels, and each path must honour only its own scope --
        inbound checks are skipped entirely when clientAuth is Disabled even
        while nodeToNode is Require, and vice versa.

        Both legs use the same revoked certificate on the same inbound
        endpoint, so the only variable is which scope is switched on. If the
        scopes leaked into each other, the second leg would refuse a
        certificate that nothing in its configured scope says to check.

        The outbound half of section C -- a revoked follower node
        certificate, leader failover with a revoked leader cert, and an
        intermediate CA revoked beneath the outbound path -- needs the
        cluster's own node certificates reissued by the test CA and is not
        covered here.
        """
        user, _ = self._create_rbac_test_user(
            "crl_bkp_scopes", "backup_admin"
        )
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))

        filename = "bkp_c_scopes.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serial, filename,
            crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)

        # ── Leg 1: the scope that governs inbound is on -> refused.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        self._enable_client_cert_auth(state="mandatory")
        self.assert_cert_refused(
            lambda: self._backup_service_request(
                "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
            ),
            "With clientAuth=Require, a revoked cert must be refused on the "
            "inbound REST path",
        )
        self.log.info("clientAuth=Require, nodeToNode=Disabled -> refused")

        # ── Leg 2: only the other scope is on -> the same cert must pass.
        self._disable_client_cert_auth()
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Require"},
        )
        self._enable_client_cert_auth(state="mandatory")
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"], cert=(cert_path, key_path)
        )
        self.assertEqual(
            resp.status_code, 200,
            f"With clientAuth=Disabled the inbound path must skip revocation "
            f"entirely, even while nodeToNode=Require -- the scopes must not "
            f"leak into one another. The same revoked cert that was refused "
            f"in leg 1 got {resp.status_code}: {resp.text}"
        )
        self.log.info(
            "clientAuth=Disabled, nodeToNode=Require -> same revoked cert "
            "admitted, so the scopes are honoured independently"
        )

    def test_node_certificates_from_test_ca_are_accepted(self):
        """
        Fixture gate for the section C outbound tests. Before any test tries
        to revoke a node's own certificate, prove the cluster will accept
        node certificates issued by this suite's CA at all, that every node
        really is serving the new certificate, and that the cluster is still
        healthy over TLS afterwards.

        Worth its own test rather than being folded into setUp: if this
        mechanism breaks, every outbound section C test fails for a reason
        that has nothing to do with revocation, and that is a confusing
        place to start debugging from.
        """
        ca_cn = self.ca_cert.subject.rfc4514_string().split(
            "CN=")[-1].split(",")[0]
        servers = self.cluster.servers[:self.nodes_init]

        serials = {}
        for server in servers:
            serials[server.ip] = self._install_node_certificate(
                server, self.ca_cert, self.ca_key
            )
        self.log.info(f"Installed test-CA node certs: {serials}")

        for server in servers:
            served = self._served_certificate(server)
            issuer = served.issuer.rfc4514_string()
            self.assertIn(
                ca_cn, issuer,
                f"Node {server.ip} should be serving a certificate issued by "
                f"this test's CA ({ca_cn}) after reloadCertificate, but its "
                f"issuer is {issuer}. A reload that reports success without "
                f"changing the served certificate would make every outbound "
                f"revocation test silently vacuous."
            )
            self.assertEqual(
                served.serial_number, serials[server.ip],
                f"Node {server.ip} is serving serial "
                f"{served.serial_number}, not the {serials[server.ip]} that "
                f"was just installed"
            )
        self.log.info("Every node is serving its newly issued test-CA cert")

        # The cluster has to still work over TLS: mgmt on each node, and the
        # backup service's own listener.
        for server in servers:
            resp = requests.get(
                f"https://{server.ip}:{MGMT_PORT}/pools/default",
                auth=(self.cluster.master.rest_username,
                      self.cluster.master.rest_password),
                verify=False, timeout=30, headers={"Connection": "close"},
            )
            self.assertEqual(
                resp.status_code, 200,
                f"Node {server.ip} must still serve mgmt over TLS after its "
                f"certificate was reissued, got {resp.status_code}"
            )
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            auth=(self.cluster.master.rest_username,
                  self.cluster.master.rest_password),
        )
        self.assertEqual(
            resp.status_code, 200,
            f"The Backup Service must still serve after every node's "
            f"certificate was reissued, got {resp.status_code}: {resp.text}"
        )
        self.log.info(
            "Cluster healthy over TLS on every node with test-CA node certs"
        )

    def test_revoked_node_certificate_is_scoped_to_node_to_node(self):
        """
        Section C: a revoked node certificate must matter only to the
        nodeToNode scope, must not trigger an automatic failover, and must
        leave unaffected nodes unimpaired.

        A follower backup node's own certificate is revoked -- never the
        master's, so that the cluster stays manageable and teardown can put
        the certificates back. The same revoked certificate is then observed
        under nodeToNode=Disabled and nodeToNode=Require.

        The assertions here are the invariants the plan requires: no
        automatic failover, the master still manageable, and the other backup
        node still serving. Whether outbound enforcement is observable from
        outside is recorded as a finding rather than asserted -- the plan
        asks for section C to be confirmed and documented, and a test that
        guessed at the observable would report its own guess rather than the
        product's behaviour.
        """
        self.assertGreaterEqual(
            len(self.cluster.backup_nodes), 2,
            f"This test needs at least two backup-service nodes so a "
            f"follower's certificate can be revoked while the master's stays "
            f"valid, got {len(self.cluster.backup_nodes)}"
        )
        # Pick a follower explicitly rather than assuming backup_nodes[-1] is
        # one: cluster.master is chosen at init from whichever node answers
        # first and the server list is reordered around it, so the master can
        # perfectly well be carrying the backup service too. Revoking the
        # master's own certificate would strand the cluster and leave teardown
        # unable to restore it.
        followers = [node for node in self.cluster.backup_nodes
                     if node.ip != self.cluster.master.ip]
        self.assertTrue(
            followers,
            f"Every backup-service node is the master "
            f"({self.cluster.master.ip}), so there is no follower whose "
            f"certificate can safely be revoked. Backup nodes: "
            f"{[node.ip for node in self.cluster.backup_nodes]}"
        )
        target = followers[-1]
        # The unimpaired node may be the master; all that matters is that it
        # is a backup node whose certificate was NOT revoked.
        healthy_backup_node = next(
            node for node in self.cluster.backup_nodes if node.ip != target.ip
        )
        self.log.info(
            f"Revoking follower {target.ip}; expecting {healthy_backup_node.ip} "
            f"to stay unimpaired (master is {self.cluster.master.ip})"
        )

        servers = self.cluster.servers[:self.nodes_init]
        serials = {}
        for server in servers:
            serials[server.ip] = self._install_node_certificate(
                server, self.ca_cert, self.ca_key
            )

        def cluster_node_count():
            resp = requests.get(
                f"https://{self.cluster.master.ip}:{MGMT_PORT}/pools/default",
                auth=(self.cluster.master.rest_username,
                      self.cluster.master.rest_password),
                verify=False, timeout=30, headers={"Connection": "close"},
            )
            resp.raise_for_status()
            return len(resp.json().get("nodes", []))

        nodes_before = cluster_node_count()
        self.log.info(f"Cluster has {nodes_before} nodes before revocation")

        filename = "bkp_c_node_cert.pem"
        status, content = self.crl_utils.revoke_and_upload(
            self.rest, self.ca_cert, self.ca_key, serials[target.ip],
            filename, crl_number=1,
        )
        self.assertTrue(status, f"CRL upload failed: {content}")
        self._track_uploaded_file(filename)
        self.log.info(
            f"Revoked node {target.ip}'s own certificate (serial "
            f"{serials[target.ip]})"
        )

        # ── Leg A: nodeToNode off. Outbound checks must be skipped entirely,
        # even with clientAuth switched on.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Disabled"},
        )
        resp = self._wait_for_backup_service_ok(
            "GET", ENDPOINT_GROUPS["plan"],
            auth=(self.cluster.master.rest_username,
                  self.cluster.master.rest_password),
        )
        self.assertEqual(
            resp.status_code, 200,
            f"With nodeToNode=Disabled, a revoked node certificate must be "
            f"irrelevant to the backup service, got {resp.status_code}: "
            f"{resp.text}"
        )
        self.log.info(
            "nodeToNode=Disabled with a revoked node cert -> backup service "
            "unaffected, as required"
        )

        # ── Leg B: nodeToNode on.
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Require", "nodeToNode": "Require"},
        )
        # New connections, not established ones, are what get judged -- give
        # cbbs a moment to make some.
        time.sleep(30)

        nodes_after = cluster_node_count()
        self.assertEqual(
            nodes_before, nodes_after,
            f"A revoked node certificate must not trigger an automatic "
            f"failover: the cluster had {nodes_before} nodes before and "
            f"{nodes_after} after switching nodeToNode to Require"
        )
        self.log.info(f"No automatic failover ({nodes_after} nodes still)")

        resp = requests.get(
            f"https://{healthy_backup_node.ip}:{self.BACKUP_SERVICE_PORT}"
            f"{ENDPOINT_GROUPS['plan']}",
            auth=(self.cluster.master.rest_username,
                  self.cluster.master.rest_password),
            verify=False, timeout=30, headers={"Connection": "close"},
        )
        self.assertEqual(
            resp.status_code, 200,
            f"The backup node whose certificate was NOT revoked must be "
            f"unimpaired, but {healthy_backup_node.ip} returned "
            f"{resp.status_code}: {resp.text}"
        )
        self.log.info(f"Unaffected node {healthy_backup_node.ip} still serving")

        log_lines = self._read_backup_service_log(tail_lines=2000) or []
        evidence = [
            line.strip() for line in log_lines
            if any(token in line.lower()
                   for token in ("revok", "crl", "certificate"))
        ]
        self.log.info(
            f"FINDING -- nodeToNode=Require with a revoked node certificate "
            f"on {target.ip}: {len(evidence)} certificate/revocation-related "
            f"backup_service.log lines. Sample: {evidence[-5:]}"
        )

    def _section_c_topology(self):
        """
        (revocation target, a backup node that keeps a valid cert).

        The target is always a follower: cluster.master is chosen at init from
        whichever node answers first and the server list is reordered around
        it, so the master can carry the backup service too, and revoking its
        certificate would strand the cluster.
        """
        self.assertGreaterEqual(
            len(self.cluster.backup_nodes), 2,
            f"Needs at least two backup-service nodes, got "
            f"{[n.ip for n in self.cluster.backup_nodes]}"
        )
        followers = [node for node in self.cluster.backup_nodes
                     if node.ip != self.cluster.master.ip]
        self.assertTrue(
            followers,
            f"Every backup node is the master ({self.cluster.master.ip}); no "
            f"follower certificate can safely be revoked"
        )
        target = followers[-1]
        probe_node = next(node for node in self.cluster.backup_nodes
                          if node.ip != target.ip)
        return target, probe_node

    def _cluster_node_count(self):
        resp = requests.get(
            f"https://{self.cluster.master.ip}:{MGMT_PORT}/pools/default",
            auth=(self.cluster.master.rest_username,
                  self.cluster.master.rest_password),
            verify=False, timeout=60, headers={"Connection": "close"},
        )
        resp.raise_for_status()
        return len(resp.json().get("nodes", []))

    def test_leader_to_follower_grpc_fails_specifically_on_revoked_peer_cert(self):
        """
        P0-07 and section C: cbbs's outbound leader-to-follower gRPC must fail
        against a revoked peer node certificate, with a revocation-specific
        reason rather than a generic one, without retrying indefinitely,
        without triggering an automatic failover, and leaving unaffected nodes
        unimpaired.

        This is the conclusive form of the earlier scope test, which could
        only report that nothing appeared in backup_service.log -- with no new
        outbound connections in its observation window there may have been
        nothing to check. Here repository creation is used to FORCE a
        leader-to-follower round trip: the service asks every other backup
        node to confirm it can reach the archive, over the internal gRPC
        channel. Aimed at a local path that check always fails, and the error
        text is the measurement -- a missing file means the peer was reached,
        a TLS/x509 complaint means the channel was refused.

        The baseline leg matters as much as the revoked one: without proving
        the probe returns a FILE-level error while all certificates are
        valid, a TLS error afterwards could not be attributed to revocation.
        """
        target, probe_node = self._section_c_topology()
        self.log.info(
            f"Revoking follower {target.ip}; probing via {probe_node.ip} "
            f"(master is {self.cluster.master.ip})"
        )

        serials = {}
        for server in self.cluster.servers[:self.nodes_init]:
            serials[server.ip] = self._install_node_certificate(
                server, self.ca_cert, self.ca_key
            )

        plan_name = "crl_p007_plan"
        self._create_backup_plan(probe_node, plan_name)
        nodes_before = self._cluster_node_count()
        try:
            # ── Baseline: every certificate valid, so the peer must be
            # reachable and the complaint must be about the file.
            #
            # The benign CRL is a precondition, not decoration. Under Require
            # with no applicable CRL for the issuing CA, cbauth answers
            # "status undetermined" and the handshake is refused fail-closed --
            # correct product behaviour, but it would make the baseline fail
            # at TLS and destroy the comparison this test depends on.
            # Confirmed against 8.5.0-1009, which returned exactly
            # "CRLsValidate: CN=<node> status undetermined" without it.
            benign = "bkp_p007_benign.pem"
            status_up, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, [], benign,
                crl_number=1,
            )
            self.assertTrue(status_up, f"Benign CRL upload failed: {content}")
            self._track_uploaded_file(benign)
            self.crl_utils.set_settings(
                self.rest,
                policyPerScope={"clientAuth": "Disabled",
                                "nodeToNode": "Require"},
            )
            status, body, elapsed = self._cross_node_archive_probe(
                probe_node, plan_name, "crl_p007_before"
            )
            self.log.info(
                f"Baseline probe ({elapsed:.1f}s, HTTP {status}): {body[:300]}"
            )
            self.assertTrue(
                self._file_level_failure_in(body),
                f"Baseline: with every node certificate valid the cross-node "
                f"check should reach the peer and complain about the archive "
                f"file, which is what makes a TLS failure below attributable "
                f"to revocation. Got HTTP {status}: {body[:400]}"
            )
            self.assertFalse(
                self._tls_failure_in(body),
                f"Baseline: no TLS/certificate failure expected while every "
                f"certificate is valid. Got HTTP {status}: {body[:400]}"
            )
            self.log.info("Baseline: peer reached over gRPC, file-level error")

            # ── Revoke the follower's own certificate.
            filename = "bkp_p007_node.pem"
            status_up, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, serials[target.ip],
                filename, crl_number=2,
            )
            self.assertTrue(status_up, f"CRL upload failed: {content}")
            self._track_uploaded_file(filename)
            self.log.info(
                f"Revoked {target.ip}'s node certificate (serial "
                f"{serials[target.ip]})"
            )

            status, body, elapsed = self._cross_node_archive_probe(
                probe_node, plan_name, "crl_p007_after"
            )
            self.log.info(
                f"Post-revocation probe ({elapsed:.1f}s, HTTP {status}): "
                f"{body[:400]}"
            )

            # No indefinite retry: the call has to come back on its own.
            self.assertIsNotNone(
                status,
                f"The cross-node check must return rather than hang when a "
                f"peer certificate is revoked; the request did not complete "
                f"in {elapsed:.0f}s: {body[:300]}"
            )
            self.assertLess(
                elapsed, 240,
                f"The cross-node check must not retry indefinitely against a "
                f"revoked peer; it took {elapsed:.0f}s"
            )

            # No automatic failover, and the other backup node unimpaired.
            nodes_after = self._cluster_node_count()
            self.assertEqual(
                nodes_before, nodes_after,
                f"A revoked peer node certificate must not trigger an "
                f"automatic failover: {nodes_before} nodes before, "
                f"{nodes_after} after"
            )
            resp = requests.get(
                f"http://{probe_node.ip}:{self.BACKUP_SERVICE_HTTP_PORT}"
                f"{ENDPOINT_GROUPS['plan']}",
                auth=(self.cluster.master.rest_username,
                      self.cluster.master.rest_password),
                timeout=60, headers={"Connection": "close"},
            )
            self.assertEqual(
                resp.status_code, 200,
                f"The node whose certificate was not revoked must be "
                f"unimpaired, got {resp.status_code}: {resp.text[:200]}"
            )
            self.log.info(
                f"No failover ({nodes_after} nodes), {probe_node.ip} still "
                f"serving"
            )

            # The reason must be revocation-specific.
            self.assertTrue(
                self._tls_failure_in(body),
                f"P0-07: leader-to-follower gRPC against a REVOKED peer node "
                f"certificate must fail at the transport, not with the same "
                f"file-level error seen while every certificate was valid. "
                f"HTTP {status}: {body[:600]}"
            )
            # "status undetermined" is fail-closed, not revocation. P0-07 asks
            # for a revocation-specific reason, so require one.
            self.assertTrue(
                self._revoked_reason_in(body),
                f"P0-07 requires a REVOCATION-specific reason. The peer was "
                f"refused, but the reason does not name revocation -- if it "
                f"says 'status undetermined' the check failed closed without "
                f"consulting the CRL that names this certificate. HTTP "
                f"{status}: {body[:600]}"
            )
            self.log.info(
                "Revoked peer node cert produced a TLS/certificate-specific "
                "cross-node failure, as P0-07 requires"
            )
        finally:
            self._delete_backup_plan(probe_node, plan_name)

    def test_revoked_intermediate_ca_invalidates_node_certs_beneath_it(self):
        """
        Section C: revoking an intermediate CA must invalidate every node
        certificate issued beneath it on the outbound path.

        Requires checkIntermediateCerts=true: with the default (false) only
        the peer certificate's own serial is consulted, so the cascade is not
        expected to happen at all.

        Only the target follower is moved onto the intermediate; the other
        nodes stay directly under the root. The intermediate's own serial is
        then revoked by a CRL from the root, so nothing names the node's
        certificate directly -- if the cascade works, the node is still
        rejected.
        """
        target, probe_node = self._section_c_topology()

        inter_cert, inter_key, inter_serial = \
            self.crl_utils.generate_intermediate_ca(
                self.ca_cert, self.ca_key, "BackupCRLOutboundInter"
            )
        inter_pem = self.crl_utils.cert_to_pem(inter_cert)
        # The intermediate has to be trusted in its own right before a CRL it
        # issued will be accepted -- ns_server refuses an upload whose issuer
        # it cannot match with "CRL issuer not trusted", even when the
        # issuer's own parent is trusted. Same fix as the peer-vs-chain test.
        self._trust_ca_on_cluster(inter_cert)

        for server in self.cluster.servers[:self.nodes_init]:
            if server.ip == target.ip:
                # Node must serve the intermediate too: the cluster trusts
                # only the root, so a leaf alone would not chain.
                self._install_node_certificate(
                    server, inter_cert, inter_key, chain_suffix=inter_pem
                )
            else:
                self._install_node_certificate(
                    server, self.ca_cert, self.ca_key
                )
        self.log.info(
            f"{target.ip} now chains through the intermediate; other nodes "
            f"remain directly under the root"
        )

        plan_name = "crl_inter_plan"
        self._create_backup_plan(probe_node, plan_name)
        nodes_before = self._cluster_node_count()
        try:
            # Benign CRLs from BOTH issuers: the target chains through the
            # intermediate, the other nodes directly under the root, and
            # Require fails closed on any issuer with no applicable CRL.
            for label, ca_cert, ca_key in (
                ("bkp_c_inter_benign_root.pem", self.ca_cert, self.ca_key),
                ("bkp_c_inter_benign_int.pem", inter_cert, inter_key),
            ):
                status_up, content = self.crl_utils.revoke_and_upload(
                    self.rest, ca_cert, ca_key, [], label, crl_number=1,
                )
                self.assertTrue(
                    status_up, f"Benign CRL upload failed for {label}: "
                               f"{content}")
                self._track_uploaded_file(label)
            # checkIntermediateCerts must be ON for this test to mean
            # anything. Its default is false, which evaluates revocation for
            # the PEER certificate only -- so a revoked intermediate is
            # correctly ignored and the cascade never happens. That default is
            # already pinned by test_peer_only_vs_full_chain_revocation_
            # checking; without setting it here this test asserted behaviour
            # the product is not configured to perform, and failed for a
            # reason that had nothing to do with the outbound path.
            self.crl_utils.set_settings(
                self.rest,
                policyPerScope={"clientAuth": "Disabled",
                                "nodeToNode": "Require"},
                checkIntermediateCerts=True,
            )
            status, body, elapsed = self._cross_node_archive_probe(
                probe_node, plan_name, "crl_inter_before"
            )
            self.log.info(
                f"Baseline probe ({elapsed:.1f}s, HTTP {status}): {body[:300]}"
            )
            self.assertTrue(
                self._file_level_failure_in(body),
                f"Baseline: a node chaining through a trusted intermediate "
                f"must still be reachable over gRPC. Got HTTP {status}: "
                f"{body[:400]}"
            )
            self.assertFalse(
                self._tls_failure_in(body),
                f"Baseline: no TLS failure expected before the intermediate "
                f"is revoked. Got HTTP {status}: {body[:400]}"
            )

            # Revoke the INTERMEDIATE, by a CRL from the root that issued it.
            filename = "bkp_c_intermediate.pem"
            status_up, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, inter_serial, filename,
                crl_number=2,
            )
            self.assertTrue(status_up, f"CRL upload failed: {content}")
            self._track_uploaded_file(filename)
            self.log.info(f"Revoked the intermediate CA (serial {inter_serial})")

            status, body, elapsed = self._cross_node_archive_probe(
                probe_node, plan_name, "crl_inter_after"
            )
            self.log.info(
                f"Post-revocation probe ({elapsed:.1f}s, HTTP {status}): "
                f"{body[:400]}"
            )
            self.assertEqual(
                nodes_before, self._cluster_node_count(),
                "Revoking an intermediate CA must not trigger an automatic "
                "failover"
            )
            self.assertTrue(
                self._tls_failure_in(body),
                f"Revoking an intermediate CA must invalidate the node "
                f"certificate issued beneath it on the outbound path, even "
                f"though no CRL names that certificate directly. HTTP "
                f"{status}: {body[:600]}"
            )
            self.log.info(
                "Revoking the intermediate cascaded to the node certificate "
                "beneath it, as required"
            )
        finally:
            self._delete_backup_plan(probe_node, plan_name)
