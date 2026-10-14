import base64
import datetime
import json
import threading
import time

import requests
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID

from couchbase_utils.security_utils.ipc_utils import IPCUtils
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection
from pytests.security.ipc_base import IPCBase


class IPCTest(IPCBase):
    """
    Internal-identity Password Check under mTLS -- MB-73874.

    See IPCBase for what the feature does and why. These tests drive it from the
    attacker's side: a certificate carrying the reserved internal SAN, and the
    question of whether presenting it alone is enough to become a cluster
    administrator.
    """

    def test_forged_internal_cert_blocked_when_ipc_enabled(self):
        """
        Core exploit and fix validation.

        A certificate that shares nothing with this cluster except a trust
        anchor -- unrelated subject, issued for an unrelated purpose -- with the
        reserved SAN rfc822Name internal@internal.couchbase.com injected into
        it. That injection is the whole attack: the SAN value is identical in
        every Couchbase installation and is published in the documentation, so
        obtaining such a certificate takes nothing more than asking a trusted CA
        for one extra SAN entry.

        Steps:
            1. Enable client certificate authentication.
            2. Forge the certificate described above.
            3. IPC enabled: the forged certificate alone is refused. Checked as
               an application-layer 401, not a TLS rejection -- see step 3's
               assertion for why that distinction is the point of the test.
            4. IPC disabled: the same certificate authenticates as the internal
               identity with administrator rights, reproducing the bypass. This
               is the "before" half of the evidence and must fail loudly if it
               ever stops reproducing, because that would mean the test is no
               longer exercising the thing it claims to.
            5. IPC re-enabled: refused again, proving the setting is what
               decides and that the result is not an artefact of ordering.
        """
        self.log.info("Step 1: enabling client certificate authentication")
        self._enable_client_cert_auth(state="enable")

        self.log.info(
            "Step 2: forging a certificate with an unrelated subject and the "
            "reserved internal SAN injected"
        )
        cert_path, key_path = self.generate_forged_internal_cert()

        self.log.info("Step 3: IPC enabled -- the forged certificate must be refused")
        self.set_ipc_setting(True)
        self.assert_no_identity_from_cert(cert_path, key_path)
        self.log.info("Step 3 passed: forged certificate established no identity")

        self.log.info(
            "Step 4: IPC disabled -- the same certificate must reproduce the "
            "original bypass"
        )
        self.set_ipc_setting(False)
        identity = self.whoami_via_mtls(cert_path, key_path)
        self.log.info(f"Step 4: /whoami resolved the forged certificate to {identity}")
        resolved_id = identity.get("id", "")
        self.assertTrue(
            resolved_id.startswith("@"),
            f"With IPC disabled the forged certificate should resolve to an "
            f"internal '@' identity, got {identity!r}. If this stops "
            f"reproducing, the certificate is no longer being recognised as an "
            f"internal one and step 3 proves nothing."
        )
        resp = self.mtls_request(cert_path, key_path, path="/pools/default")
        self.assertEqual(
            resp.status_code, 200,
            f"With IPC disabled the forged certificate should reach "
            f"/pools/default as an administrator, got HTTP "
            f"{resp.status_code}: {resp.text[:300]}"
        )
        self.log.info(
            "Step 4 passed: bypass reproduced -- the certificate alone granted "
            f"administrator access as {resolved_id}"
        )

        self.log.info("Step 5: IPC re-enabled -- must be refused again")
        self.set_ipc_setting(True)
        self.assert_no_identity_from_cert(cert_path, key_path)

        self.log.info(
            "Test passed: the forged internal certificate grants administrator "
            "access only while the internal identity password check is disabled"
        )

    def test_genuine_internal_cert_needs_credentials_when_ipc_enabled(self):
        """
        The other half of the contract: with IPC enabled the certificate stops
        conferring identity, and the request's own credentials decide it
        instead -- granting exactly that account's permissions, no more.

        Uses a genuine internal certificate issued by the cluster's own trusted
        CA, so this is about the supported path rather than the attack.

        Steps:
            1. Enable client certificate authentication.
            2. Issue a genuine internal certificate from the cluster's CA.
            3. IPC enabled, certificate alone: refused.
            4. Same certificate plus administrator credentials: authenticates as
               the administrator account, NOT as the internal identity. The
               certificate must contribute nothing to who the caller is.
            5. Same certificate plus a least-privileged account's credentials:
               authenticates as that account and gets no more than its own
               permissions. This is the case that would actually catch a
               residual escalation -- step 4 cannot, because an administrator
               would have reached those endpoints regardless.
        """
        self.log.info("Step 1: enabling client certificate authentication")
        self._enable_client_cert_auth(state="enable")

        self.log.info("Step 2: issuing a genuine internal certificate")
        cert_path, key_path = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal"
        )

        self.log.info("Step 3: IPC enabled -- certificate alone must be refused")
        self.set_ipc_setting(True)
        self.assert_no_identity_from_cert(cert_path, key_path)

        self.log.info("Step 4: certificate plus administrator credentials")
        admin_auth = (self.cluster.master.rest_username,
                      self.cluster.master.rest_password)
        identity = self.whoami_via_mtls(cert_path, key_path, auth=admin_auth)
        self.log.info(f"Step 4: /whoami resolved to {identity}")
        self.assertEqual(
            identity.get("id"), self.cluster.master.rest_username,
            f"With IPC enabled the identity must come from the supplied "
            f"credentials, not from the certificate. Expected "
            f"{self.cluster.master.rest_username!r}, got {identity!r}"
        )
        self.assertFalse(
            str(identity.get("id", "")).startswith("@"),
            f"Identity resolved to an internal '@' account despite credentials "
            f"being supplied -- the certificate is still conferring identity: "
            f"{identity!r}"
        )

        self.log.info("Step 5: certificate plus a least-privileged account")
        username, password = self._create_rbac_test_user(
            "ipc_ro_user", "ro_admin"
        )
        identity = self.whoami_via_mtls(
            cert_path, key_path, auth=(username, password)
        )
        self.log.info(f"Step 5: /whoami resolved to {identity}")
        self.assertEqual(
            identity.get("id"), username,
            f"Expected the read-only account {username!r} to be the resolved "
            f"identity, got {identity!r}"
        )
        resp = self.mtls_request(
            cert_path, key_path, path="/settings/clientCertAuth",
            auth=(username, password),
        )
        self.assertIn(
            resp.status_code, (401, 403),
            f"A read-only account presenting an internal certificate must not "
            f"reach an administrative endpoint -- the certificate confers no "
            f"privilege. Got HTTP {resp.status_code}: {resp.text[:300]}"
        )

        self.log.info(
            "Test passed: with IPC enabled the certificate establishes no "
            "identity and confers no privilege; credentials alone decide both"
        )

    def test_ipc_cca_state_auth_matrix(self):
        """
        Every client_cert_auth state x IPC on/off x five credential
        combinations, on a genuine internal certificate.

        With cca=disable the certificate is ignored in both states. The IPC
        off rows are the "before" evidence: there the certificate overrides
        even a wrong internal password and an administrator's own
        credentials.

        Mismatches are collected for the whole matrix and reported together.
        """
        cert_path, key_path = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal"
        )
        admin = self.cluster.master.rest_username
        admin_auth = (admin, self.cluster.master.rest_password)

        mismatches = []
        for cca in self.CCA_STATES:
            for ipc in (False, True):
                # Everything that goes through the framework's HTTPS REST
                # happens before cca is applied: under 'mandatory' it has no
                # client certificate and would be refused.
                self._set_client_cert_auth_http("enable")
                self.set_ipc_setting(ipc)
                internal_auth = ("@internal", self.get_internal_password())
                self.log.info(f"clientCertAuth -> {cca}, ipc={ipc}")
                self._set_client_cert_auth_http(cca)
                try:
                    if cca == "mandatory":
                        self.wait_for_mandatory()
                    probes = {
                        "cert alone": (cert_path, None),
                        "cert + admin": (cert_path, admin_auth),
                        "no cert + admin": (None, admin_auth),
                        "cert + @internal:password":
                            (cert_path, internal_auth),
                        "cert + @internal:wrong":
                            (cert_path, ("@internal", "not-the-password")),
                    }
                    expected = self.expected_auth_matrix_cells(cca, ipc, admin)
                    for name, (cert, auth) in probes.items():
                        actual = self.probe_identity(cert, key_path, auth=auth)
                        self.log.info(
                            f"cca={cca:9} ipc={ipc!s:5} {name:27} -> "
                            f"{actual}")
                        if actual != expected[name]:
                            mismatches.append(
                                f"cca={cca} ipc={ipc} {name}: expected "
                                f"{expected[name]}, got {actual}")
                finally:
                    if cca == "mandatory":
                        self._set_client_cert_auth_http("enable")

        self.assertFalse(
            mismatches,
            "Auth matrix mismatches (whoami id, /pools/default status):\n"
            + "\n".join(mismatches))
        self.log.info("Test passed: all 40 matrix cells as expected")

    def test_ipc_credential_channels_and_bypass_attempts(self):
        """
        With IPC on, every other way of attaching identity to an
        internal-certificate request is judged on its own merits, and none
        falls back to the certificate.

        Steps:
            1. Fixtures: cca=enable, genuine + forged internal certs, a
               prefix-mapped cert for a ro_admin user. IPC on.
            2. Credentials that must not authenticate: non-existent user
               locked user, deleted user.
            3. Role granted then revoked is re-evaluated per request.
            4. Forged cert + ordinary user's credentials.
            5. Every verb on sample endpoints: cert alone 401, cert + admin
               passes authorization, including /diag/eval.
            6. Other credential channels: bogus Bearer, empty Basic,
               cb-on-behalf-of, spoofed client-cert headers over HTTP and
               HTTPS.
            7. Same TLS connection, creds then none: identity is per
               request.
            8. Rejections are indistinguishable to the caller.
            9. UI cert login creates no session and cbauth
               extractUserFromCert returns 200 {} vs a user, each
               contrasted with IPC off.
        """
        master = self.cluster.master
        admin_auth = (master.rest_username, master.rest_password)

        self.log.info("Step 1: fixtures")
        self._enable_client_cert_auth(state="enable")
        cert, key = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")
        forged_cert, forged_key = self.generate_forged_internal_cert()
        ro_user, ro_pw = self._create_rbac_test_user("ipc_c_ro", "ro_admin")
        leaf, leaf_key, _ = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, ro_user)
        prefix_cert = self._write_temp_pem(self.crl_utils.cert_to_pem(leaf))
        prefix_key = self._write_temp_pem(self.crl_utils.key_to_pem(leaf_key))
        self.set_ipc_setting(True)

        self.log.info("Step 2: credentials that must not authenticate")
        resp = self.mtls_request(cert, key, auth=("ipc_no_such_user", "pw"))
        self.assert_status(resp, 401, "C-08 non-existent user")

        locked_user, locked_pw = self._create_rbac_test_user(
            "ipc_c_locked", "ro_admin")
        locked_auth = (locked_user, locked_pw)
        self.assert_status(self.mtls_request(cert, key, auth=locked_auth),
                            200, "C-09 before lock")
        self.set_user_locked(locked_user, True)
        try:
            self.assert_status(
                self.mtls_request(cert, key, auth=locked_auth), 401,
                "C-09 locked user with internal cert")
            self.assert_status(self.https_request(auth=locked_auth), 401,
                                "C-09 locked user, no cert (control)")
        finally:
            self.set_user_locked(locked_user, False)
        self.assert_status(self.mtls_request(cert, key, auth=locked_auth),
                            200, "C-09 after unlock")

        gone_user, gone_pw = self._create_rbac_test_user(
            "ipc_c_deleted", "ro_admin")
        self.rest.delete_builtin_user(gone_user)
        self._rbac_users.remove(gone_user)
        self.assert_status(
            self.mtls_request(cert, key, auth=(gone_user, gone_pw)), 401,
            "N-05 deleted user")

        self.log.info("Step 3: role change re-evaluated per request")
        ro_auth = (ro_user, ro_pw)
        audit = "/settings/audit"
        self.assert_status(
            self.mtls_request(cert, key, path=audit, auth=ro_auth), 403,
            "N-06 ro_admin on /settings/audit")
        self._set_rbac_roles(ro_user, "ro_admin,security_admin")
        self.assert_status(
            self.mtls_request(cert, key, path=audit, auth=ro_auth), 200,
            "N-06 after granting security_admin")
        self._set_rbac_roles(ro_user, "ro_admin")
        self.assert_status(
            self.mtls_request(cert, key, path=audit, auth=ro_auth), 403,
            "N-06 after revoking security_admin")

        self.log.info("Step 4: forged cert + ordinary user (S-02)")
        who = self.whoami_via_mtls(forged_cert, forged_key, auth=ro_auth)
        self.assertEqual(who.get("id"), ro_user,
                         f"S-02: forged cert must not change identity: {who}")
        self.assert_status(
            self.mtls_request(forged_cert, forged_key,
                              path="/settings/clientCertAuth", auth=ro_auth),
            403, "S-02 forged cert + ro_admin on admin endpoint")

        self.log.info("Step 5: every verb, cert alone vs cert + admin")
        verbs_user = "ipc_c_verbs_user"
        self._rbac_users.append(verbs_user)
        calls = [
            ("GET", "/settings/security", None, (200,)),
            ("POST", "/settings/autoFailover", {"timeout": "bogus"},
             (400,)),
            ("PUT", f"/settings/rbac/users/local/{verbs_user}",
             {"roles": "ro_admin", "password": "Couchbase@1234"}, (200,)),
            ("DELETE", "/pools/default/buckets/ipc_no_such_bucket", None,
             (404,)),
            ("POST", "/diag/eval", "1 + 1.", (200,)),
        ]
        for method, path, data, admin_ok in calls:
            what = f"C-12 {method} {path}"
            self.assert_status(
                self.mtls_request(cert, key, path=path, method=method,
                                  data=data),
                401, f"{what}, cert alone")
            self.assert_status(
                self.mtls_request(cert, key, path=path, method=method,
                                  data=data, auth=admin_auth),
                admin_ok, f"{what}, cert + admin")

        self.log.info("Step 6: other credential channels")
        self.assert_status(
            self.mtls_request(cert, key, headers={
                "Authorization": "Bearer aaa.bbb.ccc"}),
            401, "C-18 bogus Bearer token")
        self.assert_status(
            self.mtls_request(cert, key, headers={
                "Authorization": "Basic Og=="}),
            401, "C-19 empty Basic credentials")

        obo = {"cb-on-behalf-of":
               base64.b64encode(f"{ro_user}:local".encode()).decode()}
        self.assert_status(
            self.mtls_request(cert, key, path="/whoami", headers=obo), 401,
            "C-17 on-behalf-of, cert alone")
        who = self.mtls_request(cert, key, path="/whoami", headers=obo,
                                auth=admin_auth)
        self.assert_status(who, 200, "C-17 on-behalf-of, cert + admin")
        self.assertEqual(who.json().get("id"), ro_user,
                         f"C-17: admin acting on behalf of {ro_user}: "
                         f"{who.text}")
        self.assert_status(
            self.mtls_request(cert, key, path="/whoami", headers=obo,
                              auth=ro_auth),
            401, "C-17 on-behalf-of, cert + ro_admin (not entitled)")

        spoof = {
            "X-SSL-Client-Cert": f"internal@{self.INTERNAL_CERT_DOMAIN}",
            "X-Forwarded-Client-Cert":
                f"Subject=\"CN=internal\";URI=internal@"
                f"{self.INTERNAL_CERT_DOMAIN}",
        }
        resp = requests.get(f"http://{master.ip}:8091/pools/default",
                            headers=spoof, timeout=30)
        self.assert_status(resp, 401, "C-20 spoofed headers over HTTP")
        self.assert_status(self.https_request(headers=spoof), 401,
                            "N-03 spoofed headers over HTTPS")

        self.log.info("Step 7: one TLS connection, creds then none (N-02)")
        self.assert_identity_is_per_request(cert, key, admin_auth)

        self.log.info("Step 8: rejections are opaque (L-08)")
        rejected = {
            "cert alone": self.mtls_request(cert, key),
            "forged cert": self.mtls_request(forged_cert, forged_key),
            "wrong internal password": self.mtls_request(
                cert, key, auth=("@internal", "not-the-password")),
            "non-existent user": self.mtls_request(
                cert, key, auth=("ipc_no_such_user", "pw")),
        }
        shapes = {
            name: (r.status_code, r.text,
                   r.headers.get("WWW-Authenticate"))
            for name, r in rejected.items()}
        self.log.info(f"L-08 rejection shapes: {shapes}")
        self.assertEqual(
            len(set(shapes.values())), 1,
            f"L-08: rejections must be indistinguishable to the caller: "
            f"{shapes}")
        self.assertEqual(
            next(iter(shapes.values()))[:2], (401, ""),
            f"L-08: expected an empty 401: {shapes}")

        self.log.info("Step 9: UI cert login (C-15) and cbauth (C-21)")
        prefix = (prefix_cert, prefix_key)
        for ipc, ui_status, internal_user in ((True, 400, None),
                                              (False, 403, "@internal")):
            self.set_ipc_setting(ipc)
            resp = self.mtls_request(
                cert, key, path="/uilogin?use_cert_for_auth=1",
                method="POST")
            self.assert_status(resp, ui_status, f"C-15 ipc={ipc}")
            self.assertNotIn(
                "ui-auth", resp.headers.get("Set-Cookie", ""),
                f"C-15 ipc={ipc}: an internal cert must never get a UI "
                f"session")

            for label, (c, _k), user in (
                    ("genuine", (cert, key), internal_user),
                    ("forged", (forged_cert, forged_key), internal_user),
                    ("prefix-mapped", prefix, ro_user)):
                body = self.cbauth_extract_user(c)
                self.log.info(f"C-21 ipc={ipc} {label}: {body}")
                self.assertEqual(
                    body.get("user"), user,
                    f"C-21 ipc={ipc} {label}: expected user {user!r}, got "
                    f"{body}")
                if user is None:
                    self.assertEqual(
                        body, {},
                        f"C-21: 'no identity' must be an empty object, "
                        f"distinct from a failure: {body}")

        self.log.info(
            "Test passed: with IPC on, only the request's own credentials "
            "establish identity, on every channel tried")

    def test_ipc_setting_lifecycle_validation_and_rbac(self):
        """
        The setting itself: its default, what a write does, what is rejected,
        who may change it, and that each change is audited.

        Steps:
            1. With the key removed, GET reports the compat-derived default
               (true from 8.5) and nothing is stored.
            2. Any successful write pins the key, even one of the value GET
               already reports. This is deliberate: it is what keeps an
               explicit opt-out in place across an upgrade.
            3. Re-writing the stored value is a no-op: the config entry,
               vclock included, is unchanged.
            4. Ten toggles, each enforced immediately with no restart.
            5. Invalid values, misspelt keys and a duplicated key are each
               rejected with 400 and leave the config entry untouched.
            6. Only admin and cluster_admin may write it; security_admin may
               read it; other roles and unauthenticated callers may not.
            7. With audit on, each change emits event 8228 naming the
               setting, its new value, the real user and the node.
        """
        master = self.cluster.master
        key = self.IPC_SETTING

        self.log.info("Step 1: derived default")
        self.delete_ipc_key()
        self.assertFalse(self._ipc_key_in_config(),
                         "Key still stored after ns_config:delete")
        derived = self.rest.check_cluster_compatibility("8.5")
        self.assertEqual(
            self.get_ipc_setting(), derived,
            f"A-01/A-02: with no stored key GET must report the default "
            f"derived from compat (cluster is 8.5+: {derived})")

        self.log.info("Step 2: a write of the reported value pins the key")
        self.set_ipc_setting(derived)
        self.assertTrue(
            self._ipc_key_in_config(),
            "Writing the value GET already reports must store the key")

        self.log.info("Step 3: re-writing the stored value is a no-op")
        before = self.ipc_config_raw()
        resp = IPCUtils.post_internal_settings(
            master, {key: str(derived).lower()})
        self.assert_status(resp, 200, "A-12 no-op write")
        self.assertEqual(self.ipc_config_raw(), before,
                         "A-12: a no-op write must not touch the config")

        self.log.info("Step 4: ten toggles, each enforced immediately")
        self._enable_client_cert_auth(state="enable")
        cert, cert_key = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")
        for i in range(10):
            ipc = i % 2 == 0
            self.set_ipc_setting(ipc)
            self.assert_cert_enforcement([master], cert, cert_key, ipc,
                                         f"A-04 toggle {i + 1}")

        self.log.info("Step 5: rejected writes")
        before = self.ipc_config_raw()
        value_before = self.get_ipc_setting()
        for value in ("yes", "1", "0", "TRUE", "True", "FALSE", "null",
                      "[]", "{}", "true ", " true", "tru", ""):
            resp = IPCUtils.post_internal_settings(master, {key: value})
            self.assert_status(resp, 400, f"A-07 value {value!r}")
            self.assertIn("Accepted values are 'true' and 'false'",
                          resp.text, f"A-07 value {value!r}")
        for bad_key in (key[:-4] + "MTLS", key.lower(), self.IPC_CONFIG_KEY,
                        "internalIdentityPasswordCheck"):
            resp = IPCUtils.post_internal_settings(master, {bad_key: "true"})
            self.assert_status(resp, 400, f"A-11 key {bad_key!r}")
            self.assertIn(f"Unknown key {bad_key}", resp.text,
                          f"A-11 key {bad_key!r}")
        resp = IPCUtils.post_internal_settings(
            master, f"{key}=true&{key}=false")
        self.assert_status(resp, 400, "A-08 duplicate key")
        self.assertIn("duplicate key", resp.text, "A-08 duplicate key")
        self.assertEqual(self.ipc_config_raw(), before,
                         "A-07/A-08/A-11: a rejected write changed config")
        self.assertEqual(self.get_ipc_setting(), value_before)

        self.log.info("Step 6: who may read and write the setting")
        users = {
            role: self._create_rbac_test_user(f"ipc_a_{name}", role)
            for name, role in (("cadmin", "cluster_admin"),
                               ("sec", "security_admin"),
                               ("ro", "ro_admin"),
                               ("bucket", "bucket_admin[*]"))}
        value = str(self.get_ipc_setting()).lower()
        cases = [
            ("cluster_admin", users["cluster_admin"], 200, 200),
            ("security_admin", users["security_admin"], 200, 403),
            ("ro_admin", users["ro_admin"], 403, 403),
            ("bucket_admin[*]", users["bucket_admin[*]"], 403, 403),
            ("no credentials", (), 401, 401),
            ("wrong password", (master.rest_username, "not-the-password"),
             401, 401),
        ]
        before = self.ipc_config_raw()
        for who, auth, get_status, post_status in cases:
            self.assert_status(
                IPCUtils.get_internal_settings(master, auth=auth),
                get_status, f"A-09/A-10 GET as {who}")
            resp = IPCUtils.post_internal_settings(
                master, {key: value}, auth=auth)
            self.assert_status(resp, post_status,
                               f"A-09/A-10 POST as {who}")
            if post_status == 403:
                self.assertIn("cluster.admin.settings!write", resp.text,
                              f"A-09 POST as {who}: missing permission")
        self.assertEqual(self.ipc_config_raw(), before,
                         "A-09/A-10: a refused or no-op write changed "
                         "config")

        self.log.info("Step 7: each change is audited (8228)")
        audit_was_on = self.rest.getAuditSettings().get("auditdEnabled")
        self.assertIs(self.rest.setAuditSettings(enabled="true"), True,
                      "Failed to enable audit")
        try:
            current = self.get_ipc_setting()
            for new in (not current, current):
                baseline = self.latest_internal_settings_audit()
                self.set_ipc_setting(new)
                event = self.wait_for_internal_settings_audit(baseline, new)
                self.log.info(f"L-01 audit event: {event}")
                self.assertEqual(
                    event.get("real_userid", {}).get("user"),
                    master.rest_username, f"L-01 actor: {event}")
                self.assertTrue(event.get("remote", {}).get("ip"),
                                f"L-01 remote address missing: {event}")
                self.assertEqual(event.get("local", {}).get("ip"),
                                 master.ip, f"L-01 local address: {event}")
                self.assertTrue(event.get("timestamp"),
                                f"L-01 timestamp missing: {event}")
        finally:
            self.rest.setAuditSettings(
                enabled="true" if audit_was_on else "false")

        self.log.info(
            "Test passed: default, pinning, validation, RBAC and audit of "
            "the setting all behave as designed")

    def test_ipc_setting_replication_and_persistence(self):
        """
        The setting is cluster-wide: it replicates, converges after
        concurrent writes and node outages, survives restarts, and a node
        that leaves keeps its explicit value until it joins a cluster again.
        Each step checks enforcement on every node, not just the value.

        Steps:
            1. A write on one node is reported and enforced on all nodes.
            2. Opposite values written concurrently on all nodes converge.
            3. A node that is down during a change converges when it
               returns.
            4. The value survives a single-node and a full-cluster restart.
            5. An ejected node keeps an explicit value; on rejoining it takes
               the cluster's value instead.
        """
        nodes = self.cluster.servers[:self.nodes_init]
        self.assertGreaterEqual(len(nodes), 3, "Needs a 3-node cluster")
        master, second, third = nodes[0], nodes[1], nodes[2]
        key = self.IPC_SETTING

        self._enable_client_cert_auth(state="enable")
        cert, cert_key = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key, name="internal")

        self.log.info("Step 1: a write on one node reaches every node")
        self.set_ipc_setting(False)
        self.wait_for_ipc_value(nodes, False)
        self.assert_cert_enforcement(nodes, cert, cert_key, False, "A-05")
        resp = IPCUtils.post_internal_settings(second, {key: "true"})
        self.assert_status(resp, 200, f"A-05 POST on {second.ip}")
        self.wait_for_ipc_value(nodes, True)
        self.assert_cert_enforcement(nodes, cert, cert_key, True, "A-05")

        self.log.info("Step 2: concurrent opposite writes converge")
        start = threading.Barrier(len(nodes))
        results = {}

        def write(server, value):
            start.wait()
            results[server.ip] = IPCUtils.post_internal_settings(
                server, {key: value}).status_code

        threads = [threading.Thread(target=write, args=(s, v))
                   for s, v in zip(nodes, ("true", "false", "true"))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.log.info(f"A-16 concurrent POST statuses: {results}")
        self.assertTrue(all(code == 200 for code in results.values()),
                        f"A-16: every concurrent write must succeed: "
                        f"{results}")
        winner = self.get_ipc_setting()
        self.wait_for_ipc_value(nodes, winner)
        self.assert_cert_enforcement(nodes, cert, cert_key, winner, "A-16")

        self.log.info("Step 3: a node down during a change converges")
        shell = RemoteMachineShellConnection(third)
        try:
            shell.stop_couchbase()
            self.set_ipc_setting(not winner)
            self.wait_for_ipc_value(nodes[:2], not winner)
            shell.start_couchbase()
        finally:
            shell.disconnect()
        self.assertTrue(RestConnection(third).is_ns_server_running(300),
                        f"{third.ip} did not come back")
        self.wait_for_ipc_value([third], not winner)
        self.assert_cert_enforcement(nodes, cert, cert_key, not winner,
                                     "A-13")

        self.log.info("Step 4: restarts")
        expected = self.get_ipc_setting()
        self.restart_nodes([second])
        self.wait_for_ipc_value(nodes, expected)
        self.restart_nodes(nodes)
        self.wait_for_ipc_value(nodes, expected)
        self.assert_cert_enforcement(nodes, cert, cert_key, expected,
                                     "A-06 after full restart")

        self.log.info("Step 5: leaving keeps the key; rejoining adopts")
        self.set_ipc_setting(False)
        self.wait_for_ipc_value(nodes, False)
        self.assertTrue(
            self.task.rebalance(self.cluster, to_add=[], to_remove=[third]),
            f"Rebalance out of {third.ip} failed")
        # An ejected node is unprovisioned, so it answers without
        # credentials; its derived default would be true, so false here can
        # only be the kept explicit value.
        self.assertEqual(
            self.get_ipc_setting(server=third, auth=()), False,
            f"{third.ip} lost the explicit value on leaving")
        self.set_ipc_setting(True)
        self.assertTrue(
            self.task.rebalance(self.cluster, to_add=[third], to_remove=[]),
            f"Rebalance of {third.ip} back in failed")
        self.wait_for_ipc_value(nodes, True)
        self.assert_cert_enforcement(nodes, cert, cert_key, True,
                                     "after rejoin")

        self.log.info(
            "Test passed: the setting replicates, converges, persists and "
            "is enforced on every node")

    def test_ipc_internal_san_extraction_edge_cases(self):
        """
        Which certificates count as internal, and that certificate
        validation still runs before identity is considered.

        Steps:
            1. Internal-SAN variants -- any local part, including reserved
               service names, an empty one, a leading space and a very long
               one, and the internal email first or last among several.
               IPC off: each resolves to the '@<local part>' it names, with
               admin rights, so the holder chooses the identity. IPC on:
               each is anonymous and refused.
            2. Near-miss domains are never internal in either state: they
               fall through to prefix mapping, name no user, and fail
               authentication -- distinct from step 1's 'no identity'.
            3. Expired and not-yet-valid internal certs fail at TLS in both
               states.
            4. A CRL-revoked internal cert fails at TLS in both states under
               the nodeToNode scope, while an unrevoked one from the same CA
               still connects.
            5. An internal cert whose EKU lacks clientAuth fails at TLS in
               both states.
        """
        domain = self.INTERNAL_CERT_DOMAIN
        self._set_client_cert_auth_http("enable")

        self.log.info("Step 1: internal-SAN variants")
        long_local = "a" * 300
        variants = [
            ("internal", [f"internal@{domain}"]),
            ("prometheus", [f"prometheus@{domain}"]),
            ("localtoken", [f"localtoken@{domain}"]),
            ("", [f"@{domain}"]),
            (" x", [f" x@{domain}"]),
            (long_local, [f"{long_local}@{domain}"]),
            ("internal", ["someone@example.com", f"internal@{domain}"]),
            ("internal", [f"internal@{domain}", "someone@example.com"]),
        ]
        certs = []
        for i, (local, emails) in enumerate(variants):
            cert, key, _ = self.generate_client_cert(
                f"ipc-o-variant-{i}", email_names=emails)
            certs.append((f"@{local}", emails, cert, key))
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            for identity, emails, cert, key in certs:
                expected = ("", 401) if ipc else (identity, 200)
                actual = self.probe_identity(cert, key)
                self.assertEqual(
                    actual, expected,
                    f"O-01..O-03 ipc={ipc} SAN {emails[:1]}...: expected "
                    f"{expected}, got {actual}")

        self.log.info("Step 2: near-miss domains")
        near_misses = [
            f"x@{domain}.evil.com", f"x@sub.{domain}",
            f"x@{domain.upper()}", "x@Internal.Couchbase.Com",
            f"x@{domain}.", f"x@{domain} ",
        ]
        near_certs = [
            (email,) + self.generate_client_cert(
                f"ipc-o-nearmiss-{i}", email_names=[email])[:2]
            for i, email in enumerate(near_misses)]
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            for email, cert, key in near_certs:
                actual = self.probe_identity(cert, key)
                self.assertEqual(
                    actual, (None, 401),
                    f"O-04 ipc={ipc} {email!r} must not be treated as "
                    f"internal, got {actual}")

        self.log.info("Step 3: validity window checked before identity")
        now = datetime.datetime.now(datetime.timezone.utc)
        day = datetime.timedelta(days=1)
        internal = [f"internal@{domain}"]
        invalid = {
            "expired": self.generate_client_cert(
                "ipc-o-expired", email_names=internal,
                not_valid_before=now - 10 * day,
                not_valid_after=now - day)[:2],
            "not yet valid": self.generate_client_cert(
                "ipc-o-future", email_names=internal,
                not_valid_before=now + day,
                not_valid_after=now + 10 * day)[:2],
        }
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            for label, (cert, key) in invalid.items():
                self.assertEqual(
                    self.probe_identity(cert, key)[0], self.TLS_REJECT,
                    f"O-06 ipc={ipc}: {label} internal cert must fail at "
                    f"TLS")

        self.log.info("Step 4: CRL-revoked internal cert")
        revoked, revoked_key, serial = self.generate_client_cert(
            "ipc-o-revoked", email_names=internal)
        valid, valid_key, _ = self.generate_client_cert(
            "ipc-o-unrevoked", email_names=internal)
        filename = "ipc_revoked_internal.pem"
        try:
            status, content = self.crl_utils.revoke_and_upload(
                self.rest, self.ca_cert, self.ca_key, [serial], filename,
                crl_number=1)
            self.assertTrue(status, f"CRL upload failed: {content}")
            self.crl_utils.track_uploaded_file(filename)
            self.crl_utils.set_settings(
                self.rest, policyPerScope={"clientAuth": "Disabled",
                                           "nodeToNode": "Require"})
            for ipc in (False, True):
                self.set_ipc_setting(ipc)
                self.assertEqual(
                    self.wait_for_probe(revoked, revoked_key,
                                         self.TLS_REJECT),
                    self.TLS_REJECT,
                    f"O-07 ipc={ipc}: revoked internal cert must fail at "
                    f"TLS")
                expected = "" if ipc else "@internal"
                self.assertEqual(
                    self.probe_identity(valid, valid_key)[0], expected,
                    f"O-07 ipc={ipc}: unrevoked cert from the same CA")
        finally:
            self.crl_utils.reset_crl_settings(self.rest, self.cluster.master)
            self.crl_utils.cleanup_created_files(self.rest)

        self.log.info("Step 5: EKU without clientAuth")
        server_eku, server_eku_key, _ = self.generate_client_cert(
            "ipc-o-server-eku", email_names=internal,
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH])
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            self.assertEqual(
                self.probe_identity(server_eku, server_eku_key)[0],
                self.TLS_REJECT,
                f"O-08 ipc={ipc}: internal cert without clientAuth EKU "
                f"must fail at TLS")

        self.log.info(
            "Test passed: only the exact internal domain is internal, IPC "
            "neutralises every variant, and validation precedes identity")

    def test_ipc_prefix_mapped_cert_regression_and_no_narrowing(self):
        """
        Ordinary client certificates are untouched by IPC, and with IPC on an
        internal certificate neither adds nor removes anything from the
        credentials sent with it.

        Steps:
            1. A prefix-mapped cert resolves to the same user in every
               clientCertAuth state that uses certs, with IPC off and on;
               'disable' ignores it in both.
            2. A prefix-mapped cert naming no user fails in both states.
            3. Rule handling: a cert matching only the third rule; the
               san.email, san.dnsname and san.uri paths; a prefix plus
               delimiter, and a CN missing that prefix.
            4. A cert with both an internal SAN and a matching prefix field
               is internal: '@internal' with IPC off, anonymous with it on --
               never the prefix-mapped user.
            5. A cert whose CN is the administrator maps to the
               administrator in both states.
            6. Changing the rules while IPC is on keeps mapping working.
            7. Six principals x six endpoints x three connection types
               (internal cert, no cert, own prefix cert, each with the
               principal's credentials). IPC on: the connection type makes
               no difference. IPC off: the internal cert overrides the
               credentials and every principal becomes '@internal'.
        """
        master = self.cluster.master
        user, pw = self._create_rbac_test_user("ipciuser", "ro_admin")
        cn_cert, cn_key, _ = self.generate_client_cert(user)

        self.log.info("Step 1: prefix-mapped cert across cca states")
        try:
            for cca in self.CCA_STATES:
                for ipc in (False, True):
                    self._set_client_cert_auth_http("enable")
                    self.set_ipc_setting(ipc)
                    self._set_client_cert_auth_http(cca)
                    try:
                        if cca == "mandatory":
                            self.wait_for_mandatory()
                        expected = ("", 401) if cca == "disable" \
                            else (user, 200)
                        actual = self.probe_identity(cn_cert, cn_key)
                        self.assertEqual(
                            actual, expected,
                            f"I-01 cca={cca} ipc={ipc}: got {actual}")
                    finally:
                        if cca == "mandatory":
                            self._set_client_cert_auth_http("enable")
        finally:
            self._set_client_cert_auth_http("enable")

        self.log.info("Step 2: prefix-mapped cert naming no user")
        ghost, ghost_key, _ = self.generate_client_cert("ipcnosuchuser")
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            self.assertEqual(self.probe_identity(ghost, ghost_key),
                             (None, 401), f"I-02 ipc={ipc}")

        self.set_ipc_setting(True)
        self.log.info("Step 3: rule handling")
        uri = f"https://{user}/ipc"
        cases = [
            ("third rule",
             [{"path": "san.uri", "prefix": "nomatch-", "delimiter": ""},
              {"path": "san.dnsname", "prefix": "nomatch-",
               "delimiter": ""},
              {"path": "subject.cn", "prefix": "", "delimiter": ""}],
             self.generate_client_cert(user)[:2], user),
            ("san.email",
             [{"path": "san.email", "prefix": "", "delimiter": "@"}],
             self.generate_client_cert(
                 "ipc-i-email", email_names=[f"{user}@example.com"])[:2],
             user),
            ("san.dnsname",
             [{"path": "san.dnsname", "prefix": "", "delimiter": "."}],
             self.generate_client_cert(
                 "ipc-i-dns", dns_names=[f"{user}.example.com"])[:2],
             user),
            ("san.uri",
             [{"path": "san.uri", "prefix": "https://",
               "delimiter": "/"}],
             self.generate_client_cert(
                 "ipc-i-uri", extra_extension=(
                     x509.SubjectAlternativeName(
                         [x509.UniformResourceIdentifier(uri)]),
                     False))[:2],
             user),
            ("prefix + delimiter",
             [{"path": "subject.cn", "prefix": "www.cb-",
               "delimiter": "."}],
             self.generate_client_cert(f"www.cb-{user}.local")[:2], user),
            ("CN without the prefix",
             [{"path": "subject.cn", "prefix": "www.cb-",
               "delimiter": "."}],
             (cn_cert, cn_key), None),
        ]
        try:
            for label, prefixes, (cert, key), expected_user in cases:
                self._set_client_cert_auth_http("enable", prefixes)
                actual = self.probe_identity(cert, key)
                expected = (expected_user, 200) if expected_user \
                    else (None, 401)
                self.assertEqual(actual, expected,
                                 f"I-03/I-04 {label}: got {actual}")
        finally:
            self._set_client_cert_auth_http("enable")

        self.log.info("Step 4: internal SAN wins over a prefix match")
        both, both_key, _ = self.generate_client_cert(
            user, email_names=[f"internal@{self.INTERNAL_CERT_DOMAIN}"])
        for ipc, expected in ((False, ("@internal", 200)),
                              (True, ("", 401))):
            self.set_ipc_setting(ipc)
            self.assertEqual(self.probe_identity(both, both_key), expected,
                             f"I-05 ipc={ipc}")

        self.log.info("Step 5: CN = administrator")
        admin_cert, admin_key, _ = self.generate_client_cert(
            master.rest_username)
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            self.assertEqual(self.probe_identity(admin_cert, admin_key),
                             (master.rest_username, 200), f"I-06 ipc={ipc}")

        self.log.info("Step 6: rule changes while IPC is on")
        self.set_ipc_setting(True)
        many = [{"path": "san.uri", "prefix": "a-", "delimiter": ""},
                {"path": "san.dnsname", "prefix": "b-", "delimiter": ""},
                {"path": "san.email", "prefix": "c-", "delimiter": ""},
                {"path": "subject.cn", "prefix": "", "delimiter": ""}]
        try:
            for state, prefixes in (("enable", many),
                                    ("hybrid", self.DEFAULT_CCA_PREFIXES)):
                self._set_client_cert_auth_http(state, prefixes)
                self.assertEqual(
                    self.probe_identity(cn_cert, cn_key), (user, 200),
                    f"I-08 state={state} rules={len(prefixes)}")
        finally:
            self._set_client_cert_auth_http("enable")

        self.log.info("Step 7: no-narrowing matrix")
        principals = {master.rest_username: master.rest_password}
        for name, role in (("ipc_h_cadmin", "cluster_admin"),
                           ("ipc_h_ro", "ro_admin"),
                           ("ipc_h_sec", "security_admin"),
                           ("ipc_h_bucket", "bucket_admin[*]"),
                           ("ipc_h_app", "data_reader[*]")):
            principals[name] = self._create_rbac_test_user(name, role)[1]
        endpoints = ["/pools/default", "/settings/rbac/users",
                     "/settings/audit", "/settings/clientCertAuth",
                     "/pools/default/buckets"]
        internal_cert, internal_key = self.generate_internal_client_cert(
            self.ca_cert, self.ca_key)
        own_certs = {name: self.generate_client_cert(name)[:2]
                     for name in principals}

        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            mismatches = []
            for name, password in principals.items():
                auth = (name, password)
                conns = {
                    "internal cert": (internal_cert, internal_key),
                    "no cert": (None, None),
                    "own cert": own_certs[name],
                }
                row = {}
                for conn, (cert, key) in conns.items():
                    who, _ = self.probe_identity(cert, key, auth=auth)
                    codes = []
                    for path in endpoints:
                        resp = (self.mtls_request(cert, key, path=path,
                                                  auth=auth) if cert
                                else self.https_request(path, auth=auth))
                        codes.append(resp.status_code)
                    row[conn] = (who, tuple(codes))
                self.log.info(f"H ipc={ipc} {name}: {row}")
                if ipc:
                    if len(set(row.values())) != 1:
                        mismatches.append(f"{name}: {row}")
                    if row["no cert"][0] != name:
                        mismatches.append(f"{name} resolved as "
                                          f"{row['no cert'][0]}")
                elif row["internal cert"][0] != "@internal":
                    mismatches.append(
                        f"{name}: internal cert resolved as "
                        f"{row['internal cert'][0]}, expected @internal")
            self.assertFalse(
                mismatches,
                f"H-01..H-04 ipc={ipc} mismatches:\n" + "\n".join(mismatches))

        self.log.info(
            "Test passed: prefix-mapped certs are unaffected by IPC, and "
            "with IPC on the internal cert neither adds nor removes access")

    def test_ipc_node_addition_credential_requirement(self):
        """
        Adding a node under client certificate auth: the joining node
        decides whether the cluster's internal certificate is enough, so
        with IPC on there the addition needs that node's admin credentials.

        Steps:
            1. Joinee with IPC off: a certificate-only addition succeeds, as
               before. The node is then ejected.
            2. The same node re-provisioned with IPC on and its own admin
               password: a certificate-only addition is refused, telling
               the caller to supply credentials.
            3. The cluster's password, which is not the joinee's: refused,
               telling the caller to verify the credentials -- a different
               message from step 2.
            4. A read-only account on the joinee: refused.
            5. The joinee's own admin credentials via addNodeV2 with a
               service list: added, rebalanced in, active with those
               services.
            6. Graceful failover with delta recovery, then hard failover
               with full recovery: both rebalance back with no credentials.
            7. Node-initiated join against the cluster with IPC on: without
               credentials it is refused with the cluster-side wording;
               with them it joins.
        """
        master = self.cluster.master
        spare, joiner = self.cluster.servers[1], self.cluster.servers[2]
        admin, admin_pw = master.rest_username, master.rest_password
        cas = [(self.ca_cert, self.ca_key)]
        otp = self.otp_of(spare)

        self.deploy_test_node_certs(master, self.ca_cert, self.ca_key)
        # Joinees run 'mandatory' (prepare_joinee). The master stays at
        # 'enable' so the framework's certificate-less HTTPS (rebalance,
        # failover) keeps working; IPC treats a presented certificate the
        # same under both.
        self._set_client_cert_auth_http("enable")
        self.set_ipc_setting(True)
        try:
            self.log.info("Step 1: joinee IPC off, certificate only")
            self.prepare_joinee(spare, ipc=False, trusted_cas=cas)
            resp = IPCUtils.add_node(master, spare.ip, client_cert_auth=True)
            self.assert_status(resp, 200, "E-01 cert-only add, IPC off")
            self.assert_status(IPCUtils.eject_node(master, otp), 200,
                               "eject after E-01")
            self.wait_until_uninitialized(spare)

            self.log.info("Step 2: joinee re-provisioned with IPC on")
            joinee = self.prepare_joinee(spare, ipc=True, trusted_cas=cas)
            resp = IPCUtils.add_node(master, spare.ip, client_cert_auth=True)
            self.assert_status(resp, 400, "E-03 cert-only add, IPC on")
            self.assertIn("supply a username and password as well",
                          resp.text, "E-03 message")

            self.log.info("Step 3: the cluster's password, not the joinee's")
            resp = IPCUtils.add_node(master, spare.ip, user=admin,
                                     password=admin_pw, client_cert_auth=True)
            self.assert_status(resp, 400, "E-06/E-18 wrong password")
            self.assertIn("verify username and password", resp.text,
                          "E-06 message")

            self.log.info("Step 4: a read-only account on the joinee")
            self.assert_status(
                IPCUtils.create_local_user(joinee, "ipc_e_ro",
                                           "Couchbase@1234", "ro_admin",
                                           auth=self._auth_of(joinee)),
                200, "create ro_admin on the joinee")
            resp = IPCUtils.add_node(master, spare.ip, user="ipc_e_ro",
                                     password="Couchbase@1234",
                                     client_cert_auth=True)
            self.assert_status(resp, 400, "E-07 ro_admin")
            self.assertIn("Got HTTP status 403 from REST call", resp.text,
                          "E-07 message")
            self.assertIn("engageCluster2", resp.text, "E-07 message")

            self.log.info("Step 5: joinee's own admin, addNodeV2 + services")
            self.bucket_util.create_default_bucket(self.cluster,
                                                   ram_quota=100, replica=1)
            resp = IPCUtils.add_node(master, spare.ip, user=admin,
                                     password=self.SPARE_PASSWORD,
                                     client_cert_auth=True, services="kv",
                                     v2=True)
            self.assert_status(resp, 200, "E-02/E-04 add with credentials")
            self.rebalance_cluster()
            self.assert_node_active(spare, services=["kv"])

            self.log.info("Step 6: failover and recovery need no credentials")
            for graceful, recovery in ((True, "delta"), (False, "full")):
                self.assertTrue(self.rest.fail_over(otp, graceful=graceful),
                                f"E-16 failover graceful={graceful}")
                if graceful:
                    self.assertTrue(self.rest.monitorRebalance(),
                                    "E-16 graceful failover")
                self.assertTrue(self.rest.set_recovery_type(otp, recovery),
                                f"E-16 set {recovery} recovery")
                self.rebalance_cluster()
                self.assert_node_active(spare)

            self.log.info("Step 7: node-initiated join")
            self.prepare_joinee(joiner, ipc=True, trusted_cas=cas,
                                initialize=False)
            resp = IPCUtils.do_join_cluster(joiner, master.ip,
                                            client_cert_auth=True)
            self.assert_status(resp, 400, "E-05 join without credentials")
            self.assertIn("If the cluster does not accept the internal "
                          "client certificate on its own", resp.text,
                          "E-05 message")
            resp = IPCUtils.do_join_cluster(joiner, master.ip, user=admin,
                                            password=admin_pw,
                                            client_cert_auth=True)
            self.assert_status(resp, 200, "E-05 join with credentials")
            self.rebalance_cluster()
            self.assert_node_active(joiner)
        finally:
            self.reset_spares([spare, joiner])
            self.crl_utils.regenerate_cluster_certificate_when_healthy(
                self.rest, log=self.log)

        self.log.info(
            "Test passed: with IPC on, adding a node needs that node's admin "
            "credentials; failover recovery does not")

    def test_ipc_node_addition_cross_ca_and_join_options(self):
        """
        Certificate trust and parameter handling around node addition, with
        IPC on.

        Steps:
            1. A joinee whose node certificate comes from a CA the cluster
               does not trust is refused, naming the cluster's CAs; once
               the cluster trusts that CA the same addition succeeds.
            2. newNodeHostname is rejected by addNode as an unknown
               parameter, and accepted by a node-initiated join.
        """
        master = self.cluster.master
        spare, joiner = self.cluster.servers[1], self.cluster.servers[2]
        admin, admin_pw = master.rest_username, master.rest_password
        ca1 = (self.ca_cert, self.ca_key)
        ca2 = self.crl_utils.generate_ca("IPCSecondCA")

        self.deploy_test_node_certs(master, self.ca_cert, self.ca_key)
        self._set_client_cert_auth_http("enable")
        self.set_ipc_setting(True)
        try:
            self.log.info("Step 1: joinee certified by an untrusted CA")
            self.prepare_joinee(spare, ipc=True, trusted_cas=[ca1, ca2],
                                issuer=ca2)
            add = dict(user=admin, password=self.SPARE_PASSWORD,
                       client_cert_auth=True)
            resp = IPCUtils.add_node(master, spare.ip, **add)
            self.assert_status(resp, 400, "E-12 untrusted CA")
            self.assertIn("issued by unknown CA", resp.text, "E-12 message")
            self.assertIn("CAs available on the cluster", resp.text,
                          "E-12 message names the cluster side")
            self._trust_ca_on_cluster(ca2[0])
            resp = IPCUtils.add_node(master, spare.ip, **add)
            self.assert_status(resp, 200, "E-12 after trusting the CA")
            self.assert_status(
                IPCUtils.eject_node(master, self.otp_of(spare)), 200,
                "eject after E-12")

            self.log.info("Step 2: newNodeHostname")
            resp = IPCUtils.add_node(master, joiner.ip, user=admin,
                                     password=admin_pw,
                                     extra={"newNodeHostname": joiner.ip})
            self.assert_status(resp, 400, "E-15 addNode")
            self.assertIn("Got unknown parameters", resp.text,
                          "E-15 addNode message")
            self.prepare_joinee(joiner, ipc=True, trusted_cas=[ca1],
                                initialize=False)
            resp = IPCUtils.do_join_cluster(
                joiner, master.ip, user=admin, password=admin_pw,
                client_cert_auth=True, new_node_hostname=joiner.ip)
            self.assert_status(resp, 200, "E-15 doJoinCluster")
        finally:
            self.reset_spares([spare, joiner])
            self.crl_utils.regenerate_cluster_certificate_when_healthy(
                self.rest, log=self.log)

        self.log.info(
            "Test passed: CA trust and join parameters behave as designed "
            "with IPC on")

    def test_ipc_internal_password_rotation_enforcement(self):
        """
        With IPC on, the internal password -- not the certificate -- proves
        an internal identity, so rotation sits on the security boundary.

        Steps:
            1. Internal cert plus '@internal' and the current password is
               accepted, a wrong password refused, the cert alone refused.
            2. While an on-demand rotation runs, the pre-rotation password
               keeps working; once it completes that password is refused,
               the new one accepted, and one password remains.
            3. A rotation interval below the minimum is rejected.
            4. At the minimum interval a rotation happens on its own, and a
               client using the current password sees no failure.
            5. Toggling IPC just before and after a rotation leaves no window
               where the cert alone is accepted or credentials refused.
            6. With protection sleep 0 the old password is dropped at once;
               with 60s it stays valid until the rotation completes.
            7. No current password appears in REST responses or log files.
            8. Killing the rotation process does not fall back to the
               certificate; it is restarted and the passwords survive.
        """
        master = self.cluster.master
        admin = (master.rest_username, master.rest_password)
        self._set_client_cert_auth_http("enable")
        self.set_ipc_setting(True)
        cert, key = self.generate_internal_client_cert(self.ca_cert,
                                                       self.ca_key)

        def status(password):
            return self.internal_auth_status(cert, key, password)

        self.log.info("Step 1: password proves the internal identity")
        current = self.internal_passwords()[0]
        self.assertEqual(status(current), 200, "C-05 current password")
        self.assertEqual(status("not-the-password"), 401, "C-06 wrong")
        self.assert_cert_enforcement([master], cert, key, True, "baseline")

        self.log.info("Step 2: old password valid during the rotation")
        old = current
        samples = []
        stop = threading.Event()

        def keep_using_old():
            while not stop.is_set():
                samples.append((time.time(), status(old)))
                time.sleep(0.5)

        worker = threading.Thread(target=keep_using_old)
        worker.start()
        try:
            resp = IPCUtils.rotate_internal_credentials(master)
            finished = time.time()
        finally:
            stop.set()
            worker.join(30)
        self.assert_status(resp, 200, "F-03 on-demand rotation")
        during = [code for ts, code in samples if ts < finished - 2]
        self.log.info(f"F-03 old password during rotation: {during}")
        self.assertTrue(during and all(c == 200 for c in during),
                        f"F-03: old password refused mid-rotation: {during}")
        passwords = self.internal_passwords()
        self.assertEqual(len(passwords), 1, f"F-04 list: {len(passwords)}")
        self.assertNotEqual(passwords[0], old, "F-04 password not rotated")
        self.assertEqual(status(old), 401, "F-04 old password after rotation")
        self.assertEqual(status(passwords[0]), 200, "F-04 new password")

        self.log.info("Step 3: interval below the minimum is rejected")
        _, content = self.rest.get_security_settings()
        interval = json.loads(content)["intCredsRotationInterval"]
        resp = IPCUtils.set_rotation_interval(master, 10000)
        self.assert_status(resp, 400, "F-02 interval 10000")
        self.assertIn("must be between 30000 and 31536000000", resp.text,
                      "F-02 message")
        _, content = self.rest.get_security_settings()
        self.assertEqual(json.loads(content)["intCredsRotationInterval"],
                         interval, "F-02 interval changed by a rejected "
                                   "write")

        self.log.info("Step 4: automatic rotation at the minimum interval")
        before = self.internal_passwords()[0]
        self.set_rotation_interval(30000)
        failures, rotated = [], False
        deadline = time.time() + 180
        while time.time() < deadline:
            head = self.internal_passwords()[0]
            code = status(head)
            if code != 200:
                failures.append(code)
            if head != before:
                rotated = True
                break
            time.sleep(3)
        self.set_rotation_interval(interval)
        self.assertTrue(rotated, "F-01/F-02 no automatic rotation in 180s")
        self.assertFalse(failures,
                         f"F-02 current password refused: {failures}")

        self.log.info("Step 5: IPC toggled around a rotation")
        checks = []

        def sample(when):
            checks.append((
                when,
                self.mtls_request(cert, key).status_code,
                self.mtls_request(cert, key, auth=admin).status_code,
                status(self.internal_passwords()[0])))

        self.set_ipc_setting(False)
        self.set_ipc_setting(True)
        rotation = threading.Thread(
            target=IPCUtils.rotate_internal_credentials, args=(master,))
        rotation.start()
        while rotation.is_alive():
            sample("during rotation")
            time.sleep(3)
        rotation.join()
        self.set_ipc_setting(False)
        self.set_ipc_setting(True)
        sample("after toggle")
        self.log.info(f"F-08 samples: {checks}")
        bad = [c for c in checks if c[1:] != (401, 200, 200)]
        self.assertFalse(bad, f"F-08: unexpected (cert alone, cert + "
                              f"admin, @internal) codes: {bad}")

        self.log.info("Step 6: protection sleep extremes")
        self.set_protection_sleep(0)
        old = self.internal_passwords()[0]
        self.assert_status(IPCUtils.rotate_internal_credentials(master), 200,
                           "F-09 rotation, sleep 0")
        self.assertEqual(len(self.internal_passwords()), 1,
                         "F-09 sleep 0: old password not dropped at once")
        self.assertEqual(status(old), 401, "F-09 sleep 0: old password")

        self.set_protection_sleep(60000)
        old = self.internal_passwords()[0]
        rotation = threading.Thread(
            target=IPCUtils.rotate_internal_credentials, args=(master,))
        rotation.start()
        while_running = []
        while rotation.is_alive():
            while_running.append(status(old))
            time.sleep(5)
        rotation.join()
        self.log.info(f"F-09 sleep 60s, old password: {while_running}")
        self.assertIn(200, while_running,
                      "F-09 sleep 60s: old password never accepted")
        self.assertEqual(status(old), 401,
                         "F-09 sleep 60s: old password after rotation")

        self.log.info("Step 7: the password does not leak")
        passwords = self.internal_passwords()
        for path in ("/settings/security", "/internalSettings",
                     "/pools/default", "/nodes/self",
                     "/settings/clientCertAuth"):
            body = self.https_request(path, auth=admin).text
            for password in passwords:
                self.assertNotIn(password, body, f"F-10 leak in {path}")
        shell = RemoteMachineShellConnection(master)
        try:
            logs_dir = "/opt/couchbase/var/lib/couchbase/logs"
            for password in passwords:
                out, _ = shell.execute_command(
                    f"grep -rlF -- '{password}' {logs_dir} || true")
                self.assertFalse([line for line in out if line.strip()],
                                 f"F-10 password found in logs: {out}")
        finally:
            shell.disconnect()

        self.log.info("Step 8: killing the rotation process fails closed")
        head = self.internal_passwords()[0]
        pid = self.creds_rotation_pid()
        self.kill_creds_rotation()
        self.assert_cert_enforcement([master], cert, key, True,
                                     "G-08 after kill")
        self.assertEqual(self.mtls_request(cert, key, auth=admin).status_code,
                         200, "G-08 cert + admin after kill")
        deadline = time.time() + 60
        restarted = pid
        while time.time() < deadline:
            restarted = self.creds_rotation_pid()
            if restarted not in (pid, "undefined"):
                break
            time.sleep(2)
        self.assertNotIn(restarted, (pid, "undefined"),
                         "G-08 cb_creds_rotation not restarted")
        self.assertEqual(self.internal_passwords()[0], head,
                         "G-08 password list changed by the kill")
        self.assertEqual(status(head), 200, "G-08 @internal after restart")

        self.log.info(
            "Test passed: rotation keeps the internal password the only "
            "proof of an internal identity, with no window and no fallback")

    def test_ipc_rotation_across_node_outage_and_rebalance(self):
        """
        Rotation across the cluster.

        Steps:
            1. With one node down, the other two rotate. When it returns it
               holds both peers' new passwords with no manual step, and every
               node accepts its own '@' identity with its current password
               while refusing the cert alone.
            2. A rotation on the master during a rebalance does not disturb
               the rebalance, and auth still works on the remaining nodes.
        """
        nodes = self.cluster.servers[:self.nodes_init]
        self.assertGreaterEqual(len(nodes), 3, "Needs a 3-node cluster")
        master, second, third = nodes[0], nodes[1], nodes[2]
        self._set_client_cert_auth_http("enable")
        self.set_ipc_setting(True)
        self.set_protection_sleep(0)
        cert, key = self.generate_internal_client_cert(self.ca_cert,
                                                       self.ca_key)

        self.log.info("Step 1: rotate while a node is down")
        shell = RemoteMachineShellConnection(third)
        try:
            shell.stop_couchbase()
            before = {s.ip: self.internal_passwords(s) for s in (master,
                                                                 second)}
            for server in (master, second):
                self.assert_status(
                    IPCUtils.rotate_internal_credentials(server), 200,
                    f"F-05 rotation on {server.ip}")
            shell.start_couchbase()
        finally:
            shell.disconnect()
        self.assertTrue(RestConnection(third).is_ns_server_running(300),
                        f"{third.ip} did not come back")
        for peer in (master, second):
            own = self.internal_passwords(peer)[0]
            self.assertNotIn(own, before[peer.ip],
                             f"F-05 {peer.ip} did not rotate")
            deadline = time.time() + 120
            while self.peer_internal_password(third, peer) != own:
                if time.time() > deadline:
                    self.fail(f"F-05 {third.ip} never saw {peer.ip}'s new "
                              f"password")
                time.sleep(3)
        for server in nodes:
            own = self.internal_passwords(server)[0]
            self.assertEqual(
                self.internal_auth_status(cert, key, own, server=server),
                200, f"F-05 @internal on {server.ip}")
        self.assert_cert_enforcement(nodes, cert, key, True, "F-05")

        self.log.info("Step 2: rotate during a rebalance")
        self.rest.rebalance(
            otpNodes=[node.id for node in self.rest.node_statuses()],
            ejectedNodes=[self.otp_of(third)])
        self.assert_status(IPCUtils.rotate_internal_credentials(master), 200,
                           "F-06 rotation during rebalance")
        self.assertTrue(self.rest.monitorRebalance(),
                        "F-06 rebalance failed")
        for server in (master, second):
            own = self.internal_passwords(server)[0]
            self.assertEqual(
                self.internal_auth_status(cert, key, own, server=server),
                200, f"F-06 @internal on {server.ip}")
        self.assert_cert_enforcement([master, second], cert, key, True,
                                     "F-06")

        self.log.info(
            "Test passed: rotation propagates across outages and rebalances "
            "with enforcement intact")

    def test_ipc_runtime_toggle_dynamics(self):
        """
        The setting is applied at runtime, under load, repeatedly, and
        during a failover.

        Steps:
            1. While requests run continuously, IPC is toggled off, on, off
               and on. Requests with credentials never fail; certificate-only
               requests follow the setting within a short grace period after
               each flip, with no restart.
            2. Twenty rapid toggles leave ns_server's process count and
               memory flat, and the setting still applied and enforced on
               every node.
            3. A toggle during a hard failover is applied consistently on
               every node, the failed-over one included, and survives full
               recovery and rebalance.
        """
        nodes = self.cluster.servers[:self.nodes_init]
        self.assertGreaterEqual(len(nodes), 3, "Needs a 3-node cluster")
        master, third = nodes[0], nodes[2]
        admin = (master.rest_username, master.rest_password)
        self._set_client_cert_auth_http("enable")
        cert, key = self.generate_internal_client_cert(self.ca_cert,
                                                       self.ca_key)

        self.log.info("Step 1: toggles under continuous load")
        self.set_ipc_setting(False)
        # Each flip is (started, finished, value): the value takes effect
        # somewhere inside that window, not at either end.
        samples, flips = [], [(0, 0, False)]
        stop = threading.Event()

        def load():
            # A request error is recorded, not raised: an exception would
            # end this thread silently and leave too few samples to judge.
            while not stop.is_set():
                sent = time.time()
                codes = []
                for auth in (None, admin):
                    try:
                        codes.append(self.mtls_request(
                            cert, key, auth=auth).status_code)
                    except requests.exceptions.RequestException as exc:
                        codes.append(f"error: {exc}")
                samples.append((sent, *codes))
                time.sleep(0.3)

        worker = threading.Thread(target=load)
        worker.start()
        try:
            for ipc in (True, False, True):
                time.sleep(8)
                started = time.time()
                self.set_ipc_setting(ipc)
                flips.append((started, time.time(), ipc))
            time.sleep(8)
        finally:
            stop.set()
            worker.join(60)

        mismatched, credentialed, judged = [], [], set()
        for sent, cert_only, with_creds in samples:
            if with_creds != 200:
                credentialed.append((sent, with_creds))
            if any(start - 1 <= sent <= end + 2
                   for start, end, _ in flips[1:]):
                continue
            state = [ipc for _, end, ipc in flips if end <= sent][-1]
            judged.add(state)
            if cert_only != (401 if state else 200):
                mismatched.append((sent, state, cert_only))
        self.log.info(f"K-01/K-02: {len(samples)} samples, flips {flips}")
        self.assertEqual(judged, {False, True},
                         f"K-01/K-02: too few samples outside the toggle "
                         f"windows to judge both states: {samples}")
        self.assertFalse(credentialed,
                         f"K-01: credentialed requests failed: "
                         f"{credentialed}")
        self.assertFalse(mismatched,
                         f"K-01/K-02: cert-only requests out of step with "
                         f"the setting: {mismatched}")

        self.log.info("Step 2: twenty rapid toggles")
        procs_before, mem_before = self.beam_stats()
        for i in range(20):
            self.set_ipc_setting(i % 2 == 0)
        self.set_ipc_setting(True)
        procs_after, mem_after = self.beam_stats()
        self.log.info(f"K-03 processes {procs_before} -> {procs_after}, "
                      f"memory {mem_before} -> {mem_after}")
        self.assertLess(procs_after - procs_before, 50,
                        "K-03: process count grew")
        self.assertLess(mem_after - mem_before, 50 * 1024 * 1024,
                        "K-03: memory grew by more than 50 MiB")
        self.wait_for_ipc_value(nodes, True)
        self.assert_cert_enforcement(nodes, cert, key, True, "K-03")

        self.log.info("Step 3: toggle during a hard failover")
        otp = self.otp_of(third)
        # Both calls are released together, so the toggle reaches the
        # server while the failover (a few seconds) is still running.
        go = threading.Barrier(2)
        finished, toggle = {}, {}

        def run(name, fn, *args, **kwargs):
            go.wait()
            result = fn(*args, **kwargs)
            finished[name] = time.time()
            if name == "toggle":
                toggle["status"] = result.status_code

        # The toggle is the bare POST: timing set_ipc_setting would include
        # its confirming GET, which can land after the failover even when
        # the change itself did not.

        threads = [
            threading.Thread(target=run, args=("failover",
                                               self.rest.fail_over, otp),
                             kwargs={"graceful": False}),
            threading.Thread(target=run, args=(
                "toggle", IPCUtils.post_internal_settings, master,
                {self.IPC_SETTING: "false"})),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(300)
        self.log.info(f"K-05 finish times: {finished}")
        self.assertEqual(toggle.get("status"), 200,
                         "K-05 toggle during the failover")
        self.assertLess(finished.get("toggle", float("inf")),
                        finished.get("failover", 0),
                        "K-05: the toggle did not complete while the "
                        "failover was still running")
        self.wait_for_ipc_value(nodes, False)
        self.assert_cert_enforcement(nodes, cert, key, False,
                                     "K-05 after failover")
        self.assertTrue(self.rest.set_recovery_type(otp, "full"),
                        "K-05 set full recovery")
        self.rebalance_cluster()
        for server in nodes:
            self.assert_node_active(server)
        self.wait_for_ipc_value(nodes, False)
        self.assert_cert_enforcement(nodes, cert, key, False,
                                     "K-05 after recovery")

        self.log.info(
            "Test passed: the setting applies at runtime under load, "
            "without leaks, and consistently through a failover")

    def test_ipc_audit_and_auth_metrics(self):
        """
        What operators can observe when IPC turns a certificate away.

        Steps:
            1. Authentication counters: with IPC off, certificate-only
               requests count as internal-certificate successes; with it on
               they count as anonymous, never as internal-certificate
               successes, and each refusal counts as a REST auth failure.
            2. Audit: a forged internal certificate refused with IPC on
               emits an 'authentication failure' record with the request's
               URL and real addresses; a password failure, for contrast,
               also names the user it claimed to be.
            3. The setting is captured in /diag, which support bundles
               collect.
        """
        master = self.cluster.master
        admin = (master.rest_username, master.rest_password)
        self._set_client_cert_auth_http("enable")
        cert, key = self.generate_internal_client_cert(self.ca_cert,
                                                       self.ca_key)
        forged, forged_key = self.generate_forged_internal_cert()
        auths = "cm_authentications_total"
        failures = "cm_rest_request_auth_failure_total"

        self.log.info("Step 1: authentication counters")
        for ipc in (False, True):
            self.set_ipc_setting(ipc)
            before = (self.metric(auths, type="client_cert_int", res="succ"),
                      self.metric(auths, type="anon", res="succ"),
                      self.metric(failures))
            for _ in range(3):
                self.mtls_request(cert, key)
            after = (self.metric(auths, type="client_cert_int", res="succ"),
                     self.metric(auths, type="anon", res="succ"),
                     self.metric(failures))
            cert_int, anon, failed = (a - b for a, b in zip(after, before))
            self.log.info(f"L-06 ipc={ipc}: client_cert_int +{cert_int}, "
                          f"anon +{anon}, auth failures +{failed}")
            if ipc:
                self.assertEqual(cert_int, 0,
                                 "L-06: cert-only counted as an internal-"
                                 "certificate success with IPC on")
                self.assertGreaterEqual(anon, 3, "L-06 anon with IPC on")
                self.assertGreaterEqual(failed, 3,
                                        "L-06 auth failures with IPC on")
            else:
                self.assertGreaterEqual(cert_int, 3,
                                        "L-06 client_cert_int with IPC off")

        self.log.info("Step 2: audit of refused requests")
        audit_was_on = self.rest.getAuditSettings().get("auditdEnabled")
        self.assertIs(self.rest.setAuditSettings(enabled="true"), True,
                      "Failed to enable audit")
        try:
            baseline = self.latest_audit_event(self.AUTH_FAILURE_AUDIT_ID)
            self.assert_status(self.mtls_request(forged, forged_key), 401,
                               "L-02 forged cert")
            event = self.wait_for_audit_event(
                self.AUTH_FAILURE_AUDIT_ID, baseline,
                lambda e: e.get("raw_url") == "/pools/default",
                "forged certificate on /pools/default")
            self.log.info(f"L-02 forged cert audit: {event}")
            self.assertEqual(event.get("local", {}).get("ip"), master.ip,
                             f"L-02 local address: {event}")
            self.assertTrue(event.get("remote", {}).get("ip"),
                            f"L-02 remote address missing: {event}")

            baseline = event
            self.assert_status(
                self.https_request(auth=("ipcnosuchuser", "wrong")), 401,
                "L-02 password failure")
            event = self.wait_for_audit_event(
                self.AUTH_FAILURE_AUDIT_ID, baseline,
                lambda e: e.get("real_userid", {}).get("user") ==
                "ipcnosuchuser",
                "password failure naming the user")
            self.log.info(f"L-02 password failure audit: {event}")
        finally:
            self.rest.setAuditSettings(
                enabled="true" if audit_was_on else "false")

        self.log.info("Step 3: the setting is in /diag")
        diag = self.https_request("/diag", auth=admin, timeout=300)
        self.assert_status(diag, 200, "L-07 GET /diag")
        self.assertIn(self.IPC_CONFIG_KEY, diag.text,
                      "L-07: setting missing from /diag")

        self.log.info(
            "Test passed: refusals are counted, audited and the setting is "
            "visible to support")

    def test_ipc_enforcement_across_tls_n2n_and_restarts(self):
        """
        Enforcement holds through changes to how the cluster is secured and
        through process and cluster restarts, on every node.

        Steps:
            1. Node-to-node encryption at 'control', then 'all', then off.
            2. Minimum TLS version 1.3, then 1.2, then a restricted cipher
               list; the original settings are restored afterwards.
            3. ns_server and memcached on one node killed with SIGKILL;
               the node comes back on its own.
            4. A full cluster restart; the setting and clientCertAuth
               survive it.
            5. Prometheus stats are still collected, so the '@prometheus'
               identity still authenticates with IPC on.
        """
        nodes = self.cluster.servers[:self.nodes_init]
        self.assertGreaterEqual(len(nodes), 3, "Needs a 3-node cluster")
        master, second = nodes[0], nodes[1]
        self._set_client_cert_auth_http("enable")
        self.set_ipc_setting(True)
        cert, key = self.generate_internal_client_cert(self.ca_cert,
                                                       self.ca_key)
        self.assert_enforced_everywhere(nodes, cert, key, "baseline")

        self.log.info("Step 1: node-to-node encryption levels")
        try:
            for level in ("control", "all"):
                self.set_n2n_encryption(nodes, level)
                self.assert_enforced_everywhere(nodes, cert, key,
                                                f"P-04 n2n {level}")
        finally:
            self.set_n2n_encryption(nodes, "off")
        self.assert_enforced_everywhere(nodes, cert, key, "P-04 n2n off")

        self.log.info("Step 2: TLS versions and cipher suites")
        _, content = self.rest.get_security_settings()
        original = json.loads(content)
        ciphers = ["TLS_AES_256_GCM_SHA384",
                   "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"]
        try:
            for version in ("tlsv1.3", "tlsv1.2"):
                self.assert_status(
                    IPCUtils.set_security_settings(
                        master, {"tlsMinVersion": version}),
                    200, f"P-06 tlsMinVersion={version}")
                self.assert_enforced_everywhere(nodes, cert, key,
                                                f"P-06 {version}")
            self.assert_status(
                IPCUtils.set_security_settings(
                    master, {"cipherSuites": json.dumps(ciphers)}),
                200, "P-06 restricted cipher suites")
            _, content = self.rest.get_security_settings()
            self.assertEqual(json.loads(content).get("cipherSuites"),
                             ciphers, "P-06 cipher list not applied")
            self.assert_enforced_everywhere(nodes, cert, key,
                                            "P-06 restricted ciphers")
        finally:
            IPCUtils.set_security_settings(master, {
                "tlsMinVersion": original.get("tlsMinVersion", "tlsv1.2"),
                "cipherSuites": json.dumps(original.get("cipherSuites",
                                                        []))})
        self.assert_enforced_everywhere(nodes, cert, key,
                                        "P-06 settings restored")

        self.log.info("Step 3: SIGKILL ns_server and memcached on one node")
        # Not shell.kill_erlang(): it waits for no beam.smp to remain, but
        # systemd restarts the service at once, so it reports a failure.
        # New PIDs afterwards prove the kill instead.
        shell = RemoteMachineShellConnection(second)
        pids = "pgrep -x beam.smp; pgrep -x memcached"
        try:
            before, _ = shell.execute_command(pids)
            shell.execute_command("pkill -9 -x beam.smp; "
                                  "pkill -9 -x memcached")
            self.assertTrue(
                RestConnection(second).is_ns_server_running(300),
                f"P-07 {second.ip} did not come back")
            after, _ = shell.execute_command(pids)
        finally:
            shell.disconnect()
        before = {p.strip() for p in before if p.strip()}
        after = {p.strip() for p in after if p.strip()}
        self.log.info(f"P-07 pids before {sorted(before)} after "
                      f"{sorted(after)}")
        self.assertTrue(before and after and not before & after,
                        f"P-07: processes were not all restarted: "
                        f"{before} -> {after}")
        self.assert_node_active(second, timeout=300)
        self.assert_enforced_everywhere(nodes, cert, key, "P-07")

        self.log.info("Step 4: full cluster restart")
        self.restart_nodes(nodes)
        for server in nodes:
            self.assert_node_active(server, timeout=300)
        self.wait_for_ipc_value(nodes, True)
        cca = IPCUtils.get_client_cert_auth(master).json()
        self.log.info(f"P-08 clientCertAuth after restart: {cca}")
        self.assertEqual(cca.get("state"), "enable",
                         "P-08 clientCertAuth lost on restart")
        self.assert_enforced_everywhere(nodes, cert, key, "P-08")

        self.log.info("Step 5: Prometheus stats still collected")
        deadline = time.time() + 120
        fresh = False
        while time.time() < deadline and not fresh:
            resp = self.https_request(
                "/pools/default/stats/range/sys_cpu_utilization_rate"
                "?start=-60", auth=(master.rest_username,
                                    master.rest_password))
            if resp.status_code == 200:
                fresh = any(series.get("values")
                            for series in resp.json().get("data", []))
            if not fresh:
                time.sleep(10)
        self.assertTrue(fresh, "G-10: no stats datapoints in the last 60s")

        self.log.info(
            "Test passed: enforcement holds on every node across n2n "
            "levels, TLS settings, a hard kill and a full restart")
