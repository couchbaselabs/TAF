import base64
import threading

import requests

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
