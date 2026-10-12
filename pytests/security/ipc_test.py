import base64

import requests

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
