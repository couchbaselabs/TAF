import json
import shlex

import requests

from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
from pytests.onPrem_basetestcase import ClusterSetup
from TestInput import TestInputSingleton


class IPCBase(ClusterSetup):
    """
    Base class for Internal-identity Password Check (IPC) tests -- MB-73874.

    The feature under test: a Couchbase node recognises its own internal client
    certificate purely by a SAN rfc822Name of the form
    <name>@internal.couchbase.com, and historically accepted that certificate as
    proof of identity on its own, granting full administrator rights with no
    password and no RBAC evaluation. Any certificate carrying that SAN, signed
    by ANY CA the cluster trusts, was therefore a cluster super-admin -- which
    is the bypass a customer demonstrated by adding the reserved SAN email to an
    unrelated certificate that merely shared their corporate root CA.

    The cluster setting 'internalIdentityPasswordCheckUnderMtls' (ns_config key
    internal_identity_password_check_under_mtls, served from /internalSettings)
    removes that: the certificate is still presented and chain-validated as part
    of the TLS connection, but it no longer establishes identity. The request
    must carry its own credentials, and the identity comes from those -- exactly
    as if no certificate had been presented.

    The default is NOT stored in config; it is derived at read time from
    cluster_compat_mode:is_cluster_85(), so it reports false on an 8.0 cluster
    and true once compat reaches 8.5. An explicitly set value always wins. This
    base class therefore always reads the current value in setUp and restores it
    in tearDown rather than assuming either state.
    """

    # The reserved domain ns_server matches on. Deliberately a constant here
    # rather than a literal at each call site -- a typo in it would make an
    # "exploit blocked" assertion pass for entirely the wrong reason (the cert
    # simply would not be an internal cert at all).
    INTERNAL_CERT_DOMAIN = "internal.couchbase.com"

    IPC_SETTING = "internalIdentityPasswordCheckUnderMtls"

    MGMT_SSL_PORT = 18091
    KV_SSL_PORT = 11207

    def setUp(self):
        self._self_heal_stuck_client_cert_auth()
        super().setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)
        self.cluster_rest = ClusterRestAPI(self.cluster.master)

        self._require_ipc_supported()
        self._self_heal_stuck_trusted_cas()

        # RBAC users created during a test -- cleaned up in tearDown. Trusted
        # CAs and temp PEM files are tracked on self.crl_utils itself.
        self._rbac_users = []

        # Remember what the cluster reported before we touched anything, so
        # tearDown can put it back whichever way the test drove it.
        self._original_ipc_state = self.get_ipc_setting()
        self.log.info(f"{self.IPC_SETTING} at setUp: {self._original_ipc_state}")

        # The cluster's own trusted CA for this run. Tests that need to prove
        # the cluster boundary generate a second, unrelated CA themselves.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("IPCTestCA")
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        try:
            self._restore_ipc_setting()
        except Exception as exc:
            self.log.warning(f"{self.IPC_SETTING} restore error: {exc}")
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
        try:
            self._cleanup_rbac_users()
        except Exception as exc:
            self.log.warning(f"RBAC user cleanup error: {exc}")
        try:
            self._cleanup_temp_pem_files()
        except Exception as exc:
            self.log.warning(f"Temp PEM file cleanup error: {exc}")
        try:
            self._cleanup_trusted_cas()
        except Exception as exc:
            self.log.warning(f"Trusted CA cleanup error: {exc}")
        finally:
            super().tearDown()

    # -- The setting under test ---------------------------------------------

    def get_ipc_setting(self):
        """
        Current value of internalIdentityPasswordCheckUnderMtls.

        Returns the server's own answer rather than a cached expectation: the
        default is version-derived, not written to config, so GET is the only
        honest source of truth for what the cluster is actually enforcing.
        """
        status, content = self.cluster_rest.set_internal_settings()
        if not status:
            self.fail(f"GET /internalSettings failed: {content}")
        if self.IPC_SETTING not in content:
            self.fail(
                f"/internalSettings has no '{self.IPC_SETTING}' key -- this "
                f"build predates MB-73874. Keys present: {sorted(content)}"
            )
        return content[self.IPC_SETTING]

    def set_ipc_setting(self, enabled):
        """Enable/disable the check and confirm the cluster agrees it took."""
        value = "true" if enabled else "false"
        status, content = self.cluster_rest.set_internal_settings(
            self.IPC_SETTING, value
        )
        if not status:
            self.fail(
                f"POST /internalSettings {self.IPC_SETTING}={value} failed: "
                f"{content}"
            )
        actual = self.get_ipc_setting()
        if actual != enabled:
            self.fail(
                f"Set {self.IPC_SETTING}={enabled} but the cluster reports "
                f"{actual} -- the setting did not take effect."
            )
        self.log.info(f"{self.IPC_SETTING} set to {enabled}")

    def _restore_ipc_setting(self):
        """
        Put the setting back only if a test actually changed it.

        Writing it back unconditionally would be worse than a no-op: an explicit
        write pins the key in config, and from then on the version-derived
        default no longer applies -- so a cluster reused by a later test would
        silently stop tracking its own compat version.
        """
        current = self.get_ipc_setting()
        if current != self._original_ipc_state:
            self.log.info(
                f"Restoring {self.IPC_SETTING} to {self._original_ipc_state}"
            )
            self.set_ipc_setting(self._original_ipc_state)

    # -- Certificate fixtures ------------------------------------------------

    def generate_internal_client_cert(self, ca_cert, ca_key, name="internal",
                                      cn=None):
        """
        A client certificate carrying the reserved internal SAN.

        Args:
            ca_cert, ca_key: the issuing CA. Pass the cluster's trusted CA for a
                genuine internal certificate, or an unrelated CA to forge one.
            name: local part of the SAN email, i.e. the '<name>' that ns_server
                maps to the identity '@<name>'.
            cn: subject CN. Defaults to `name`. The whole point of the forged
                case is that this can be anything at all -- ns_server keys off
                the SAN, never the subject.

        Returns:
            (cert_path, key_path) -- temp PEM files, tracked for teardown.
        """
        cert, key, _serial = self.crl_utils.generate_leaf_cert(
            ca_cert, ca_key, cn or name,
            email_names=[f"{name}@{self.INTERNAL_CERT_DOMAIN}"],
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        return cert_path, key_path

    def generate_forged_internal_cert(self, ca_cert=None, ca_key=None,
                                      cn="ads-dashboard.example.com"):
        """
        The customer's bypass, reproduced.

        A leaf sharing nothing with this cluster except a trust anchor -- an
        unrelated subject, issued for an unrelated purpose -- with the reserved
        internal SAN injected into it. That injection is the entire attack: the
        SAN value is identical in every Couchbase installation and is published
        in the documentation, so obtaining a certificate carrying it requires
        nothing more than asking a trusted CA for one extra SAN entry.

        Defaults to this cluster's trusted CA so the certificate reaches the
        application layer; pass a different CA to test the cluster boundary.
        """
        return self.generate_internal_client_cert(
            ca_cert if ca_cert is not None else self.ca_cert,
            ca_key if ca_key is not None else self.ca_key,
            name="internal", cn=cn,
        )

    # -- mTLS probes ---------------------------------------------------------

    def mtls_request(self, cert_path, key_path, path="/pools/default",
                     server=None, port=None, auth=None, timeout=30):
        """
        One HTTPS request presenting `cert_path`/`key_path`.

        Leave `auth` as None to present the certificate ALONE. That is the case
        the whole feature is about, and it only means anything if no
        Authorization header goes with it -- a request carrying Basic auth is
        authenticated by password, so it would report success regardless of what
        the certificate did or did not establish.

        Returns the raw requests.Response so the caller can assert on
        status_code. Raises requests.exceptions.SSLError only if the connection
        is refused at the TLS layer, which for these tests is a distinct (and
        usually unwanted) outcome from an application-layer 401.
        """
        server = server or self.cluster.master
        return self.crl_utils.perform_mtls_handshake(
            server.ip, port or self.MGMT_SSL_PORT, cert_path, key_path,
            path=path, timeout=timeout, auth=auth,
        )

    def whoami_via_mtls(self, cert_path, key_path, server=None, auth=None,
                        timeout=30):
        """
        Identity the server resolved for this certificate, via GET /whoami.

        Note /whoami does NOT reject an unidentified caller -- it answers 200
        with {"roles": [], "id": "", "domain": "anonymous"}. That is the signal
        to assert on when a certificate is expected to confer no identity;
        waiting for a non-2xx here would wait forever. Use
        assert_no_identity_from_cert rather than reading this directly.
        """
        server = server or self.cluster.master
        return self.crl_utils.get_identity_via_mtls(
            server.ip, self.MGMT_SSL_PORT, cert_path, key_path,
            timeout=timeout, auth=auth,
        )

    def assert_no_identity_from_cert(self, cert_path, key_path, server=None,
                                     path="/pools/default"):
        """
        Assert the certificate reaches the application layer and names nobody.

        Three things are checked, and all three matter:

        1. The TLS handshake SUCCEEDS. An SSLError would also make the request
           "fail", but for an entirely different reason -- the certificate is
           meant to stay perfectly valid for the connection and merely stop
           conferring an identity. A TLS-layer rejection would leave the actual
           behaviour under test unverified, so it is an explicit failure here.
        2. A permissioned endpoint returns 401 -- the caller got nothing.
        3. /whoami reports the anonymous identity. This is the positive form of
           the same fact and the more precise one: it shows the request was
           processed and resolved to no user, rather than merely being refused
           for some unrelated reason. Note /whoami answers 200 for an
           unidentified caller, so it is the body that carries the signal.
        """
        try:
            resp = self.mtls_request(cert_path, key_path, path=path,
                                     server=server)
        except requests.exceptions.SSLError as exc:
            self.fail(
                f"Certificate was rejected at the TLS layer ({exc}). Expected "
                f"the handshake to succeed and {path} to return 401 -- the "
                f"certificate is meant to stay valid for the connection and "
                f"merely stop conferring an identity."
            )
        self.assertEqual(
            resp.status_code, 401,
            f"Certificate alone should not authenticate against {path} when "
            f"{self.IPC_SETTING} is enabled, got HTTP {resp.status_code}: "
            f"{resp.text[:300]}"
        )

        identity = self.whoami_via_mtls(cert_path, key_path, server=server)
        self.assertEqual(
            identity.get("domain"), "anonymous",
            f"Certificate alone should resolve to the anonymous identity when "
            f"{self.IPC_SETTING} is enabled, got {identity!r}"
        )
        self.assertFalse(
            identity.get("roles"),
            f"Anonymous caller should hold no roles, got {identity!r}"
        )
        return resp

    # -- EE gating -----------------------------------------------------------

    def _require_ipc_supported(self):
        """Client certificate authentication is Enterprise-only, so the whole
        suite is."""
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail(
                "The internal identity password check requires an Enterprise "
                "Edition cluster."
            )

    # -- Self-healing preconditions ------------------------------------------

    def _self_heal_stuck_client_cert_auth(self):
        """
        Reset clientCertAuth to 'disable' if an aborted run left it 'mandatory'.

        Left mandatory, every later HTTPS call -- including the framework's own
        setUp -- fails the TLS handshake with "certificate required", so the
        suite cannot even reach the point of reporting a real failure. Uses the
        plain HTTP port to get underneath the TLS layer.

        Runs before super().setUp(), so self.cluster and self.log do not exist
        yet; uses TestInputSingleton and print(). Best-effort: a genuinely
        down node should surface during the real setUp, not here.
        """
        server = TestInputSingleton.input.servers[0]
        base_url = f"http://{server.ip}:8091"
        auth = (server.rest_username, server.rest_password)

        try:
            resp = requests.get(
                f"{base_url}/settings/clientCertAuth", auth=auth, timeout=30
            )
            resp.raise_for_status()

            if resp.json().get("state") == "mandatory":
                print(
                    f"[IPCBase] {server.ip} was stuck with "
                    f"clientCertAuth='mandatory'. Resetting to 'disable' via "
                    f"HTTP before setUp()."
                )
                reset = requests.post(
                    f"{base_url}/settings/clientCertAuth", auth=auth, timeout=30,
                    headers={"Content-Type": "application/json"},
                    json={"state": "disable", "prefixes": []},
                )
                reset.raise_for_status()
        except requests.exceptions.RequestException:
            pass

    def _self_heal_stuck_trusted_cas(self):
        """
        Untrust leftover CAs from a previous run before this test trusts its own.

        Tries to delete every CA rather than guessing which one is the node's
        own: CA ids are a plain counter, and a freshly provisioned node has
        already rotated past id 0 by the time node-init finishes. The server
        refuses to delete a CA that is actually in use by a node's current
        certificate, so attempting all of them removes exactly the orphans and
        leaves the real one alone. Best-effort: logs, never raises.
        """
        try:
            status, content = self.rest.get_trusted_CAs()
            if not status:
                raise RuntimeError(f"GET trustedCAs failed: {content}")
            removed = 0
            for entry in json.loads(content):
                ca_id = entry.get("id")
                try:
                    del_status, _, _ = self.rest.delete_trusted_CA(ca_id)
                    if del_status:
                        removed += 1
                except Exception as exc:
                    # One failed delete must not abandon the rest of the pass,
                    # or a single hiccup carries every other stale CA into the
                    # next test too.
                    self.log.warning(
                        f"Trusted CA self-heal: delete of id={ca_id} failed, "
                        f"continuing with the rest: {exc}"
                    )
            if removed > 0:
                self.log.warning(
                    f"{self.cluster.master.ip} had {removed} stale trusted "
                    f"CA(s) from a previous run -- untrusted them before this "
                    f"test starts."
                )
        except Exception as exc:
            self.log.warning(f"Trusted CA self-heal error: {exc}")

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            ca_dir = self.crl_utils._ca_dir(shell)
            # Quote the directory but leave the glob outside it -- the Windows
            # install path contains a space, and unquoted it word-splits into
            # two rm arguments that silently match nothing.
            shell.execute_command(f"rm -f {shlex.quote(ca_dir)}/*")
        except Exception as exc:
            self.log.warning(f"Trusted CA inbox/CA cleanup error: {exc}")
        finally:
            shell.disconnect()

    # -- Fixture helpers (thin wrappers over CRLUtils) -----------------------

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        self.crl_utils.trust_ca_on_cluster(
            self.rest, server or self.cluster.master, ca_cert
        )

    def _enable_client_cert_auth(self, state="enable", prefixes=None):
        self.crl_utils.enable_client_cert_auth(
            self.cluster.master, state=state, prefixes=prefixes
        )

    def _disable_client_cert_auth(self):
        self.crl_utils.disable_client_cert_auth(self.cluster.master)

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        return self.crl_utils.write_temp_pem(pem_bytes, suffix=suffix)

    def _cleanup_temp_pem_files(self):
        self.crl_utils.cleanup_temp_pem_files()

    def _cleanup_trusted_cas(self):
        self.crl_utils.cleanup_trusted_cas(self.rest)

    # -- RBAC helpers --------------------------------------------------------

    def _create_rbac_test_user(self, username, role, password="Couchbase@1234"):
        RbacUtils(self.cluster.master)._create_user_and_grant_role(
            username, role, password=password
        )
        self._rbac_users.append(username)
        return username, password

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning(
                    f"Failed to delete RBAC user {username}: {exc}"
                )
        self._rbac_users = []
