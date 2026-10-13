import copy
import json
import shlex
import time

import requests
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtendedKeyUsageOID

from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils, \
    get_audit_event
from couchbase_utils.security_utils.ipc_utils import IPCUtils
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
    IPC_CONFIG_KEY = "internal_identity_password_check_under_mtls"

    MGMT_SSL_PORT = 18091
    AUDIT_LOG_PATH = "/opt/couchbase/var/lib/couchbase/logs/current-audit.log"
    INTERNAL_SETTINGS_AUDIT_ID = 8228
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
        self._ipc_key_was_absent = not self._ipc_key_in_config()
        self.log.info(
            f"{self.IPC_SETTING} at setUp: {self._original_ipc_state} "
            f"({'derived' if self._ipc_key_was_absent else 'explicit'})"
        )

        # The cluster's own trusted CA for this run. Tests that need to prove
        # the cluster boundary generate a second, unrelated CA themselves.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca("IPCTestCA")
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        # clientCertAuth first: left 'mandatory', it blocks the HTTPS REST
        # call that restores the setting.
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
        try:
            self._restore_ipc_setting()
        except Exception as exc:
            self.log.warning(f"{self.IPC_SETTING} restore error: {exc}")
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

    def get_ipc_setting(self, server=None, auth=None):
        """
        Current value of internalIdentityPasswordCheckUnderMtls.

        Returns the server's own answer rather than a cached expectation: the
        default is version-derived, not written to config, so GET is the only
        honest source of truth for what the cluster is actually enforcing.

        With `server`, asks that node directly over plain HTTP -- each node
        serves the value from its own copy of the config. `auth` as for
        IPCUtils.get_internal_settings.
        """
        if server is None and auth is None:
            status, content = self.cluster_rest.set_internal_settings()
        else:
            resp = IPCUtils.get_internal_settings(
                server or self.cluster.master, auth=auth)
            status = resp.status_code == 200
            content = resp.json() if status else resp.text
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

    def _diag_eval(self, code, server=None):
        rest = RestConnection(server) if server else self.rest
        status, content = rest.diag_eval(code)
        if not status:
            self.fail(f"diag/eval {code!r} failed: {content}")
        if isinstance(content, bytes):
            content = content.decode()
        return content.strip()

    def _ipc_key_in_config(self, server=None):
        """True if the key is explicitly stored in ns_config, False if the
        value GET reports is the compat-derived default."""
        return self._diag_eval(
            f"ns_config:search({self.IPC_CONFIG_KEY}).", server) != "false"

    def delete_ipc_key(self, server=None):
        """Remove the stored key so the compat-derived default applies."""
        self._diag_eval(f"ns_config:delete({self.IPC_CONFIG_KEY}).", server)

    def ipc_config_raw(self, server=None):
        """
        The key's raw config entry, vclock included. Equal before and after
        a request proves the request wrote nothing -- the value alone cannot
        show that, since a write of the same value leaves it unchanged.
        """
        return self._diag_eval(
            f"ns_config:search_raw(ns_config:get(), {self.IPC_CONFIG_KEY}).",
            server)

    def wait_for_ipc_value(self, servers, expected, timeout=120):
        """Poll every node until each reports `expected`."""
        deadline = time.time() + timeout
        while True:
            seen = {s.ip: self.get_ipc_setting(server=s) for s in servers}
            if all(v == expected for v in seen.values()):
                return
            if time.time() > deadline:
                self.fail(f"{self.IPC_SETTING} did not converge to "
                          f"{expected} within {timeout}s: {seen}")
            time.sleep(2)

    def assert_cert_enforcement(self, servers, cert_path, key_path, ipc,
                                what=""):
        """
        The setting is not just reported but enforced on every node: the
        internal certificate alone gets 401 with IPC on, 200 with it off.
        """
        expected = 401 if ipc else 200
        for server in servers:
            resp = self.mtls_request(cert_path, key_path, server=server)
            self.assert_status(
                resp, expected,
                f"{what} cert alone on {server.ip} with ipc={ipc}")

    def restart_nodes(self, servers):
        """Stop every node, then start every node, then wait for each to
        report healthy -- a full-cluster restart when given all nodes."""
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                shell.stop_couchbase()
            finally:
                shell.disconnect()
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                shell.start_couchbase()
            finally:
                shell.disconnect()
        for server in servers:
            if not RestConnection(server).is_ns_server_running(300):
                self.fail(f"{server.ip} not healthy after restart")

    def wait_for_internal_settings_audit(self, baseline, expected,
                                         timeout=60):
        """
        Wait for a new 8228 'internal settings' audit event (one that is
        not `baseline`) recording `expected` for this setting.
        """
        deadline = time.time() + timeout
        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            while True:
                event = get_audit_event(shell, self.AUDIT_LOG_PATH,
                                        self.INTERNAL_SETTINGS_AUDIT_ID)
                if event and event != baseline and \
                        event.get("settings", {}).get(
                            self.IPC_CONFIG_KEY) == expected:
                    return event
                if time.time() > deadline:
                    self.fail(f"No new audit event "
                              f"{self.INTERNAL_SETTINGS_AUDIT_ID} with "
                              f"{self.IPC_CONFIG_KEY}={expected} within "
                              f"{timeout}s. Last seen: {event}")
                time.sleep(2)
        finally:
            shell.disconnect()

    def latest_internal_settings_audit(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            return get_audit_event(shell, self.AUDIT_LOG_PATH,
                                   self.INTERNAL_SETTINGS_AUDIT_ID)
        finally:
            shell.disconnect()

    def _restore_ipc_setting(self):
        """
        Put the setting back only if a test actually changed it.

        Writing it back unconditionally would be worse than a no-op: an explicit
        write pins the key in config, and from then on the version-derived
        default no longer applies -- so a cluster reused by a later test would
        silently stop tracking its own compat version. For the same reason,
        if the key was absent at setUp it is deleted rather than re-written,
        since any POST pins the key.
        """
        if self._ipc_key_was_absent:
            if self._ipc_key_in_config():
                self.log.info(
                    f"Deleting {self.IPC_CONFIG_KEY} -- it was derived, not "
                    f"stored, at setUp"
                )
                self.delete_ipc_key()
            return
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
        cert_path, key_path, _serial = self.generate_client_cert(
            cn or name, ca_cert=ca_cert, ca_key=ca_key,
            email_names=[f"{name}@{self.INTERNAL_CERT_DOMAIN}"],
        )
        return cert_path, key_path

    def generate_client_cert(self, cn, ca_cert=None, ca_key=None,
                             **leaf_kwargs):
        """
        Any client certificate, written to temp PEM files tracked for
        teardown. Issued by the cluster's trusted CA unless another is
        given; `leaf_kwargs` go to CRLUtils.generate_leaf_cert (SANs, EKU,
        validity window, extra_extension).

        Returns:
            (cert_path, key_path, serial)
        """
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            ca_cert if ca_cert is not None else self.ca_cert,
            ca_key if ca_key is not None else self.ca_key,
            cn, **leaf_kwargs,
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        return cert_path, key_path, serial

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
                     server=None, port=None, auth=None, timeout=30,
                     method="GET", data=None, headers=None):
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
        return self._retry_on_timeout(
            self.crl_utils.perform_mtls_handshake,
            server.ip, port or self.MGMT_SSL_PORT, cert_path, key_path,
            path=path, timeout=timeout, auth=auth, method=method, data=data,
            headers=headers,
        )

    def _retry_on_timeout(self, fn, *args, **kwargs):
        """
        Retry once on a timeout. A handshake occasionally stalls with the
        request never reaching the server; that is not an auth outcome, so
        it must not decide an assertion. TLS rejections are SSLError, not
        timeouts, and are never retried.
        """
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.Timeout as exc:
            self.log.warning(f"Request timed out, retrying once: {exc}")
            return fn(*args, **kwargs)

    def https_request(self, path="/pools/default", server=None, auth=None,
                      method="GET", data=None, headers=None, timeout=30):
        """The same request as mtls_request but with NO client certificate,
        for the certificate-less control in each comparison."""
        server = server or self.cluster.master
        return self._retry_on_timeout(
            requests.request,
            method, f"https://{server.ip}:{self.MGMT_SSL_PORT}{path}",
            verify=False, auth=auth, data=data, timeout=timeout,
            headers={**(headers or {}), "Connection": "close"},
        )

    def get_internal_password(self, server=None):
        """
        Current internal ('special') password of `server`.

        With IPC enabled this password, not the certificate, is what proves an
        '@' identity. Passwords are per node and rotate, so read it from the
        node being probed, right before use.
        """
        rest = RestConnection(server) if server else self.rest
        status, content = rest.diag_eval(
            "ns_config_auth:get_password(special).", print_log=False
        )
        if not status:
            self.fail(f"diag/eval get_password(special) failed: {content}")
        if isinstance(content, bytes):
            content = content.decode()
        return content.strip().strip('"')

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

    # -- Auth probes and assertions -----------------------------------------

    CCA_STATES = ("disable", "enable", "hybrid", "mandatory")
    TLS_REJECT = "tls_reject"

    def probe_identity(self, cert_path, key_path, auth=None):
        """
        (whoami id, /pools/default status) for one credential combination.

        cert_path=None sends no client certificate. whoami id is None when
        /whoami itself is refused, and the whole tuple is TLS_REJECT when the
        connection never reaches the application.
        """
        try:
            if cert_path:
                who = self.mtls_request(cert_path, key_path, path="/whoami",
                                        auth=auth)
                pools = self.mtls_request(cert_path, key_path, auth=auth)
            else:
                who = self.https_request("/whoami", auth=auth)
                pools = self.https_request(auth=auth)
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError):
            return self.TLS_REJECT, self.TLS_REJECT
        who_id = who.json().get("id") if who.status_code == 200 else None
        return who_id, pools.status_code

    def expected_auth_matrix_cells(self, cca, ipc, admin):
        """
        What each probe_identity combination must resolve to for one
        (cca, IPC) pair, keyed by combination name.

        The certificate confers identity only when cca is not 'disable' AND
        IPC is off; when it does, it overrides any supplied credentials.
        """
        cert_wins = cca != "disable" and not ipc
        internal = "@internal"
        return {
            "cert alone":
                (internal, 200) if cert_wins else ("", 401),
            "cert + admin":
                (internal if cert_wins else admin, 200),
            "no cert + admin":
                ((self.TLS_REJECT, self.TLS_REJECT) if cca == "mandatory"
                 else (admin, 200)),
            "cert + @internal:password":
                (internal, 200),
            "cert + @internal:wrong":
                (internal, 200) if cert_wins else (None, 401),
        }

    def wait_for_probe(self, cert_path, key_path, expected, timeout=60):
        """
        Poll probe_identity until its whoami part is `expected`, for
        changes the server applies asynchronously (e.g. a CRL reload).
        Returns the last whoami part seen.
        """
        deadline = time.time() + timeout
        while True:
            who, _ = self.probe_identity(cert_path, key_path)
            if who == expected or time.time() > deadline:
                return who
            time.sleep(2)

    def wait_for_mandatory(self, timeout=30):
        """Wait until the listener refuses a certificate-less connection,
        so probes see 'mandatory' rather than the reload window."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.https_request("/whoami", timeout=5)
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError):
                return
            time.sleep(1)
        self.fail(f"clientCertAuth=mandatory not enforced after {timeout}s")

    def assert_status(self, resp, expected, what):
        expected = expected if isinstance(expected, tuple) else (expected,)
        self.assertIn(
            resp.status_code, expected,
            f"{what}: expected HTTP {expected}, got {resp.status_code}: "
            f"{resp.text[:300]}")

    def assert_identity_is_per_request(self, cert, key, admin_auth):
        """
        Two requests on ONE kept-alive TLS connection: the first with admin
        credentials, the second with none. The second must be anonymous.
        Pool connection count proves the socket was actually reused.
        """
        url = (f"https://{self.cluster.master.ip}:{self.MGMT_SSL_PORT}"
               f"/whoami")
        with requests.Session() as session:
            session.cert = (cert, key)
            session.verify = False
            first = session.get(url, auth=admin_auth, timeout=30).json()
            second = session.get(url, timeout=30).json()
            pools = session.get_adapter(url).poolmanager.pools
            conns = sum(pools[k].num_connections for k in pools.keys())
        self.log.info(f"N-02 first={first} second={second} conns={conns}")
        self.assertEqual(conns, 1,
                         "N-02: both requests must share one connection")
        self.assertEqual(first.get("id"), admin_auth[0],
                         f"N-02 first request: {first}")
        self.assertEqual(second.get("domain"), "anonymous",
                         f"N-02: identity leaked across requests on one "
                         f"connection: {second}")

    def set_user_locked(self, username, locked):
        resp = IPCUtils.set_user_locked(self.cluster.master, username, locked)
        self.assert_status(resp, 200, f"lock={locked} {username}")

    def cbauth_extract_user(self, cert_path):
        """POST /_cbauth/extractUserFromCert; asserts the HTTP 200 the
        cbauth contract requires for both 'user' and 'no identity'."""
        resp = IPCUtils.extract_user_from_cert(
            self.cluster.master, self._cert_der_from_path(cert_path))
        self.assert_status(resp, 200, "C-21 extractUserFromCert")
        return resp.json()

    # -- Node addition fixtures ----------------------------------------------

    SPARE_PASSWORD = "SparePass@1234"

    def deploy_test_node_certs(self, server, ca_cert, ca_key, crl_utils=None):
        """
        Give `server` a node certificate and an internal client certificate
        issued by `ca_cert`. The client certificate is what a node presents
        when it adds or joins another under client certificate auth.
        """
        utils = crl_utils or self.crl_utils
        node_cert, node_key, _ = utils.generate_leaf_cert(
            ca_cert, ca_key, cn=server.ip, dns_names=[server.ip],
            extended_key_usage=[ExtendedKeyUsageOID.SERVER_AUTH,
                                ExtendedKeyUsageOID.CLIENT_AUTH])
        utils.deploy_node_cert(RestConnection(server), server, node_cert,
                               node_key)
        client_cert, client_key, _ = utils.generate_leaf_cert(
            ca_cert, ca_key, cn=server.ip,
            email_names=[f"{server.ip.replace('.', '-')}"
                         f"@{self.INTERNAL_CERT_DOMAIN}"])
        utils.deploy_client_cert(server, client_cert, client_key)

    def prepare_joinee(self, server, ipc, trusted_cas, issuer=None,
                       initialize=True, cca="mandatory"):
        """
        Make `server` a node that is about to be added to the cluster:
        optionally initialised as its own single-node cluster with its own
        admin password, trusting `trusted_cas`, with node and client certs
        from `issuer` (default: the first CA), clientCertAuth `cca` and IPC
        `ipc`.

        An initialised joinee matters: an uninitialised node accepts any
        caller, so it would hide what IPC does to the joining handshake.

        Returns a copy of `server` carrying the joinee's own credentials.
        """
        joinee = copy.copy(server)
        if initialize:
            status, content = ClusterRestAPI(server).initialize_cluster(
                hostname=server.ip, username=server.rest_username,
                password=self.SPARE_PASSWORD, services="kv",
                memory_quota=256)
            if not status:
                self.fail(f"Initialising {server.ip} failed: {content}")
            joinee.rest_password = self.SPARE_PASSWORD
        # Separate tracker: these CA ids belong to the joinee, and must not
        # end up in the cluster's own trusted-CA cleanup list.
        spare_utils = CRLUtils(log=self.log)
        for ca_cert, _ in trusted_cas:
            spare_utils.trust_ca_on_cluster(RestConnection(joinee), joinee,
                                            ca_cert)
        issuer = issuer or trusted_cas[0]
        self.deploy_test_node_certs(joinee, issuer[0], issuer[1],
                                    crl_utils=spare_utils)
        if initialize:
            IPCUtils.set_client_cert_auth(
                joinee, cca, self.DEFAULT_CCA_PREFIXES,
                auth=self._auth_of(joinee)).raise_for_status()
            resp = IPCUtils.post_internal_settings(
                joinee, {self.IPC_SETTING: "true" if ipc else "false"},
                auth=self._auth_of(joinee))
            self.assert_status(resp, 200, f"IPC={ipc} on {server.ip}")
        return joinee

    @staticmethod
    def _auth_of(server):
        return server.rest_username, server.rest_password

    def otp_of(self, server):
        return f"ns_1@{server.ip}"

    def rebalance_cluster(self, ejected=()):
        """Rebalance every known node in, ejecting `ejected`, and wait."""
        known = [node.id for node in self.rest.node_statuses()]
        self.rest.rebalance(otpNodes=known,
                            ejectedNodes=[self.otp_of(s) for s in ejected])
        self.assertTrue(self.rest.monitorRebalance(), "Rebalance failed")

    def wait_until_uninitialized(self, server, timeout=180):
        """A node removed from a cluster resets asynchronously; wait until
        it reports no pool before preparing it again."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = requests.get(
                    f"http://{server.ip}:8091/pools/default",
                    auth=self._auth_of(server), timeout=10)
                if "unknown pool" in resp.text:
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(3)
        self.fail(f"{server.ip} did not return to uninitialised in "
                  f"{timeout}s")

    def assert_node_active(self, server, services=None, timeout=120):
        """Wait until `server` is a healthy, active member of the cluster,
        optionally with exactly `services`."""
        deadline = time.time() + timeout
        entry = None
        while time.time() < deadline:
            nodes = self.rest.cluster_status().get("nodes", [])
            entry = next((n for n in nodes
                          if n.get("otpNode") == self.otp_of(server)), None)
            if entry and entry.get("status") == "healthy" and \
                    entry.get("clusterMembership") == "active":
                break
            time.sleep(3)
        else:
            self.fail(f"{server.ip} not healthy/active: {entry}")
        if services is not None:
            self.assertEqual(sorted(entry.get("services", [])),
                             sorted(services),
                             f"{server.ip} services: {entry}")

    def reset_spares(self, spares):
        """
        Best-effort return of spare nodes to the uninitialised state TAF
        expects: drop any still attached to the cluster, then hard-reset
        any that are initialised on their own.
        """
        attached = {node.id for node in self.rest.node_statuses()}
        active = [s for s in spares if self.otp_of(s) in attached]
        for server in active:
            IPCUtils.eject_node(self.cluster.master, self.otp_of(server))
        still = {node.id for node in self.rest.node_statuses()}
        members = [s for s in spares if self.otp_of(s) in still]
        if members:
            try:
                self.rebalance_cluster(ejected=members)
            except Exception as exc:
                self.log.warning(f"Rebalancing out spares failed: {exc}")
        # Plain HTTP: a spare left at clientCertAuth 'mandatory' refuses the
        # framework's certificate-less HTTPS.
        for server in spares:
            for password in (self.SPARE_PASSWORD, server.rest_password):
                try:
                    resp = IPCUtils.hard_reset_node(
                        server, auth=(server.rest_username, password))
                    if resp.status_code == 200:
                        break
                except requests.exceptions.RequestException:
                    continue

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
        self._set_client_cert_auth_http("disable")

    DEFAULT_CCA_PREFIXES = [
        {"path": "subject.cn", "prefix": "", "delimiter": ""}]

    def _set_client_cert_auth_http(self, state, prefixes=None):
        """
        Set clientCertAuth over plain HTTP 8091. The framework's own REST
        goes over HTTPS without a client certificate, so it cannot switch
        'mandatory' back off, or do anything else while it is on.

        Always sends at least one prefix rule: memcached rejects an empty
        'prefixes' array and would be left enforcing the previous state
        until restarted.
        """
        IPCUtils.set_client_cert_auth(
            self.cluster.master, state,
            prefixes or self.DEFAULT_CCA_PREFIXES).raise_for_status()

    @staticmethod
    def _cert_der_from_path(cert_path):
        with open(cert_path, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read())
        return cert.public_bytes(serialization.Encoding.DER)

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

    def _set_rbac_roles(self, username, roles):
        """Replace an existing local user's roles (comma-separated)."""
        RbacUtils(self.cluster.master)._update_user_roles(username, roles)

    def _cleanup_rbac_users(self):
        for username in self._rbac_users:
            try:
                self.rest.delete_builtin_user(username)
            except Exception as exc:
                self.log.warning(
                    f"Failed to delete RBAC user {username}: {exc}"
                )
        self._rbac_users = []
