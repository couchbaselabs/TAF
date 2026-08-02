import datetime
import ipaddress
import json
import uuid

import requests
from cb_server_rest_util.security.crl import (
    CRLAPI,
    ENDPOINT_CBAUTH_CRLS_VALIDATE,
    ENDPOINT_CRL_DIAGNOSTICS_STATUS,
    ENDPOINT_CRL_DIAGNOSTICS_VALIDATE,
    ENDPOINT_CRL_FILES,
    ENDPOINT_CRL_SETTINGS,
    ENDPOINT_RELOAD_CRL,
)
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.security_utils.jwt_utils import (
    remote_curl,
    remote_write_file_b64,
    start_remote_http_server,
)

__all__ = [
    "CRLUtils",
    "setup_url_poll_crl_env",
    "cleanup_url_poll_crl_env",
    "POLICY_MODES",
    "SCOPES",
    "CACHE_STATUS_VALUES",
    "RELOAD_RESULT_VALUES",
    "DIAGNOSTIC_STATUS_VALUES",
    "ENDPOINT_CRL_SETTINGS",
    "ENDPOINT_CRL_FILES",
    "ENDPOINT_CRL_DIAGNOSTICS_STATUS",
    "ENDPOINT_CRL_DIAGNOSTICS_VALIDATE",
    "ENDPOINT_RELOAD_CRL",
    "ENDPOINT_CBAUTH_CRLS_VALIDATE",
]

# ── Enums, from CRL_API_Contract.md ─────────────────────────────────────────
POLICY_MODES = {"Disabled", "Permissive", "Require"}
SCOPES = {"clientAuth", "nodeToNode"}
CACHE_STATUS_VALUES = {
    "active", "expired", "notYetValid", "untrusted", "invalid", "notLoaded",
}
RELOAD_RESULT_VALUES = {
    "loaded", "failed", "notAttempted", "uploaded", "notDownloaded",
    "checksumMismatch", "readError",
}
DIAGNOSTIC_STATUS_VALUES = {"valid", "revoked", "undetermined", "failed"}

# Key algorithms covering the two most common real-world Couchbase PKI shapes:
# RSA 2048 (still the default for legacy/enterprise AD CS setups) and ECDSA
# P-256 (default for Vault PKI, cert-manager, and cloud-native private CAs).
KEY_ALGORITHMS = {"rsa2048", "ecdsa_p256"}


class CRLUtils:
    """
    High-level utilities for CRL (Certificate Revocation List) tests.

    Combines CA/CRL crypto generation (in-memory, via `cryptography`, not
    x509main's SSH/openssl-shellout pattern) with REST orchestration against
    /settings/crl* — mirrors the JWTUtils/CredentialStoreUtils convention:
    every REST-facing method takes a `rest_connection` as first argument and
    builds a CRLAPI (CBRestConnection) via the _crl_api() shim.
    """

    def __init__(self, log=None):
        self.log = log

    # ── Internal helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _crl_api(rest_connection, username=None, password=None):
        """Build and configure a CRLAPI from a RestConnection-like object."""

        class _Shim:
            pass

        shim = _Shim()
        shim.ip = getattr(rest_connection, "ip", None)
        shim.port = getattr(rest_connection, "port", None)
        shim.rest_username = (
            username
            or getattr(rest_connection, "username", None)
            or getattr(rest_connection, "rest_username", None)
        )
        shim.rest_password = (
            password
            or getattr(rest_connection, "password", None)
            or getattr(rest_connection, "rest_password", None)
        )
        shim.type = getattr(rest_connection, "type", "default")
        shim.services = getattr(rest_connection, "services", None)
        shim.hostname = getattr(rest_connection, "hostname", None)
        api = CRLAPI()
        api.set_server_values(shim)
        api.set_endpoint_urls(shim)
        caller_base = (
            getattr(rest_connection, "baseUrl", None)
            or getattr(rest_connection, "base_url", None)
        )
        if caller_base:
            api.base_url = caller_base.rstrip("/")
        return api

    @staticmethod
    def parse_content(content):
        """Return parsed JSON, tolerating bytes/str/dict input."""
        if content is None:
            return None
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8", "replace")
        if isinstance(content, str):
            try:
                return json.loads(content)
            except ValueError:
                return content
        return content

    # ── Crypto: CA / leaf certs ──────────────────────────────────────────────

    @staticmethod
    def _generate_private_key(key_algorithm):
        """Return a private key for one of KEY_ALGORITHMS."""
        if key_algorithm == "rsa2048":
            return rsa.generate_private_key(public_exponent=65537, key_size=2048)
        if key_algorithm == "ecdsa_p256":
            return ec.generate_private_key(ec.SECP256R1())
        raise ValueError(
            f"Unsupported key_algorithm: {key_algorithm!r}, expected one of {KEY_ALGORITHMS}"
        )

    @staticmethod
    def generate_ca(cn, key_algorithm="rsa2048", valid_days=3650):
        """
        Generate a self-signed CA cert/key pair, in memory.

        Args:
            key_algorithm: one of KEY_ALGORITHMS — "rsa2048" (default, matches
                legacy/enterprise AD CS) or "ecdsa_p256" (matches Vault PKI,
                cert-manager, cloud-native private CAs)

        Returns:
            tuple: (ca_cert: x509.Certificate, ca_key)
        """
        key = CRLUtils._generate_private_key(key_algorithm)
        subject = issuer = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, cn)]
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=False, content_commitment=False,
                    key_encipherment=False, data_encipherment=False,
                    key_agreement=False, key_cert_sign=True, crl_sign=True,
                    encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        return cert, key

    @staticmethod
    def generate_intermediate_ca(parent_cert, parent_key, cn, key_algorithm="rsa2048",
                                  valid_days=1825, path_length=0):
        """
        Generate an intermediate CA cert/key pair, signed by parent_cert/
        parent_key (not self-signed) -- same CA:TRUE / key_cert_sign+crl_sign
        extension shape as generate_ca, but issued by a parent CA instead of
        being a root. Useful for multi-tier chain tests (leaf -> intermediate
        -> parent), where the intermediate's own serial can independently be
        revoked by the parent, separately from the leaf's.

        Args:
            path_length: max number of further intermediate CAs this one may
                sign beneath it (0 = may only sign end-entity leaf certs)

        Returns:
            tuple: (cert: x509.Certificate, key, serial: int)
        """
        key = CRLUtils._generate_private_key(key_algorithm)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
        now = datetime.datetime.now(datetime.timezone.utc)
        serial = x509.random_serial_number()
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(parent_cert.subject)
            .public_key(key.public_key())
            .serial_number(serial)
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=path_length), critical=True
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=False, content_commitment=False,
                    key_encipherment=False, data_encipherment=False,
                    key_agreement=False, key_cert_sign=True, crl_sign=True,
                    encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False,
            )
            .sign(parent_key, hashes.SHA256())
        )
        return cert, key, serial

    @staticmethod
    def generate_leaf_cert(ca_cert, ca_key, cn, key_algorithm="rsa2048",
                            valid_days=825, extended_key_usage=None,
                            crl_distribution_url=None, dns_names=None,
                            serial=None):
        """
        Generate a leaf cert signed by ca_cert/ca_key, in memory.

        Args:
            key_algorithm: one of KEY_ALGORITHMS — "rsa2048" (default) or
                "ecdsa_p256". Independent of ca_key's own algorithm — a CA can
                sign leaf certs of either algorithm.
            extended_key_usage: list of x509.oid.ExtendedKeyUsageOID, defaults
                to CLIENT_AUTH
            crl_distribution_url: optional http(s) URL to embed as a
                CRLDistributionPoints extension (informational only — Couchbase
                does not auto-fetch from this per the PRD; useful only for
                tests that assert the extension is present/ignored)
            dns_names: optional list of SAN DNS names (needed for node certs)
            serial: optional int to force a specific serial number (e.g. to
                deliberately collide two leaf certs from different CAs, for
                cross-CA scope-isolation tests) — defaults to a random serial

        Returns:
            tuple: (cert: x509.Certificate, key, serial: int)
        """
        if extended_key_usage is None:
            extended_key_usage = [x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]
        key = CRLUtils._generate_private_key(key_algorithm)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
        now = datetime.datetime.now(datetime.timezone.utc)
        serial = serial if serial is not None else x509.random_serial_number()
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(serial)
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=valid_days))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True, content_commitment=False,
                    key_encipherment=True, data_encipherment=False,
                    key_agreement=False, key_cert_sign=False, crl_sign=False,
                    encipher_only=False, decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(x509.ExtendedKeyUsage(extended_key_usage), critical=False)
        )
        if dns_names:
            names = []
            for name in dns_names:
                try:
                    names.append(x509.IPAddress(ipaddress.ip_address(name)))
                except ValueError:
                    names.append(x509.DNSName(name))
            builder = builder.add_extension(x509.SubjectAlternativeName(names), critical=False)
        if crl_distribution_url:
            dp = x509.DistributionPoint(
                full_name=[x509.UniformResourceIdentifier(crl_distribution_url)],
                relative_name=None, reasons=None, crl_issuer=None,
            )
            builder = builder.add_extension(x509.CRLDistributionPoints([dp]), critical=False)
        cert = builder.sign(ca_key, hashes.SHA256())
        return cert, key, serial

    @staticmethod
    def build_crl(ca_cert, ca_key, revoked_serials=None, this_update=None,
                  next_update=None, crl_number=None, expired=False):
        """
        Build and sign a CRL for ca_cert/ca_key.

        Args:
            revoked_serials: list of int serial numbers to mark revoked
            this_update / next_update: datetime, defaults to now / now+30d
            crl_number: optional int, adds a CRLNumber extension
            expired: if True and next_update not given, sets next_update to
                30 days in the past (server rejects genuinely-expired CRLs at
                upload time unless allow_expired_crls is set — see
                CRL_MANUAL_VALIDATIONS.md Test 2)

        Returns:
            bytes: PEM-encoded CRL
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        if this_update is None:
            # When expired, this_update must default to further in the past
            # than next_update's own expired default (now-30d) below --
            # otherwise both defaulting independently can produce
            # this_update > next_update, which cryptography correctly
            # rejects ("next update date must be after last update date").
            this_update = now - datetime.timedelta(days=60 if expired else 1)
        if next_update is None:
            next_update = (
                now - datetime.timedelta(days=30) if expired
                else now + datetime.timedelta(days=30)
            )
        builder = x509.CertificateRevocationListBuilder().issuer_name(
            ca_cert.subject
        ).last_update(this_update).next_update(next_update)
        for serial in (revoked_serials or []):
            revoked = (
                x509.RevokedCertificateBuilder()
                .serial_number(serial)
                .revocation_date(now)
                .build()
            )
            builder = builder.add_revoked_certificate(revoked)
        if crl_number is not None:
            builder = builder.add_extension(
                x509.CRLNumber(crl_number), critical=False
            )
        crl = builder.sign(private_key=ca_key, algorithm=hashes.SHA256())
        return crl.public_bytes(serialization.Encoding.PEM)

    @staticmethod
    def pem_crl_to_der(pem_bytes):
        """Convert a PEM-encoded CRL (as returned by build_crl) to DER bytes."""
        crl = x509.load_pem_x509_crl(pem_bytes)
        return crl.public_bytes(serialization.Encoding.DER)

    @staticmethod
    def cert_to_pem(cert):
        return cert.public_bytes(serialization.Encoding.PEM)

    @staticmethod
    def key_to_pem(key):
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    # ── REST orchestration ───────────────────────────────────────────────────

    def get_settings(self, rest):
        """GET /settings/crl. Returns (status_bool, content_dict)."""
        api = self._crl_api(rest)
        status, content, _ = api.get_crl_settings()
        return status, self.parse_content(content)

    def set_settings(self, rest, **fields):
        """
        POST /settings/crl with the given fields (partial update).
        Accepts keys: policyPerScope, directory, dirPollIntervalMs,
        checkIntermediateCerts, urls, urlPollIntervalMs.

        Returns (status_bool, content_dict) — the full merged config on success.
        """
        api = self._crl_api(rest)
        status, content, _ = api.post_crl_settings(fields)
        return status, self.parse_content(content)

    def list_files(self, rest):
        """GET /settings/crl/files. Returns (status_bool, content_list)."""
        api = self._crl_api(rest)
        status, content, _ = api.get_crl_files()
        return status, self.parse_content(content)

    def upload_file(self, rest, filename, pem_bytes, timeout=300):
        """
        POST /settings/crl/files. Returns (status_bool, content).

        Pass a larger `timeout` for large CRLs (many thousands of revoked
        entries) — the default 300s can be too short for very large uploads.
        """
        api = self._crl_api(rest)
        status, content, _ = api.upload_crl_file(filename, pem_bytes, timeout=timeout)
        return status, self.parse_content(content)

    def delete_file(self, rest, filename):
        """DELETE /settings/crl/files/:filename. Returns (status_bool, content)."""
        api = self._crl_api(rest)
        status, content, _ = api.delete_crl_file(filename)
        return status, self.parse_content(content)

    def diagnostics_status(self, rest, nodes=None):
        """GET /settings/crl/diagnostics/status. Returns (status_bool, content)."""
        api = self._crl_api(rest)
        status, content, _ = api.get_diagnostics_status(nodes=nodes)
        return status, self.parse_content(content)

    def diagnostics_validate(self, rest, policy=None, certs=None):
        """POST /settings/crl/diagnostics/validate. Returns (status_bool, content)."""
        api = self._crl_api(rest)
        status, content, _ = api.post_diagnostics_validate(policy=policy, certs=certs)
        return status, self.parse_content(content)

    def reload_crl(self, rest):
        """POST /node/controller/reloadCrl. Returns (status_bool, content)."""
        api = self._crl_api(rest)
        status, content, _ = api.reload_crl()
        return status, self.parse_content(content)

    def cbauth_crls_validate(self, rest, certs, scope):
        """POST /_cbauth/crlsValidate. Returns (status_bool, content)."""
        api = self._crl_api(rest)
        status, content, _ = api.cbauth_crls_validate(certs, scope)
        return status, self.parse_content(content)

    def revoke_and_upload(self, rest, ca_cert, ca_key, serials, filename,
                           timeout=300, **crl_kwargs):
        """
        Convenience: build_crl() revoking `serials` (int or list of int) then
        upload_file() it. The single most-used helper across the test plan.

        Returns (status_bool, content).
        """
        if isinstance(serials, int):
            serials = [serials]
        pem = self.build_crl(ca_cert, ca_key, revoked_serials=serials, **crl_kwargs)
        return self.upload_file(rest, filename, pem, timeout=timeout)

    # ── mTLS handshake helper ────────────────────────────────────────────────

    @staticmethod
    def perform_mtls_handshake(host, port, client_cert_path, client_key_path,
                               path="/whoami", timeout=30):
        """
        Performs a mutual TLS (mTLS) handshake and HTTP GET request using the 
        provided client certificate.

        Args:
            host (str): Target node IP or hostname.
            port (int|str): Target TLS port (e.g., 18091 for mgmt).
            client_cert_path (str): Filesystem path to the client's PEM certificate.
            client_key_path (str): Filesystem path to the client's PEM private key.
            path (str): HTTP endpoint to hit upon successful handshake (default: "/whoami").
            timeout (int): Request timeout in seconds.

        Returns:
            requests.Response: On a successful handshake and HTTP response.

        Raises:
            requests.exceptions.SSLError: If the connection is rejected at the TLS layer 
                (e.g., due to a revoked or expired client certificate).

        Notes:
            - Server identity verification is intentionally disabled (verify=False). 
              The Couchbase node's default self-signed cert lacks the `CA:TRUE` 
              constraint. Enabling verification causes the client's OpenSSL binding 
              to abort the connection locally before the server can evaluate the CRL.
            - `Connection: close` is forced to prevent urllib3 from reusing kept-alive 
              sockets, ensuring a fresh TLS handshake occurs on every invocation.
        """
        url = f"https://{host}:{port}{path}"
        return requests.get(
            url, 
            cert=(client_cert_path, client_key_path), 
            verify=False,
            timeout=timeout, 
            headers={"Connection": "close"}
        )

    # ── Assert helpers ───────────────────────────────────────────────────────

    @staticmethod
    def assert_settings_equal(actual, expected_subset):
        """Assert every key/value in expected_subset matches actual."""
        for key, value in expected_subset.items():
            if actual.get(key) != value:
                raise AssertionError(
                    f"CRL settings mismatch for '{key}': "
                    f"expected {value!r}, got {actual.get(key)!r}"
                )

    @staticmethod
    def assert_diagnostics_entry(entry, expected_status=None, expected_source=None):
        """Assert a single diagnostics/status file entry matches expectations."""
        if expected_status is not None and entry.get("cacheStatus") != expected_status:
            raise AssertionError(
                f"Expected cacheStatus={expected_status!r}, "
                f"got {entry.get('cacheStatus')!r}"
            )
        if expected_source is not None and entry.get("source") != expected_source:
            raise AssertionError(
                f"Expected source={expected_source!r}, got {entry.get('source')!r}"
            )


# ── URL-poll ingestion helpers ──────────────────────────────────────────────
# Mirrors jwt_utils.py's setup_jwks_uri_issuer_env/cleanup_jwks_uri_issuer_env
# exactly -- same throwaway-HTTP-server pattern, reusing jwt_utils.py's
# generic remote_write_file_b64/start_remote_http_server/remote_curl helpers
# directly rather than re-deriving them, just serving a CRL instead of a
# JWKS document and pointing /settings/crl's `urls` at it instead of jwksUri.
# stop_process_on_port (below) has no jwt_utils.py equivalent -- JWT's own
# cleanup uses PID-based killing (stop_remote_process) and accepts its known
# unreliability over a non-interactive SSH channel (see that function's own
# docstring); CRL's retry loop needs killing-by-port specifically, since a
# failed attempt must not leave a listener behind on the port before retrying.

def stop_process_on_port(shell_conn, port):
    """
    Kill whatever is listening on `port`, regardless of how it was started.
    More reliable than a PID captured from a backgrounded shell job (see
    jwt_utils.stop_remote_process's docstring) -- this looks at the actual
    live socket table instead of trusting shell job-control state that may
    not have propagated correctly over a non-interactive SSH channel.
    """
    shell_conn.execute_command(
        f"sh -c \"lsof -ti:{int(port)} | xargs -r kill -9 2>/dev/null || true\""
    )


def setup_url_poll_crl_env(*, crl_utils_obj, cluster_master, rest, ca_cert, ca_key,
                            revoked_serials=None, crl_kwargs=None, filename="crl.pem",
                            http_port=18990, http_bind="127.0.0.1",
                            url_host_mode="localhost", url_poll_interval_ms=5000,
                            start_attempts=10, log_callback=None):
    """
    Serves a signed CRL over a throwaway HTTP server on cluster_master, then
    configures /settings/crl's `urls`/`urlPollIntervalMs` to fetch it.

    Returns an env dict: shell_conn/pid/tmp_dir/crl_url/settings_status/settings_content.
    Raises AssertionError if the server never started/served correctly after
    start_attempts tries (port conflict or start failure).
    """
    tmp_dir = f"/tmp/taf_crl_url_{uuid.uuid4().hex[:8]}"
    pid = None

    shell_conn = RemoteMachineShellConnection(cluster_master)
    shell_conn.execute_command("sh -c \"pkill -f 'http.server' >/dev/null 2>&1 || true\"")

    crl_pem = crl_utils_obj.build_crl(
        ca_cert, ca_key, revoked_serials=revoked_serials, **(crl_kwargs or {})
    )
    remote_write_file_b64(shell_conn, f"{tmp_dir}/{filename}", crl_pem.decode("utf-8"))

    host = (
        (getattr(cluster_master, "ip", None) or getattr(cluster_master, "hostname", None))
        if url_host_mode == "node_ip" else "127.0.0.1"
    )

    chosen_port, crl_url = None, None
    last_code, last_body = None, None

    for attempt in range(1, max(1, int(start_attempts)) + 1):
        candidate_port = int(http_port) + (attempt - 1)
        pid, _listen_diag, start_cmd = start_remote_http_server(
            shell_conn, port=candidate_port, directory=tmp_dir, bind=http_bind
        )
        if log_callback:
            log_callback(f"Starting remote CRL HTTP server: {start_cmd}")

        crl_url = f"http://{host}:{candidate_port}/{filename}"
        if log_callback:
            log_callback(f"Using CRL URL (attempt {attempt}): {crl_url}")

        http_code, body, _curl_err = remote_curl(shell_conn, crl_url, timeout_seconds=5)
        last_code, last_body = http_code, body
        if str(http_code) == "200" and body and "BEGIN X509 CRL" in body:
            chosen_port = candidate_port
            break

        # stop_process_on_port, not stop_remote_process -- $! captured after
        # backgrounding a job over a non-interactive SSH channel isn't
        # reliable (confirmed by direct testing), so killing by the port
        # this attempt just tried to bind is the only way to guarantee the
        # failed attempt's process doesn't linger before retrying.
        stop_process_on_port(shell_conn, candidate_port)
        pid = None

    if chosen_port is None:
        try:
            shell_conn.execute_command(f"sh -c \"rm -rf '{tmp_dir}' || true\"")
        except Exception:
            pass
        shell_conn.disconnect()
        raise AssertionError(
            "CRL HTTP server never started correctly.\n"
            f"Last HTTP code={last_code}\n"
            f"Last body={last_body}\n"
            "Likely port conflict or server start failure."
        )

    status, content = crl_utils_obj.set_settings(
        rest, urls=[crl_url], urlPollIntervalMs=url_poll_interval_ms
    )

    return {
        "shell_conn": shell_conn,
        "pid": pid,
        "port": chosen_port,
        "tmp_dir": tmp_dir,
        "crl_url": crl_url,
        "settings_status": status,
        "settings_content": content,
    }


def cleanup_url_poll_crl_env(env):
    """
    Mirrors jwt_utils.cleanup_jwks_uri_issuer_env: kills the background HTTP
    server, removes its temp dir, disconnects the shell. Does NOT reset CRL
    settings itself -- CRLBase.tearDown()'s _reset_crl_settings() already
    handles that.

    Kills by port (stop_process_on_port), not by the PID captured at start
    time -- confirmed by direct testing that $! after backgrounding a job
    over a non-interactive SSH channel doesn't reliably name the actual
    running process, which silently left a stray http.server process behind
    on a real node even though stop_remote_process(shell_conn, pid) reported success.
    """
    if not env:
        return
    shell_conn = env.get("shell_conn")
    port = env.get("port")
    tmp_dir = env.get("tmp_dir")

    try:
        if shell_conn:
            if port:
                stop_process_on_port(shell_conn, port)
            if tmp_dir:
                shell_conn.execute_command(f"sh -c \"rm -rf '{tmp_dir}' || true\"")
    except Exception:
        pass
    try:
        if shell_conn:
            shell_conn.disconnect()
    except Exception:
        pass
