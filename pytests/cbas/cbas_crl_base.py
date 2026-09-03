import json
import os
import re
import tempfile
import time
import traceback
import uuid

import requests
from cb_constants import CbServer
from cb_server_rest_util.security.security_api import SecurityRestAPI
from couchbase_utils.rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.crl_utils import CRLUtils
from couchbase_utils.security_utils.x509main import x509main
from cryptography.x509.oid import NameOID
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from cbas.cbas_base_server import CBASBaseTest


class CBASCRLBase(CBASBaseTest):
    """
    Fixture for CRL (Certificate Revocation List) tests against the Analytics
    service, per Analytics_CRL_Lifecycle_TestPlan.

    Deliberately a sibling of pytests/backup_restore/crl_base.py rather than a
    subclass of it: that one is rooted in CollectionBase and requires a Backup
    Service node, while these tests need CBASBaseTest's Analytics setup. The
    CA-trust / certificate / clientCertAuth helpers below are the same shape in
    both, and are the natural thing to lift into a shared mixin if a third
    service-specific CRL suite appears -- see the note on each helper that has
    a counterpart.
    """

    ANALYTICS_PORT = CbServer.cbas_port          # 8095
    ANALYTICS_SSL_PORT = CbServer.ssl_cbas_port  # 18095
    MGMT_SSL_PORT = 18091

    def setUp(self):
        super(CBASCRLBase, self).setUp()

        self.crl_utils = CRLUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        # Cleaned up in tearDown
        self._created_files = []
        self._rbac_users = []
        self._trusted_ca_ids = []
        self._temp_pem_files = []

        self._require_crl_supported()
        self._require_analytics_node()

        # Unique CN per test, NOT a constant one. CRLs are matched to a
        # certificate by issuer NAME, so a CRL or trusted CA surviving a
        # previous test's failed teardown collides with a fresh CA that has the
        # same CN but a different key -- and the validator then rejects the
        # stale CRL against the new key with `invalid_signature`, leaving
        # revocation status 'undetermined' and failing closed under Require.
        # Observed exactly that after a teardown could not reach the node:
        # three trusted CAs all named CN=AnalyticsCRLTestCA1, and a leftover
        # CRL from one of them poisoning the next test.
        self.ca_cert, self.ca_key = self.crl_utils.generate_ca(
            f"AnalyticsCRLTestCA_{uuid.uuid4().hex[:8]}"
        )
        self._trust_ca_on_cluster(self.ca_cert)

    def tearDown(self):
        # Log any failure's traceback before the cleanup below, so a failing
        # run is triageable while it is still executing rather than only once
        # testrunner prints its summary.
        self._log_test_failure()

        # Plain HTTP, and first: a test that left clientCertAuth in
        # 'mandatory' walls out self.rest, and every other cleanup step below
        # goes over HTTPS with no client certificate attached.
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
        for label, fn in (
            ("audit restore", self._restore_audit),
            ("external link cleanup", self._cleanup_links),
            ("crlsValidate counter removal", self._crls_validate_counter_stop),
            ("CRL file cleanup", self._cleanup_created_files),
            ("CRL settings reset", self._reset_crl_settings),
            ("RBAC user cleanup", self._cleanup_rbac_users),
            ("temp PEM cleanup", self._cleanup_temp_pem_files),
            ("trusted CA cleanup", self._cleanup_trusted_cas),
        ):
            try:
                fn()
            except Exception as exc:
                self.log.warning(f"{label} error: {exc}")
        super(CBASCRLBase, self).tearDown()

    # ── Preconditions ────────────────────────────────────────────────────────


    # ── External object store (section 4) ────────────────────────────────────

    # Matches the packets cbauth sends to ns_server to ask for a revocation
    # verdict. Counting them is how a test shows a code path consulted the
    # CRL machinery -- or, for an external link, that it did not.
    _CRLS_VALIDATE_MATCH = ("OUTPUT -o lo -p tcp --dport 8091 "
                            "-m string --string crlsValidate --algo bm")
    CRLS_VALIDATE_RULE = f"{_CRLS_VALIDATE_MATCH} -j ACCEPT"

    def _require_object_store(self):
        """
        Connection details for an S3-compatible object store, from the same
        LOCALSTACK_* environment variables the backup CRL suite uses (which in
        practice point at a MinIO instance).

        Fails rather than skips when unset: a silently skipped test reports as
        passing coverage that never ran, and section 4 is exactly the area
        where that would matter.
        """
        endpoint = os.getenv("LOCALSTACK_ENDPOINT")
        if not endpoint:
            self.fail(
                "This test needs an S3-compatible object store (MinIO) that "
                "the Analytics node can reach. Set LOCALSTACK_ENDPOINT, "
                "LOCALSTACK_ACCESS_KEY_ID, LOCALSTACK_SECRET_ACCESS_KEY and "
                "optionally LOCALSTACK_REGION / LOCALSTACK_BUCKET."
            )
        return {
            "endpoint": endpoint,
            "access_key": os.getenv("LOCALSTACK_ACCESS_KEY_ID", "minioadmin"),
            "secret_key": os.getenv("LOCALSTACK_SECRET_ACCESS_KEY",
                                    "minioadmin"),
            "region": os.getenv("LOCALSTACK_REGION", "us-east-1"),
            "bucket": os.getenv("LOCALSTACK_BUCKET", "cbas-crl-test"),
        }

    def _create_s3_link(self, name, store, dataverse="Default"):
        """
        Create an S3-type external link pointing at `store`, and register it
        for cleanup. Returns the link_properties dict that was used.

        Access-key authentication on purpose: that is the case section 4 asks
        about, and it is the one where nothing certificate-shaped is involved.
        """
        link_properties = {
            "name": name,
            "dataverse": dataverse,
            "scope": dataverse,
            "type": "s3",
            "accessKeyId": store["access_key"],
            "secretAccessKey": store["secret_key"],
            "region": store["region"],
            "serviceEndpoint": store["endpoint"],
        }
        created = self.cbas_util.create_link(
            self.cluster, link_properties, create_dataverse=False
        )
        if not created:
            self.fail(f"Could not create S3 external link {dataverse}.{name}")
        self._created_links.append((dataverse, name))
        self.log.info(
            f"Created S3 external link {dataverse}.{name} -> "
            f"{store['endpoint']}"
        )
        return link_properties

    def _cleanup_links(self):
        """Drop every external link a test created."""
        for dataverse, name in getattr(self, "_created_links", []):
            try:
                self.cbas_util.drop_link(
                    self.cluster, f"{dataverse}.{name}", if_exists=True
                )
            except Exception as exc:
                self.log.warning(f"Link {dataverse}.{name} drop error: {exc}")
        self._created_links = []

    def _link_exists(self, name, dataverse="Default"):
        """True if the named link is present in Analytics metadata."""
        return self.cbas_util.validate_link_in_metadata(
            self.cluster, name, dataverse, "s3"
        )

    # ── crlsValidate accounting ──────────────────────────────────────────────

    def _crls_validate_counter_start(self):
        """Install the counting rule on the Analytics node; return the count."""
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            # Drop any rule left by an aborted run so the count comes from one
            # rule rather than several stacked ones.
            shell.execute_command(
                f"iptables -D {self.CRLS_VALIDATE_RULE} 2>/dev/null")
            shell.execute_command(f"iptables -I {self.CRLS_VALIDATE_RULE}")
            self._crls_counter_installed = True
        finally:
            shell.disconnect()
        return self._crls_validate_count()

    def _crls_validate_count(self):
        """Packets matched so far, or None if the rule is absent."""
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            # execute_command returns (stdout, stderr) in that order.
            output, _ = shell.execute_command(
                "iptables -L OUTPUT -v -n | grep crlsValidate | awk '{print $1}'"
            )
        finally:
            shell.disconnect()
        for line in (output or []):
            token = line.strip()
            if token.isdigit():
                return int(token)
        return None

    def _crls_validate_counter_stop(self):
        """Remove the counting rule. Safe when it was never installed."""
        if not getattr(self, "_crls_counter_installed", False):
            return
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            shell.execute_command(
                f"iptables -D {self.CRLS_VALIDATE_RULE} 2>/dev/null")
        finally:
            shell.disconnect()
        self._crls_counter_installed = False

    def _require_crl_supported(self):
        if not self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("CRL support requires an Enterprise Edition cluster.")

    def _require_analytics_node(self):
        if not self.cluster.cbas_nodes:
            self.fail(
                "Analytics CRL tests need a cbas node -- pass "
                "services_init with 'cbas' on at least one node."
            )
        self.cbas_node = self.cluster.cbas_nodes[0]
        # External links created during a test -- dropped in tearDown
        self._created_links = []
        # Whether the crlsValidate counting iptables rule is installed
        self._crls_counter_installed = False

    # ── Failure reporting ────────────────────────────────────────────────────

    def _log_test_failure(self):
        """
        Emit the current test's traceback, if it failed, at the start of
        tearDown. Reads unittest's own _outcome.errors -- the structure
        cb_basetest.is_test_failed() checks -- so it reports what testrunner
        will report, just sooner. Best-effort: _outcome is a unittest internal
        whose shape changed after Python 3.10, so a miss logs nothing rather
        than masking the real failure.
        """
        try:
            outcome = getattr(self, "_outcome", None)
            errors = getattr(outcome, "errors", None) if outcome else None
            if not errors:
                return
            for _test, exc_info in errors:
                if not exc_info:
                    continue
                self.log.error(
                    f"TEST FAILED: {self._testMethodName}\n"
                    + "".join(traceback.format_exception(*exc_info)).strip()
                )
        except Exception as exc:
            self.log.warning(f"Could not log test failure traceback: {exc}")

    # ── CA trust (counterpart: backup_restore/crl_base.py) ───────────────────

    @staticmethod
    def _ca_dir(shell):
        os_type = shell.extract_remote_info().distribution_type
        if os_type == "windows":
            install_path = x509main.WININSTALLPATH
        elif os_type == "Mac":
            install_path = x509main.MACINSTALLPATH
        else:
            install_path = x509main.LININSTALLPATH
        return f"{install_path}{x509main.CHAINFILEPATH}/CA"

    @staticmethod
    def _ca_remote_filename(ca_cert):
        cn_attrs = ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn = cn_attrs[0].value if cn_attrs else "ca"
        safe_cn = re.sub(r"[^A-Za-z0-9_.-]", "_", cn)
        return f"{safe_cn}_{ca_cert.serial_number}.pem"

    def _trust_ca_on_cluster(self, ca_cert, server=None):
        server = server or self.cluster.master
        pem_bytes = self.crl_utils.cert_to_pem(ca_cert)
        remote_filename = self._ca_remote_filename(ca_cert)

        shell = RemoteMachineShellConnection(server)
        try:
            ca_dir = self._ca_dir(shell)
            shell.execute_command(f"mkdir -p {ca_dir}")
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".pem", mode="wb"
            ) as tmp_file:
                tmp_file.write(pem_bytes)
                local_path = tmp_file.name
            try:
                shell.copy_file_local_to_remote(
                    local_path, f"{ca_dir}/{remote_filename}"
                )
            finally:
                os.remove(local_path)
        finally:
            shell.disconnect()

        status, content = self.rest.load_trusted_CAs()
        if not status:
            self.fail(f"Failed to load trusted CAs on {server.ip}: {content}")

        cn_attrs = ca_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        cn = cn_attrs[0].value if cn_attrs else None
        try:
            trusted = json.loads(content)
            matching = [
                entry.get("id") for entry in trusted
                if cn and cn in entry.get("subject", "")
            ]
            if matching:
                self._trusted_ca_ids.append(max(matching))
        except (ValueError, TypeError) as exc:
            self.log.warning(
                f"Could not identify trusted CA id for {cn!r}; it will not be "
                f"auto-untrusted in tearDown: {exc}"
            )

    # ── mTLS / CRL configuration ─────────────────────────────────────────────

    def _enable_client_cert_auth(self, state="enable", prefixes=None):
        if prefixes is None:
            prefixes = [{"path": "subject.cn", "prefix": "", "delimiter": ""}]
        status, content, _ = SecurityRestAPI(
            self.cluster.master
        ).set_client_cert_auth_config(state=state, prefixes=prefixes)
        self.assertTrue(status, f"Failed to set clientCertAuth: {content}")

    def _disable_client_cert_auth(self):
        # Plain HTTP on purpose -- works even while clientCertAuth is
        # 'mandatory', which walls out every HTTPS call including the one that
        # would relax it.
        server = self.cluster.master
        requests.post(
            f"http://{server.ip}:8091/settings/clientCertAuth",
            auth=(server.rest_username, server.rest_password),
            headers={"Content-Type": "application/json"},
            json={"state": "disable", "prefixes": []},
            timeout=30,
        )

    def _reset_crl_settings(self):
        self.crl_utils.set_settings(
            self.rest,
            policyPerScope={"clientAuth": "Disabled", "nodeToNode": "Disabled"},
            checkIntermediateCerts=False,
            urls=[],
        )

    # ── Certificates and CRL files ───────────────────────────────────────────

    def _write_temp_pem(self, pem_bytes, suffix=".pem"):
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix, mode="wb"
        ) as tmp_file:
            tmp_file.write(pem_bytes)
            path = tmp_file.name
        self._temp_pem_files.append(path)
        return path

    def _cleanup_temp_pem_files(self):
        for path in self._temp_pem_files:
            try:
                os.remove(path)
            except OSError:
                pass
        self._temp_pem_files = []

    def _client_cert_for(self, username, role="analytics_admin"):
        """
        An RBAC user plus a leaf certificate whose CN maps to it.

        Returns:
            tuple: (cert_path, key_path, serial)
        """
        user, _ = self._create_rbac_test_user(username, role)
        cert, key, serial = self.crl_utils.generate_leaf_cert(
            self.ca_cert, self.ca_key, user
        )
        cert_path = self._write_temp_pem(self.crl_utils.cert_to_pem(cert))
        key_path = self._write_temp_pem(self.crl_utils.key_to_pem(key))
        return cert_path, key_path, serial

    def _track_uploaded_file(self, filename):
        self._created_files.append(filename)

    def _cleanup_created_files(self):
        for filename in self._created_files:
            status, _ = self.crl_utils.delete_file(self.rest, filename)
            if not status:
                self.log.warning(f"Failed to delete CRL file {filename}")
        self._created_files = []

    # ── RBAC ─────────────────────────────────────────────────────────────────

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
                self.log.warning(f"Failed to delete RBAC user {username}: {exc}")
        self._rbac_users = []

    def _cleanup_trusted_cas(self):
        """
        Drop this run's test CAs from the cluster trust store.

        Filters ca_certificates in chronicle_kv rather than calling a
        delete-CA API: this is the form already proven against a live cluster
        by pytests/backup_restore/crl_base.py, and a stale test CA left behind
        can trip the NEXT run's setUp.
        """
        if not self._trusted_ca_ids:
            return
        ids_literal = "[" + ",".join(str(i) for i in self._trusted_ca_ids) + "]"
        code = (
            "{ok, {Certs, _Rev}} = chronicle_kv:get(kv, ca_certificates), "
            f"Ids = {ids_literal}, "
            "NewCerts = lists:filter(fun(PL) -> "
            "not lists:member(proplists:get_value(id, PL), Ids) end, Certs), "
            "chronicle_kv:set(kv, ca_certificates, NewCerts)."
        )
        status, content = self.rest.diag_eval(code)
        if not status:
            self.log.warning(f"Trusted CA cleanup diag/eval failed: {content}")
        self._trusted_ca_ids = []

    # ── Analytics requests ───────────────────────────────────────────────────

    def _analytics_query(self, statement, cert=None, auth=None, timeout=60,
                         node=None):
        """
        POST a SQL++ statement to the Analytics service over TLS, optionally
        presenting a client certificate.

        Bypasses CbasUtil deliberately: this suite needs `cert=` plumbed
        straight through to `requests`, which the util layer does not expose,
        and `verify=False` because the node's own certificate is self-signed
        (enabling verification would abort locally before the server ever
        evaluates the CRL).
        """
        node = node or self.cbas_node
        url = (f"https://{node.ip}:{self.ANALYTICS_SSL_PORT}"
               f"/analytics/service")
        return requests.post(
            url, data={"statement": statement}, cert=cert, auth=auth,
            verify=False, timeout=timeout, headers={"Connection": "close"},
        )

    def _mgmt_request(self, path="/pools/default", cert=None, auth=None,
                      timeout=30, node=None):
        """
        The same request against ns_server's mgmt port, for the cross-service
        consistency scenario: Analytics enforcement must match ns_server for
        the same certificate and policy.
        """
        node = node or self.cluster.master
        url = f"https://{node.ip}:{self.MGMT_SSL_PORT}{path}"
        return requests.get(
            url, cert=cert, auth=auth, verify=False, timeout=timeout,
            headers={"Connection": "close"},
        )

    def _read_analytics_log(self, grep=None, tail_lines=3000):
        """
        Lines from the Analytics logs on self.cbas_node.

        Reads analytics_info/error/debug together: the CRL verdict for a
        rejected handshake has been observed in analytics_info.log, while the
        Java exception chain carrying the same detail lands in the error log,
        and which one a given build uses is not worth assuming.

        Args:
            grep: substring to filter on (e.g. a certificate CN), applied on
                the node so only matching lines cross the wire.
            tail_lines: cap per file -- a validator problem can produce very
                large logs, and an unbounded read over SSH looks like a hang.
        """
        pattern = grep.replace("'", "") if grep else ""
        cmd = (
            "for f in /opt/couchbase/var/lib/couchbase/logs/analytics_info.log "
            "/opt/couchbase/var/lib/couchbase/logs/analytics_error.log "
            "/opt/couchbase/var/lib/couchbase/logs/analytics_debug.log; do "
            f"[ -f \"$f\" ] && tail -n {int(tail_lines)} \"$f\"; done"
        )
        if pattern:
            cmd += f" | grep -a '{pattern}'"
        cmd += " || true"
        shell = RemoteMachineShellConnection(self.cbas_node)
        try:
            output, _ = shell.execute_command(cmd)
        finally:
            shell.disconnect()
        return [line.rstrip("\n") for line in (output or []) if line.strip()]

    # A ConnectionError only proves refusal if it carries one of these. Same
    # set the backup and FTS CRL suites use, so a rejection is identified
    # consistently across suites.
    TLS_REJECTION_MARKERS = (
        "certificate revoked", "tlsv1 alert", "sslv3 alert",
        "alert certificate", "handshake failure", "bad certificate",
        "unknown ca", "certificate unknown", "certificate required",
        "decrypt error",
    )

    @classmethod
    def _is_tls_rejection(cls, exc):
        """
        True when a ConnectionError carries an explicit TLS alert, i.e. the
        peer refused the certificate rather than the connection simply
        failing.

        Shared by assert_cert_refused and the long-running probes, so both
        agree on what counts as a refusal instead of each deciding for
        itself.
        """
        text = str(exc).lower()
        return any(marker in text for marker in cls.TLS_REJECTION_MARKERS)

    def assert_cert_refused(self, request_fn, msg):
        """
        Assert a certificate was refused, in either of the two forms the PRD
        permits.

        The PRD's runtime-enforcement section says of the revocation check:
        "For Phase I. It is acceptable to return 401 instead of sending a tls
        alert to allow NS-server to use the callback." A test that insists on a
        TLS alert would therefore fail against a conformant implementation that
        answers 401 instead. Analytics sends alerts today, so these tests pass
        either way -- accepting both keeps them from breaking on a legitimate
        change.

        Deliberately narrow about what counts as refusal:
          TLS-layer alert (SSLError)           -> refused
          ConnectionError carrying a TLS alert -> refused
          ConnectionError with no TLS alert    -> FAILURE, not refusal
          HTTP 401                             -> refused (the PRD's allowance)
          anything else, including 200/403/5xx -> failure

        403 is excluded because it means the certificate was accepted and then
        denied by RBAC, which is a different outcome from revocation.

        The ConnectionError split is the important one. A wrong port, a node
        that is down and a dropped packet all raise ConnectionError just as a
        refused handshake does, so accepting the bare exception would let
        every test here pass against a product that never enforced anything.
        Only an error carrying an explicit TLS alert counts.

        Args:
            request_fn: zero-arg callable performing the request.
            msg: what the caller is proving, used in the failure message.

        Returns:
            The response when refusal came as a 401, else None.
        """
        try:
            resp = request_fn()
        except requests.exceptions.SSLError:
            # A TLS-layer failure is a refusal by definition.
            return None
        except requests.exceptions.ConnectionError as exc:
            # NOT a refusal on its own. A wrong port, a down node or a
            # dropped packet also raises ConnectionError, and treating those
            # as "revocation enforced" lets the test pass while the product
            # does nothing. Only accept one that carries an explicit TLS
            # alert marker.
            if not self._is_tls_rejection(exc):
                self.fail(
                    f"{msg}. The connection failed, but with no TLS alert to "
                    f"show it was refused for the certificate: {exc}. That "
                    f"looks like an infrastructure problem (wrong port, node "
                    f"down, packet dropped) rather than enforcement, and "
                    f"passing on it would hide a product that never enforced "
                    f"at all."
                )
            return None
        self.assertEqual(
            resp.status_code, 401,
            f"{msg}. The connection was not refused at the TLS layer, so the "
            f"only other acceptable outcome is HTTP 401 (per the PRD's Phase I "
            f"allowance); got {resp.status_code}: {resp.text[:300]}"
        )
        return resp

    def _wait_for_analytics_ok(self, statement="SELECT 1;", cert=None,
                               auth=None, timeout_s=60, interval=3):
        """
        Poll until Analytics answers 200, absorbing the propagation delay
        between a CA-trust / CRL / clientCertAuth change landing on
        cluster.master and the Analytics node picking it up. Raises the last
        SSLError, or returns the last non-200 response, if the deadline passes.
        """
        deadline = time.time() + timeout_s
        last_exc = None
        last_resp = None
        while time.time() < deadline:
            try:
                resp = self._analytics_query(statement, cert=cert, auth=auth)
                if resp.status_code == 200:
                    return resp
                last_resp = resp
            except requests.exceptions.SSLError as exc:
                last_exc = exc
            time.sleep(interval)
        if last_resp is not None:
            return last_resp
        raise last_exc

    # ── Diagnostics, metrics and audit (sections 6 and 9) ────────────────────

    # Statuses the diagnostics endpoint uses for a certificate it is happy
    # with. Anything else means it would refuse, which is how the section 6
    # consistency check maps a verdict onto expected runtime behaviour.
    # Determined empirically against 8.5.0-1009 rather than taken from the
    # plan, whose status list ("not_revoked", "unknown_missing_crl", ...) does
    # not match what the server actually returns.
    DIAGNOSTICS_OK_STATUSES = ("valid", "not_revoked", "ok")

    def _diagnostics_verdict(self, pem_bytes, policy="Require"):
        """
        Ask the admin diagnostic endpoint how it judges one certificate.

        Returns (status, details) from the single results entry, or
        (None, raw) if the call or its shape was not what we expect.
        """
        pem = pem_bytes.decode() if isinstance(pem_bytes, bytes) else pem_bytes
        status, content = self.crl_utils.diagnostics_validate(
            self.rest, policy=policy, certs=[pem]
        )
        if not status or not isinstance(content, dict):
            return None, content
        results = content.get("results") or []
        if len(results) != 1:
            return None, content
        return results[0].get("status"), results[0].get("details")

    def _diagnostics_says_acceptable(self, verdict_status):
        """True if the diagnostics verdict is one a connection should survive."""
        return (verdict_status or "").lower() in self.DIAGNOSTICS_OK_STATUSES

    def _crl_metrics(self, service="cbas", node=None):
        """
        The cbauth CRL metrics for one service, from Prometheus on ns_server.

        Returns {metric_name: float}. Four families exist on 8.5.0-1009, all
        cache-oriented and all carrying a service label:
        cm_cbauth_crl_cache_{current_items,hit_total,miss_total,max_items}.

        Defaults to the Analytics node, not the master: /metrics is served
        per node and reports that node's own cbauth counters, so scraping the
        master shows nothing for a check performed on a different node.
        """
        node = node or self.cbas_node
        resp = requests.get(
            f"http://{node.ip}:8091/metrics",
            auth=(node.rest_username, node.rest_password), timeout=60,
        )
        resp.raise_for_status()
        out = {}
        for line in resp.text.splitlines():
            if not line.startswith("cm_cbauth_crl"):
                continue
            if f'service="{service}"' not in line:
                continue
            name = line.split("{", 1)[0]
            try:
                out[name] = float(line.rsplit(" ", 1)[1])
            except (IndexError, ValueError):
                continue
        return out

    def _cbas_crl_metrics(self, node=None):
        """
        The Analytics service's own CRL metrics, from Prometheus on ns_server.

        Returns {series: float} keyed by the full series including labels,
        e.g. 'cbas_crl_checks_total{result="rejected",scope="clientAuth"}'.
        Label order is whatever the exporter emits, so match with
        _cbas_crl_metric_sum rather than by exact key.

        Added by MB-73654 (cbas-core 3acd9eb, first in 8.5.0-1101), and
        distinct from the cm_cbauth_crl_* family that _crl_metrics reads:
        these are cbas_-prefixed on the Analytics registry. ns_server
        re-exposes them on its own /metrics alongside the other cbas_ series,
        so one scrape of the same endpoint serves both families.

        Scraped from the Analytics node for the same reason as _crl_metrics:
        each process reports its own counters.
        """
        node = node or self.cbas_node
        resp = requests.get(
            f"http://{node.ip}:8091/metrics",
            auth=(node.rest_username, node.rest_password), timeout=60,
        )
        resp.raise_for_status()
        out = {}
        for line in resp.text.splitlines():
            if not line.startswith("cbas_crl"):
                continue
            try:
                series, value = line.rsplit(" ", 1)
                out[series.strip()] = float(value)
            except (IndexError, ValueError):
                continue
        return out

    @staticmethod
    def _cbas_crl_metric_sum(metrics, name, **labels):
        """
        Sum every series of `name` whose label set includes all of `labels`.

        Matching a subset rather than an exact series string keeps callers
        independent of label ordering and of labels added later.

        :param metrics: dict from _cbas_crl_metrics
        :param name: metric name without labels, e.g. cbas_crl_checks_total
        :param labels: label values that must all be present
        """
        total = 0.0
        for series, value in metrics.items():
            if series.split("{", 1)[0] != name:
                continue
            if all(f'{key}="{val}"' in series for key, val in labels.items()):
                total += value
        return total

    def _crl_metric_services(self, node=None):
        """Every service label present on the cbauth CRL metrics."""
        node = node or self.cbas_node
        resp = requests.get(
            f"http://{node.ip}:8091/metrics",
            auth=(node.rest_username, node.rest_password), timeout=60,
        )
        resp.raise_for_status()
        services = set()
        for line in resp.text.splitlines():
            if line.startswith("cm_cbauth_crl") and 'service="' in line:
                services.add(line.split('service="', 1)[1].split('"', 1)[0])
        return services

    def _set_audit(self, enabled):
        """Turn cluster auditing on or off, remembering the original state."""
        if not hasattr(self, "_audit_was_enabled"):
            try:
                self._audit_was_enabled = bool(
                    self.rest.getAuditSettings().get("auditdEnabled"))
            except Exception:
                self._audit_was_enabled = False
        self.rest.setAuditSettings(enabled="true" if enabled else "false")
        self._audit_restore_needed = True

    def _restore_audit(self):
        """Put auditing back the way the test found it."""
        if not getattr(self, "_audit_restore_needed", False):
            return
        try:
            self.rest.setAuditSettings(
                enabled="true" if self._audit_was_enabled else "false")
        except Exception as exc:
            self.log.warning(f"Audit restore error: {exc}")
        self._audit_restore_needed = False

    def _read_audit_log(self, since_marker=None, tail_lines=2000, node=None):
        """
        Lines from audit.log across the cluster.

        Every node keeps its own audit.log and only records what it handled,
        so a REST action against the master is not visible on the Analytics
        node. Unless a specific node is asked for, all of them are read and
        the lines concatenated.
        """
        nodes = [node] if node else list(
            self.cluster.nodes_in_cluster or [self.cbas_node])
        lines = []
        for target in nodes:
            shell = RemoteMachineShellConnection(target)
            try:
                # The file is NOT audit.log. ns_server writes a
                # per-rotation file named <host>-<timestamp>-audit.log and
                # points current-audit.log at it, so a plain audit.log read
                # finds nothing and looks exactly like "auditing recorded
                # nothing".
                output, _ = shell.execute_command(
                    f"tail -n {tail_lines} "
                    f"/opt/couchbase/var/lib/couchbase/logs/current-audit.log "
                    f"2>/dev/null || tail -n {tail_lines} "
                    f"/opt/couchbase/var/lib/couchbase/logs/*-audit.log "
                    f"2>/dev/null"
                )
                lines.extend(output or [])
            except Exception as exc:
                self.log.warning(f"audit.log read failed on {target.ip}: {exc}")
            finally:
                shell.disconnect()
        return lines

