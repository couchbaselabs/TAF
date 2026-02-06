
import json
import time
import uuid
import urllib.parse
import ast

from couchbase_utils.security_utils.jwt_utils import JWTUtils
from membase.api.rest_client import RestConnection
from pytests.onPrem_basetestcase import OnPremBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class JWTTokenTest(OnPremBaseTest):
    """
    Basic JWT auth sanity test for Couchbase.
    """

    def setUp(self):
        super(JWTTokenTest, self).setUp()

        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            install_dir = getattr(shell_conn, "default_install_dir", None)
            if not install_dir:
                install_dir = getattr(shell_conn, "cb_path", None)
                if install_dir:
                    shell_conn.default_install_dir = install_dir.rstrip("/")

            self.default_install_dir = getattr(shell_conn, "default_install_dir", None)
            self.cli_bin_dir = (
                f"{self.default_install_dir.rstrip('/')}/bin"
                if self.default_install_dir else None
            )
        finally:
            shell_conn.disconnect()

        self.binary_path = self.input.param("couchbase_cli_path", None)
        if not self.binary_path:
            if not self.cli_bin_dir:
                self.fail(
                    "Unable to derive Couchbase CLI path from remote install dir. "
                    "Please pass `couchbase_cli_path` in conf."
                )
            self.binary_path = f"{self.cli_bin_dir}/couchbase-cli"

        self.issuer_name = self.input.param("issuer_name", "custom-issuer")
        self.user_name = self.input.param("user_name", "jwt_user")
        self.algorithm = self.input.param("algorithm", "ES256")
        self.key_size = self.input.param("key_size", 2048)
        self.ttl = self.input.param("ttl", 300)
        self.nbf_seconds = self.input.param("nbf_seconds", 0)

        token_audience = self.input.param("token_audience", "['cb-cluster']")
        self.token_audience = self._safe_literal_list(token_audience, ["cb-cluster"])

        user_groups = self.input.param("user_groups", "['admin']")
        self.user_groups = self._safe_literal_list(user_groups, ["admin"])

        token_group_rule = self.input.param("token_group_rule", None)
        self.token_group_matching_rule = (
            self._safe_literal_list(token_group_rule, None) if token_group_rule else None
        )

        self.group_name = self.input.param("cb_group_name", None) or \
            f"jwt_admin_{uuid.uuid4().hex[:8]}"

        self.rest = RestConnection(self.cluster.master)
        self.jwt_utils = JWTUtils(log=self.log)
        self.private_key, self.pub_key = self.jwt_utils.generate_key_pair(
            algorithm=self.algorithm, key_size=self.key_size
        )

        self._enable_dev_preview()

    def tearDown(self):
        try:
            self._disable_jwt()
        finally:
            group_maps = self.token_group_matching_rule or [f"^admin$ {self.group_name}"]
            for group in self._extract_cb_groups_from_group_maps(group_maps):
                self._delete_group(group)
            super(JWTTokenTest, self).tearDown()

    @staticmethod
    def _safe_literal_list(val, default):
        """
        Parse a python-literal list from conf safely.
        `val` is usually a string like "['a','b']" from the .conf.
        """
        if val is None:
            return default
        if isinstance(val, (list, tuple)):
            return list(val)
        if not isinstance(val, str) or not val.strip():
            return default
        try:
            parsed = ast.literal_eval(val)
            if parsed is None:
                return default
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
            if isinstance(parsed, str):
                inner = parsed.strip()
                if (inner.startswith("[") and inner.endswith("]")) or \
                        (inner.startswith("(") and inner.endswith(")")):
                    try:
                        inner_parsed = ast.literal_eval(inner)
                        if isinstance(inner_parsed, (list, tuple)):
                            return list(inner_parsed)
                    except Exception:
                        pass
                return [parsed]
        except Exception:
            try:
                parsed = eval(val)  # noqa: S307
                if isinstance(parsed, (list, tuple)):
                    return list(parsed)
            except Exception:
                return default
        return default

    def _cluster_cli_args(self):
        if self.use_https:
            cluster_arg = f"https://localhost:{self.cluster.master.port}"
            no_ssl_verify = " --no-ssl-verify"
        else:
            cluster_arg = f"localhost:{self.cluster.master.port}"
            no_ssl_verify = ""
        return cluster_arg, no_ssl_verify

    def _enable_dev_preview(self):
        self.log.info("Enabling Developer Preview mode (JWT prerequisite)")
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            cluster_arg, no_ssl_verify = self._cluster_cli_args()

            cmd = (
                f"echo y | {self.binary_path} enable-developer-preview "
                f"-c {cluster_arg} "
                f"-u {self.cluster.master.rest_username} "
                f"-p {self.cluster.master.rest_password} "
                f"--enable{no_ssl_verify}"
            )
            output, err = shell_conn.execute_command(cmd)
            if err:
                err_text = " ".join(err).strip().lower()
                if "preview" in err_text and "already" in err_text:
                    self.log.info("Developer Preview already enabled")
                else:
                    self.log.error(f"enable-developer-preview stderr: {err}")
                    self.fail("Failed to enable Developer Preview mode")
            self.log.info(f"enable-developer-preview output: {output}")
        finally:
            shell_conn.disconnect()

    @staticmethod
    def _extract_cb_groups_from_group_maps(group_maps):
        groups = set()
        if not group_maps:
            return groups
        for entry in group_maps:
            try:
                group = entry.strip().split()[-1]
                if group:
                    groups.add(group)
            except Exception:
                continue
        return groups

    def _create_group(self, group_name, roles="admin", description=None):
        description = description or "Admin group for JWT authentication"
        payload = urllib.parse.urlencode({"description": description, "roles": roles})

        self._delete_group(group_name)
        self.log.info(f"Creating RBAC group {group_name} with roles {roles}")

        try:
            self.rest.add_set_bulitin_group(group_name, payload)
        except Exception as e:
            self.log.warn(f"Group helper failed ({e}); retrying raw REST call")
            api = f"{self.rest.baseUrl}settings/rbac/groups/{group_name}"
            ok, content, resp = self.rest._http_request(api, "PUT", payload)
            if not ok:
                status = self._status_code(resp)
                self.fail(
                    f"Failed to create group {group_name}. status={status} content={content}"
                )

    def _delete_group(self, group_name):
        if not group_name:
            return
        try:
            self.rest.delete_builtin_group(group_name)
        except Exception:
            pass

    def _get_jwt_config(self):
        group_maps = self.token_group_matching_rule or [f"^admin$ {self.group_name}"]
        return self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=group_maps,
            jit_provisioning=True,
        )

    def _put_jwt_config(self, config):
        headers = self.rest._create_capi_headers(contentType="application/json")
        body = json.dumps(config)

        paths = ["settings/security/jwt", "settings/jwt"]
        last_status, last_content = None, None
        for path in paths:
            api = f"{self.rest.baseUrl}{path}"
            ok, content, resp = self.rest._http_request(api, "PUT", body, headers=headers)
            status = self._status_code(resp)
            last_status, last_content = status, content
            if ok:
                return status, content
            if status == 404:
                continue
            self.fail(f"Failed to PUT /{path}. status={status} content={content}")

        self.fail(
            "Failed to PUT JWT config using all known endpoints. "
            f"last_status={last_status} last_content={last_content}"
        )

    def _disable_jwt(self):
        try:
            self._put_jwt_config({"enabled": False})
        except Exception:
            pass

    @staticmethod
    def _status_code(response):
        if response is None:
            return None
        return getattr(response, "status", None) or getattr(response, "status_code", None)

    def _request_with_jwt(self, token, endpoint, method="GET", params=""):
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        api = f"{self.rest.baseUrl}{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        ok, content, resp = self.rest._http_request(api, method, params, headers=headers)
        return ok, self._status_code(resp), content

    def create_token(self):
        token_audience = self.token_audience
        if isinstance(token_audience, list) and len(token_audience) == 1:
            token_audience = token_audience[0]
        return self.jwt_utils.create_token(
            issuer_name=self.issuer_name,
            user_name=self.user_name,
            algorithm=self.algorithm,
            private_key=self.private_key,
            token_audience=token_audience,
            user_groups=self.user_groups,
            ttl=self.ttl,
            nbf_seconds=self.nbf_seconds,
        )

    def _verify_token_rest(self, token):
        """
        Verify JWT token works for a REST request.
        Returns: (ok_bool, status_code, content)
        """
        return self._request_with_jwt(token, "/pools/default/buckets", method="GET")

    def _verify_token_cli(self, token):
        """
        Verify JWT token works for a CLI request.

        Prefer `couchbase-cli` if it supports bearer token flags; else fallback
        to `curl` as a CLI-based verification for the same REST endpoint.
        Returns: (ok_bool, status_code_or_none, output)
        """
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            cluster_arg, no_ssl_verify = self._cluster_cli_args()

            help_out, help_err = shell_conn.execute_command(f"{self.binary_path} --help")
            help_text = "\n".join(help_out + help_err).lower()

            bearer_flag = None
            for flag in ["--bearer-token", "--jwt", "--auth-token", "--access-token"]:
                if flag in help_text:
                    bearer_flag = flag
                    break

            if bearer_flag:
                cmd = (
                    f"{self.binary_path} server-list "
                    f"-c {cluster_arg} {bearer_flag} '{token}'{no_ssl_verify}"
                )
                out, err = shell_conn.execute_command(cmd)
                text = "\n".join(out + err).strip()
                ok = "error" not in text.lower() and "unauthorized" not in text.lower()
                return ok, None, text

            scheme = "https" if self.use_https else "http"
            insecure = "-k " if self.use_https else ""
            url = f"{scheme}://127.0.0.1:{self.cluster.master.port}/pools/default/buckets"
            cmd = (
                f"curl -s {insecure}-o /tmp/jwt_cli_out.txt -w '%{{http_code}}' "
                f"-H 'Authorization: Bearer {token}' "
                f"'{url}'"
            )
            http_code_out, err = shell_conn.execute_command(cmd)
            http_code = None
            if http_code_out and http_code_out[0]:
                http_code = http_code_out[0].strip().strip("'").strip('"')
            body_out, _ = shell_conn.execute_command("cat /tmp/jwt_cli_out.txt || true")
            body = "\n".join(body_out).strip()
            ok = http_code is not None and str(http_code).isdigit() and (200 <= int(http_code) < 300)
            return ok, http_code, body
        finally:
            shell_conn.disconnect()

    def _setup_jwt_config(self, config, create_groups=True):
        """
        PUT jwt config, optionally create referenced groups, and wait for it to take effect.
        """
        group_maps = config.get("issuers", [{}])[0].get("groupsMaps", [])
        if create_groups:
            for group in self._extract_cb_groups_from_group_maps(group_maps):
                self._create_group(group, roles="admin")

        self.log.info(f"JWT config payload: {json.dumps(config, indent=2)}")
        status, content = self._put_jwt_config(config)
        self.log.info(f"JWT config PUT status={status} content={content}")
        self.sleep(10, "Waiting for JWT config to take effect")

    def test_create_config(self):
        """
        Basic JWT auth test.

        Validates token works for:
        - REST request (python)
        - CLI request (couchbase-cli if supported, else curl fallback)
        """
        expected_jwt_auth = self.input.param("expected_jwt_auth", True)
        create_groups = self.input.param("create_groups", True)
        verify_cli = self.input.param("verify_cli", True)

        config = self._get_jwt_config()
        self._setup_jwt_config(config, create_groups=create_groups)

        token = self.create_token()
        self.log.info("JWT token generated (redacted)")

        rest_ok, status_code, content = self._verify_token_rest(token)
        self.log.info(f"JWT REST ok={rest_ok} status={status_code} content={content}")

        cli_ok, cli_status, cli_out = True, None, ""
        if verify_cli:
            cli_ok, cli_status, cli_out = self._verify_token_cli(token)
            self.log.info(f"JWT CLI ok={cli_ok} status={cli_status} out={cli_out}")

        if expected_jwt_auth:
            if status_code is None:
                self.fail("No HTTP status code returned for REST JWT request")
            if not (200 <= int(status_code) < 300):
                self.fail(f"JWT REST auth failed. status={status_code} content={content}")
            if verify_cli and not cli_ok:
                self.fail(f"JWT CLI auth failed. status={cli_status} out={cli_out}")
        else:
            rest_failed = (status_code is None) or not (200 <= int(status_code) < 300)
            cli_failed = (not cli_ok) if verify_cli else True
            if not (rest_failed and cli_failed):
                self.fail(
                    "Expected JWT auth to fail but at least one path succeeded. "
                    f"rest_status={status_code} cli_ok={cli_ok}"
                )

    def test_jwt_token_matrix(self):
        """
        Permutation-friendly JWT token tests for positive/negative scenarios.

        Allows overriding token fields independently of config fields using:
        - token_issuer_name
        - token_user_name
        - token_algorithm
        - token_private_key_mode: "config_key" (default) | "new_key"
        - token_audience
        - token_user_groups
        - token_ttl
        - token_nbf_seconds

        Expected outcomes:
        - expected_token_create: True/False
        - expected_jwt_auth: True/False
        """
        expected_token_create = self.input.param("expected_token_create", True)
        expected_jwt_auth = self.input.param("expected_jwt_auth", True)
        create_groups = self.input.param("create_groups", True)
        verify_cli = self.input.param("verify_cli", True)

        config = self._get_jwt_config()
        self._setup_jwt_config(config, create_groups=create_groups)

        token_issuer_name = self.input.param("token_issuer_name", self.issuer_name)
        token_user_name = self.input.param("token_user_name", self.user_name)
        token_algorithm = self.input.param("token_algorithm", self.algorithm)

        token_audience_raw = self.input.param("token_audience_override", None)
        token_audience = (
            self._safe_literal_list(token_audience_raw, None)
            if token_audience_raw is not None
            else self.token_audience
        )
        if isinstance(token_audience, list) and len(token_audience) == 1:
            token_audience = token_audience[0]

        token_groups_raw = self.input.param("token_user_groups_override", None)
        token_user_groups = (
            self._safe_literal_list(token_groups_raw, None)
            if token_groups_raw is not None
            else self.user_groups
        )

        token_ttl = self.input.param("token_ttl", self.ttl)
        token_nbf_seconds = self.input.param("token_nbf_seconds", self.nbf_seconds)

        token_private_key_mode = self.input.param("token_private_key_mode", "config_key")
        token_private_key = self.private_key
        if token_private_key_mode == "new_key":
            token_private_key, _ = self.jwt_utils.generate_key_pair(
                algorithm=token_algorithm, key_size=self.key_size
            )

        token = None
        token_create_err = None
        try:
            token = self.jwt_utils.create_token(
                issuer_name=token_issuer_name,
                user_name=token_user_name,
                algorithm=token_algorithm,
                private_key=token_private_key,
                token_audience=token_audience,
                user_groups=token_user_groups,
                ttl=token_ttl,
                nbf_seconds=token_nbf_seconds,
            )
        except Exception as e:
            token_create_err = str(e)

        if expected_token_create:
            if not token:
                self.fail(f"Expected token creation to succeed but failed: {token_create_err}")
        else:
            if token:
                self.fail("Expected token creation to fail but it succeeded")
            return

        self.log.info("JWT token generated for matrix test (redacted)")

        rest_ok, status_code, content = self._verify_token_rest(token)
        self.log.info(f"JWT REST ok={rest_ok} status={status_code} content={content}")

        cli_ok, cli_status, cli_out = True, None, ""
        if verify_cli:
            cli_ok, cli_status, cli_out = self._verify_token_cli(token)
            self.log.info(f"JWT CLI ok={cli_ok} status={cli_status} out={cli_out}")

        if expected_jwt_auth:
            if status_code is None or not (200 <= int(status_code) < 300):
                self.fail(f"JWT REST auth failed. status={status_code} content={content}")
            if verify_cli and not cli_ok:
                self.fail(f"JWT CLI auth failed. status={cli_status} out={cli_out}")
        else:
            rest_failed = (status_code is None) or not (200 <= int(status_code) < 300)
            cli_failed = (not cli_ok) if verify_cli else True
            if not (rest_failed and cli_failed):
                self.fail(
                    "Expected JWT auth to fail but at least one path succeeded. "
                    f"rest_status={status_code} cli_ok={cli_ok}"
                )


