"""
Kubectl / EKS Library for TAF Fusion Tests
Provides functionality to assume an IAM role (e.g. jenkins-cp-cli), point
kubectl at a target control-plane EKS cluster, and run the kubectl commands
needed to reach a Couchbase control-plane database running in a private
subnet (get pods, describe pod, port-forward).
"""

import base64
import json
import logging
import os
import platform
import shutil
import stat
import subprocess
import tempfile
import threading
import urllib.request
from typing import Optional

import yaml
from botocore.signers import RequestSigner

from .iam_lib import IAMLib


class KubectlLib:
    """
    EKS/kubectl library: assumes an IAM role for cluster access (via IAMLib),
    configures kubectl for the target EKS cluster entirely via boto3 (describe
    cluster + mint a bearer token — no `aws` CLI), and wraps the `kubectl`
    commands needed to reach a private control-plane Couchbase pod
    (e.g. sandbox database access).
    """

    DEFAULT_COMMAND_TIMEOUT = 60
    DEFAULT_PORT_FORWARD_READY_TIMEOUT = 60
    DEFAULT_CP_POD_NAME = "cp-couchbase-0000"

    # Pinned kubectl for the auto-download fallback (see _kubectl_bin): used only
    # when the agent has no `kubectl` on PATH. Any recent kubectl talks fine to
    # the EKS API server via the static-token kubeconfig we generate.
    KUBECTL_DOWNLOAD_VERSION = "v1.30.5"

    # The Datadog Autodiscovery annotation the operator stamps onto each CP
    # Couchbase pod; its value is a JSON array of {server, user, password}
    # dicts describing the readonly monitoring credentials for that pod.
    COUCHBASE_INSTANCES_ANNOTATION = "ad.datadoghq.com/couchbase-server.instances"

    def __init__(self, access_key=None, secret_key=None, session_token=None, region=None):
        """
        Initialize the kubectl/EKS client.

        :param access_key: AWS access key for the base/source profile
                            (e.g. the credentials used to assume jenkins-cp-cli).
                            Falls back to the AWS_ACCESS_KEY_ID_004 env var
                            (via IAMLib) if not given.
        :param secret_key: AWS secret key for the base/source profile. Falls
                            back to the AWS_SECRET_ACCESS_KEY_004 env var
                            (via IAMLib) if not given.
        :param session_token: AWS session token for the base profile (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        """
        logging.basicConfig()
        self.logger = logging.getLogger("Kubectl_Util")
        self.region = region or 'us-east-1'

        self.iam = IAMLib(access_key, secret_key, session_token, region=self.region)

        self._lock = threading.Lock()
        self._port_forward_procs = {}

        # EKS access is done WITHOUT the `aws` CLI: we describe the cluster and
        # mint the k8s bearer token via boto3 (works on any arch where botocore
        # imports), then write a static-token kubeconfig that kubectl reads via
        # the KUBECONFIG env var. This avoids depending on a correctly-arch'd
        # `aws` binary on the runner (an x86 aws on an arm64 agent gives ENOEXEC).
        self._kubeconfig_path = None
        self._eks_session = None
        self._eks_endpoint = None
        self._eks_ca_data = None
        self._eks_cluster_name = None
        self._eks_region = None
        self._eks_context = None
        # Resolved `kubectl` binary (PATH if present, else an auto-downloaded
        # static build — some minimal CI agents ship neither aws nor kubectl).
        self._kubectl_path = None
        self.last_error = None

    def assume_role(self, role_arn: str, external_id: str = None,
                     role_session_name: str = None,
                     duration_seconds: int = None) -> bool:
        """
        Assume an IAM role (e.g. jenkins-cp-cli) via IAMLib, so subsequent
        aws/kubectl subprocess calls run with its temporary credentials.

        :param role_arn: ARN of the role to assume
        :param external_id: External ID required by the role's trust policy (optional)
        :param role_session_name: Session name to tag the assumed session with
        :param duration_seconds: Duration of the assumed session in seconds
                                  (max 3600 when role chaining is involved)
        :return: True on success, False otherwise
        """
        ok = self.iam.assume_role(role_arn, external_id=external_id,
                                   role_session_name=role_session_name,
                                   duration_seconds=duration_seconds)
        if not ok:
            self.last_error = self.iam.last_error
        return ok

    def _subprocess_env(self) -> dict[str, str]:
        """Build the environment used for kubectl subprocess calls."""
        env = os.environ.copy()
        env.update(self.iam.get_env())
        # Point kubectl at the static-token kubeconfig we generate via boto3 so
        # it never invokes the `aws` CLI (no exec-credential plugin).
        if self._kubeconfig_path:
            env["KUBECONFIG"] = self._kubeconfig_path
        return env

    def _kubectl_bin(self) -> str:
        """Path to a usable `kubectl`: the one on PATH if present, else a static
        build downloaded once for this arch to a temp dir. Minimal CI agents ship
        neither `aws` nor `kubectl`; EKS auth is handled via boto3 + the static-
        token kubeconfig, so a plain binary is all that's needed here."""
        if self._kubectl_path:
            return self._kubectl_path
        found = shutil.which("kubectl")
        if found:
            self._kubectl_path = found
            return found
        self._kubectl_path = self._download_kubectl()
        return self._kubectl_path

    def _download_kubectl(self) -> str:
        """Download a static kubectl matching the host arch (linux/amd64|arm64)
        to a stable temp path (once), make it executable, and return its path."""
        arch = {"x86_64": "amd64", "amd64": "amd64",
                "aarch64": "arm64", "arm64": "arm64"}.get(
                    platform.machine().lower(), "amd64")
        dest = os.path.join(tempfile.gettempdir(),
                            f"kubectl-{self.KUBECTL_DOWNLOAD_VERSION}-{arch}")
        with self._lock:
            if os.path.exists(dest) and os.path.getsize(dest) > 0:
                return dest
            url = (f"https://dl.k8s.io/release/{self.KUBECTL_DOWNLOAD_VERSION}"
                   f"/bin/linux/{arch}/kubectl")
            self.logger.info(f"kubectl not on PATH — downloading {url}")
            tmp = dest + ".part"
            urllib.request.urlretrieve(url, tmp)
            os.chmod(tmp, os.stat(tmp).st_mode | stat.S_IEXEC
                     | stat.S_IXGRP | stat.S_IXOTH)
            os.replace(tmp, dest)
            self.logger.info(f"kubectl ready at {dest}")
        return dest

    def _run(self, cmd: list[str], timeout: int = None):
        """
        Run a command (argv list form, never shell=True) and return
        (success, stdout, stderr).
        """
        # Refresh the kubeconfig's EKS token right before each kubectl call so a
        # long test run never races the token's ~15 min lifetime, and resolve the
        # kubectl binary (PATH or auto-downloaded).
        if cmd and cmd[0] == "kubectl":
            if self._kubeconfig_path:
                self._write_kubeconfig()
            cmd = [self._kubectl_bin()] + cmd[1:]
        try:
            result = subprocess.run(
                cmd,
                env=self._subprocess_env(),
                capture_output=True,
                text=True,
                timeout=timeout or self.DEFAULT_COMMAND_TIMEOUT)
            if result.returncode != 0:
                self.logger.error(f"Command {' '.join(cmd)} failed: {result.stderr.strip()}")
                return False, result.stdout, result.stderr
            return True, result.stdout, result.stderr
        except subprocess.TimeoutExpired as e:
            self.logger.error(f"Command {' '.join(cmd)} timed out: {e}")
            return False, "", str(e)
        except OSError as e:
            self.logger.error(f"Command {' '.join(cmd)} failed to start: {e}")
            return False, "", str(e)

    def _boto3_session(self, region: str):
        """boto3 session for the currently assumed role (auto-refreshing), or the
        base credentials session if assume_role() was not called."""
        try:
            return self.iam.get_boto3_session(region=region)
        except RuntimeError:
            return self.iam.aws_session

    def _generate_eks_token(self, session, cluster_name: str, region: str) -> str:
        """Mint an EKS `k8s-aws-v1.` bearer token in pure Python (the same
        presigned STS GetCallerIdentity URL that `aws eks get-token` produces),
        so no `aws` CLI is needed. The `x-k8s-aws-id` header binds the token to
        the target cluster; EKS accepts it for ~15 min from signing."""
        botocore_session = session._session
        sts = session.client("sts", region_name=region)
        signer = RequestSigner(
            sts.meta.service_model.service_id, region, "sts", "v4",
            botocore_session.get_credentials(),
            botocore_session.get_component("event_emitter"))
        signed_url = signer.generate_presigned_url(
            {"method": "GET",
             "url": f"https://sts.{region}.amazonaws.com/"
                    "?Action=GetCallerIdentity&Version=2011-06-15",
             "body": {},
             "headers": {"x-k8s-aws-id": cluster_name},
             "context": {}},
            region_name=region, expires_in=900, operation_name="")
        return "k8s-aws-v1." + base64.urlsafe_b64encode(
            signed_url.encode("utf-8")).decode("utf-8").rstrip("=")

    def _write_kubeconfig(self) -> None:
        """(Re)write the static-token kubeconfig using the cached EKS endpoint/CA
        and a freshly minted token. Cheap (token is a local presign); called on
        connect and before each kubectl call to keep the token within its TTL."""
        with self._lock:
            token = self._generate_eks_token(
                self._eks_session, self._eks_cluster_name, self._eks_region)
            ctx = self._eks_context
            cfg = {
                "apiVersion": "v1", "kind": "Config",
                "clusters": [{"name": ctx, "cluster": {
                    "server": self._eks_endpoint,
                    "certificate-authority-data": self._eks_ca_data}}],
                "contexts": [{"name": ctx, "context": {"cluster": ctx, "user": ctx}}],
                "current-context": ctx,
                "users": [{"name": ctx, "user": {"token": token}}],
            }
            if not self._kubeconfig_path:
                fd, self._kubeconfig_path = tempfile.mkstemp(suffix=".kubeconfig")
                os.close(fd)
            with open(self._kubeconfig_path, "w") as fh:
                yaml.safe_dump(cfg, fh, default_flow_style=False)

    def update_kubeconfig(self, cluster_name: str, region: str = None,
                           alias: str = None) -> bool:
        """
        Configure kubectl for the target EKS cluster WITHOUT the `aws` CLI:
        describe the cluster (endpoint + CA) and mint a bearer token via boto3,
        then write a static-token kubeconfig that kubectl reads via KUBECONFIG.
        Uses the currently assumed role (falls back to the base session
        credentials if assume_role() was not called).

        :param cluster_name: Name of the EKS cluster (e.g. sbx-10-cp-eks, qe-7-cp-eks)
        :param region: AWS region override (defaults to self.region)
        :param alias: Optional kubeconfig context name
        :return: True on success, False otherwise
        """
        region = region or self.region
        try:
            session = self._boto3_session(region)
            cluster = session.client("eks", region_name=region).describe_cluster(
                name=cluster_name)["cluster"]
            self._eks_session = session
            self._eks_region = region
            self._eks_cluster_name = cluster_name
            self._eks_endpoint = cluster["endpoint"]
            self._eks_ca_data = cluster["certificateAuthority"]["data"]
            self._eks_context = alias or cluster_name
            self._write_kubeconfig()
            self.logger.info(
                f"Configured kubeconfig for EKS cluster {cluster_name} via boto3 "
                f"(no aws CLI); endpoint {self._eks_endpoint}")
            return True
        except Exception as e:
            self.last_error = str(e)
            self.logger.error(
                f"Failed to configure kubeconfig for EKS cluster {cluster_name}: {e}")
            return False

    def use_context(self, context_name: str) -> bool:
        """Switch the active kubectl context to `context_name`."""
        success, _, _ = self._run(["kubectl", "config", "use-context", context_name])
        return success

    def get_pods(self, label_selector: str = None, context: str = None,
                 namespace: str = None) -> list[str]:
        """
        Run `kubectl get pods` and return the matching pod names.

        :param label_selector: e.g. "app=couchbase"
        :param context: kubectl context to target (optional)
        :param namespace: kubernetes namespace (optional)
        :return: list of pod names (empty list on failure)
        """
        cmd = ["kubectl", "get", "pods",
               "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        if label_selector:
            cmd += ["-l", label_selector]
        if context:
            cmd += ["--context", context]
        if namespace:
            cmd += ["-n", namespace]
        success, stdout, _ = self._run(cmd)
        if not success:
            return []
        return [line.strip() for line in stdout.splitlines() if line.strip()]

    def describe_pod(self, pod_name: str, context: str = None,
                      namespace: str = None) -> str:
        """
        Run `kubectl describe pod <pod_name>` and return the raw text output
        (used to read out the Couchbase readonly credentials, empty string on failure).
        """
        cmd = ["kubectl", "describe", "pod", pod_name]
        if context:
            cmd += ["--context", context]
        if namespace:
            cmd += ["-n", namespace]
        success, stdout, _ = self._run(cmd)
        return stdout if success else ""

    def get_pod_json(self, pod_name: str, context: str = None,
                      namespace: str = None) -> dict:
        """
        Run `kubectl get pod <pod_name> -o json` and return the parsed pod
        object. Unlike `describe_pod`, kubectl does not line-wrap `-o json`
        output, so long annotation values (e.g. embedded JSON blobs) come
        back intact instead of split across lines by terminal width.

        :return: the parsed pod object, or {} on failure
        """
        cmd = ["kubectl", "get", "pod", pod_name, "-o", "json"]
        if context:
            cmd += ["--context", context]
        if namespace:
            cmd += ["-n", namespace]
        success, stdout, _ = self._run(cmd)
        if not success:
            return {}
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse `kubectl get pod {pod_name} -o json` output: {e}")
            return {}

    def get_couchbase_instances(self, pod_name: str = None, context: str = None,
                                 namespace: str = None) -> list[dict]:
        """
        Return the parsed `ad.datadoghq.com/couchbase-server.instances`
        annotation of a CP Couchbase pod: a list of
        {"server": ..., "user": ..., "password": ...} dicts.

        :param pod_name: Pod to inspect (defaults to DEFAULT_CP_POD_NAME)
        :return: list of credential dicts, empty list on failure
        """
        pod_name = pod_name or self.DEFAULT_CP_POD_NAME
        pod = self.get_pod_json(pod_name, context=context, namespace=namespace)
        if not pod:
            return []

        annotations = pod.get("metadata", {}).get("annotations") or {}
        raw = annotations.get(self.COUCHBASE_INSTANCES_ANNOTATION)
        if not raw:
            self.logger.error(
                f"{self.COUCHBASE_INSTANCES_ANNOTATION} annotation not found on pod {pod_name}")
            return []
        try:
            instances = json.loads(raw)
        except json.JSONDecodeError as e:
            self.logger.error(
                f"Failed to parse {self.COUCHBASE_INSTANCES_ANNOTATION} annotation on pod {pod_name}: {e}")
            return []
        if not isinstance(instances, list) or not all(isinstance(i, dict) for i in instances):
            self.logger.error(
                f"Unexpected {self.COUCHBASE_INSTANCES_ANNOTATION} annotation shape on pod {pod_name}: "
                f"expected a list of dicts, got {type(instances).__name__}")
            return []
        return instances

    def get_cp_db_credentials(self, pod_name: str = None, context: str = None,
                               namespace: str = None) -> dict:
        """
        Return the readonly Couchbase credentials (server, user, password)
        for a CP database pod, read from its Datadog Autodiscovery
        annotation. Never logs the password.

        :param pod_name: Pod to inspect (defaults to DEFAULT_CP_POD_NAME)
        :return: {"server": ..., "user": ..., "password": ...} for the first
                 instance in the annotation, or {} if unavailable
        """
        instances = self.get_couchbase_instances(pod_name, context=context, namespace=namespace)
        if not instances or "user" not in instances[0] or "password" not in instances[0]:
            return {}
        creds = instances[0]
        self.logger.info(
            f"Fetched CP db credentials for pod {pod_name or self.DEFAULT_CP_POD_NAME}: "
            f"server={creds.get('server')}, user={creds.get('user')}")
        return creds

    # The k8s secret holding the CP Couchbase admin credentials (the same source
    # `cbc-db` uses). Unlike the Datadog Autodiscovery annotation — which carries
    # a monitoring-only user that lacks the query_select role — this user can run
    # N1QL against the CP database.
    CP_COUCHBASE_AUTH_SECRET = "cp-couchbase-auth"

    def get_cp_db_credentials_from_secret(self, context: str = None,
                                           namespace: str = None) -> dict:
        """
        Return the CP Couchbase credentials from the `cp-couchbase-auth` k8s
        secret (base64-decoded username/password). This is the query-capable DB
        user; prefer it over the Datadog annotation for N1QL. Never logs the
        password. Returns {} if the secret is unavailable.
        """
        cmd = ["kubectl", "get", "secret", self.CP_COUCHBASE_AUTH_SECRET, "-o", "json"]
        if context:
            cmd += ["--context", context]
        if namespace:
            cmd += ["-n", namespace]
        success, stdout, _ = self._run(cmd)
        if not success:
            return {}
        try:
            data = json.loads(stdout).get("data", {}) or {}
            user = base64.b64decode(data.get("username", "")).decode("utf-8").strip()
            password = base64.b64decode(data.get("password", "")).decode("utf-8").strip()
        except (ValueError, json.JSONDecodeError) as e:
            self.logger.error(
                f"Failed to parse {self.CP_COUCHBASE_AUTH_SECRET} secret: {e}")
            return {}
        if not user or not password:
            return {}
        self.logger.info(
            f"Fetched CP db credentials from {self.CP_COUCHBASE_AUTH_SECRET} secret: user={user}")
        return {"user": user, "password": password}

    # The k8s secret holding the cp-api internal-support token (the value the
    # /internal/support/* endpoints authenticate against, e.g. feature flags).
    CP_API_SECRETS_AUTH_SECRET = "cp-api-secrets-auth"
    INTERNAL_SUPPORT_TOKEN_KEY = "TOKEN_FOR_INTERNAL_SUPPORT"

    def get_internal_support_token(self, context: str = None,
                                    namespace: str = None) -> Optional[str]:
        """
        Return the cp-api internal-support token from the `cp-api-secrets-auth`
        secret (base64-decoded `TOKEN_FOR_INTERNAL_SUPPORT`). This is the Bearer
        token the /internal/support/* endpoints require (feature flags, etc.).
        Never logs the token. None if unavailable.
        """
        cmd = ["kubectl", "get", "secret", self.CP_API_SECRETS_AUTH_SECRET, "-o", "json"]
        if context:
            cmd += ["--context", context]
        if namespace:
            cmd += ["-n", namespace]
        success, stdout, _ = self._run(cmd)
        if not success:
            return None
        try:
            data = json.loads(stdout).get("data", {}) or {}
            token = base64.b64decode(
                data.get(self.INTERNAL_SUPPORT_TOKEN_KEY, "")).decode("utf-8").strip()
        except (ValueError, json.JSONDecodeError) as e:
            self.logger.error(
                f"Failed to parse {self.CP_API_SECRETS_AUTH_SECRET} secret: {e}")
            return None
        return token or None

    def start_port_forward(self, pod_name: str, local_port: int, remote_port: int,
                            context: str = None, namespace: str = None,
                            ready_timeout: int = None) -> Optional[subprocess.Popen]:
        """
        Start `kubectl port-forward <pod_name> <local_port>:<remote_port>` as a
        background process and block until the "Forwarding from" line is seen.

        :return: the Popen handle (also tracked internally, keyed by pod_name)
                 for use with stop_port_forward(); None if it failed to start
                 or did not become ready in time.
        """
        cmd = [self._kubectl_bin(), "port-forward", pod_name,
               f"{local_port}:{remote_port}"]
        if context:
            cmd += ["--context", context]
        if namespace:
            cmd += ["-n", namespace]
        # Freshly mint the token before the (long-lived) tunnel authenticates.
        if self._kubeconfig_path:
            self._write_kubeconfig()
        try:
            proc = subprocess.Popen(
                cmd,
                env=self._subprocess_env(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True)
        except OSError as e:
            self.logger.error(f"Failed to start port-forward for {pod_name}: {e}")
            return None

        # proc.stdout.readline() has no timeout of its own, so a plain read
        # loop can't honor ready_timeout precisely. Instead, a background
        # thread drains stdout for the lifetime of the process (avoiding a
        # pipe-buffer deadlock once the tunnel is handling traffic) and wakes
        # this call as soon as it sees the "Forwarding from" confirmation
        # line, or as soon as the stream closes (process exited early).
        ready_state = {"confirmed": False}
        wake_event = threading.Event()
        reader = threading.Thread(
            target=self._stream_port_forward_output,
            args=(proc, ready_state, wake_event),
            daemon=True)
        reader.start()

        wake_event.wait(timeout=ready_timeout or self.DEFAULT_PORT_FORWARD_READY_TIMEOUT)

        if not ready_state["confirmed"] or proc.poll() is not None:
            self.logger.error(f"Port-forward for {pod_name} did not become ready in time")
            self.stop_port_forward(proc)
            return None

        with self._lock:
            self._port_forward_procs[pod_name] = proc
        return proc

    def _stream_port_forward_output(self, proc: subprocess.Popen, ready_state: dict[str, bool],
                                     wake_event: threading.Event) -> None:
        """
        Continuously drain a port-forward process's stdout so its pipe buffer
        never fills up and blocks kubectl, and signal `wake_event` as soon as
        the "Forwarding from" readiness line is seen (or the stream closes).
        """
        try:
            for line in iter(proc.stdout.readline, ''):
                if not line:
                    break
                stripped = line.strip()
                if "Forwarding from" in stripped:
                    ready_state["confirmed"] = True
                    self.logger.info(stripped)
                    wake_event.set()
                else:
                    self.logger.debug(stripped)
        except (OSError, ValueError):
            pass
        finally:
            wake_event.set()

    def stop_port_forward(self, proc_or_pod_name) -> bool:
        """
        Terminate a running port-forward, given either the Popen handle
        returned by start_port_forward() or the pod name it targets.
        """
        proc = proc_or_pod_name
        if isinstance(proc_or_pod_name, str):
            with self._lock:
                proc = self._port_forward_procs.pop(proc_or_pod_name, None)
        if proc is None:
            return False
        if proc.poll() is not None:
            return True
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.logger.error(f"Failed to kill port-forward process pid={proc.pid}")
                return False
        except OSError as e:
            self.logger.error(f"Error stopping port-forward: {e}")
            return False
        return True

    def stop_all_port_forwards(self) -> None:
        """Terminate all tracked port-forward processes (tearDown helper)."""
        with self._lock:
            pod_names = list(self._port_forward_procs.keys())
        for pod_name in pod_names:
            self.stop_port_forward(pod_name)
