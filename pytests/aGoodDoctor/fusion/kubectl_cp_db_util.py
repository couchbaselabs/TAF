"""
Kubectl CP DB Util - Fetches Couchbase control-plane database credentials
from a private CP EKS cluster via the jenkins-cp-cli IAM role, and runs
N1QL queries against it, so fusion tests can reach a CP database (e.g. the
sandbox database) without direct network access.
"""

import requests

from .awslib.kubectl_lib import KubectlLib


class KubectlCPDBUtil:
    """High-level util for reaching a CP Couchbase pod via kubectl."""

    DEFAULT_MGMT_PORT = 8091
    DEFAULT_MGMT_LOCAL_PORT = 21091
    DEFAULT_QUERY_PORT = 8093
    DEFAULT_QUERY_LOCAL_PORT = 21093
    DEFAULT_QUERY_TIMEOUT_SECONDS = 30

    def __init__(self, access_key=None, secret_key=None, session_token=None, region=None):
        """
        :param access_key: AWS access key for the base/source profile used to assume
                            the CP cluster access role (e.g. jenkins-cp-cli). Falls
                            back to the AWS_ACCESS_KEY_ID_004 env var (via IAMLib)
                            if not given.
        :param secret_key: AWS secret key for the base/source profile. Falls back
                            to the AWS_SECRET_ACCESS_KEY_004 env var (via IAMLib)
                            if not given.
        :param session_token: AWS session token for the base profile (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        """
        self.kubectl = KubectlLib(access_key, secret_key, session_token, region=region)
        self.log = self.kubectl.logger

    def connect(self, cluster_name, role_arn, external_id, alias=None):
        """
        Assume the CP cluster access IAM role (e.g. jenkins-cp-cli) and point
        kubectl at the given CP EKS cluster. Must be called once before
        get_cp_db_credentials().

        role_arn/external_id are secrets that gate cross-account role
        assumption — callers must source them from test params/config
        (e.g. TestInputSingleton), never hard-code them here.

        :param cluster_name: CP EKS cluster name, e.g. "qe-7-cp-eks", "sbx-10-cp-eks"
        :param role_arn: ARN of the IAM role to assume
        :param external_id: External ID required by the role's trust policy
        :param alias: Optional kubeconfig context alias
        :return: True on success, False otherwise
        """
        if not self.kubectl.assume_role(role_arn=role_arn, external_id=external_id):
            return False
        return self.kubectl.update_kubeconfig(cluster_name, alias=alias)

    def get_cp_db_credentials(self, pod_name=None, namespace=None):
        """
        Fetch the CP database's readonly credentials (server, user, password)
        by inspecting the pod's Datadog Autodiscovery annotation. connect()
        must be called first.

        :param pod_name: Pod to inspect (defaults to KubectlLib.DEFAULT_CP_POD_NAME,
                          i.e. "cp-couchbase-0000")
        :param namespace: Kubernetes namespace (optional)
        :return: {"server": ..., "user": ..., "password": ...} dict,
                 or None if the credentials could not be read
        """
        return self.kubectl.get_cp_db_credentials(pod_name=pod_name, namespace=namespace)

    def _find_query_pod(self, creds, namespace=None):
        """
        Discover which pod in the CP cluster is running the query (n1ql)
        service. The service isn't necessarily on DEFAULT_CP_POD_NAME (that
        pod may be KV-only) and can move between pods as the cluster scales,
        so this reads it live from `/pools/default/nodeServices` via a
        port-forward to a management endpoint.

        :param creds: {"user": ..., "password": ...} dict from get_cp_db_credentials()
        :return: pod name of a node running the n1ql service, or None if
                 none was found / reachable
        """
        proc = self.kubectl.start_port_forward(
            self.kubectl.DEFAULT_CP_POD_NAME, self.DEFAULT_MGMT_LOCAL_PORT,
            self.DEFAULT_MGMT_PORT, namespace=namespace)
        if proc is None:
            return None
        try:
            response = requests.get(
                f"http://127.0.0.1:{self.DEFAULT_MGMT_LOCAL_PORT}/pools/default/nodeServices",
                auth=(creds["user"], creds["password"]), timeout=10)
            response.raise_for_status()
            for node in response.json().get("nodesExt", []):
                if "n1ql" in node.get("services", {}):
                    return node["hostname"].split(".")[0]
            self.log.error("No node running the n1ql service found in nodeServices")
            return None
        except requests.RequestException as e:
            self.log.error(f"Failed to query nodeServices for the n1ql pod: {e}")
            return None
        finally:
            self.kubectl.stop_port_forward(self.kubectl.DEFAULT_CP_POD_NAME)

    def run_n1ql_query(self, statement, pod_name=None, namespace=None,
                        local_port=None, timeout=None):
        """
        Run a N1QL query against the CP database over a kubectl port-forward
        to the query service's REST endpoint (`/query/service`). connect()
        must be called first.

        A full Couchbase SDK cluster connection isn't viable here: the CP
        cluster is multi-node and each node advertises an in-cluster
        Kubernetes Service DNS hostname (e.g.
        "cp-couchbase-0000.cp-couchbase.default.svc") that a kubectl
        port-forward has no way to substitute for once the SDK looks past
        its initial bootstrap hop. Going straight to the query REST endpoint
        sidesteps that — N1QL is one HTTP hop, and the query engine resolves
        the rest of the cluster (KV, indexer) internally on the server side.

        :param statement: N1QL statement to execute
        :param pod_name: Pod whose query service to reach (auto-discovered
                          via nodeServices if not given — the query service
                          isn't necessarily on DEFAULT_CP_POD_NAME)
        :param namespace: Kubernetes namespace (optional)
        :param local_port: Local port for the tunnel (defaults to DEFAULT_QUERY_LOCAL_PORT)
        :param timeout: Query timeout in seconds (defaults to DEFAULT_QUERY_TIMEOUT_SECONDS)
        :return: list of result rows (dicts)
        :raises RuntimeError: if credentials, the port-forward, or the query itself fail
        """
        local_port = local_port or self.DEFAULT_QUERY_LOCAL_PORT
        timeout = timeout or self.DEFAULT_QUERY_TIMEOUT_SECONDS

        creds = self.get_cp_db_credentials(namespace=namespace)
        if not creds:
            raise RuntimeError("CP db credentials unavailable")

        if pod_name is None:
            pod_name = self._find_query_pod(creds, namespace=namespace)
            if not pod_name:
                raise RuntimeError("No node running the n1ql query service was found in the CP cluster")

        proc = self.kubectl.start_port_forward(
            pod_name, local_port, self.DEFAULT_QUERY_PORT, namespace=namespace)
        if proc is None:
            raise RuntimeError(
                f"Could not open a port-forward to {pod_name}:{self.DEFAULT_QUERY_PORT}")

        try:
            response = requests.post(
                f"http://127.0.0.1:{local_port}/query/service",
                data={"statement": statement, "timeout": f"{timeout}s"},
                auth=(creds["user"], creds["password"]),
                timeout=timeout + 10)
            body = response.json()
            if body.get("status") != "success":
                raise RuntimeError(
                    f"N1QL query failed on pod {pod_name}: {body.get('errors', body)}")
            return body.get("results", [])
        except requests.RequestException as e:
            raise RuntimeError(f"N1QL query request to pod {pod_name} failed: {e}") from e
        finally:
            self.kubectl.stop_port_forward(pod_name)

    def disconnect(self):
        """Tear down any port-forwards started via this util (no-op if none were started)."""
        self.kubectl.stop_all_port_forwards()
